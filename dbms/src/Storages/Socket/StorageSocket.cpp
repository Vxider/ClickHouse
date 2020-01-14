#include <Storages/Socket/StorageSocket.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Socket/SocketBlockInputStream.h>
#include <Storages/Socket/SocketSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/Exception.h>
#include <Common/Macros.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    const auto RESCHEDULE_MS = 500;
}

StorageSocket::StorageSocket(
    const std::string & table_name_,
    const std::string & database_name_,
    Context & context_,
    const ColumnsDescription & columns_,
    UInt64 zmq_parallelism,
    const String & zmq_host_,
    UInt16 zmq_port_,
    const String & format_name_,
    char row_delimiter_,
    UInt64 max_block_size_,
    UInt64 socket_timeout_,
    size_t skip_broken_,
    bool intermediate_commit_)
    : IStorage(columns_)
    , table_name(table_name_)
    , database_name(database_name_)
    , global_context(context_)
    , host(zmq_host_)
    , port(zmq_port_)
    , format_name(global_context.getMacros()->expand(format_name_))
    , row_delimiter(row_delimiter_)
    , max_block_size(max_block_size_)
    , socket_timeout(socket_timeout_)
    , log(&Logger::get("StorageSocket (" + table_name_ + ")"))
    , semaphore(0, 1)
    , skip_broken(skip_broken_)
    , intermediate_commit(intermediate_commit_)
    , zmq_context(zmq_parallelism)
{
    task = global_context.getSchedulePool().createTask(log->name(), [this] { streamThread(); });
    task->deactivate();
}

BlockInputStreams StorageSocket::read(
    const Names & column_names,
    const SelectQueryInfo & /* query_info */,
    const Context & context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    check(column_names);

    if (has_consumer_connected)
        return BlockInputStreams();

    /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        /// TODO: probably that leads to awful performance.
        /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
        streams.emplace_back(std::make_shared<SocketBlockInputStream>(*this, context, 1));
    }

    LOG_DEBUG(log, "Starting reading " << streams.size() << " streams");
    return streams;
}

void StorageSocket::startup()
{
    try
    {
        pushBuffer(createBuffer());
        ++num_created_consumers;
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log);
    }
}

void StorageSocket::shutdown()
{
    // Interrupt streaming thread
    stream_cancelled = true;

    // Close all consumers
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto buffer = claimBuffer();
        // FIXME: not sure if we really close consumers here, and if we really need to close them here.
    }

    LOG_TRACE(log, "Waiting for cleanup");

    //TODO: Close all zmq connection

    task->deactivate();
}

BufferPtr StorageSocket::createBuffer()
{
    // Create a consumer.
    auto consumer = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PULL);
    std::stringstream url;
    url << "tcp://" << host << ":" << port;
    LOG_DEBUG(log, "StorageSocket connect url:" << url.str());
    consumer->connect(url.str());
    consumer->setsockopt(ZMQ_RCVTIMEO, 1000);


    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = global_context.getSettingsRef();
    size_t batch_size = max_block_size;
    if (!batch_size)
        batch_size = settings.max_block_size.value;
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();

    return std::make_shared<DelimitedReadBuffer>(
        std::make_unique<ReadBufferFromSocketConsumer>(consumer, log, batch_size, poll_timeout,intermediate_commit), row_delimiter);
}

BufferPtr StorageSocket::tryClaimBuffer(long wait_ms)
{
    // Wait for the first free buffer
    if (wait_ms >= 0)
    {
        if (!semaphore.tryWait(wait_ms))
            return nullptr;
    }
    else
        semaphore.wait();

    // Take the first available buffer from the list
    std::lock_guard lock(mutex);
    auto buffer = buffers.back();
    buffers.pop_back();
    return buffer;
}

BufferPtr StorageSocket::claimBuffer()
{
    return tryClaimBuffer(-1L);
}

void StorageSocket::pushBuffer(BufferPtr buffer)
{
    std::lock_guard lock(mutex);
    buffers.push_back(buffer);
    semaphore.set();
}

void StorageSocket::streamThread()
{
    try
    {
        // Check if at least one direct dependency is attached
        auto dependencies = global_context.getDependencies(database_name, table_name);

        // Keep streaming as long as there are attached views and streaming is not cancelled
        while (!stream_cancelled && num_created_consumers > 0 && dependencies.size() > 0)
        {
            if (!checkDependencies(database_name, table_name))
                break;

            LOG_DEBUG(log, "Started streaming to " << dependencies.size() << " attached views");

            // Reschedule if not limited
            if (!streamToViews())
                break;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!stream_cancelled)
        task->scheduleAfter(RESCHEDULE_MS);
}

void StorageSocket::updateDependencies()
{
    task->activateAndSchedule();
}

bool StorageSocket::streamToViews()
{
    auto table = global_context.getTable(database_name, table_name);
    if (!table)
        throw Exception("Engine table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->database = database_name;
    insert->table = table_name;
    insert->no_destination = true; // Only insert into dependent views

    const Settings & settings = global_context.getSettingsRef();
    size_t block_size = max_block_size;
    if (block_size == 0)
        block_size = settings.max_block_size.value;

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream = std::make_shared<SocketBlockInputStream>(*this, global_context, block_size);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;
        limits.max_execution_time = settings.stream_flush_interval_ms;
        limits.timeout_overflow_mode = OverflowMode::BREAK;
        stream->setLimits(limits);
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    // Execute the query
    InterpreterInsertQuery interpreter{insert, global_context};
    auto block_io = interpreter.execute();
    copyData(*in, *block_io.out, &stream_cancelled);

    // Check whether the limits were applied during query execution
    bool limits_applied = false;
    const BlockStreamProfileInfo & info = in->getProfileInfo();
    limits_applied = info.hasAppliedLimit();

    return limits_applied;
}

bool StorageSocket::checkDependencies(const String & current_database_name, const String & current_table_name)
{
    // Check if all dependencies are attached
    auto dependencies = global_context.getDependencies(current_database_name, current_table_name);
    if (dependencies.size() == 0)
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = global_context.tryGetTable(db_tab.first, db_tab.second);
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab.first, db_tab.second))
            return false;
    }

    return true;
}

void registerStorageSocket(StorageFactory & factory)
{
    factory.registerStorage("Socket", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        SocketSettings socket_settings;
        if (has_settings)
        {
            socket_settings.loadFromQuery(*args.storage_def);
        }

/** Arguments of engine is following:
          * - Socket host
          * - Socket port
          * - Message format (string)
          * - Row delimiter
          * - Skip (at least) unreadable messages number
          */

// Check arguments and settings
#define CHECK_SOCKET_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME) \
    /* One of the four required arguments is not specified */ \
    if (args_count < ARG_NUM && ARG_NUM <= 4 && !socket_settings.PAR_NAME.changed) \
    { \
        throw Exception( \
            "Required parameter '" #PAR_NAME "' " \
            "for storage Socket not specified", \
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH); \
    } \
    /* The same argument is given in two places */ \
    if (has_settings && socket_settings.PAR_NAME.changed && args_count >= ARG_NUM) \
    { \
        throw Exception( \
            "The argument №" #ARG_NUM " of storage Socket" \
            "and the parameter '" #PAR_NAME "' " \
            "in SETTINGS cannot be specified at the same time", \
            ErrorCodes::BAD_ARGUMENTS); \
    }

        CHECK_SOCKET_STORAGE_ARGUMENT(1, socket_host)
        CHECK_SOCKET_STORAGE_ARGUMENT(2, socket_port)
        CHECK_SOCKET_STORAGE_ARGUMENT(3, socket_format)
        CHECK_SOCKET_STORAGE_ARGUMENT(4, socket_row_delimiter)
        CHECK_SOCKET_STORAGE_ARGUMENT(5, socket_max_block_size)
        CHECK_SOCKET_STORAGE_ARGUMENT(6, socket_skip_broken_messages)
        CHECK_SOCKET_STORAGE_ARGUMENT(7, socket_zmq_parallelism)
        CHECK_SOCKET_STORAGE_ARGUMENT(8, socket_timeout)

#undef CHECK_SOCKET_STORAGE_ARGUMENT

        // Get and check socket host
        String host_ = socket_settings.socket_host.value;
        if (args_count >= 1)
        {
            const auto * ast = engine_args[0]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                host_ = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception(String("Socket host must be a string"), ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Get and check socket port
        UInt64 port_ = socket_settings.socket_port.value;
        if (args_count >= 2)
        {
            const auto * ast = engine_args[1]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                port_ = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception(String("Socket port must be a integer"), ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Get and check message format name
        String format_ = socket_settings.socket_format.value;
        if (args_count >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

            const auto * ast = engine_args[2]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                format_ = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Parse row delimiter (optional)
        char row_delimiter_ = socket_settings.socket_row_delimiter.value;
        if (args_count >= 4)
        {
            engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.local_context);

            const auto * ast = engine_args[3]->as<ASTLiteral>();
            String arg;
            if (ast && ast->value.getType() == Field::Types::String)
            {
                arg = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            if (arg.size() > 1)
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            else if (arg.size() == 0)
            {
                row_delimiter_ = '\0';
            }
            else
            {
                row_delimiter_ = arg[0];
            }
        }

        // Parse max block size
        UInt64 max_block_size_ = static_cast<UInt64>(socket_settings.socket_max_block_size.value);
        if (args_count >= 5)
        {
            const auto * ast = engine_args[4]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                max_block_size_ = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of max block size must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Parse skip_broken
        size_t skip_broken_ = static_cast<size_t>(socket_settings.socket_skip_broken_messages.value);
        if (args_count >= 6)
        {
            const auto * ast = engine_args[5]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                skip_broken_ = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of broken messages to skip must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Parse zmq_parallelism
        UInt64 zmq_parallelism_ = static_cast<UInt64>(socket_settings.socket_zmq_parallelism.value);
        if (args_count >= 7)
        {
            const auto * ast = engine_args[6]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                zmq_parallelism_ = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of zmq parallelism must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        // Parse zmq_parallelism
        UInt64 socket_timeout_ = static_cast<UInt64>(socket_settings.socket_timeout.value);
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                socket_timeout_ = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of socket timeout must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageSocket::create(
            args.table_name,
            args.database_name,
            args.context,
            args.columns,
            zmq_parallelism_,
            host_,
            port_,
            format_,
            row_delimiter_,
            max_block_size_,
            socket_timeout_,
            skip_broken_,
            true);
    });
}
}