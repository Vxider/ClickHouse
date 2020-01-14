#include <Common/Exception.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/IterativeMemory/StorageIterativeMemoryGenTime.h>
#include <Storages/StorageFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}


class IterativeMemoryGenTimeBlockInputStream : public IBlockInputStream
{
public:
    IterativeMemoryGenTimeBlockInputStream(
        const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_, const StorageIterativeMemoryGenTime & storage_)
        : column_names(column_names_), begin(begin_), end(end_), it(begin), storage(storage_)
    {
    }

    String getName() const override { return "IterativeMemoryGenTime"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        if (it == end)
        {
            // return Block();
            it = begin;
        }
        // else
        {
            Block src = *it;
            Block res;

            /// Add only required columns to `res`.
            for (size_t i = 0, size = column_names.size(); i < size; ++i)
                res.insert(src.getByName(column_names[i]));

            UInt64 timestamp_usec = static_cast<UInt64>(timestamp.epochMicroseconds());

            size_t row_size = res.rows();
            auto column_event_time = ColumnUInt64::create(row_size);
            auto & c_data = column_event_time->getData();
            for (size_t i = 0; i < row_size; ++i)
            {
                c_data[i] = timestamp_usec;
            }
            res.getByName("event_time").column = std::move(column_event_time);

            ++it;
            return res;
        }
    }

private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
    Poco::Timestamp timestamp;
    const StorageIterativeMemoryGenTime & storage;
};


class IterativeMemoryGenTimeBlockOutputStream : public IBlockOutputStream
{
public:
    explicit IterativeMemoryGenTimeBlockOutputStream(StorageIterativeMemoryGenTime & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard lock(storage.mutex);
        storage.data.push_back(block);
    }

private:
    StorageIterativeMemoryGenTime & storage;
};


StorageIterativeMemoryGenTime::StorageIterativeMemoryGenTime(
    String database_name_, String table_name_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
    : database_name(std::move(database_name_)), table_name(std::move(table_name_))
{
    setColumns(std::move(columns_description_));
    setConstraints(std::move(constraints_));

    if (!hasColumn("event_time"))
    {
        throw Exception("Not found column event_time in column description.", ErrorCodes::ILLEGAL_COLUMN);
    }
}

BlockInputStreams StorageIterativeMemoryGenTime::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    check(column_names);

    std::lock_guard lock(mutex);

    size_t size = data.size();

    // if (num_streams > size)
    num_streams = size;

    BlockInputStreams res;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.push_back(std::make_shared<IterativeMemoryGenTimeBlockInputStream>(column_names, begin, end, *this));
    }

    return res;
}


BlockOutputStreamPtr StorageIterativeMemoryGenTime::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<IterativeMemoryGenTimeBlockOutputStream>(*this);
}


void StorageIterativeMemoryGenTime::drop(TableStructureWriteLockHolder &)
{
    std::lock_guard lock(mutex);
    data.clear();
}

void StorageIterativeMemoryGenTime::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    std::lock_guard lock(mutex);
    data.clear();
}

void registerStorageIterativeMemoryGenTime(StorageFactory & factory)
{
    factory.registerStorage("IterativeMemoryGenTime", [](const StorageFactory::Arguments & args) {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageIterativeMemoryGenTime::create(args.database_name, args.table_name, args.columns, args.constraints);
    });
}

}
