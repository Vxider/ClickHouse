#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>
#include <cppzmq/zmq.hpp>
#include <Poco/Event.h>
#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>
#include "ReadBufferFromSocketConsumer.h"

namespace DB
{
class StorageSocket : public ext::shared_ptr_helper<StorageSocket>, public IStorage
{
    friend class SocketBlockInputStream;
    friend class SocketBlockOutputStream;

public:
    std::string getName() const override { return "Socket"; }

    std::string getTableName() const override { return table_name; }

    std::string getDatabaseName() const override { return database_name; }

    void startup() override;

    void shutdown() override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void rename(const String & /* new_path_to_db */, const String & new_database_name, const String & new_table_name) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

    void updateDependencies() override;

    const auto & getFormatName() const { return format_name; }

    const auto & skipBroken() const { return skip_broken; }

protected:
    StorageSocket(
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
        bool intermediate_commit_);

private:
    // Configuration and state
    String table_name;
    String database_name;
    Context global_context;
    String host;
    UInt64 port;
    const String format_name;
    char row_delimiter; /// optional row delimiter for generating char delimited stream in order to make various input stream parsers happy.
    UInt64 max_block_size; /// maximum block size for insertion into this table
    UInt64 socket_timeout;

    size_t num_created_consumers = 0;
    bool has_consumer_connected = false;

    Poco::Logger * log;

    // Consumer list
    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<BufferPtr> buffers; 

    size_t skip_broken;

    bool intermediate_commit;

    // Stream thread
    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};

    BufferPtr createBuffer();
    BufferPtr claimBuffer();
    BufferPtr tryClaimBuffer(long wait_ms);
    void pushBuffer(BufferPtr buffer);

    // zmq
    zmq::context_t zmq_context;

    void streamThread();

    bool streamToViews();

    bool checkDependencies(const String & current_database_name, const String & current_table_name);
};
}