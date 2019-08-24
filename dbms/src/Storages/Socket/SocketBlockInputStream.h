#pragma once

//
// Created by 李冰 on 2019-07-16.
//

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>

#include <Storages/Socket/StorageSocket.h>

namespace DB
{
class SocketBlockInputStream : public IBlockInputStream
{
public:
    SocketBlockInputStream(StorageSocket & storage_, const Context & context_, size_t max_block_size_);
    ~SocketBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block readImpl() override { return children.back()->read(); }
    Block getHeader() const override { return storage.getSampleBlock(); }

protected:
    void readPrefixImpl() override;
    void readSuffixImpl() override;

private:
    StorageSocket & storage;
    Context context;
    UInt64 max_block_size;

    BufferPtr buffer;
    bool broken = true, claimed = false;
};
}