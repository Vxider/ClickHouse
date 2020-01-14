//
// Created by 李冰 on 2019-07-16.
//

#include "SocketBlockInputStream.h"
#include <Formats/FormatFactory.h>

namespace DB
{
SocketBlockInputStream::SocketBlockInputStream(StorageSocket & storage_, const Context & context_, size_t max_block_size_)
    : storage(storage_), context(context_), max_block_size(max_block_size_)
{
    context.setSetting("input_format_skip_unknown_fields",
                       1u); // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skip_broken);

    // if (!schema.empty())
    //     context.setSetting("format_schema", schema);
}

SocketBlockInputStream::~SocketBlockInputStream()
{
    if (!claimed)
        return;
    //TODO: complete this
}

void SocketBlockInputStream::readPrefixImpl()
{
    buffer = storage.tryClaimBuffer(context.getSettingsRef().queue_max_wait_ms.totalMilliseconds());
    claimed = !!buffer;

    if (!buffer)
        buffer = storage.createBuffer();

    buffer->subBufferAs<ReadBufferFromSocketConsumer>();//->subscribe(storage.topics);
    const auto & limits_ = getLimits();

    auto child = FormatFactory::instance().getInput(storage.format_name, *buffer, storage.getSampleBlock(), context, max_block_size);
    child->setLimits(limits_);

    addChild(child);

    broken = true;
}

void SocketBlockInputStream::readSuffixImpl()
{
    // buffer->subBufferAs<ReadBufferFromSocketConsumer>()->commit();
    broken = false;
}

}
