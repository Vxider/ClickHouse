//
// Created by 李冰 on 2019-07-16.
//

#include <Storages/Socket/ReadBufferFromSocketConsumer.h>

namespace DB
{
ReadBufferFromSocketConsumer::ReadBufferFromSocketConsumer(ReceiverPtr receiver_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_,bool intermediate_commit_)
    : ReadBuffer(nullptr, 0)
    , receiver(receiver_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_commit(intermediate_commit_)
{
}

ReadBufferFromSocketConsumer::~ReadBufferFromSocketConsumer()
{
    receiver->setsockopt(ZMQ_LINGER, 0);
    receiver->close();
}

bool ReadBufferFromSocketConsumer::nextImpl()
{
    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.
    if (stalled)
        return false;

    //TODO: add max_batch_size and poll_timeout
    //TODO: uncomment this
    if (!receiver->recv(message))
    {
        stalled = true;
        return false;
    }

    // XXX: very fishy place with const casting.
    // auto new_position = reinterpret_cast<char *>(static_cast<char *>(message.data()));
    // auto new_position = reinterpret_cast<char *>(static_cast<char *>(message.data()));
    BufferBase::set(static_cast<char*>(message.data()), message.size(), 0);
    return true;
}

}