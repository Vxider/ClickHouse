#pragma once

//
// Created by 李冰 on 2019-07-16.
//

#include <IO/DelimitedReadBuffer.h>
#include <cppzmq/zmq.hpp>

namespace DB
{
using BufferPtr = std::shared_ptr<DelimitedReadBuffer>;
using ReceiverPtr = std::shared_ptr<zmq::socket_t>;

class ReadBufferFromSocketConsumer : public ReadBuffer
{
public:
    ReadBufferFromSocketConsumer(
        ReceiverPtr receiver_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_, bool intermediate_commit_);
    ~ReadBufferFromSocketConsumer() override;

private:
    //    using Messages = std::vector<zmq::message_t>;

    ReceiverPtr receiver;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    bool stalled = false;
    bool intermediate_commit = true;
    // std::string tmp_content = "{ \"tweet_id\":123123, \"author_id\": \"asdasdasd\" }\n";

    //    Messages messages;
    zmq::message_t message;
    //    Messages::const_iterator current;

    bool nextImpl() override;
};
}
