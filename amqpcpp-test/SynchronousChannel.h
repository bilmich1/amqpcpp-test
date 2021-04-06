#pragma once

#define NOMINMAX
#include <amqpcpp.h>

#include <condition_variable>
#include <mutex>

namespace RabbitMqStreamingPlugin
{
    /*
     * SynchronousChannel wraps an AMQP::Channel in a way to make the start and publish function block until a response
     * has been received. This means that if the underlying AMQP::Connection object isn't forwarding the messages for
     * any reasons, the functions will block indefinitely (e.g. the boost::asio::io_service of the
     * AsioHandler isn't running). In such case, stop can be used in another thread to short-circuit the
     * blocking call.
     *
     * From AMQP-CPP documentation
     * https://github.com/CopernicaMarketingSoftware/AMQP-CPP#channel-errors
     * It is important to realize that any error that occurs on a channel invalidates the entire channel,
     * including all subsequent instructions that were already sent over it.
     * This means that if you call multiple methods in a row, and the first method fails,
     * all subsequent methods will not be executed either.
     *
     * Considering that the SynchronousChannel role is to wrap the channel object object, if an error is to happen,
     * a new instance of SynchronousChannel needs to be created to reopen the connection.
     *
     */

    class SynchronousChannel
    {
    public:
        SynchronousChannel(AMQP::Connection& connection, std::recursive_mutex& connection_mutex);
        ~SynchronousChannel();

        void stop();

        void publish(const std::string& topic, const std::string& partition_key, const AMQP::Envelope& message);

    private:
        void waitForOperationToFinish(std::unique_lock<std::recursive_mutex> lock);
        void onSuccess();
        void onError(const std::string& message);

        std::mutex publish_mutex_;
        std::recursive_mutex& connection_mutex_;
        std::condition_variable_any operation_finished_cv_;
        bool operation_finished_;
        bool is_in_error_state_;
        std::string error_message_;

        // Order is important, as reliable_ is built using channel_
        AMQP::Channel channel_;
        AMQP::Reliable<> reliable_;
    };

}  // namespace RabbitMqStreamingPlugin
