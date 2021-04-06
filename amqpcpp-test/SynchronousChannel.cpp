#include "SynchronousChannel.h"

#include <iostream>

namespace RabbitMqStreamingPlugin
{
    SynchronousChannel::SynchronousChannel(AMQP::Connection& connection, std::recursive_mutex& connection_mutex)
        : connection_mutex_(connection_mutex)
        , operation_finished_(false)
        , is_in_error_state_(false)
        , channel_(&connection)
        , reliable_(channel_)
    {
    }

    SynchronousChannel::~SynchronousChannel()
    {
        stop();
    }

    void SynchronousChannel::stop()
    {
        onError("Operation aborted.");
    }

    void SynchronousChannel::publish(
        const std::string& topic,
        const std::string& partition_key,
        const AMQP::Envelope& message)
    {
        std::cout << "SynchronousChannel::publish begin\n";

        std::unique_lock publish_lock(publish_mutex_);
        std::unique_lock connection_lock(connection_mutex_);
        operation_finished_ = false;

        if (!is_in_error_state_)
        {
            std::cout << "SynchronousChannel::publish locked\n";

            reliable_.publish(topic, partition_key, message)
                .onAck([this]()
            {
                std::cout << "SynchronousChannel::publish onAck\n";
                onSuccess();
            })
                .onLost([this]()
            {
                std::cout << "SynchronousChannel::publish onLost\n";
                onError("Message failed to publish!");
            });

            //channel_.confirmSelect()
            //    .onSuccess([this, &topic, &partition_key, &message]()
            //{
            //    std::cout << "SynchronousChannel::publish onSuccess\n";
            //    channel_.publish(topic, partition_key, message);
            //})
            //    .onAck([this](uint64_t /*delivery_tag*/, bool /*multiple*/)
            //{
            //    std::cout << "SynchronousChannel::publish onAck\n";
            //    onSuccess();
            //})
            //    .onNack([this](uint64_t /*delivery_tag*/, bool /*multiple*/, bool /*requeue*/)
            //{
            //    std::cout << "SynchronousChannel::publish onNAck\n";
            //    onError("Message failed to publish!");
            //});
        }

        waitForOperationToFinish(std::move(connection_lock));
    }

    void SynchronousChannel::waitForOperationToFinish(std::unique_lock<std::recursive_mutex> lock)
    {
        std::cout << "SynchronousChannel::waitForOperationToFinish begin\n";
        operation_finished_cv_.wait(lock, [this]()
        {
            return operation_finished_ || is_in_error_state_;
        });
        std::cout << "SynchronousChannel::waitForOperationToFinish finished\n";
        if (is_in_error_state_)
        {
            std::string message("SynchronousChannel error: ");
            message.append(error_message_);
            std::cout << "SynchronousChannel::waitForOperationToFinish error: " << message << "\n";
            throw std::runtime_error(message);
        }
    }

    void SynchronousChannel::onSuccess()
    {
        std::unique_lock lock(connection_mutex_);
        operation_finished_ = true;
        operation_finished_cv_.notify_all();
    }

    void SynchronousChannel::onError(const std::string& message)
    {
        std::unique_lock lock(connection_mutex_);
        operation_finished_ = true;
        is_in_error_state_ = true;
        error_message_ = message;
        operation_finished_cv_.notify_all();
    }
}  // namespace RabbitMqStreamingPlugin
