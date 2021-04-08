#include "SynchronousChannel.h"

#include <iostream>

namespace RabbitMqStreamingPlugin
{
    namespace SynchronousChannelPrivate
    {
        void setEnvelopeAsProtobuf(const std::string& event_type_name, AMQP::Envelope& envelope)
        {
            envelope.setContentType("application/protobuf");
            envelope.setPersistent(true);

            AMQP::Table header_table;
            header_table.set("proto", event_type_name.c_str());
            envelope.setHeaders(header_table);
        }
    }  // namespace SynchronousChannelPrivate

    SynchronousChannel::SynchronousChannel(boost::asio::io_service& io_service, AMQP::Connection& connection)
        : io_service_(io_service)
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
        const std::string& event_type_name,
        const std::string& message)
    {
        std::cout << std::this_thread::get_id() << ": SynchronousChannel::publish begin\n";

        std::unique_lock publish_lock(publish_mutex_);
        std::unique_lock operation_lock(operation_mutex_);
        operation_finished_ = false;

        if (!is_in_error_state_)
        {
            std::cout << std::this_thread::get_id() << ": SynchronousChannel::publish locked\n";

            io_service_.post([this, topic, partition_key, event_type_name, message]()
            {
                AMQP::Envelope envelope(message.data(), message.size());
                SynchronousChannelPrivate::setEnvelopeAsProtobuf(event_type_name, envelope);

                reliable_.publish(topic, partition_key, envelope)
                    .onAck([this]()
                {
                    std::cout << std::this_thread::get_id() << ": SynchronousChannel::publish onAck\n";
                    onSuccess();
                })
                    .onLost([this]()
                {
                    std::cout << std::this_thread::get_id() << ": SynchronousChannel::publish onLost\n";
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
            
            });
        }

        waitForOperationToFinish(std::move(operation_lock));
    }

    void SynchronousChannel::waitForOperationToFinish(std::unique_lock<std::recursive_mutex> lock)
    {
        std::cout << std::this_thread::get_id() << ": SynchronousChannel::waitForOperationToFinish begin\n";
        operation_finished_cv_.wait(lock, [this]()
        {
            return operation_finished_ || is_in_error_state_;
        });
        std::cout << std::this_thread::get_id() << ": SynchronousChannel::waitForOperationToFinish finished\n";
        if (is_in_error_state_)
        {
            std::string message("SynchronousChannel error: ");
            message.append(error_message_);
            std::cout << std::this_thread::get_id() << ": SynchronousChannel::waitForOperationToFinish error: " << message << "\n";
            throw std::runtime_error(message);
        }
    }

    void SynchronousChannel::onSuccess()
    {
        std::unique_lock lock(operation_mutex_);
        operation_finished_ = true;
        operation_finished_cv_.notify_all();
    }

    void SynchronousChannel::onError(const std::string& message)
    {
        std::unique_lock lock(operation_mutex_);
        operation_finished_ = true;
        is_in_error_state_ = true;
        error_message_ = message;
        operation_finished_cv_.notify_all();
    }
}  // namespace RabbitMqStreamingPlugin
