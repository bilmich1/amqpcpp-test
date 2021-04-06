#include "AmqpCppStreamer.h"

#include "AsioHandler.h"
#include "SynchronousChannel.h"

#define NOMINMAX

#include <amqpcpp.h>
#include <boost/asio/io_service.hpp>

#include <iostream>

namespace RabbitMqStreamingPlugin
{
    namespace AmqpCppStreamerPrivate
    {
        void setEnvelopeAsProtobuf(const std::string& event_type_name, AMQP::Envelope& envelope)
        {
            envelope.setContentType("application/protobuf");
            envelope.setPersistent(true);

            AMQP::Table header_table;
            header_table.set("proto", event_type_name.c_str());
            envelope.setHeaders(header_table);
        }
    }  // namespace AmqpCppStreamerPrivate

    AmqpCppStreamer::AmqpCppStreamer(
        const RabbitMqServerConfig& server_config,
        const OnErrorCallback error_callback)
        : server_config_(server_config)
        , error_callback_(error_callback)
    {
    }

    AmqpCppStreamer::~AmqpCppStreamer()
    {
        stop();
    }

    void AmqpCppStreamer::publish(
        const std::string& topic,
        const std::string& partition_key,
        const std::string& event_type_name,
        const std::string& message)
    {
        try
        {
            std::cout << "AmqpCppStreamer::publish begin\n";

            AMQP::Envelope envelope(message.data(), message.size());
            AmqpCppStreamerPrivate::setEnvelopeAsProtobuf(event_type_name, envelope);

            channel_->publish(
                topic,
                partition_key,
                envelope);

            std::cout << "AmqpCppStreamer::publish success\n";
        }
        catch (const std::exception& e)
        {
            std::cout << "AmqpCppStreamer::publish error: " << e.what() << "\n";
            throw;
        }
    }

    bool AmqpCppStreamer::connect()
    {
        try
        {
            std::cout << "AmqpCppStreamer::connect begin\n";

            stop();

            io_service_ = std::make_unique<boost::asio::io_service>();

            connection_handler_ =
                std::make_unique<AsioHandler>(connection_mutex_, *io_service_, server_config_.ip_address_, server_config_.port_);

            connection_ = std::make_unique<AMQP::Connection>(
                connection_handler_.get(),
                AMQP::Login(server_config_.username_, server_config_.password_),
                server_config_.vhost_);

            channel_ = std::make_unique<SynchronousChannel>(*connection_, connection_mutex_);

            io_service_thread_ = std::thread([this]()
            {
                runConnectionService();
            });

            std::cout << "AmqpCppStreamer::connect success\n";
            return true;
        }
        catch (const std::exception& e)
        {
            std::cout << "AmqpCppStreamer::connect error: " << e.what() << "\n";
            return false;
        }
    }

    void AmqpCppStreamer::runConnectionService()
    {
        try
        {
            std::cout << "AmqpCppStreamer::runConnectionService begin\n";
            io_service_->run();
            std::cout << "AmqpCppStreamer::runConnectionService success\n";
        }
        catch (const std::exception& e)
        {
            std::cout << "AmqpCppStreamer::runConnectionService error: " << e.what() << "\n";
            error_callback_(std::current_exception());
        }
        channel_->stop();
    }

    void AmqpCppStreamer::stop()
    {
        std::cout << "AmqpCppStreamer::stop begin\n";
        if (io_service_thread_.joinable())
        {
            io_service_->stop();
            io_service_thread_.join();
        }
        std::cout << "AmqpCppStreamer::stop joined\n";

        channel_.reset();
        connection_.reset();
        // destruction order is important between the handler and the service
        // because some handler's internal objects depends on the service being in a valid state during destruction
        connection_handler_.reset();
        io_service_.reset();
        std::cout << "AmqpCppStreamer::stop reset\n";
    }

}  // namespace RabbitMqStreamingPlugin
