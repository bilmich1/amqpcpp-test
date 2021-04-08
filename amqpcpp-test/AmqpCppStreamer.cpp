#include "AmqpCppStreamer.h"

#include "AsioHandler.h"
#include "SynchronousChannel.h"

#define NOMINMAX

#include <amqpcpp.h>
#include <boost/asio/io_service.hpp>

#include <iostream>

namespace RabbitMqStreamingPlugin
{
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
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::publish begin\n";

            channel_->publish(topic, partition_key, event_type_name, message);

            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::publish success\n";
        }
        catch (const std::exception& e)
        {
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::publish error: " << e.what() << "\n";
            throw;
        }
    }

    bool AmqpCppStreamer::connect()
    {
        try
        {
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::connect begin\n";

            stop();

            io_service_ = std::make_unique<boost::asio::io_service>();

            connection_handler_ =
                std::make_unique<AsioHandler>(*io_service_, server_config_.ip_address_, server_config_.port_);

            connection_ = std::make_unique<AMQP::Connection>(
                connection_handler_.get(),
                AMQP::Login(server_config_.username_, server_config_.password_),
                server_config_.vhost_);

            channel_ = std::make_unique<SynchronousChannel>(*io_service_, *connection_);

            io_service_thread_ = std::thread([this]()
            {
                runConnectionService();
            });

            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::connect success\n";
            return true;
        }
        catch (const std::exception& e)
        {
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::connect error: " << e.what() << "\n";
            return false;
        }
    }

    void AmqpCppStreamer::runConnectionService()
    {
        try
        {
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::runConnectionService begin\n";
            io_service_->run();
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::runConnectionService success\n";
        }
        catch (const std::exception& e)
        {
            std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::runConnectionService error: " << e.what() << "\n";
            error_callback_(std::current_exception());
        }
        channel_->stop();
    }

    void AmqpCppStreamer::stop()
    {
        std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::stop begin\n";
        if (io_service_thread_.joinable())
        {
            io_service_->stop();
            io_service_thread_.join();
        }
        std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::stop joined\n";

        channel_.reset();
        connection_.reset();
        // destruction order is important between the handler and the service
        // because some handler's internal objects depends on the service being in a valid state during destruction
        connection_handler_.reset();
        io_service_.reset();
        std::cout << std::this_thread::get_id() << ": AmqpCppStreamer::stop reset\n";
    }

}  // namespace RabbitMqStreamingPlugin
