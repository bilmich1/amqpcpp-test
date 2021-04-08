#pragma once

#include <boost/asio/io_service.hpp>

#include <memory>
#include <string>
#include <thread>
#include <functional>
#include <mutex>

namespace AMQP
{
    class Connection;
    class ConnectionHandler;
}  // namespace AMQP

namespace RabbitMqStreamingPlugin
{
    class SynchronousChannel;

    struct RabbitMqServerConfig
    {
        std::string ip_address_;
        int port_;
        std::string vhost_;
        std::string username_;
        std::string password_;
    };

    using ErrorCallbackSignature = void(std::exception_ptr exception);
    using OnErrorCallback = std::function<ErrorCallbackSignature>;

    class AmqpCppStreamer
    {
    public:
        AmqpCppStreamer(
            const RabbitMqServerConfig& server_config,
            const OnErrorCallback error_callback);
        ~AmqpCppStreamer();

        void publish(
            const std::string& topic,
            const std::string& partition_key,
            const std::string& event_type_name,
            const std::string& message);

        bool connect();

    private:
        void runConnectionService();
        void stop();

        const RabbitMqServerConfig server_config_;
        const OnErrorCallback error_callback_;

        std::unique_ptr<AMQP::ConnectionHandler> connection_handler_;
        std::unique_ptr<AMQP::Connection> connection_;
        std::unique_ptr<SynchronousChannel> channel_;

        std::thread io_service_thread_;
        std::unique_ptr<boost::asio::io_service> io_service_;
    };

}  // namespace RabbitMqStreamingPlugin
