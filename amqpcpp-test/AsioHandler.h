#pragma once

#define NOMINMAX

#include <boost/asio.hpp>
#include <amqpcpp.h>

#include <deque>
#include <vector>
#include <memory>

namespace RabbitMqStreamingPlugin
{
    namespace AsioHandlerPrivate
    {
        class AmqpBuffer;
    }

    // AsioHandler implementation based on
    // https//:github.com/fantastory/AMQP-CPP/blob/master/examples/rabbitmq_tutorials
    class AsioHandler : public AMQP::ConnectionHandler
    {
    public:

        AsioHandler(std::recursive_mutex& connection_mutex, boost::asio::io_service& io_service, const std::string& host, uint16_t port);
        ~AsioHandler() override;

        AsioHandler(const AsioHandler&) = delete;
        AsioHandler& operator=(const AsioHandler&) = delete;

    private:
        void doConnect(const std::string& host, uint16_t port);

        void onData(AMQP::Connection* connection, const char* data, size_t size) override;
        void onError(AMQP::Connection* connection, const char* message) override;
        void onClosed(AMQP::Connection* connection) override;

        void onNetworkError(boost::system::error_code error, const std::string& source);

        void doWrite();
        void doRead();
        void parseData();

        static constexpr size_t asio_input_buffer_size__ = 4 * 1024; //4kb

        boost::asio::io_service& io_service_;
        boost::asio::ip::tcp::socket socket_;
        boost::asio::deadline_timer timer_;

        std::vector<char> input_buffer_;
        std::shared_ptr<AsioHandlerPrivate::AmqpBuffer> amqp_buffer_;
        std::recursive_mutex& connection_mutex_;
        AMQP::Connection* connection_;
        std::deque<std::vector<char>> output_buffer_;
        bool is_writing_;
        bool is_connected_;
        bool should_quit_;
    };
}