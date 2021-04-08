#include "asiohandler.h"

#include <iostream>

namespace RabbitMqStreamingPlugin
{
    namespace AsioHandlerPrivate
    {
        class AmqpBuffer
        {
        public:
            AmqpBuffer(size_t size) :
                data_(size, 0),
                use_(0)
            {
            }

            size_t write(const char* data, size_t size)
            {
                if (use_ == data_.size())
                {
                    return 0;
                }

                const size_t length = (size + use_);
                size_t write = length < data_.size() ? size : data_.size() - use_;
                memcpy(data_.data() + use_, data, write);
                use_ += write;
                return write;
            }

            void drain()
            {
                use_ = 0;
            }

            size_t available() const
            {
                return use_;
            }

            const char* data() const
            {
                return data_.data();
            }

            void shl(size_t count)
            {
                assert(count < use_);

                const size_t diff = use_ - count;
                std::memmove(data_.data(), data_.data() + count, diff);
                use_ = use_ - count;
            }

        private:
            std::vector<char> data_;
            size_t use_;
        };
    }

    AsioHandler::AsioHandler(boost::asio::io_service& io_service, const std::string& host, uint16_t port)
        : io_service_(io_service)
        , socket_(io_service)
        , timer_(io_service)
        , input_buffer_(asio_input_buffer_size__, 0)
        , amqp_buffer_(std::make_shared<AsioHandlerPrivate::AmqpBuffer>(asio_input_buffer_size__ * 2))
        , connection_(nullptr)
        , is_writing_(false)
        , is_connected_(false)
        , should_quit_(false)
    {
        doConnect(host, port);
    }

    AsioHandler::~AsioHandler()
    {
    }

    void AsioHandler::doConnect(const std::string& host, uint16_t port)
    {
        using boost::asio::ip::tcp;

        tcp::resolver::query query(host, std::to_string(port));
        tcp::resolver::iterator iter = tcp::resolver(io_service_).resolve(query);
        timer_.expires_from_now(boost::posix_time::milliseconds(500));
        timer_.async_wait([this](const boost::system::error_code& ec)
        {
            if (!ec && !is_connected_)
            {
                boost::system::error_code ignoredError;
                socket_.cancel(ignoredError);
                onNetworkError(ec, "Connection timed out");
            }
        });

        boost::asio::async_connect(socket_, iter,
            [this](boost::system::error_code ec, tcp::resolver::iterator)
        {
            if (!ec)
            {
                is_connected_ = true;
                doRead();

                if (!output_buffer_.empty())
                {
                    doWrite();
                }
            }
            else
            {
                onNetworkError(ec, "connect");
            }
        });

    }

    void AsioHandler::onData(
        AMQP::Connection* connection, const char* data, size_t size)
    {
        connection_ = connection;

        output_buffer_.push_back(std::vector<char>(data, data + size));
        if (!is_writing_ && is_connected_)
        {
            doWrite();
        }
    }

    void AsioHandler::doRead()
    {
        std::cout << std::this_thread::get_id() << ": AsioHandler::doRead begin\n";
        socket_.async_receive(boost::asio::buffer(input_buffer_),
            [this](boost::system::error_code ec, std::size_t length)
        {
            std::cout << std::this_thread::get_id() << ": AsioHandler::doRead async_receive\n";
            if (!ec)
            {
                amqp_buffer_->write(input_buffer_.data(), length);
                parseData();
                doRead();
            }
            else
            {
                onNetworkError(ec, "read");
            }
        });
    }

    void AsioHandler::doWrite()
    {
        std::cout << std::this_thread::get_id() << ": AsioHandler::doWrite begin\n";
        is_writing_ = true;
        boost::asio::async_write(socket_,
            boost::asio::buffer(output_buffer_.front()),
            [this](boost::system::error_code ec, std::size_t length)
        {
            std::cout << std::this_thread::get_id() << ": AsioHandler::doWrite async_write\n";
            if (!ec)
            {
                output_buffer_.pop_front();
                if (!output_buffer_.empty())
                {
                    doWrite();
                }
                else
                {
                    is_writing_ = false;
                }

                if (should_quit_)
                {
                    socket_.close();
                }
            }
            else
            {
                boost::system::error_code ignoredError;
                socket_.close(ignoredError);
                onNetworkError(ec, "write");
            }
        });
    }

    void AsioHandler::parseData()
    {
        if (connection_ == nullptr)
        {
            return;
        }

        const auto count = connection_->parse(amqp_buffer_->data(), amqp_buffer_->available());

        if (count == amqp_buffer_->available())
        {
            amqp_buffer_->drain();
        }
        else if (count > 0)
        {
            amqp_buffer_->shl(count);
        }
    }

    void AsioHandler::onError(AMQP::Connection* connection, const char* message)
    {
        std::cout << std::this_thread::get_id() << ": AsioHandler::onError: " << message << "\n";
        throw std::runtime_error(message);
    }

    void AsioHandler::onClosed(AMQP::Connection* connection)
    {
        std::cout << std::this_thread::get_id() << ": AsioHandler::onClosed\n";
        should_quit_ = true;
        if (!is_writing_)
        {
            socket_.close();
        }
    }

    void AsioHandler::onNetworkError(boost::system::error_code error_code, const std::string& source)
    {
        std::cout << std::this_thread::get_id() << ": AsioHandler::onNetworkError: " << error_code.message() << "(Source: " << source << ")\n";
        boost::asio::detail::throw_error(error_code);
    }
}