﻿
#include "AmqpCppStreamer.h"

#include <iostream>

RabbitMqStreamingPlugin::RabbitMqServerConfig getServerConfig()
{
    RabbitMqStreamingPlugin::RabbitMqServerConfig config{
    "127.0.0.1",
    5671,
    "/",
    "guest",
    "guest",
    };
    return config;
}

int main()
{
    const auto server_config = getServerConfig();
    const std::string topic = "my_topic";
    const std::string partition_key = "po=amqpcpp-test";
    const std::string event_type_name = "event_type_name";

    std::exception_ptr exception = nullptr;
    auto error_callback = [&exception](std::exception_ptr new_exception)
    {
        exception = new_exception;
    };

    RabbitMqStreamingPlugin::AmqpCppStreamer streamer(server_config, error_callback);
    streamer.connect();

    for (int i = 0; i < 100000; ++i)
    {
        if (nullptr != exception)
        {
            std::rethrow_exception(exception);
        }

        std::string message(500, 'a'); // 500 bytes message content
        streamer.publish(topic, partition_key, event_type_name, message);
    }

    std::cout << "Done\n";
    return 0;
}
