# amqpcpp-test

Basic test project using the [AMQP-CPP lib](https://github.com/CopernicaMarketingSoftware/AMQP-CPP) on Windows.

The purpose of this project is to test a basic implementation of a synchronous streamer by publishing 100k messages.

# Classes Description

**AmqpCppStreamer**

Wrapper of the AMQP-CPP classes and the boost event loop. 

**SynchronousChannel**

Wrapper around an `AMQP::Channel` object that will block until the response is available before returning.

**AsioHandler**

The `AMQP::ConnectionHandle` derived class using `boost::asio`.

# How to build

This project is built using CMake and requires both the AMQP-CPP library and Boost ASIO to compile.