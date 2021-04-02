# CMakeList.txt : CMake project for amqpcpp-test, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

# Add source to this project's executable.
add_executable (amqpcpp-test "main.cpp" "AmqpCppStreamer.cpp" "AsioHandler.cpp" "SynchronousChannel.cpp")

target_link_libraries(amqpcpp-test PRIVATE amqpcpp)
target_link_libraries(amqpcpp-test PRIVATE Boost::boost)

# TODO: Add tests and install targets if needed.
