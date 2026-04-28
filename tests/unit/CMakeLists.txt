cmake_minimum_required(VERSION 3.25.0)
project(flow-unit-tests LANGUAGES CXX)

# ------------------------------------------------------------------------------
# dependencies
# ------------------------------------------------------------------------------

find_package(GTest REQUIRED)

if(PROJECT_IS_TOP_LEVEL)
  find_package(flow REQUIRED)
endif()

include(CTest)
include(GoogleTest)

# ------------------------------------------------------------------------------
# general settings and options
# ------------------------------------------------------------------------------

add_executable(flow-unit-tests)

# ------------------------------------------------------------------------------
# sources
# ------------------------------------------------------------------------------

set(SOURCES
    templates/msgbuf.hpp
    templates/queue.hpp
    templates/stream.hpp
    templates/templates.hpp
    locked_ring_buffer_test.cpp
    mpmc_queue_test.cpp
    mpsc_msgbuf_test.cpp
    mpsc_queue_test.cpp
    msgbuf_test.cpp
    queue_test.cpp
    spmc_queue_test.cpp
    spsc_msgbuf_test.cpp
    spsc_queue_test.cpp
    spsc_stream_test.cpp
    stream_test.cpp
    unsafe_msgbuf_test.cpp
    unsafe_queue_test.cpp
    unsafe_stream_test.cpp)

# ------------------------------------------------------------------------------
# target
# ------------------------------------------------------------------------------

target_sources(flow-unit-tests PRIVATE ${SOURCES})

target_link_libraries(flow-unit-tests PRIVATE flow::flow GTest::gtest_main)

gtest_discover_tests(flow-unit-tests)
