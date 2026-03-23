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
    locked_mpmc_queue_test.cpp
    locked_std_deque_test.cpp
    lockfree_mpmc_queue_test.cpp
    lockfree_mpsc_queue_test.cpp
    lockfree_spmc_queue_test.cpp
    lockless_mpmc_queue_test.cpp
    queue_test.cpp
    single_threaded_queue_test.cpp
    waitfree_spsc_queue_test.cpp)

# ------------------------------------------------------------------------------
# target
# ------------------------------------------------------------------------------

target_sources(flow-unit-tests PRIVATE ${SOURCES})

target_link_libraries(flow-unit-tests PRIVATE flow::flow GTest::gtest_main)

gtest_discover_tests(flow-unit-tests)
