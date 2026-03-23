cmake_minimum_required(VERSION 3.25.0)
project(flow-benchmarks LANGUAGES CXX)

# ------------------------------------------------------------------------------
# dependencies
# ------------------------------------------------------------------------------

find_package(benchmark REQUIRED)

if(PROJECT_IS_TOP_LEVEL)
  find_package(flow REQUIRED)
endif()

include(CTest)

# ------------------------------------------------------------------------------
# general settings and options
# ------------------------------------------------------------------------------

add_executable(flow-benchmarks)

# ------------------------------------------------------------------------------
# sources
# ------------------------------------------------------------------------------

set(SOURCES
    locked_mpmc_queue_bench.cpp
    locked_std_deque_bench.cpp
    lockfree_mpmc_queue_bench.cpp
    lockfree_mpsc_queue_bench.cpp
    lockfree_spmc_queue_bench.cpp
    lockless_mpmc_queue_bench.cpp
    single_threaded_queue_bench.cpp
    waitfree_spsc_queue_bench.cpp
    templates.hpp
    main.cpp)

# ------------------------------------------------------------------------------
# target
# ------------------------------------------------------------------------------

target_sources(flow-benchmarks PRIVATE ${SOURCES})

target_link_libraries(flow-benchmarks PRIVATE flow::flow benchmark::benchmark)

# ------------------------------------------------------------------------------
# workflow automation
# ------------------------------------------------------------------------------

add_test(NAME flow-benchmarks COMMAND flow-benchmarks --benchmark_color=true)
set_tests_properties(flow-benchmarks PROPERTIES LABELS benchmark)
