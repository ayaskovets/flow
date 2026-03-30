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
    cas128_mpmc_queue_bench.cpp
    locked_deque_bench.cpp
    locked_ring_buffer_bench.cpp
    main.cpp
    mpmc_queue_bench.cpp
    mpsc_queue_bench.cpp
    spmc_queue_bench.cpp
    spsc_queue_bench.cpp
    templates.hpp)

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
