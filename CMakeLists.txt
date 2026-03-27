cmake_minimum_required(VERSION 3.25.0)
project(
  flow
  VERSION 0.0.1
  LANGUAGES CXX)

# ------------------------------------------------------------------------------
# dependencies
# ------------------------------------------------------------------------------

find_package(swiss REQUIRED PATHS)

# ------------------------------------------------------------------------------
# general settings and options
# ------------------------------------------------------------------------------

option(BUILD_BENCHMARKS "Build the benchmarks tree." OFF)

add_library(flow INTERFACE)
add_library(flow::flow ALIAS flow)

# ------------------------------------------------------------------------------
# sources
# ------------------------------------------------------------------------------

set(HEADERS
    include/flow/locked_mpmc_queue.hpp
    include/flow/locked_std_deque.hpp
    include/flow/lockfree_mpmc_queue.hpp
    include/flow/lockfree_mpsc_queue.hpp
    include/flow/lockfree_spmc_queue.hpp
    include/flow/lockless_mpmc_queue.hpp
    include/flow/queue.hpp
    include/flow/unsafe_queue.hpp
    include/flow/spsc_queue.hpp)

# ------------------------------------------------------------------------------
# target
# ------------------------------------------------------------------------------

target_sources(
  flow INTERFACE FILE_SET HEADERS BASE_DIRS
                 "${CMAKE_CURRENT_SOURCE_DIR}/include" FILES ${HEADERS})

target_compile_features(flow INTERFACE cxx_std_23)

target_link_libraries(flow INTERFACE swiss::swiss)

# ------------------------------------------------------------------------------
# install
# ------------------------------------------------------------------------------

if(PROJECT_IS_TOP_LEVEL)
  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  configure_package_config_file(
    cmake/flow-config.cmake.in flow-config.cmake
    INSTALL_DESTINATION "${CMAKE_INSTALL_DATADIR}/cmake/flow")

  write_basic_package_version_file(flow-config-version.cmake
                                   COMPATIBILITY SameMajorVersion)

  install(
    TARGETS flow
    EXPORT flow_export
    FILE_SET HEADERS)

  install(
    EXPORT flow_export
    FILE "flow-targets.cmake"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/cmake/flow"
    NAMESPACE flow::)

  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/flow-config.cmake"
                "${CMAKE_CURRENT_BINARY_DIR}/flow-config-version.cmake"
          DESTINATION "${CMAKE_INSTALL_DATADIR}/cmake/flow")
endif()

# ------------------------------------------------------------------------------
# benchmarks
# ------------------------------------------------------------------------------

if(PROJECT_IS_TOP_LEVEL)
  if(BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
  endif()
endif()

# ------------------------------------------------------------------------------
# tests
# ------------------------------------------------------------------------------

if(PROJECT_IS_TOP_LEVEL)
  include(CTest)

  if(BUILD_TESTING)
    add_subdirectory(tests/unit)
  endif()
endif()
