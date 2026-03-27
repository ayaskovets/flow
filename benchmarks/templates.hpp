#pragma once

#include <flow/queue.hpp>

#include <cstddef>
#include <optional>
#include <thread>

#include <benchmark/benchmark.h>

#define BENCHMARK_T(benchmark_suite_name, benchmark_name, type, ...)   \
  constexpr void BM##_##benchmark_suite_name##_##benchmark_name(       \
      benchmark::State & state) noexcept {                             \
    templates::benchmark_name<type>(state __VA_OPT__(, ) __VA_ARGS__); \
  }                                                                    \
  BENCHMARK(BM##_##benchmark_suite_name##_##benchmark_name)

#define BENCHMARK_TEMPLATE_T(benchmark_suite_name, benchmark_name,       \
                             benchmark_type, type, ...)                  \
  constexpr void                                                         \
      BM##_##benchmark_suite_name##_##benchmark_name##_##benchmark_type( \
          benchmark::State & state) noexcept {                           \
    templates::benchmark_name<type>(state __VA_OPT__(, ) __VA_ARGS__);   \
  }                                                                      \
  BENCHMARK(BM##_##benchmark_suite_name##_##benchmark_name##_##benchmark_type)

namespace benchmarks::templates {

template <flow::concepts::nonblocking_queue Queue>
constexpr void nonblocking_throughput(benchmark::State & state) noexcept {
  auto const queue_size = static_cast<std::size_t>(state.range(0));
  auto const producers  = state.range(1);
  auto const consumers  = state.range(2);

  if (producers + consumers != state.threads()) {
    state.SkipWithError(
        "Benchmark Threads() parameter must be = (producers + consumers)");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Queue> queue;
  if (state.thread_index() == 0) {
    queue.emplace(queue_size);
  }

  std::int64_t items = 0;
  if (state.thread_index() < producers) {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      if (queue->try_emplace()) {
        ++items;
      } else {
        std::this_thread::yield();
      }
    }

    state.counters["push%"] =
        benchmark::Counter(100.0 * static_cast<double>(items) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(producers));
  } else {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      typename Queue::value_type value{};
      if (queue->try_pop(value)) {
        benchmark::DoNotOptimize(value);
        ++items;
      } else {
        std::this_thread::yield();
      }
    }

    state.counters["pop%"] =
        benchmark::Counter(100.0 * static_cast<double>(items) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(consumers));
    state.SetBytesProcessed(
        items * static_cast<std::int64_t>(sizeof(typename Queue::value_type)));
    state.SetItemsProcessed(items);
  }
}

template <flow::concepts::blocking_queue Queue>
constexpr void blocking_throughput(benchmark::State & state) noexcept {
  auto const queue_size = static_cast<std::size_t>(state.range(0));
  auto const producers  = state.range(1);
  auto const consumers  = state.range(2);

  if (producers + consumers != state.threads()) {
    state.SkipWithError(
        "Benchmark Threads() parameter must be equal to (consumers + "
        "producers)");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Queue> queue;
  if (state.thread_index() == 0) {
    queue.emplace(queue_size);
  }

  if (state.thread_index() < producers) {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      queue->emplace();
    }
  } else {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      benchmark::DoNotOptimize(queue->pop());
    }

    state.SetBytesProcessed(
        state.iterations() *
        static_cast<std::int64_t>(sizeof(typename Queue::value_type)));
    state.SetItemsProcessed(state.iterations());
  }
}

}  // namespace benchmarks::templates
