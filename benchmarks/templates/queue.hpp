#pragma once

#include <flow/queue.hpp>

#include <cstddef>
#include <optional>

#include <benchmark/benchmark.h>

namespace benchmarks::templates {

template <flow::concepts::nonblocking_queue Queue>
constexpr void nonblocking_throughput(benchmark::State & state) {
  auto const queue_capacity = state.range(0);
  auto const producers      = state.range(1);
  auto const consumers      = state.range(2);

  if (producers + consumers != state.threads()) {
    state.SkipWithMessage("Invalid (producers + consumers) for thread count");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Queue> queue;
  if (state.thread_index() == 0) {
    queue.emplace(static_cast<std::size_t>(queue_capacity));
  }

  std::int64_t items = 0;
  if (state.thread_index() < producers) {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      items += queue->try_emplace();
    }

    /**
     * @note Division by producers is necessary because percentages get summed
     * up for all threads
     */
    state.counters["push%"] =
        benchmark::Counter(100.0 * static_cast<double>(items) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(producers));
  } else {
    typename Queue::value_type value{};
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      items += queue->try_pop(value);
    }

    /**
     * @note Division by producers is necessary because percentages get summed
     * up for all threads
     */
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
constexpr void blocking_throughput(benchmark::State & state) {
  auto const queue_capacity = state.range(0);
  auto const producers      = state.range(1);
  auto const consumers      = state.range(2);

  if (producers + consumers != state.threads()) {
    state.SkipWithMessage("Invalid (producers + consumers) for thread count");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Queue> queue;
  if (state.thread_index() == 0) {
    queue.emplace(static_cast<std::size_t>(queue_capacity));
  }

  if (state.thread_index() < producers) {
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      queue->emplace();
    }
  } else {
    typename Queue::value_type value{};
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      queue->pop(value);
    }

    state.SetBytesProcessed(
        state.iterations() *
        static_cast<std::int64_t>(sizeof(typename Queue::value_type)));
    state.SetItemsProcessed(state.iterations());
  }
}

}  // namespace benchmarks::templates
