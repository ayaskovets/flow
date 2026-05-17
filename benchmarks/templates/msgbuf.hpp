#pragma once

#include <flow/msgbuf.hpp>

#include <cstddef>
#include <optional>

#include <benchmark/benchmark.h>

namespace benchmarks::templates {

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking_throughput(benchmark::State & state) {
  auto const msgbuf_capacity = state.range(0);
  auto const producers       = state.range(1);
  auto const consumers       = state.range(2);
  auto const msg_size        = state.range(3);

  if (producers + consumers != state.threads()) {
    state.SkipWithMessage("Invalid (producers + consumers) for thread count");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Msgbuf> msgbuf;
  if (state.thread_index() == 0) {
    msgbuf.emplace(static_cast<std::size_t>(msgbuf_capacity));
  }

  std::int64_t bytes      = 0;
  std::int64_t successful = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      bool const success = msgbuf->try_push(msg);
      bytes += success * msg_size;
      successful += success;
    }

    state.counters["push%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(producers));
  } else {
    std::vector<std::byte> msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      bool const success = msgbuf->try_pop(msg);
      bytes += success * msg_size;
      successful += success;
    }

    state.counters["pop%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(consumers));
    state.SetBytesProcessed(bytes);
  }
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_throughput(benchmark::State & state) {
  auto const msgbuf_capacity = state.range(0);
  auto const producers       = state.range(1);
  auto const consumers       = state.range(2);
  auto const msg_size        = state.range(3);

  if (producers + consumers != state.threads()) {
    state.SkipWithMessage("Invalid (producers + consumers) for thread count");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Msgbuf> msgbuf;
  if (state.thread_index() == 0) {
    msgbuf.emplace(static_cast<std::size_t>(msgbuf_capacity));
  }

  std::int64_t bytes = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      msgbuf->push(msg);
      bytes += msg_size;
    }
  } else {
    std::vector<std::byte> msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      std::ignore = msgbuf->pop(msg);
      bytes += msg_size;
    }

    state.SetBytesProcessed(bytes);
  }
}

}  // namespace benchmarks::templates
