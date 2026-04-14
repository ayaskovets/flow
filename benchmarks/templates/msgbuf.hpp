#pragma once

#include <flow/msgbuf.hpp>

#include <cstddef>
#include <optional>
#include <thread>

#include <benchmark/benchmark.h>

namespace benchmarks::templates {

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking_throughput(benchmark::State & state) noexcept {
  auto const msgbuf_capacity = static_cast<std::size_t>(state.range(0));
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
    msgbuf.emplace(msgbuf_capacity);
  }

  std::int64_t bytes      = 0;
  std::int64_t successful = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      if (msgbuf->try_push(msg)) {
        bytes += msg_size;
        successful += 1;
      } else {
        std::this_thread::yield();
      }
    }

    state.counters["push%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(producers));
  } else {
    std::vector<std::byte> msg(static_cast<std::size_t>(msg_size));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      if (msgbuf->try_pop(msg)) {
        bytes += msg_size;
        successful += 1;
      } else {
        std::this_thread::yield();
      }
    }

    state.counters["pop%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(consumers));
    state.SetBytesProcessed(bytes);
  }
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_throughput(benchmark::State & state) noexcept {
  auto const msgbuf_capacity = static_cast<std::size_t>(state.range(0));
  auto const producers       = state.range(1);
  auto const consumers       = state.range(2);
  auto const buffer_capacity = static_cast<std::size_t>(state.range(3));

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
    msgbuf.emplace(msgbuf_capacity);
  }

  std::int64_t bytes = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const buffer(buffer_capacity);
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      msgbuf->push(buffer);
      bytes += static_cast<std::int64_t>(buffer_capacity);
    }
  } else {
    std::vector<std::byte> buffer(buffer_capacity);
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      msgbuf->pop(buffer);
      bytes += static_cast<std::int64_t>(buffer_capacity);
    }

    state.SetBytesProcessed(bytes);
  }
}

}  // namespace benchmarks::templates
