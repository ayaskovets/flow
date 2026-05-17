#pragma once

#include <flow/stream.hpp>

#include <cstddef>
#include <optional>

#include <benchmark/benchmark.h>

namespace benchmarks::templates {

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking_throughput(benchmark::State & state) {
  auto const stream_capacity = state.range(0);
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

  static std::optional<Stream> stream;
  if (state.thread_index() == 0) {
    stream.emplace(static_cast<std::size_t>(stream_capacity));
  }

  std::int64_t bytes      = 0;
  std::int64_t successful = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const buffer(buffer_capacity);
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      auto const pushed = stream->try_push(buffer);
      bytes += pushed;
      successful += pushed != 0;
    }

    state.counters["push%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(producers));
  } else {
    std::vector<std::byte> buffer(buffer_capacity);
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      auto const popped = stream->try_pop(buffer);
      bytes += popped;
      successful += popped != 0;
    }

    state.counters["pop%"] =
        benchmark::Counter(100.0 * static_cast<double>(successful) /
                           static_cast<double>(state.iterations()) /
                           static_cast<double>(consumers));
    state.SetBytesProcessed(bytes);
  }
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_throughput(benchmark::State & state) {
  auto const stream_capacity = state.range(0);
  auto const producers       = state.range(1);
  auto const consumers       = state.range(2);
  auto const buffer_capacity = state.range(3);

  if (producers + consumers != state.threads()) {
    state.SkipWithMessage("Invalid (producers + consumers) for thread count");
    return;
  }

  if (producers == 0 || consumers == 0) {
    state.SkipWithError("Benchmark requires (producer > 0 && consumers > 0)");
    return;
  }

  static std::optional<Stream> stream;
  if (state.thread_index() == 0) {
    stream.emplace(static_cast<std::size_t>(stream_capacity));
  }

  std::int64_t bytes = 0;
  if (state.thread_index() < producers) {
    std::vector<std::byte> const buffer(
        static_cast<std::size_t>(buffer_capacity));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      stream->push(buffer);
      bytes += buffer_capacity;
    }
  } else {
    std::vector<std::byte> buffer(static_cast<std::size_t>((buffer_capacity)));
    // NOLINTNEXTLINE(readability-identifier-length)
    for (auto const _ : state) {
      stream->pop(buffer);
      bytes += buffer_capacity;
    }

    state.SetBytesProcessed(bytes);
  }
}

}  // namespace benchmarks::templates