#pragma once

#include <flow/stream.hpp>

#include <algorithm>
#include <atomic>
#include <latch>
#include <limits>
#include <stop_token>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

namespace tests::templates {

template <flow::concepts::stream T>
constexpr void minimal_capacity(std::size_t capacity) noexcept {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

template <flow::concepts::blocking_stream Stream>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kStreamCapacity = 8;

  std::vector<std::byte> bytes(2);
  Stream stream(kStreamCapacity);

  EXPECT_EQ(stream.size(), 0);
  EXPECT_EQ(stream.capacity(), kStreamCapacity);

  stream.push(std::vector(4, std::byte(0)));
  EXPECT_EQ(stream.size(), 4);
  EXPECT_EQ(stream.capacity(), kStreamCapacity);

  for (std::size_t i = 0; i < kStreamCapacity * kStreamCapacity * 2; ++i) {
    stream.push(std::vector(2, std::byte(0)));
    EXPECT_EQ(stream.size(), 6);
    EXPECT_EQ(stream.capacity(), kStreamCapacity);

    stream.pop(bytes);
    EXPECT_EQ(stream.size(), 4);
    EXPECT_EQ(stream.capacity(), kStreamCapacity);
  }
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking_smoke() noexcept {
  constexpr std::size_t kStreamCapacity = 5;

  std::vector<std::byte> bytes(2);
  Stream stream(kStreamCapacity);

  EXPECT_EQ(stream.try_pop(bytes), 0);

  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(0))), 2);
  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(1))), 2);
  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(2))), 1);
  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(3))), 0);

  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, std::vector(2, std::byte(0)));
  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));

  EXPECT_EQ(stream.try_pop(bytes), 1);
  EXPECT_EQ(bytes, (std::vector{std::byte(2), std::byte{1}}));
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_smoke() noexcept {
  constexpr std::size_t kStreamCapacity = 5;

  std::vector<std::byte> bytes(2);
  Stream stream(kStreamCapacity);

  stream.push(std::vector(2, std::byte(0)));
  stream.push(std::vector(2, std::byte(1)));
  stream.push(std::vector(1, std::byte(2)));

  stream.pop(bytes);
  EXPECT_EQ(bytes, std::vector(2, std::byte(0)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));

  stream.pop(std::span<std::byte, 1>(bytes));
  EXPECT_EQ(bytes, (std::vector{std::byte(2), std::byte{1}}));
}

template <typename Stream>
  requires(flow::concepts::blocking_stream<Stream> &&
           flow::concepts::nonblocking_stream<Stream>)
constexpr void mixed_smoke() noexcept {
  constexpr std::size_t kStreamCapacity = 5;

  std::array<std::byte, 2> bytes{};
  Stream stream(kStreamCapacity);

  EXPECT_EQ(stream.size(), 0);
  EXPECT_EQ(stream.try_pop(bytes), 0);
  EXPECT_EQ(stream.size(), 0);

  stream.push(std::array{std::byte(0), std::byte(1)});
  EXPECT_EQ(stream.size(), 2);

  stream.push(std::array{std::byte(2), std::byte(3)});
  EXPECT_EQ(stream.size(), 4);
  EXPECT_EQ(stream.try_push(std::array{std::byte(4), std::byte(0)}), 1);
  EXPECT_EQ(stream.size(), 5);

  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(0), std::byte(1)}));
  EXPECT_EQ(stream.size(), 3);

  EXPECT_EQ(stream.try_push(std::array{std::byte(5), std::byte(6)}), 2);
  EXPECT_EQ(stream.size(), 5);
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(2), std::byte(3)}));
  EXPECT_EQ(stream.size(), 3);

  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(4), std::byte(5)}));
  EXPECT_EQ(stream.size(), 1);

  EXPECT_EQ(stream.try_pop(bytes), 1);
  EXPECT_EQ(bytes, (std::array{std::byte(6), std::byte(5)}));
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_push() noexcept {
  constexpr std::size_t kStreamCapacity = 6;

  std::array<std::byte, 4> bytes{};
  Stream stream(kStreamCapacity);

  stream.push(std::vector(3, std::byte(0)));
  stream.push(std::vector(3, std::byte(1)));

  std::jthread const consumer([&] {
    EXPECT_EQ(stream.try_pop(bytes), 4);
    EXPECT_EQ(bytes, (std::array{std::byte(0), std::byte(0), std::byte(0),
                                 std::byte(1)}));
  });

  stream.push(std::vector(2, std::byte(2)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(1), std::byte(1), std::byte(2),
                               std::byte(2)}));
}

template <typename Stream>
  requires(flow::concepts::blocking_stream<Stream> &&
           flow::concepts::nonblocking_stream<Stream>)
constexpr void blocking_pop() noexcept {
  constexpr std::size_t kStreamCapacity = 2;

  Stream stream(kStreamCapacity);

  std::jthread const producer(
      [&] { EXPECT_EQ(stream.try_push(std::vector(2, std::byte(1))), 2); });

  std::array<std::byte, 2> bytes{};
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(1), std::byte(1)}));
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void blocking_push_stop_token() noexcept {
  constexpr std::size_t kStreamCapacity = 2;

  Stream stream(kStreamCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::vector(2, std::byte(0))));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(stream.push(stop_token, std::vector(1, std::byte(0))));
  });
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void blocking_pop_stop_token() noexcept {
  constexpr std::size_t kStreamCapacity = 2;

  std::array<std::byte, 2> bytes{};
  Stream stream(kStreamCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::vector(2, std::byte())));
  EXPECT_TRUE(stream.pop(std::stop_token{}, bytes));

  std::jthread const producer([&](std::stop_token stop_token) {
    EXPECT_FALSE(stream.pop(stop_token, bytes));
  });
}

template <flow::concepts::blocking_stream Stream>
constexpr void allocator() noexcept {
  constexpr std::size_t kStreamCapacity = 2;

  swiss::leak_detector_allocator<typename Stream::allocator_type::value_type,
                                 true>
      allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());
  {
    Stream const stream(kStreamCapacity, allocator);
    EXPECT_FALSE(allocator.empty());
  }
  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking(auto const & param) noexcept {
  auto const & [bytes_count, stream_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, stream_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, stream_capacity / 7);

  Stream stream(stream_capacity);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));

  std::vector<std::atomic<std::size_t>> pushed_byte_counts(
      std::numeric_limits<std::uint8_t>::max());
  std::vector<std::atomic<std::size_t>> popped_byte_counts(
      std::numeric_limits<std::uint8_t>::max());
  std::atomic<std::size_t> push_index = 0;
  std::atomic<std::size_t> pop_index  = 0;

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> bytes(producer_bytes);
        while (true) {
          std::size_t const index = push_index.fetch_add(producer_bytes);
          if (index >= bytes_count) {
            return;
          }
          auto const size = std::min(producer_bytes, bytes_count - index);
          for (std::size_t i = 0; i < size; ++i) {
            bytes[i] =
                std::byte(index % std::numeric_limits<std::uint8_t>::max());
            pushed_byte_counts[static_cast<std::size_t>(bytes[i])] += 1;
          }
          for (std::size_t offset = 0; offset < size;
               offset += stream.try_push(
                   std::span<std::byte>(bytes.data(), size).subspan(offset))) {
          }
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> bytes(consumer_bytes);
        while (true) {
          std::size_t const index = pop_index.fetch_add(consumer_bytes);
          if (index >= bytes_count) {
            return;
          }
          auto const size = std::min(consumer_bytes, bytes_count - index);
          for (std::size_t offset = 0; offset < size;
               offset += stream.try_pop(
                   std::span<std::byte>(bytes.data(), size).subspan(offset))) {
          }
          for (std::size_t i = 0; i < size; ++i) {
            popped_byte_counts[static_cast<std::size_t>(bytes[i])] += 1;
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking(auto const & param) noexcept {
  auto const & [bytes_count, stream_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, stream_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, stream_capacity / 7);

  Stream stream(stream_capacity);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));

  std::vector<std::atomic<std::size_t>> pushed_byte_counts(
      std::numeric_limits<std::uint8_t>::max());
  std::vector<std::atomic<std::size_t>> popped_byte_counts(
      std::numeric_limits<std::uint8_t>::max());
  std::atomic<std::size_t> push_index = 0;
  std::atomic<std::size_t> pop_index  = 0;

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> bytes(producer_bytes);
        while (true) {
          std::size_t const index = push_index.fetch_add(producer_bytes);
          if (index >= bytes_count) {
            return;
          }
          auto const size = std::min(producer_bytes, bytes_count - index);
          for (std::size_t i = 0; i < size; ++i) {
            bytes[i] =
                std::byte(index % std::numeric_limits<std::uint8_t>::max());
            pushed_byte_counts[static_cast<std::size_t>(bytes[i])] += 1;
          }
          stream.push(std::span<std::byte>(bytes.data(), size));
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> bytes(consumer_bytes);
        while (true) {
          std::size_t const index = pop_index.fetch_add(consumer_bytes);
          if (index >= bytes_count) {
            return;
          }
          auto const size = std::min(consumer_bytes, bytes_count - index);
          stream.pop(std::span<std::byte>(bytes.data(), size));
          for (std::size_t i = 0; i < size; ++i) {
            popped_byte_counts[static_cast<std::size_t>(bytes[i])] += 1;
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void rollover() noexcept {
  constexpr std::size_t kStreamCapacity = 8;
  constexpr std::size_t kRepeats        = 3;

  std::array<std::byte, 3> bytes{};
  Stream stream(kStreamCapacity);

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
    for (std::size_t i = 0;
         i < std::numeric_limits<typename Stream::index_type>::max(); ++i) {
      EXPECT_EQ(
          stream.try_push(std::array{std::byte(0), std::byte(1), std::byte(2)}),
          3);
      EXPECT_EQ(stream.try_pop(bytes), 3);
      EXPECT_EQ(bytes, (std::array{std::byte(0), std::byte(1), std::byte(2)}));
    }
  }
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking_callback() noexcept {
  constexpr std::size_t kStreamCapacity = 8;

  Stream stream(kStreamCapacity);

  EXPECT_EQ(
      stream.try_push(
          [](std::span<std::byte> chunk) { EXPECT_EQ(chunk.size(), 3); }, 3),
      3);
  EXPECT_EQ(stream.size(), 3);

  EXPECT_EQ(
      stream.try_push(
          [](std::span<std::byte> chunk) { EXPECT_EQ(chunk.size(), 2); }, 2),
      2);
  EXPECT_EQ(stream.size(), 5);

  EXPECT_EQ(
      stream.try_pop(
          [](std::span<std::byte> chunk) { EXPECT_EQ(chunk.size(), 3); }, 3),
      3);
  EXPECT_EQ(stream.size(), 2);

  EXPECT_EQ(stream.try_push(
                [](std::span<std::byte> chunk) { EXPECT_EQ(chunk.size(), 3); }),
            3);
  EXPECT_EQ(stream.size(), 5);
}

}  // namespace tests::templates
