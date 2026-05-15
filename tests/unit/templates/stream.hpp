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
// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

template <flow::concepts::stream T>
constexpr void minimal_capacity(std::size_t capacity) {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

template <flow::concepts::blocking_stream Stream>
constexpr void size_capacity() {
  constexpr std::size_t kCapacity = 8;

  std::vector<std::byte> bytes(2);
  Stream stream(kCapacity);

  EXPECT_EQ(stream.size(), 0);
  EXPECT_EQ(stream.capacity(), kCapacity);

  stream.push(std::vector(4, std::byte(0)));
  EXPECT_EQ(stream.size(), 4);
  EXPECT_EQ(stream.capacity(), kCapacity);

  for (std::size_t i = 0; i < kCapacity * kCapacity * 2; ++i) {
    stream.push(std::vector(2, std::byte(0)));
    EXPECT_EQ(stream.size(), 6);
    EXPECT_EQ(stream.capacity(), kCapacity);

    stream.pop(bytes);
    EXPECT_EQ(stream.size(), 4);
    EXPECT_EQ(stream.capacity(), kCapacity);
  }

  stream.clear();
  EXPECT_EQ(stream.size(), 0);
  EXPECT_TRUE(stream.empty());
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking_smoke() {
  constexpr std::size_t kCapacity = 8;

  std::vector<std::byte> bytes(2);
  Stream stream(kCapacity);

  EXPECT_EQ(stream.try_pop(bytes), 0);

  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(0))), 2);
  EXPECT_EQ(stream.try_push(std::vector(5, std::byte(1))), 5);
  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(2))), 1);
  EXPECT_EQ(stream.try_push(std::vector(2, std::byte(3))), 0);

  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, std::vector(2, std::byte(0)));
  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));
  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));
  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, (std::vector{std::byte(1), std::byte(2)}));
  EXPECT_EQ(stream.try_pop(bytes), 0);
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_smoke() {
  constexpr std::size_t kCapacity = 8;

  std::vector<std::byte> bytes(2);
  Stream stream(kCapacity);

  stream.push(std::vector(2, std::byte(0)));
  stream.push(std::vector(5, std::byte(1)));
  stream.push(std::vector(1, std::byte(2)));

  stream.pop(bytes);
  EXPECT_EQ(bytes, std::vector(2, std::byte(0)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, std::vector(2, std::byte(1)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::vector{std::byte(1), std::byte(2)}));
}

template <typename Stream>
  requires(flow::concepts::blocking_stream<Stream> &&
           flow::concepts::nonblocking_stream<Stream>)
constexpr void mixed_smoke() {
  constexpr std::size_t kCapacity = 8;

  std::array<std::byte, 2> bytes{};
  Stream stream(kCapacity);

  EXPECT_EQ(stream.size(), 0);
  EXPECT_EQ(stream.try_pop(bytes), 0);
  EXPECT_EQ(stream.size(), 0);

  stream.push(std::array{std::byte(0), std::byte(1)});
  EXPECT_EQ(stream.size(), 2);

  stream.push(std::array{std::byte(2), std::byte(3)});
  EXPECT_EQ(stream.size(), 4);
  EXPECT_EQ(stream.try_push(std::array{std::byte(4), std::byte(0)}), 2);
  EXPECT_EQ(stream.size(), 6);

  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(0), std::byte(1)}));
  EXPECT_EQ(stream.size(), 4);

  EXPECT_EQ(stream.try_push(std::array{std::byte(5), std::byte(6)}), 2);
  EXPECT_EQ(stream.size(), 6);
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(2), std::byte(3)}));
  EXPECT_EQ(stream.size(), 4);

  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(4), std::byte(0)}));
  EXPECT_EQ(stream.size(), 2);

  EXPECT_EQ(stream.try_pop(bytes), 2);
  EXPECT_EQ(bytes, (std::array{std::byte(5), std::byte(6)}));

  EXPECT_EQ(stream.try_pop(bytes), 0);
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_push() {
  constexpr std::size_t kCapacity = 8;

  Stream stream(kCapacity);

  stream.push(std::vector(4, std::byte(0)));
  stream.push(std::vector(4, std::byte(1)));

  std::jthread const consumer([&] {
    std::array<std::byte, 6> bytes{};
    EXPECT_EQ(stream.try_pop(bytes), 6);
    EXPECT_EQ(bytes, (std::array{std::byte(0), std::byte(0), std::byte(0),
                                 std::byte(0), std::byte(1), std::byte(1)}));
  });

  std::array<std::byte, 4> bytes{};
  stream.push(std::vector(2, std::byte(2)));
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(1), std::byte(1), std::byte(2),
                               std::byte(2)}));
}

template <typename Stream>
  requires(flow::concepts::blocking_stream<Stream> &&
           flow::concepts::nonblocking_stream<Stream>)
constexpr void blocking_pop() {
  constexpr std::size_t kCapacity = 2;

  Stream stream(kCapacity);

  std::jthread const producer(
      [&] { EXPECT_EQ(stream.try_push(std::vector(2, std::byte(1))), 2); });

  std::array<std::byte, 2> bytes{};
  stream.pop(bytes);
  EXPECT_EQ(bytes, (std::array{std::byte(1), std::byte(1)}));
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void stop_token_noop() {
  constexpr std::size_t kCapacity = 2;

  std::array<std::byte, 1> bytes{};
  Stream stream(kCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::array<std::byte, 1>{}));
  EXPECT_TRUE(stream.push(std::stop_token{}, std::array<std::byte, 1>{}));

  std::stop_source stop_source;
  EXPECT_TRUE(stop_source.request_stop());

  EXPECT_FALSE(
      stream.push(stop_source.get_token(), std::array<std::byte, 1>{}));

  EXPECT_TRUE(stream.pop(std::stop_token{}, bytes));
  EXPECT_TRUE(stream.pop(std::stop_token{}, bytes));

  EXPECT_FALSE(stream.pop(stop_source.get_token(), bytes));
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void blocking_push_stop_token() {
  constexpr std::size_t kCapacity = 2;

  Stream stream(kCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::vector(2, std::byte(0))));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(stream.push(stop_token, std::vector(1, std::byte(0))));
  });
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void blocking_pop_stop_token() {
  constexpr std::size_t kCapacity = 2;

  std::array<std::byte, 2> bytes{};
  Stream stream(kCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::vector(2, std::byte())));
  EXPECT_TRUE(stream.pop(std::stop_token{}, bytes));

  std::jthread const producer([&](std::stop_token stop_token) {
    EXPECT_FALSE(stream.pop(stop_token, bytes));
  });
}

template <flow::concepts::blocking_stream Stream>
constexpr void allocator() {
  constexpr std::size_t kCapacity = 2;

  swiss::leak_detector_allocator<typename Stream::allocator_type::value_type,
                                 true>
      allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());
  {
    Stream const stream(kCapacity, allocator);
    EXPECT_FALSE(allocator.empty());
  }
  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void nonblocking(auto const & param) {
  auto const & [bytes_count, stream_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, stream_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, stream_capacity / 7);

  Stream stream(static_cast<typename Stream::index_type>(stream_capacity));
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
            ++pushed_byte_counts[static_cast<std::size_t>(bytes[i])];
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
            ++popped_byte_counts[static_cast<std::size_t>(bytes[i])];
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking(auto const & param) {
  auto const & [bytes_count, stream_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, stream_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, stream_capacity / 7);

  Stream stream(static_cast<typename Stream::index_type>(stream_capacity));
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
            ++pushed_byte_counts[static_cast<std::size_t>(bytes[i])];
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
            ++popped_byte_counts[static_cast<std::size_t>(bytes[i])];
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void rollover() {
  constexpr std::size_t kCapacity = 8;
  constexpr std::size_t kRepeats  = 3;

  std::array<std::byte, 3> bytes{};
  Stream stream(kCapacity);

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
constexpr void nonblocking_callback() {
  constexpr std::size_t kCapacity = 8;

  Stream stream(kCapacity);

  EXPECT_EQ(stream.try_push(
                [](std::span<std::byte> postfix,
                   std::span<std::byte> prefix) noexcept {
                  EXPECT_EQ(postfix.size(), 3);
                  EXPECT_EQ(prefix.size(), 0);
                },
                3),
            3);
  EXPECT_EQ(stream.size(), 3);

  EXPECT_EQ(stream.try_push(
                [](std::span<std::byte> postfix,
                   std::span<std::byte> prefix) noexcept {
                  EXPECT_EQ(postfix.size(), 2);
                  EXPECT_EQ(prefix.size(), 0);
                },
                2),
            2);
  EXPECT_EQ(stream.size(), 5);

  EXPECT_EQ(stream.try_pop(
                [](std::span<std::byte> postfix,
                   std::span<std::byte> prefix) noexcept {
                  EXPECT_EQ(postfix.size(), 3);
                  EXPECT_EQ(prefix.size(), 0);
                },
                3),
            3);
  EXPECT_EQ(stream.size(), 2);

  EXPECT_EQ(stream.try_push([](std::span<std::byte> postfix,
                               std::span<std::byte> prefix) noexcept {
    EXPECT_EQ(postfix.size(), 3);
    EXPECT_EQ(prefix.size(), 3);
    return 3;
  }),
            3);
  EXPECT_EQ(stream.size(), 5);
}

template <flow::concepts::blocking_stream Stream>
constexpr void blocking_callback() {
  constexpr std::size_t kCapacity = 8;

  Stream stream(kCapacity);

  stream.push(3, [](std::span<std::byte> postfix,
                    std::span<std::byte> prefix) noexcept {
    EXPECT_EQ(postfix.size(), 3);
    EXPECT_EQ(prefix.size(), 0);
  });

  EXPECT_EQ(stream.size(), 3);

  stream.push(2, [](std::span<std::byte> postfix,
                    std::span<std::byte> prefix) noexcept {
    EXPECT_EQ(postfix.size(), 2);
    EXPECT_EQ(prefix.size(), 0);
  });
  EXPECT_EQ(stream.size(), 5);

  stream.pop(3, [](std::span<std::byte> postfix,
                   std::span<std::byte> prefix) noexcept {
    EXPECT_EQ(postfix.size(), 3);
    EXPECT_EQ(prefix.size(), 0);
  });
  EXPECT_EQ(stream.size(), 2);

  stream.push(3, [](std::span<std::byte> postfix,
                    std::span<std::byte> prefix) noexcept {
    EXPECT_EQ(postfix.size(), 3);
    EXPECT_EQ(prefix.size(), 0);
  });
  EXPECT_EQ(stream.size(), 5);
}

template <flow::concepts::nonblocking_stream Stream>
constexpr void exception_callback() {
  constexpr std::size_t kCapacity = 32;

  Stream stream(kCapacity);

  EXPECT_ANY_THROW(
      stream.try_push([](std::span<std::byte>, std::span<std::byte>) {
        throw std::runtime_error("");
      }));
  EXPECT_EQ(stream.size(), 0);
  EXPECT_EQ(stream.try_pop([](std::span<std::byte>,
                              std::span<std::byte>) noexcept { GTEST_FAIL(); }),
            0);

  EXPECT_TRUE(stream.try_push(std::vector(4, std::byte(0))));
  EXPECT_EQ(stream.size(), 4);
  EXPECT_EQ(stream.try_pop(
                [](std::span<std::byte>, std::span<std::byte>) noexcept {}),
            4);
  EXPECT_EQ(stream.size(), 0);

  EXPECT_TRUE(stream.try_push(std::vector(1, std::byte(0))));
  EXPECT_EQ(stream.size(), 1);

  EXPECT_EQ(stream.try_pop(
                [](std::span<std::byte>, std::span<std::byte>) noexcept {}),
            1);
  EXPECT_EQ(stream.size(), 0);
}

template <typename Stream>
constexpr void input_validation() {
  constexpr std::size_t kCapacity = 8;

  std::array<std::byte, 0> empty_bytes{};
  std::array<std::byte, kCapacity + 1> overflow_bytes{};
  Stream stream(kCapacity);

  if constexpr (flow::concepts::nonblocking_stream<Stream>) {
    EXPECT_EQ(stream.try_push(overflow_bytes), kCapacity);
    EXPECT_EQ(stream.try_pop(overflow_bytes), kCapacity);

    EXPECT_EQ(stream.try_push(empty_bytes), 0);
    EXPECT_EQ(stream.try_pop(empty_bytes), 0);
  }

  if constexpr (flow::concepts::blocking_stream<Stream>) {
    stream.push(overflow_bytes.size(),
                [](std::span<std::byte>, std::span<std::byte>) noexcept {
                  GTEST_FAIL();
                });
    stream.pop(overflow_bytes.size(),
               [](std::span<std::byte>, std::span<std::byte>) noexcept {
                 GTEST_FAIL();
               });

    stream.push(empty_bytes.size(),
                [](std::span<std::byte>, std::span<std::byte>) noexcept {
                  GTEST_FAIL();
                });
    stream.pop(empty_bytes.size(),
               [](std::span<std::byte>, std::span<std::byte>) noexcept {
                 GTEST_FAIL();
               });
  }

  if constexpr (flow::concepts::cancellable_blocking_stream<Stream>) {
    EXPECT_FALSE(stream.push(std::stop_token{}, overflow_bytes));
    EXPECT_FALSE(stream.pop(std::stop_token{}, overflow_bytes));
    EXPECT_FALSE(
        stream.push(std::stop_token{}, overflow_bytes.size(),
                    [](std::span<std::byte>, std::span<std::byte>) noexcept {
                      GTEST_FAIL();
                    }));
    EXPECT_FALSE(
        stream.pop(std::stop_token{}, overflow_bytes.size(),
                   [](std::span<std::byte>, std::span<std::byte>) noexcept {
                     GTEST_FAIL();
                   }));

    EXPECT_FALSE(stream.push(std::stop_token{}, empty_bytes));
    EXPECT_FALSE(stream.pop(std::stop_token{}, empty_bytes));
    EXPECT_FALSE(
        stream.push(std::stop_token{}, empty_bytes.size(),
                    [](std::span<std::byte>, std::span<std::byte>) noexcept {
                      GTEST_FAIL();
                    }));
    EXPECT_FALSE(
        stream.pop(std::stop_token{}, empty_bytes.size(),
                   [](std::span<std::byte>, std::span<std::byte>) noexcept {
                     GTEST_FAIL();
                   }));
  }
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}  // namespace tests::templates
