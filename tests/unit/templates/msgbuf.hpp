#pragma once

#include <flow/msgbuf.hpp>

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

template <flow::concepts::msgbuf T>
constexpr void minimal_capacity(std::size_t capacity) {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void size_capacity() {
  constexpr std::size_t kCapacity = 32;

  std::vector<std::byte> msg(4);
  Msgbuf msgbuf(kCapacity);

  EXPECT_EQ(msgbuf.size(), 0);
  EXPECT_EQ(msgbuf.capacity(), kCapacity);
  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept { GTEST_FAIL(); }),
            0);

  msgbuf.push(std::vector(8, std::byte(0)));
  EXPECT_EQ(msgbuf.size(), 16);
  EXPECT_EQ(msgbuf.capacity(), kCapacity);

  for (std::size_t i = 0; i < kCapacity * kCapacity * 2; ++i) {
    msgbuf.push(std::vector(8, std::byte(0)));
    EXPECT_EQ(msgbuf.size(), 32);
    EXPECT_EQ(msgbuf.capacity(), kCapacity);

    EXPECT_EQ(msgbuf.pop(msg), 8);
    EXPECT_EQ(msgbuf.size(), 16);
    EXPECT_EQ(msgbuf.capacity(), kCapacity);
  }
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking_smoke() {
  constexpr std::size_t kCapacity = 32;

  std::vector<std::byte> msg(4);
  Msgbuf msgbuf(kCapacity);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept { GTEST_FAIL(); }),
            0);

  EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(0))));
  EXPECT_TRUE(msgbuf.try_push(std::vector(4, std::byte(1))));
  EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(2))));

  EXPECT_FALSE(msgbuf.try_push(std::vector(1, std::byte(3))));

  EXPECT_EQ(msgbuf.try_pop(msg), 2);
  EXPECT_EQ(msg, std::vector(4, std::byte(0)));
  EXPECT_EQ(msgbuf.try_pop(msg), 4);
  EXPECT_EQ(msg, std::vector(4, std::byte(1)));
  EXPECT_EQ(msgbuf.try_pop(msg), 2);
  EXPECT_EQ(msg, (std::vector{std::byte(2), std::byte(2), std::byte(1),
                              std::byte(1)}));

  EXPECT_TRUE(msgbuf.try_push(std::vector(1, std::byte(4))));
  EXPECT_EQ(msgbuf.try_pop(std::span<std::byte>(msg.data(), 1)), 1);

  EXPECT_EQ(msg, (std::vector{std::byte(4), std::byte(2), std::byte(1),
                              std::byte(1)}));

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept { GTEST_FAIL(); }),
            0);
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_smoke() {
  constexpr std::size_t kCapacity = 32;

  std::vector<std::byte> msg(4);
  Msgbuf msgbuf(kCapacity);

  msgbuf.push(std::vector(2, std::byte(0)));
  msgbuf.push(std::vector(4, std::byte(1)));
  msgbuf.push(std::vector(2, std::byte(2)));

  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, std::vector(4, std::byte(0)));
  EXPECT_EQ(msgbuf.pop(msg), 4);
  EXPECT_EQ(msg, std::vector(4, std::byte(1)));
  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::vector{std::byte(2), std::byte(2), std::byte(1),
                              std::byte(1)}));

  msgbuf.push(std::vector(1, std::byte(4)));
  EXPECT_EQ(msgbuf.pop(std::span<std::byte>(msg.data(), 1)), 1);

  EXPECT_EQ(msg, (std::vector{std::byte(4), std::byte(2), std::byte(1),
                              std::byte(1)}));
}

template <typename Msgbuf>
  requires(flow::concepts::blocking_msgbuf<Msgbuf> &&
           flow::concepts::nonblocking_msgbuf<Msgbuf>)
constexpr void mixed_smoke() {
  constexpr std::size_t kCapacity = 32;

  std::array<std::byte, 3> msg{};
  Msgbuf msgbuf(kCapacity);

  EXPECT_EQ(msgbuf.size(), 0);
  EXPECT_EQ(msgbuf.try_pop(msg), 0);
  EXPECT_EQ(msgbuf.size(), 0);

  msgbuf.push(std::array{std::byte(0), std::byte(1)});
  EXPECT_EQ(msgbuf.size(), 10);

  msgbuf.push(std::array{std::byte(2), std::byte(3)});
  EXPECT_EQ(msgbuf.size(), 20);

  EXPECT_TRUE(msgbuf.try_push(std::array{std::byte(4), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 30);

  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(1), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 20);

  EXPECT_TRUE(msgbuf.try_push(std::array{std::byte(5), std::byte(6)}));
  EXPECT_EQ(msgbuf.size(), 32);

  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(2), std::byte(3), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 22);

  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(4), std::byte(0), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 12);

  EXPECT_EQ(msgbuf.try_pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(5), std::byte(6), std::byte(0)}));
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_push() {
  constexpr std::size_t kCapacity = 16;

  Msgbuf msgbuf(kCapacity);

  msgbuf.push(std::vector(8, std::byte(0)));

  std::jthread const consumer([&] {
    std::array<std::byte, 8> msg{};
    EXPECT_EQ(msgbuf.try_pop(msg), 8);
    EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(0), std::byte(0),
                               std::byte(0), std::byte(0), std::byte(0),
                               std::byte(0), std::byte(0)}));
  });

  std::array<std::byte, 3> msg{};
  msgbuf.push(std::vector(2, std::byte(1)));
  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(1), std::byte(1), std::byte(0)}));
}

template <typename Msgbuf>
  requires(flow::concepts::blocking_msgbuf<Msgbuf> &&
           flow::concepts::nonblocking_msgbuf<Msgbuf>)
constexpr void blocking_pop() {
  constexpr std::size_t kCapacity = 16;

  Msgbuf msgbuf(kCapacity);

  std::jthread const producer(
      [&] { EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(1)))); });

  std::array<std::byte, 3> msg{};
  EXPECT_EQ(msgbuf.pop(msg), 2);
  EXPECT_EQ(msg, (std::array{std::byte(1), std::byte(1), std::byte(0)}));
}

template <flow::concepts::cancellable_blocking_msgbuf Msgbuf>
constexpr void stop_token_noop() {
  constexpr std::size_t kCapacity = 32;

  std::array<std::byte, 1> msg{};
  Msgbuf msgbuf(kCapacity);

  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::array<std::byte, 1>{}));
  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::array<std::byte, 1>{}));

  std::stop_source stop_source;
  EXPECT_TRUE(stop_source.request_stop());

  EXPECT_FALSE(
      msgbuf.push(stop_source.get_token(), std::array<std::byte, 1>{}));

  EXPECT_TRUE(msgbuf.pop(std::stop_token{}, msg));
  EXPECT_TRUE(msgbuf.pop(std::stop_token{}, msg));

  EXPECT_FALSE(msgbuf.pop(stop_source.get_token(), msg));
}

template <flow::concepts::cancellable_blocking_msgbuf Msgbuf>
constexpr void blocking_push_stop_token() {
  constexpr std::size_t kCapacity = 16;

  Msgbuf msgbuf(kCapacity);

  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::vector(2, std::byte(0))));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(msgbuf.push(stop_token, std::vector(2, std::byte(0))));
  });
}

template <flow::concepts::cancellable_blocking_msgbuf Msgbuf>
constexpr void blocking_pop_stop_token() {
  constexpr std::size_t kCapacity = 16;

  std::array<std::byte, 2> msg{};
  Msgbuf msgbuf(kCapacity);

  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::vector(2, std::byte(0))));
  EXPECT_TRUE(msgbuf.pop(std::stop_token{}, msg));

  std::jthread const producer([&](std::stop_token stop_token) {
    EXPECT_FALSE(msgbuf.pop(stop_token, msg));
  });
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void allocator() {
  constexpr std::size_t kCapacity = 2;

  swiss::leak_detector_allocator<typename Msgbuf::allocator_type::value_type,
                                 true>
      allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());
  {
    Msgbuf const msgbuf(kCapacity, allocator);
    EXPECT_FALSE(allocator.empty());
  }
  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking(auto const & param) {
  auto const & [msg_count, msgbuf_capacity, producers, consumers] = param;

  std::size_t const msg_size = std::max<std::size_t>(1, msgbuf_capacity / 7);

  Msgbuf msgbuf(msgbuf_capacity);
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

        std::vector<std::byte> msg(msg_size);
        while (true) {
          std::size_t const index = push_index.fetch_add(msg_size);
          if (index >= msg_count) {
            return;
          }
          for (std::byte & byte : msg) {
            byte = std::byte(index % std::numeric_limits<std::uint8_t>::max());
            pushed_byte_counts[static_cast<std::size_t>(byte)] += 1;
          }
          while (!msgbuf.try_push(msg)) {
          }
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> msg(msg_size);
        while (true) {
          std::size_t const index = pop_index.fetch_add(msg_size);
          if (index >= msg_count) {
            return;
          }
          while (msgbuf.try_pop(msg) == 0) {
          }
          for (std::byte const byte : msg) {
            popped_byte_counts[static_cast<std::size_t>(byte)] += 1;
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking(auto const & param) {
  auto const & [msg_count, msgbuf_capacity, producers, consumers] = param;

  std::size_t const msg_size = std::max<std::size_t>(1, msgbuf_capacity / 7);

  Msgbuf msgbuf(msgbuf_capacity);
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

        std::vector<std::byte> msg(msg_size);
        while (true) {
          std::size_t const index = push_index.fetch_add(msg_size);
          if (index >= msg_count) {
            return;
          }
          for (std::byte & byte : msg) {
            byte = std::byte(index % std::numeric_limits<std::uint8_t>::max());
            pushed_byte_counts[static_cast<std::size_t>(byte)] += 1;
          }
          msgbuf.push(msg);
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        std::vector<std::byte> msg(msg_size);
        while (true) {
          std::size_t const index = pop_index.fetch_add(msg_size);
          if (index >= msg_count) {
            return;
          }
          EXPECT_EQ(msgbuf.pop(msg), msg_size);
          for (std::byte const byte : msg) {
            popped_byte_counts[static_cast<std::size_t>(byte)] += 1;
          }
        }
      });
    }
  }

  EXPECT_EQ(pushed_byte_counts, popped_byte_counts);
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void rollover() {
  constexpr std::size_t kCapacity = 32;
  constexpr std::size_t kRepeats  = 3;

  std::array<std::byte, 3> msg{};
  Msgbuf msgbuf(kCapacity);

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
    for (std::size_t i = 0;
         i < std::numeric_limits<typename Msgbuf::index_type>::max(); ++i) {
      EXPECT_TRUE(msgbuf.try_push(
          std::array{std::byte(0), std::byte(1), std::byte(2)}));
      EXPECT_EQ(msgbuf.try_pop(msg), 3);
      EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(1), std::byte(2)}));
    }
  }
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking_callback() {
  constexpr std::size_t kCapacity = 32;

  Msgbuf msgbuf(kCapacity);

  EXPECT_TRUE(msgbuf.try_push(3, [](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  }));
  EXPECT_EQ(msgbuf.size(), 11);

  EXPECT_TRUE(msgbuf.try_push(5, [](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 5);
  }));
  EXPECT_EQ(msgbuf.size(), 24);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  }),
            3);
  EXPECT_EQ(msgbuf.size(), 13);

  EXPECT_TRUE(msgbuf.try_push(std::vector(3, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 24);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 5);
  }),
            5);
  EXPECT_EQ(msgbuf.size(), 11);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  }),
            3);
  EXPECT_EQ(msgbuf.size(), 0);
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_callback() {
  constexpr std::size_t kCapacity = 32;

  Msgbuf msgbuf(kCapacity);

  msgbuf.push(3, [](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  });
  EXPECT_EQ(msgbuf.size(), 11);

  msgbuf.push(5, [](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 5);
  });
  EXPECT_EQ(msgbuf.size(), 24);

  EXPECT_EQ(msgbuf.pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  }),
            3);
  EXPECT_EQ(msgbuf.size(), 13);

  EXPECT_TRUE(msgbuf.try_push(std::vector(3, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 24);

  EXPECT_EQ(msgbuf.pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 5);
  }),
            5);
  EXPECT_EQ(msgbuf.size(), 11);

  EXPECT_EQ(msgbuf.pop([](std::span<std::byte> chunk) noexcept {
    EXPECT_EQ(chunk.size(), 3);
  }),
            3);
  EXPECT_EQ(msgbuf.size(), 0);
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void padding() {
  constexpr std::size_t kCapacity = 64;

  Msgbuf msgbuf(kCapacity);

  EXPECT_TRUE(msgbuf.try_push(std::vector(16, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 24);

  EXPECT_TRUE(msgbuf.try_push(std::vector(24, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 56);

  EXPECT_FALSE(msgbuf.try_push(std::vector(1, std::byte(0))));

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 16);
  EXPECT_EQ(msgbuf.size(), 32);

  EXPECT_TRUE(msgbuf.try_push(std::vector(24, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 64);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 24);
  EXPECT_EQ(msgbuf.size(), 32);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 24);
  EXPECT_EQ(msgbuf.size(), 0);

  EXPECT_FALSE(msgbuf.try_push(std::vector(33, std::byte(0))));

  EXPECT_TRUE(msgbuf.try_push(std::vector(25, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 33);

  EXPECT_TRUE(msgbuf.try_push(std::vector(8, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 56);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 25);
  EXPECT_EQ(msgbuf.size(), 23);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 8);
  EXPECT_EQ(msgbuf.size(), 0);

  EXPECT_TRUE(msgbuf.try_push(std::vector(16, std::byte(0))));
  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 16);
  EXPECT_EQ(msgbuf.size(), 0);

  EXPECT_TRUE(msgbuf.try_push(std::vector(30, std::byte(0))));
  EXPECT_EQ(msgbuf.size(), 62);

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 30);
  EXPECT_EQ(msgbuf.size(), 0);
}

template <typename Msgbuf>
  requires(flow::concepts::blocking_msgbuf<Msgbuf> &&
           flow::concepts::nonblocking_msgbuf<Msgbuf>)
constexpr void exception_callback() {
  constexpr std::size_t kCapacity = 32;

  Msgbuf msgbuf(kCapacity);

  EXPECT_ANY_THROW(msgbuf.try_push(
      4, [](std::span<std::byte>) { throw std::runtime_error(""); }));
  EXPECT_ANY_THROW(msgbuf.push(
      4, [](std::span<std::byte>) { throw std::runtime_error(""); }));

  EXPECT_TRUE(msgbuf.try_push(std::vector(4, std::byte(0))));
  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 4);

  EXPECT_TRUE(msgbuf.try_push(std::vector(1, std::byte(0))));

  EXPECT_ANY_THROW(msgbuf.try_pop(
      [](std::span<std::byte>) { throw std::runtime_error(""); }));
  EXPECT_ANY_THROW(
      msgbuf.pop([](std::span<std::byte>) { throw std::runtime_error(""); }));

  EXPECT_EQ(msgbuf.try_pop([](std::span<std::byte>) noexcept {}), 1);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}  // namespace tests::templates
