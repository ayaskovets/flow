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

template <flow::concepts::msgbuf T>
constexpr void minimal_capacity(std::size_t capacity) noexcept {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kMsgbufCapacity = 22;

  std::vector<std::byte> msg(3);
  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_EQ(msgbuf.peek(), 0);
  EXPECT_EQ(msgbuf.size(), 0);
  EXPECT_EQ(msgbuf.capacity(), kMsgbufCapacity);

  msgbuf.push(std::vector(3, std::byte(0)));
  EXPECT_EQ(msgbuf.peek(), 3);
  EXPECT_EQ(msgbuf.size(), 11);
  EXPECT_EQ(msgbuf.capacity(), kMsgbufCapacity);

  for (std::size_t i = 0; i < kMsgbufCapacity * kMsgbufCapacity * 2; ++i) {
    msgbuf.push(std::vector(3, std::byte(0)));
    EXPECT_EQ(msgbuf.peek(), 3);
    EXPECT_EQ(msgbuf.size(), 22);
    EXPECT_EQ(msgbuf.capacity(), kMsgbufCapacity);

    msgbuf.pop(msg);
    EXPECT_EQ(msgbuf.peek(), 3);
    EXPECT_EQ(msgbuf.size(), 11);
    EXPECT_EQ(msgbuf.capacity(), kMsgbufCapacity);
  }
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking_smoke() noexcept {
  constexpr std::size_t kMsgbufCapacity = 30;

  std::vector<std::byte> msg(2);
  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_FALSE(msgbuf.try_pop(msg));
  EXPECT_EQ(msgbuf.peek(), 0);

  EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(0))));
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(1))));
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(2))));
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_FALSE(msgbuf.try_push(std::vector(2, std::byte(3))));
  EXPECT_EQ(msgbuf.peek(), 2);

  EXPECT_TRUE(msgbuf.try_pop(msg));
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_EQ(msg, std::vector(2, std::byte(0)));
  EXPECT_TRUE(msgbuf.try_pop(msg));
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_EQ(msg, std::vector(2, std::byte(1)));
  EXPECT_TRUE(msgbuf.try_pop(msg));
  EXPECT_EQ(msgbuf.peek(), 0);
  EXPECT_EQ(msg, std::vector(2, std::byte(2)));

  EXPECT_TRUE(msgbuf.try_push(std::vector(1, std::byte(4))));
  EXPECT_EQ(msgbuf.peek(), 1);
  EXPECT_FALSE(msgbuf.try_pop(std::span<std::byte>(msg.data(), 2)));
  EXPECT_TRUE(msgbuf.try_pop(std::span<std::byte>(msg.data(), 1)));
  EXPECT_EQ(msgbuf.peek(), 0);
  EXPECT_EQ(msg, (std::vector{std::byte(4), std::byte(2)}));
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_smoke() noexcept {
  constexpr std::size_t kMsgbufCapacity = 30;

  std::vector<std::byte> msg(2);
  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_EQ(msgbuf.peek(), 0);

  msgbuf.push(std::vector(2, std::byte(0)));
  EXPECT_EQ(msgbuf.peek(), 2);
  msgbuf.push(std::vector(2, std::byte(1)));
  EXPECT_EQ(msgbuf.peek(), 2);
  msgbuf.push(std::vector(2, std::byte(2)));
  EXPECT_EQ(msgbuf.peek(), 2);

  msgbuf.pop(msg);
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_EQ(msg, std::vector(2, std::byte(0)));
  msgbuf.pop(msg);
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_EQ(msg, std::vector(2, std::byte(1)));
  msgbuf.pop(msg);
  EXPECT_EQ(msgbuf.peek(), 0);
  EXPECT_EQ(msg, std::vector(2, std::byte(2)));

  msgbuf.push(std::vector(1, std::byte(3)));
  EXPECT_EQ(msgbuf.peek(), 1);
  msgbuf.pop(std::span<std::byte>(msg.data(), 1));
  EXPECT_EQ(msgbuf.peek(), 0);
  EXPECT_EQ(msg, (std::vector{std::byte(3), std::byte(2)}));
}

template <typename Msgbuf>
  requires(flow::concepts::blocking_msgbuf<Msgbuf> &&
           flow::concepts::nonblocking_msgbuf<Msgbuf>)
constexpr void mixed_smoke() noexcept {
  constexpr std::size_t kMsgbufCapacity = 30;

  std::array<std::byte, 2> msg{};
  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_EQ(msgbuf.size(), 0);
  EXPECT_FALSE(msgbuf.try_pop(msg));
  EXPECT_EQ(msgbuf.size(), 0);

  msgbuf.push(std::array{std::byte(0), std::byte(1)});
  EXPECT_EQ(msgbuf.size(), 10);
  EXPECT_EQ(msgbuf.peek(), 2);

  msgbuf.push(std::array{std::byte(2), std::byte(3)});
  EXPECT_EQ(msgbuf.size(), 20);
  EXPECT_EQ(msgbuf.peek(), 2);
  EXPECT_TRUE(msgbuf.try_push(std::array{std::byte(4), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 30);
  EXPECT_EQ(msgbuf.peek(), 2);

  msgbuf.pop(msg);
  EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(1)}));
  EXPECT_EQ(msgbuf.size(), 20);
  EXPECT_EQ(msgbuf.peek(), 2);

  EXPECT_TRUE(msgbuf.try_push(std::array{std::byte(5), std::byte(6)}));
  EXPECT_EQ(msgbuf.size(), 30);
  EXPECT_EQ(msgbuf.peek(), 2);
  msgbuf.pop(msg);
  EXPECT_EQ(msg, (std::array{std::byte(2), std::byte(3)}));
  EXPECT_EQ(msgbuf.size(), 20);
  EXPECT_EQ(msgbuf.peek(), 2);

  msgbuf.pop(msg);
  EXPECT_EQ(msg, (std::array{std::byte(4), std::byte(0)}));
  EXPECT_EQ(msgbuf.size(), 10);
  EXPECT_EQ(msgbuf.peek(), 2);

  EXPECT_TRUE(msgbuf.try_pop(msg));
  EXPECT_EQ(msg, (std::array{std::byte(5), std::byte(6)}));
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void blocking_push() noexcept {
  constexpr std::size_t kMsgbufCapacity = 11;

  Msgbuf msgbuf(kMsgbufCapacity);

  msgbuf.push(std::vector(3, std::byte(0)));

  std::jthread const consumer([&] {
    std::array<std::byte, 3> msg{};
    EXPECT_TRUE(msgbuf.try_pop(msg));
    EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(0), std::byte(0)}));
  });

  std::array<std::byte, 2> msg{};
  msgbuf.push(std::vector(2, std::byte(1)));
  msgbuf.pop(msg);
  EXPECT_EQ(msg, (std::array{std::byte(1), std::byte(1)}));
}

template <typename Msgbuf>
  requires(flow::concepts::blocking_msgbuf<Msgbuf> &&
           flow::concepts::nonblocking_msgbuf<Msgbuf>)
constexpr void blocking_pop() noexcept {
  constexpr std::size_t kMsgbufCapacity = 10;

  Msgbuf msgbuf(kMsgbufCapacity);

  std::jthread const producer(
      [&] { EXPECT_TRUE(msgbuf.try_push(std::vector(2, std::byte(1)))); });

  std::array<std::byte, 2> msg{};
  msgbuf.pop(msg);
  EXPECT_EQ(msg, (std::array{std::byte(1), std::byte(1)}));
}

template <flow::concepts::cancellable_blocking_msgbuf Msgbuf>
constexpr void blocking_push_stop_token() noexcept {
  constexpr std::size_t kMsgbufCapacity = 10;

  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::vector(2, std::byte(0))));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(msgbuf.push(stop_token, std::vector(2, std::byte(0))));
  });
}

template <flow::concepts::cancellable_blocking_msgbuf Msgbuf>
constexpr void blocking_pop_stop_token() noexcept {
  constexpr std::size_t kMsgbufCapacity = 10;

  std::array<std::byte, 2> msg{};
  Msgbuf msgbuf(kMsgbufCapacity);

  EXPECT_TRUE(msgbuf.push(std::stop_token{}, std::vector(2, std::byte(0))));
  EXPECT_TRUE(msgbuf.pop(std::stop_token{}, msg));

  std::jthread const producer([&](std::stop_token stop_token) {
    EXPECT_FALSE(msgbuf.pop(stop_token, msg));
  });
}

template <flow::concepts::blocking_msgbuf Msgbuf>
constexpr void allocator() noexcept {
  constexpr std::size_t kMsgbufCapacity = 2;

  swiss::leak_detector_allocator<typename Msgbuf::allocator_type::value_type,
                                 true>
      allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());
  {
    Msgbuf const msgbuf(kMsgbufCapacity, allocator);
    EXPECT_FALSE(allocator.empty());
  }
  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::nonblocking_msgbuf Msgbuf>
constexpr void nonblocking(auto const & param) noexcept {
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
          while (!msgbuf.try_pop(msg)) {
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
constexpr void blocking(auto const & param) noexcept {
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
          msgbuf.pop(msg);
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
constexpr void rollover() noexcept {
  constexpr std::size_t kMsgbufCapacity = 4;
  constexpr std::size_t kRepeats        = 3;

  std::array<std::byte, 3> msg{};
  Msgbuf msgbuf(kMsgbufCapacity);

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
    for (std::size_t i = 0;
         i < std::numeric_limits<typename Msgbuf::index_type>::max(); ++i) {
      EXPECT_TRUE(msgbuf.try_push(
          std::array{std::byte(0), std::byte(1), std::byte(2)}));
      EXPECT_EQ(msgbuf.peek(), 3);
      EXPECT_TRUE(msgbuf.try_pop(msg));
      EXPECT_EQ(msg, (std::array{std::byte(0), std::byte(1), std::byte(2)}));
    }
  }
}

}  // namespace tests::templates
