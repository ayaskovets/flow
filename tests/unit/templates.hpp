#pragma once

#include <flow/queue.hpp>
#include <flow/stream.hpp>

#include <algorithm>
#include <atomic>
#include <latch>
#include <limits>
#include <ranges>
#include <stop_token>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#define TEST_T(test_suite_name, test_name, type, ...) \
  TEST(test_suite_name, test_name) { templates::test_name<type>(__VA_ARGS__); }

#define TEST_P_T(test_suite_name, test_name, type, ...)                \
  TEST_P(test_suite_name, test_name) {                                 \
    templates::test_name<type>(GetParam() __VA_OPT__(, ) __VA_ARGS__); \
  }

namespace tests::templates {

template <typename T>
  requires(flow::concepts::queue<T> || flow::concepts::stream<T>)
constexpr void minimal_capacity(std::size_t capacity) noexcept {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

}  // namespace tests::templates

namespace tests::templates {

template <flow::concepts::blocking_queue Queue>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kQueueCapacity = 8;
  constexpr std::size_t kRepeats       = 128;

  Queue queue(kQueueCapacity);

  typename Queue::value_type value{};
  queue.push(value);
  queue.emplace();

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.capacity(), kQueueCapacity);

    queue.emplace();
    EXPECT_EQ(queue.size(), 3);
    EXPECT_EQ(queue.capacity(), kQueueCapacity);

    queue.pop(value);
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.capacity(), kQueueCapacity);
  }
}

template <flow::concepts::nonblocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void nonblocking_smoke() noexcept {
  constexpr std::size_t kQueueCapacity = 4;

  int value{};
  Queue queue(kQueueCapacity);

  EXPECT_FALSE(queue.try_pop(value));

  EXPECT_TRUE(queue.try_push(0));
  EXPECT_TRUE(queue.try_push(1));
  EXPECT_TRUE(queue.try_push(2));
  EXPECT_TRUE(queue.try_push(3));

  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 0);
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 1);
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 2);
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 3);

  EXPECT_FALSE(queue.try_pop(value));

  EXPECT_TRUE(queue.try_push(4));
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 4);
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking_smoke() noexcept {
  constexpr std::size_t kQueueCapacity = 4;

  int value{};
  Queue queue(kQueueCapacity);

  queue.push(0);
  queue.push(1);
  queue.push(2);
  queue.push(3);

  queue.pop(value);
  EXPECT_EQ(value, 0);
  queue.pop(value);
  EXPECT_EQ(value, 1);
  queue.pop(value);
  EXPECT_EQ(value, 2);
  queue.pop(value);
  EXPECT_EQ(value, 3);

  queue.push(4);
  queue.pop(value);
  EXPECT_EQ(value, 4);
}

template <typename Queue>
  requires(flow::concepts::blocking_queue<Queue> &&
           flow::concepts::nonblocking_queue<Queue> &&
           std::same_as<typename Queue::value_type, int>)
constexpr void mixed_smoke() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  int value{};
  Queue queue(kQueueCapacity);

  EXPECT_EQ(queue.size(), 0);
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_EQ(queue.size(), 0);

  queue.push(1);
  EXPECT_EQ(queue.size(), 1);

  queue.push(2);
  EXPECT_EQ(queue.size(), 2);
  EXPECT_FALSE(queue.try_emplace(3));
  EXPECT_EQ(queue.size(), 2);

  queue.pop(value);
  EXPECT_EQ(value, 1);
  EXPECT_EQ(queue.size(), 1);

  queue.emplace(4);
  EXPECT_EQ(queue.size(), 2);
  EXPECT_FALSE(queue.try_push(5));
  EXPECT_EQ(queue.size(), 2);

  queue.pop(value);
  EXPECT_EQ(value, 2);
  EXPECT_EQ(queue.size(), 1);

  queue.pop(value);
  EXPECT_EQ(value, 4);
  EXPECT_EQ(queue.size(), 0);
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_EQ(queue.size(), 0);
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking_push() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  queue.push(1);
  queue.push(2);

  std::jthread const consumer([&] {
    int value{};
    EXPECT_TRUE(queue.try_pop(value));
    EXPECT_EQ(value, 1);
  });

  queue.push(3);

  int value{};
  queue.pop(value);
  EXPECT_EQ(value, 2);
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking_pop() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  std::jthread const producer([&] { EXPECT_TRUE(queue.try_push(2)); });

  int value{};
  queue.pop(value);
  EXPECT_EQ(value, 2);
}

template <flow::concepts::cancellable_blocking_queue Queue>
constexpr void blocking_push_stop_token() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  EXPECT_TRUE(queue.push(std::stop_token{}, typename Queue::value_type{}));
  EXPECT_TRUE(queue.emplace(std::stop_token{}));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(queue.emplace(stop_token));
    EXPECT_FALSE(queue.emplace(stop_token));
  });
}

template <flow::concepts::cancellable_blocking_queue Queue>
constexpr void blocking_pop_stop_token() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  typename Queue::value_type value{};
  Queue queue(kQueueCapacity);

  EXPECT_TRUE(queue.emplace(std::stop_token{}));
  EXPECT_TRUE(queue.pop(std::stop_token{}, value));

  std::jthread const producer([&](std::stop_token stop_token) {
    EXPECT_FALSE(queue.pop(stop_token, value));
  });
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void capacity_one() noexcept {
  constexpr std::size_t kQueueCapacity = 1;

  int value{};
  Queue queue(kQueueCapacity);

  EXPECT_FALSE(queue.try_pop(value));
  queue.push(1);
  EXPECT_FALSE(queue.try_push(3));

  queue.pop(value);
  EXPECT_EQ(value, 1);

  queue.push(4);
  EXPECT_FALSE(queue.try_push(5));

  queue.pop(value);
  EXPECT_EQ(value, 4);

  EXPECT_FALSE(queue.try_pop(value));
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, std::shared_ptr<int>>)
constexpr void queue_destructor() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  std::shared_ptr<int> const value = std::make_shared<int>();

  {
    Queue queue(kQueueCapacity);

    EXPECT_EQ(value.use_count(), 1);
    queue.push(value);
    EXPECT_EQ(value.use_count(), 2);
    queue.push(value);
    EXPECT_EQ(value.use_count(), 3);
  }

  EXPECT_EQ(value.use_count(), 1);
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, std::shared_ptr<int>>)
constexpr void item_destructor() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  std::shared_ptr<int> value = std::make_shared<int>();
  Queue queue(kQueueCapacity);

  EXPECT_EQ(value.use_count(), 1);

  queue.push(value);
  EXPECT_EQ(value.use_count(), 2);
  queue.push(value);
  EXPECT_EQ(value.use_count(), 3);
  queue.pop(value);
  EXPECT_EQ(value.use_count(), 2);
  queue.push(value);
  EXPECT_EQ(value.use_count(), 3);
  queue.pop(value);
  EXPECT_EQ(value.use_count(), 2);
  queue.pop(value);
  EXPECT_EQ(value.use_count(), 1);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, std::unique_ptr<int>>)
constexpr void non_copyable_item_type() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  std::unique_ptr<int> value{};
  Queue queue(kQueueCapacity);

  queue.push(std::move(value));
  queue.pop(value);

  EXPECT_TRUE(queue.try_push(std::move(value)));
  EXPECT_TRUE(queue.try_pop(value));
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void allocator() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  swiss::leak_detector_allocator<typename Queue::allocator_type::value_type,
                                 true>
      allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());

  {
    int value{};
    Queue queue(kQueueCapacity, allocator);
    EXPECT_EQ(queue.get_allocator(), allocator);

    queue.push(1);
    EXPECT_FALSE(allocator.empty());

    queue.push(2);
    EXPECT_FALSE(allocator.empty());

    queue.pop(value);
    EXPECT_EQ(value, 1);
    EXPECT_FALSE(allocator.empty());

    queue.pop(value);
    EXPECT_EQ(value, 2);
    EXPECT_FALSE(allocator.empty());
  }

  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::nonblocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void nonblocking(auto const & param) noexcept {
  auto const & [items_size, queue_capacity, producers, consumers] = param;

  Queue queue(queue_capacity);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));

  std::vector<int> const pushed_items =
      std::ranges::views::iota(0ZU, items_size) |
      std::ranges::to<std::vector<int>>();
  std::vector<int> popped_items(items_size);
  std::atomic<std::size_t> push_index = 0;
  std::atomic<std::size_t> pop_index  = 0;

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        while (true) {
          std::size_t const index = push_index++;
          if (index >= items_size) {
            return;
          }
          while (!queue.try_push(pushed_items[index])) {
          }
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        while (true) {
          std::size_t const index = pop_index++;
          if (index >= items_size) {
            return;
          }
          while (!queue.try_pop(popped_items[index])) {
          }
        }
      });
    }
  }

  std::ranges::sort(popped_items);
  EXPECT_EQ(pushed_items, popped_items);
}

template <flow::concepts::blocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking(auto const & param) noexcept {
  auto const & [items_size, queue_capacity, producers, consumers] = param;

  Queue queue(queue_capacity);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));

  std::vector<int> const pushed_items =
      std::ranges::views::iota(0ZU, items_size) |
      std::ranges::to<std::vector<int>>();
  std::vector<int> popped_items(items_size);
  std::atomic<std::size_t> push_index = 0;
  std::atomic<std::size_t> pop_index  = 0;

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        while (true) {
          std::size_t const index = push_index++;
          if (index >= items_size) {
            return;
          }
          queue.push(pushed_items[index]);
        }
      });
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back([&] {
        latch.arrive_and_wait();

        while (true) {
          std::size_t const index = pop_index++;
          if (index >= items_size) {
            return;
          }
          queue.pop(popped_items[index]);
        }
      });
    }
  }

  std::ranges::sort(popped_items);
  EXPECT_EQ(pushed_items, popped_items);
}

template <flow::concepts::queue Queue>
constexpr void queued_producers_nonblocking() noexcept {
  constexpr std::size_t kQueueCapacity = 2;
  constexpr std::size_t kProducers     = 16;

  Queue queue(kQueueCapacity);
  std::latch latch(static_cast<std::ptrdiff_t>(kProducers + 1));

  queue.emplace();
  queue.emplace();

  std::vector<std::jthread> producers;
  producers.reserve(kProducers);
  for (std::size_t i = 0; i < kProducers; ++i) {
    producers.emplace_back([&] {
      latch.count_down();
      queue.emplace();
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kProducers;) {
    typename Queue::value_type value{};
    i += queue.try_pop(value);
  }
}

template <flow::concepts::blocking_queue Queue>
constexpr void queued_producers_blocking() noexcept {
  constexpr std::size_t kQueueCapacity = 2;
  constexpr std::size_t kProducers     = 16;

  Queue queue(kQueueCapacity);
  std::latch latch(static_cast<std::ptrdiff_t>(kProducers + 1));

  queue.emplace();
  queue.emplace();

  std::vector<std::jthread> producers;
  producers.reserve(kProducers);
  for (std::size_t i = 0; i < kProducers; ++i) {
    producers.emplace_back([&] {
      latch.count_down();
      queue.emplace();
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kProducers; ++i) {
    typename Queue::value_type value{};
    queue.pop(value);
  }
}

template <flow::concepts::nonblocking_queue Queue>
constexpr void queued_consumers_nonblocking() noexcept {
  constexpr std::size_t kQueueCapacity = 2;
  constexpr std::size_t kConsumers     = 16;

  Queue queue(kQueueCapacity);
  std::latch latch(static_cast<std::ptrdiff_t>(kConsumers + 1));

  std::vector<std::jthread> consumers;
  consumers.reserve(kConsumers);
  for (std::size_t i = 0; i < kConsumers; ++i) {
    consumers.emplace_back([&] {
      typename Queue::value_type value{};
      latch.count_down();
      queue.pop(value);
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kConsumers;) {
    i += queue.try_emplace();
  }
}

template <flow::concepts::queue Queue>
constexpr void queued_consumers_blocking() noexcept {
  constexpr std::size_t kQueueCapacity = 2;
  constexpr std::size_t kConsumers     = 16;

  Queue queue(kQueueCapacity);
  std::latch latch(static_cast<std::ptrdiff_t>(kConsumers + 1));

  std::vector<std::jthread> consumers;
  consumers.reserve(kConsumers);
  for (std::size_t i = 0; i < kConsumers; ++i) {
    consumers.emplace_back([&] {
      typename Queue::value_type value{};
      latch.count_down();
      queue.pop(value);
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kConsumers; ++i) {
    queue.emplace();
  }
}

template <flow::concepts::nonblocking_queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void rollover() noexcept {
  constexpr std::size_t kQueueCapacity = 8;
  constexpr std::size_t kRepeats       = 3;

  int value{};
  Queue queue(kQueueCapacity);

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
    for (std::size_t i = 0;
         i < std::numeric_limits<typename Queue::index_type>::max(); ++i) {
      EXPECT_TRUE(queue.try_push(static_cast<int>(i)));
      EXPECT_TRUE(queue.try_pop(value));
      EXPECT_EQ(value, static_cast<int>(i));
    }
  }
}

}  // namespace tests::templates

namespace tests::templates {

template <flow::concepts::blocking_stream Stream>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kStreamCapacity = 8;

  std::vector<std::byte> bytes(2);
  Stream stream(kStreamCapacity);

  stream.push(std::vector(4, std::byte(0)));

  for (std::size_t i = 0; i < kStreamCapacity * kStreamCapacity * 2; ++i) {
    EXPECT_EQ(stream.size(), 4);
    EXPECT_EQ(stream.capacity(), kStreamCapacity);

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
    EXPECT_FALSE(stream.push(stop_token, std::vector(2, std::byte(0))));
  });
}

template <flow::concepts::cancellable_blocking_stream Stream>
constexpr void blocking_pop_stop_token() noexcept {
  constexpr std::size_t kStreamCapacity = 2;

  std::array<std::byte, 2> bytes{};
  Stream stream(kStreamCapacity);

  EXPECT_TRUE(stream.push(std::stop_token{}, std::vector(2, std::byte(0))));
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
  auto const & [bytes_count, queue_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, queue_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, queue_capacity / 7);

  Stream stream(queue_capacity);
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
          while (!stream.try_push(std::span<std::byte>(bytes.data(), size))) {
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
          while (!stream.try_pop(std::span<std::byte>(bytes.data(), size))) {
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
constexpr void blocking(auto const & param) noexcept {
  auto const & [bytes_count, queue_capacity, producers, consumers] = param;

  std::size_t const producer_bytes =
      std::max<std::size_t>(1, queue_capacity / 5);
  std::size_t const consumer_bytes =
      std::max<std::size_t>(1, queue_capacity / 7);

  Stream stream(queue_capacity);
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

}  // namespace tests::templates
