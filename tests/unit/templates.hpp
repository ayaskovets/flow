#pragma once

#include <flow/queue.hpp>

#include <algorithm>
#include <atomic>
#include <latch>
#include <ranges>
#include <thread>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#define TEST_T(test_suite_name, test_name, type, ...) \
  TEST(test_suite_name, test_name) { templates::test_name<type>(__VA_ARGS__); }

#define TEST_P_T(test_suite_name, test_name, type, ...)                \
  TEST_P(test_suite_name, test_name) {                                 \
    templates::test_name<type>(GetParam() __VA_OPT__(, ) __VA_ARGS__); \
  }

namespace tests::templates {

template <flow::concepts::queue Queue>
constexpr void minimal_capacity(std::size_t capacity) noexcept {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((Queue(i)));
  }
  EXPECT_NO_THROW((Queue(capacity)));
}

template <flow::concepts::queue Queue>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kQueueSize = 8;

  Queue queue(kQueueSize);
  queue.push({});
  queue.push({});

  for (std::size_t i = 0; i < kQueueSize * kQueueSize * 2; ++i) {
    queue.push({});
    queue.pop();
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.capacity(), kQueueSize);
  }
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void nonblocking_smoke() noexcept {
  int value{};
  Queue queue(3);

  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_TRUE(queue.try_push(1));
  EXPECT_TRUE(queue.try_push(2));
  EXPECT_TRUE(queue.try_push(3));
  EXPECT_FALSE(queue.try_push(4));
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 1);
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 2);
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 3);
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_TRUE(queue.try_push(5));
  EXPECT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 5);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking_smoke() noexcept {
  Queue queue(3);

  queue.push(1);
  queue.push(2);
  queue.push(3);
  EXPECT_EQ(queue.pop(), 1);
  EXPECT_EQ(queue.pop(), 2);
  EXPECT_EQ(queue.pop(), 3);
  queue.push(4);
  EXPECT_EQ(queue.pop(), 4);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void mixed_smoke() noexcept {
  int value{};
  Queue queue(2);

  EXPECT_EQ(queue.size(), 0);
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_EQ(queue.size(), 0);
  queue.push(1);
  EXPECT_EQ(queue.size(), 1);
  queue.push(2);
  EXPECT_EQ(queue.size(), 2);
  EXPECT_FALSE(queue.try_emplace(3));
  EXPECT_EQ(queue.size(), 2);
  EXPECT_EQ(queue.pop(), 1);
  EXPECT_EQ(queue.size(), 1);
  queue.emplace(4);
  EXPECT_EQ(queue.size(), 2);
  EXPECT_FALSE(queue.try_push(5));
  EXPECT_EQ(queue.size(), 2);
  EXPECT_EQ(queue.pop(), 2);
  EXPECT_EQ(queue.size(), 1);
  EXPECT_EQ(queue.pop(), 4);
  EXPECT_EQ(queue.size(), 0);
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_EQ(queue.size(), 0);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking_push() noexcept {
  Queue queue(2);
  queue.push(1);
  queue.push(2);

  std::jthread const consumer([&queue] {
    int value{};
    EXPECT_TRUE(queue.try_pop(value));
    EXPECT_EQ(value, 1);
  });

  queue.push(3);
  EXPECT_EQ(queue.pop(), 2);
}

template <flow::concepts::queue Queue>
constexpr void blocking_pop() noexcept {
  Queue queue(2);

  std::jthread const producer([&queue] { EXPECT_TRUE(queue.try_push({})); });

  EXPECT_EQ(queue.pop(), {});
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void capacity_one() noexcept {
  int value{};
  Queue queue(1);

  EXPECT_FALSE(queue.try_pop(value));
  queue.push(1);
  EXPECT_FALSE(queue.try_push(3));
  EXPECT_EQ(queue.pop(), 1);
  queue.push(4);
  EXPECT_FALSE(queue.try_push(5));
  EXPECT_EQ(queue.pop(), 4);
  EXPECT_FALSE(queue.try_pop(value));
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, std::shared_ptr<int>>)
constexpr void queue_destructor() noexcept {
  std::shared_ptr<int> value = std::make_shared<int>();

  {
    Queue queue(2);

    EXPECT_EQ(value.use_count(), 1);
    queue.push(value);
    EXPECT_EQ(value.use_count(), 2);
    queue.push(value);
    EXPECT_EQ(value.use_count(), 3);
  }

  EXPECT_EQ(value.use_count(), 1);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, std::shared_ptr<int>>)
constexpr void item_destructor() noexcept {
  std::shared_ptr<int> value = std::make_shared<int>();
  Queue queue(2);

  EXPECT_EQ(value.use_count(), 1);
  queue.push(value);
  EXPECT_EQ(value.use_count(), 2);
  queue.push(value);
  EXPECT_EQ(value.use_count(), 3);
  queue.pop();
  EXPECT_EQ(value.use_count(), 2);
  queue.push(value);
  EXPECT_EQ(value.use_count(), 3);
  queue.pop();
  EXPECT_EQ(value.use_count(), 2);
  queue.pop();
  EXPECT_EQ(value.use_count(), 1);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, std::unique_ptr<int>>)
constexpr void non_copyable_item_type() noexcept {
  std::unique_ptr<int> value{};
  Queue queue(2);

  queue.push(std::move(value));
  queue.pop();
  queue.try_push(std::move(value));
  queue.try_pop(value);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void allocator() noexcept {
  swiss::leak_detector_allocator<typename Queue::value_type, true> allocator;
  allocator.clear();

  EXPECT_TRUE(allocator.empty());
  {
    Queue queue(allocator);
    EXPECT_EQ(queue.get_allocator(), allocator);

    queue.push(1);
    EXPECT_FALSE(allocator.empty());
    queue.push(2);
    EXPECT_FALSE(allocator.empty());
    EXPECT_EQ(queue.pop(), 1);
    EXPECT_FALSE(allocator.empty());
    EXPECT_EQ(queue.pop(), 2);
    EXPECT_FALSE(allocator.empty());
  }
  EXPECT_TRUE(allocator.empty());
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void nonblocking(auto const & param) noexcept {
  auto const & [items_size, queue_size, producers, consumers] = param;

  auto const pushed_items =
      std::ranges::views::iota(static_cast<std::size_t>(0), items_size);

  Queue queue(queue_size);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));
  std::vector<int> popped_items(items_size);

  std::atomic<int> push_value = 0;
  auto const producer         = [&] {
    latch.arrive_and_wait();

    while (true) {
      int const index = push_value.fetch_add(1, std::memory_order::relaxed);
      if (static_cast<std::size_t>(index) >= items_size) {
        return;
      }
      while (!queue.try_push(index)) {
      }
    }
  };

  std::atomic<std::size_t> pop_index = 0;
  auto const consumer                = [&] {
    latch.arrive_and_wait();

    while (true) {
      std::size_t const index =
          pop_index.fetch_add(1, std::memory_order::relaxed);
      if (index >= items_size) {
        return;
      }
      while (!queue.try_pop(popped_items[index])) {
      }
    }
  };

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back(producer);
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back(consumer);
    }
  }

  std::ranges::sort(popped_items);
  EXPECT_EQ(std::vector<int>(pushed_items.begin(), pushed_items.end()),
            popped_items);
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void blocking(auto const & param) noexcept {
  auto const & [items_size, queue_size, producers, consumers] = param;

  auto const pushed_items =
      std::ranges::views::iota(static_cast<std::size_t>(0), items_size);

  Queue queue(queue_size);
  std::latch latch(static_cast<std::ptrdiff_t>(producers + consumers));
  std::vector<int> popped_items(items_size);

  std::atomic<int> push_value = 0;
  auto const producer         = [&] {
    latch.arrive_and_wait();

    while (true) {
      int const index = push_value.fetch_add(1, std::memory_order::relaxed);
      if (static_cast<std::size_t>(index) >= items_size) {
        return;
      }
      queue.push(index);
    }
  };

  std::atomic<std::size_t> pop_index = 0;
  auto const consumer                = [&] {
    latch.arrive_and_wait();

    while (true) {
      std::size_t const index =
          pop_index.fetch_add(1, std::memory_order::relaxed);
      if (index >= items_size) {
        return;
      }
      popped_items[index] = queue.pop();
    }
  };

  {
    std::vector<std::jthread> threads;
    threads.reserve(producers + consumers);
    for (std::size_t i = 0; i < producers; ++i) {
      threads.emplace_back(producer);
    }
    for (std::size_t i = 0; i < consumers; ++i) {
      threads.emplace_back(consumer);
    }
  }

  std::ranges::sort(popped_items);
  EXPECT_EQ(std::vector<int>(pushed_items.begin(), pushed_items.end()),
            popped_items);
}

template <flow::concepts::queue Queue>
constexpr void queued_producers_nonblocking() noexcept {
  constexpr std::size_t kProducers = 16;

  Queue queue(2);
  queue.push({});
  queue.push({});

  std::vector<std::jthread> producers;
  producers.reserve(kProducers);
  std::latch latch(static_cast<std::ptrdiff_t>(kProducers + 1));

  for (std::size_t i = 0; i < kProducers; ++i) {
    producers.emplace_back([&] {
      latch.count_down();
      queue.push({});
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kProducers;) {
    typename Queue::value_type value{};
    i += queue.try_pop(value);
  }
}

template <flow::concepts::queue Queue>
constexpr void queued_producers_blocking() noexcept {
  constexpr std::size_t kProducers = 16;

  Queue queue(2);
  queue.push({});
  queue.push({});

  std::vector<std::jthread> producers;
  producers.reserve(kProducers);
  std::latch latch(static_cast<std::ptrdiff_t>(kProducers + 1));

  for (std::size_t i = 0; i < kProducers; ++i) {
    producers.emplace_back([&] {
      latch.count_down();
      queue.push({});
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kProducers; ++i) {
    queue.pop();
  }
}

template <flow::concepts::queue Queue>
constexpr void queued_consumers_nonblocking() noexcept {
  constexpr std::size_t kConsumers = 16;

  Queue queue(2);
  std::vector<std::jthread> consumers;
  std::latch latch(static_cast<std::ptrdiff_t>(kConsumers + 1));

  for (std::size_t i = 0; i < kConsumers; ++i) {
    consumers.emplace_back([&] {
      latch.count_down();
      queue.pop();
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kConsumers;) {
    i += queue.try_push({});
  }
}

template <flow::concepts::queue Queue>
constexpr void queued_consumers_blocking() noexcept {
  constexpr std::size_t kConsumers = 16;

  Queue queue(2);
  std::vector<std::jthread> consumers;
  consumers.reserve(kConsumers);
  std::latch latch(static_cast<std::ptrdiff_t>(kConsumers + 1));

  for (std::size_t i = 0; i < kConsumers; ++i) {
    consumers.emplace_back([&] {
      latch.count_down();
      queue.pop();
    });
  }

  latch.arrive_and_wait();
  for (std::size_t i = 0; i < kConsumers; ++i) {
    queue.push({});
  }
}

template <flow::concepts::queue Queue>
  requires(std::same_as<typename Queue::value_type, int>)
constexpr void rollover() noexcept {
  constexpr std::size_t kQueueSize = 8;

  Queue queue(kQueueSize);

  for (std::size_t i = 0; i < kQueueSize * kQueueSize * 2; ++i) {
    EXPECT_TRUE(queue.try_push(static_cast<int>(i)));
    EXPECT_EQ(queue.pop(), static_cast<int>(i));
  }
}

}  // namespace tests::templates
