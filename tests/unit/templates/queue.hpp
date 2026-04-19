#pragma once

#include <flow/queue.hpp>

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

namespace tests::templates {

template <flow::concepts::queue T>
constexpr void minimal_capacity(std::size_t capacity) noexcept {
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_ANY_THROW((T(i)));
  }
  EXPECT_NO_THROW((T(capacity)));
}

template <flow::concepts::blocking_queue Queue>
constexpr void size_capacity() noexcept {
  constexpr std::size_t kQueueCapacity = 8;
  constexpr std::size_t kRepeats       = 128;

  typename Queue::value_type value{};
  Queue queue(kQueueCapacity);

  EXPECT_EQ(queue.size(), 0);
  EXPECT_EQ(queue.capacity(), kQueueCapacity);

  queue.push(value);
  queue.emplace();
  EXPECT_EQ(queue.size(), 2);
  EXPECT_EQ(queue.capacity(), kQueueCapacity);

  for (std::size_t repeat = 0; repeat < kRepeats; ++repeat) {
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
constexpr void stop_token_noop() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  typename Queue::value_type value{};
  Queue queue(kQueueCapacity);

  EXPECT_TRUE(queue.push(std::stop_token{}, typename Queue::value_type{}));
  EXPECT_TRUE(queue.push(std::stop_token{}, typename Queue::value_type{}));

  std::stop_source stop_source;
  EXPECT_TRUE(stop_source.request_stop());

  EXPECT_FALSE(
      queue.push(stop_source.get_token(), typename Queue::value_type{}));

  EXPECT_TRUE(queue.pop(std::stop_token{}, value));
  EXPECT_TRUE(queue.pop(std::stop_token{}, value));

  EXPECT_FALSE(queue.pop(stop_source.get_token(), value));
}

template <flow::concepts::cancellable_blocking_queue Queue>
constexpr void blocking_push_stop_token() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  EXPECT_TRUE(queue.push(std::stop_token{}, typename Queue::value_type{}));
  EXPECT_TRUE(queue.emplace(std::stop_token{}));

  std::jthread const consumer([&](std::stop_token stop_token) {
    EXPECT_FALSE(queue.push(
        stop_token, [](typename Queue::value_type &) { GTEST_FAIL(); }));
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
    EXPECT_FALSE(queue.pop(stop_token,
                           [](typename Queue::value_type &) { GTEST_FAIL(); }));
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

template <flow::concepts::nonblocking_queue Queue>
constexpr void nonblocking_callback() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  EXPECT_TRUE(queue.try_push([](int & value) { value = 1; }));
  EXPECT_TRUE(queue.try_push([](int & value) { value = 2; }));
  EXPECT_FALSE(queue.try_push([](int &) { GTEST_FAIL(); }));

  EXPECT_TRUE(queue.try_pop([](int & value) { EXPECT_EQ(value, 1); }));
  EXPECT_TRUE(queue.try_pop([](int & value) { EXPECT_EQ(value, 2); }));
  EXPECT_FALSE(queue.try_pop([](int &) { GTEST_FAIL(); }));
}

template <flow::concepts::blocking_queue Queue>
constexpr void blocking_callback() noexcept {
  constexpr std::size_t kQueueCapacity = 2;

  Queue queue(kQueueCapacity);

  queue.push([](int & value) { value = 1; });
  queue.push([](int & value) { value = 2; });

  queue.pop([](int & value) { EXPECT_EQ(value, 1); });
  queue.pop([](int & value) { EXPECT_EQ(value, 2); });
}

}  // namespace tests::templates
