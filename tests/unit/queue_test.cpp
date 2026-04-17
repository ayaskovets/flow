#include <flow/cas128_mpmc_queue.hpp>
#include <flow/locked_deque.hpp>
#include <flow/locked_ring_buffer.hpp>
#include <flow/mpmc_queue.hpp>
#include <flow/mpsc_queue.hpp>
#include <flow/queue.hpp>
#include <flow/spmc_queue.hpp>
#include <flow/spsc_queue.hpp>
#include <flow/unsafe_queue.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(queue, cas128_mpmc_queue) {
  using queue_t = flow::cas128_mpmc_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(!flow::concepts::single_producer_queue<queue_t>);
  static_assert(!flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, locked_deque) {
  using queue_t = flow::locked_deque<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(!flow::concepts::single_producer_queue<queue_t>);
  static_assert(!flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, locked_ring_buffer) {
  using queue_t = flow::locked_ring_buffer<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(!flow::concepts::single_producer_queue<queue_t>);
  static_assert(!flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, mpsc_queue) {
  using queue_t = flow::mpsc_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(!flow::concepts::single_producer_queue<queue_t>);
  static_assert(flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, mpmc_queue) {
  using queue_t = flow::mpmc_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(!flow::concepts::single_producer_queue<queue_t>);
  static_assert(!flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, spmc_queue) {
  using queue_t = flow::spmc_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(flow::concepts::single_producer_queue<queue_t>);
  static_assert(!flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, spsc_queue) {
  using queue_t = flow::spsc_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

TEST(queue, unsafe_queue) {
  using queue_t = flow::unsafe_queue<int>;

  static_assert(flow::concepts::queue<queue_t>);
  static_assert(flow::concepts::single_producer_queue<queue_t>);
  static_assert(flow::concepts::single_consumer_queue<queue_t>);
  static_assert(flow::concepts::nonblocking_queue<queue_t>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_t>);
  static_assert(flow::concepts::blocking_queue<queue_t>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_t>);
  static_assert(!flow::concepts::cancellable_blocking_queue<queue_t>);
  static_assert(!flow::concepts::noexcept_cancellable_blocking_queue<queue_t>);
}

}  // namespace tests::unit
