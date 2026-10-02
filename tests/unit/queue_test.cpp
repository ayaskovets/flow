#include <flow/locked_ring_buffer.hpp>
#include <flow/mpmc_queue.hpp>
#include <flow/mpsc_queue.hpp>
#include <flow/queue.hpp>
#include <flow/spmc_queue.hpp>
#include <flow/spsc_queue.hpp>
#include <flow/unbounded_spsc_queue.hpp>
#include <flow/unsafe_queue.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(queue, locked_ring_buffer) {
  using queue_type = flow::locked_ring_buffer<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(!flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(!flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

TEST(queue, mpmc_queue) {
  using queue_type = flow::mpmc_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

TEST(queue, mpsc_queue) {
  using queue_type = flow::mpsc_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

TEST(queue, spmc_queue) {
  using queue_type = flow::spmc_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
}

TEST(queue, spsc_queue) {
  using queue_type = flow::spsc_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

TEST(queue, unbounded_spsc_queue) {
  using queue_type = flow::unbounded_spsc_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(!flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(!flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

TEST(queue, unsafe_queue) {
  using queue_type = flow::unsafe_queue<int>;

  static_assert(flow::concepts::queue<queue_type>);
  static_assert(flow::concepts::queue<queue_type, int>);
  static_assert(flow::concepts::nonblocking_queue<queue_type>);
  static_assert(flow::concepts::nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type>);
  static_assert(flow::concepts::noexcept_nonblocking_queue<queue_type, int>);
  static_assert(flow::concepts::blocking_queue<queue_type>);
  static_assert(flow::concepts::blocking_queue<queue_type, int>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type>);
  static_assert(!flow::concepts::noexcept_blocking_queue<queue_type, int>);
  static_assert(!flow::concepts::cancellable_blocking_queue<queue_type>);
  static_assert(!flow::concepts::cancellable_blocking_queue<queue_type, int>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_queue<queue_type, int>);
}

}  // namespace tests::unit
