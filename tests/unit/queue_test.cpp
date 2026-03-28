#include <flow/locked_deque.hpp>
#include <flow/locked_ring_buffer.hpp>
#include <flow/mpmc_queue.hpp>
#include <flow/queue.hpp>
#include <flow/spsc_queue.hpp>
#include <flow/unsafe_queue.hpp>

#include <gtest/gtest.h>

namespace tests {

TEST(queue, locked_deque) {
  static_assert(
      flow::concepts::noexcept_nonblocking_queue<flow::locked_deque<int>>);
  static_assert(
      flow::concepts::noexcept_blocking_queue<flow::locked_deque<int>>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<
                flow::locked_deque<int>>);
}

TEST(queue, locked_ring_buffer) {
  static_assert(flow::concepts::noexcept_nonblocking_queue<
                flow::locked_ring_buffer<int>>);
  static_assert(
      flow::concepts::noexcept_blocking_queue<flow::locked_ring_buffer<int>>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<
                flow::locked_ring_buffer<int>>);
}

TEST(queue, mpmc_queue) {
  static_assert(
      flow::concepts::noexcept_nonblocking_queue<flow::mpmc_queue<int>>);
  static_assert(flow::concepts::noexcept_blocking_queue<flow::mpmc_queue<int>>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<
                flow::mpmc_queue<int>>);
}

TEST(queue, spsc_queue) {
  static_assert(
      flow::concepts::noexcept_nonblocking_queue<flow::spsc_queue<int>>);
  static_assert(flow::concepts::noexcept_blocking_queue<flow::spsc_queue<int>>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<
                flow::spsc_queue<int>>);
}

TEST(queue, unsafe_queue) {
  static_assert(
      flow::concepts::noexcept_nonblocking_queue<flow::unsafe_queue<int>>);
  static_assert(flow::concepts::blocking_queue<flow::unsafe_queue<int>>);
}

}  // namespace tests
