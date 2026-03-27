#include <flow/queue.hpp>
#include <flow/spsc_queue.hpp>

#include <gtest/gtest.h>

namespace tests {

TEST(queue, spsc_queue) {
  static_assert(
      flow::concepts::noexcept_nonblocking_queue<flow::spsc_queue<int>>);
  static_assert(flow::concepts::noexcept_blocking_queue<flow::spsc_queue<int>>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_queue<
                flow::spsc_queue<int>>);
}

}  // namespace tests
