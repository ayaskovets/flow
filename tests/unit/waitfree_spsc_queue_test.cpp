#include <flow/waitfree_spsc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(waitfree_spsc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::waitfree_spsc_queue<int, std::size_t, 10>) == 128);
  static_assert(alignof(flow::waitfree_spsc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::waitfree_spsc_queue<int, std::uint8_t>) == 128);
  static_assert(alignof(flow::waitfree_spsc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(waitfree_spsc_queue, capacity) {
  EXPECT_EQ(
      (flow::waitfree_spsc_queue<std::string, std::size_t, 128>().capacity()),
      128);
  EXPECT_EQ(
      (flow::waitfree_spsc_queue<std::string, std::size_t, 64>().capacity()),
      64);
  EXPECT_EQ(flow::waitfree_spsc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::waitfree_spsc_queue<std::string>(64).capacity(), 64);
}

TEST_T(waitfree_spsc_queue, minimal_capacity, flow::waitfree_spsc_queue<int>,
       1);

TEST_T(waitfree_spsc_queue, size_capacity, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, nonblocking_smoke, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, blocking_smoke, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, mixed_smoke, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, blocking_push, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, blocking_pop, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, capacity_one, flow::waitfree_spsc_queue<int>);

TEST_T(waitfree_spsc_queue, queue_destructor,
       flow::waitfree_spsc_queue<std::shared_ptr<int>>);

TEST_T(waitfree_spsc_queue, item_destructor,
       flow::waitfree_spsc_queue<std::shared_ptr<int>>);

TEST_T(waitfree_spsc_queue, non_copyable_item_type,
       flow::waitfree_spsc_queue<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::waitfree_spsc_queue<int, std::size_t, 2,
                              swiss::leak_detector_allocator<int, true>>;
TEST_T(waitfree_spsc_queue, allocator, allocator_queue_t);

class queues_waitfree_spsc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_size */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(queues_waitfree_spsc_queue_workload, nonblocking,
         flow::waitfree_spsc_queue<int>);

TEST_P_T(queues_waitfree_spsc_queue_workload, blocking,
         flow::waitfree_spsc_queue<int>);

INSTANTIATE_TEST_SUITE_P(queues_waitfree_spsc_queue_workload,
                         queues_waitfree_spsc_queue_workload,
                         ::testing::Values(std::make_tuple(5, 4, 1, 1),
                                           std::make_tuple(20, 16, 1, 1),
                                           std::make_tuple(100, 16, 1, 1),
                                           std::make_tuple(30, 8, 1, 1),
                                           std::make_tuple(10000, 128, 1, 1)));

TEST(waitfree_spsc_queue, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(waitfree_spsc_queue, queued_producers_blocking) { GTEST_SKIP(); }

TEST(waitfree_spsc_queue, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(waitfree_spsc_queue, queued_consumers_blocking) { GTEST_SKIP(); }

TEST_T(waitfree_spsc_queue, rollover, flow::waitfree_spsc_queue<int>);

}  // namespace tests
