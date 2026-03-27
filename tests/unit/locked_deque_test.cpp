#include <flow/locked_deque.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(locked_deque, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::locked_deque<int, std::size_t, 10>) == 240);
  static_assert(alignof(flow::locked_deque<int, std::size_t, 10>) == 8);
  static_assert(sizeof(flow::locked_deque<int, std::uint8_t>) == 248);
  static_assert(alignof(flow::locked_deque<int, std::uint8_t>) == 8);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(locked_deque, capacity) {
  EXPECT_EQ((flow::locked_deque<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::locked_deque<std::string, std::size_t, 64>().capacity()),
            64);
  EXPECT_EQ(flow::locked_deque<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::locked_deque<std::string>(64).capacity(), 64);
}

TEST_T(locked_deque, minimal_capacity, flow::locked_deque<int>, 1);

TEST_T(locked_deque, size_capacity, flow::locked_deque<int>);

TEST_T(locked_deque, nonblocking_smoke, flow::locked_deque<int>);

TEST_T(locked_deque, blocking_smoke, flow::locked_deque<int>);

TEST_T(locked_deque, mixed_smoke, flow::locked_deque<int>);

TEST_T(locked_deque, blocking_push, flow::locked_deque<int>);

TEST_T(locked_deque, blocking_pop, flow::locked_deque<int>);

TEST_T(locked_deque, blocking_push_stop_token, flow::locked_deque<int>);

TEST_T(locked_deque, blocking_pop_stop_token, flow::locked_deque<int>);

TEST_T(locked_deque, capacity_one, flow::locked_deque<int>);

TEST_T(locked_deque, queue_destructor,
       flow::locked_deque<std::shared_ptr<int>>);

TEST_T(locked_deque, item_destructor, flow::locked_deque<std::shared_ptr<int>>);

TEST_T(locked_deque, non_copyable_item_type,
       flow::locked_deque<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::locked_deque<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                       swiss::leak_detector_allocator<int, true>>;
TEST_T(locked_deque, allocator, allocator_queue_t);

class locked_deque_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_size */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(locked_deque_workload, nonblocking, flow::locked_deque<int>);

TEST_P_T(locked_deque_workload, blocking, flow::locked_deque<int>);

INSTANTIATE_TEST_SUITE_P(locked_deque_workload, locked_deque_workload,
                         ::testing::Values(std::make_tuple(5, 3, 1, 1),
                                           std::make_tuple(20, 10, 2, 1),
                                           std::make_tuple(100, 10, 4, 1),
                                           std::make_tuple(100, 10, 1, 4),
                                           std::make_tuple(10000, 100, 4, 4)));

TEST_T(locked_deque, queued_producers_nonblocking, flow::locked_deque<int>);

TEST_T(locked_deque, queued_producers_blocking, flow::locked_deque<int>);

TEST_T(locked_deque, queued_consumers_nonblocking, flow::locked_deque<int>);

TEST_T(locked_deque, queued_consumers_blocking, flow::locked_deque<int>);

TEST_T(locked_deque, rollover, flow::locked_deque<int>);

}  // namespace tests
