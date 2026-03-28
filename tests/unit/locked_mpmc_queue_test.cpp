#include <flow/locked_mpmc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(locked_mpmc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::locked_mpmc_queue<int, std::size_t, 10>) == 384);
  static_assert(alignof(flow::locked_mpmc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::locked_mpmc_queue<int, std::uint8_t>) == 384);
  static_assert(alignof(flow::locked_mpmc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(locked_mpmc_queue, capacity) {
  EXPECT_EQ(
      (flow::locked_mpmc_queue<std::string, std::size_t, 128>().capacity()),
      128);
  EXPECT_EQ(
      (flow::locked_mpmc_queue<std::string, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::locked_mpmc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::locked_mpmc_queue<std::string>(64).capacity(), 64);
}

TEST_T(locked_mpmc_queue, minimal_capacity, flow::locked_mpmc_queue<int>, 1);

TEST_T(locked_mpmc_queue, size_capacity, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, nonblocking_smoke, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, blocking_smoke, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, mixed_smoke, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, blocking_push, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, blocking_pop, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, blocking_push_stop_token,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, blocking_pop_stop_token,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, capacity_one, flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, queue_destructor,
       flow::locked_mpmc_queue<std::shared_ptr<int>>);

TEST_T(locked_mpmc_queue, item_destructor,
       flow::locked_mpmc_queue<std::shared_ptr<int>>);

TEST_T(locked_mpmc_queue, non_copyable_item_type,
       flow::locked_mpmc_queue<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::locked_mpmc_queue<int, std::size_t,
                            swiss::kDynamicExtent<std::size_t>,
                            swiss::leak_detector_allocator<int, true>>;
TEST_T(locked_mpmc_queue, allocator, allocator_queue_t);

class locked_mpmc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_size */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(locked_mpmc_queue_workload, nonblocking, flow::locked_mpmc_queue<int>);

TEST_P_T(locked_mpmc_queue_workload, blocking, flow::locked_mpmc_queue<int>);

INSTANTIATE_TEST_SUITE_P(locked_mpmc_queue_workload, locked_mpmc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 2, 2),
                                           std::make_tuple(1000, 32, 4, 1),
                                           std::make_tuple(1000, 64, 1, 4),
                                           std::make_tuple(1000, 128, 4, 4)));

TEST_T(locked_mpmc_queue, queued_producers_nonblocking,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, queued_producers_blocking,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, queued_consumers_nonblocking,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, queued_consumers_blocking,
       flow::locked_mpmc_queue<int>);

TEST_T(locked_mpmc_queue, rollover, flow::locked_mpmc_queue<int>);

}  // namespace tests
