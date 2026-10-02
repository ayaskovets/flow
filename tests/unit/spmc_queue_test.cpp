#include <flow/spmc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(spmc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::spmc_queue<int, std::size_t, 10>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::spmc_queue<int, std::size_t, 10>) ==
                std::hardware_destructive_interference_size);
  static_assert(sizeof(flow::spmc_queue<int, std::uint8_t>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::spmc_queue<int, std::uint8_t>) ==
                std::hardware_destructive_interference_size);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(spmc_queue, capacity) {
  EXPECT_EQ((flow::spmc_queue<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::spmc_queue<std::string, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::spmc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::spmc_queue<std::string>(64).capacity(), 64);
}

TEST_T(spmc_queue, minimal_capacity, flow::spmc_queue<int>, 2);

TEST_T(spmc_queue, size_capacity, flow::spmc_queue<int>);

TEST_T(spmc_queue, nonblocking_smoke, flow::spmc_queue<int>);

TEST_T(spmc_queue, blocking_smoke, flow::spmc_queue<int>);

TEST_T(spmc_queue, mixed_smoke, flow::spmc_queue<int>);

using alignment_queue_type =
    flow::spmc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     alignof(int) * 2,
                     swiss::leak_detector_allocator<int, true>>;
TEST_T(spmc_queue, mixed_smoke, alignment_queue_type);

TEST_T(spmc_queue, blocking_push, flow::spmc_queue<int>);

TEST_T(spmc_queue, blocking_pop, flow::spmc_queue<int>);

TEST_T(spmc_queue, stop_token_noop, flow::spmc_queue<int>);

TEST_T(spmc_queue, blocking_push_stop_token, flow::spmc_queue<int>);

TEST_T(spmc_queue, blocking_pop_stop_token, flow::spmc_queue<int>);

TEST(spmc_queue, capacity_one) { GTEST_SKIP(); }

TEST_T(spmc_queue, queue_destructor, flow::spmc_queue<std::shared_ptr<int>>);

TEST_T(spmc_queue, item_destructor, flow::spmc_queue<std::shared_ptr<int>>);

TEST_T(spmc_queue, non_copyable_item_type,
       flow::spmc_queue<std::unique_ptr<int>>);

TEST_T(spmc_queue, non_default_constructible_type,
       flow::spmc_queue<std::reference_wrapper<int>>);

TEST_T(spmc_queue, pop_assignable_type, flow::spmc_queue<int>);

using allocator_queue_type =
    flow::spmc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     alignof(std::pair<int, std::atomic<std::size_t>>),
                     swiss::leak_detector_allocator<int, true>>;
TEST_T(spmc_queue, allocator, allocator_queue_type);

class spmc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(spmc_queue_workload, nonblocking, flow::spmc_queue<int>);

TEST_P_T(spmc_queue_workload, blocking, flow::spmc_queue<int>);

INSTANTIATE_TEST_SUITE_P(spmc_queue_workload, spmc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 1, 2),
                                           std::make_tuple(1000, 32, 1, 1),
                                           std::make_tuple(1000, 64, 1, 4),
                                           std::make_tuple(1000, 128, 1, 4)));

TEST(spmc_queue, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(spmc_queue, queued_producers_blocking) { GTEST_SKIP(); }

TEST_T(spmc_queue, queued_consumers_nonblocking, flow::spmc_queue<int>);

TEST_T(spmc_queue, queued_consumers_blocking, flow::spmc_queue<int>);

using uint8_index_queue_type = flow::spmc_queue<int, std::uint8_t>;
TEST_T(spmc_queue, rollover, uint8_index_queue_type);

}  // namespace tests::unit
