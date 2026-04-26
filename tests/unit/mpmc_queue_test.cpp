#include <flow/mpmc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(mpmc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::mpmc_queue<int, std::size_t, 10>) == 128);
  static_assert(alignof(flow::mpmc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::mpmc_queue<int, std::uint8_t>) == 128);
  static_assert(alignof(flow::mpmc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(mpmc_queue, capacity) {
  EXPECT_EQ((flow::mpmc_queue<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::mpmc_queue<std::string, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::mpmc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::mpmc_queue<std::string>(64).capacity(), 64);
}

TEST_T(mpmc_queue, minimal_capacity, flow::mpmc_queue<int>, 2);

TEST_T(mpmc_queue, size_capacity, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, nonblocking_smoke, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, blocking_smoke, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, mixed_smoke, flow::mpmc_queue<int>);

using alignment_queue_t =
    flow::mpmc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     alignof(int) * 2,
                     swiss::leak_detector_allocator<int, true>>;
TEST_T(mpmc_queue, mixed_smoke, alignment_queue_t);

TEST_T(mpmc_queue, blocking_push, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, blocking_pop, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, stop_token_noop, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, blocking_push_stop_token, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, blocking_pop_stop_token, flow::mpmc_queue<int>);

TEST(mpmc_queue, capacity_one) { GTEST_SKIP(); }

TEST_T(mpmc_queue, queue_destructor, flow::mpmc_queue<std::shared_ptr<int>>);

TEST_T(mpmc_queue, item_destructor, flow::mpmc_queue<std::shared_ptr<int>>);

TEST_T(mpmc_queue, non_copyable_item_type,
       flow::mpmc_queue<std::unique_ptr<int>>);

TEST_T(mpmc_queue, non_default_constructible_item_type,
       flow::mpmc_queue<std::reference_wrapper<int>>);

using allocator_queue_t =
    flow::mpmc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     alignof(std::pair<int, std::atomic<std::size_t>>),
                     swiss::leak_detector_allocator<int, true>>;
TEST_T(mpmc_queue, allocator, allocator_queue_t);

class mpmc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(mpmc_queue_workload, nonblocking, flow::mpmc_queue<int>);

TEST_P_T(mpmc_queue_workload, blocking, flow::mpmc_queue<int>);

INSTANTIATE_TEST_SUITE_P(mpmc_queue_workload, mpmc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 2, 2),
                                           std::make_tuple(1000, 32, 4, 1),
                                           std::make_tuple(1000, 64, 1, 4),
                                           std::make_tuple(1000, 128, 4, 4)));

TEST_T(mpmc_queue, queued_producers_nonblocking, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, queued_producers_blocking, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, queued_consumers_nonblocking, flow::mpmc_queue<int>);

TEST_T(mpmc_queue, queued_consumers_blocking, flow::mpmc_queue<int>);

using uint8_index_queue_t = flow::mpmc_queue<int, std::uint8_t>;
TEST_T(mpmc_queue, rollover, uint8_index_queue_t);

}  // namespace tests::unit
