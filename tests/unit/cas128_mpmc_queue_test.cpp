#include <flow/cas128_mpmc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(cas128_mpmc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::cas128_mpmc_queue<int, std::size_t, 10>) == 128);
  static_assert(alignof(flow::cas128_mpmc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::cas128_mpmc_queue<int, std::uint8_t>) == 128);
  static_assert(alignof(flow::cas128_mpmc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(cas128_mpmc_queue, capacity) {
  EXPECT_EQ((flow::cas128_mpmc_queue<int, std::size_t, 128>().capacity()), 128);
  EXPECT_EQ((flow::cas128_mpmc_queue<int, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::cas128_mpmc_queue<int>(128).capacity(), 128);
  EXPECT_EQ(flow::cas128_mpmc_queue<int>(64).capacity(), 64);
}

TEST_T(cas128_mpmc_queue, minimal_capacity, flow::cas128_mpmc_queue<int>, 2);

TEST_T(cas128_mpmc_queue, size_capacity, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, nonblocking_smoke, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, blocking_smoke, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, mixed_smoke, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, blocking_push, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, blocking_pop, flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, blocking_push_stop_token,
       flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, blocking_pop_stop_token,
       flow::cas128_mpmc_queue<int>);

TEST(cas128_mpmc_queue, capacity_one) { GTEST_SKIP(); }

TEST(cas128_mpmc_queue, queue_destructor) { GTEST_SKIP(); }

TEST(cas128_mpmc_queue, item_destructor) { GTEST_SKIP(); }

TEST(cas128_mpmc_queue, non_copyable_item_type) { GTEST_SKIP(); }

using allocator_queue_t = flow::cas128_mpmc_queue<
    int, std::size_t, swiss::kDynamicExtent<std::size_t>,
    swiss::leak_detector_allocator<
        std::atomic<swiss::trivial_pair<int, std::size_t>>, true>>;
TEST_T(cas128_mpmc_queue, allocator, allocator_queue_t);

class cas128_mpmc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(cas128_mpmc_queue_workload, nonblocking, flow::cas128_mpmc_queue<int>);

TEST_P_T(cas128_mpmc_queue_workload, blocking, flow::cas128_mpmc_queue<int>);

INSTANTIATE_TEST_SUITE_P(cas128_mpmc_queue_workload, cas128_mpmc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 2, 2),
                                           std::make_tuple(1000, 32, 4, 1),
                                           std::make_tuple(1000, 64, 1, 4),
                                           std::make_tuple(1000, 128, 4, 4)));

TEST_T(cas128_mpmc_queue, queued_producers_nonblocking,
       flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, queued_producers_blocking,
       flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, queued_consumers_nonblocking,
       flow::cas128_mpmc_queue<int>);

TEST_T(cas128_mpmc_queue, queued_consumers_blocking,
       flow::cas128_mpmc_queue<int>);

using uint8_index_queue_t = flow::cas128_mpmc_queue<int, std::uint8_t>;
TEST_T(cas128_mpmc_queue, rollover, uint8_index_queue_t);

}  // namespace tests::unit
