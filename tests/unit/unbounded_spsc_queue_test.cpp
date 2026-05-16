#include <flow/unbounded_spsc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {
// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

TEST(unbounded_spsc_queue, size) {
  static_assert(sizeof(flow::unbounded_spsc_queue<int, std::size_t, 10>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::unbounded_spsc_queue<int, std::size_t, 10>) ==
                std::hardware_destructive_interference_size);
  static_assert(sizeof(flow::unbounded_spsc_queue<int, std::uint8_t>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::unbounded_spsc_queue<int, std::uint8_t>) ==
                std::hardware_destructive_interference_size);
}

TEST(unbounded_spsc_queue, capacity) {
  EXPECT_EQ((flow::unbounded_spsc_queue<std::string, std::size_t, 128>()
                 .chunk_capacity()),
            128);
  EXPECT_EQ((flow::unbounded_spsc_queue<std::string, std::size_t, 64>()
                 .chunk_capacity()),
            64);
}

TEST_T(unbounded_spsc_queue, minimal_capacity, flow::unbounded_spsc_queue<int>,
       1);

TEST_T(unbounded_spsc_queue, size_capacity, flow::unbounded_spsc_queue<int>);

TEST_T(unbounded_spsc_queue, nonblocking_smoke,
       flow::unbounded_spsc_queue<int>);

TEST_T(unbounded_spsc_queue, blocking_smoke, flow::unbounded_spsc_queue<int>);

TEST(unbounded_spsc_queue, mixed_smoke) { GTEST_SKIP(); }

TEST(unbounded_spsc_queue, blocking_push) { GTEST_SKIP(); }

TEST_T(unbounded_spsc_queue, blocking_pop, flow::unbounded_spsc_queue<int>);

TEST_T(unbounded_spsc_queue, stop_token_noop, flow::unbounded_spsc_queue<int>);

TEST_T(unbounded_spsc_queue, blocking_push_stop_token,
       flow::unbounded_spsc_queue<int>);

TEST_T(unbounded_spsc_queue, blocking_pop_stop_token,
       flow::unbounded_spsc_queue<int>);

TEST(unbounded_spsc_queue, capacity_one) { GTEST_SKIP(); }

TEST_T(unbounded_spsc_queue, queue_destructor,
       flow::unbounded_spsc_queue<std::shared_ptr<int>>);

TEST_T(unbounded_spsc_queue, item_destructor,
       flow::unbounded_spsc_queue<std::shared_ptr<int>>);

TEST_T(unbounded_spsc_queue, non_copyable_item_type,
       flow::unbounded_spsc_queue<std::unique_ptr<int>>);

TEST_T(unbounded_spsc_queue, non_default_constructible_type,
       flow::unbounded_spsc_queue<std::reference_wrapper<int>>);

TEST_T(unbounded_spsc_queue, pop_assignable_type,
       flow::unbounded_spsc_queue<int>);

using allocator_queue_t =
    flow::unbounded_spsc_queue<int, std::size_t,
                               swiss::kDynamicExtent<std::size_t>, alignof(int),
                               swiss::leak_detector_allocator<int, true>>;
TEST_T(unbounded_spsc_queue, allocator, allocator_queue_t);

class unbounded_spsc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_chunk_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(unbounded_spsc_queue_workload, nonblocking,
         flow::unbounded_spsc_queue<int>);

TEST_P_T(unbounded_spsc_queue_workload, blocking,
         flow::unbounded_spsc_queue<int>);

INSTANTIATE_TEST_SUITE_P(unbounded_spsc_queue_workload,
                         unbounded_spsc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 1, 1),
                                           std::make_tuple(1000, 32, 1, 1),
                                           std::make_tuple(1000, 64, 1, 1),
                                           std::make_tuple(1000, 128, 1, 1)));

TEST(unbounded_spsc_queue, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(unbounded_spsc_queue, queued_producers_blocking) { GTEST_SKIP(); }

TEST(unbounded_spsc_queue, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(unbounded_spsc_queue, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_queue_t = flow::unbounded_spsc_queue<int, std::uint8_t>;
TEST_T(unbounded_spsc_queue, rollover, uint8_index_queue_t);

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}  // namespace tests::unit
