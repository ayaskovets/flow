#include <flow/spsc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(spsc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::spsc_queue<int, std::size_t, 10>) == 128);
  static_assert(alignof(flow::spsc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::spsc_queue<int, std::uint8_t>) == 128);
  static_assert(alignof(flow::spsc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(spsc_queue, capacity) {
  EXPECT_EQ((flow::spsc_queue<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::spsc_queue<std::string, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::spsc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::spsc_queue<std::string>(64).capacity(), 64);
}

TEST_T(spsc_queue, minimal_capacity, flow::spsc_queue<int>, 1);

TEST_T(spsc_queue, size_capacity, flow::spsc_queue<int>);

TEST_T(spsc_queue, nonblocking_smoke, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_smoke, flow::spsc_queue<int>);

TEST_T(spsc_queue, mixed_smoke, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_push, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_pop, flow::spsc_queue<int>);

TEST_T(spsc_queue, stop_token_noop, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_push_stop_token, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_pop_stop_token, flow::spsc_queue<int>);

TEST_T(spsc_queue, capacity_one, flow::spsc_queue<int>);

TEST_T(spsc_queue, queue_destructor, flow::spsc_queue<std::shared_ptr<int>>);

TEST_T(spsc_queue, item_destructor, flow::spsc_queue<std::shared_ptr<int>>);

TEST_T(spsc_queue, non_copyable_item_type,
       flow::spsc_queue<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::spsc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     swiss::leak_detector_allocator<int, true>>;
TEST_T(spsc_queue, allocator, allocator_queue_t);

class spsc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(spsc_queue_workload, nonblocking, flow::spsc_queue<int>);

TEST_P_T(spsc_queue_workload, blocking, flow::spsc_queue<int>);

INSTANTIATE_TEST_SUITE_P(spsc_queue_workload, spsc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 1, 1),
                                           std::make_tuple(1000, 32, 1, 1),
                                           std::make_tuple(1000, 64, 1, 1),
                                           std::make_tuple(1000, 128, 1, 1)));

TEST(spsc_queue, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(spsc_queue, queued_producers_blocking) { GTEST_SKIP(); }

TEST(spsc_queue, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(spsc_queue, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_queue_t = flow::spsc_queue<int, std::uint8_t>;
TEST_T(spsc_queue, rollover, uint8_index_queue_t);

TEST_T(spsc_queue, nonblocking_callback, flow::spsc_queue<int>);

TEST_T(spsc_queue, blocking_callback, flow::spsc_queue<int>);

TEST_T(spsc_queue, exception_callback, flow::spsc_queue<int>);

}  // namespace tests::unit
