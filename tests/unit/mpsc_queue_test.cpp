#include <flow/mpsc_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(mpsc_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::mpsc_queue<int, std::size_t, 10>) == 128);
  static_assert(alignof(flow::mpsc_queue<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::mpsc_queue<int, std::uint8_t>) == 128);
  static_assert(alignof(flow::mpsc_queue<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(mpsc_queue, capacity) {
  EXPECT_EQ((flow::mpsc_queue<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::mpsc_queue<std::string, std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::mpsc_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::mpsc_queue<std::string>(64).capacity(), 64);
}

TEST_T(mpsc_queue, minimal_capacity, flow::mpsc_queue<int>, 2);

TEST_T(mpsc_queue, size_capacity, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, nonblocking_smoke, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, blocking_smoke, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, mixed_smoke, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, blocking_push, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, blocking_pop, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, blocking_push_stop_token, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, blocking_pop_stop_token, flow::mpsc_queue<int>);

TEST(mpsc_queue, capacity_one) { GTEST_SKIP(); }

TEST_T(mpsc_queue, queue_destructor, flow::mpsc_queue<std::shared_ptr<int>>);

TEST_T(mpsc_queue, item_destructor, flow::mpsc_queue<std::shared_ptr<int>>);

TEST_T(mpsc_queue, non_copyable_item_type,
       flow::mpsc_queue<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::mpsc_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                     swiss::leak_detector_allocator<
                         std::pair<int, std::atomic<std::size_t>>, true>>;
TEST_T(mpsc_queue, allocator, allocator_queue_t);

class mpsc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(mpsc_queue_workload, nonblocking, flow::mpsc_queue<int>);

TEST_P_T(mpsc_queue_workload, blocking, flow::mpsc_queue<int>);

INSTANTIATE_TEST_SUITE_P(mpsc_queue_workload, mpsc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 2, 1),
                                           std::make_tuple(1000, 32, 1, 1),
                                           std::make_tuple(1000, 64, 4, 1),
                                           std::make_tuple(1000, 128, 4, 1)));

TEST_T(mpsc_queue, queued_producers_nonblocking, flow::mpsc_queue<int>);

TEST_T(mpsc_queue, queued_producers_blocking, flow::mpsc_queue<int>);

TEST(mpsc_queue, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(mpsc_queue, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_queue_t = flow::mpsc_queue<int, std::uint8_t>;
TEST_T(mpsc_queue, rollover, uint8_index_queue_t);

}  // namespace tests
