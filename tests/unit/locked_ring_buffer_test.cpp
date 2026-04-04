#include <flow/locked_ring_buffer.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(locked_ring_buffer, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::locked_ring_buffer<int, std::size_t, 10>) == 384);
  static_assert(alignof(flow::locked_ring_buffer<int, std::size_t, 10>) == 64);
  static_assert(sizeof(flow::locked_ring_buffer<int, std::uint8_t>) == 384);
  static_assert(alignof(flow::locked_ring_buffer<int, std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(locked_ring_buffer, capacity) {
  EXPECT_EQ(
      (flow::locked_ring_buffer<std::string, std::size_t, 128>().capacity()),
      128);
  EXPECT_EQ(
      (flow::locked_ring_buffer<std::string, std::size_t, 64>().capacity()),
      64);
  EXPECT_EQ(flow::locked_ring_buffer<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::locked_ring_buffer<std::string>(64).capacity(), 64);
}

TEST_T(locked_ring_buffer, minimal_capacity, flow::locked_ring_buffer<int>, 1);

TEST_T(locked_ring_buffer, size_capacity, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, nonblocking_smoke, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, blocking_smoke, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, mixed_smoke, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, blocking_push, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, blocking_pop, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, blocking_push_stop_token,
       flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, blocking_pop_stop_token,
       flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, capacity_one, flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, queue_destructor,
       flow::locked_ring_buffer<std::shared_ptr<int>>);

TEST_T(locked_ring_buffer, item_destructor,
       flow::locked_ring_buffer<std::shared_ptr<int>>);

TEST_T(locked_ring_buffer, non_copyable_item_type,
       flow::locked_ring_buffer<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::locked_ring_buffer<int, std::size_t,
                             swiss::kDynamicExtent<std::size_t>,
                             swiss::leak_detector_allocator<int, true>>;
TEST_T(locked_ring_buffer, allocator, allocator_queue_t);

class locked_ring_buffer_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(locked_ring_buffer_workload, nonblocking,
         flow::locked_ring_buffer<int>);

TEST_P_T(locked_ring_buffer_workload, blocking, flow::locked_ring_buffer<int>);

INSTANTIATE_TEST_SUITE_P(locked_ring_buffer_workload,
                         locked_ring_buffer_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 2, 2),
                                           std::make_tuple(1000, 32, 4, 1),
                                           std::make_tuple(1000, 64, 1, 4),
                                           std::make_tuple(1000, 128, 4, 4)));

TEST_T(locked_ring_buffer, queued_producers_nonblocking,
       flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, queued_producers_blocking,
       flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, queued_consumers_nonblocking,
       flow::locked_ring_buffer<int>);

TEST_T(locked_ring_buffer, queued_consumers_blocking,
       flow::locked_ring_buffer<int>);

using uint8_index_queue_t = flow::locked_ring_buffer<int, std::uint8_t>;
TEST_T(locked_ring_buffer, rollover, uint8_index_queue_t);

}  // namespace tests
