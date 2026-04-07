#include <flow/spsc_stream.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates.hpp"

namespace tests {

TEST(spsc_stream, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::spsc_stream<std::size_t, 10>) == 128);
  static_assert(alignof(flow::spsc_stream<std::size_t, 10>) == 64);
  static_assert(sizeof(flow::spsc_stream<std::uint8_t>) == 128);
  static_assert(alignof(flow::spsc_stream<std::uint8_t>) == 64);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(spsc_stream, capacity) {
  EXPECT_EQ((flow::spsc_stream<std::size_t, 128>().capacity()), 128);
  EXPECT_EQ((flow::spsc_stream<std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::spsc_stream<>(128).capacity(), 128);
  EXPECT_EQ(flow::spsc_stream<>(64).capacity(), 64);
}

TEST_T(spsc_stream, minimal_capacity, flow::spsc_stream<>, 1);

TEST_T(spsc_stream, size_capacity, flow::spsc_stream<>);

TEST_T(spsc_stream, nonblocking_smoke, flow::spsc_stream<>);

TEST_T(spsc_stream, blocking_smoke, flow::spsc_stream<>);

TEST_T(spsc_stream, mixed_smoke, flow::spsc_stream<>);

TEST_T(spsc_stream, blocking_push, flow::spsc_stream<>);

TEST_T(spsc_stream, blocking_pop, flow::spsc_stream<>);

TEST_T(spsc_stream, blocking_push_stop_token, flow::spsc_stream<>);

TEST_T(spsc_stream, blocking_pop_stop_token, flow::spsc_stream<>);

using allocator_stream_t =
    flow::spsc_stream<std::size_t, swiss::kDynamicExtent<std::size_t>,
                      swiss::leak_detector_allocator<std::byte, true>>;
TEST_T(unsafe_stream, allocator, allocator_stream_t);

class spsc_queue_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* items_size */, std::size_t /* queue_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(spsc_queue_workload, nonblocking, flow::spsc_stream<>);

TEST_P_T(spsc_queue_workload, blocking, flow::spsc_stream<>);

INSTANTIATE_TEST_SUITE_P(spsc_queue_workload, spsc_queue_workload,
                         ::testing::Values(std::make_tuple(1000, 4, 1, 1),
                                           std::make_tuple(1000, 16, 1, 1),
                                           std::make_tuple(1000, 32, 1, 1),
                                           std::make_tuple(1000, 64, 1, 1),
                                           std::make_tuple(1000, 128, 1, 1)));

using uint8_index_queue_t = flow::spsc_stream<std::uint8_t>;
TEST_T(spsc_stream, rollover, uint8_index_queue_t);

}  // namespace tests
