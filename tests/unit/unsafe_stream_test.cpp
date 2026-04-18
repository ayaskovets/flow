#include <flow/unsafe_stream.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/stream.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(unsafe_stream, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::unsafe_stream<std::size_t, 10>) == 24);
  static_assert(alignof(flow::unsafe_stream<std::size_t, 10>) == 8);
  static_assert(sizeof(flow::unsafe_stream<std::uint16_t>) == 16);
  static_assert(alignof(flow::unsafe_stream<std::uint16_t>) == 8);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(unsafe_stream, capacity) {
  EXPECT_EQ((flow::unsafe_stream<std::size_t, 128>().capacity()), 128);
  EXPECT_EQ((flow::unsafe_stream<std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::unsafe_stream<>(128).capacity(), 128);
  EXPECT_EQ(flow::unsafe_stream<>(64).capacity(), 64);
}

TEST_T(unsafe_stream, minimal_capacity, flow::unsafe_stream<>, 1);

TEST_T(unsafe_stream, size_capacity, flow::unsafe_stream<>);

TEST_T(unsafe_stream, nonblocking_smoke, flow::unsafe_stream<>);

TEST_T(unsafe_stream, blocking_smoke, flow::unsafe_stream<>);

TEST_T(unsafe_stream, mixed_smoke, flow::unsafe_stream<>);

TEST(unsafe_stream, blocking_push) { GTEST_SKIP(); }

TEST(unsafe_stream, blocking_pop) { GTEST_SKIP(); }

TEST(unsafe_stream, blocking_push_stop_token) { GTEST_SKIP(); }

TEST(unsafe_stream, blocking_pop_stop_token) { GTEST_SKIP(); }

using allocator_stream_t =
    flow::unsafe_stream<std::size_t, swiss::kDynamicExtent<std::size_t>,
                        swiss::leak_detector_allocator<std::byte, true>>;
TEST_T(unsafe_stream, allocator, allocator_stream_t);

TEST(unsafe_stream, nonblocking) { GTEST_SKIP(); }

TEST(unsafe_stream, blocking) { GTEST_SKIP(); }

TEST(unsafe_stream, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_stream, queued_producers_blocking) { GTEST_SKIP(); }

TEST(unsafe_stream, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_stream, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_stream_t = flow::unsafe_stream<std::uint8_t>;
TEST_T(unsafe_stream, rollover, uint8_index_stream_t);

TEST_T(unsafe_stream, nonblocking_callback, flow::unsafe_stream<>);

TEST_T(unsafe_stream, blocking_callback, flow::unsafe_stream<>);

}  // namespace tests::unit
