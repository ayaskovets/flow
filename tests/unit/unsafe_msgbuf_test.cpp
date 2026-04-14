#include <flow/unsafe_msgbuf.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/msgbuf.hpp"
#include "templates/templates.hpp"

namespace tests {

TEST(unsafe_msgbuf, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::unsafe_msgbuf<std::size_t, 10>) == 24);
  static_assert(alignof(flow::unsafe_msgbuf<std::size_t, 10>) == 8);
  static_assert(sizeof(flow::unsafe_msgbuf<std::uint16_t>) == 16);
  static_assert(alignof(flow::unsafe_msgbuf<std::uint16_t>) == 8);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(unsafe_msgbuf, capacity) {
  EXPECT_EQ((flow::unsafe_msgbuf<std::size_t, 128>().capacity()), 128);
  EXPECT_EQ((flow::unsafe_msgbuf<std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::unsafe_msgbuf<>(128).capacity(), 128);
  EXPECT_EQ(flow::unsafe_msgbuf<>(64).capacity(), 64);
}

TEST_T(unsafe_msgbuf, minimal_capacity, flow::unsafe_msgbuf<>, 1);

TEST_T(unsafe_msgbuf, size_capacity, flow::unsafe_msgbuf<>);

TEST_T(unsafe_msgbuf, nonblocking_smoke, flow::unsafe_msgbuf<>);

TEST_T(unsafe_msgbuf, blocking_smoke, flow::unsafe_msgbuf<>);

TEST_T(unsafe_msgbuf, mixed_smoke, flow::unsafe_msgbuf<>);

TEST(unsafe_msgbuf, blocking_push) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, blocking_pop) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, blocking_push_stop_token) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, blocking_pop_stop_token) { GTEST_SKIP(); }

using allocator_msgbuf_t =
    flow::unsafe_msgbuf<std::size_t, swiss::kDynamicExtent<std::size_t>,
                        swiss::leak_detector_allocator<std::byte, true>>;
TEST_T(unsafe_msgbuf, allocator, allocator_msgbuf_t);

TEST(unsafe_msgbuf, nonblocking) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, blocking) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, queued_producers_blocking) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_msgbuf, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_msgbuf_t = flow::unsafe_msgbuf<std::uint8_t>;
TEST_T(unsafe_msgbuf, rollover, uint8_index_msgbuf_t);

}  // namespace tests
