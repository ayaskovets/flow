#include <flow/mpsc_msgbuf.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/msgbuf.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(mpsc_msgbuf, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::mpsc_msgbuf<std::size_t, 10>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::mpsc_msgbuf<std::size_t, 10>) ==
                std::hardware_destructive_interference_size);
  static_assert(sizeof(flow::mpsc_msgbuf<std::uint8_t>) ==
                std::hardware_destructive_interference_size * 3);
  static_assert(alignof(flow::mpsc_msgbuf<std::uint8_t>) ==
                std::hardware_destructive_interference_size);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(mpsc_msgbuf, capacity) {
  EXPECT_EQ((flow::mpsc_msgbuf<std::size_t, 128>().capacity()), 128);
  EXPECT_EQ((flow::mpsc_msgbuf<std::size_t, 64>().capacity()), 64);
  EXPECT_EQ(flow::mpsc_msgbuf<>(128).capacity(), 128);
  EXPECT_EQ(flow::mpsc_msgbuf<>(64).capacity(), 64);
}

TEST_T(mpsc_msgbuf, minimal_capacity, flow::mpsc_msgbuf<>, 16);

TEST_T(mpsc_msgbuf, size_capacity, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, nonblocking_smoke, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_smoke, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, mixed_smoke, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_push, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_pop, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, stop_token_noop, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_push_stop_token, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_pop_stop_token, flow::mpsc_msgbuf<std::uint32_t>);

using allocator_msgbuf_type =
    flow::mpsc_msgbuf<std::uint32_t, swiss::kDynamicExtent<std::uint32_t>,
                      swiss::leak_detector_allocator<std::uint32_t, true>>;
TEST_T(mpsc_msgbuf, allocator, allocator_msgbuf_type);

class mpsc_msgbuf_workload
    : public ::testing::TestWithParam<std::tuple<
          std::size_t /* msg_count */, std::size_t /* msgbuf_capacity */,
          std::size_t /* producers */, std::size_t /* consumers */>> {};

TEST_P_T(mpsc_msgbuf_workload, nonblocking, flow::mpsc_msgbuf<std::uint32_t>);

TEST_P_T(mpsc_msgbuf_workload, blocking, flow::mpsc_msgbuf<std::uint32_t>);

INSTANTIATE_TEST_SUITE_P(mpsc_msgbuf_workload, mpsc_msgbuf_workload,
                         ::testing::Values(std::make_tuple(1000, 16, 1, 1),
                                           std::make_tuple(1000, 32, 2, 1),
                                           std::make_tuple(1000, 64, 2, 1),
                                           std::make_tuple(1000, 128, 4, 1)));

TEST_T(mpsc_msgbuf, rollover, flow::mpsc_msgbuf<std::uint8_t>);

TEST_T(mpsc_msgbuf, nonblocking_callback, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, blocking_callback, flow::mpsc_msgbuf<std::uint32_t>);

TEST_T(mpsc_msgbuf, padding, flow::mpsc_msgbuf<std::uint32_t>);

TEST(mpsc_msgbuf, exception_callback) { GTEST_SKIP(); }

TEST_T(mpsc_msgbuf, input_validation, flow::mpsc_msgbuf<>);

}  // namespace tests::unit
