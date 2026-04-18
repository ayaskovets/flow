#include <flow/unsafe_queue.hpp>

#include <gtest/gtest.h>
#include <swiss/leak_detector_allocator.hpp>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace tests::unit {

TEST(unsafe_queue, size) {
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
  static_assert(sizeof(flow::unsafe_queue<int, std::size_t, 10>) == 24);
  static_assert(alignof(flow::unsafe_queue<int, std::size_t, 10>) == 8);
  static_assert(sizeof(flow::unsafe_queue<int>) == 32);
  static_assert(alignof(flow::unsafe_queue<int>) == 8);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}

TEST(unsafe_queue, capacity) {
  EXPECT_EQ((flow::unsafe_queue<std::string, std::size_t, 128>().capacity()),
            128);
  EXPECT_EQ((flow::unsafe_queue<std::string, std::size_t, 64>().capacity()),
            64);
  EXPECT_EQ(flow::unsafe_queue<std::string>(128).capacity(), 128);
  EXPECT_EQ(flow::unsafe_queue<std::string>(64).capacity(), 64);
}

TEST_T(unsafe_queue, minimal_capacity, flow::unsafe_queue<int>, 1);

TEST_T(unsafe_queue, size_capacity, flow::unsafe_queue<int>);

TEST_T(unsafe_queue, nonblocking_smoke, flow::unsafe_queue<int>);

TEST_T(unsafe_queue, blocking_smoke, flow::unsafe_queue<int>);

TEST_T(unsafe_queue, mixed_smoke, flow::unsafe_queue<int>);

TEST(unsafe_queue, blocking_push) { GTEST_SKIP(); }

TEST(unsafe_queue, blocking_pop) { GTEST_SKIP(); }

TEST(unsafe_queue, blocking_push_stop_token) { GTEST_SKIP(); }

TEST(unsafe_queue, blocking_pop_stop_token) { GTEST_SKIP(); }

TEST_T(unsafe_queue, capacity_one, flow::unsafe_queue<int>);

TEST_T(unsafe_queue, queue_destructor,
       flow::unsafe_queue<std::shared_ptr<int>>);

TEST_T(unsafe_queue, item_destructor, flow::unsafe_queue<std::shared_ptr<int>>);

TEST_T(unsafe_queue, non_copyable_item_type,
       flow::unsafe_queue<std::unique_ptr<int>>);

using allocator_queue_t =
    flow::unsafe_queue<int, std::size_t, swiss::kDynamicExtent<std::size_t>,
                       swiss::leak_detector_allocator<int, true>>;
TEST_T(unsafe_queue, allocator, allocator_queue_t);

TEST(unsafe_queue, nonblocking) { GTEST_SKIP(); }

TEST(unsafe_queue, blocking) { GTEST_SKIP(); }

TEST(unsafe_queue, queued_producers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_queue, queued_producers_blocking) { GTEST_SKIP(); }

TEST(unsafe_queue, queued_consumers_nonblocking) { GTEST_SKIP(); }

TEST(unsafe_queue, queued_consumers_blocking) { GTEST_SKIP(); }

using uint8_index_queue_t = flow::unsafe_queue<int, std::uint8_t>;
TEST_T(unsafe_queue, rollover, uint8_index_queue_t);

TEST_T(unsafe_queue, nonblocking_callback, flow::unsafe_queue<int>);

TEST_T(unsafe_queue, blocking_callback, flow::unsafe_queue<int>);

}  // namespace tests::unit
