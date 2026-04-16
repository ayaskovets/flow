#include <flow/spsc_stream.hpp>
#include <flow/stream.hpp>
#include <flow/unsafe_stream.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(stream, unsafe_stream) {
  using stream_t = flow::unsafe_stream<>;

  static_assert(flow::concepts::stream<stream_t>);
  static_assert(flow::concepts::nonblocking_stream<stream_t>);
  static_assert(flow::concepts::noexcept_nonblocking_stream<stream_t>);
  static_assert(flow::concepts::blocking_stream<stream_t>);
  static_assert(!flow::concepts::noexcept_blocking_stream<stream_t>);
  static_assert(!flow::concepts::cancellable_blocking_stream<stream_t>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_stream<stream_t>);
}

TEST(stream, spsc_stream) {
  using stream_t = flow::spsc_stream<>;

  static_assert(flow::concepts::stream<stream_t>);
  static_assert(flow::concepts::nonblocking_stream<stream_t>);
  static_assert(flow::concepts::noexcept_nonblocking_stream<stream_t>);
  static_assert(flow::concepts::blocking_stream<stream_t>);
  static_assert(flow::concepts::noexcept_blocking_stream<stream_t>);
  static_assert(flow::concepts::cancellable_blocking_stream<stream_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_stream<stream_t>);
}

}  // namespace tests::unit
