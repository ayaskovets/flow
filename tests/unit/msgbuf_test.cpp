#include <flow/msgbuf.hpp>
#include <flow/spsc_msgbuf.hpp>
#include <flow/unsafe_msgbuf.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(msgbuf, unsafe_msgbuf) {
  using msgbuf_t = flow::unsafe_msgbuf<>;

  static_assert(flow::concepts::msgbuf<msgbuf_t>);
  static_assert(flow::concepts::nonblocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::noexcept_nonblocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::blocking_msgbuf<msgbuf_t>);
  static_assert(!flow::concepts::noexcept_blocking_msgbuf<msgbuf_t>);
  static_assert(!flow::concepts::cancellable_blocking_msgbuf<msgbuf_t>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_msgbuf<msgbuf_t>);
}

TEST(msgbuf, spsc_msgbuf) {
  using msgbuf_t = flow::spsc_msgbuf<>;

  static_assert(flow::concepts::msgbuf<msgbuf_t>);
  static_assert(flow::concepts::nonblocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::noexcept_nonblocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::blocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::noexcept_blocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::cancellable_blocking_msgbuf<msgbuf_t>);
  static_assert(flow::concepts::noexcept_cancellable_blocking_msgbuf<msgbuf_t>);
}

}  // namespace tests::unit
