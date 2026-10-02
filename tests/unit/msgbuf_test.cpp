#include <flow/mpsc_msgbuf.hpp>
#include <flow/msgbuf.hpp>
#include <flow/spsc_msgbuf.hpp>
#include <flow/unsafe_msgbuf.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(msgbuf, mpsc_msgbuf) {
  using msgbuf_type = flow::mpsc_msgbuf<>;

  static_assert(flow::concepts::msgbuf<msgbuf_type>);
  static_assert(flow::concepts::nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::noexcept_nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::blocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::noexcept_blocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::cancellable_blocking_msgbuf<msgbuf_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_msgbuf<msgbuf_type>);
}

TEST(msgbuf, spsc_msgbuf) {
  using msgbuf_type = flow::spsc_msgbuf<>;

  static_assert(flow::concepts::msgbuf<msgbuf_type>);
  static_assert(flow::concepts::nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::noexcept_nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::blocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::noexcept_blocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::cancellable_blocking_msgbuf<msgbuf_type>);
  static_assert(
      flow::concepts::noexcept_cancellable_blocking_msgbuf<msgbuf_type>);
}

TEST(msgbuf, unsafe_msgbuf) {
  using msgbuf_type = flow::unsafe_msgbuf<>;

  static_assert(flow::concepts::msgbuf<msgbuf_type>);
  static_assert(flow::concepts::nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::noexcept_nonblocking_msgbuf<msgbuf_type>);
  static_assert(flow::concepts::blocking_msgbuf<msgbuf_type>);
  static_assert(!flow::concepts::noexcept_blocking_msgbuf<msgbuf_type>);
  static_assert(!flow::concepts::cancellable_blocking_msgbuf<msgbuf_type>);
  static_assert(
      !flow::concepts::noexcept_cancellable_blocking_msgbuf<msgbuf_type>);
}

}  // namespace tests::unit
