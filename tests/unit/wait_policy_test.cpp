#include <flow/noop_wait_policy.hpp>
#include <flow/wait_policy.hpp>
#include <flow/yield_wait_policy.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(wait_policy, noop_wait_policy) {
  static_assert(flow::concepts::wait_policy<flow::noop_wait_policy>);
}

TEST(wait_policy, yield_wait_policy) {
  static_assert(flow::concepts::wait_policy<flow::yield_wait_policy>);
}

}  // namespace tests::unit
