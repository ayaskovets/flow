#include <flow/backoff_policy.hpp>
#include <flow/exponential_backoff_policy.hpp>
#include <flow/noop_backoff_policy.hpp>

#include <gtest/gtest.h>

namespace tests::unit {

TEST(backoff_policy, noop_wait_policy) {
  static_assert(flow::concepts::backoff_policy<flow::noop_backoff_policy>);
}

TEST(backoff_policy, exponential_backoff_policy) {
  static_assert(
      flow::concepts::backoff_policy<flow::exponential_backoff_policy<>>);
}

}  // namespace tests::unit
