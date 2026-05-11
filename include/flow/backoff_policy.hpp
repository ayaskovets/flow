#pragma once

#include <swiss/concepts.hpp>

namespace flow::concepts {

/**
 * @brief Customizable backoff strategy for lock-free containers
 */
template <typename T>
concept backoff_policy = requires(T policy) {
  /**
   * @brief Backoff implementation for relaxing CPU usage in hot loops. Should
   * not block
   */
  { policy.backoff_token() } -> swiss::not_convertible_to<void>;
  requires requires(decltype(policy.backoff_token()) backoff_token) {
    policy.backoff(backoff_token);
  };
};

}  // namespace flow::concepts
