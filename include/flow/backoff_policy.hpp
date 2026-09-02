#pragma once

#include <concepts>

namespace flow::concepts {

/**
 * @brief Customizable backoff strategy for lock-free containers. Implementation
 * should not block
 */
template <typename T>
concept backoff_policy = requires(T policy, T::token_type backoff_token) {
  { policy.backoff_token() } -> std::same_as<typename T::token_type>;
  { policy.backoff(backoff_token) } -> std::same_as<void>;

  requires noexcept(policy.backoff_token());
  requires noexcept(policy.backoff(backoff_token));
};

}  // namespace flow::concepts
