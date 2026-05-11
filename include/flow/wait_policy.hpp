#pragma once

#include <stop_token>

namespace flow::concepts {

/**
 * @brief Customizable yield strategy for lock-free containers
 */
template <typename T>
concept wait_policy = requires(T policy, std::stop_token stop_token) {
  /**
   * @brief Called in blocking methods when the operation can possibly be
   * blocking due to container being full/empty (or not having enough space)
   * @warning In a MP or MC scenario even though these functions are invoked
   * immediately after the full/empty check the container could not be
   * full/empty at the monent of invocation
   * @warning Container operations could be stopped externally via the
   * stop_token so a blocking implementation should respect that
   */
  { policy.wait_not_full(stop_token) };
  { policy.wait_not_empty(stop_token) };

  /**
   * @brief Called after a successful push and pop
   */
  { policy.notify_not_empty() };
  { policy.notify_not_full() };
};

}  // namespace flow::concepts
