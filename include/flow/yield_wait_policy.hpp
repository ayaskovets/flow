#pragma once

#include <stop_token>
#include <thread>

namespace flow {

/**
 * @brief Wait policy with does yield on any blocking operation
 */
struct yield_wait_policy final {
  static constexpr void wait_not_full(std::stop_token const &) noexcept {
    std::this_thread::yield();
  }
  static constexpr void wait_not_empty(std::stop_token const &) noexcept {
    std::this_thread::yield();
  }

  static constexpr void notify_not_full() noexcept {}
  static constexpr void notify_not_empty() noexcept {}
};

}  // namespace flow
