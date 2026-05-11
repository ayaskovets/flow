#pragma once

#include <stop_token>
#include <thread>

#include <swiss/cpu_relax.hpp>

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

  static constexpr auto backoff_token() noexcept -> int { return {}; }
  static constexpr void backoff(int &) noexcept {}
};

}  // namespace flow
