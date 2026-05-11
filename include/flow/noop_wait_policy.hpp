#pragma once

#include <stop_token>

namespace flow {

/**
 * @brief No-op wait policy offering highest CPU usage without any cooperative
 * yields or rescheduling
 */
struct noop_wait_policy final {
  static constexpr void wait_not_full(std::stop_token const &) noexcept {}
  static constexpr void wait_not_empty(std::stop_token const &) noexcept {}

  static constexpr void notify_not_full() noexcept {}
  static constexpr void notify_not_empty() noexcept {}
};

}  // namespace flow
