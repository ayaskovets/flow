#pragma once

namespace flow {

/**
 * @brief No-op backoff policy offering highest CPU usage without any
 * cooperative yields or rescheduling
 */
struct noop_backoff_policy final {
  static constexpr auto backoff_token() noexcept -> int { return {}; }
  static constexpr void backoff(int &) noexcept {}
};

}  // namespace flow
