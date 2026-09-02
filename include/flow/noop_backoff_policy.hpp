#pragma once

namespace flow {

/**
 * @brief No-op backoff policy offering highest CPU usage without any
 * cooperative yields or rescheduling
 */
struct noop_backoff_policy final {
  using token_type = bool;

  static constexpr auto backoff_token() noexcept -> token_type { return {}; }
  static constexpr void backoff(bool &) noexcept {}
};

}  // namespace flow
