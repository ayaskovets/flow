#pragma once

#include <cstdint>
#include <thread>

#include <swiss/cpu_relax.hpp>
#include <swiss/predicates.hpp>

namespace flow {

/**
 * @brief Exponential backoff policy with random jitter
 */
// NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
template <std::uint16_t MaxSpins = 128, std::uint16_t MinSpins = 8>
  requires(swiss::is_power_of_two(MaxSpins) && swiss::is_power_of_two(MinSpins))
struct exponential_backoff_policy {
  struct token_type final {
    std::uint16_t max_spins;
    std::uint16_t spins;
  };

  static constexpr auto backoff_token() noexcept -> token_type {
    return {.max_spins = MaxSpins, .spins = 0};
  }

  static constexpr void backoff(struct token_type & token) noexcept {
    if (token.spins < token.max_spins) [[likely]] {
      swiss::cpu_relax();
    } else if (token.spins == token.max_spins) {
      std::this_thread::yield();

      token = {
          .max_spins = std::max<std::uint16_t>(token.max_spins / 2, MinSpins),
          .spins     = 0,
      };
    }

    ++token.spins;
  }
};

}  // namespace flow
