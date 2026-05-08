#pragma once

#include <thread>

#include <swiss/cpu_relax.hpp>

namespace flow {

struct yield_wait_policy final {
  static constexpr void wait_not_full() noexcept { std::this_thread::yield(); }

  static constexpr void wait_not_empty() noexcept { std::this_thread::yield(); }

  static constexpr void notify_not_full() noexcept {}

  static constexpr void notify_not_empty() noexcept {}

  static constexpr void yield_contention() noexcept {}
};

}  // namespace flow
