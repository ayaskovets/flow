#pragma once

namespace flow::concepts {

template <typename T>
concept wait_policy = requires(T policy) {
  { policy.wait_not_full() };
  { policy.wait_not_empty() };
  { policy.notify_not_full() };
  { policy.notify_not_empty() };
  { policy.yield_contention() };
};

}  // namespace flow::concepts
