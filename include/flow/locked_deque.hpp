#pragma once

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/variadic_traits.hpp>

namespace flow {

/**
 * @brief Locked std::deque
 * @note Serves as a naive baseline implementation of an MPMC queue
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class locked_deque final : public swiss::non_copyable,
                           public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit locked_deque(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : deque_(allocator), capacity_() {
    static_assert(Capacity > 0,
                  "locked_deque::locked_deque(): invalid capacity < 1");
  }

  constexpr explicit locked_deque(index_type capacity,
                                  allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : deque_(allocator), capacity_(capacity) {
    if (capacity < 1) [[unlikely]] {
      throw std::invalid_argument(
          "locked_deque::locked_deque(): invalid capacity < 1");
    }
  }

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~locked_deque() noexcept {
    std::lock_guard const lock(mutex_);
    deque_.clear();
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return emplace_impl<false, false>(std::forward<Args>(args)...);
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace_impl<false, false>(std::move(value));
  }

  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return emplace_impl<true, true>(stop_token, std::forward<Args>(args)...);
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace_impl<true, true>(stop_token, std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) {
    emplace_impl<true, false>(std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) {
    emplace_impl<true, false>(std::move(value));
  }

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop_impl<false, false>(value);
  }

  constexpr auto pop(std::stop_token stop_token, value_type & value) noexcept
      -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop_impl<true, true>(stop_token, value);
  }

  constexpr void pop(value_type & value) noexcept
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    pop_impl<true, false>(value);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    std::lock_guard const lock(mutex_);
    return deque_.size();
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return deque_.get_allocator();
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable_any pop_available_cv_;
  std::condition_variable_any push_available_cv_;
  std::deque<value_type, allocator_type> deque_;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto full() const noexcept {
    return deque_.size() == *capacity_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return deque_.empty();
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto emplace_impl(Args &&... args) -> decltype(auto) {
    std::unique_lock lock(mutex_, std::defer_lock);
    if constexpr (Blocking) {
      lock.lock();

      if constexpr (Cancellable) {
        std::stop_token const & stop_token =
            swiss::variadic_nth<0>(std::forward<Args>(args)...);
        if (!push_available_cv_.wait(lock, stop_token,
                                     [this]() { return !full(); })) {
          return false;
        }
      } else {
        push_available_cv_.wait(lock, [this]() { return !full(); });
      }
    } else {
      if (!lock.try_lock() || full()) {
        return false;
      }
    }

    if constexpr (Cancellable && sizeof...(Args) > 0) {
      [this](auto &&, auto &&... tail) {
        deque_.emplace_back(std::forward<decltype(tail)>(tail)...);
      }(std::forward<Args>(args)...);
    } else {
      deque_.emplace_back(std::forward<decltype(args)>(args)...);
    }

    pop_available_cv_.notify_all();
    if constexpr (!Blocking || Cancellable) {
      return true;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    std::unique_lock lock(mutex_, std::defer_lock);

    if constexpr (Blocking) {
      lock.lock();

      if constexpr (Cancellable) {
        std::stop_token const & stop_token =
            swiss::variadic_nth<0>(std::forward<Args>(args)...);
        if (!pop_available_cv_.wait(lock, stop_token,
                                    [this]() { return !empty(); })) {
          return false;
        }
      } else {
        pop_available_cv_.wait(lock, [this]() { return !empty(); });
      }
    } else {
      if (!lock.try_lock() || empty()) {
        return false;
      }
    }

    /**
     * @warning This code exception safety is totally dependent ot pop_front()
     * being noexcept
     */
    auto & value =
        swiss::variadic_nth<Cancellable ? 1 : 0>(std::forward<Args>(args)...);
    value = std::move(deque_.front());
    deque_.pop_front();

    push_available_cv_.notify_all();
    return true;
  }
};

}  // namespace flow
