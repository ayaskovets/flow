#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/mixins.hpp>
#include <swiss/variadic_traits.hpp>

namespace flow {

/**
 * @brief Locked ring buffer for many producers and many consumers
 * @note Implemented as a ring buffer
 * @note Provides mostly the same performance as a locked std::deque but with a
 * pre-allocated memory buffer
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class locked_ring_buffer final : public swiss::non_copyable,
                                 public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit locked_ring_buffer(
      allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : ring_buffer_(allocator.allocate(Capacity + 1)),
        capacity_(),
        allocator_(std::move(allocator)) {
    static_assert(
        Capacity > 0,
        "locked_ring_buffer::locked_ring_buffer(): invalid capacity < 1");
  }

  constexpr explicit locked_ring_buffer(
      index_type capacity, allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : ring_buffer_([capacity, &allocator] {
          if (capacity == 0) {
            throw std::invalid_argument(
                "locked_ring_buffer::locked_ring_buffer(): invalid capacity < "
                "1");
          }
          return allocator.allocate(capacity + 1);
        }()),
        capacity_(capacity),
        allocator_(std::move(allocator)) {}

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~locked_ring_buffer() noexcept {
    std::lock_guard const lock(mutex_);

    while (!empty()) {
      std::allocator_traits<allocator_type>::destroy(allocator_,
                                                     ring_buffer_ + pop_from_);
      pop_from_ = next(pop_from_);
    }

    allocator_.deallocate(ring_buffer_, *capacity_ + 1);
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
    return pop_from_ <= push_to_ ? push_to_ - pop_from_
                                 : *capacity_ + push_to_ + 1 - pop_from_;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable_any push_available_cv_;
  index_type push_to_ = 0;
  std::condition_variable_any pop_available_cv_;
  index_type pop_from_ = 0;
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  value_type * const ring_buffer_;
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] allocator_type allocator_;

 private:
  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return next(push_to_) == pop_from_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return push_to_ == pop_from_;
  }

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index == *capacity_ ? 0 : index + 1;
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto emplace_impl(Args &&... args) -> decltype(auto) {
    auto const emplace_and_notify = [this](auto &&... args) {
      if constexpr (Cancellable) {
        swiss::invoke_tail(
            [this](auto &&... tail) {
              std::allocator_traits<allocator_type>::construct(
                  allocator_, ring_buffer_ + push_to_,
                  std::forward<decltype(tail)>(tail)...);
            },
            std::forward<Args>(args)...);
      }

      if constexpr (!Cancellable) {
        std::allocator_traits<allocator_type>::construct(
            allocator_, ring_buffer_ + push_to_, std::forward<Args>(args)...);
      }

      push_to_ = next(push_to_);
      pop_available_cv_.notify_all();
    };

    if constexpr (Blocking) {
      std::unique_lock lock(mutex_);

      if constexpr (Cancellable) {
        std::stop_token const & stop_token =
            swiss::variadic_nth<0>(std::forward<Args>(args)...);
        if (!push_available_cv_.wait(lock, stop_token,
                                     [this] { return !full(); })) {
          return false;
        }
        emplace_and_notify(std::forward<Args>(args)...);
        return true;
      }

      if constexpr (!Cancellable) {
        push_available_cv_.wait(lock, [this] { return !full(); });
        emplace_and_notify(std::forward<Args>(args)...);
      }
    }

    if constexpr (!Blocking) {
      std::unique_lock lock(mutex_, std::defer_lock);
      if (!lock.try_lock() || full()) {
        return false;
      }
      emplace_and_notify(std::forward<Args>(args)...);
      return true;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    auto const pop_and_notify = [this](auto &&... args) {
      value_type & value = swiss::variadic_nth<Cancellable ? 1 : 0>(
          std::forward<decltype(args)>(args)...);
      value = std::move(*(ring_buffer_ + pop_from_));
      std::allocator_traits<allocator_type>::destroy(allocator_,
                                                     ring_buffer_ + pop_from_);

      pop_from_ = next(pop_from_);
      push_available_cv_.notify_all();
    };

    if constexpr (Blocking) {
      std::unique_lock lock(mutex_);

      if constexpr (Cancellable) {
        std::stop_token const & stop_token =
            swiss::variadic_nth<0>(std::forward<Args>(args)...);
        if (!pop_available_cv_.wait(lock, stop_token,
                                    [this] { return !empty(); })) {
          return false;
        }
        pop_and_notify(std::forward<Args>(args)...);
        return true;
      }

      if constexpr (!Cancellable) {
        pop_available_cv_.wait(lock, [this] { return !empty(); });
        pop_and_notify(std::forward<Args>(args)...);
      }
    }

    if constexpr (!Blocking) {
      std::unique_lock lock(mutex_, std::defer_lock);
      if (!lock.try_lock() || empty()) {
        return false;
      }
      pop_and_notify(std::forward<Args>(args)...);
      return true;
    }
  }
};

}  // namespace flow
