#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

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
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, *capacity_)) {
    static_assert(swiss::is_power_of_two(Capacity),
                  "mpmc_queue::mpmc_queue(): invalid capacity != 2^n");
  }

  constexpr explicit locked_ring_buffer(
      index_type capacity, allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([this, capacity] {
          if (!swiss::is_power_of_two(capacity)) {
            throw std::invalid_argument(
                "mpmc_queue::mpmc_queue(): invalid capacity != 2^n");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 *capacity_);
        }()) {}

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~locked_ring_buffer() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    while (try_pop([this](value_type & value) noexcept {
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    })) {
    }

    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto try_push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    std::unique_lock lock(mutex_, std::defer_lock);
    if (!lock.try_lock() || full()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback),
                *(ring_buffer_ + slot(push_to_)));

    push_to_ = next(push_to_);
    pop_available_cv_.notify_all();
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto try_pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    std::unique_lock lock(mutex_, std::defer_lock);
    if (!lock.try_lock() || empty()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback),
                *(ring_buffer_ + slot(pop_from_)));

    pop_from_ = next(pop_from_);
    push_available_cv_.notify_all();
    return true;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto
  push(std::stop_token const & stop_token, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    std::unique_lock lock(mutex_);
    if (!push_available_cv_.wait(lock, stop_token,
                                 [this] { return !full(); })) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback),
                *(ring_buffer_ + slot(push_to_)));

    push_to_ = next(push_to_);
    pop_available_cv_.notify_all();
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto
  pop(std::stop_token const & stop_token, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    std::unique_lock lock(mutex_);
    if (!pop_available_cv_.wait(lock, stop_token,
                                [this] { return !empty(); })) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback),
                *(ring_buffer_ + slot(pop_from_)));

    pop_from_ = next(pop_from_);
    push_available_cv_.notify_all();
    return true;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr void push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) {
    push(std::stop_token{}, std::forward<Callback>(callback));
  }

  template <std::invocable<value_type &> Callback>
  constexpr void pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) {
    pop(std::stop_token{}, std::forward<Callback>(callback));
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([&](value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return try_pop([this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return push(stop_token, [&](value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token stop_token, value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return pop(stop_token, [this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
  }

 public:
  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) {
    push(std::stop_token{}, std::move(value));
  }

  constexpr void pop(value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) {
    pop(std::stop_token{}, value);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    std::lock_guard const lock(mutex_);

    return size(push_to_, pop_from_);
  }

  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  [[no_unique_address]] allocator_type allocator_;
  mutable std::mutex mutex_;
  std::condition_variable_any push_available_cv_;
  index_type push_to_ = 0;
  std::condition_variable_any pop_available_cv_;
  index_type pop_from_ = 0;
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  value_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return *capacity_ == size(push_to_, pop_from_);
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return push_to_ == pop_from_;
  }

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

  [[nodiscard]] constexpr auto slot(index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(*capacity_ - 1);
  }
};

}  // namespace flow
