#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>

#include <swiss/aligned.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/manual_lifetime.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/scope_exit.hpp>

namespace flow {

/**
 * @brief Locked ring buffer for many producers and many consumers
 * @note Implemented as a ring buffer
 * @note Serves as a naive baseline implementation of an MPMC queue
 * @note Capacity must be a power of 2 for efficient division
 * @warning Uses standard locking privitives (i.e. std::mutex and
 * std::condition_variable) and thus not safe to use in any coroutine
 * environment
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          Index Alignment    = alignof(swiss::manual_lifetime<Value>),
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class locked_ring_buffer : public swiss::non_copyable,
                           public swiss::non_movable {
 private:
  template <typename T>
  static constexpr bool is_callback = std::invocable<T, Value *>;

 private:
  using entry_type         = swiss::manual_lifetime<Value>;
  using aligned_entry_type = swiss::aligned<entry_type, Alignment>;

 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<aligned_entry_type>;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return swiss::is_power_of_two(capacity);
  }

  constexpr void initialize() noexcept {
    for (index_type index = 0; index < *capacity_; ++index) {
      std::allocator_traits<allocator_type>::construct(allocator_,
                                                       aligned_at(index));
    }
  }

  constexpr void deinitialize() noexcept {
    for (index_type index = 0; index < *capacity_; ++index) {
      std::allocator_traits<allocator_type>::destroy(allocator_,
                                                     aligned_at(index));
    }
  }

 public:
  explicit constexpr locked_ring_buffer(
      allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(valid(Capacity),
                  "locked_ring_buffer::locked_ring_buffer(): invalid capacity");
    initialize();
  }

  explicit constexpr locked_ring_buffer(
      index_type capacity, allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([&] -> aligned_entry_type * {
          if (!valid(capacity)) {
            throw std::invalid_argument(
                "locked_ring_buffer::locked_ring_buffer(): invalid capacity");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {
    initialize();
  }

  constexpr ~locked_ring_buffer() noexcept {
    clear();
    deinitialize();
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) -> bool {
    return try_push([&](value_type * front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type const & value) -> bool {
    return try_emplace<value_type const &>(value);
  }

  constexpr auto try_push(value_type && value) -> bool {
    return try_emplace<value_type &&>(std::move(value));
  }

  constexpr auto try_pop(auto & value) -> bool {
    return try_pop([&](value_type * front) -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(at(pop_from_)); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto emplace(std::stop_token const & stop_token, Args &&... args)
      -> bool {
    return push(stop_token, [&](value_type * front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type const & value) -> bool {
    return emplace(stop_token, value);
  }

  constexpr auto push(std::stop_token const & stop_token, value_type && value)
      -> bool {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token, auto & value) -> bool {
    return pop(stop_token, [&](value_type * front) -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(at(pop_from_)); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr void emplace(Args &&... args) {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type const & value) {
    push(std::stop_token{}, value);
  }

  constexpr void push(value_type && value) {
    push(std::stop_token{}, std::move(value));
  }

  constexpr void pop(auto & value) { pop(std::stop_token{}, value); }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const -> std::size_t {
    std::lock_guard const lock(mutex_);

    return size(push_to_, pop_from_);
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    std::lock_guard const lock(mutex_);

    return empty(push_to_, pop_from_);
  }

  constexpr void clear() noexcept {
    std::lock_guard const lock(mutex_);

    while (!empty(push_to_, pop_from_)) {
      std::destroy_at(at(pop_from_));
      ++pop_from_;
    }
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

 private:
  [[no_unique_address]] allocator_type allocator_;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  aligned_entry_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  mutable std::mutex mutex_;

  std::condition_variable_any pop_available_cv_;
  index_type push_to_ = 0;
  std::condition_variable_any push_available_cv_;
  index_type pop_from_ = 0;

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return *capacity_ == size(push_to_, pop_from_);
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  [[nodiscard]] constexpr auto aligned_at(index_type index) const noexcept
      -> aligned_entry_type * {
    index_type const slot = index & static_cast<index_type>(*capacity_ - 1);
    return ring_buffer_ + slot;
  }

  [[nodiscard]] constexpr auto at(index_type index) const noexcept
      -> value_type * {
    return &***aligned_at(index);
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(Callback && callback) -> bool {
    std::lock_guard const lock(mutex_);
    if (full()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), at(push_to_));

    ++push_to_;
    pop_available_cv_.notify_one();
    return true;
  }

  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_pop(Callback && callback) -> bool {
    std::lock_guard const lock(mutex_);
    if (empty(push_to_, pop_from_)) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), at(pop_from_));

    ++pop_from_;
    push_available_cv_.notify_one();
    return true;
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token, Callback && callback)
      -> bool {
    std::unique_lock lock(mutex_);
    if (!push_available_cv_.wait(lock, stop_token,
                                 [this] noexcept -> bool { return !full(); })) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), at(push_to_));

    ++push_to_;
    pop_available_cv_.notify_one();
    return true;
  }

  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token, Callback && callback)
      -> bool {
    std::unique_lock lock(mutex_);
    if (!pop_available_cv_.wait(lock, stop_token, [this] noexcept -> bool {
          return !empty(push_to_, pop_from_);
        })) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), at(pop_from_));

    ++pop_from_;
    push_available_cv_.notify_one();
    return true;
  }
};

}  // namespace flow
