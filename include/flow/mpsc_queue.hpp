#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>

#include <swiss/aligned.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/manual_lifetime.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/scope_fail.hpp>

#include "backoff_policy.hpp"
#include "noop_backoff_policy.hpp"
#include "noop_wait_policy.hpp"
#include "wait_policy.hpp"

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 consumer and many producers
 * @note Implemented as a ring buffer with sequence numbers
 * @note Capacity must be a power of 2 for efficient division
 * @note Minimal capacity is 2
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity  = swiss::kDynamicExtent<Index>,
          Index Alignment = alignof(
              std::pair<swiss::manual_lifetime<Value>, std::atomic<Index>>),
          typename Allocator                     = std::allocator<Value>,
          concepts::wait_policy WaitPolicy       = noop_wait_policy,
          concepts::backoff_policy BackoffPolicy = noop_backoff_policy>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpsc_queue : public swiss::non_copyable, public swiss::non_movable {
 private:
  /**
   * @note It is impossible to implement an exception safe constructor without
   * value being nothrow destructible
   */
  static_assert(std::is_nothrow_destructible_v<Value>);

 private:
  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, Value *>;

 private:
  using entry_type =
      std::pair<swiss::manual_lifetime<Value>, std::atomic<Index>>;
  using aligned_entry_type = swiss::aligned<entry_type, Alignment>;

 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<aligned_entry_type>;
  using wait_policy_type    = WaitPolicy;
  using backoff_policy_type = BackoffPolicy;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return swiss::is_power_of_two_gt(capacity, static_cast<index_type>(1));
  }

  constexpr void initialize() {
    index_type constructed = 0;

    swiss::scope_fail const scope_fail([&]() noexcept {
      for (index_type index = 0; index < constructed; ++index) {
        std::allocator_traits<allocator_type>::destroy(allocator_,
                                                       aligned_at(constructed));
      }
      std::allocator_traits<allocator_type>::deallocate(
          allocator_, ring_buffer_, *capacity_);
    });

    for (; constructed < *capacity_; ++constructed) {
      std::allocator_traits<allocator_type>::construct(allocator_,
                                                       aligned_at(constructed));

      seqnum(at(constructed)).store(constructed, std::memory_order::relaxed);
    }
  }

 public:
  explicit constexpr mpsc_queue(
      allocator_type allocator           = allocator_type(),
      wait_policy_type wait_policy       = wait_policy_type(),
      backoff_policy_type backoff_policy = backoff_policy_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        backoff_policy_(std::move(backoff_policy)),
        capacity_(),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(valid(Capacity),
                  "mpsc_queue::mpsc_queue(): invalid capacity");
    initialize();
  }

  explicit constexpr mpsc_queue(
      index_type capacity, allocator_type allocator = allocator_type(),
      wait_policy_type wait_policy       = wait_policy_type(),
      backoff_policy_type backoff_policy = backoff_policy_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        backoff_policy_(std::move(backoff_policy)),
        capacity_(capacity),
        ring_buffer_([&] {
          if (!valid(capacity)) {
            throw std::invalid_argument(
                "mpsc_queue::mpsc_queue(): invalid capacity");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {
    initialize();
  }

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~mpsc_queue() noexcept {
    while (
        try_pop([&](value_type * front) noexcept { std::destroy_at(front); })) {
    }

    for (index_type index = 0; index < *capacity_; ++index) {
      std::allocator_traits<allocator_type>::destroy(allocator_,
                                                     aligned_at(index));
    }

    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <typename... Args>
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) noexcept -> bool {
    return try_push([&](value_type * front) noexcept {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type const & value) noexcept -> bool
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    return try_emplace(value);
  }

  constexpr auto try_push(value_type && value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(auto & value) noexcept -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type>)
  {
    return try_pop([&](value_type * front) noexcept {
      value = std::move(*front);
      std::destroy_at(front);
    });
  }

 public:
  template <typename... Args>
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  constexpr auto emplace(std::stop_token const & stop_token,
                         Args &&... args) noexcept -> bool {
    return push(stop_token, [&](value_type * front) noexcept {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type const & value) noexcept -> bool
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    return emplace(stop_token, value);
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type && value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token, auto & value) noexcept
      -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type>)
  {
    return pop(stop_token, [&](value_type * front) noexcept {
      value = std::move(*front);
      std::destroy_at(front);
    });
  }

 public:
  template <typename... Args>
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  constexpr void emplace(Args &&... args) noexcept {
    return push([&](value_type * front) noexcept {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr void push(value_type const & value) noexcept
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    emplace(value);
  }

  constexpr void push(value_type && value) noexcept
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    emplace(std::move(value));
  }

  constexpr void pop(auto & value) noexcept
    requires(std::is_nothrow_assignable_v<decltype(value), value_type>)
  {
    return pop([&](value_type * front) noexcept {
      value = std::move(*front);
      std::destroy_at(front);
    });
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);
    index_type const push_to  = push_to_.load(std::memory_order::relaxed);

    return size(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size() == 0;
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

  [[nodiscard]] constexpr auto get_wait_policy() const
      noexcept(std::is_nothrow_copy_constructible_v<wait_policy_type>)
          -> wait_policy_type {
    return wait_policy_;
  }

 private:
  [[no_unique_address]] allocator_type allocator_;
  [[no_unique_address]] wait_policy_type wait_policy_;
  [[no_unique_address]] backoff_policy_type backoff_policy_;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  aligned_entry_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> push_to_ = 0;
  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> pop_from_ = 0;

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto vacant(index_type push_to,
                                      index_type seqnum) const noexcept
      -> bool {
    return push_to == seqnum;
  }

  [[nodiscard]] constexpr auto occupied(index_type pop_from,
                                        index_type seqnum) const noexcept
      -> bool {
    return static_cast<index_type>(pop_from + 1) == seqnum;
  }

  [[nodiscard]] constexpr auto aligned_at(index_type index) const noexcept
      -> aligned_entry_type * {
    index_type const slot = index & static_cast<index_type>(*capacity_ - 1);
    return ring_buffer_ + slot;
  }

  [[nodiscard]] constexpr auto at(index_type index) const noexcept
      -> entry_type * {
    return &**aligned_at(index);
  }

  [[nodiscard]] constexpr auto value(entry_type * entry) const noexcept
      -> value_type * {
    return &*entry->first;
  }

  [[nodiscard]] constexpr auto seqnum(entry_type * entry) const noexcept
      -> std::atomic<index_type> & {
    return entry->second;
  }

 private:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_push(Callback && callback) noexcept -> bool {
    index_type push_to = push_to_.load(std::memory_order::relaxed);

    auto backoff_token = backoff_policy_.backoff_token();
    while (true) {
      entry_type * const entry = at(push_to);

      if (vacant(push_to, seqnum(entry).load(std::memory_order::acquire))) {
        if (push_to_.compare_exchange_weak(push_to, push_to + 1,
                                           std::memory_order::relaxed,
                                           std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry).store(push_to + 1, std::memory_order::release);
          wait_policy_.notify_not_empty();
          return true;
        }
        backoff_policy_.backoff(backoff_token);
      } else {
        index_type const actual_push_to =
            push_to_.load(std::memory_order::relaxed);
        if (push_to == actual_push_to) {
          return false;
        }
        backoff_policy_.backoff(backoff_token);
        push_to = actual_push_to;
      }
    }
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> bool {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);
    entry_type * const entry  = at(pop_from);

    if (!occupied(pop_from, seqnum(entry).load(std::memory_order::acquire))) {
      return false;
    }
    pop_from_.store(pop_from + 1, std::memory_order::relaxed);

    std::invoke(std::forward<Callback>(callback), value(entry));

    seqnum(entry).store(pop_from + *capacity_, std::memory_order::release);
    wait_policy_.notify_not_full();
    return true;
  }

 private:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token,
                      Callback && callback) noexcept -> bool {
    index_type push_to = push_to_.load(std::memory_order::relaxed);

    auto backoff_token = backoff_policy_.backoff_token();
    while (!stop_token.stop_requested()) {
      entry_type * const entry = at(push_to);

      if (vacant(push_to, seqnum(entry).load(std::memory_order::acquire))) {
        if (push_to_.compare_exchange_weak(push_to, push_to + 1,
                                           std::memory_order::relaxed,
                                           std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry).store(push_to + 1, std::memory_order::release);
          wait_policy_.notify_not_empty();
          return true;
        }
        backoff_policy_.backoff(backoff_token);
      } else {
        wait_policy_.wait_not_full(stop_token);
        push_to = push_to_.load(std::memory_order::relaxed);
      }
    }

    return false;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      entry_type * const entry = at(pop_from);

      if (occupied(pop_from, seqnum(entry).load(std::memory_order::acquire))) {
        pop_from_.store(pop_from + 1, std::memory_order::relaxed);
        std::invoke(std::forward<Callback>(callback), value(entry));

        seqnum(entry).store(pop_from + *capacity_, std::memory_order::release);
        wait_policy_.notify_not_full();
        return true;
      }

      wait_policy_.wait_not_empty(stop_token);
    }

    return false;
  }

 private:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr void push(Callback && callback) noexcept {
    index_type const push_to =
        push_to_.fetch_add(1, std::memory_order::relaxed);
    entry_type * const entry = at(push_to);

    while (!vacant(push_to, seqnum(entry).load(std::memory_order::acquire))) {
      wait_policy_.wait_not_full(std::stop_token{});
    }

    std::invoke(std::forward<Callback>(callback), value(entry));

    seqnum(entry).store(push_to + 1, std::memory_order::release);
    wait_policy_.notify_not_empty();
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr void pop(Callback && callback) noexcept {
    index_type const pop_from =
        pop_from_.fetch_add(1, std::memory_order::relaxed);
    entry_type * const entry = at(pop_from);

    while (
        !occupied(pop_from, seqnum(entry).load(std::memory_order::acquire))) {
      wait_policy_.wait_not_empty(std::stop_token{});
    }

    std::invoke(std::forward<Callback>(callback), value(entry));

    seqnum(entry).store(pop_from + *capacity_, std::memory_order::release);
    wait_policy_.notify_not_full();
  }
};

}  // namespace flow
