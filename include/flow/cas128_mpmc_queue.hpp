#pragma once

#include <atomic>
#include <concepts>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <type_traits>

#include <swiss/aligned.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/trivial_pair.hpp>

namespace flow {

/**
 * @brief Threadsafe queue for many producers and many consumers
 * @note Implementation requires both value and index be trivally copyable and a
 * lockfree CAS operation for {value, index} pair. For std::size_t index this
 * pretty much means 128-bit CAS
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity = swiss::kDynamicExtent<Index>,
          Index Alignment =
              alignof(std::atomic<swiss::trivial_pair<Value, Index>>),
          typename Allocator = std::allocator<Value>>
  requires(std::is_trivial_v<Value>)
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class cas128_mpmc_queue : public swiss::non_copyable,
                          public swiss::non_movable {
 public:
  using value_type = Value;
  using index_type = Index;
  using allocator_type =
      std::allocator_traits<Allocator>::template rebind_alloc<swiss::aligned<
          std::atomic<swiss::trivial_pair<Value, Index>>, Alignment>>;

 private:
  static constexpr value_type kUninitializedValue = 0;

  using entry_type        = swiss::trivial_pair<Value, Index>;
  using atomic_entry_type = std::atomic<swiss::trivial_pair<Value, Index>>;

 public:
  constexpr explicit cas128_mpmc_queue(
      allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity) && Capacity >= 2,
                  "cas128_mpmc_queue::cas128_mpmc_queue(): invalid capacity != "
                  "2^n and < 2");
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<allocator_type>::construct(
          allocator_, at(writer_.ring_buffer, *writer_.capacity, index),
          entry_type(kUninitializedValue, index));
    }
  }

  constexpr explicit cas128_mpmc_queue(
      index_type capacity, allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity) || capacity < 2) {
                    throw std::invalid_argument(
                        "cas128_mpmc_queue::cas128_mpmc_queue(): invalid "
                        "capacity != 2^n or < 2");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }(),
        },
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<allocator_type>::construct(
          allocator_, at(writer_.ring_buffer, *writer_.capacity, index),
          entry_type(kUninitializedValue, index));
    }
  }

  constexpr ~cas128_mpmc_queue() noexcept {
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<allocator_type>::destroy(
          allocator_, at(writer_.ring_buffer, *writer_.capacity, index));
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return try_push([&](value_type & front) noexcept {
      front = value_type(std::forward<Args>(args)...);
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

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return try_pop(
        [&](value_type & front) noexcept { value = std::move(front); });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept
      -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return push(stop_token, [&](value_type & front) noexcept {
      front = value_type(std::forward<Args>(args)...);
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

  constexpr auto pop(std::stop_token const & stop_token,
                     value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token,
               [&](value_type & front) noexcept { value = std::move(front); });
  }

 public:
  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type const & value) noexcept
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    emplace(std::stop_token{}, value);
  }

  constexpr void push(value_type && value) noexcept
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    emplace(std::stop_token{}, std::move(value));
  }

  constexpr void pop(value_type & value) noexcept
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    pop(std::stop_token{}, value);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    auto const _size = size(push_to, pop_from);
    return _size > *writer_.capacity ? 0 : _size;
  }

  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    swiss::aligned<atomic_entry_type, Alignment> * const ring_buffer;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  [[no_unique_address]] allocator_type allocator_;
  CacheLine writer_;
  CacheLine reader_;

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
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

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }

  [[nodiscard]] constexpr auto at(
      swiss::aligned<atomic_entry_type, Alignment> * ring_buffer,
      index_type capacity, index_type index) const noexcept
      -> atomic_entry_type * {
    return &**(ring_buffer + slot(capacity, index));
  }

  [[nodiscard]] constexpr auto value(entry_type & entry) const noexcept
      -> value_type & {
    return entry.first;
  }

  [[nodiscard]] constexpr auto seqnum(entry_type & entry) const noexcept
      -> index_type {
    return entry.second;
  }

 private:
  template <std::invocable<value_type &> Callback>
  constexpr auto try_push(Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    index_type push_to = writer_.index.load(std::memory_order::relaxed);

    atomic_entry_type * entry =
        at(writer_.ring_buffer, *writer_.capacity, push_to);
    entry_type expected = entry->load(std::memory_order::acquire);
    if (!vacant(push_to, seqnum(expected))) {
      return false;
    }

    entry_type desired(kUninitializedValue, push_to + 1);

    std::invoke(callback, value(desired));

    if (!entry->compare_exchange_strong(expected, desired,
                                        std::memory_order::release,
                                        std::memory_order::acquire)) {
      return false;
    }

    writer_.index.compare_exchange_strong(push_to, next(push_to),
                                          std::memory_order::relaxed,
                                          std::memory_order::relaxed);
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto try_pop(Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);

    atomic_entry_type * entry =
        at(reader_.ring_buffer, *reader_.capacity, pop_from);
    entry_type expected = entry->load(std::memory_order::acquire);
    if (!occupied(pop_from, seqnum(expected))) {
      return false;
    }

    entry_type const desired(kUninitializedValue, pop_from + *reader_.capacity);

    if (!entry->compare_exchange_strong(expected, desired,
                                        std::memory_order::release,
                                        std::memory_order::acquire)) {
      return false;
    }

    std::invoke(callback, value(expected));

    reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                          std::memory_order::relaxed,
                                          std::memory_order::relaxed);
    return true;
  }

 private:
  template <std::invocable<value_type &> Callback>
  constexpr auto push(std::stop_token const & stop_token,
                      Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    while (!stop_token.stop_requested()) {
      if (cas128_mpmc_queue::try_push(callback)) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    while (!stop_token.stop_requested()) {
      if (cas128_mpmc_queue::try_pop(callback)) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }
};

}  // namespace flow
