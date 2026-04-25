#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <thread>

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
 * @warning std::allocator_traits construction/destruction is not supported
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity = swiss::kDynamicExtent<Index>,
          typename Allocator =
              std::allocator<std::atomic<swiss::trivial_pair<Value, Index>>>>
  requires(Allocator::value_type::is_always_lock_free)
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class cas128_mpmc_queue final : public swiss::non_copyable,
                                public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

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

    for (index_type i = 0; i < Capacity; ++i) {
      auto * entry = writer_.ring_buffer + i;
      entry->store([i] {
        typename allocator_type::value_type::value_type entry{};
        auto & [_, seqnum] = entry;
        seqnum             = i;
        return entry;
      }());
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
    for (index_type i = 0; i < *writer_.capacity; ++i) {
      auto * entry = writer_.ring_buffer + i;
      entry->store([i] {
        typename allocator_type::value_type::value_type entry{};
        auto & [_, seqnum] = entry;
        seqnum             = i;
        return entry;
      }());
    }
  }

  constexpr ~cas128_mpmc_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto try_push(Callback && callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    index_type push_to = writer_.index.load(std::memory_order::relaxed);
    auto * entry       = writer_.ring_buffer + slot(*writer_.capacity, push_to);
    auto expected      = entry->load(std::memory_order::acquire);
    auto const & [_, seqnum] = expected;

    if (!vacant(push_to, seqnum)) {
      return false;
    }

    typename allocator_type::value_type::value_type desired{};
    auto & [item, _seqnum] = desired;

    std::invoke(std::forward<Callback>(callback), item);

    _seqnum = push_to + 1;

    if (!entry->compare_exchange_strong(expected, desired,
                                        std::memory_order::release,
                                        std::memory_order::acquire)) {
      return false;
    }

    writer_.index.compare_exchange_strong(push_to, next(push_to),
                                          std::memory_order::release,
                                          std::memory_order::acquire);
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto try_pop(Callback && callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);
    auto * entry  = reader_.ring_buffer + slot(*reader_.capacity, pop_from);
    auto expected = entry->load(std::memory_order::acquire);
    auto & [item, seqnum] = expected;

    if (!occupied(pop_from, seqnum)) {
      return false;
    }

    typename allocator_type::value_type::value_type desired{};
    auto & [_, _seqnum] = desired;
    _seqnum             = pop_from + *reader_.capacity;

    if (!entry->compare_exchange_strong(expected, desired,
                                        std::memory_order::release,
                                        std::memory_order::acquire)) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), item);

    reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                          std::memory_order::release,
                                          std::memory_order::acquire);

    return true;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto push(std::stop_token const & stop_token,
                      Callback && callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    while (!stop_token.stop_requested()) {
      if (cas128_mpmc_queue::try_push(std::forward<Callback>(callback))) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    while (!stop_token.stop_requested()) {
      if (cas128_mpmc_queue::try_pop(std::forward<Callback>(callback))) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr void push(Callback && callback) noexcept
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    push(std::stop_token{}, std::forward<Callback>(callback));
  }

  template <std::invocable<value_type &> Callback>
  constexpr void pop(Callback && callback) noexcept
    requires(std::is_nothrow_invocable_v<Callback, value_type &>)
  {
    pop(std::stop_token{}, std::forward<Callback>(callback));
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return try_push([&](value_type & value) mutable noexcept {
      std::construct_at(&value, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return try_pop([&out = value](value_type & value) mutable noexcept {
      out = std::move(value);
      std::destroy_at(&value);
    });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept
      -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return push(stop_token, [&](value_type & value) mutable noexcept {
      std::construct_at(&value, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept
      -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token stop_token, value_type & value) noexcept
      -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token, [&out = value](value_type & value) mutable noexcept {
      out = std::move(value);
      std::destroy_at(&value);
    });
  }

 public:
  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    push(std::stop_token{}, std::move(value));
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

    return size(push_to, pop_from);
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
    typename allocator_type::value_type * const ring_buffer;
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
};

}  // namespace flow
