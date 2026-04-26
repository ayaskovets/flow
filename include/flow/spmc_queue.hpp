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

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 consumer and many producers
 * @note Implemented as a ring buffer with cached pop and push indices
 * @note Capacity for this queue must be a power of 2 for efficient division
 * @warning std::allocator_traits construction/destruction is not supported
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spmc_queue final : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<std::pair<Value, std::atomic<Index>>>;

 public:
  constexpr explicit spmc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity) && Capacity >= 2,
                  "spmc_queue::spmc_queue(): invalid capacity != 2^n or < 2");

    for (index_type i = 0; i < Capacity; ++i) {
      auto & [_, seqnum] = *(writer_.ring_buffer + i);
      seqnum.store(i, std::memory_order::relaxed);
    }
  }

  constexpr explicit spmc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity) || capacity < 2) {
                    throw std::invalid_argument(
                        "spmc_queue::spmc_queue(): invalid capacity != 2^n or "
                        "< 2");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }(),
        },
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {
    for (index_type i = 0; i < *writer_.capacity; ++i) {
      auto & [_, seqnum] = *(writer_.ring_buffer + i);
      seqnum.store(i, std::memory_order::relaxed);
    }
  }

  /**
   * @warning Destructor is not thread safe and assumes that there is only one
   * thread operating on the queue during the destruction
   */
  constexpr ~spmc_queue() {
    while (
        try_pop([](value_type * front) noexcept { std::destroy_at(front); })) {
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return try_push([&](value_type * front) noexcept {
      std::construct_at(front, std::forward<Args>(args)...);
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
    return try_pop([&](value_type * front) noexcept {
      value = std::move(*front);
      std::destroy_at(front);
    });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept
      -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return push(stop_token, [&](value_type * front) noexcept {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token, [&](value_type * front) noexcept {
      value = std::move(*front);
      std::destroy_at(front);
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
    return *writer_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    return push_to >= pop_from
               ? std::min(*reader_.capacity, size(push_to, pop_from))
               : 0;
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

 private:
  template <std::invocable<value_type *> Callback>
  constexpr auto try_push(Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type *>)
  {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);
    auto * entry = writer_.ring_buffer + slot(*writer_.capacity, push_to);
    auto & [item, seqnum] = *entry;

    if (!vacant(push_to, seqnum.load(std::memory_order::acquire))) {
      return false;
    }
    writer_.index.store(next(push_to), std::memory_order::relaxed);

    std::invoke(callback, &item);

    seqnum.store(push_to + 1, std::memory_order::release);
    return true;
  }

  template <std::invocable<value_type *> Callback>
  constexpr auto try_pop(Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type *>)
  {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);
    auto * entry = reader_.ring_buffer + slot(*reader_.capacity, pop_from);
    auto & [item, seqnum] = *entry;

    if (!occupied(pop_from, seqnum.load(std::memory_order::acquire)) ||
        !reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                               std::memory_order::relaxed)) {
      return false;
    }

    std::invoke(callback, &item);

    seqnum.store(pop_from + *reader_.capacity, std::memory_order::release);
    return true;
  }

 private:
  template <std::invocable<value_type *> Callback>
  constexpr auto push(std::stop_token const & stop_token,
                      Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type *>)
  {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);
    auto * entry = writer_.ring_buffer + slot(*writer_.capacity, push_to);

    while (!stop_token.stop_requested()) {
      auto & [item, seqnum] = *entry;
      if (vacant(push_to, seqnum.load(std::memory_order::acquire))) {
        writer_.index.store(next(push_to), std::memory_order::relaxed);

        std::invoke(callback, &item);

        seqnum.store(push_to + 1, std::memory_order::release);
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

  template <std::invocable<value_type *> Callback>
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, value_type *>)
  {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);
    auto * entry = reader_.ring_buffer + slot(*reader_.capacity, pop_from);

    while (!stop_token.stop_requested()) {
      auto & [item, seqnum] = *entry;
      bool const occ =
          occupied(pop_from, seqnum.load(std::memory_order::acquire));
      if (occ && reader_.index.compare_exchange_weak(
                     pop_from, next(pop_from), std::memory_order::relaxed)) {
        std::invoke(callback, &item);

        seqnum.store(pop_from + *reader_.capacity, std::memory_order::release);
        return true;
      }

      std::this_thread::yield();

      if (!occ) {
        pop_from = reader_.index.load(std::memory_order::relaxed);
      }
      entry = reader_.ring_buffer + slot(*reader_.capacity, pop_from);
    }

    return false;
  }
};

}  // namespace flow
