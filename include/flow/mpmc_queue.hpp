#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <thread>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/variadic_traits.hpp>

namespace flow {

/**
 * @brief Threadsafe queue for many producers and many consumers
 * @note Implemented as a ring buffer with a sequence number for each element as
 * a means to mitigate the ABA problem which could happen when Index overflows
 * @note Capacity for this queue must be a power of 2 for efficient division
 * @warning std::allocator_traits construction/destruction is not supported
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity = swiss::kDynamicExtent<Index>,
          typename Allocator =
              std::allocator<std::pair<Value, std::atomic<Index>>>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpmc_queue final : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit mpmc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer = allocator.allocate(Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer},
        allocator_(std::move(allocator)) {
    static_assert(Capacity > 1,
                  "mpmc_queue::mpmc_queue(): invalid capacity < 2");
    static_assert(swiss::is_power_of_two(Capacity),
                  "mpmc_queue::mpmc_queue(): capacity must be 2^n");

    for (index_type i = 0; i < Capacity; ++i) {
      auto & [_, seqnum] = *(writer_.ring_buffer + i);
      seqnum.store(i, std::memory_order::relaxed);
    }
  }

  constexpr explicit mpmc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer =
                    [capacity, &allocator] {
                      if (capacity < 2) {
                        throw std::invalid_argument(
                            "mpmc_queue::mpmc_queue(): "
                            "invalid capacity "
                            "< 2");
                      }
                      if (!swiss::is_power_of_two(capacity)) {
                        throw std::invalid_argument(
                            "mpmc_queue::mpmc_queue(): "
                            "capacity must be 2^n");
                      }
                      return allocator.allocate(capacity);
                    }(),
                .capacity{capacity}},
        reader_{.ring_buffer = writer_.ring_buffer, .capacity{capacity}},
        allocator_(std::move(allocator)) {
    for (index_type i = 0; i < capacity; ++i) {
      auto & [_, seqnum] = *(writer_.ring_buffer + i);
      seqnum.store(i, std::memory_order::relaxed);
    }
  }

  /**
   * @warning Destructor is not thread safe and assumes that there is only one
   * thread operating on the queue during the destruction
   */
  constexpr ~mpmc_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    auto const push_to = writer_.index.load(std::memory_order::relaxed);

    auto pop_from = reader_.index.load(std::memory_order::relaxed);
    while (!empty(push_to, pop_from)) {
      std::allocator_traits<allocator_type>::destroy(
          allocator_,
          reader_.ring_buffer + (pop_from & (*reader_.capacity - 1)));
      pop_from = next(pop_from);
    }

    allocator_.deallocate(reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return emplace_impl<false, false>(std::forward<Args>(args)...);
  }

  constexpr auto try_push(value_type value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace_impl<false, false>(std::move(value));
  }

  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept
      -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return emplace_impl<true, true>(stop_token, std::forward<Args>(args)...);
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept
      -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace_impl<true, true>(stop_token, std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    emplace_impl<true, false>(std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
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
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    auto const push_to  = writer_.index.load(std::memory_order::relaxed);
    auto const pop_from = reader_.index.load(std::memory_order::relaxed);

    return pop_from <= push_to ? push_to - pop_from
                               : *reader_.capacity + push_to + 1 - pop_from;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    typename allocator_type::value_type * const ring_buffer;
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  CacheLine writer_;
  CacheLine reader_;
  [[no_unique_address]] allocator_type allocator_;

 private:
  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  [[nodiscard]] constexpr auto at(
      typename allocator_type::value_type * const ring_buffer,
      index_type capacity, index_type index) const noexcept ->
      typename allocator_type::value_type * {
    return ring_buffer + (index & capacity - 1);
  }

  [[nodiscard]] constexpr auto vacant(index_type index,
                                      index_type seqnum) const noexcept
      -> bool {
    return index == seqnum;
  }

  [[nodiscard]] constexpr auto occupied(index_type index,
                                        index_type seqnum) const noexcept
      -> bool {
    /**
     * @note Cast here is required for types that are converted to int on
     * addition like std::uint8_t, std::uint16_t etc.
     */
    return static_cast<index_type>(index + 1) == seqnum;
  }

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto emplace_impl(Args &&... args) -> decltype(auto) {
    auto push_to = writer_.index.load(std::memory_order::relaxed);
    auto entry   = at(writer_.ring_buffer, *writer_.capacity, push_to);

    if constexpr (Blocking) {
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        auto const & [_, seqnum] = *entry;
        auto const vac =
            vacant(push_to, seqnum.load(std::memory_order::acquire));
        if (vac && writer_.index.compare_exchange_weak(
                       push_to, next(push_to), std::memory_order::relaxed)) {
          break;
        }

        std::this_thread::yield();
        if (!vac) {
          push_to = writer_.index.load(std::memory_order::relaxed);
        }
        entry = at(writer_.ring_buffer, *writer_.capacity, push_to);
      }
    }

    if constexpr (!Blocking) {
      auto const & [_, seqnum] = *entry;
      if (!vacant(push_to, seqnum.load(std::memory_order::acquire)) ||
          !writer_.index.compare_exchange_strong(push_to, next(push_to),
                                                 std::memory_order::relaxed)) {
        return false;
      }
    }

    auto & [item, seqnum] = *entry;
    if constexpr (Cancellable) {
      swiss::invoke_tail(
          [&item](auto &&... tail) {
            std::construct_at(&item, std::forward<decltype(tail)>(tail)...);
          },
          std::forward<Args>(args)...);
    }

    if constexpr (!Cancellable) {
      std::construct_at(&item, std::forward<Args>(args)...);
    }

    seqnum.store(push_to + 1, std::memory_order::release);
    if constexpr (!Blocking || Cancellable) {
      return true;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    auto pop_from = reader_.index.load(std::memory_order::relaxed);
    auto entry    = at(reader_.ring_buffer, *reader_.capacity, pop_from);

    if constexpr (Blocking) {
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        auto const & [_, seqnum] = *entry;
        auto const occ =
            occupied(pop_from, seqnum.load(std::memory_order::acquire));
        if (occ && reader_.index.compare_exchange_weak(
                       pop_from, next(pop_from), std::memory_order::relaxed)) {
          break;
        }

        std::this_thread::yield();
        if (!occ) {
          pop_from = reader_.index.load(std::memory_order::relaxed);
        }
        entry = at(reader_.ring_buffer, *reader_.capacity, pop_from);
      }
    }

    if constexpr (!Blocking) {
      auto const & [_, seqnum] = *entry;
      if (!occupied(pop_from, seqnum.load(std::memory_order::acquire)) ||
          !reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                                 std::memory_order::relaxed)) {
        return false;
      }
    }

    auto & [item, seqnum] = *entry;
    auto & value =
        swiss::variadic_nth<Cancellable ? 1 : 0>(std::forward<Args>(args)...);
    value = std::move(item);
    std::destroy_at(&item);

    seqnum.store(pop_from + *reader_.capacity, std::memory_order::release);
    return true;
  }
};

}  // namespace flow
