#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <thread>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/variadic_traits.hpp>

namespace flow {

template <typename Value, std::unsigned_integral Index>
struct test {
  Value value;
  Index index;
};

/**
 * @brief Threadsafe queue for many producers and many consumers
 * @note Implemented with 128-bit CAS. The last bit of seqnum indicates whether
 * the element is occupied. Allocated value must be an always-lockfree atomic
 * @warning std::allocator_traits construction/destruction is not supported
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<std::atomic<test<Value, Index>>>>
  requires(static_cast<bool>(Allocator::value_type::is_always_lock_free))
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
      : writer_{.ring_buffer = allocator.allocate(Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer},
        allocator_(std::move(allocator)) {
    static_assert(
        Capacity > 1,
        "cas128_mpmc_queue::cas128_mpmc_queue(): invalid capacity < 2");
    static_assert(
        swiss::is_power_of_two(Capacity),
        "cas128_mpmc_queue::cas128_mpmc_queue(): capacity must be 2^n");

    for (std::size_t i = 0; i < Capacity; ++i) {
      auto entry = at(writer_.ring_buffer, *writer_.capacity, i);
      entry->store([i]() {
        typename allocator_type::value_type::value_type entry;
        auto & [_, seqnum] = entry;
        seqnum             = i << 1U;
        return entry;
      }());
    }
  }

  constexpr explicit cas128_mpmc_queue(
      index_type capacity, allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer =
                    [capacity, &allocator]() {
                      if (capacity < 2) [[unlikely]] {
                        throw std::invalid_argument(
                            "cas128_mpmc_queue::cas128_mpmc_queue(): "
                            "invalid capacity "
                            "< 2");
                      }
                      if (!swiss::is_power_of_two(capacity)) [[unlikely]] {
                        throw std::invalid_argument(
                            "cas128_mpmc_queue::cas128_mpmc_queue(): "
                            "capacity must be 2^n");
                      }
                      return allocator.allocate(capacity);
                    }(),
                .capacity{capacity}},
        reader_{.ring_buffer = writer_.ring_buffer, .capacity{capacity}},
        allocator_(std::move(allocator)) {
    for (std::size_t i = 0; i < capacity; ++i) {
      auto entry = at(writer_.ring_buffer, *writer_.capacity, i);
      entry->store([i]() {
        typename allocator_type::value_type::value_type entry;
        auto & [_, seqnum] = entry;
        seqnum             = i << 1U;
        return entry;
      }());
    }
  }

  constexpr ~cas128_mpmc_queue() noexcept {
    allocator_.deallocate(reader_.ring_buffer, *reader_.capacity);
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
    return *writer_.capacity;
  }

  /**
   * @brief Approximate size
   */
  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

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
  [[nodiscard]] constexpr auto at(
      typename allocator_type::value_type * const ring_buffer,
      index_type capacity, index_type index) const noexcept ->
      typename allocator_type::value_type * {
    return ring_buffer + (index & capacity - 1);
  }

  // [[nodiscard]] constexpr auto vacant(index_type push_to,
  //                                     index_type seqnum) const noexcept
  //     -> bool {
  //   return (push_to << 1) == seqnum;
  // }

  // [[nodiscard]] constexpr auto occupied(index_type pop_from,
  //                                       index_type seqnum) const noexcept
  //     -> bool {
  //   return pop_from + 1 == seqnum;
  // }

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto emplace_impl(Args &&... args) -> decltype(auto) {
    if constexpr (Blocking) {
      auto const try_emplace = [this](Args &&... args) {
        if constexpr (Cancellable) {
          return [this](auto &&, auto &&... tail) {
            return this->emplace_impl<false, false>(
                std::forward<decltype(tail)>(tail)...);
          }(std::forward<Args>(args)...);
        } else {
          return this->emplace_impl<false, false>(std::forward<Args>(args)...);
        }
      };

      while (!try_emplace(std::forward<Args>(args)...)) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        // std::this_thread::yield();
      }
      if constexpr (Cancellable) {
        return true;
      }
    } else {
      auto push_to  = writer_.index.load(std::memory_order::relaxed);
      auto entry    = at(writer_.ring_buffer, *writer_.capacity, push_to);
      auto expected = entry->load(std::memory_order::acquire);
      auto const & [_, seqnum] = expected;

      if (seqnum == (push_to << 1)) {
        typename allocator_type::value_type::value_type desired;
        auto & [item, _seqnum] = desired;
        std::construct_at(&item, std::forward<Args>(args)...);
        std::construct_at(&_seqnum, push_to << 1 | 1);

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

      // TODO: invalid conditions?
      if (seqnum == ((push_to << 1) | 1) ||
          seqnum == ((push_to + *writer_.capacity) << 1)) {
        writer_.index.compare_exchange_strong(push_to, next(push_to),
                                              std::memory_order::release,
                                              std::memory_order::acquire);
      }
      return false;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    if constexpr (Blocking) {
      auto const try_pop = [this](Args &&... args) {
        if constexpr (Cancellable) {
          return [this](auto &&, auto &&... tail) {
            return this->pop_impl<false, false>(
                std::forward<decltype(tail)>(tail)...);
          }(std::forward<Args>(args)...);
        } else {
          return this->pop_impl<false, false>(std::forward<Args>(args)...);
        }
      };

      while (!try_pop(std::forward<Args>(args)...)) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        // std::this_thread::yield();
      }
      if constexpr (Cancellable) {
        return true;
      }
    } else {
      auto pop_from = reader_.index.load(std::memory_order::relaxed);
      auto entry    = at(reader_.ring_buffer, *reader_.capacity, pop_from);
      auto expected = entry->load(std::memory_order::acquire);
      auto & [item, seqnum] = expected;

      if (seqnum == ((pop_from << 1) | 1)) {
        typename allocator_type::value_type::value_type desired;
        auto & [_, _seqnum] = desired;
        std::construct_at(&_seqnum, (pop_from + *reader_.capacity) << 1);

        if (!entry->compare_exchange_strong(expected, desired,
                                            std::memory_order::release,
                                            std::memory_order::acquire)) {
          return false;
        }

        auto & value = swiss::variadic_nth<0>(std::forward<Args>(args)...);
        value        = std::move(item);
        reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                              std::memory_order::release,
                                              std::memory_order::acquire);

        return true;
      }

      // TODO: invalid conditions?
      if ((seqnum | 1) == (((pop_from + *reader_.capacity) << 1) | 1)) {
        reader_.index.compare_exchange_strong(pop_from, next(pop_from),
                                              std::memory_order::release,
                                              std::memory_order::acquire);
      }
      return false;
    }
  }
};

}  // namespace flow
