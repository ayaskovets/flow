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
#include <swiss/variadic_traits.hpp>

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a ring buffer with cached pop and push indices
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_queue : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit spsc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer = allocator.allocate(Capacity + 1)},
        reader_{.ring_buffer = writer_.ring_buffer},
        allocator_(std::move(allocator)) {
    static_assert(Capacity > 0,
                  "spsc_queue::spsc_queue(): invalid capacity < 1");
  }

  constexpr explicit spsc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer =
                    [capacity, &allocator]() {
                      if (capacity == 0) [[unlikely]] {
                        throw std::invalid_argument(
                            "spsc_queue::spsc_queue(): invalid capacity < 1");
                      }
                      return allocator.allocate(capacity + 1);
                    }(),
                .capacity{capacity}},
        reader_{.ring_buffer = writer_.ring_buffer, .capacity{capacity}},
        allocator_(std::move(allocator)) {}

  /**
   * @warning Destructor is not thread safe and assumes that there is only one
   * thread operating on the queue during the destruction
   */
  constexpr ~spsc_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    auto const push_to = writer_.index.load(std::memory_order::relaxed);

    auto pop_from = reader_.index.load(std::memory_order::relaxed);
    while (!empty(push_to, pop_from)) {
      std::allocator_traits<allocator_type>::destroy(
          allocator_, reader_.ring_buffer + pop_from);
      pop_from = next(*reader_.capacity, pop_from);
    }

    allocator_.deallocate(reader_.ring_buffer, *reader_.capacity + 1);
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
   * @warning Approximate size
   */
  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    auto const push_to  = writer_.index.load(std::memory_order::relaxed);
    auto const pop_from = reader_.index.load(std::memory_order::relaxed);

    return pop_from <= push_to ? push_to - pop_from
                               : *writer_.capacity + push_to + 1 - pop_from;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    value_type * const ring_buffer;
    swiss::conditionally_runtime<index_type,
                                 Capacity == swiss::kDynamicExtent<index_type>,
                                 Capacity> const capacity;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  CacheLine writer_;
  CacheLine reader_;
  [[no_unique_address]] allocator_type allocator_;

 private:
  [[nodiscard]] constexpr auto full(index_type next_push_to,
                                    index_type pop_from) const noexcept
      -> bool {
    return next_push_to == pop_from;
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  [[nodiscard]] constexpr auto next(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index == capacity ? 0 : index + 1;
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto emplace_impl(Args &&... args) -> decltype(auto) {
    auto const push_to      = writer_.index.load(std::memory_order::relaxed);
    auto const next_push_to = next(*writer_.capacity, push_to);

    if constexpr (Blocking) {
      while (full(next_push_to, writer_.cache)) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        writer_.cache = reader_.index.load(std::memory_order::acquire);
        if (full(next_push_to, writer_.cache)) {
          std::this_thread::yield();
        }
      }
    } else {
      if (full(next_push_to, writer_.cache)) {
        auto const cache = reader_.index.load(std::memory_order::acquire);
        if (full(next_push_to, cache)) {
          return false;
        }
        writer_.cache = cache;
      }
    }

    if constexpr (Cancellable && sizeof...(Args) > 0) {
      [this, push_to](auto &&, auto &&... tail) {
        std::allocator_traits<allocator_type>::construct(
            allocator_, writer_.ring_buffer + push_to,
            std::forward<decltype(tail)>(tail)...);
      }(std::forward<Args>(args)...);
    } else {
      std::allocator_traits<allocator_type>::construct(
          allocator_, writer_.ring_buffer + push_to,
          std::forward<Args>(args)...);
    }

    writer_.index.store(next_push_to, std::memory_order::release);
    if constexpr (!Blocking || Cancellable) {
      return true;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    auto const pop_from      = reader_.index.load(std::memory_order::relaxed);
    auto const next_pop_from = next(*reader_.capacity, pop_from);

    if constexpr (Blocking) {
      while (empty(reader_.cache, pop_from)) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        reader_.cache = writer_.index.load(std::memory_order::acquire);
        if (empty(reader_.cache, pop_from)) {
          std::this_thread::yield();
        }
      }
    } else {
      if (empty(reader_.cache, pop_from)) {
        auto const cache = writer_.index.load(std::memory_order::acquire);
        if (empty(cache, pop_from)) {
          return false;
        }
        reader_.cache = cache;
      }
    }

    auto & value =
        swiss::variadic_nth<Cancellable ? 1 : 0>(std::forward<Args>(args)...);
    value = std::move(*(reader_.ring_buffer + pop_from));
    std::allocator_traits<allocator_type>::destroy(
        allocator_, reader_.ring_buffer + pop_from);

    reader_.index.store(next_pop_from, std::memory_order::release);
    return true;
  }
};

}  // namespace flow
