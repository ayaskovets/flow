#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>

namespace flow {

/**
 * @brief Threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a ring buffer with cached pop and push indices
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_queue final : public swiss::non_copyable, public swiss::non_movable {
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
    static_assert(Capacity > 0, "capacity must be greater than zero");
  }

  constexpr explicit spsc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer =
                    [capacity, &allocator]() {
                      if (capacity == 0) [[unlikely]] {
                        throw std::invalid_argument(
                            "capacity must be greater than zero");
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
    auto const push_to     = writer_.index.load(std::memory_order::relaxed);
    auto const ring_buffer = writer_.ring_buffer;
    auto const capacity    = *writer_.capacity;

    auto pop_from = reader_.index.load(std::memory_order::relaxed);
    while (!empty(push_to, pop_from)) {
      std::destroy_at(ring_buffer + pop_from);
      pop_from = next(capacity, pop_from);
    }

    allocator_.deallocate(ring_buffer, capacity + 1);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    auto const push_to      = writer_.index.load(std::memory_order::relaxed);
    auto const ring_buffer  = writer_.ring_buffer;
    auto const capacity     = *writer_.capacity;
    auto const next_push_to = next(capacity, push_to);

    if (full(next_push_to, writer_.cache)) {
      auto const cache = reader_.index.load(std::memory_order::acquire);
      if (full(next_push_to, cache)) {
        return false;
      }
      writer_.cache = cache;
    }

    std::allocator_traits<allocator_type>::construct(
        allocator_, ring_buffer + push_to, std::forward<Args>(args)...);
    writer_.index.store(next_push_to, std::memory_order::release);

    return true;
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  template <typename... Args>
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    auto const push_to      = writer_.index.load(std::memory_order::relaxed);
    auto const ring_buffer  = writer_.ring_buffer;
    auto const capacity     = *writer_.capacity;
    auto const next_push_to = next(capacity, push_to);

    while (full(next_push_to, writer_.cache)) {
      if (stop_token.stop_requested()) {
        return false;
      }
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      if (!full(next_push_to, writer_.cache)) {
        break;
      }
      std::this_thread::yield();
    }

    std::allocator_traits<allocator_type>::construct(
        allocator_, ring_buffer + push_to, std::forward<Args>(args)...);
    writer_.index.store(next_push_to, std::memory_order::release);
    return true;
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace(stop_token, std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) {
    emplace(std::stop_token(), std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept { emplace(std::move(value)); }

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    auto const pop_from      = reader_.index.load(std::memory_order::relaxed);
    auto const ring_buffer   = reader_.ring_buffer;
    auto const capacity      = *reader_.capacity;
    auto const next_pop_from = next(capacity, pop_from);

    if (empty(reader_.cache, pop_from)) {
      auto const cache = writer_.index.load(std::memory_order::acquire);
      if (empty(cache, pop_from)) {
        return false;
      }
      reader_.cache = cache;
    }

    value = std::move(*(ring_buffer + pop_from));
    std::allocator_traits<allocator_type>::destroy(allocator_,
                                                   ring_buffer + pop_from);
    reader_.index.store(next_pop_from, std::memory_order::release);

    return true;
  }

  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  constexpr auto pop(std::stop_token stop_token) noexcept
      -> std::optional<value_type>
    requires(std::is_nothrow_move_constructible_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    auto const pop_from      = reader_.index.load(std::memory_order::relaxed);
    auto const ring_buffer   = reader_.ring_buffer;
    auto const capacity      = *reader_.capacity;
    auto const next_pop_from = next(capacity, pop_from);

    std::optional<value_type> value{};
    while (empty(reader_.cache, pop_from)) {
      if (stop_token.stop_requested()) {
        return value;
      }
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      if (!empty(reader_.cache, pop_from)) {
        break;
      }
      std::this_thread::yield();
    }

    value.emplace(std::move(*(ring_buffer + pop_from)));
    std::allocator_traits<allocator_type>::destroy(allocator_,
                                                   ring_buffer + pop_from);
    reader_.index.store(next_pop_from, std::memory_order::release);

    return value;
  }

  constexpr auto pop() noexcept -> value_type
    requires(std::is_nothrow_move_constructible_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return *pop(std::stop_token());
  }

 public:
  constexpr auto capacity() const noexcept -> index_type {
    return *writer_.capacity;
  }

  /**
   * @warning Approximate size
   */
  constexpr auto size() const noexcept -> index_type {
    auto const push_to  = writer_.index.load(std::memory_order::relaxed);
    auto const capacity = *writer_.capacity;
    auto const pop_from = reader_.index.load(std::memory_order::relaxed);

    return pop_from <= push_to ? push_to - pop_from
                               : capacity + push_to + 1 - pop_from;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    value_type * const ring_buffer;
    swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
        Capacity> const capacity;
  };

  CacheLine writer_;
  CacheLine reader_;
  [[no_unique_address]] allocator_type allocator_;

 private:
  constexpr auto full(index_type next_push_to,
                      index_type pop_from) const noexcept {
    return next_push_to == pop_from;
  }

  constexpr auto empty(index_type push_to, index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  constexpr auto next(index_type capacity, index_type index) const noexcept
      -> index_type {
    return index == capacity ? 0 : index + 1;
  }
};

}  // namespace flow
