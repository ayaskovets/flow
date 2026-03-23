#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>

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
          Index Capacity = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class waitfree_spsc_queue final : public swiss::non_copyable,
                                  public swiss::non_movable {
 public:
  using value_type = Value;
  using index_type = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit waitfree_spsc_queue(
      allocator_type const & allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(allocator),
        ring_buffer_(allocator_.allocate(Capacity + 1)),
        capacity_() {
    static_assert(Capacity > 0, "capacity must be greater than zero");
  }

  constexpr explicit waitfree_spsc_queue(
      index_type capacity, allocator_type const & allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(allocator),
        ring_buffer_(allocator_.allocate(capacity + 1)),
        capacity_(capacity) {
    if (capacity == 0) [[unlikely]] {
      throw std::invalid_argument("capacity must be greater than zero");
    }
  }

  /**
   * @warning Destructor operations are not thread safe and it is assumed that
   * there is only one thread doing any operations on the queue at the moment
   * destruction occurs
   */
  constexpr ~waitfree_spsc_queue() noexcept {
    index_type const push_to = push_to_.load(std::memory_order::relaxed);

    for (index_type pop_from = pop_from_.load(std::memory_order::relaxed);
         !empty(push_to, pop_from); pop_from = next(pop_from)) {
      std::destroy_at(ring_buffer_ + pop_from);
    }

    allocator_.deallocate(ring_buffer_, *capacity_ + 1);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<Value, Args...>)
  {
    index_type const push_to = push_to_.load(std::memory_order::relaxed);
    index_type const next_push_to = next(push_to);

    if (full(next_push_to, cached_pop_from_)) {
      cached_pop_from_ = pop_from_.load(std::memory_order::acquire);
      if (full(next_push_to, cached_pop_from_)) {
        return false;
      }
    }

    std::construct_at(ring_buffer_ + push_to, std::forward<Args>(args)...);
    push_to_.store(next_push_to, std::memory_order::release);

    return true;
  }

  constexpr auto try_push(value_type value) noexcept -> bool {
    return try_emplace(std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept
    requires(std::is_nothrow_constructible_v<Value, Args...>)
  {
    index_type const push_to = push_to_.load(std::memory_order::relaxed);
    index_type const next_push_to = next(push_to);

    while (full(next_push_to, cached_pop_from_)) {
      cached_pop_from_ = pop_from_.load(std::memory_order::acquire);
    }

    std::construct_at(ring_buffer_ + push_to, std::forward<Args>(args)...);
    push_to_.store(next_push_to, std::memory_order::release);
  }

  constexpr void push(value_type value) noexcept { emplace(std::move(value)); }

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<Value>)
  {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);
    index_type const next_pop_from = next(pop_from);

    if (empty(cached_push_to_, pop_from)) {
      cached_push_to_ = push_to_.load(std::memory_order::acquire);
      if (empty(cached_push_to_, pop_from)) {
        return false;
      }
    }

    value = std::move(*(ring_buffer_ + pop_from));
    std::destroy_at(ring_buffer_ + pop_from);
    pop_from_.store(next_pop_from, std::memory_order::release);

    return true;
  }

  constexpr auto pop() noexcept -> value_type {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);
    index_type const next_pop_from = next(pop_from);

    while (empty(cached_push_to_, pop_from)) {
      cached_push_to_ = push_to_.load(std::memory_order::acquire);
    }

    value_type value(std::move(*(ring_buffer_ + pop_from)));
    std::destroy_at(ring_buffer_ + pop_from);
    pop_from_.store(next_pop_from, std::memory_order::release);

    return value;
  }

 public:
  constexpr auto capacity() const noexcept -> index_type { return *capacity_; }

  /**
   * @warning Approximate size
   */
  constexpr auto size() const noexcept -> index_type {
    index_type const push_to = push_to_.load(std::memory_order::relaxed);
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);

    return pop_from <= push_to ? push_to - pop_from
                               : *capacity_ + push_to + 1 - pop_from;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> push_to_ = 0;
  index_type cached_pop_from_ = 0;

  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> pop_from_ = 0;
  index_type cached_push_to_ = 0;

  [[no_unique_address]] allocator_type allocator_;

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  Value * const ring_buffer_;

  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
      capacity_;

 private:
  constexpr auto full(index_type next_push_to,
                      index_type pop_from) const noexcept {
    return next_push_to == pop_from;
  }

  constexpr auto empty(index_type push_to, index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  constexpr auto next(index_type index) const noexcept -> index_type {
    return index == *capacity_ ? 0 : index + 1;
  }
};

}  // namespace flow
