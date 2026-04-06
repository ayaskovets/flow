#pragma once

#include <memory>
#include <stdexcept>
#include <type_traits>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>

namespace flow {

/**
 * @brief Fixed-size queue for single-threaded usage
 * @note Implemented as a ring buffer
 * @warning Not thread-safe
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_queue final : public swiss::non_copyable,
                           public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit unsafe_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : ring_buffer_(allocator.allocate(Capacity)),
        capacity_(),
        allocator_(std::move(allocator)) {
    static_assert(Capacity > 0,
                  "unsafe_queue::unsafe_queue(): invalid capacity < 1");
  }

  constexpr explicit unsafe_queue(index_type capacity,
                                  allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : ring_buffer_([capacity, &allocator] {
          if (capacity == 0) {
            throw std::invalid_argument(
                "unsafe_queue::unsafe_queue(): invalid capacity < 1");
          }
          return allocator.allocate(capacity);
        }()),
        capacity_(capacity),
        allocator_(std::move(allocator)) {}

  constexpr ~unsafe_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    while (!empty()) {
      std::allocator_traits<allocator_type>::destroy(allocator_,
                                                     ring_buffer_ + pop_from_);
      size_ -= 1;
    }

    allocator_.deallocate(ring_buffer_, *capacity_);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<Value, Args...>) -> bool {
    if (full()) {
      return false;
    }

    std::allocator_traits<allocator_type>::construct(
        allocator_, ring_buffer_ + next(pop_from_, size_),
        std::forward<Args>(args)...);

    size_ += 1;
    return true;
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args)
    requires(std::is_nothrow_constructible_v<Value, Args...>)
  {
    if (!try_emplace(std::forward<Args>(args)...)) {
      throw std::runtime_error("unsafe_queue::emplace(): full");
    }
  }

  constexpr void push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) {
    emplace(std::move(value));
  }

  constexpr auto try_pop(value_type & value) noexcept -> bool
    requires(std::is_nothrow_move_assignable_v<value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    if (empty()) {
      return false;
    }

    value = std::move(*(ring_buffer_ + pop_from_));
    std::allocator_traits<allocator_type>::destroy(allocator_,
                                                   ring_buffer_ + pop_from_);

    pop_from_ = next(pop_from_, 1);
    size_ -= 1;
    return true;
  }

  constexpr void pop(value_type & value) {
    if (!try_pop(value)) {
      throw std::runtime_error("unsafe_queue::pop(): empty");
    }
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    return size_;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  index_type pop_from_ = 0;
  index_type size_     = 0;
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  value_type * const ring_buffer_;
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] allocator_type allocator_;

 private:
  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return size_ == *capacity_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type distance) const noexcept
      -> index_type {
    return (index += distance) >= *capacity_ ? index - *capacity_ : index;
  }
};

}  // namespace flow
