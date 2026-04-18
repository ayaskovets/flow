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
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(Capacity > 0,
                  "unsafe_queue::unsafe_queue(): invalid capacity < 1");
  }

  constexpr explicit unsafe_queue(index_type capacity,
                                  allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([this] {
          if (*capacity_ == 0) {
            throw std::invalid_argument(
                "unsafe_queue::unsafe_queue(): invalid capacity < 1");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 *capacity_);
        }()) {}

  constexpr ~unsafe_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    while (try_pop([this](value_type & value) {
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    })) {
    }

    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto try_push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    if (full()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), *(ring_buffer_ + push_to()));

    size_ += 1;
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr void push(Callback && callback) {
    if (!try_push(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::push(): full");
    }
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto try_pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    if (empty()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), *(ring_buffer_ + pop_from_));

    pop_from_ = next(pop_from_, 1);
    size_ -= 1;
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr void pop(Callback && callback) {
    if (!try_pop(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::pop(): empty");
    }
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([this, ... args = std::forward<Args>(args)](
                        value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  template <typename... Args>
  constexpr void emplace(Args &&... args) {
    push([this,
          ... args = std::forward<Args>(args)](value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr void push(value_type value) { emplace(std::move(value)); }

  constexpr auto try_pop(value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return try_pop([this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
  }

  constexpr void pop(value_type & value) {
    pop([this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
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
  [[no_unique_address]] allocator_type allocator_;
  index_type pop_from_ = 0;
  index_type size_     = 0;
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  value_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return size_ == *capacity_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  [[nodiscard]] constexpr auto push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type distance) const noexcept
      -> index_type {
    return (index += distance) >= *capacity_ ? index - *capacity_ : index;
  }
};

}  // namespace flow
