#pragma once

#include <memory>
#include <stdexcept>

#include <swiss/dynamic_extent.hpp>
#include <swiss/ebo.hpp>
#include <swiss/mixins.hpp>
#include <swiss/scope_exit.hpp>

namespace flow {

/**
 * @brief Fixed-size queue for single-threaded usage
 * @note Implemented as a ring buffer
 * @note Minimal capacity is 1
 * @warning Not thread-safe
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_queue : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type      = Value;
  using reference       = Value &;
  using const_reference = Value const &;
  using pointer         = Value *;
  using const_pointer   = Value const *;
  using index_type      = Index;
  using allocator_type  = Allocator;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return capacity > 0;
  }

 public:
  explicit constexpr unsafe_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(valid(Capacity),
                  "unsafe_queue::unsafe_queue(): invalid capacity");
  }

  explicit constexpr unsafe_queue(index_type capacity,
                                  allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([&] -> pointer {
          if (!valid(capacity)) {
            throw std::invalid_argument(
                "unsafe_queue::unsafe_queue(): invalid capacity");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {}

  constexpr ~unsafe_queue() noexcept {
    clear();
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([&](pointer front) -> void {
      std::allocator_traits<allocator_type>::construct(
          allocator_, front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(const_reference value) noexcept(
      std::is_nothrow_copy_constructible_v<value_type>) -> bool {
    return try_emplace(value);
  }

  constexpr auto try_push(value_type && value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(auto & value) noexcept(
      std::is_nothrow_assignable_v<decltype(value), value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return try_pop([&](pointer front) -> void {
      swiss::scope_exit const scope_exit([&]() noexcept -> void {
        std::allocator_traits<allocator_type>::destroy(allocator_, front);
      });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr void emplace(Args &&... args) {
    push([&](pointer front) -> void {
      std::allocator_traits<allocator_type>::construct(
          allocator_, front, std::forward<Args>(args)...);
    });
  }

  constexpr void push(const_reference value) { emplace(value); }

  constexpr void push(value_type && value) { emplace(std::move(value)); }

  constexpr void pop(auto & value) {
    pop([&](pointer front) -> void {
      swiss::scope_exit const scope_exit([&]() noexcept -> void {
        std::allocator_traits<allocator_type>::destroy(allocator_, front);
      });
      value = std::move(*front);
    });
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return size_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  constexpr void clear() noexcept {
    while (try_pop([&](pointer front) noexcept -> void {
      std::allocator_traits<allocator_type>::destroy(allocator_, front);
    })) {
    }
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

 private:
  [[no_unique_address]] allocator_type allocator_;

  index_type pop_from_ = 0;
  index_type size_     = 0;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::ebo<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  value_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return size_ == *capacity_;
  }

  [[nodiscard]] constexpr auto push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type distance) const noexcept
      -> index_type {
    index_type const postfix = *capacity_ - index;
    return (postfix > distance) ? (index + distance) : (distance - postfix);
  }

 private:
  template <typename Callback>
  static constexpr bool is_callback = std::invocable<Callback, Value *>;

  template <typename Callback>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<Callback, Value *>;

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(Callback && callback) noexcept(
      is_nothrow_callback<Callback>) -> bool {
    if (full()) {
      return false;
    }

    std::invoke(std::forward<Callback>(callback), ring_buffer_ + push_to());
    ++size_;
    return true;
  }

  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept(
      is_nothrow_callback<Callback>) -> bool {
    if (empty()) {
      return false;
    }

    index_type const pop_from = pop_from_;
    pop_from_                 = next(pop_from_, 1);
    size_ -= 1;
    std::invoke(std::forward<Callback>(callback), ring_buffer_ + pop_from);
    return true;
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr void push(Callback && callback) {
    if (!try_push(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::push(): full");
    }
  }

  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr void pop(Callback && callback) {
    if (!try_pop(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::pop(): empty");
    }
  }
};

}  // namespace flow
