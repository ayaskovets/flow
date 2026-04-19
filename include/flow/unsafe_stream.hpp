#pragma once

#include <algorithm>
#include <memory>
#include <span>
#include <stdexcept>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>

namespace flow {

/**
 * @brief Fixed-size stream for single-threaded usage
 * @note Implemented as a ring buffer
 * @warning Not thread-safe
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_stream final : public swiss::non_copyable,
                            public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit unsafe_stream(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(Capacity > 0,
                  "unsafe_stream::unsafe_stream(): invalid capacity < 1");
  }

  constexpr explicit unsafe_stream(index_type capacity,
                                   allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([this] {
          if (*capacity_ == 0) {
            throw std::invalid_argument(
                "unsafe_stream::unsafe_stream(): invalid capacity < 1");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 *capacity_);
        }()) {}

  constexpr ~unsafe_stream() noexcept {
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>>) -> bool {
    if (full()) {
      return false;
    }

    index_type const postfix = *capacity_ - push_to();
    index_type const chunk   = std::min(postfix, free());
    index_type const cb_total =
        std::invoke(std::forward<Callback>(callback),
                    std::span(ring_buffer_ + push_to(), chunk));

    size_ += cb_total;
    return true;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr void push(Callback && callback) {
    if (!try_push(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::push(): full");
    }
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_pop(Callback && callback) noexcept(noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)) -> bool {
    if (empty()) {
      return false;
    }

    index_type const postfix = *capacity_ - pop_from_;
    index_type const chunk   = std::min(postfix, size());
    index_type const cb_total =
        std::invoke(std::forward<Callback>(callback),
                    std::span(ring_buffer_ + pop_from_, chunk));

    pop_from_ = next(pop_from_, cb_total);
    size_ -= cb_total;
    return true;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr void pop(Callback && callback) {
    if (!try_pop(std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_queue::pop(): empty");
    }
  }

 public:
  constexpr auto try_push(std::span<std::byte const> bytes) noexcept
      -> index_type {
    index_type const total =
        std::min(static_cast<index_type>(bytes.size()), free());

    if (total == 0) {
      return 0;
    }

    index_type const postfix = *capacity_ - push_to();
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(bytes.data(), chunk1, ring_buffer_ + push_to());
    std::copy_n(bytes.data() + chunk1, chunk2, ring_buffer_);

    size_ += total;
    return total;
  }

  constexpr void push(std::span<std::byte const> bytes) {
    if (free() < static_cast<index_type>(bytes.size())) {
      throw std::runtime_error("unsafe_stream::push(): not enough space");
    }
    try_push(bytes);
  }

  constexpr auto try_pop(std::span<std::byte> bytes) noexcept -> index_type {
    index_type const total =
        std::min(static_cast<index_type>(bytes.size()), size());

    if (total == 0) {
      return 0;
    }

    index_type const postfix = *capacity_ - pop_from_;
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(ring_buffer_ + pop_from_, chunk1, bytes.data());
    std::copy_n(ring_buffer_, chunk2, bytes.data() + chunk1);

    size_ -= total;
    pop_from_ = next(pop_from_, total);
    return total;
  }

  constexpr void pop(std::span<std::byte> bytes) {
    if (size() < static_cast<index_type>(bytes.size())) {
      throw std::runtime_error("unsafe_stream::pop(): not enough data");
    }
    try_pop(bytes);
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
  std::byte * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    index += bytes;
    return index >= *capacity_ ? index - *capacity_ : index;
  }

  [[nodiscard]] constexpr auto full() const noexcept -> bool {
    return size_ == *capacity_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  [[nodiscard]] constexpr auto free() const noexcept -> index_type {
    return *capacity_ - size_;
  }

  [[nodiscard]] constexpr auto push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }
};

}  // namespace flow
