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
      : ring_buffer_(allocator.allocate(Capacity)),
        capacity_(),
        allocator_(std::move(allocator)) {
    static_assert(Capacity > 0,
                  "unsafe_stream::unsafe_stream(): invalid capacity < 1");
  }

  constexpr explicit unsafe_stream(index_type capacity,
                                   allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : ring_buffer_([capacity, &allocator] {
          if (capacity == 0) {
            throw std::invalid_argument(
                "unsafe_stream::unsafe_stream(): invalid capacity < 1");
          }
          return allocator.allocate(capacity);
        }()),
        capacity_(capacity),
        allocator_(std::move(allocator)) {}

  constexpr ~unsafe_stream() noexcept {
    allocator_.deallocate(ring_buffer_, *capacity_);
  }

 public:
  constexpr auto try_push(std::span<std::byte const> const bytes) noexcept
      -> index_type {
    if (bytes.empty() || full()) {
      return 0;
    }

    index_type const push_to = next(pop_from_, size_);
    index_type const total =
        std::min(static_cast<index_type>(bytes.size()),
                 static_cast<index_type>(*capacity_ - size_));
    index_type const postfix = *capacity_ - push_to;
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(bytes.data(), chunk1, ring_buffer_ + push_to);
    std::copy_n(bytes.data() + chunk1, chunk2, ring_buffer_);

    size_ += total;
    return total;
  }

  constexpr void push(std::span<std::byte const> const bytes) {
    if (*capacity_ - size_ < bytes.size() || try_push(bytes) == 0) {
      throw std::runtime_error("unsafe_stream::push(): full");
    }
  }

  constexpr auto try_pop(std::span<std::byte> const bytes) noexcept
      -> index_type {
    if (bytes.empty() || empty()) {
      return 0;
    }

    index_type const total =
        std::min(static_cast<index_type>(bytes.size()), size_);
    index_type const postfix = *capacity_ - pop_from_;
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(ring_buffer_ + pop_from_, chunk1, bytes.data());
    std::copy_n(ring_buffer_, chunk2, bytes.data() + chunk1);

    size_ -= total;
    pop_from_ = next(pop_from_, total);
    return total;
  }

  constexpr void pop(std::span<std::byte> const bytes) {
    if (size_ < bytes.size() || try_pop(bytes) == 0) {
      throw std::runtime_error("unsafe_stream::pop(): empty");
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
  std::byte * const ring_buffer_;
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
                                    index_type bytes) const noexcept
      -> index_type {
    return (index += bytes) >= *capacity_ ? index - *capacity_ : index;
  }
};

}  // namespace flow
