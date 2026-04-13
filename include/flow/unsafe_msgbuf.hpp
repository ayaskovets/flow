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
 * @brief Fixed-size message buffer for single-threaded usage
 * @note Implemented as a ring buffer
 * @warning Not thread-safe
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_msgbuf final : public swiss::non_copyable,
                            public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  static constexpr index_type kMsgSizeBytes = sizeof(index_type);

 public:
  constexpr explicit unsafe_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(Capacity > 0,
                  "unsafe_msgbuf::unsafe_msgbuf(): invalid capacity < 1");
  }

  constexpr explicit unsafe_msgbuf(index_type capacity,
                                   allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([this] {
          if (*capacity_ == 0) {
            throw std::invalid_argument(
                "unsafe_msgbuf::unsafe_msgbuf(): invalid capacity < 1");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 *capacity_);
        }()) {}

  constexpr ~unsafe_msgbuf() noexcept {
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  constexpr auto try_push(std::span<std::byte const> bytes) noexcept -> bool {
    auto const msg_size = static_cast<index_type>(bytes.size());

    if (msg_size == 0 || free() < kMsgSizeBytes + msg_size) {
      return false;
    }

    write_unchecked(
        std::bit_cast<std::array<std::byte, kMsgSizeBytes>>(msg_size));
    size_ += kMsgSizeBytes;

    write_unchecked(bytes);
    size_ += msg_size;

    return true;
  }

  constexpr void push(std::span<std::byte const> bytes) {
    if (free() < kMsgSizeBytes + bytes.size()) {
      throw std::runtime_error("unsafe_msgbuf::push(): full");
    }
    try_push(bytes);
  }

  constexpr auto try_pop(std::span<std::byte> bytes) noexcept -> bool {
    auto const msg_size = peek();
    auto const total    = static_cast<index_type>(bytes.size());

    if (msg_size == 0 || msg_size != total) {
      return false;
    }

    pop_from_ = next(pop_from_, kMsgSizeBytes);
    size_ -= kMsgSizeBytes;

    read_unchecked(bytes);
    pop_from_ = next(pop_from_, total);
    size_ -= total;
    return total;
  }

  constexpr void pop(std::span<std::byte> bytes) {
    if (peek() != bytes.size()) {
      throw std::runtime_error("unsafe_msgbuf::pop(): empty");
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

  [[nodiscard]] constexpr auto peek() const noexcept -> index_type {
    if (size_ == 0) {
      return 0;
    }

    std::array<std::byte, kMsgSizeBytes> msg_size{};
    read_unchecked(msg_size);
    return std::bit_cast<index_type>(msg_size);
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
  [[nodiscard]] constexpr auto free() const noexcept -> index_type {
    return *capacity_ - size_;
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    index += bytes;
    return index >= *capacity_ ? index - *capacity_ : index;
  }

 private:
  constexpr void write_unchecked(
      std::span<std::byte const> bytes) const noexcept {
    auto const total         = static_cast<index_type>(bytes.size());
    index_type const push_to = next(pop_from_, size_);
    index_type const postfix = *capacity_ - push_to;
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(bytes.data(), chunk1, ring_buffer_ + push_to);
    std::copy_n(bytes.data() + chunk1, chunk2, ring_buffer_);
  }

  constexpr void read_unchecked(std::span<std::byte> bytes) const noexcept {
    auto const total         = static_cast<index_type>(bytes.size());
    index_type const postfix = *capacity_ - pop_from_;
    index_type const chunk1  = std::min(total, postfix);
    index_type const chunk2  = total - chunk1;

    std::copy_n(ring_buffer_ + pop_from_, chunk1, bytes.data());
    std::copy_n(ring_buffer_, chunk2, bytes.data() + chunk1);
  }
};

}  // namespace flow
