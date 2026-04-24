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
  static constexpr index_type kHeaderSize    = sizeof(index_type);
  static constexpr index_type kPaddingHeader = 0;

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
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_push(index_type msg_size, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>>) -> bool {
    if (msg_size == 0) {
      return false;
    }

    index_type padding       = 0;
    index_type _push_to      = push_to();
    index_type const _free   = free();
    index_type const postfix = *capacity_ - _push_to;
    index_type const prefix  = pop_from_;
    index_type const chunk   = std::min(postfix, _free);

    // NOLINTNEXTLINE(bugprone-branch-clone)
    if (chunk >= kHeaderSize + msg_size) {
    } else if (postfix == kHeaderSize && prefix >= msg_size &&
               _free >= kHeaderSize + msg_size) {
    } else {
      if (chunk != postfix || prefix < kHeaderSize + msg_size) {
        return false;
      }

      if (postfix >= kHeaderSize) {
        std::copy_n(
            std::bit_cast<std::array<std::byte, kHeaderSize>>(kPaddingHeader)
                .data(),
            kHeaderSize, ring_buffer_ + _push_to);
      }

      padding  = postfix;
      _push_to = 0;
    }

    std::copy_n(
        std::bit_cast<std::array<std::byte, kHeaderSize>>(msg_size).data(),
        kHeaderSize, ring_buffer_ + _push_to);

    _push_to = next(_push_to, kHeaderSize);

    std::invoke(std::forward<Callback>(callback),
                std::span(ring_buffer_ + _push_to, msg_size));

    size_ += padding + kHeaderSize + msg_size;
    return true;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
      -> index_type {
    if (size_ == 0) {
      return 0;
    }

    index_type padding       = 0;
    index_type _pop_from     = pop_from_;
    index_type const postfix = *capacity_ - _pop_from;
    index_type msg_size =
        postfix < kHeaderSize ? kPaddingHeader : header(_pop_from);

    if (msg_size == kPaddingHeader) {
      padding   = postfix;
      _pop_from = 0;

      msg_size = header(_pop_from);
    }

    _pop_from = next(_pop_from, kHeaderSize);

    std::invoke(std::forward<Callback>(callback),
                std::span(ring_buffer_ + _pop_from, msg_size));

    pop_from_ = next(_pop_from, msg_size);
    size_ -= padding + kHeaderSize + msg_size;
    return msg_size;
  }

 public:
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr void push(index_type msg_size, Callback && callback) {
    if (!try_push(msg_size, std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_msgbuf::push(): full");
    }
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto pop(Callback && callback) -> index_type {
    index_type const msg_size = try_pop(std::forward<Callback>(callback));
    if (msg_size == 0) {
      throw std::runtime_error("unsafe_msgbuf::pop(): empty");
    }
    return msg_size;
  }

 public:
  constexpr auto try_push(std::span<std::byte const> msg) noexcept -> bool {
    return try_push(static_cast<index_type>(msg.size()),
                    [msg](std::span<std::byte> chunk) noexcept {
                      std::copy_n(msg.begin(), msg.size(), chunk.begin());
                    });
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> msg) noexcept
      -> index_type {
    return try_pop([msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(chunk.begin(), std::min(msg.size(), chunk.size()),
                  msg.begin());
    });
  }

 public:
  constexpr void push(std::span<std::byte const> msg) {
    push(msg.size(), [msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(msg.begin(), msg.size(), chunk.begin());
    });
  }

  [[nodiscard]] constexpr auto pop(std::span<std::byte> msg) -> index_type {
    return pop([msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(chunk.begin(), std::min(msg.size(), chunk.size()),
                  msg.begin());
    });
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    return size_;
  }

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

  [[nodiscard]] constexpr auto free() const noexcept -> index_type {
    return *capacity_ - size_;
  }

  [[nodiscard]] constexpr auto push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }

  [[nodiscard]] constexpr auto header(index_type index) const noexcept
      -> index_type {
    std::array<std::byte, kHeaderSize> msg_size{};
    std::copy_n(ring_buffer_ + index, kHeaderSize, msg_size.data());
    return std::bit_cast<index_type>(msg_size);
  }
};

}  // namespace flow
