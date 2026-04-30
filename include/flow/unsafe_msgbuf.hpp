#pragma once

#include <algorithm>
#include <memory>
#include <span>
#include <stdexcept>
#include <type_traits>

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
class unsafe_msgbuf : public swiss::non_copyable, public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  template <typename T>
  static constexpr bool is_callback = std::invocable<T, std::span<std::byte>>;

  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, std::span<std::byte>>;

 public:
  constexpr explicit unsafe_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(
        Capacity >= header_size(),
        "unsafe_msgbuf::unsafe_msgbuf(): invalid capacity < header_size()");
  }

  constexpr explicit unsafe_msgbuf(index_type capacity,
                                   allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([&] {
          if (capacity < header_size()) {
            throw std::invalid_argument(
                "unsafe_msgbuf::unsafe_msgbuf(): invalid capacity < "
                "header_size()");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {}

  constexpr ~unsafe_msgbuf() noexcept {
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(std::size_t _msg_size, Callback && callback) noexcept(
      is_nothrow_callback<Callback>) -> bool {
    if (!is_valid_input_size(_msg_size)) {
      return false;
    }

    index_type const msg_size           = bound_to_index_type(_msg_size);
    auto const [fits, padding, push_to] = [&]() {
      index_type const _push_to = get_push_to();
      index_type const _free    = free();
      index_type const postfix  = *capacity_ - _push_to;
      index_type const prefix   = pop_from_;
      index_type const chunk    = std::min(postfix, _free);

      bool const can_insert_whole = (chunk >= header_size() + msg_size);
      bool const can_split_header =
          (_free >= header_size() + msg_size && postfix == header_size() &&
           prefix >= msg_size);
      if (can_insert_whole || can_split_header) {
        return std::tuple{true, static_cast<index_type>(0), _push_to};
      }

      bool const can_insert_with_padding =
          (chunk == postfix && prefix >= header_size() + msg_size);
      if (can_insert_with_padding) {
        if (postfix >= header_size()) {
          write_header(_push_to, padding_header());
        }
        return std::tuple{true, postfix, static_cast<index_type>(0)};
      }

      return std::tuple{false, static_cast<index_type>(0),
                        static_cast<index_type>(0)};
    }();

    if (!fits) {
      return false;
    }

    std::span<std::byte> const msg(ring_buffer_ + next(push_to, header_size()),
                                   msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    write_header(push_to, msg_size);
    size_ += padding + header_size() + msg_size;
    return true;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> std::size_t {
    if (empty()) {
      return 0;
    }

    auto const [padding, pop_from, msg_size] = [&]() {
      index_type const postfix = *capacity_ - pop_from_;
      index_type const header =
          postfix < header_size() ? padding_header() : read_header(pop_from_);

      if (header == padding_header()) {
        return std::tuple{postfix, static_cast<index_type>(header_size()),
                          read_header(0)};
      }

      return std::tuple{static_cast<index_type>(0),
                        next(pop_from_, header_size()), header};
    }();

    std::span<std::byte> const msg(ring_buffer_ + pop_from, msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    pop_from_ = next(pop_from, msg_size);
    size_ -= padding + header_size() + msg_size;
    return msg_size;
  }

 public:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr void push(std::size_t msg_size, Callback && callback) {
    if (!try_push(msg_size, std::forward<Callback>(callback))) {
      throw std::runtime_error("unsafe_msgbuf::push(): full");
    }
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(Callback && callback) -> std::size_t {
    std::size_t const msg_size = try_pop(std::forward<Callback>(callback));
    if (msg_size == 0) {
      throw std::runtime_error("unsafe_msgbuf::pop(): empty");
    }
    return msg_size;
  }

 public:
  constexpr auto try_push(std::span<std::byte const> msg) noexcept -> bool {
    return try_push(msg.size(), [msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(msg.begin(), msg.size(), chunk.begin());
    });
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> msg) noexcept
      -> std::size_t {
    if (msg.empty()) {
      return 0;
    }

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

  [[nodiscard]] constexpr auto pop(std::span<std::byte> msg) -> std::size_t {
    if (msg.empty()) {
      return 0;
    }

    return pop([msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(chunk.begin(), std::min(msg.size(), chunk.size()),
                  msg.begin());
    });
  }

 public:
  [[nodiscard]] static constexpr auto header_size() noexcept -> std::size_t {
    return sizeof(index_type);
  }

  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return size_;
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
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  std::byte * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto bound_to_index_type(
      std::size_t size) const noexcept -> index_type {
    return static_cast<index_type>(
        std::min<std::size_t>(std::numeric_limits<index_type>::max(), size));
  }

  [[nodiscard]] constexpr auto is_valid_input_size(
      std::size_t bytes) const noexcept -> bool {
    return bytes != 0 && bytes <= *capacity_ - header_size();
  }

  [[nodiscard]] constexpr auto padding_header() const noexcept -> index_type {
    return 0;
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    index_type const postfix = *capacity_ - index;
    return (postfix > bytes) ? (index + bytes) : (bytes - postfix);
  }

  [[nodiscard]] constexpr auto free() const noexcept -> index_type {
    return *capacity_ - size_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  [[nodiscard]] constexpr auto get_push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }

  constexpr void write_header(index_type index,
                              index_type msg_size) const noexcept {
    std::copy_n(
        std::bit_cast<std::array<std::byte, header_size()>>(msg_size).data(),
        header_size(), ring_buffer_ + index);
  }

  [[nodiscard]] constexpr auto read_header(index_type index) const noexcept
      -> index_type {
    std::array<std::byte, header_size()> msg_size{};
    std::copy_n(ring_buffer_ + index, header_size(), msg_size.data());
    return std::bit_cast<index_type>(msg_size);
  }
};

}  // namespace flow
