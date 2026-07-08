#pragma once

#include <limits>
#include <memory>
#include <span>
#include <stdexcept>

#include <swiss/concepts.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/math.hpp>
#include <swiss/mixins.hpp>

namespace flow {

/**
 * @brief Fixed-size stream for single-threaded usage
 * @note Implemented as a ring buffer
 * @note Callback may return a value of an integral type to restrict the number
 * of bytes that will be marked as read in the stream
 * @note Minimal capacity is 1
 * @warning Not thread-safe
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_stream : public swiss::non_copyable, public swiss::non_movable {
 private:
  template <typename T>
  static constexpr bool is_callback =
      std::invocable<T, std::span<std::byte>, std::span<std::byte>>;

  template <typename T>
  static constexpr bool is_void_callback =
      swiss::void_invocable<T, std::span<std::byte>, std::span<std::byte>>;

  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, std::span<std::byte>,
                                  std::span<std::byte>>;

 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return capacity > 0;
  }

 public:
  explicit constexpr unsafe_stream(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, Capacity)) {
    static_assert(valid(Capacity),
                  "unsafe_stream::unsafe_stream(): invalid capacity");
  }

  explicit constexpr unsafe_stream(index_type capacity,
                                   allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        capacity_(capacity),
        ring_buffer_([&] -> std::byte * {
          if (!valid(capacity)) {
            throw std::invalid_argument(
                "unsafe_stream::unsafe_stream(): invalid capacity");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {}

  constexpr ~unsafe_stream() noexcept {
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(
      Callback && callback,
      std::size_t _max_size = std::numeric_limits<
          std::size_t>::max()) noexcept(is_nothrow_callback<Callback>)
      -> std::size_t {
    index_type const max_size =
        std::min(swiss::bound<index_type>(_max_size), free());
    if (max_size == 0) {
      return 0;
    }

    auto const [postfix_span, prefix_span] = [&]() noexcept
        -> std::pair<std::span<std::byte>, std::span<std::byte>> {
      index_type const _push_to = push_to();
      index_type const postfix  = *capacity_ - _push_to;
      index_type const chunk    = std::min(max_size, postfix);
      index_type const prefix   = max_size - chunk;
      return std::pair{std::span<std::byte>(ring_buffer_ + _push_to, chunk),
                       std::span<std::byte>(ring_buffer_, prefix)};
    }();

    std::size_t _callback_size = max_size;
    swiss::assign_invoke_result_if_not_void(_callback_size,
                                            std::forward<Callback>(callback),
                                            postfix_span, prefix_span);
    index_type const bytes_pushed =
        std::min(max_size, swiss::bound<index_type>(_callback_size));

    size_ += bytes_pushed;
    return bytes_pushed;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(
      Callback && callback,
      std::size_t _max_size = std::numeric_limits<std::size_t>::max()) noexcept
      -> std::size_t {
    index_type const max_size =
        std::min(swiss::bound<index_type>(_max_size), size_);
    if (max_size == 0) {
      return 0;
    }

    auto const [postfix_span, prefix_span] = [&]() noexcept
        -> std::pair<std::span<std::byte>, std::span<std::byte>> {
      index_type const postfix = *capacity_ - pop_from_;
      index_type const chunk   = std::min(max_size, postfix);
      index_type const prefix  = max_size - chunk;
      return std::pair{std::span<std::byte>(ring_buffer_ + pop_from_, chunk),
                       std::span<std::byte>(ring_buffer_, prefix)};
    }();

    std::size_t _callback_size = max_size;
    swiss::assign_invoke_result_if_not_void(_callback_size,
                                            std::forward<Callback>(callback),
                                            postfix_span, prefix_span);
    index_type const bytes_popped =
        std::min(max_size, swiss::bound<index_type>(_callback_size));

    size_ -= bytes_popped;
    pop_from_ = next(pop_from_, bytes_popped);
    return bytes_popped;
  }

 public:
  template <typename Callback>
    requires(is_void_callback<Callback>)
  constexpr void push(std::size_t exact_size, Callback && callback) {
    if (exact_size > std::min(free(), *capacity_)) {
      throw std::runtime_error("unsafe_stream::push(): not enough space");
    }
    std::ignore = try_push(std::forward<Callback>(callback), exact_size);
  }

  template <typename Callback>
    requires(is_void_callback<Callback> && is_nothrow_callback<Callback>)
  constexpr void pop(std::size_t exact_size, Callback && callback) {
    if (exact_size > std::min(size(), *capacity_)) {
      throw std::runtime_error("unsafe_stream::pop(): not enough data");
    }
    std::ignore = try_pop(std::forward<Callback>(callback), exact_size);
  }

 public:
  [[nodiscard]] constexpr auto try_push(
      std::span<std::byte const> bytes) noexcept -> std::size_t {
    return try_push(
        [&](std::span<std::byte> postfix,
            std::span<std::byte> prefix) noexcept -> void {
          std::memcpy(postfix.data(), bytes.data(), postfix.size());
          std::memcpy(prefix.data(), bytes.subspan(postfix.size()).data(),
                      prefix.size());
        },
        bytes.size());
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> bytes) noexcept
      -> std::size_t {
    return try_pop(
        [&](std::span<std::byte> postfix,
            std::span<std::byte> prefix) noexcept -> void {
          std::memcpy(bytes.data(), postfix.data(), postfix.size());
          std::memcpy(bytes.subspan(postfix.size()).data(), prefix.data(),
                      prefix.size());
        },
        bytes.size());
  }

 public:
  constexpr void push(std::span<std::byte const> bytes) {
    push(bytes.size(),
         [&](std::span<std::byte> postfix,
             std::span<std::byte> prefix) noexcept -> void {
           std::memcpy(postfix.data(), bytes.data(), postfix.size());
           std::memcpy(prefix.data(), bytes.subspan(postfix.size()).data(),
                       prefix.size());
         });
  }

  constexpr void pop(std::span<std::byte> bytes) {
    pop(bytes.size(),
        [&](std::span<std::byte> postfix,
            std::span<std::byte> prefix) noexcept -> void {
          std::memcpy(bytes.data(), postfix.data(), postfix.size());
          std::memcpy(bytes.subspan(postfix.size()).data(), prefix.data(),
                      prefix.size());
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
    while (try_pop(
        [](std::span<std::byte>, std::span<std::byte>) noexcept -> void {})) {
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
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  std::byte * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

 private:
  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    index_type const postfix = *capacity_ - index;
    return (postfix > bytes) ? (index + bytes) : (bytes - postfix);
  }

  [[nodiscard]] constexpr auto free() const noexcept -> index_type {
    return *capacity_ - size_;
  }

  [[nodiscard]] constexpr auto push_to() const noexcept -> index_type {
    return next(pop_from_, size_);
  }
};

}  // namespace flow
