#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <type_traits>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/mixins.hpp>

namespace flow {

/**
 * @brief Fixed-size stream for single-threaded usage
 * @note Implemented as a ring buffer
 * @note If callback returns a value of index type, only a prefix of size equal
 * to the returned value is affected by the operation
 * @warning Not thread-safe
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unsafe_stream : public swiss::non_copyable, public swiss::non_movable {
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
        ring_buffer_([&] {
          if (capacity < 1) {
            throw std::invalid_argument(
                "unsafe_stream::unsafe_stream(): invalid capacity < 1");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 capacity);
        }()) {}

  constexpr ~unsafe_stream() noexcept {
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      *capacity_);
  }

 public:
  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr auto try_push(
      Callback && callback,
      index_type max_size = std::numeric_limits<index_type>::
          max()) noexcept(std::is_nothrow_invocable_v<Callback,
                                                      std::span<std::byte>,
                                                      std::span<std::byte>>)
      -> index_type {
    max_size = std::min(max_size, free());
    if (max_size == 0) {
      return 0;
    }

    index_type const _push_to  = push_to();
    index_type const postfix   = *capacity_ - _push_to;
    index_type const chunk     = std::min(max_size, postfix);
    index_type const remainder = max_size - chunk;

    index_type callback_size = max_size;
    swiss::assign_invoke_result_if_not_void(
        callback_size, std::forward<Callback>(callback),
        std::span(ring_buffer_ + _push_to, chunk),
        std::span(ring_buffer_, remainder));
    max_size = std::min(max_size, callback_size);

    size_ += max_size;
    return max_size;
  }

  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr auto try_pop(
      Callback && callback,
      index_type max_size = std::numeric_limits<index_type>::
          max()) noexcept(std::is_nothrow_invocable_v<Callback,
                                                      std::span<std::byte>,
                                                      std::span<std::byte>>)
      -> index_type {
    max_size = std::min(max_size, size());
    if (max_size == 0) {
      return 0;
    }

    index_type const postfix   = *capacity_ - pop_from_;
    index_type const chunk     = std::min(max_size, postfix);
    index_type const remainder = max_size - chunk;

    index_type callback_size = max_size;
    swiss::assign_invoke_result_if_not_void(
        callback_size, std::forward<Callback>(callback),
        std::span(ring_buffer_ + pop_from_, chunk),
        std::span(ring_buffer_, remainder));
    max_size = std::min(max_size, callback_size);

    size_ -= max_size;
    pop_from_ = next(pop_from_, max_size);
    return max_size;
  }

 public:
  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr void push(index_type size, Callback && callback)
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    if (free() < size) {
      throw std::runtime_error("unsafe_stream::push(): not enough space");
    }
    std::ignore = try_push(std::forward<Callback>(callback), size);
  }

  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr void pop(index_type _size, Callback && callback)
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    if (size() < _size) {
      throw std::runtime_error("unsafe_stream::pop(): not enough data");
    }
    std::ignore = try_pop(std::forward<Callback>(callback), _size);
  }

 public:
  [[nodiscard]] constexpr auto try_push(
      std::span<std::byte const> bytes) noexcept -> index_type {
    return try_push(
        [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
          std::copy_n(bytes.data(), postfix.size(), postfix.data());
          std::copy_n(bytes.subspan(postfix.size()).data(), prefix.size(),
                      prefix.data());
        },
        static_cast<index_type>(bytes.size()));
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> bytes) noexcept
      -> index_type {
    return try_pop(
        [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
          std::copy_n(postfix.data(), postfix.size(), bytes.data());
          std::copy_n(prefix.data(), prefix.size(),
                      bytes.subspan(postfix.size()).data());
        },
        static_cast<index_type>(bytes.size()));
  }

 public:
  constexpr void push(std::span<std::byte const> bytes) {
    push(static_cast<index_type>(bytes.size()),
         [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
           std::copy_n(bytes.data(), postfix.size(), postfix.data());
           std::copy_n(bytes.subspan(postfix.size()).data(), prefix.size(),
                       prefix.data());
         });
  }

  constexpr void pop(std::span<std::byte> bytes) {
    pop(static_cast<index_type>(bytes.size()),
        [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
          std::copy_n(postfix.data(), postfix.size(), bytes.data());
          std::copy_n(prefix.data(), prefix.size(),
                      bytes.subspan(postfix.size()).data());
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
    return (*capacity_ - index > bytes) ? (index + bytes)
                                        : (bytes - (*capacity_ - index));
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
