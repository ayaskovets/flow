#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <thread>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

namespace flow {

/**
 * @brief Fixed-size stream for 1 producer and 1 consumer
 * @note Implemented as a ring buffer
 * @note Capacity must be a power of 2
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_stream : public swiss::non_copyable, public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit spsc_stream(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "spsc_stream::spsc_stream(): invalid capacity != 2^n");
  }

  constexpr explicit spsc_stream(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity)) {
                    throw std::invalid_argument(
                        "spsc_stream::spsc_stream(): invalid capacity != 2^n");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  constexpr ~spsc_stream() noexcept {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
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
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);
    index_type _free         = free(*writer_.capacity, push_to, writer_.cache);

    if (max_size > _free) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      _free         = free(*writer_.capacity, push_to, writer_.cache);
    }

    index_type total = std::min(max_size, _free);

    if (total == 0) {
      return 0;
    }

    index_type const _slot     = slot(*writer_.capacity, push_to);
    index_type const postfix   = *writer_.capacity - _slot;
    index_type const chunk     = std::min(total, postfix);
    index_type const remainder = total - chunk;

    index_type callback_size = total;
    swiss::assign_invoke_result_if_not_void(
        callback_size, std::forward<Callback>(callback),
        std::span(writer_.ring_buffer + _slot, chunk),
        std::span(writer_.ring_buffer, remainder));
    total = std::min(total, callback_size);

    writer_.index.store(next(push_to, total), std::memory_order::release);
    return total;
  }

  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr auto try_pop(
      Callback && callback,
      index_type max_size = std::numeric_limits<index_type>::
          max()) noexcept(std::is_nothrow_invocable_v<Callback,
                                                      std::span<std::byte>,
                                                      std::span<std::byte>>)
      -> index_type {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type _size          = size(reader_.cache, pop_from);

    if (max_size > _size) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      _size         = size(reader_.cache, pop_from);
    }

    index_type total = std::min(max_size, _size);

    if (total == 0) {
      return 0;
    }

    index_type const _slot     = slot(*reader_.capacity, pop_from);
    index_type const postfix   = *reader_.capacity - _slot;
    index_type const chunk     = std::min(total, postfix);
    index_type const remainder = total - chunk;

    index_type callback_size = total;
    swiss::assign_invoke_result_if_not_void(
        callback_size, std::forward<Callback>(callback),
        std::span(reader_.ring_buffer + _slot, chunk),
        std::span(reader_.ring_buffer, remainder));
    total = std::min(total, callback_size);

    reader_.index.store(next(pop_from, total), std::memory_order::release);
    return total;
  }

 public:
  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr auto push(
      std::stop_token const & stop_token, index_type size,
      Callback &&
          callback) noexcept(std::is_nothrow_invocable_v<Callback,
                                                         std::span<std::byte>,
                                                         std::span<std::byte>>)
      -> bool
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    if (size > *writer_.capacity) {
      return false;
    }

    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (size <= free(*writer_.capacity, push_to, writer_.cache)) {
        std::ignore = try_push(std::forward<Callback>(callback), size);

        writer_.index.store(next(push_to, size), std::memory_order::release);
        return true;
      }

      std::this_thread::yield();

      writer_.cache = reader_.index.load(std::memory_order::acquire);
    }

    return false;
  }

  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr auto pop(
      std::stop_token const & stop_token, index_type _size,
      Callback &&
          callback) noexcept(std::is_nothrow_invocable_v<Callback,
                                                         std::span<std::byte>,
                                                         std::span<std::byte>>)
      -> bool
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    if (_size > *reader_.capacity) {
      return false;
    }

    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (_size <= size(reader_.cache, pop_from)) {
        std::ignore = try_pop(std::forward<Callback>(callback), _size);

        reader_.index.store(next(pop_from, _size), std::memory_order::release);
        return true;
      }

      std::this_thread::yield();

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    return false;
  }

 public:
  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr void push(index_type size, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>,
                                  std::span<std::byte>>)
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    push(std::stop_token{}, size, std::forward<Callback>(callback));
  }

  template <std::invocable<std::span<std::byte>, std::span<std::byte>> Callback>
  constexpr void pop(index_type _size, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, std::span<std::byte>,
                                  std::span<std::byte>>)
    requires(std::is_same_v<std::invoke_result_t<Callback, std::span<std::byte>,
                                                 std::span<std::byte>>,
                            void>)
  {
    pop(std::stop_token{}, _size, std::forward<Callback>(callback));
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
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> bytes) noexcept -> bool {
    return push(stop_token, static_cast<index_type>(bytes.size()),
                [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
                  std::copy_n(bytes.data(), postfix.size(), postfix.data());
                  std::copy_n(bytes.subspan(postfix.size()).data(),
                              prefix.size(), prefix.data());
                });
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> bytes) noexcept -> bool {
    return pop(stop_token, static_cast<index_type>(bytes.size()),
               [&](std::span<std::byte> postfix, std::span<std::byte> prefix) {
                 std::copy_n(postfix.data(), postfix.size(), bytes.data());
                 std::copy_n(prefix.data(), prefix.size(),
                             bytes.subspan(postfix.size()).data());
               });
  }

 public:
  constexpr void push(std::span<std::byte const> bytes) noexcept {
    push(std::stop_token{}, bytes);
  }

  constexpr void pop(std::span<std::byte> bytes) noexcept {
    pop(std::stop_token{}, bytes);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    auto const _size = size(push_to, pop_from);
    return _size > *writer_.capacity ? 0 : _size;
  }

  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    std::byte * const ring_buffer;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  [[no_unique_address]] allocator_type allocator_;
  CacheLine writer_;
  CacheLine reader_;

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto free(index_type capacity, index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return capacity - size(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto next(index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    return index + bytes;
  }

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }
};

}  // namespace flow
