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
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

namespace flow {

/**
 * @brief Fixed-size message buffer for many producers and 1 consumer
 * @note Implemented as a ring buffer
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpsc_msgbuf : public swiss::non_copyable, public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  static constexpr index_type kHeaderSize    = sizeof(index_type);
  static constexpr index_type kPaddingHeader = 0;

 public:
  constexpr explicit mpsc_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity != 2^n");
  }

  constexpr explicit mpsc_msgbuf(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity)) {
                    throw std::invalid_argument(
                        "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity != 2^n");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  constexpr ~mpsc_msgbuf() noexcept {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_push(index_type msg_size,
                          Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    if (msg_size == 0) {
      return false;
    }

    index_type push_to = writer_.index.load(std::memory_order::relaxed);
    index_type const postfix =
        *writer_.capacity - slot(*writer_.capacity, push_to);
    index_type const required = kHeaderSize + msg_size;

    index_type prefix = slot(*writer_.capacity, writer_.cache);
    index_type _free  = free(*writer_.capacity, push_to, writer_.cache);
    index_type chunk  = std::min(postfix, _free);

    if (chunk < required || prefix < msg_size) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);

      prefix = slot(*writer_.capacity, writer_.cache);
      _free  = free(*writer_.capacity, push_to, writer_.cache);
      chunk  = std::min(postfix, _free);
    }

    bool prepared = false;
    // NOLINTNEXTLINE(bugprone-branch-clone)
    if (chunk >= required) {
    } else if (postfix == kHeaderSize && prefix >= msg_size &&
               _free >= required) {
    } else {
      if (chunk != postfix || prefix < required) {
        return false;
      }

      if (!writer_.prepare.compare_exchange_weak(
              push_to, next(push_to, postfix + kHeaderSize + msg_size),
              std::memory_order::relaxed, std::memory_order::relaxed)) {
        return false;
      }

      if (postfix >= kHeaderSize) {
        write_header(writer_.ring_buffer, *writer_.capacity, push_to,
                     kPaddingHeader);
      }
      push_to  = next(push_to, postfix);
      prepared = true;
    }

    if (!prepared &&
        !writer_.prepare.compare_exchange_weak(
            push_to, next(push_to, kHeaderSize + msg_size),
            std::memory_order::relaxed, std::memory_order::relaxed)) {
      return false;
    }

    write_header(writer_.ring_buffer, *writer_.capacity, push_to, msg_size);
    push_to = next(push_to, kHeaderSize);

    std::invoke(callback, std::span(writer_.ring_buffer +
                                        slot(*writer_.capacity, push_to),
                                    msg_size));

    writer_.index.store(next(push_to, msg_size), std::memory_order::release);
    return true;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_pop(Callback const & callback) noexcept -> index_type
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);

    if (empty(reader_.cache, pop_from)) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      if (empty(reader_.cache, pop_from)) {
        return 0;
      }
    }

    index_type const postfix =
        *reader_.capacity - slot(*reader_.capacity, pop_from);
    index_type msg_size =
        postfix < kHeaderSize
            ? kPaddingHeader
            : read_header(reader_.ring_buffer, *reader_.capacity, pop_from);

    if (msg_size == kPaddingHeader) {
      pop_from = next(pop_from, postfix);
      msg_size = read_header(reader_.ring_buffer, *reader_.capacity, pop_from);
    }

    pop_from = next(pop_from, kHeaderSize);

    std::invoke(callback, std::span(reader_.ring_buffer +
                                        slot(*reader_.capacity, pop_from),
                                    msg_size));

    reader_.index.store(next(pop_from, msg_size), std::memory_order::release);
    return msg_size;
  }

 public:
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto push(std::stop_token const & stop_token, index_type msg_size,
                      Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    index_type const required = kHeaderSize + msg_size;
    if (required > *writer_.capacity) {
      return false;
    }

    while (!stop_token.stop_requested()) {
      if (try_push(msg_size, callback)) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback const & callback) noexcept -> bool
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    while (!stop_token.stop_requested()) {
      if (try_pop(callback) != 0) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

 public:
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr void push(index_type msg_size, Callback const & callback) noexcept
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    push(std::stop_token{}, msg_size, callback);
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto pop(Callback const & callback) noexcept -> index_type
    requires(std::is_nothrow_invocable_v<Callback, std::span<std::byte>>)
  {
    index_type msg_size{};
    pop(std::stop_token{}, [&](std::span<std::byte> chunk) noexcept {
      msg_size = chunk.size();
      std::invoke(callback, chunk);
    });
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
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> msg) noexcept -> bool {
    return push(stop_token, msg.size(),
                [msg](std::span<std::byte> chunk) noexcept {
                  std::copy_n(msg.begin(), msg.size(), chunk.begin());
                });
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> msg) noexcept -> bool {
    return pop(stop_token, [msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(chunk.begin(), std::min(msg.size(), chunk.size()),
                  msg.begin());
    });
  }

 public:
  constexpr void push(std::span<std::byte const> msg) noexcept {
    push(std::stop_token{}, std::move(msg));
  }

  [[nodiscard]] constexpr auto pop(std::span<std::byte> msg) noexcept
      -> index_type {
    return pop([msg](std::span<std::byte> chunk) noexcept {
      std::copy_n(chunk.begin(), std::min(msg.size(), chunk.size()),
                  msg.begin());
    });
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
    std::atomic<index_type> prepare = 0;
    std::atomic<index_type> index   = 0;
    index_type cache;
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

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
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

  constexpr void write_header(std::byte * ring_buffer, index_type capacity,
                              index_type index,
                              index_type msg_size) const noexcept {
    std::copy_n(
        std::bit_cast<std::array<std::byte, kHeaderSize>>(msg_size).data(),
        kHeaderSize, ring_buffer + slot(capacity, index));
  }

  [[nodiscard]] constexpr auto read_header(std::byte const * ring_buffer,
                                           index_type capacity,
                                           index_type index) const noexcept
      -> index_type {
    std::array<std::byte, kHeaderSize> msg_size{};
    std::copy_n(ring_buffer + slot(capacity, index), kHeaderSize,
                msg_size.data());
    return std::bit_cast<index_type>(msg_size);
  }
};

}  // namespace flow
