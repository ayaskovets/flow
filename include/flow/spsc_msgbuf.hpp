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
#include <swiss/variadic_traits.hpp>

namespace flow {

/**
 * @brief Fixed-size message buffer for 1 producer and 1 consumer
 * @note Implemented as a ring buffer
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_msgbuf final : public swiss::non_copyable,
                          public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  static constexpr index_type kMsgSizeBytes = sizeof(index_type);

 public:
  constexpr explicit spsc_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity + 1)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(Capacity > 0,
                  "spsc_msgbuf::spsc_msgbuf(): invalid capacity < 1");
  }

  constexpr explicit spsc_msgbuf(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.capacity{capacity},
                .ring_buffer =
                    [this] {
                      if (*writer_.capacity == 0) {
                        throw std::invalid_argument(
                            "spsc_msgbuf::spsc_msgbuf(): invalid capacity < 1");
                      }
                      return std::allocator_traits<allocator_type>::allocate(
                          allocator_, *writer_.capacity + 1);
                    }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  constexpr ~spsc_msgbuf() noexcept {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity + 1);
  }

 public:
  constexpr auto try_push(std::span<std::byte const> msg) noexcept -> bool {
    return push_impl<false, false>(msg);
  }

  constexpr auto push(std::stop_token stop_token,
                      std::span<std::byte const> msg) noexcept -> bool {
    return push_impl<true, true>(stop_token, msg);
  }

  constexpr void push(std::span<std::byte const> msg) noexcept {
    push_impl<true, false>(msg);
  }

  constexpr auto try_pop(std::span<std::byte> msg) noexcept -> bool {
    return pop_impl<false, false>(msg);
  }

  constexpr auto pop(std::stop_token stop_token,
                     std::span<std::byte> msg) noexcept -> bool {
    return pop_impl<true, true>(stop_token, msg);
  }

  constexpr void pop(std::span<std::byte> msg) noexcept {
    pop_impl<true, false>(msg);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    return size(*reader_.capacity, push_to, pop_from);
  }

  [[nodiscard]] constexpr auto peek() noexcept -> index_type {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    if (peek(reader_.ring_buffer, *reader_.capacity, reader_.cache, pop_from) ==
        0) {
      reader_.cache = writer_.index.load(std::memory_order::relaxed);
    }
    return peek(reader_.ring_buffer, *reader_.capacity, reader_.cache,
                pop_from);
  }

 public:
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
  [[nodiscard]] constexpr auto size(index_type capacity, index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return pop_from <= push_to ? push_to - pop_from
                               : capacity + push_to + 1 - pop_from;
  }

  [[nodiscard]] constexpr auto free(index_type capacity, index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return capacity - size(capacity, push_to, pop_from);
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  [[nodiscard]] constexpr auto next(index_type capacity, index_type msg,
                                    index_type index) const noexcept
      -> index_type {
    index += msg;
    return index > capacity ? index - capacity - 1 : index;
  }

  [[nodiscard]] constexpr auto peek(
      // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
      std::byte const * ring_buffer, index_type capacity, index_type push_to,
      index_type pop_from) const noexcept -> index_type {
    if (empty(push_to, pop_from)) {
      return 0;
    }

    std::array<std::byte, kMsgSizeBytes> msg_size{};
    read_unchecked(msg_size, ring_buffer, capacity, pop_from);
    return std::bit_cast<index_type>(msg_size);
  }

 private:
  constexpr void write_unchecked(std::span<std::byte const> msg,
                                 std::byte * ring_buffer, index_type capacity,
                                 index_type push_to) const noexcept {
    auto const msg_size      = static_cast<index_type>(msg.size());
    index_type const postfix = capacity - push_to + 1;
    index_type const chunk1  = std::min(msg_size, postfix);
    index_type const chunk2  = msg_size - chunk1;

    std::copy_n(msg.data(), chunk1, ring_buffer + push_to);
    std::copy_n(msg.data() + chunk1, chunk2, ring_buffer);
  }

  constexpr void read_unchecked(std::span<std::byte> msg,
                                std::byte const * ring_buffer,
                                index_type capacity,
                                index_type pop_from) const noexcept {
    auto const msg_size      = static_cast<index_type>(msg.size());
    index_type const postfix = capacity - pop_from + 1;
    index_type const chunk1  = std::min(msg_size, postfix);
    index_type const chunk2  = msg_size - chunk1;

    std::copy_n(ring_buffer + pop_from, chunk1, msg.data());
    std::copy_n(ring_buffer, chunk2, msg.data() + chunk1);
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto push_impl(Args &&... args) -> decltype(auto) {
    std::span<std::byte const> const msg =
        swiss::variadic_nth<Cancellable ? 1 : 0>(args...);

    if constexpr (Blocking) {
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        if (push_impl<false, false>(msg)) {
          if constexpr (Cancellable) {
            return true;
          } else {
            break;
          }
        }

        std::this_thread::yield();
      }
    }

    if constexpr (!Blocking) {
      auto const msg_size = static_cast<index_type>(msg.size());

      if (msg_size == 0) {
        return false;
      }

      index_type const push_to = writer_.index.load(std::memory_order::relaxed);

      if (free(*writer_.capacity, push_to, writer_.cache) <
          kMsgSizeBytes + msg_size) {
        index_type const cache = reader_.index.load(std::memory_order::relaxed);
        if (free(*writer_.capacity, push_to, cache) <
            kMsgSizeBytes + msg_size) {
          return false;
        }
        writer_.cache = cache;
      }

      write_unchecked(
          std::bit_cast<std::array<std::byte, kMsgSizeBytes>>(msg_size),
          writer_.ring_buffer, *writer_.capacity, push_to);

      index_type const next_push_to =
          next(*writer_.capacity, push_to, kMsgSizeBytes);
      write_unchecked(msg, writer_.ring_buffer, *writer_.capacity,
                      next_push_to);

      writer_.index.store(next(*writer_.capacity, msg_size, next_push_to),
                          std::memory_order::relaxed);
      return true;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    std::span<std::byte> msg =
        swiss::variadic_nth<Cancellable ? 1 : 0>(args...);

    if constexpr (Blocking) {
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        if (pop_impl<false, false>(msg)) {
          return true;
        }

        std::this_thread::yield();
      }
    }

    if constexpr (!Blocking) {
      auto const total = static_cast<index_type>(msg.size());
      index_type const pop_from =
          reader_.index.load(std::memory_order::relaxed);

      if (total == 0) {
        return false;
      }

      if (peek(reader_.ring_buffer, *reader_.capacity, reader_.cache,
               pop_from) != total) {
        index_type const cache = writer_.index.load(std::memory_order::relaxed);
        if (peek(reader_.ring_buffer, *reader_.capacity, cache, pop_from) !=
            total) {
          return false;
        }
        reader_.cache = cache;
      }

      index_type const next_pop_from =
          next(*reader_.capacity, pop_from, kMsgSizeBytes);
      read_unchecked(msg, reader_.ring_buffer, *reader_.capacity,
                     next_pop_from);

      reader_.index.store(next(*reader_.capacity, total, next_pop_from),
                          std::memory_order::relaxed);
      return true;
    }
  }
};

}  // namespace flow
