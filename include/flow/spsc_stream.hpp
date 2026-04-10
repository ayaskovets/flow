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
 * @brief Fixed-size stream for 1 producer and 1 consumer
 * @note Implemented as a ring buffer
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_stream final : public swiss::non_copyable,
                          public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit spsc_stream(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity + 1)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(Capacity > 0,
                  "spsc_stream::spsc_stream(): invalid capacity < 1");
  }

  constexpr explicit spsc_stream(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.capacity{capacity},
                .ring_buffer =
                    [this] {
                      if (*writer_.capacity == 0) {
                        throw std::invalid_argument(
                            "spsc_stream::spsc_stream(): invalid capacity < 1");
                      }
                      return std::allocator_traits<allocator_type>::allocate(
                          allocator_, *writer_.capacity + 1);
                    }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  constexpr ~spsc_stream() noexcept {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity + 1);
  }

 public:
  constexpr auto try_push(std::span<std::byte const> bytes) noexcept
      -> index_type {
    return push_impl<false, false>(bytes);
  }

  constexpr auto push(std::stop_token stop_token,
                      std::span<std::byte const> bytes) noexcept -> bool {
    return push_impl<true, true>(stop_token, bytes);
  }

  constexpr void push(std::span<std::byte const> bytes) noexcept {
    push_impl<true, false>(bytes);
  }

  constexpr auto try_pop(std::span<std::byte> bytes) noexcept -> index_type {
    return pop_impl<false, false>(bytes);
  }

  constexpr auto pop(std::stop_token stop_token,
                     std::span<std::byte> bytes) noexcept -> bool {
    return pop_impl<true, true>(stop_token, bytes);
  }

  constexpr void pop(std::span<std::byte> bytes) noexcept {
    pop_impl<true, false>(bytes);
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

  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  [[nodiscard]] constexpr auto next(index_type capacity, index_type bytes,
                                    index_type index) const noexcept
      -> index_type {
    index += bytes;
    return index > capacity ? index - capacity - 1 : index;
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto push_impl(Args &&... args) -> decltype(auto) {
    std::span<std::byte const> const bytes =
        swiss::variadic_nth<Cancellable ? 1 : 0>(args...);

    if constexpr (Blocking) {
      index_type offset = 0;
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        offset += push_impl<false, false>(bytes.subspan(offset));
        if (offset == static_cast<index_type>(bytes.size())) {
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
      index_type const push_to = writer_.index.load(std::memory_order::relaxed);

      auto total = static_cast<index_type>(bytes.size());
      if (total > free(*writer_.capacity, push_to, writer_.cache)) {
        index_type const cache = reader_.index.load(std::memory_order::relaxed);
        if (total = std::min(total, free(*writer_.capacity, push_to, cache));
            total == 0) {
          return static_cast<index_type>(0);
        }
        writer_.cache = cache;
      }

      index_type const postfix = *writer_.capacity - push_to + 1;
      index_type const chunk1  = std::min(total, postfix);
      index_type const chunk2  = total - chunk1;

      std::copy_n(bytes.data(), chunk1, writer_.ring_buffer + push_to);
      std::copy_n(bytes.data() + chunk1, chunk2, writer_.ring_buffer);

      writer_.index.store(next(*writer_.capacity, total, push_to),
                          std::memory_order::relaxed);
      return total;
    }
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&... args) -> decltype(auto) {
    std::span<std::byte> bytes =
        swiss::variadic_nth<Cancellable ? 1 : 0>(args...);

    if constexpr (Blocking) {
      index_type offset = 0;
      while (true) {
        if constexpr (Cancellable) {
          if (std::stop_token const & stop_token =
                  swiss::variadic_nth<0>(std::forward<Args>(args)...);
              stop_token.stop_requested()) {
            return false;
          }
        }

        offset += pop_impl<false, false>(bytes.subspan(offset));
        if (offset == static_cast<index_type>(bytes.size())) {
          return true;
        }

        std::this_thread::yield();
      }
    }

    if constexpr (!Blocking) {
      index_type const pop_from =
          reader_.index.load(std::memory_order::relaxed);

      auto total = static_cast<index_type>(bytes.size());
      if (total > size(*reader_.capacity, reader_.cache, pop_from)) {
        index_type const cache = writer_.index.load(std::memory_order::relaxed);
        if (total = std::min(total, size(*reader_.capacity, cache, pop_from));
            total == 0) {
          return static_cast<index_type>(0);
        }
        reader_.cache = cache;
      }

      index_type const postfix = *reader_.capacity - pop_from + 1;
      index_type const chunk1  = std::min(total, postfix);
      index_type const chunk2  = total - chunk1;

      std::copy_n(reader_.ring_buffer + pop_from, chunk1, bytes.data());
      std::copy_n(reader_.ring_buffer, chunk2, bytes.data() + chunk1);

      reader_.index.store(next(*reader_.capacity, total, pop_from),
                          std::memory_order::relaxed);
      return total;
    }
  }
};

}  // namespace flow
