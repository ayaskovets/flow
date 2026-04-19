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
  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_push(
      Callback && callback,
      index_type max_size = std::numeric_limits<index_type>::
          max()) noexcept(std::is_nothrow_invocable_v<Callback,
                                                      std::span<std::byte>>)
      -> index_type {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);
    index_type _free         = free(*writer_.capacity, push_to, writer_.cache);

    if (_free == 0) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      _free         = free(*writer_.capacity, push_to, writer_.cache);
    }

    index_type const postfix = *writer_.capacity + 1 - push_to;
    index_type total         = std::min(max_size, std::min(postfix, _free));
    if (total == 0) {
      return 0;
    }

    swiss::assign_invoke_result_if_not_void(
        total, std::forward<Callback>(callback),
        std::span(writer_.ring_buffer + push_to, total));

    writer_.index.store(next(*writer_.capacity, push_to, total),
                        std::memory_order::release);
    return total;
  }

  template <std::invocable<std::span<std::byte>> Callback>
  constexpr auto try_pop(
      Callback && callback,
      index_type max_size = std::numeric_limits<index_type>::
          max()) noexcept(std::is_nothrow_invocable_v<Callback,
                                                      std::span<std::byte>>)
      -> index_type {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type _size = size(*reader_.capacity, reader_.cache, pop_from);

    if (_size == 0) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      _size         = size(*reader_.capacity, reader_.cache, pop_from);
    }

    index_type const postfix = *reader_.capacity + 1 - pop_from;
    index_type total         = std::min(max_size, std::min(postfix, _size));
    if (total == 0) {
      return 0;
    }

    swiss::assign_invoke_result_if_not_void(
        total, std::forward<Callback>(callback),
        std::span(reader_.ring_buffer + pop_from, total));

    reader_.index.store(next(*reader_.capacity, pop_from, total),
                        std::memory_order::release);
    return total;
  }

 public:
  [[nodiscard]] constexpr auto try_push(
      std::span<std::byte const> bytes) noexcept -> index_type {
    auto const bytes_size    = static_cast<index_type>(bytes.size());
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);
    index_type _free         = free(*writer_.capacity, push_to, writer_.cache);

    if (bytes_size > _free) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      _free         = free(*writer_.capacity, push_to, writer_.cache);
    }

    index_type const total = std::min(bytes_size, _free);
    if (total == 0) {
      return 0;
    }

    write_unchecked(*writer_.capacity, writer_.ring_buffer, push_to,
                    bytes.subspan(0, total));
    writer_.index.store(next(*writer_.capacity, push_to, total),
                        std::memory_order::release);
    return total;
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> bytes) noexcept
      -> index_type {
    auto const bytes_size     = static_cast<index_type>(bytes.size());
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type _size = size(*reader_.capacity, reader_.cache, pop_from);

    if (bytes_size > _size) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      _size         = size(*reader_.capacity, reader_.cache, pop_from);
    }

    index_type const total = std::min(bytes_size, _size);
    if (total == 0) {
      return 0;
    }

    read_unchecked(*reader_.capacity, reader_.ring_buffer, pop_from,
                   bytes.subspan(0, total));
    reader_.index.store(next(*reader_.capacity, pop_from, total),
                        std::memory_order::release);
    return total;
  }

 public:
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> bytes) noexcept -> bool {
    auto const bytes_size    = static_cast<index_type>(bytes.size());
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    while (bytes_size > free(*writer_.capacity, push_to, writer_.cache)) {
      if (stop_token.stop_requested()) {
        return false;
      }

      std::this_thread::yield();

      writer_.cache = reader_.index.load(std::memory_order::acquire);
    }

    write_unchecked(*writer_.capacity, writer_.ring_buffer, push_to, bytes);
    writer_.index.store(next(*writer_.capacity, push_to, bytes_size),
                        std::memory_order::release);
    return true;
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> bytes) noexcept -> bool {
    auto const bytes_size     = static_cast<index_type>(bytes.size());
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (bytes_size > size(*reader_.capacity, reader_.cache, pop_from)) {
      if (stop_token.stop_requested()) {
        return false;
      }

      std::this_thread::yield();

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    read_unchecked(*reader_.capacity, reader_.ring_buffer, pop_from, bytes);
    reader_.index.store(next(*reader_.capacity, pop_from, bytes_size),
                        std::memory_order::release);
    return true;
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

    return size(*reader_.capacity, push_to, pop_from);
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
  [[nodiscard]] constexpr auto next(index_type capacity, index_type index,
                                    index_type bytes) const noexcept
      -> index_type {
    index += bytes;
    return index > capacity ? index - capacity - 1 : index;
  }

  constexpr void write_unchecked(
      index_type capacity, std::byte * ring_buffer, index_type push_to,
      std::span<std::byte const> bytes) const noexcept {
    auto const bytes_size    = static_cast<index_type>(bytes.size());
    index_type const postfix = capacity - push_to + 1;
    index_type const chunk1  = std::min(bytes_size, postfix);
    index_type const chunk2  = bytes_size - chunk1;

    std::copy_n(bytes.data(), chunk1, ring_buffer + push_to);
    std::copy_n(bytes.data() + chunk1, chunk2, ring_buffer);
  }

  constexpr void read_unchecked(index_type capacity,
                                std::byte const * ring_buffer,
                                index_type pop_from,
                                std::span<std::byte> bytes) const noexcept {
    auto const bytes_size    = static_cast<index_type>(bytes.size());
    index_type const postfix = capacity - pop_from + 1;
    index_type const chunk1  = std::min(bytes_size, postfix);
    index_type const chunk2  = bytes_size - chunk1;

    std::copy_n(ring_buffer + pop_from, chunk1, bytes.data());
    std::copy_n(ring_buffer, chunk2, bytes.data() + chunk1);
  }
};

}  // namespace flow
