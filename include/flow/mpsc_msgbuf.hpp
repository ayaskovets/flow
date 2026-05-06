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
 * @note Has alignment which is equal to index_type, memory occupied by a
 * message in the internal buffer is always a multiple of this value
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<Index>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpsc_msgbuf : public swiss::non_copyable, public swiss::non_movable {
 public:
  using index_type     = Index;
  using allocator_type = Allocator;

 private:
  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, std::span<std::byte>>;

 public:
  explicit constexpr mpsc_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, blocks(Capacity))},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity != 2^n");
    static_assert(
        Capacity >= header_size(),
        "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity < header_size()");
    std::uninitialized_fill_n(writer_.ring_buffer, blocks(*writer_.capacity),
                              0);
  }

  explicit constexpr mpsc_msgbuf(index_type capacity,
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
                  if (capacity < header_size()) {
                    throw std::invalid_argument(
                        "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity < "
                        "header_size()");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, blocks(capacity));
                }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {
    std::uninitialized_fill_n(writer_.ring_buffer, blocks(*writer_.capacity),
                              0);
  }

  constexpr ~mpsc_msgbuf() noexcept {
    std::destroy_n(reader_.ring_buffer, blocks(*reader_.capacity));
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, blocks(*reader_.capacity));
  }

 public:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_push(std::size_t _msg_size, Callback && callback) noexcept
      -> bool {
    if (!is_valid_input_size(*writer_.capacity, _msg_size)) {
      return false;
    }

    auto const write_commited_header = [&](index_type push_to,
                                           index_type msg_size) {
      header_msg_size(writer_.ring_buffer, *writer_.capacity, push_to) =
          msg_size;
      header_seqnum(writer_.ring_buffer, *writer_.capacity, push_to)
          .store(commit_seqnum(*writer_.capacity, push_to),
                 std::memory_order::release);
    };

    index_type const msg_size         = bound_to_index_type(_msg_size);
    index_type const aligned_msg_size = aligned(msg_size);
    auto const [fits, push_to]        = [&]() {
      index_type _push_to = writer_.index.load(std::memory_order::relaxed);
      index_type pop_from = reader_.index.load(std::memory_order::relaxed);

      while (true) {
        index_type postfix =
            *writer_.capacity - slot(*writer_.capacity, _push_to);
        index_type prefix = slot(*writer_.capacity, pop_from);
        index_type _free  = free(*writer_.capacity, _push_to, pop_from);
        index_type chunk  = std::min(postfix, _free);
        if (chunk < header_size() + aligned_msg_size ||
            prefix < aligned_msg_size) {
          pop_from = reader_.index.load(std::memory_order::relaxed);

          prefix = slot(*writer_.capacity, pop_from);
          _free  = free(*writer_.capacity, _push_to, pop_from);
          chunk  = std::min(postfix, _free);
        }

        bool const can_insert_whole =
            (chunk >= header_size() + aligned_msg_size);
        bool const can_split_header =
            (postfix == header_size() && prefix >= aligned_msg_size &&
             _free >= header_size() + aligned_msg_size);
        if (can_insert_whole || can_split_header) {
          if (!writer_.index.compare_exchange_weak(
                  _push_to, next(_push_to, header_size() + aligned_msg_size),
                  std::memory_order::relaxed, std::memory_order::relaxed)) {
            continue;
          }

          return std::pair{true, _push_to};
        }

        bool const can_insert_with_padding =
            (chunk == postfix && prefix >= header_size() + aligned_msg_size);
        if (can_insert_with_padding) {
          if (!writer_.index.compare_exchange_weak(
                  _push_to,
                  next(_push_to, postfix + header_size() + aligned_msg_size),
                  std::memory_order::relaxed, std::memory_order::relaxed)) {
            continue;
          }

          if (postfix >= header_size()) {
            write_commited_header(_push_to, padding_header_msg_size());
          }
          return std::pair{true, next(_push_to, postfix)};
        }

        return std::pair{false, static_cast<index_type>(0)};
      }
    }();

    if (!fits) {
      return false;
    }

    std::span<index_type> const blocks(
        at(writer_.ring_buffer, *writer_.capacity,
           next(push_to, header_size())),
        aligned(msg_size));
    std::span<std::byte> const msg =
        std::as_writable_bytes(blocks).subspan(0, msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    write_commited_header(push_to, msg_size);
    return true;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> std::size_t {
    auto const read_committed_msg_size = [&](index_type pop_from,
                                             index_type & msg_size) -> bool {
      index_type const seqnum =
          header_seqnum(reader_.ring_buffer, *reader_.capacity, pop_from)
              .load(std::memory_order::acquire);
      if (seqnum != commit_seqnum(*reader_.capacity, pop_from)) {
        return false;
      }

      msg_size =
          header_msg_size(reader_.ring_buffer, *reader_.capacity, pop_from);
      return true;
    };

    auto const [success, pop_from, msg_size] = [&]() {
      index_type _pop_from = reader_.index.load(std::memory_order::relaxed);
      index_type const postfix =
          *reader_.capacity - slot(*reader_.capacity, _pop_from);

      index_type _msg_size = padding_header_msg_size();
      if (postfix >= header_size()) {
        if (!read_committed_msg_size(_pop_from, _msg_size)) {
          return std::tuple{false, static_cast<index_type>(0),
                            static_cast<index_type>(0)};
        }
      }

      if (_msg_size == padding_header_msg_size()) {
        _pop_from = next(_pop_from, postfix);
        if (!read_committed_msg_size(_pop_from, _msg_size)) {
          return std::tuple{false, static_cast<index_type>(0),
                            static_cast<index_type>(0)};
        }
      }

      return std::tuple{true, next(_pop_from, header_size()), _msg_size};
    }();

    if (!success) {
      return 0;
    }

    std::span<index_type> const blocks(
        at(reader_.ring_buffer, *reader_.capacity, pop_from),
        aligned(msg_size));
    std::span<std::byte> const msg =
        std::as_writable_bytes(blocks).subspan(0, msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    reader_.index.store(next(pop_from, aligned(msg_size)),
                        std::memory_order::relaxed);
    return msg_size;
  }

 public:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token, std::size_t msg_size,
                      Callback && callback) noexcept -> bool {
    if (!is_valid_input_size(*writer_.capacity, msg_size)) {
      return false;
    }

    while (!stop_token.stop_requested()) {
      if (try_push(msg_size, std::forward<Callback>(callback))) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool {
    while (!stop_token.stop_requested()) {
      if (try_pop(std::forward<Callback>(callback)) != 0) {
        return true;
      }

      std::this_thread::yield();
    }

    return false;
  }

 public:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr void push(std::size_t msg_size, Callback && callback) noexcept {
    push(std::stop_token{}, msg_size, std::forward<Callback>(callback));
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(Callback && callback) noexcept -> std::size_t {
    std::size_t msg_size{};
    pop(std::stop_token{}, [&](std::span<std::byte> chunk) noexcept {
      msg_size = chunk.size();
      std::invoke(std::forward<Callback>(callback), chunk);
    });
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
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> msg) noexcept -> bool {
    return push(stop_token, msg.size(),
                [msg](std::span<std::byte> chunk) noexcept {
                  std::copy_n(msg.begin(), msg.size(), chunk.begin());
                });
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> msg) noexcept -> bool {
    if (msg.empty()) {
      return false;
    }

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
      -> std::size_t {
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
    return 2 * sizeof(index_type);
  }

  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);

    index_type const _size = size(push_to, pop_from);
    return _size > *writer_.capacity ? 0 : _size;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);

    return empty(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    index_type * const ring_buffer;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  [[no_unique_address]] allocator_type allocator_;
  CacheLine writer_;
  CacheLine reader_;

 private:
  [[nodiscard]] constexpr auto bound_to_index_type(
      std::size_t size) const noexcept -> index_type {
    return static_cast<index_type>(
        std::min<std::size_t>(std::numeric_limits<index_type>::max(), size));
  }

  [[nodiscard]] constexpr auto is_valid_input_size(
      index_type capacity, std::size_t bytes) const noexcept -> bool {
    return bytes != 0 && bytes <= capacity - header_size();
  }

  [[nodiscard]] constexpr auto padding_header_msg_size() const noexcept
      -> index_type {
    return 0;
  }

  [[nodiscard]] auto aligned(index_type msg_size) const noexcept -> index_type {
    return (msg_size + sizeof(index_type) - 1) & ~(sizeof(index_type) - 1);
  }

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

  [[nodiscard]] constexpr auto blocks(index_type bytes) const noexcept
      -> index_type {
    return bytes / sizeof(index_type);
  }

  [[nodiscard]] constexpr auto at(index_type * ring_buffer, index_type capacity,
                                  index_type index) const noexcept
      -> index_type * {
    return ring_buffer + blocks(slot(capacity, index));
  }

  [[nodiscard]] constexpr auto commit_seqnum(index_type capacity,
                                             index_type index) const noexcept
      -> index_type {
    return static_cast<index_type>((index / capacity) + 1);
  }

  [[nodiscard]] constexpr auto header_seqnum(index_type * ring_buffer,
                                             index_type capacity,
                                             index_type index) const noexcept
      -> std::atomic_ref<index_type> {
    return std::atomic_ref<index_type>(*at(ring_buffer, capacity, index));
  }

  [[nodiscard]] constexpr auto header_msg_size(index_type * ring_buffer,
                                               index_type capacity,
                                               index_type index) const noexcept
      -> index_type & {
    return *at(ring_buffer, capacity, next(index, sizeof(index_type)));
  }
};

}  // namespace flow
