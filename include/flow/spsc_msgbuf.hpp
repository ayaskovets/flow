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
 * @brief Fixed-size message buffer for 1 producer and 1 consumer
 * @note Implemented as a ring buffer
 */
template <std::unsigned_integral Index = std::size_t,
          Index Capacity               = swiss::kDynamicExtent<Index>,
          typename Allocator           = std::allocator<std::byte>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_msgbuf : public swiss::non_copyable, public swiss::non_movable {
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
  explicit constexpr spsc_msgbuf(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "spsc_msgbuf::spsc_msgbuf(): invalid capacity != 2^n");
    static_assert(
        Capacity >= header_size(),
        "spsc_msgbuf::spsc_msgbuf(): invalid capacity < header_size()");
  }

  explicit constexpr spsc_msgbuf(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity)) {
                    throw std::invalid_argument(
                        "spsc_msgbuf::spsc_msgbuf(): invalid capacity != 2^n");
                  }
                  if (capacity < header_size()) {
                    throw std::invalid_argument(
                        "spsc_msgbuf::spsc_msgbuf(): invalid capacity < "
                        "header_size()");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  constexpr ~spsc_msgbuf() noexcept {
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(std::size_t _msg_size, Callback && callback) noexcept(
      is_nothrow_callback<Callback>) -> bool {
    if (!is_valid_input_size(*writer_.capacity, _msg_size)) {
      return false;
    }

    index_type const msg_size  = bound_to_index_type(_msg_size);
    auto const [fits, push_to] = [&]() {
      index_type const _push_to =
          writer_.index.load(std::memory_order::relaxed);
      index_type const postfix =
          *writer_.capacity - slot(*writer_.capacity, _push_to);

      index_type prefix = slot(*writer_.capacity, writer_.cache);
      index_type _free  = free(*writer_.capacity, _push_to, writer_.cache);
      index_type chunk  = std::min(postfix, _free);

      if (chunk < (header_size() + msg_size) || prefix < msg_size) {
        writer_.cache = reader_.index.load(std::memory_order::acquire);

        prefix = slot(*writer_.capacity, writer_.cache);
        _free  = free(*writer_.capacity, _push_to, writer_.cache);
        chunk  = std::min(postfix, _free);
      }

      bool const can_insert_whole = (chunk >= header_size() + msg_size);
      bool const can_split_header =
          (_free >= (header_size() + msg_size) && postfix == header_size() &&
           prefix >= msg_size);
      if (can_insert_whole || can_split_header) {
        return std::pair{true, _push_to};
      }

      bool const can_insert_with_padding =
          (chunk == postfix && prefix >= header_size() + msg_size);
      if (can_insert_with_padding) {
        if (postfix >= header_size()) {
          write_header(writer_.ring_buffer, *writer_.capacity, _push_to,
                       padding_header());
        }
        return std::pair{true, next(_push_to, postfix)};
      }

      return std::pair{false, static_cast<index_type>(0)};
    }();

    if (!fits) {
      return false;
    }

    std::span<std::byte> const msg(
        writer_.ring_buffer +
            slot(*writer_.capacity, next(push_to, header_size())),
        msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    write_header(writer_.ring_buffer, *writer_.capacity, push_to, msg_size);
    writer_.index.store(next(push_to, header_size() + msg_size),
                        std::memory_order::release);
    return true;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> std::size_t {
    index_type const _pop_from = reader_.index.load(std::memory_order::relaxed);

    if (empty(reader_.cache, _pop_from)) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      if (empty(reader_.cache, _pop_from)) {
        return 0;
      }
    }

    auto const [pop_from, msg_size] = [&]() {
      index_type const postfix =
          *reader_.capacity - slot(*reader_.capacity, _pop_from);
      index_type const header =
          postfix < header_size()
              ? padding_header()
              : read_header(reader_.ring_buffer, *reader_.capacity, _pop_from);

      if (header == padding_header()) {
        return std::pair{next(_pop_from, postfix + header_size()),
                         read_header(reader_.ring_buffer, *reader_.capacity,
                                     next(_pop_from, postfix))};
      }

      return std::pair{next(_pop_from, header_size()), header};
    }();

    std::span<std::byte> const msg(
        reader_.ring_buffer + slot(*reader_.capacity, pop_from), msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    reader_.index.store(next(pop_from, msg_size), std::memory_order::release);
    return msg_size;
  }

 public:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto push(
      std::stop_token const & stop_token, std::size_t msg_size,
      Callback && callback) noexcept(is_nothrow_callback<Callback>) -> bool {
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
    requires(is_callback<Callback>)
  constexpr void push(std::size_t msg_size, Callback && callback) noexcept(
      is_nothrow_callback<Callback>) {
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
    return sizeof(index_type);
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
  [[nodiscard]] constexpr auto bound_to_index_type(
      std::size_t size) const noexcept -> index_type {
    return static_cast<index_type>(
        std::min<std::size_t>(std::numeric_limits<index_type>::max(), size));
  }

  [[nodiscard]] constexpr auto is_valid_input_size(
      index_type capacity, std::size_t bytes) const noexcept -> bool {
    return bytes != 0 && bytes <= capacity - header_size();
  }

  [[nodiscard]] constexpr auto padding_header() const noexcept -> index_type {
    return 0;
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

  constexpr void write_header(std::byte * ring_buffer, index_type capacity,
                              index_type index,
                              index_type msg_size) const noexcept {
    std::copy_n(
        std::bit_cast<std::array<std::byte, header_size()>>(msg_size).data(),
        header_size(), ring_buffer + slot(capacity, index));
  }

  [[nodiscard]] constexpr auto read_header(std::byte const * ring_buffer,
                                           index_type capacity,
                                           index_type index) const noexcept
      -> index_type {
    std::array<std::byte, header_size()> msg_size{};
    std::copy_n(ring_buffer + slot(capacity, index), header_size(),
                msg_size.data());
    return std::bit_cast<index_type>(msg_size);
  }
};

}  // namespace flow
