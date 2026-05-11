#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <stop_token>

#include <swiss/concepts.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/functional.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

#include "noop_wait_policy.hpp"
#include "wait_policy.hpp"

namespace flow {

/**
 * @brief Fixed-size stream for 1 producer and 1 consumer
 * @note Implemented as a ring buffer
 * @note Callback may return a value of an integral type to restrict the number
 * of bytes that will be marked as read in the stream
 * @note Capacity must be a power of 2 for efficient division
 */
template <std::unsigned_integral Index     = std::size_t,
          Index Capacity                   = swiss::kDynamicExtent<Index>,
          typename Allocator               = std::allocator<std::byte>,
          concepts::wait_policy WaitPolicy = noop_wait_policy>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_stream : public swiss::non_copyable, public swiss::non_movable {
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
  using index_type       = Index;
  using allocator_type   = Allocator;
  using wait_policy_type = WaitPolicy;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return swiss::is_power_of_two(capacity);
  }

 public:
  explicit constexpr spsc_stream(
      allocator_type allocator     = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(valid(Capacity),
                  "spsc_stream::spsc_stream(): invalid capacity");
  }

  explicit constexpr spsc_stream(
      index_type capacity, allocator_type allocator = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.capacity{capacity},
                .ring_buffer =
                    [&] {
                      if (!valid(capacity)) {
                        throw std::invalid_argument(
                            "spsc_stream::spsc_stream(): invalid capacity");
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
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(
      Callback && callback,
      std::size_t _max_size = std::numeric_limits<
          std::size_t>::max()) noexcept(is_nothrow_callback<Callback>)
      -> std::size_t {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    index_type const total = [&]() {
      index_type const max_size = bound(_max_size);

      index_type _free = free(*writer_.capacity, push_to, writer_.cache);
      if (max_size > _free) {
        writer_.cache = reader_.index.load(std::memory_order::acquire);
        _free         = free(*writer_.capacity, push_to, writer_.cache);
      }
      return std::min(max_size, _free);
    }();

    if (total == 0) {
      return 0;
    }

    auto const [postfix_span, prefix_span] = [&]() {
      index_type const _slot   = slot(*writer_.capacity, push_to);
      index_type const postfix = *writer_.capacity - _slot;
      index_type const chunk   = std::min(total, postfix);
      index_type const prefix  = total - chunk;
      return std::pair{std::span<std::byte>(writer_.ring_buffer + _slot, chunk),
                       std::span<std::byte>(writer_.ring_buffer, prefix)};
    }();

    std::size_t _callback_total = total;
    swiss::assign_invoke_result_if_not_void(_callback_total,
                                            std::forward<Callback>(callback),
                                            postfix_span, prefix_span);
    index_type const callback_total = std::min(bound(_callback_total), total);

    writer_.index.store(push_to + callback_total, std::memory_order::release);
    wait_policy_.notify_not_full();
    return callback_total;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(
      Callback && callback,
      std::size_t _max_size = std::numeric_limits<std::size_t>::max()) noexcept
      -> std::size_t {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    index_type const total = [&]() {
      index_type const max_size = bound(_max_size);
      index_type _size          = size(reader_.cache, pop_from);
      if (max_size > _size) {
        reader_.cache = writer_.index.load(std::memory_order::acquire);
        _size         = size(reader_.cache, pop_from);
      }
      return std::min(max_size, _size);
    }();

    if (total == 0) {
      return 0;
    }

    auto const [postfix_span, prefix_span] = [&]() {
      index_type const _slot   = slot(*reader_.capacity, pop_from);
      index_type const postfix = *reader_.capacity - _slot;
      index_type const chunk   = std::min(total, postfix);
      index_type const prefix  = total - chunk;
      return std::pair{std::span<std::byte>(reader_.ring_buffer + _slot, chunk),
                       std::span<std::byte>(reader_.ring_buffer, prefix)};
    }();

    std::size_t _callback_total = total;
    swiss::assign_invoke_result_if_not_void(_callback_total,
                                            std::forward<Callback>(callback),
                                            postfix_span, prefix_span);
    index_type const callback_total = std::min(bound(_callback_total), total);

    reader_.index.store(pop_from + callback_total, std::memory_order::release);
    wait_policy_.notify_not_empty();
    return callback_total;
  }

 public:
  template <typename Callback>
    requires(is_void_callback<Callback>)
  constexpr auto push(
      std::stop_token const & stop_token, std::size_t _exact_size,
      Callback && callback) noexcept(is_nothrow_callback<Callback>) -> bool {
    if (!fits(*writer_.capacity, _exact_size)) {
      return false;
    }

    index_type const exact_size = bound(_exact_size);
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (exact_size <= free(*writer_.capacity, push_to, writer_.cache)) {
        std::ignore = try_push(std::forward<Callback>(callback), exact_size);

        writer_.index.store(push_to + exact_size, std::memory_order::release);
        wait_policy_.notify_not_full();
        return true;
      }

      wait_policy_.wait_not_empty(stop_token);

      writer_.cache = reader_.index.load(std::memory_order::acquire);
    }

    return false;
  }

  template <typename Callback>
    requires(is_void_callback<Callback> && is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     std::size_t _exact_size, Callback && callback) noexcept
      -> bool {
    if (!fits(*reader_.capacity, _exact_size)) {
      return false;
    }

    index_type const exact_size = bound(_exact_size);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (exact_size <= size(reader_.cache, pop_from)) {
        std::ignore = try_pop(std::forward<Callback>(callback), exact_size);

        reader_.index.store(pop_from + exact_size, std::memory_order::release);
        wait_policy_.notify_not_empty();
        return true;
      }

      wait_policy_.wait_not_full(stop_token);

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    return false;
  }

 public:
  template <typename Callback>
    requires(is_void_callback<Callback>)
  constexpr void push(std::size_t exact_size, Callback && callback) noexcept(
      is_nothrow_callback<Callback>) {
    push(std::stop_token{}, exact_size, std::forward<Callback>(callback));
  }

  template <typename Callback>
    requires(is_void_callback<Callback> && is_nothrow_callback<Callback>)
  constexpr void pop(std::size_t exact_size, Callback && callback) noexcept {
    pop(std::stop_token{}, exact_size, std::forward<Callback>(callback));
  }

 public:
  [[nodiscard]] constexpr auto try_push(
      std::span<std::byte const> bytes) noexcept -> std::size_t {
    return try_push(
        [&](std::span<std::byte> postfix,
            std::span<std::byte> prefix) noexcept {
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
            std::span<std::byte> prefix) noexcept {
          std::memcpy(bytes.data(), postfix.data(), postfix.size());
          std::memcpy(bytes.subspan(postfix.size()).data(), prefix.data(),
                      prefix.size());
        },
        bytes.size());
  }

 public:
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> bytes) noexcept -> bool {
    return push(stop_token, bytes.size(),
                [&](std::span<std::byte> postfix,
                    std::span<std::byte> prefix) noexcept {
                  std::memcpy(postfix.data(), bytes.data(), postfix.size());
                  std::memcpy(prefix.data(),
                              bytes.subspan(postfix.size()).data(),
                              prefix.size());
                });
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> bytes) noexcept -> bool {
    return pop(stop_token, bytes.size(),
               [&](std::span<std::byte> postfix,
                   std::span<std::byte> prefix) noexcept {
                 std::memcpy(bytes.data(), postfix.data(), postfix.size());
                 std::memcpy(bytes.subspan(postfix.size()).data(),
                             prefix.data(), prefix.size());
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
  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);

    return size(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);

    return pop_from == push_to;
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
  [[no_unique_address]] wait_policy_type wait_policy_;

  CacheLine writer_;
  CacheLine reader_;

 private:
  [[nodiscard]] constexpr auto bound(std::size_t size) const noexcept
      -> index_type {
    return static_cast<index_type>(
        std::min<std::size_t>(std::numeric_limits<index_type>::max(), size));
  }

  [[nodiscard]] constexpr auto fits(index_type capacity,
                                    std::size_t bytes) const noexcept -> bool {
    return (bytes != 0) && (bytes <= capacity);
  }

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

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }
};

}  // namespace flow
