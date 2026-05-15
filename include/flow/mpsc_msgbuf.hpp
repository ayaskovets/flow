#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <stop_token>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/memory.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

#include "backoff_policy.hpp"
#include "noop_backoff_policy.hpp"
#include "noop_wait_policy.hpp"
#include "wait_policy.hpp"

namespace flow {

/**
 * @brief Fixed-size message buffer for many producers and 1 consumer
 * @note Implemented as a ring buffer
 * @note Each message is aligned to alignof(Index)
 * @note Capacity must be a power of 2 for efficient division
 * @note Minimal capacity is twice the alignof(Index)
 */
template <std::unsigned_integral Index           = std::size_t,
          Index Capacity                         = swiss::kDynamicExtent<Index>,
          typename Allocator                     = std::allocator<Index>,
          concepts::wait_policy WaitPolicy       = noop_wait_policy,
          concepts::backoff_policy BackoffPolicy = noop_backoff_policy>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpsc_msgbuf : public swiss::non_copyable, public swiss::non_movable {
 private:
  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, std::span<std::byte>>;

 private:
  using block_type = Index;

 public:
  using index_type = Index;
  using allocator_type =
      std::allocator_traits<Allocator>::template rebind_alloc<block_type>;
  using wait_policy_type    = WaitPolicy;
  using backoff_policy_type = BackoffPolicy;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return swiss::is_power_of_two_gt(
        capacity, static_cast<index_type>(header_size() - 1));
  }

 public:
  explicit constexpr mpsc_msgbuf(
      allocator_type allocator           = allocator_type(),
      wait_policy_type wait_policy       = wait_policy_type(),
      backoff_policy_type backoff_policy = backoff_policy_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        backoff_policy_(std::move(backoff_policy)),
        capacity_(),
        ring_buffer_(std::allocator_traits<allocator_type>::allocate(
            allocator_, size_blocks())) {
    static_assert(valid(Capacity),
                  "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity");
    std::uninitialized_fill_n(ring_buffer_, size_blocks(), 0);
  }

  explicit constexpr mpsc_msgbuf(
      index_type capacity, allocator_type allocator = allocator_type(),
      wait_policy_type wait_policy       = wait_policy_type(),
      backoff_policy_type backoff_policy = backoff_policy_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        backoff_policy_(std::move(backoff_policy)),
        capacity_(capacity),
        ring_buffer_([&] {
          if (!valid(capacity)) {
            throw std::invalid_argument(
                "mpsc_msgbuf::mpsc_msgbuf(): invalid capacity");
          }
          return std::allocator_traits<allocator_type>::allocate(allocator_,
                                                                 size_blocks());
        }()) {
    std::uninitialized_fill_n(ring_buffer_, size_blocks(), 0);
  }

  constexpr ~mpsc_msgbuf() noexcept {
    std::destroy_n(ring_buffer_, size_blocks());
    std::allocator_traits<allocator_type>::deallocate(allocator_, ring_buffer_,
                                                      size_blocks());
  }

 public:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_push(std::size_t _msg_size, Callback && callback) noexcept
      -> bool {
    if (!fits(_msg_size)) {
      return false;
    }

    auto const write_commited_header = [&](index_type push_to,
                                           index_type msg_size) {
      header_msg_size(push_to) = msg_size;
      header_seqnum(push_to).store(commit_seqnum(push_to),
                                   std::memory_order::release);
    };

    index_type const msg_size = bound(_msg_size);
    auto const aligned_msg_size =
        static_cast<index_type>(swiss::align_up(msg_size, alignof(index_type)));
    auto const [fits, push_to] = [&]() -> std::pair<bool, index_type> {
      index_type _push_to = push_to_.load(std::memory_order::relaxed);
      index_type pop_from = pop_from_.load(std::memory_order::relaxed);

      auto backoff_token = backoff_policy_.backoff_token();
      while (true) {
        index_type postfix = *capacity_ - slot(_push_to);
        index_type prefix  = slot(pop_from);
        index_type _free   = free(_push_to, pop_from);
        index_type chunk   = std::min(postfix, _free);
        if (chunk < header_size() + aligned_msg_size ||
            prefix < aligned_msg_size) {
          pop_from = pop_from_.load(std::memory_order::relaxed);

          prefix = slot(pop_from);
          _free  = free(_push_to, pop_from);
          chunk  = std::min(postfix, _free);
        }

        bool const can_insert_whole =
            (chunk >= header_size() + aligned_msg_size);
        bool const can_split_header =
            (postfix == header_size() && prefix >= aligned_msg_size &&
             _free >= header_size() + aligned_msg_size);
        if (can_insert_whole || can_split_header) {
          if (!push_to_.compare_exchange_weak(
                  _push_to, _push_to + header_size() + aligned_msg_size,
                  std::memory_order::relaxed, std::memory_order::relaxed)) {
            backoff_policy_.backoff(backoff_token);
            continue;
          }

          return {true, _push_to};
        }

        bool const can_insert_with_padding =
            (chunk == postfix && prefix >= header_size() + aligned_msg_size);
        if (can_insert_with_padding) {
          if (!push_to_.compare_exchange_weak(
                  _push_to,
                  _push_to + postfix + header_size() + aligned_msg_size,
                  std::memory_order::relaxed, std::memory_order::relaxed)) {
            backoff_policy_.backoff(backoff_token);
            continue;
          }

          if (postfix >= header_size()) {
            write_commited_header(_push_to, padding_header_msg_size());
          }
          return {true, _push_to + postfix};
        }

        return {false, 0};
      }
    }();

    if (!fits) {
      return false;
    }

    std::span<index_type> const blocks(
        at(push_to + header_size()),
        swiss::align_up(msg_size, alignof(index_type)));
    std::span<std::byte> const msg =
        std::as_writable_bytes(blocks).subspan(0, msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    write_commited_header(push_to, msg_size);

    wait_policy_.notify_not_empty();

    return true;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> std::size_t {
    auto const read_committed_msg_size = [&](index_type pop_from,
                                             index_type & msg_size) -> bool {
      index_type const seqnum =
          header_seqnum(pop_from).load(std::memory_order::acquire);
      if (seqnum != commit_seqnum(pop_from)) {
        return false;
      }

      msg_size = header_msg_size(pop_from);
      return true;
    };

    auto const [success, pop_from,
                msg_size] = [&]() -> std::tuple<bool, index_type, index_type> {
      index_type _pop_from     = pop_from_.load(std::memory_order::relaxed);
      index_type const postfix = *capacity_ - slot(_pop_from);

      index_type _msg_size = padding_header_msg_size();
      if (postfix >= header_size()) {
        if (!read_committed_msg_size(_pop_from, _msg_size)) {
          return {false, 0, 0};
        }
      }

      if (_msg_size == padding_header_msg_size()) {
        _pop_from = _pop_from + postfix;
        if (!read_committed_msg_size(_pop_from, _msg_size)) {
          return {false, 0, 0};
        }
      }

      return {true, _pop_from + header_size(), _msg_size};
    }();

    if (!success) {
      return 0;
    }

    std::span<index_type> const blocks(
        at(pop_from), static_cast<index_type>(
                          swiss::align_up(msg_size, alignof(index_type))));
    std::span<std::byte> const msg =
        std::as_writable_bytes(blocks).subspan(0, msg_size);
    std::invoke(std::forward<Callback>(callback), msg);

    pop_from_.store(pop_from + static_cast<index_type>(swiss::align_up(
                                   msg_size, alignof(index_type))),
                    std::memory_order::relaxed);

    wait_policy_.notify_not_full();

    return msg_size;
  }

 public:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token, std::size_t msg_size,
                      Callback && callback) noexcept -> bool {
    if (!fits(msg_size)) {
      return false;
    }

    while (!stop_token.stop_requested()) {
      if (try_push(msg_size, std::forward<Callback>(callback))) {
        return true;
      }

      wait_policy_.wait_not_full(stop_token);
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

      wait_policy_.wait_not_empty(stop_token);
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
      std::memcpy(chunk.data(), msg.data(), msg.size());
    });
  }

  [[nodiscard]] constexpr auto try_pop(std::span<std::byte> msg) noexcept
      -> std::size_t {
    if (msg.empty()) {
      return 0;
    }

    return try_pop([msg](std::span<std::byte> chunk) noexcept {
      std::memcpy(msg.data(), chunk.data(), std::min(msg.size(), chunk.size()));
    });
  }

 public:
  constexpr auto push(std::stop_token const & stop_token,
                      std::span<std::byte const> msg) noexcept -> bool {
    return push(stop_token, msg.size(),
                [msg](std::span<std::byte> chunk) noexcept {
                  std::memcpy(chunk.data(), msg.data(), msg.size());
                });
  }

  constexpr auto pop(std::stop_token const & stop_token,
                     std::span<std::byte> msg) noexcept -> bool {
    if (msg.empty()) {
      return false;
    }

    return pop(stop_token, [msg](std::span<std::byte> chunk) noexcept {
      std::memcpy(msg.data(), chunk.data(), std::min(msg.size(), chunk.size()));
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
      std::memcpy(msg.data(), chunk.data(), std::min(msg.size(), chunk.size()));
    });
  }

 public:
  [[nodiscard]] static constexpr auto header_size() noexcept -> std::size_t {
    return 2 * sizeof(index_type);
  }

  [[nodiscard]] constexpr auto capacity() const noexcept -> std::size_t {
    return *capacity_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    index_type const pop_from = pop_from_.load(std::memory_order::relaxed);
    index_type const push_to  = push_to_.load(std::memory_order::relaxed);

    return size_bytes(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size() == 0;
  }

  constexpr void clear() noexcept {
    while (try_pop([](std::span<std::byte>) noexcept {})) {
    }
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

 private:
  [[no_unique_address]] allocator_type allocator_;
  [[no_unique_address]] wait_policy_type wait_policy_;
  [[no_unique_address]] backoff_policy_type backoff_policy_;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  [[no_unique_address]] swiss::conditionally_runtime<
      index_type, Capacity == swiss::kDynamicExtent<index_type>, Capacity> const
      capacity_;
  block_type * const ring_buffer_;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> push_to_ = 0;
  alignas(std::hardware_destructive_interference_size)
      std::atomic<index_type> pop_from_ = 0;

 private:
  [[nodiscard]] constexpr auto bound(std::size_t size) const noexcept
      -> index_type {
    return static_cast<index_type>(
        std::min<std::size_t>(std::numeric_limits<index_type>::max(), size));
  }

  [[nodiscard]] constexpr auto fits(std::size_t bytes) const noexcept -> bool {
    return (bytes != 0) && bytes <= (*capacity_ - header_size());
  }

  [[nodiscard]] constexpr auto padding_header_msg_size() const noexcept
      -> index_type {
    return 0;
  }

  [[nodiscard]] constexpr auto size_bytes(index_type push_to,
                                          index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  [[nodiscard]] constexpr auto free(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return *capacity_ - size_bytes(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto slot(index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(*capacity_ - 1);
  }

  [[nodiscard]] constexpr auto blocks(index_type bytes) const noexcept
      -> index_type {
    return bytes / sizeof(index_type);
  }

  [[nodiscard]] constexpr auto size_blocks() const noexcept -> index_type {
    return *capacity_ / sizeof(index_type);
  }

  [[nodiscard]] constexpr auto at(index_type index) const noexcept
      -> index_type * {
    return ring_buffer_ + blocks(slot(index));
  }

  [[nodiscard]] constexpr auto commit_seqnum(index_type index) const noexcept
      -> index_type {
    return static_cast<index_type>((index / *capacity_) + 1);
  }

  [[nodiscard]] constexpr auto header_seqnum(index_type index) const noexcept
      -> std::atomic_ref<index_type> {
    return std::atomic_ref<index_type>(*at(index));
  }

  [[nodiscard]] constexpr auto header_msg_size(index_type index) const noexcept
      -> index_type & {
    return *at(index + sizeof(index_type));
  }
};

}  // namespace flow
