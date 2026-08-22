#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>

#include <swiss/aligned.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/ebo.hpp>
#include <swiss/manual_lifetime.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/scope_exit.hpp>

#include "noop_wait_policy.hpp"
#include "wait_policy.hpp"

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a ring buffer with cached pop and push indices
 * @note Capacity must be a power of 2 for efficient division
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          Index Alignment    = alignof(swiss::manual_lifetime<Value>),
          typename Allocator = std::allocator<Value>,
          concepts::wait_policy WaitPolicy = noop_wait_policy>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_queue : public swiss::non_copyable, public swiss::non_movable {
 private:
  template <typename T>
  static constexpr bool is_callback = std::invocable<T, Value *>;

  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, Value *>;

 private:
  using entry_type         = swiss::manual_lifetime<Value>;
  using aligned_entry_type = swiss::aligned<entry_type, Alignment>;

 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<aligned_entry_type>;
  using wait_policy_type = WaitPolicy;

 private:
  [[nodiscard]] static constexpr auto valid(index_type capacity) noexcept
      -> bool {
    return swiss::is_power_of_two(capacity);
  }

  constexpr void initialize() noexcept {
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<allocator_type>::construct(
          allocator_,
          aligned_at(writer_.ring_buffer, *writer_.capacity, index));
    }
  }

  constexpr void deinitialize() noexcept {
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<allocator_type>::destroy(
          allocator_,
          aligned_at(writer_.ring_buffer, *writer_.capacity, index));
    }
  }

 public:
  explicit constexpr spsc_queue(
      allocator_type allocator     = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(valid(Capacity),
                  "spsc_queue::spsc_queue(): invalid capacity");
    initialize();
  }

  explicit constexpr spsc_queue(
      index_type capacity, allocator_type allocator = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.capacity{capacity},
                .ring_buffer = [&] -> aligned_entry_type * {
                  if (!valid(capacity)) {
                    throw std::invalid_argument(
                        "spsc_queue::spsc_queue(): invalid capacity");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }()},
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {
    initialize();
  }

  constexpr ~spsc_queue() noexcept {
    clear();
    deinitialize();
    std::allocator_traits<allocator_type>::deallocate(
        allocator_, writer_.ring_buffer, *writer_.capacity);
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([&](value_type * front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type const & value) noexcept(
      std::is_nothrow_copy_constructible_v<value_type>) -> bool {
    return try_emplace(value);
  }

  constexpr auto try_push(value_type && value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(auto & value) noexcept -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return try_pop([&](value_type * front) noexcept -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(front); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto
  emplace(std::stop_token const & stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return push(stop_token, [&](value_type * front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto
  push(std::stop_token const & stop_token, value_type const & value) noexcept(
      std::is_nothrow_copy_constructible_v<value_type>) -> bool {
    return emplace(stop_token, value);
  }

  constexpr auto
  push(std::stop_token const & stop_token, value_type && value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token, auto & value) noexcept
      -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token, [&](value_type * front) noexcept -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(front); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type const & value) noexcept(
      std::is_nothrow_copy_constructible_v<value_type>) {
    emplace(std::stop_token{}, value);
  }

  constexpr void push(value_type && value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) {
    emplace(std::stop_token{}, std::move(value));
  }

  constexpr void pop(auto & value) noexcept
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    pop(std::stop_token{}, value);
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
    return size() == 0;
  }

  constexpr void clear() noexcept {
    while (try_pop(
        [](value_type * front) noexcept -> void { std::destroy_at(front); })) {
    }
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

  [[nodiscard]] constexpr auto get_wait_policy() const
      noexcept(std::is_nothrow_copy_constructible_v<wait_policy_type>)
          -> wait_policy_type {
    return wait_policy_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::ebo<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    aligned_entry_type * const ring_buffer;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  [[no_unique_address]] allocator_type allocator_;
  [[no_unique_address]] wait_policy_type wait_policy_;

  CacheLine writer_;
  CacheLine reader_;

 private:
  [[nodiscard]] constexpr auto size(index_type push_to,
                                    index_type pop_from) const noexcept
      -> index_type {
    return push_to - pop_from;
  }

  [[nodiscard]] constexpr auto full(index_type capacity, index_type push_to,
                                    index_type pop_from) const noexcept
      -> bool {
    return capacity == size(push_to, pop_from);
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }

  [[nodiscard]] constexpr auto aligned_at(aligned_entry_type * ring_buffer,
                                          index_type capacity,
                                          index_type index) const noexcept
      -> aligned_entry_type * {
    index_type const slot = index & static_cast<index_type>(capacity - 1);
    return ring_buffer + slot;
  }

  [[nodiscard]] constexpr auto at(aligned_entry_type * ring_buffer,
                                  index_type capacity,
                                  index_type index) const noexcept
      -> value_type * {
    return &***aligned_at(ring_buffer, capacity, index);
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(Callback && callback) noexcept(
      is_nothrow_callback<Callback>) -> bool {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    if (full(*writer_.capacity, push_to, writer_.cache)) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      if (full(*writer_.capacity, push_to, writer_.cache)) {
        return false;
      }
    }

    std::invoke(std::forward<Callback>(callback),
                at(writer_.ring_buffer, *writer_.capacity, push_to));
    writer_.index.store(push_to + 1, std::memory_order::release);

    wait_policy_.notify_not_empty();

    return true;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    if (empty(reader_.cache, pop_from)) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      if (empty(reader_.cache, pop_from)) {
        return false;
      }
    }

    std::invoke(std::forward<Callback>(callback),
                at(reader_.ring_buffer, *reader_.capacity, pop_from));
    reader_.index.store(pop_from + 1, std::memory_order::release);

    wait_policy_.notify_not_full();

    return true;
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto push(
      std::stop_token const & stop_token,
      Callback && callback) noexcept(is_nothrow_callback<Callback>) -> bool {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (!full(*writer_.capacity, push_to, writer_.cache)) {
        std::invoke(std::forward<Callback>(callback),
                    at(writer_.ring_buffer, *writer_.capacity, push_to));
        writer_.index.store(push_to + 1, std::memory_order::release);

        wait_policy_.notify_not_empty();

        return true;
      }

      wait_policy_.wait_not_full(stop_token);

      writer_.cache = reader_.index.load(std::memory_order::acquire);
    }

    return false;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (!empty(reader_.cache, pop_from)) {
        std::invoke(std::forward<Callback>(callback),
                    at(reader_.ring_buffer, *reader_.capacity, pop_from));
        reader_.index.store(pop_from + 1, std::memory_order::release);

        wait_policy_.notify_not_full();

        return true;
      }

      wait_policy_.wait_not_empty(stop_token);

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    return false;
  }
};

}  // namespace flow
