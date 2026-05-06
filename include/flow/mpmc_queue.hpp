#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <thread>

#include <swiss/aligned.hpp>
#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

namespace flow {

/**
 * @brief Threadsafe queue for many producers and many consumers
 * @note Implemented as a ring buffer with sequence numbers
 * @note Capacity for this queue must be a power of 2 for efficient division
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          Index Alignment    = alignof(std::pair<Value, std::atomic<Index>>),
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class mpmc_queue : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type = Value;
  using index_type = Index;
  using allocator_type =
      std::allocator_traits<Allocator>::template rebind_alloc<
          swiss::aligned<std::pair<Value, std::atomic<Index>>, Alignment>>;

 private:
  using entry_type           = std::pair<Value, std::atomic<Index>>;
  using index_allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<std::atomic<Index>>;
  using value_allocator_type = Allocator;

 private:
  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, value_type *>;

 public:
  explicit constexpr mpmc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        index_allocator_(allocator_),
        value_allocator_(allocator_),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity) && Capacity >= 2,
                  "mpmc_queue::mpmc_queue(): invalid capacity != 2^n or < 2");
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<index_allocator_type>::construct(
          index_allocator_,
          seqnum(at(writer_.ring_buffer, *writer_.capacity, index)), index);
    }
  }

  explicit constexpr mpmc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        index_allocator_(allocator_),
        value_allocator_(allocator_),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity) || capacity < 2) {
                    throw std::invalid_argument(
                        "mpmc_queue::mpmc_queue(): invalid capacity != 2^n "
                        "or "
                        "< 2");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }(),
        },
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {
    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<index_allocator_type>::construct(
          index_allocator_,
          seqnum(at(writer_.ring_buffer, *writer_.capacity, index)), index);
    }
  }

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~mpmc_queue() noexcept {
    while (try_pop([&](value_type * front) noexcept {
      std::allocator_traits<value_allocator_type>::destroy(value_allocator_,
                                                           front);
    })) {
    }

    for (index_type index = 0; index < *writer_.capacity; ++index) {
      std::allocator_traits<index_allocator_type>::destroy(
          index_allocator_,
          seqnum(at(writer_.ring_buffer, *writer_.capacity, index)));
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return try_push([&](value_type * front) noexcept {
      std::allocator_traits<value_allocator_type>::construct(
          value_allocator_, front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type const & value) noexcept -> bool
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    return try_emplace(value);
  }

  constexpr auto try_push(value_type && value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(auto & value) noexcept -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return try_pop([&](value_type * front) noexcept {
      value = std::move(*front);
      std::allocator_traits<value_allocator_type>::destroy(value_allocator_,
                                                           front);
    });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token const & stop_token,
                         Args &&... args) noexcept -> bool
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    return push(stop_token, [&](value_type * front) noexcept {
      std::allocator_traits<value_allocator_type>::construct(
          value_allocator_, front, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type const & value) noexcept -> bool
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    return emplace(stop_token, value);
  }

  constexpr auto push(std::stop_token const & stop_token,
                      value_type && value) noexcept -> bool
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token, auto & value) noexcept
      -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token, [&](value_type * front) noexcept {
      value = std::move(*front);
      std::allocator_traits<value_allocator_type>::destroy(value_allocator_,
                                                           front);
    });
  }

 public:
  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept
    requires(std::is_nothrow_constructible_v<value_type, Args...>)
  {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type const & value) noexcept
    requires(std::is_nothrow_copy_constructible_v<value_type>)
  {
    emplace(std::stop_token{}, value);
  }

  constexpr void push(value_type && value) noexcept
    requires(std::is_nothrow_move_constructible_v<value_type>)
  {
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
    return *writer_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);

    index_type const _size = size(push_to, pop_from);
    return _size > *writer_.capacity ? 0 : _size;
  }

  [[nodiscard]] constexpr auto get_allocator() const
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
          -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    swiss::aligned<entry_type, Alignment> * const ring_buffer;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  [[no_unique_address]] allocator_type allocator_;
  [[no_unique_address]] index_allocator_type index_allocator_;
  [[no_unique_address]] value_allocator_type value_allocator_;
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

  [[nodiscard]] constexpr auto vacant(index_type push_to,
                                      index_type seqnum) const noexcept
      -> bool {
    return push_to == seqnum;
  }

  [[nodiscard]] constexpr auto occupied(index_type pop_from,
                                        index_type seqnum) const noexcept
      -> bool {
    return static_cast<index_type>(pop_from + 1) == seqnum;
  }

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }

  [[nodiscard]] constexpr auto at(
      swiss::aligned<entry_type, Alignment> * ring_buffer, index_type capacity,
      index_type index) const noexcept -> entry_type * {
    return &**(ring_buffer + slot(capacity, index));
  }

  [[nodiscard]] constexpr auto value(entry_type * entry) const noexcept
      -> value_type * {
    return &entry->first;
  }

  [[nodiscard]] constexpr auto seqnum(entry_type * entry) const noexcept
      -> std::atomic<index_type> * {
    return &entry->second;
  }

 private:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_push(Callback && callback) noexcept -> bool {
    index_type push_to = writer_.index.load(std::memory_order::relaxed);

    while (true) {
      entry_type * entry = at(writer_.ring_buffer, *writer_.capacity, push_to);

      if (vacant(push_to, seqnum(entry)->load(std::memory_order::acquire))) {
        if (writer_.index.compare_exchange_weak(push_to, next(push_to),
                                                std::memory_order::relaxed,
                                                std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry)->store(push_to + 1, std::memory_order::release);
          return true;
        }
      } else {
        index_type const actual_push_to =
            writer_.index.load(std::memory_order::relaxed);
        if (push_to == actual_push_to) {
          return false;
        }
        push_to = actual_push_to;
      }
    }
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto try_pop(Callback && callback) noexcept -> bool {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);

    while (true) {
      entry_type * entry = at(reader_.ring_buffer, *reader_.capacity, pop_from);

      if (occupied(pop_from, seqnum(entry)->load(std::memory_order::acquire))) {
        if (reader_.index.compare_exchange_weak(pop_from, next(pop_from),
                                                std::memory_order::relaxed,
                                                std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry)->store(pop_from + *reader_.capacity,
                               std::memory_order::release);
          return true;
        }
      } else {
        index_type const actual_pop_from =
            reader_.index.load(std::memory_order::relaxed);
        if (pop_from == actual_pop_from) {
          return false;
        }
        pop_from = actual_pop_from;
      }
    }
  }

 private:
  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token,
                      Callback && callback) noexcept -> bool {
    index_type push_to = writer_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      entry_type * entry = at(writer_.ring_buffer, *writer_.capacity, push_to);

      if (vacant(push_to, seqnum(entry)->load(std::memory_order::acquire))) {
        if (writer_.index.compare_exchange_weak(push_to, next(push_to),
                                                std::memory_order::relaxed,
                                                std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry)->store(push_to + 1, std::memory_order::release);
          return true;
        }
      } else {
        std::this_thread::yield();
        push_to = writer_.index.load(std::memory_order::relaxed);
      }
    }

    return false;
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool {
    index_type pop_from = reader_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      entry_type * entry = at(reader_.ring_buffer, *reader_.capacity, pop_from);

      if (occupied(pop_from, seqnum(entry)->load(std::memory_order::acquire))) {
        if (reader_.index.compare_exchange_weak(pop_from, next(pop_from),
                                                std::memory_order::relaxed,
                                                std::memory_order::relaxed)) {
          std::invoke(std::forward<Callback>(callback), value(entry));

          seqnum(entry)->store(pop_from + *reader_.capacity,
                               std::memory_order::release);
          return true;
        }
      } else {
        std::this_thread::yield();
        pop_from = reader_.index.load(std::memory_order::relaxed);
      }
    }

    return false;
  }
};

}  // namespace flow
