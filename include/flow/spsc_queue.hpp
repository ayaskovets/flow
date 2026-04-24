#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <thread>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a ring buffer with cached pop and push indices
 * @warning Allocated buffer always contains one additional element to
 * differentiate the empty state
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_queue : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = Allocator;

 public:
  constexpr explicit spsc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "spsc_queue::spsc_queue(): invalid capacity != 2^n");
  }

  constexpr explicit spsc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [this] {
                  if (!swiss::is_power_of_two(*writer_.capacity)) {
                    throw std::invalid_argument(
                        "spsc_queue::spsc_queue(): invalid capacity != 2^n");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, *writer_.capacity);
                }(),
        },
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  /**
   * @warning Destructor is not thread safe and assumes that there is only one
   * thread operating on the queue during the destruction
   */
  constexpr ~spsc_queue() noexcept
    requires(std::is_nothrow_destructible_v<value_type>)
  {
    while (try_pop([this](value_type & value) {
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    })) {
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto try_push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    if (full(*writer_.capacity, push_to, writer_.cache)) {
      writer_.cache = reader_.index.load(std::memory_order::acquire);
      if (full(*writer_.capacity, push_to, writer_.cache)) {
        return false;
      }
    }

    std::invoke(std::forward<Callback>(callback),
                *(writer_.ring_buffer + slot(*writer_.capacity, push_to)));

    writer_.index.store(next(push_to), std::memory_order::release);
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto try_pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    if (empty(reader_.cache, pop_from)) {
      reader_.cache = writer_.index.load(std::memory_order::acquire);
      if (empty(reader_.cache, pop_from)) {
        return false;
      }
    }

    std::invoke(std::forward<Callback>(callback),
                *(reader_.ring_buffer + slot(*reader_.capacity, pop_from)));

    reader_.index.store(next(pop_from), std::memory_order::release);
    return true;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr auto
  push(std::stop_token stop_token, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    while (true) {
      if (stop_token.stop_requested()) {
        return false;
      }

      if (!full(*writer_.capacity, push_to, writer_.cache)) {
        break;
      }

      std::this_thread::yield();

      writer_.cache = reader_.index.load(std::memory_order::acquire);
    }

    std::invoke(std::forward<Callback>(callback),
                *(writer_.ring_buffer + slot(*writer_.capacity, push_to)));

    writer_.index.store(next(push_to), std::memory_order::release);
    return true;
  }

  template <std::invocable<value_type &> Callback>
  constexpr auto pop(std::stop_token stop_token, Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (true) {
      if (stop_token.stop_requested()) {
        return false;
      }

      if (!empty(reader_.cache, pop_from)) {
        break;
      }

      std::this_thread::yield();

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    std::invoke(std::forward<Callback>(callback),
                *(reader_.ring_buffer + slot(*reader_.capacity, pop_from)));

    reader_.index.store(next(pop_from), std::memory_order::release);
    return true;
  }

 public:
  template <std::invocable<value_type &> Callback>
  constexpr void push(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) {
    push(std::stop_token{}, std::forward<Callback>(callback));
  }

  template <std::invocable<value_type &> Callback>
  constexpr void pop(Callback && callback) noexcept(
      std::is_nothrow_invocable_v<Callback, value_type &>) {
    pop(std::stop_token{}, std::forward<Callback>(callback));
  }

 public:
  template <typename... Args>
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([this, ... args = std::forward<Args>(args)](
                        value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return try_pop([this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
  }

 public:
  template <typename... Args>
  constexpr auto emplace(std::stop_token stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return push(stop_token, [this, ... args = std::forward<Args>(args)](
                                value_type & value) mutable {
      std::allocator_traits<allocator_type>::construct(
          allocator_, &value, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token stop_token, value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) -> bool {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token stop_token, value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) -> bool {
    return pop(stop_token, [this, &out = value](value_type & value) mutable {
      out = std::move(value);
      std::allocator_traits<allocator_type>::destroy(allocator_, &value);
    });
  }

 public:
  template <typename... Args>
  constexpr void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(value_type value) noexcept(
      std::is_nothrow_move_constructible_v<value_type>) {
    push(std::stop_token{}, std::move(value));
  }

  constexpr void pop(value_type & value) noexcept(
      std::is_nothrow_move_assignable_v<value_type> &&
      std::is_nothrow_destructible_v<value_type>) {
    pop(std::stop_token{}, value);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    return size(push_to, pop_from);
  }

  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    value_type * const ring_buffer;
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

  [[nodiscard]] constexpr auto next(index_type index) const noexcept
      -> index_type {
    return index + 1;
  }

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }
};

}  // namespace flow
