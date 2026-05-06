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
#include <swiss/scope_exit.hpp>

namespace flow {

/**
 * @brief Fixed-size threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a ring buffer with cached pop and push indices
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          Index Capacity     = swiss::kDynamicExtent<Index>,
          Index Alignment    = alignof(Value),
          typename Allocator = std::allocator<Value>>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class spsc_queue : public swiss::non_copyable, public swiss::non_movable {
 public:
  using value_type     = Value;
  using index_type     = Index;
  using allocator_type = std::allocator_traits<
      Allocator>::template rebind_alloc<swiss::aligned<Value, Alignment>>;

 private:
  template <typename T>
  static constexpr bool is_callback = std::invocable<T, value_type *>;

  template <typename T>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<T, value_type *>;

 public:
  explicit constexpr spsc_queue(allocator_type allocator = allocator_type())
    requires(Capacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{.ring_buffer = std::allocator_traits<allocator_type>::allocate(
                    allocator_, Capacity)},
        reader_{.ring_buffer = writer_.ring_buffer} {
    static_assert(swiss::is_power_of_two(Capacity),
                  "spsc_queue::spsc_queue(): invalid capacity != 2^n");
  }

  explicit constexpr spsc_queue(index_type capacity,
                                allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        writer_{
            .capacity{capacity},
            .ring_buffer =
                [&] {
                  if (!swiss::is_power_of_two(capacity)) {
                    throw std::invalid_argument(
                        "spsc_queue::spsc_queue(): invalid capacity != 2^n");
                  }
                  return std::allocator_traits<allocator_type>::allocate(
                      allocator_, capacity);
                }(),
        },
        reader_{.capacity    = writer_.capacity,
                .ring_buffer = writer_.ring_buffer} {}

  /**
   * @warning Destructor requires exlusive access to the queue
   */
  constexpr ~spsc_queue() noexcept {
    while (try_pop([this](value_type * front) noexcept {
      std::allocator_traits<allocator_type>::destroy(allocator_, front);
    })) {
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, reader_.ring_buffer, *reader_.capacity);
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return try_push([&](value_type * front) {
      std::allocator_traits<allocator_type>::construct(
          allocator_, front, std::forward<Args>(args)...);
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
    return try_pop([&](value_type * front) noexcept {
      swiss::scope_exit const scope_exit([&]() noexcept {
        std::allocator_traits<allocator_type>::destroy(allocator_, front);
      });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto
  emplace(std::stop_token const & stop_token, Args &&... args) noexcept(
      std::is_nothrow_constructible_v<value_type, Args...>) -> bool {
    return push(stop_token, [&](value_type * front) {
      std::allocator_traits<allocator_type>::construct(
          allocator_, front, std::forward<Args>(args)...);
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
    return pop(stop_token, [&](value_type * front) noexcept {
      swiss::scope_exit const scope_exit([&]() noexcept {
        std::allocator_traits<allocator_type>::destroy(allocator_, front);
      });
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
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    swiss::aligned<value_type, Alignment> * const ring_buffer;
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

  [[nodiscard]] constexpr auto at(
      swiss::aligned<value_type, Alignment> * ring_buffer, index_type capacity,
      index_type index) const noexcept -> value_type * {
    return &**(ring_buffer + slot(capacity, index));
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
    writer_.index.store(next(push_to), std::memory_order::release);
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
    reader_.index.store(next(pop_from), std::memory_order::release);
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
        writer_.index.store(next(push_to), std::memory_order::release);
        return true;
      }

      std::this_thread::yield();

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
        reader_.index.store(next(pop_from), std::memory_order::release);
        return true;
      }

      std::this_thread::yield();

      reader_.cache = writer_.index.load(std::memory_order::acquire);
    }

    return false;
  }
};

}  // namespace flow
