#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <stop_token>

#include <swiss/aligned.hpp>
#include <swiss/dynamic_extent.hpp>
#include <swiss/ebo.hpp>
#include <swiss/manual_lifetime.hpp>
#include <swiss/memory.hpp>
#include <swiss/mixins.hpp>
#include <swiss/predicates.hpp>
#include <swiss/scope_exit.hpp>

#include "noop_wait_policy.hpp"
#include "wait_policy.hpp"

namespace flow {

/**
 * @brief Unbounded threadsafe queue for 1 producer and 1 consumer
 * @note Implemented as a list of contiguous chunks with a free-list for chunk
 * recycling for chunk recycling to achieve zero-allocation steady-state
 * @note ChunkCapacity must be a power of 2 for efficient division
 */
template <typename Value, std::unsigned_integral Index = std::size_t,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
          Index ChunkCapacity = swiss::kDynamicExtent<Index>,
          Index Alignment     = alignof(swiss::manual_lifetime<Value>),
          typename Allocator  = std::allocator<Value>,
          concepts::wait_policy WaitPolicy = noop_wait_policy>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class unbounded_spsc_queue : public swiss::non_copyable,
                             public swiss::non_movable {
 private:
  using entry_type         = swiss::manual_lifetime<Value>;
  using aligned_entry_type = swiss::aligned<entry_type, Alignment>;

  struct chunk_tail final {
    aligned_entry_type * next = nullptr;
  };

  /**
   * @note Chunk tail is stored at the end of each value strip and occupies an
   * amount bytes which is a multiple of the value alignment
   */
  static constexpr Index kAlignedChunkTailEntries =
      swiss::align_up(sizeof(chunk_tail), Alignment) / Alignment;

 public:
  using value_type      = Value;
  using reference       = Value &;
  using const_reference = Value const &;
  using pointer         = Value *;
  using const_pointer   = Value const *;
  using index_type      = Index;
  using allocator_type  = std::allocator_traits<
       Allocator>::template rebind_alloc<aligned_entry_type>;
  using wait_policy_type = WaitPolicy;

 private:
  [[nodiscard]] static constexpr auto valid(index_type size) noexcept -> bool {
    return swiss::is_power_of_two(size);
  }

  [[nodiscard]] constexpr auto allocate_chunk() -> aligned_entry_type * {
    aligned_entry_type * const chunk =
        std::allocator_traits<allocator_type>::allocate(
            allocator_, *writer_.chunk_capacity + kAlignedChunkTailEntries);

    for (index_type index = 0; index < *writer_.chunk_capacity; ++index) {
      std::allocator_traits<allocator_type>::construct(
          allocator_, aligned_at(chunk, *writer_.chunk_capacity, index));
    }

    write_tail(chunk, *writer_.chunk_capacity, chunk_tail{});

    shared_.chunks.fetch_add(1, std::memory_order::relaxed);

    return chunk;
  }

  constexpr void deallocate_chunk(aligned_entry_type * chunk) noexcept {
    for (index_type index = 0; index < *reader_.chunk_capacity; ++index) {
      std::allocator_traits<allocator_type>::destroy(
          allocator_, aligned_at(chunk, *reader_.chunk_capacity, index));
    }

    std::allocator_traits<allocator_type>::deallocate(
        allocator_, chunk, *reader_.chunk_capacity + kAlignedChunkTailEntries);

    shared_.chunks.fetch_sub(1, std::memory_order::relaxed);
  }

  constexpr void deallocate_chain(aligned_entry_type * chunk) noexcept {
    while (chunk) {
      aligned_entry_type * const next =
          read_tail(chunk, *reader_.chunk_capacity).next;
      deallocate_chunk(chunk);
      chunk = next;
    }
  };

  [[nodiscard]] constexpr auto try_reclaim_chunk() noexcept
      -> aligned_entry_type * {
    if (!writer_.freelist) {
      writer_.freelist =
          shared_.freelist.exchange(nullptr, std::memory_order::acquire);
      if (!writer_.freelist) {
        return nullptr;
      }
    }

    aligned_entry_type * const chunk = writer_.freelist;
    writer_.freelist = read_tail(chunk, *writer_.chunk_capacity).next;
    write_tail(chunk, *writer_.chunk_capacity, chunk_tail{});

    return chunk;
  }

  constexpr void retire_chunk(aligned_entry_type * chunk) noexcept {
    aligned_entry_type * head =
        shared_.freelist.load(std::memory_order::relaxed);

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while)
    do {
      write_tail(chunk, *reader_.chunk_capacity, chunk_tail{.next = head});
    } while (!shared_.freelist.compare_exchange_weak(
        head, chunk, std::memory_order::release, std::memory_order::relaxed));
  }

 public:
  explicit constexpr unbounded_spsc_queue(
      allocator_type allocator     = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(ChunkCapacity != swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.chunk = allocate_chunk()},
        reader_{.chunk = writer_.chunk} {
    static_assert(
        valid(ChunkCapacity),
        "unbounded_spsc_queue::unbounded_spsc_queue(): invalid chunk capacity");
  }

  explicit constexpr unbounded_spsc_queue(
      index_type chunk_capacity, allocator_type allocator = allocator_type(),
      wait_policy_type wait_policy = wait_policy_type())
    requires(ChunkCapacity == swiss::kDynamicExtent<index_type>)
      : allocator_(std::move(allocator)),
        wait_policy_(std::move(wait_policy)),
        writer_{.chunk_capacity{chunk_capacity}},
        reader_{.chunk_capacity = writer_.chunk_capacity} {
    if (!valid(chunk_capacity)) {
      throw std::invalid_argument(
          "unbounded_spsc_queue::unbounded_spsc_queue(): invalid "
          "chunk capacity");
    }
    writer_.chunk = allocate_chunk();
    reader_.chunk = writer_.chunk;
  }

  constexpr ~unbounded_spsc_queue() noexcept {
    clear();
    deallocate_chain(reader_.chunk);
    deallocate_chain(writer_.freelist);
    deallocate_chain(shared_.freelist.load(std::memory_order::relaxed));
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto try_emplace(Args &&... args) -> bool {
    return try_push([&](pointer front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto try_push(const_reference value) -> bool {
    return try_emplace(value);
  }

  constexpr auto try_push(value_type && value) -> bool {
    return try_emplace(std::move(value));
  }

  constexpr auto try_pop(auto & value) noexcept -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return try_pop([&](pointer front) noexcept -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(front); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr auto emplace(std::stop_token const & stop_token, Args &&... args)
      -> bool {
    return push(stop_token, [&](pointer front) -> void {
      std::construct_at(front, std::forward<Args>(args)...);
    });
  }

  constexpr auto push(std::stop_token const & stop_token, const_reference value)
      -> bool {
    return emplace(stop_token, value);
  }

  constexpr auto push(std::stop_token const & stop_token, value_type && value)
      -> bool {
    return emplace(stop_token, std::move(value));
  }

  constexpr auto pop(std::stop_token const & stop_token, auto & value) noexcept
      -> bool
    requires(std::is_nothrow_assignable_v<decltype(value), value_type> &&
             std::is_nothrow_destructible_v<value_type>)
  {
    return pop(stop_token, [&](pointer front) noexcept -> void {
      swiss::scope_exit const scope_exit(
          [&]() noexcept -> void { std::destroy_at(front); });
      value = std::move(*front);
    });
  }

 public:
  template <typename... Args>
    requires(std::constructible_from<value_type, Args...>)
  constexpr void emplace(Args &&... args) {
    emplace(std::stop_token{}, std::forward<Args>(args)...);
  }

  constexpr void push(const_reference value) {
    emplace(std::stop_token{}, value);
  }

  constexpr void push(value_type && value) {
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
    return *reader_.chunk_capacity *
           shared_.chunks.load(std::memory_order::relaxed);
  }

  [[nodiscard]] constexpr auto chunk_capacity() const noexcept -> std::size_t {
    return *reader_.chunk_capacity;
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
        [](pointer front) noexcept -> void { std::destroy_at(front); })) {
    }
  }

  constexpr void shrink_to_fit() noexcept {
    deallocate_chain(writer_.freelist);
    deallocate_chain(shared_.freelist.load(std::memory_order::relaxed));
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
    aligned_entry_type * chunk    = nullptr;
    aligned_entry_type * freelist = nullptr;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    [[no_unique_address]] swiss::ebo<
        index_type, ChunkCapacity == swiss::kDynamicExtent<index_type>,
        ChunkCapacity> const chunk_capacity;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  struct alignas(std::hardware_destructive_interference_size)
      SharedCacheLine final {
    std::atomic<aligned_entry_type *> freelist = nullptr;
    std::atomic<index_type> chunks             = 0;
  };

  [[no_unique_address]] allocator_type allocator_;
  [[no_unique_address]] wait_policy_type wait_policy_;

  CacheLine writer_;
  CacheLine reader_;
  SharedCacheLine shared_;

 private:
  [[nodiscard]] constexpr auto read_tail(aligned_entry_type * chunk,
                                         index_type capacity) const noexcept
      -> chunk_tail {
    chunk_tail tail{};
    std::memcpy(&tail, chunk + capacity, sizeof(chunk_tail));
    return tail;
  }

  constexpr void write_tail(aligned_entry_type * chunk, index_type capacity,
                            chunk_tail tail) const noexcept {
    std::memcpy(static_cast<void *>(chunk + capacity), &tail, sizeof(tail));
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

  [[nodiscard]] constexpr auto last(index_type capacity,
                                    index_type index) const noexcept -> bool {
    return slot(capacity, index) == static_cast<index_type>(capacity - 1);
  }

  [[nodiscard]] constexpr auto slot(index_type capacity,
                                    index_type index) const noexcept
      -> index_type {
    return index & static_cast<index_type>(capacity - 1);
  }

  [[nodiscard]] constexpr auto aligned_at(aligned_entry_type * chunk,
                                          index_type capacity,
                                          index_type index) const noexcept
      -> aligned_entry_type * {
    return chunk + slot(capacity, index);
  }

  [[nodiscard]] constexpr auto at(aligned_entry_type * chunk,
                                  index_type capacity,
                                  index_type index) noexcept -> pointer {
    return &***aligned_at(chunk, capacity, index);
  }

 private:
  template <typename Callback>
  static constexpr bool is_callback = std::invocable<Callback, Value *>;

  template <typename Callback>
  static constexpr bool is_nothrow_callback =
      std::is_nothrow_invocable_v<Callback, Value *>;

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto try_push(Callback && callback) -> bool {
    index_type const push_to = writer_.index.load(std::memory_order::relaxed);

    aligned_entry_type * const next = [&]() -> aligned_entry_type * {
      if (!last(*writer_.chunk_capacity, push_to)) {
        return nullptr;
      }

      aligned_entry_type * const _next = try_reclaim_chunk();
      if (_next) {
        return _next;
      }
      return allocate_chunk();
    }();

    try {
      std::invoke(std::forward<Callback>(callback),
                  at(writer_.chunk, *writer_.chunk_capacity, push_to));
    } catch (...) {
      if (next) {
        write_tail(next, *writer_.chunk_capacity,
                   chunk_tail{.next = writer_.freelist});
        writer_.freelist = next;
      }
      throw;
    }

    if (next) {
      write_tail(writer_.chunk, *writer_.chunk_capacity,
                 chunk_tail{.next = next});
      writer_.chunk = next;
    }

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
                at(reader_.chunk, *reader_.chunk_capacity, pop_from));

    if (last(*reader_.chunk_capacity, pop_from)) {
      aligned_entry_type * const next =
          read_tail(reader_.chunk, *reader_.chunk_capacity).next;
      retire_chunk(reader_.chunk);
      reader_.chunk = next;
    }

    reader_.index.store(pop_from + 1, std::memory_order::release);

    wait_policy_.notify_not_full();

    return true;
  }

 private:
  template <typename Callback>
    requires(is_callback<Callback>)
  constexpr auto push(std::stop_token const & stop_token, Callback && callback)
      -> bool {
    if (stop_token.stop_requested()) {
      return false;
    }

    return try_push(std::forward<Callback>(callback));
  }

  template <typename Callback>
    requires(is_nothrow_callback<Callback>)
  constexpr auto pop(std::stop_token const & stop_token,
                     Callback && callback) noexcept -> bool {
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    while (!stop_token.stop_requested()) {
      if (!empty(reader_.cache, pop_from)) {
        std::invoke(std::forward<Callback>(callback),
                    at(reader_.chunk, *reader_.chunk_capacity, pop_from));

        if (last(*reader_.chunk_capacity, pop_from)) {
          aligned_entry_type * const next =
              read_tail(reader_.chunk, *reader_.chunk_capacity).next;
          retire_chunk(reader_.chunk);
          reader_.chunk = next;
        }

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
