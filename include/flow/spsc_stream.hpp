#pragma once

#include <algorithm>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <stop_token>

#include <swiss/conditionally_runtime.hpp>
#include <swiss/dynamic_extent.hpp>
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
      : writer_{.ring_buffer = allocator.allocate(Capacity + 1)},
        reader_{.ring_buffer = writer_.ring_buffer},
        allocator_(std::move(allocator)) {
    static_assert(Capacity > 0,
                  "spsc_stream::spsc_stream(): invalid capacity < 1");
  }

  constexpr explicit spsc_stream(index_type capacity,
                                 allocator_type allocator = allocator_type())
    requires(Capacity == swiss::kDynamicExtent<index_type>)
      : writer_{.ring_buffer =
                    [capacity, &allocator] {
                      if (capacity == 0) {
                        throw std::invalid_argument(
                            "spsc_stream::spsc_stream(): invalid capacity < 1");
                      }
                      return allocator.allocate(capacity + 1);
                    }(),
                .capacity{capacity}},
        reader_{.ring_buffer = writer_.ring_buffer, .capacity{capacity}},
        allocator_(std::move(allocator)) {}

  constexpr ~spsc_stream() noexcept {
    allocator_.deallocate(reader_.ring_buffer, *reader_.capacity);
  }

 public:
  constexpr auto try_push(std::span<std::byte const> const bytes) noexcept
      -> index_type {
    return push_impl<false, false>(bytes);
  }

  constexpr auto push(std::stop_token stop_token,
                      std::span<std::byte const> const bytes) noexcept -> bool {
    return push_impl<true, true>(stop_token, bytes);
  }

  constexpr void push(std::span<std::byte const> const bytes) noexcept {
    push_impl<true, false>(bytes);
  }

  constexpr auto try_pop(std::span<std::byte> const bytes) noexcept
      -> index_type {
    return pop_impl<false, false>(bytes);
  }

  constexpr auto pop(std::stop_token stop_token,
                     std::span<std::byte> const bytes) noexcept -> bool {
    return pop_impl<true, true>(stop_token, bytes);
  }

  constexpr void pop(std::span<std::byte> const bytes) noexcept {
    pop_impl<true, false>(bytes);
  }

 public:
  [[nodiscard]] constexpr auto capacity() const noexcept -> index_type {
    return *reader_.capacity;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> index_type {
    index_type const push_to  = writer_.index.load(std::memory_order::relaxed);
    index_type const pop_from = reader_.index.load(std::memory_order::relaxed);

    return pop_from <= push_to ? push_to - pop_from
                               : *reader_.capacity + push_to + 1 - pop_from;
  }

 public:
  constexpr auto get_allocator() const noexcept -> allocator_type {
    return allocator_;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
  struct alignas(std::hardware_destructive_interference_size) CacheLine final {
    std::atomic<index_type> index = 0;
    index_type cache              = 0;
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::byte * const ring_buffer;
    [[no_unique_address]] swiss::conditionally_runtime<
        index_type, Capacity == swiss::kDynamicExtent<index_type>,
        Capacity> const capacity;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  CacheLine writer_;
  CacheLine reader_;
  [[no_unique_address]] allocator_type allocator_;

 private:
  [[nodiscard]] constexpr auto full(index_type next_push_to,
                                    index_type pop_from) const noexcept
      -> bool {
    return next_push_to == pop_from;
  }

  [[nodiscard]] constexpr auto empty(index_type push_to,
                                     index_type pop_from) const noexcept
      -> bool {
    return push_to == pop_from;
  }
  [[nodiscard]] constexpr auto next(index_type index, index_type bytes,
                                    index_type capacity) const noexcept
      -> index_type {
    return (index += bytes) >= capacity ? index - capacity : index;
  }

 private:
  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto push_impl(Args &&...) -> decltype(auto) {
    if constexpr (Cancellable || !Blocking) {
      return false;
    }

    // TODO: impl

    // if (bytes.empty() || full()) {
    //   return 0;
    // }

    // index_type const push_to = next(pop_from_, size_);
    // index_type const total =
    //     std::min(static_cast<index_type>(bytes.size()),
    //              static_cast<index_type>(*capacity_ - size_));
    // index_type const postfix = *capacity_ - push_to;
    // index_type const chunk1  = std::min(total, postfix);
    // index_type const chunk2  = total - chunk1;

    // std::copy_n(bytes.data(), chunk1, ring_buffer_ + push_to);
    // std::copy_n(bytes.data() + chunk1, chunk2, ring_buffer_);

    // size_ += total;
    // return total;
  }

  template <bool Blocking, bool Cancellable, typename... Args>
  constexpr auto pop_impl(Args &&...) -> decltype(auto) {
    if constexpr (Cancellable || !Blocking) {
      return false;
    }

    // TODO: impl

    // if (bytes.empty() || empty()) {
    //   return 0;
    // }

    // index_type const total =
    //     std::min(static_cast<index_type>(bytes.size()), size_);
    // index_type const postfix = *capacity_ - pop_from_;
    // index_type const chunk1  = std::min(total, postfix);
    // index_type const chunk2  = total - chunk1;

    // std::copy_n(ring_buffer_ + pop_from_, chunk1, bytes.data());
    // std::copy_n(ring_buffer_, chunk2, bytes.data() + chunk1);

    // size_ -= total;
    // pop_from_ = next(pop_from_, total);
    // return total;
  }
};

}  // namespace flow
