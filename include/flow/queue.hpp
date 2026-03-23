#pragma once

#include <concepts>

namespace wip::queues::concepts {

namespace detail {

/**
 * @brief Concept for a generic queue with custom allocator support
 */
template <typename T, bool Noexcept = false>
concept queue = requires(T queue, typename T::value_type value) {
  { queue.try_emplace(value) } -> std::same_as<bool>;
  { queue.emplace(value) } -> std::same_as<void>;
  { queue.try_push(value) } -> std::same_as<bool>;
  { queue.push(value) } -> std::same_as<void>;
  { queue.try_pop(value) } -> std::same_as<bool>;
  { queue.pop() } -> std::convertible_to<typename T::value_type>;
  { queue.capacity() } -> std::convertible_to<typename T::index_type>;
  { queue.size() } -> std::same_as<typename T::index_type>;
  { queue.get_allocator() } -> std::same_as<typename T::allocator_type>;

  requires !Noexcept || noexcept(queue.try_emplace(value));
  requires !Noexcept || noexcept(queue.emplace(value));
  requires !Noexcept || noexcept(queue.try_push(value));
  requires !Noexcept || noexcept(queue.push(value));
  requires !Noexcept || noexcept(queue.try_pop(value));
  requires !Noexcept || noexcept(queue.pop());
  requires noexcept(queue.capacity());
  requires noexcept(queue.size());
  requires noexcept(queue.get_allocator());
};

}  // namespace detail

template <typename T>
concept queue = detail::queue<T, false>;

template <typename T>
concept noexcept_queue = detail::queue<T, true>;

}  // namespace wip::queues::concepts
