#pragma once

#include <concepts>
#include <stop_token>

namespace flow::concepts {

/**
 * @brief Generic queue of fixed capacity with custom allocator support and
 * typed items
 */
template <typename T>
concept queue = requires(T queue, typename T::value_type) {
  { queue.capacity() } -> std::same_as<typename T::index_type>;
  { queue.size() } -> std::same_as<typename T::index_type>;
  { queue.get_allocator() } -> std::same_as<typename T::allocator_type>;

  requires noexcept(queue.capacity());
  requires noexcept(queue.size());
  requires noexcept(queue.get_allocator());
};

/**
 * @brief Generic queue that supports nonblocking operations
 * @note Nonblocking methods return a boolean indicating whether
 * insertion/removal took place
 * @note Callback-based methods receive a reference to the value slot at the
 * moment the value is accessible for a write/read
 * @note Code inside the callback should be minimal not to block other
 * threads
 */
template <typename T>
concept nonblocking_queue =
    queue<T> &&
    requires(T queue, typename T::value_type value,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      { queue.try_push(callback) } -> std::same_as<bool>;
      { queue.try_pop(callback) } -> std::same_as<bool>;
      { queue.try_emplace(std::move(value)) } -> std::same_as<bool>;
      { queue.try_push(std::move(value)) } -> std::same_as<bool>;
      { queue.try_pop(value) } -> std::same_as<bool>;
    };

/**
 * @brief Nonblocking queue with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_queue =
    nonblocking_queue<T> &&
    requires(T queue, typename T::value_type value,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      requires noexcept(queue.try_push(callback));
      requires noexcept(queue.try_pop(callback));
      requires noexcept(queue.try_emplace(std::move(value)));
      requires noexcept(queue.try_push(std::move(value)));
      requires noexcept(queue.try_pop(value));
    };

/**
 * @brief Generic queue that supports blocking operations
 * @note Blocking methods block until the value gets inserted or removed from
 * the queue
 * @note Callback-based methods receive a reference to the value slot at the
 * moment the value is accessible for a write/read
 * @note Code inside the callback should be minimal not to block other
 * threads
 */
template <typename T>
concept blocking_queue =
    queue<T> &&
    requires(T queue, typename T::value_type value,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      { queue.push(callback) } -> std::same_as<void>;
      { queue.pop(callback) } -> std::same_as<void>;
      { queue.emplace(std::move(value)) } -> std::same_as<void>;
      { queue.push(std::move(value)) } -> std::same_as<void>;
      { queue.pop(value) } -> std::same_as<void>;
    };

/**
 * @brief Blocking queue with noexcept operations
 */
template <typename T>
concept noexcept_blocking_queue =
    blocking_queue<T> &&
    requires(T queue, typename T::value_type value,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      requires noexcept(queue.push(callback));
      requires noexcept(queue.pop(callback));
      requires noexcept(queue.emplace(std::move(value)));
      requires noexcept(queue.push(std::move(value)));
      requires noexcept(queue.pop(value));
    };

/**
 * @brief Blocking queue that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_queue =
    blocking_queue<T> &&
    requires(T queue, typename T::value_type value, std::stop_token stop_token,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      { queue.push(stop_token, callback) } -> std::same_as<void>;
      { queue.pop(stop_token, callback) } -> std::same_as<void>;
      { queue.emplace(stop_token, std::move(value)) } -> std::same_as<bool>;
      { queue.push(stop_token, std::move(value)) } -> std::same_as<bool>;
      { queue.pop(stop_token, value) } -> std::same_as<bool>;
    };

/**
 * @brief Blocking queue that supports cancellation tokens with noexcept
 * operations
 */
template <typename T>
concept noexcept_cancellable_blocking_queue =
    cancellable_blocking_queue<T> &&
    requires(T queue, typename T::value_type value, std::stop_token stop_token,
             decltype([](typename T::value_type &) noexcept {}) callback) {
      requires noexcept(queue.push(stop_token, callback));
      requires noexcept(queue.pop(stop_token, callback));
      requires noexcept(queue.emplace(stop_token, std::move(value)));
      requires noexcept(queue.push(stop_token, std::move(value)));
      requires noexcept(queue.pop(stop_token, value));
    };

}  // namespace flow::concepts
