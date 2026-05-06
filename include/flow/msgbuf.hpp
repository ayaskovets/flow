#pragma once

#include <concepts>
#include <span>
#include <stop_token>

namespace flow::concepts {

/**
 * @brief Generic message buffer of fixed capacity with custom allocator support
 */
template <typename T>
concept msgbuf = requires(T msgbuf) {
  { T::header_size() } -> std::same_as<std::size_t>;
  { msgbuf.capacity() } -> std::same_as<std::size_t>;
  { msgbuf.size() } -> std::same_as<std::size_t>;
  { msgbuf.empty() } -> std::same_as<bool>;
  { msgbuf.get_allocator() } -> std::same_as<typename T::allocator_type>;
};

/**
 * @brief Generic message buffer that supports nonblocking operations
 * @note Nonblocking push methods return true only upon a successful insertion
 * or removal
 * @note Nonblocking pop methods return the size of the popped message
 * @note Callback-based methods reserve a contiguous chunk of memory (which may
 * require padding) and pass it to the callback
 * @warning In non-callback-based pop the message gets truncated if it is larger
 * than the provider buffer
 */
template <typename T>
concept nonblocking_msgbuf =
    msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      { msgbuf.try_push(msg_size, callback) } -> std::same_as<bool>;
      { msgbuf.try_pop(callback) } -> std::same_as<std::size_t>;
      { msgbuf.try_push(msg) } -> std::same_as<bool>;
      { msgbuf.try_pop(msg) } -> std::same_as<std::size_t>;
    };

/**
 * @brief Nonblocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_msgbuf =
    nonblocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      requires noexcept(msgbuf.try_push(msg_size, callback));
      requires noexcept(msgbuf.try_pop(callback));
      requires noexcept(msgbuf.try_push(msg));
      requires noexcept(msgbuf.try_pop(msg));
    };

/**
 * @brief Generic message buffer that supports blocking operations
 * @note Blocking methods wait until the operation is completed or return
 * immediately if input size in invalid
 * @note Blocking pop methods return the size of the popped message
 * @warning In non-callback-based pop the message gets truncated if it is larger
 * than the provider buffer
 */
template <typename T>
concept blocking_msgbuf =
    msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      { msgbuf.push(msg_size, callback) } -> std::same_as<void>;
      { msgbuf.pop(callback) } -> std::same_as<std::size_t>;
      { msgbuf.push(msg) } -> std::same_as<void>;
      { msgbuf.pop(msg) } -> std::same_as<std::size_t>;
    };

/**
 * @brief Blocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      requires noexcept(msgbuf.push(msg_size, callback));
      requires noexcept(msgbuf.pop(callback));
      requires noexcept(msgbuf.push(msg));
      requires noexcept(msgbuf.pop(msg));
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens
 * @note Calcellable methods return false if operation was cancelled by
 * stop_token
 */
template <typename T>
concept cancellable_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::stop_token stop_token,
             std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      { msgbuf.push(stop_token, msg_size, callback) } -> std::same_as<bool>;
      { msgbuf.pop(stop_token, callback) } -> std::same_as<bool>;
      { msgbuf.push(stop_token, msg) } -> std::same_as<bool>;
      { msgbuf.pop(stop_token, msg) } -> std::same_as<bool>;
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens with
 * noexcept operations
 */
template <typename T>
concept noexcept_cancellable_blocking_msgbuf =
    cancellable_blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::stop_token stop_token,
             std::size_t msg_size,
             decltype([](std::span<std::byte>) noexcept {}) callback) {
      requires noexcept(msgbuf.push(stop_token, msg_size, callback));
      requires noexcept(msgbuf.pop(stop_token, callback));
      requires noexcept(msgbuf.push(stop_token, msg));
      requires noexcept(msgbuf.pop(stop_token, msg));
    };

}  // namespace flow::concepts
