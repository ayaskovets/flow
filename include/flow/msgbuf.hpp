#pragma once

#include <concepts>
#include <span>
#include <stop_token>

namespace flow::concepts {

/**
 * @brief Generic message buffer of fixed capacity with custom allocator support
 * @note Callback-based methods either reserve a contiguous chunk of memory
 * (which involves adding padding if the message does not fit at the vacant end
 * of the ring buffer) and call the callback with the reserved chunk or return
 * false
 */
template <typename T>
concept msgbuf = requires(T msgbuf) {
  { msgbuf.capacity() } -> std::same_as<typename T::index_type>;
  { msgbuf.size() } -> std::same_as<typename T::index_type>;
  { msgbuf.get_allocator() } -> std::same_as<typename T::allocator_type>;

  requires noexcept(msgbuf.capacity());
  requires noexcept(msgbuf.size());
  requires noexcept(msgbuf.get_allocator());
};

/**
 * @brief Generic message buffer that supports nonblocking operations
 * @note Nonblocking methods return true only upon a successful insertion or
 * removal
 * @warning Popping the message with non-callback-based methods requires buffer
 * to be exactly the same size as the message that gets popped, otherwise false
 * is returned
 */
template <typename T>
concept nonblocking_msgbuf =
    msgbuf<T> &&
    requires(
        T msgbuf, std::span<std::byte> msg, typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      { msgbuf.try_push(size, callback) } -> std::same_as<bool>;
      { msgbuf.try_pop(callback) } -> std::same_as<bool>;
      { msgbuf.try_push(msg) } -> std::same_as<bool>;
      { msgbuf.try_pop(msg) } -> std::same_as<bool>;
    };

/**
 * @brief Nonblocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_msgbuf =
    nonblocking_msgbuf<T> &&
    requires(
        T msgbuf, std::span<std::byte> msg, typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      requires noexcept(msgbuf.try_push(size, callback));
      requires noexcept(msgbuf.try_pop(callback));
      requires noexcept(msgbuf.try_push(msg));
      requires noexcept(msgbuf.try_pop(msg));
    };

/**
 * @brief Generic message buffer that supports blocking operations
 * @note Blocking methods block until the message is inserted into the message
 * buffer or popped from the message buffer
 * @warning The pop() blocks until message buffer is not empty and the message
 * to be popped is exactly the same size as the input buffer. Beware that it can
 * block indefinitely if the message of the expected size never arrives
 */
template <typename T>
concept blocking_msgbuf =
    msgbuf<T> &&
    requires(
        T msgbuf, std::span<std::byte> msg, typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      { msgbuf.push(size, callback) } -> std::same_as<void>;
      { msgbuf.pop(callback) } -> std::same_as<void>;
      { msgbuf.push(msg) } -> std::same_as<void>;
      { msgbuf.pop(msg) } -> std::same_as<void>;
    };

/**
 * @brief Blocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(
        T msgbuf, std::span<std::byte> msg, typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      requires noexcept(msgbuf.push(size, callback));
      requires noexcept(msgbuf.pop(callback));
      requires noexcept(msgbuf.push(msg));
      requires noexcept(msgbuf.pop(msg));
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(
        T msgbuf, std::span<std::byte> msg, std::stop_token stop_token,
        typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      { msgbuf.push(stop_token, size, callback) } -> std::same_as<bool>;
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
    requires(
        T msgbuf, std::span<std::byte> msg, std::stop_token stop_token,
        typename T::index_type size,
        decltype([](std::span<std::byte>) noexcept { return true; }) callback) {
      requires noexcept(msgbuf.push(stop_token, size, callback));
      requires noexcept(msgbuf.pop(stop_token, callback));
      requires noexcept(msgbuf.push(stop_token, msg));
      requires noexcept(msgbuf.pop(stop_token, msg));
    };

}  // namespace flow::concepts
