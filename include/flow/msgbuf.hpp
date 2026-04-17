#pragma once

#include <concepts>
#include <span>
#include <stop_token>

namespace flow::concepts {

/**
 * @brief Generic message buffer of fixed capacity with custom allocator support
 * @note If the message buffer is empty, peek() should return 0
 * @warning peek() should only be called from the consumer thread
 */
template <typename T>
concept msgbuf = requires(T msgbuf) {
  { msgbuf.capacity() } -> std::same_as<typename T::index_type>;
  { msgbuf.size() } -> std::same_as<typename T::index_type>;
  { msgbuf.peek() } -> std::same_as<typename T::index_type>;
  { msgbuf.get_allocator() } -> std::same_as<typename T::allocator_type>;

  requires noexcept(msgbuf.capacity());
  requires noexcept(msgbuf.size());
  requires noexcept(msgbuf.peek());
  requires noexcept(msgbuf.get_allocator());
};

/**
 * @brief Generic message buffer that supports nonblocking operations
 * @note Nonblocking methods return true only upon a successful insertion or
 * removal
 * @warning Popping the message requires buffer to be exactly the same size as
 * the message that gets popped, otherwise false is returned. To reserve the
 * necessary storage use the peek() method
 */
template <typename T>
concept nonblocking_msgbuf =
    msgbuf<T> && requires(T msgbuf, std::span<std::byte> msg) {
      { msgbuf.try_push(msg) } -> std::same_as<bool>;
      { msgbuf.try_pop(msg) } -> std::same_as<bool>;
    };

/**
 * @brief Nonblocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_msgbuf =
    nonblocking_msgbuf<T> && requires(T msgbuf, std::span<std::byte> msg) {
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
    msgbuf<T> && requires(T msgbuf, std::span<std::byte> msg) {
      { msgbuf.push(msg) } -> std::same_as<void>;
      { msgbuf.pop(msg) } -> std::same_as<void>;
    };

/**
 * @brief Blocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_blocking_msgbuf =
    blocking_msgbuf<T> && requires(T msgbuf, std::span<std::byte> msg) {
      requires noexcept(msgbuf.push(msg));
      requires noexcept(msgbuf.pop(msg));
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> msg, std::stop_token stop_token) {
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
    requires(T msgbuf, std::span<std::byte> msg, std::stop_token stop_token) {
      requires noexcept(msgbuf.push(stop_token, msg));
      requires noexcept(msgbuf.pop(stop_token, msg));
    };

}  // namespace flow::concepts
