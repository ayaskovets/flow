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
 */
template <typename T>
concept nonblocking_msgbuf =
    msgbuf<T> && requires(T msgbuf, std::span<std::byte> bytes) {
      { msgbuf.try_push(bytes) } -> std::same_as<bool>;
      { msgbuf.try_pop(bytes) } -> std::same_as<bool>;
    };

/**
 * @brief Nonblocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_msgbuf =
    nonblocking_msgbuf<T> && requires(T msgbuf, std::span<std::byte> bytes) {
      requires noexcept(msgbuf.try_push(bytes));
      requires noexcept(msgbuf.try_pop(bytes));
    };

/**
 * @brief Generic message buffer that supports blocking operations
 */
template <typename T>
concept blocking_msgbuf =
    msgbuf<T> && requires(T msgbuf, std::span<std::byte> bytes) {
      { msgbuf.push(bytes) } -> std::same_as<void>;
      { msgbuf.pop(bytes) } -> std::same_as<void>;
    };

/**
 * @brief Blocking message buffer with noexcept operations
 */
template <typename T>
concept noexcept_blocking_msgbuf =
    blocking_msgbuf<T> && requires(T msgbuf, std::span<std::byte> bytes) {
      requires noexcept(msgbuf.push(bytes));
      requires noexcept(msgbuf.pop(bytes));
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_msgbuf =
    blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> bytes, std::stop_token stop_token) {
      { msgbuf.push(stop_token, bytes) } -> std::same_as<bool>;
      { msgbuf.pop(stop_token, bytes) } -> std::same_as<bool>;
    };

/**
 * @brief Blocking message buffer that supports cancellation tokens with
 * noexcept operations
 */
template <typename T>
concept noexcept_cancellable_blocking_msgbuf =
    cancellable_blocking_msgbuf<T> &&
    requires(T msgbuf, std::span<std::byte> bytes, std::stop_token stop_token) {
      requires noexcept(msgbuf.push(stop_token, bytes));
      requires noexcept(msgbuf.pop(stop_token, bytes));
    };

}  // namespace flow::concepts
