#pragma once

#include <concepts>
#include <span>
#include <stop_token>

namespace flow::concepts {

/**
 * @brief Generic byte stream of fixed capacity with custom allocator support
 */
template <typename T>
concept stream = requires(T stream) {
  { stream.capacity() } -> std::same_as<typename T::index_type>;
  { stream.size() } -> std::same_as<typename T::index_type>;
  { stream.get_allocator() } -> std::same_as<typename T::allocator_type>;

  requires noexcept(stream.capacity());
  requires noexcept(stream.size());
  requires noexcept(stream.get_allocator());
};

/**
 * @brief Generic byte stream that supports nonblocking operations
 * @note Nonblocking methods insert and remove as many bytes as possible and
 * return the number of bytes written/read
 */
template <typename T>
concept nonblocking_stream =
    stream<T> && requires(T stream, std::span<std::byte> bytes) {
      { stream.try_push(bytes) } -> std::same_as<typename T::index_type>;
      { stream.try_pop(bytes) } -> std::same_as<typename T::index_type>;
    };

/**
 * @brief Nonblocking byte stream with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_stream =
    nonblocking_stream<T> && requires(T stream, std::span<std::byte> bytes) {
      requires noexcept(stream.try_push(bytes));
      requires noexcept(stream.try_pop(bytes));
    };

/**
 * @brief Generic byte stream that supports blocking operations
 * @note Blocking methods block until the whole input buffer is inserted into
 * the stream or the input buffer is full with the data from the stream
 */
template <typename T>
concept blocking_stream =
    stream<T> && requires(T stream, std::span<std::byte> bytes) {
      { stream.push(bytes) } -> std::same_as<void>;
      { stream.pop(bytes) } -> std::same_as<void>;
    };

/**
 * @brief Blocking byte stream with noexcept operations
 */
template <typename T>
concept noexcept_blocking_stream =
    blocking_stream<T> && requires(T stream, std::span<std::byte> bytes) {
      requires noexcept(stream.push(bytes));
      requires noexcept(stream.pop(bytes));
    };

/**
 * @brief Blocking byte stream that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_stream =
    blocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes, std::stop_token stop_token) {
      { stream.push(stop_token, bytes) } -> std::same_as<bool>;
      { stream.pop(stop_token, bytes) } -> std::same_as<bool>;
    };

/**
 * @brief Blocking byte stream that supports cancellation tokens with noexcept
 * operations
 */
template <typename T>
concept noexcept_cancellable_blocking_stream =
    cancellable_blocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes, std::stop_token stop_token) {
      requires noexcept(stream.push(stop_token, bytes));
      requires noexcept(stream.pop(stop_token, bytes));
    };

}  // namespace flow::concepts
