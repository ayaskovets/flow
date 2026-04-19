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
 * @note Callback-based methods reserve the a chunk in the stream of size less
 * or equal than the size value provided by the caller
 * @note Callback-based methods return a boolean indicating whether the
 * insertion/removal of the chunk passed to the callback took place
 */
template <typename T>
concept nonblocking_stream =
    stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      { stream.try_push(callback, max_size) } -> std::same_as<bool>;
      { stream.try_pop(callback, max_size) } -> std::same_as<bool>;
      { stream.try_push(bytes) } -> std::same_as<typename T::index_type>;
      { stream.try_pop(bytes) } -> std::same_as<typename T::index_type>;
    };

/**
 * @brief Nonblocking byte stream with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_stream =
    nonblocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      requires noexcept(stream.try_push(callback, max_size));
      requires noexcept(stream.try_pop(callback, max_size));
      requires noexcept(stream.try_push(bytes));
      requires noexcept(stream.try_pop(bytes));
    };

/**
 * @brief Generic byte stream that supports blocking operations
 * @note Blocking methods wait until the whole input buffer is inserted into
 * the stream (for writes) / input buffer is full with the data from the stream
 * (for reads)
 * @note Callback-based methods reserve the a chunk in the stream of size less
 * or equal than the size value provided by the caller
 */
template <typename T>
concept blocking_stream =
    stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      { stream.push(callback, max_size) } -> std::same_as<void>;
      { stream.pop(callback, max_size) } -> std::same_as<void>;
      { stream.push(bytes) } -> std::same_as<void>;
      { stream.pop(bytes) } -> std::same_as<void>;
    };

/**
 * @brief Blocking byte stream with noexcept operations
 */
template <typename T>
concept noexcept_blocking_stream =
    blocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      requires noexcept(stream.push(callback, max_size));
      requires noexcept(stream.pop(callback, max_size));
      requires noexcept(stream.push(bytes));
      requires noexcept(stream.pop(bytes));
    };

/**
 * @brief Blocking byte stream that supports cancellation tokens
 */
template <typename T>
concept cancellable_blocking_stream =
    blocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes, std::stop_token stop_token,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      { stream.push(stop_token, callback, max_size) } -> std::same_as<bool>;
      { stream.pop(stop_token, callback, max_size) } -> std::same_as<bool>;
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
    requires(T stream, std::span<std::byte> bytes, std::stop_token stop_token,
             decltype([](std::span<std::byte>) noexcept {}) callback,
             typename T::index_type max_size) {
      requires noexcept(stream.push(stop_token, callback, max_size));
      requires noexcept(stream.pop(stop_token, callback, max_size));
      requires noexcept(stream.push(stop_token, bytes));
      requires noexcept(stream.pop(stop_token, bytes));
    };

}  // namespace flow::concepts
