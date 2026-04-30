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
  { stream.capacity() } -> std::same_as<std::size_t>;
  { stream.size() } -> std::same_as<std::size_t>;
  { stream.get_allocator() } -> std::same_as<typename T::allocator_type>;
};

/**
 * @brief Generic byte stream that supports nonblocking operations
 * @note Nonblocking methods return the number of bytes written/read
 * @note Nonblocking methods insert and remove as many bytes as possible and
 * return the number of bytes written/read
 * @note Callback-based methods may return two contiguous chunks of memory
 * depending on the implementation
 */
template <typename T>
concept nonblocking_stream =
    stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t max_size) {
      { stream.try_push(callback, max_size) } -> std::same_as<std::size_t>;
      { stream.try_pop(callback, max_size) } -> std::same_as<std::size_t>;
      { stream.try_push(bytes) } -> std::same_as<std::size_t>;
      { stream.try_pop(bytes) } -> std::same_as<std::size_t>;
    };

/**
 * @brief Nonblocking byte stream with noexcept operations
 */
template <typename T>
concept noexcept_nonblocking_stream =
    nonblocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t max_size) {
      requires noexcept(stream.try_push(callback, max_size));
      requires noexcept(stream.try_pop(callback, max_size));
      requires noexcept(stream.try_push(bytes));
      requires noexcept(stream.try_pop(bytes));
    };

/**
 * @brief Generic byte stream that supports blocking operations
 * @note Blocking methods wait until the whole input buffer is inserted into
 * the stream (for writes) / input buffer is full with the data from the stream
 * (for reads) or return immediately if input size in invalid
 */
template <typename T>
concept blocking_stream =
    stream<T> &&
    requires(T stream, std::span<std::byte> bytes,
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t exact_size) {
      { stream.push(exact_size, callback) } -> std::same_as<void>;
      { stream.pop(exact_size, callback) } -> std::same_as<void>;
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
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t exact_size) {
      requires noexcept(stream.push(exact_size, callback));
      requires noexcept(stream.pop(exact_size, callback));
      requires noexcept(stream.push(bytes));
      requires noexcept(stream.pop(bytes));
    };

/**
 * @brief Blocking byte stream that supports cancellation tokens
 * @note Calcellable methods return false if operation was cancelled by
 * stop_token
 */
template <typename T>
concept cancellable_blocking_stream =
    blocking_stream<T> &&
    requires(T stream, std::span<std::byte> bytes, std::stop_token stop_token,
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t exact_size) {
      { stream.push(stop_token, exact_size, callback) } -> std::same_as<bool>;
      { stream.pop(stop_token, exact_size, callback) } -> std::same_as<bool>;
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
             decltype([](std::span<std::byte>, std::span<std::byte>) noexcept {
             }) callback,
             std::size_t exact_size) {
      requires noexcept(stream.push(stop_token, exact_size, callback));
      requires noexcept(stream.pop(stop_token, exact_size, callback));
      requires noexcept(stream.push(stop_token, bytes));
      requires noexcept(stream.pop(stop_token, bytes));
    };

}  // namespace flow::concepts
