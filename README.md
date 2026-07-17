# flow

A collection of thread-safe queues, ring buffers, byte streams and other containers with main focus on passing data between threads

# brief

This library is header-only and requires C++23

This library uses CMake workflows (CMake >= 3.25.0) for all its build scripts

Concepts that describe the general interface of all of the library containers can be found in [queue.hpp](./include/flow/queue.hpp), [stream.hpp](./include/flow/stream.hpp) and [mbuffer](./include/flow/msgbuf.hpp)

# dependencies

- [swiss](https://github.com/ayaskovets/swiss)

# install

Install location is defined in **installDir** [CMakePresets.json](./CMakePresets.json)

```sh
cmake --workflow --preset=install
```

# benchmark

```sh
cmake --workflow --preset=benchmark | grep -v 'SKIPPED'
```

# test

```sh
cmake --workflow --preset=test
```
