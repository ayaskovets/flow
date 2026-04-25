#pragma once

#include <benchmark/benchmark.h>

#define BENCHMARK_T(benchmark_suite_name, benchmark_name, type, ...)   \
  constexpr void BM##_##benchmark_suite_name##_##benchmark_name(       \
      benchmark::State & state) {                                      \
    templates::benchmark_name<type>(state __VA_OPT__(, ) __VA_ARGS__); \
  }                                                                    \
  BENCHMARK(BM##_##benchmark_suite_name##_##benchmark_name)

#define BENCHMARK_TEMPLATE_T(benchmark_suite_name, benchmark_name,      \
                             benchmark_tag, type, ...)                  \
  constexpr void                                                        \
      BM##_##benchmark_suite_name##_##benchmark_name##_##benchmark_tag( \
          benchmark::State & state) {                                   \
    templates::benchmark_name<type>(state __VA_OPT__(, ) __VA_ARGS__);  \
  }                                                                     \
  BENCHMARK(BM##_##benchmark_suite_name##_##benchmark_name##_##benchmark_tag)
