#include <flow/spmc_queue.hpp>

#include <benchmark/benchmark.h>

#include "templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

template <typename Value>
using queue_t =
    flow::spmc_queue<Value, std::size_t, swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_TEMPLATE_T(spmc_queue, nonblocking_throughput, int, queue_t<int>)
    /* queue_capacity, producers, consumers */
    ->Args({64, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spmc_queue, blocking_throughput, int, queue_t<int>)
    /* queue_capacity, producers, consumers */
    ->Args({64, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spmc_queue, nonblocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({64, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spmc_queue, blocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({64, 1, 1})
    ->UseRealTime()
    ->Threads(2);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
