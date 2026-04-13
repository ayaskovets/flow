#include <flow/spsc_queue.hpp>

#include <benchmark/benchmark.h>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

template <typename Value>
using queue_t =
    flow::spsc_queue<Value, std::size_t, swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_TEMPLATE_T(spsc_queue, nonblocking_throughput, int, queue_t<int>)
    /* queue_capacity, producers, consumers */
    ->Args({63, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spsc_queue, blocking_throughput, int, queue_t<int>)
    /* queue_capacity, producers, consumers */
    ->Args({63, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spsc_queue, nonblocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({63, 1, 1})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_TEMPLATE_T(spsc_queue, blocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({63, 1, 1})
    ->UseRealTime()
    ->Threads(2);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
