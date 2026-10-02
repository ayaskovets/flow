#include <flow/locked_ring_buffer.hpp>

#include <benchmark/benchmark.h>

#include "templates/queue.hpp"
#include "templates/templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

template <typename Value>
using queue_type = flow::locked_ring_buffer<Value, std::size_t,
                                            swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_TEMPLATE_T(locked_ring_buffer, nonblocking_throughput, int,
                     queue_type<int>)
    /* queue_capacity, producers, consumers */
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_ring_buffer, blocking_throughput, int,
                     queue_type<int>)
    /* queue_capacity, producers, consumers */
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_ring_buffer, nonblocking_throughput, shared_ptr,
                     queue_type<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_ring_buffer, blocking_throughput, shared_ptr,
                     queue_type<std::shared_ptr<int>>)
    /* queue_capacity, producers, consumers */
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
