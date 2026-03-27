#include <flow/locked_mpmc_queue.hpp>

#include <benchmark/benchmark.h>

#include "templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

template <typename Value>
using queue_t = flow::locked_mpmc_queue<Value, std::size_t,
                                        swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_TEMPLATE_T(locked_mpmc_queue, nonblocking_throughput, int,
                     queue_t<int>)
    /* queue_size, producers, consumers */
    ->Args({64, 1, 1})
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_mpmc_queue, blocking_throughput, int, queue_t<int>)
    /* queue_size, producers, consumers */
    ->Args({64, 1, 1})
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_mpmc_queue, nonblocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_size, producers, consumers */
    ->Args({64, 1, 1})
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_TEMPLATE_T(locked_mpmc_queue, blocking_throughput, shared_ptr,
                     queue_t<std::shared_ptr<int>>)
    /* queue_size, producers, consumers */
    ->Args({64, 1, 1})
    ->Args({128, 2, 2})
    ->Args({512, 4, 4})
    ->UseRealTime()
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
