#include <flow/spsc_msgbuf.hpp>

#include <benchmark/benchmark.h>

#include "templates/msgbuf.hpp"
#include "templates/templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

using msgbuf_type =
    flow::spsc_msgbuf<std::size_t, swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_T(spsc_msgbuf, nonblocking_throughput, msgbuf_type)
    /* stream_capacity, producers, consumers, msg_size */
    ->Args({128, 1, 1, 8})
    ->Args({128, 1, 1, 16})
    ->Args({512, 1, 1, 54})
    ->Args({1024, 1, 1, 128})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_T(spsc_msgbuf, blocking_throughput, msgbuf_type)
    /* stream_capacity, producers, consumers, msg_size */
    ->Args({128, 1, 1, 8})
    ->Args({128, 1, 1, 16})
    ->Args({512, 1, 1, 54})
    ->Args({1024, 1, 1, 128})
    ->UseRealTime()
    ->Threads(2);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
