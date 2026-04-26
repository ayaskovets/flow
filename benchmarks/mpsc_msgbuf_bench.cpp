#include <flow/mpsc_msgbuf.hpp>

#include <benchmark/benchmark.h>

#include "templates/msgbuf.hpp"
#include "templates/templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

using msgbuf_t =
    flow::mpsc_msgbuf<std::size_t, swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_T(mpsc_msgbuf, nonblocking_throughput, msgbuf_t)
    /* stream_capacity, producers, consumers, msg_size */
    ->Args({64, 1, 1, 4})
    ->Args({128, 1, 1, 8})
    ->Args({128, 2, 1, 16})
    ->Args({512, 2, 1, 54})
    ->Args({1024, 4, 1, 128})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_T(mpsc_msgbuf, blocking_throughput, msgbuf_t)
    /* stream_capacity, producers, consumers, msg_size */
    ->Args({64, 1, 1, 4})
    ->Args({128, 1, 1, 8})
    ->Args({128, 2, 1, 16})
    ->Args({512, 2, 1, 54})
    ->Args({1024, 4, 1, 128})
    ->UseRealTime()
    ->Threads(2);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
