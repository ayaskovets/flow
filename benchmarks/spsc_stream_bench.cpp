#include <flow/spsc_stream.hpp>

#include <benchmark/benchmark.h>

#include "templates/stream.hpp"
#include "templates/templates.hpp"

namespace benchmarks {
// NOLINTBEGIN(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)

using stream_t =
    flow::spsc_stream<std::size_t, swiss::kDynamicExtent<std::size_t>>;

BENCHMARK_T(spsc_stream, nonblocking_throughput, stream_t)
    /* stream_capacity, producers, consumers, buffer_capacity */
    ->Args({63, 1, 1, 4})
    ->Args({127, 1, 1, 8})
    ->Args({127, 1, 1, 16})
    ->Args({512, 1, 1, 54})
    ->Args({1023, 1, 1, 128})
    ->UseRealTime()
    ->Threads(2);

BENCHMARK_T(spsc_stream, blocking_throughput, stream_t)
    /* stream_capacity, producers, consumers, buffer_capacity */
    ->Args({63, 1, 1, 4})
    ->Args({127, 1, 1, 8})
    ->Args({127, 1, 1, 16})
    ->Args({512, 1, 1, 54})
    ->Args({1023, 1, 1, 128})
    ->UseRealTime()
    ->Threads(2);

// NOLINTEND(misc-use-internal-linkage,cppcoreguidelines-avoid-magic-numbers)
}  // namespace benchmarks
