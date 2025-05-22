BASE_CMD="build/release/benchmark/benchmark_runner --root-dir ."

# Regular Iceberg benchmarks
# 1k files
# shellcheck disable=SC2012
bench_count=$(ls -1 benchmark/regular/ordered/1k/*.benchmark | wc -l)
for i in $(seq 1 "${bench_count}"); do
  ${BASE_CMD} benchmark/regular/ordered/1k/"${i}".benchmark
done

# 10k files
# shellcheck disable=SC2012
bench_count=$(ls -1 benchmark/regular/ordered/10k/*.benchmark | wc -l)
for i in $(seq 1 "${bench_count}"); do
  ${BASE_CMD} benchmark/regular/ordered/10k/"${i}".benchmark
done

# BF_EDS benchmarks
# 1k files
# shellcheck disable=SC2012
bench_count=$(ls -1 benchmark/bf_eds/ordered/1k/*.benchmark | wc -l)
for i in $(seq 1 "${bench_count}"); do
  ${BASE_CMD} benchmark/bf_eds/ordered/1k/"${i}".benchmark
done

# 10k files
# shellcheck disable=SC2012
bench_count=$(ls -1 benchmark/bf_eds/ordered/10k/*.benchmark | wc -l)
for i in $(seq 1 "${bench_count}"); do
  ${BASE_CMD} benchmark/bf_eds/ordered/10k/"${i}".benchmark
done
