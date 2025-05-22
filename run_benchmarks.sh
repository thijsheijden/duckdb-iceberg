BASE_CMD="build/release/benchmark/benchmark_runner --root-dir ."
FILE_COUNTS=(1k 10k 100k)

# Regular Iceberg benchmarks
for i in "${FILE_COUNTS[@]}"; do
  # shellcheck disable=SC2012
  bench_count=$(ls -1 benchmark/regular/ordered/"${i}"/*.benchmark | wc -l)
  for c in $(seq 1 "${bench_count}"); do
    ${BASE_CMD} benchmark/regular/ordered/"${i}"/"${c}".benchmark
  done
done

# BF_EDS benchmarks
# shellcheck disable=SC2012
for i in "${FILE_COUNTS[@]}"; do
  bench_count=$(ls -1 benchmark/bf_eds/ordered/"${i}"/*.benchmark | wc -l)
  for c in $(seq 1 "${bench_count}"); do
    ${BASE_CMD} benchmark/bf_eds/ordered/"${i}"/"${c}".benchmark
  done
done