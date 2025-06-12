table_name=$1

BASE_CMD="build/release/benchmark/benchmark_runner --root-dir ."

bench_out_dir=benchmark_out/"$table_name"
if [ ! -d "$bench_out_dir" ]; then
  # Create directory
  mkdir "$bench_out_dir"
fi

if [ -f "$bench_out_dir"/0.out ]; then
  echo "Found existing benchmark results, remove and re-run benchmark? (y/n)"
  read -n1 re_run_benchmarks
  if [ "$re_run_benchmarks" == 'n' ]; then
    exit 0
  fi
fi

bench_count=$(ls -1 benchmark/"$table_name"/*.benchmark | wc -l)
for c in $(seq 1 "${bench_count}"); do
  ${BASE_CMD} --out="$bench_out_dir"/"$c".out benchmark/"$table_name"/"$c".benchmark
done