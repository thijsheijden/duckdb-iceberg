"""
Script to generate benchmarks.

Example usage:
python generate_benchmarks.py --template_path 'benchmark/bf_eds/load_iceberg_table.benchmark.in' --out_dir 'bf_eds/ordered/1k' --file_count 1k --table_name ordered_with_bf --seed 1 --benchmark_count 10
python generate_benchmarks.py --template_path 'benchmark/bf_eds/load_iceberg_table.benchmark.in' --out_dir 'bf_eds/ordered/100k' --file_count 100k --table_name ordered_100000_with_bf --seed 124812745 --benchmark_count 1000
"""

import argparse
import os.path
import random
import math
import json

# Use some set seed to generate N benchmark files
parser = argparse.ArgumentParser()
parser.add_argument('--template_path', type=str, help='Absolute path to the Iceberg table loading template')
parser.add_argument('--bf_encryption_method', default='none')
parser.add_argument('-m', type=int, default=8192)
parser.add_argument('-t', type=str, help='Iceberg table name')
parser.add_argument('-s', type=int, default=123, help='Seed used to seed randomness')
parser.add_argument('-c', type=int, default=64, help='Number of benchmarks to generate')
args = parser.parse_args()

random.seed(args.s)

if not os.path.isdir(args.t):
    os.mkdir(args.t)

def generate_uniform_log_ranges(ranges_per_bucket, max_log):
    ranges = []
    max_range_val = (1 << 63) - 1
    for log_size in range(0, max_log + 1):
        size = (1 << log_size) - 1
        for _ in range(ranges_per_bucket):
            start = random.randint(0, max_range_val - size)
            end = min(start + size, 9223372036854775807)
            ranges.append({'min': start, 'max': end})
    return ranges

# Determine how many benchmarks to generate per bin
benchmarks_per_log2 = int(args.c / 64)
benchmark_ranges = generate_uniform_log_ranges(benchmarks_per_log2, 63)
benchmark_idx = 1
for r in benchmark_ranges:
    with open(os.path.join(args.t, f'{benchmark_idx}.benchmark'), 'w') as bench_out_f:
        bench_out_f.write(f'''# name: benchmark/{args.t}/{benchmark_idx}.benchmark
# group: [iceberg]

template {args.template_path}
BF_M={args.m}
BF_ENC_METHOD={args.bf_encryption_method}
TABLE_NAME={args.t}
QUERY_NUMBER={benchmark_idx}
QUERY_MIN={r['min']}
QUERY_MAX={r['max']}
''')

        benchmark_idx += 1

# Write all the benchmark ranges to a separate file which contains a dictionary of benchmark -> benchmarange
with open(os.path.join(args.t, "benchmark_ranges.json"), 'w') as benchmark_ranges_f:
    json.dump(benchmark_ranges, benchmark_ranges_f)