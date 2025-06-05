"""
Script to generate benchmarks.

Example usage:
python generate_benchmarks.py --template_path 'benchmark/bf_eds/load_iceberg_table.benchmark.in' --out_dir 'bf_eds/ordered/1k' --file_count 1k --table_name ordered_with_bf --seed 1 --benchmark_count 10
python generate_benchmarks.py --template_path 'benchmark/bf_eds/load_iceberg_table.benchmark.in' --out_dir 'bf_eds/ordered/100k' --file_count 100k --table_name ordered_100000_with_bf --seed 124812745 --benchmark_count 1000
"""

import argparse
import random
import math

# Use some set seed to generate N benchmark files
parser = argparse.ArgumentParser()
parser.add_argument('--template_path', type=str)
parser.add_argument('--out_dir', type=str)
parser.add_argument('--file_count', type=str)
parser.add_argument('--table_name', type=str)
parser.add_argument('--seed', type=int, default=123)
parser.add_argument('--benchmark_count', type=int, default=10)
parser.add_argument('--range_max', type=int, default=9223372036854775806)
args = parser.parse_args()

random.seed(args.seed)

# Calculate log_2 of range max
range_max_lg2 = math.log2(args.range_max)

# Determine how many ranges to generate per power of 2
ranges_per_2_power = int(math.ceil(args.benchmark_count / range_max_lg2))
cur_pow_2 = 1

for i in range(1, args.benchmark_count + 1):
    cur_max = (pow(2, cur_pow_2)) - 1
    print(f'Generating benchmark {i}...')
    print(f'cur_max: {cur_max}, below int64 max: {cur_max <= 9223372036854775807}')
    with open(args.out_dir + f'/{i}.benchmark', 'w') as f:
        range_min = random.randint(0, cur_max)
        range_max = random.randint(range_min, cur_max)
        f.write(f'# name: benchmark/iceberg/bf_eds/ordered/query_1k.benchmark\n\
# description: Querying {args.file_count} files using Iceberg\n\
# group: [iceberg]\n\
\n\
template {args.template_path}\n\
FILE_COUNT={args.file_count}\n\
TABLE_NAME={args.table_name}\n\
QUERY_NUMBER={i}\n\
QUERY_MIN={range_min}\n\
QUERY_MAX={range_max}')

        if i % ranges_per_2_power == 0:
            cur_pow_2 += 1