'''
Script to generate benchmarks.

Example usage: python generate_benchmarks.py --template_path 'benchmark/bf_eds/load_iceberg_table.benchmark.in' --out_dir 'bf_eds/ordered/1k' --file_count 1k --table_name ordered_with_bf --seed 1 --benchmark_count 10
'''
import argparse
import random

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

for i in range(1, args.benchmark_count + 1):
    print(f'Generating benchmark {i}...')
    with open(args.out_dir + f'/{i}.benchmark', 'w') as f:
        range_min = random.randint(0, args.range_max)
        range_max = random.randint(range_min, args.range_max)
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