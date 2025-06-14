import argparse
import json
import math
import os.path
import random
import shutil
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('-c', default=100, type=int) # The number of testcases to generate
parser.add_argument('-m', default=8192, type=int) # Bloom filter bitset size
parser.add_argument('--table_name', required=True)
parser.add_argument('--file_count', required=True, type=str)
parser.add_argument('--seed', required=True, type=int)
parser.add_argument('--no_bf', action=argparse.BooleanOptionalAction)
args = parser.parse_args()

# This function adds the encrypted BF config to the test file
def add_bf_eds_config(out_f, encryption_method):
    out_f.write(
        f'''statement ok
SET use_encrypted_bloom_filters=true;
SET bloom_filter_m={args.m};
SET bloom_filter_encryption_method={encryption_method};

statement ok
CREATE SECRET bf_eds_nc_keys (
    type BF_EDS,
    k1 '4C304578746D6A506C70736D6D554442706174465944664E6942653936685843674E35374A39666B3438776356703533687A5A6F6F456736317030516A4D485A',
    k2 '45656A343332674C466F3879303958315342635841347A6757647869507645384E5A656C58375752463861366C596A6673715A55486950695759417855644856',
    bf_aes_k 'e44ce562fdef0ac2a31d082582c4c45fe93197b756fe3671722d51081ca3c51e'
);

'''
    )

# Adds the generic test header
def add_header(out_f, test_idx, test_output_dir, table_name):
    out_f.write(
        f'''# name: test/sql/BF_EDS/1M/test_{test_idx}.test
# description: Test data skipping accuracy using encrypted bloom filters
# group: [BF_EDS]

require avro

require parquet

require iceberg

statement ok
attach ':memory:' as my_datalake;

statement ok
create schema my_datalake.default;

statement ok
create view my_datalake.default.filtering_using_query_token as select * from ICEBERG_SCAN('/Users/thijsheijden/Developer/university/Thesis/code/BF-EDS-NC/test_data/iceberg/data/default/{table_name}');

statement ok
pragma enable_logging=true;

statement ok
set enable_logging=false;
set logging_storage='stdout';
set logging_storage='memory';
set enable_logging=true;

statement ok
SET write_to_file=true;
SET file_name="{test_output_dir}/res_{test_idx}.json";

'''
    )

# Grab the current directory to allow relative directories to work
cur_dir_name = os.path.dirname(__file__)

# The actual test files will be written to the directories 'file_count/none', 'file_count/aes', etc
root_test_file_dir = str(args.file_count)

# Remove existing tests
if os.path.exists(root_test_file_dir):
    if len(os.listdir(root_test_file_dir)) > 0:
        ans = input("Previously generated tests found, remove tests and generate new tests? (y/n)")[0]
        if ans == 'y':
            shutil.rmtree(root_test_file_dir)
            os.mkdir(root_test_file_dir)
        else:
            exit(0)
else:
    os.mkdir(root_test_file_dir)

# Create root output directory for the test files
root_test_output_dir = f"/Users/thijsheijden/Developer/university/Thesis/code/BF-EDS-NC/test_data/results/accuracy/{args.file_count}"
if not os.path.exists(root_test_output_dir):
    os.mkdir(root_test_output_dir)

# Generates the specified number of ranges within each power of 2 bucket
def generate_uniform_log_ranges(ranges_per_bucket, max_log):
    random.seed(args.seed)
    ranges = []
    max_range_val = (1 << 63) - 1
    for log_size in range(0, max_log + 1):
        size = (1 << log_size) - 1
        for _ in range(ranges_per_bucket):
            start = random.randint(0, max_range_val - size)
            end = min(start + size, 9223372036854775807)
            ranges.append({'min': start, 'max': end})
            testcase_range_deltas.append(end - start)
    return ranges

if args.no_bf:
    testcase_range_deltas = [] # The delta of each testcase range
    testcases_per_log2 = int(args.c / 64)
    ranges = generate_uniform_log_ranges(testcases_per_log2, 63)

    test_output_dir = os.path.join(root_test_output_dir, "no_bf")
    if not os.path.exists(test_output_dir):
        os.mkdir(test_output_dir)

    test_file_dir = os.path.join(cur_dir_name, root_test_file_dir, "no_bf")
    if not os.path.exists(test_file_dir):
        os.mkdir(test_file_dir)

    table_name = args.table_name
    testcase_idx = 0
    for r in ranges:
        with open(os.path.join(test_file_dir, f'test_{testcase_idx}.test'), 'w') as out_f:
            add_header(out_f, testcase_idx, test_output_dir, table_name)

            out_f.write(
                f'''query I
select count(*) from my_datalake.default.filtering_using_query_token where value between {r['min']} and {r['max']};
----
0
                    '''
            )

            testcase_idx += 1

    # Write all the test case ranges to a separate file which contains a dictionary of testcase -> testcase range
    with open(os.path.join(test_file_dir, 'test_ranges.json'), 'w') as test_ranges_f:
        json.dump(ranges, test_ranges_f)

    print(f'Generated {testcases_per_log2} testcases for regular Iceberg table per log_2 for {len(ranges)} total testcases')
    exit(0)

# For each encryption method, generate tests
bf_encryption_methods = ["none", "aes", "xor"]
for encryption_method in bf_encryption_methods:
    testcase_range_deltas = [] # The delta of each testcase range
    testcases_per_log2 = int(args.c / 64)
    ranges = generate_uniform_log_ranges(testcases_per_log2, 63)

    test_output_dir = os.path.join(root_test_output_dir, encryption_method)
    if not os.path.exists(test_output_dir):
        os.mkdir(test_output_dir)

    test_file_dir = os.path.join(cur_dir_name, root_test_file_dir, encryption_method)
    if not os.path.exists(test_file_dir):
        os.mkdir(test_file_dir)

    table_name = args.table_name + f'_{encryption_method}'
    testcase_idx = 0
    for r in ranges:
        with open(os.path.join(test_file_dir, f'test_{testcase_idx}.test'), 'w') as out_f:
            add_header(out_f, testcase_idx, test_output_dir, table_name)

            if not args.no_bf:
                add_bf_eds_config(out_f, encryption_method)

            out_f.write(
                f'''query I
select count(*) from my_datalake.default.filtering_using_query_token where value between {r['min']} and {r['max']};
----
0
                    '''
            )

            testcase_idx += 1

    # Write all the test case ranges to a separate file which contains a dictionary of testcase -> testcase range
    with open(os.path.join(test_file_dir, 'test_ranges.json'), 'w') as test_ranges_f:
        json.dump(ranges, test_ranges_f)

    print(f'Generated {testcases_per_log2} testcases for encryption method {encryption_method} per log_2 for {len(ranges)} total testcases')

# Plot distribution of range deltas
# Convert to numpy array
# deltas = np.array(testcase_range_deltas, dtype=np.float64)
# log_bins = np.floor(np.log2(deltas + 1)).astype(int) # Compute log2 of each delta and floor to get bin index
# bins = np.arange(0, 65)  # Bins from log2(1) to log2(2^64)
#
# # Plot histogram
# plt.style.use('physrev.mplstyle') # Add Gnuplot styling
# plt.rcParams['figure.dpi'] = "300"
# plt.figure(figsize=(20, 6))
# plt.hist(log_bins, bins=bins, align='left', rwidth=2)
# plt.xticks(bins)
# plt.title(f"Query Range Size Histogram ($log_2(size)$) For {args.c} Randomly Generated Ranges", size=16, pad=16)
# plt.xlabel("Range Size", size=12)
# plt.ylabel("Frequency", size=12)
# plt.savefig(fname=f'query_range_test_deltas_{args.table_name}.png')



