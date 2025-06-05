import argparse
import json
import math
import os.path
import random

s = "/Users/thijsheijden/Developer/university/Thesis/code/BF-EDS-NC/test_data/results/accuracy/matched_files_%llu_bf_%d.json"

parser = argparse.ArgumentParser()
parser.add_argument('-c', default=100, type=int) # The number of testcases to generate
parser.add_argument('--test_out_dir', required=True, type=str)
parser.add_argument('--table_name', required=True, type=str)
parser.add_argument('--seed', required=True, type=int)
parser.add_argument('--testcase_file_output', required=True)
args = parser.parse_args()

random.seed(args.seed)

def add_header(out_f, test_idx):
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
create view my_datalake.default.filtering_using_query_token as select * from ICEBERG_SCAN('/Users/thijsheijden/Developer/university/Thesis/code/BF-EDS-NC/test_data/iceberg/data/default/{args.table_name}');

statement ok
pragma enable_logging=true;

statement ok
set enable_logging=false;
set logging_storage='stdout';
set logging_storage='memory';
set enable_logging=true;

statement ok
SET use_encrypted_bloom_filters=true;
SET write_to_file=true;
SET file_name="{args.testcase_file_output}";

statement ok
CREATE SECRET bf_eds_nc_keys (
    type BF_EDS,
    k1 '4C304578746D6A506C70736D6D554442706174465944664E6942653936685843674E35374A39666B3438776356703533687A5A6F6F456736317030516A4D485A',
    k2 '45656A343332674C466F3879303958315342635841347A6757647869507645384E5A656C58375752463861366C596A6673715A55486950695759417855644856'
);\n
'''
    )


# Calculate log_2 of range max
range_max_lg2 = math.log2(9223372036854775807)

# Determine how many ranges to generate per power of 2
ranges_per_2_power = int(math.ceil(args.c / range_max_lg2))
cur_pow_2 = 1

# Generate a number of testcases with ranges based on some seed
testcase_ranges = []
for testcase_idx in range(args.c):
    # Create output file
    with open(os.path.join(args.test_out_dir, f"test_{testcase_idx}.test"), 'w') as out_f:
        add_header(out_f, testcase_idx)

        cur_max = (pow(2, cur_pow_2)) - 1
        print(f'Generating test {testcase_idx}...')
        range_min = random.randint(0, cur_max)
        range_max = random.randint(range_min, cur_max)

        testcase_ranges.append({'min': range_min, 'max': range_max})

        # Generate random range
        out_f.write(
            f'''query I
select count(*) from my_datalake.default.filtering_using_query_token where value between {range_min} and {range_max};
----
0
            '''

        )

        if testcase_idx % ranges_per_2_power == 0:
            cur_pow_2 += 1

# Write all the test case ranges to a separate file which contains a dictionary of testcase -> testcase range
with open(os.path.join(args.test_out_dir, "test_ranges.json"), 'w') as test_ranges_f:
    json.dump(testcase_ranges, test_ranges_f)

