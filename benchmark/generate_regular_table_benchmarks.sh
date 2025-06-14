rm "$1"/*
python generate_benchmarks.py --template_path /Users/thijsheijden/Developer/university/Thesis/code/duckdb-iceberg/benchmark/load_iceberg_table.benchmark.in -t "$1" -s 8987329487 -c "$2"
