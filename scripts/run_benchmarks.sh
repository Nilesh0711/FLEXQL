#!/bin/bash
# Script to run all benchmarks and store results in benchmark.out

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT" || exit 1

# Allow passing the number of rows as an argument to the script
# If no argument is provided, default to what is configured in the cpp files (no arg passed)
ROW_ARGS=""
if [ "$#" -ge 1 ]; then
    ROW_ARGS="$1"
    echo "Running benchmarks with explicit row count: $ROW_ARGS"
else
    echo "Running benchmarks with default row counts..."
fi

echo "Running benchmarks..." > benchmark.out

echo "======================================" >> benchmark.out
echo "benchmark_flexql" >> benchmark.out
echo "======================================" >> benchmark.out
./bin/benchmark_flexql $ROW_ARGS >> benchmark.out 2>&1

echo "" >> benchmark.out
echo "======================================" >> benchmark.out
echo "benchmark_after_insert" >> benchmark.out
echo "======================================" >> benchmark.out
./bin/benchmark_after_insert $ROW_ARGS >> benchmark.out 2>&1

echo "Benchmarks completed. Results stored in benchmark.out"
