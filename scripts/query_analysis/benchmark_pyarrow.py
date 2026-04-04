"""
PyArrow Query Benchmark - Using only STATE_CODE_001 (the only truly safe column)
Safe columns: STATE_CODE_001 (Int64 - consistent across all files)
Fair test: Load ONLY the column needed, like Pandas/Polars/DuckDB
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
import time
import statistics

# Work from project root regardless of execution directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_ROOT = PROJECT_ROOT / "data/storage/parquet_snappy"
NUM_RUNS = 3


def get_parquet_files():
    """Get all parquet files"""
    files = list(PARQUET_ROOT.rglob("*.parquet"))
    print(f"Found {len(files)} parquet files")
    if not files:
        print("ERROR: No parquet files found!")
        exit(1)
    return [str(f) for f in files]


def time_query(query_func, num_runs=NUM_RUNS):
    """Run query multiple times and return statistics"""
    times = []
    for i in range(num_runs):
        try:
            start = time.perf_counter()
            query_func()
            end = time.perf_counter()
            times.append(end - start)
        except Exception as e:
            if i == 0:
                return {
                    'best': None,
                    'avg': None,
                    'median': None,
                    'error': str(e)[:50]
                }
            break

    if not times:
        return {'best': None, 'avg': None, 'median': None, 'error': 'No runs'}

    return {
        'best': min(times),
        'avg': statistics.mean(times),
        'median': statistics.median(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0
    }


def run_pyarrow_benchmarks():
    """Run all benchmarks with PyArrow"""
    print("=" * 80)
    print("PYARROW BENCHMARK")
    print("=" * 80)
    print()

    files = get_parquet_files()
    results = {}

    # Query 1: Count by STATE
    def q1():
        # Read and combine all files
        tables = [pq.read_table(f, columns=['STATE_CODE_001']) for f in files]
        combined = pa.concat_tables(tables)

        # Get unique states and counts
        states = pc.unique(combined['STATE_CODE_001'])
        for state in states.to_pylist():
            if state is not None:
                pc.sum(pc.equal(combined['STATE_CODE_001'], state))

    stats = time_query(q1)
    results['Q1: Count by State'] = stats
    if stats['avg']:
        print(f"Q1: Count by State")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q1: Count by State - ERROR: {stats.get('error', 'Unknown')}")

    # Query 2: Filter and Count
    def q2():
        tables = [pq.read_table(f, columns=['STATE_CODE_001']) for f in files]
        combined = pa.concat_tables(tables)

        # Filter STATE > 0
        filtered = combined.filter(pc.greater(combined['STATE_CODE_001'], 0))
        states = pc.unique(filtered['STATE_CODE_001'])
        for state in states.to_pylist():
            if state is not None:
                pc.sum(pc.equal(filtered['STATE_CODE_001'], state))

    stats = time_query(q2)
    results['Q2: Filter & Count'] = stats
    if stats['avg']:
        print(f"Q2: Filter & Count (STATE > 0)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q2: Filter & Count - ERROR: {stats.get('error', 'Unknown')}")

    # Query 3: Aggregation with threshold
    def q3():
        tables = [pq.read_table(f, columns=['STATE_CODE_001']) for f in files]
        combined = pa.concat_tables(tables)

        # Count per state with threshold > 10000
        states = pc.unique(combined['STATE_CODE_001'])
        for state in states.to_pylist():
            if state is not None:
                count = pc.sum(pc.equal(combined['STATE_CODE_001'], state))
                if count.as_py() > 10000:
                    count.as_py()

    stats = time_query(q3)
    results['Q3: Aggregate HAVING'] = stats
    if stats['avg']:
        print(f"Q3: Aggregate with HAVING (count > 10000)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q3: Aggregate HAVING - ERROR: {stats.get('error', 'Unknown')}")

    # Summary
    print()
    print("=" * 80)
    print("PYARROW SUMMARY")
    print("=" * 80)
    valid_results = {k: v for k, v in results.items() if v['avg']}
    if valid_results:
        print(f"Total queries completed: {len(valid_results)}")
        print(f"Overall avg time: {statistics.mean([r['avg'] for r in valid_results.values()]):.3f}s")
        print()
        print("Ranking (by average time):")
        for rank, (query, stats) in enumerate(sorted(valid_results.items(), key=lambda x: x[1]['avg']), 1):
            print(f"  {rank}. {query:25s} - {stats['avg']:.3f}s avg")
    else:
        print("No queries completed")

    return results


if __name__ == "__main__":
    run_pyarrow_benchmarks()
