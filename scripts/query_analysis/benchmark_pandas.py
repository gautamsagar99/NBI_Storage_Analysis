"""
Pandas Query Benchmark - Using only STATE_CODE_001 (the only truly safe column)
Safe columns: STATE_CODE_001 (Int64 - consistent across all files)
"""

import pandas as pd
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
    return files


def time_query(query_func, num_runs=NUM_RUNS):
    """Run query multiple times and return statistics"""
    times = []
    for i in range(num_runs):
        try:
            start = time.perf_counter()
            query_func()
            end = time.perf_counter()
            times.append(end - start)
        except MemoryError:
            if i == 0:
                return {
                    'best': None,
                    'avg': None,
                    'median': None,
                    'error': 'Out of memory'
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


def run_pandas_benchmarks():
    """Run all benchmarks with Pandas"""
    print("=" * 80)
    print("PANDAS BENCHMARK")
    print("=" * 80)
    print()

    files = get_parquet_files()

    # Load only STATE_CODE_001 column to save memory
    print("Loading STATE_CODE_001 column from all parquet files...")
    try:
        dfs = []
        for f in files:
            df = pd.read_parquet(f, columns=['STATE_CODE_001'])
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(df):,} rows")
        print()
    except MemoryError:
        print("ERROR: Out of memory during data loading")
        return {}

    results = {}

    # Query 1: Count by STATE
    def q1():
        df.groupby('STATE_CODE_001').size().sort_values(ascending=False)

    stats = time_query(q1)
    results['Q1: Count by State'] = stats
    if stats['avg']:
        print(f"Q1: Count by State")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q1: Count by State - ERROR: {stats.get('error', 'Unknown')}")

    # Query 2: Filter and Count
    def q2():
        filtered = df[df['STATE_CODE_001'] > 0]
        filtered.groupby('STATE_CODE_001').size().sort_values(ascending=False)

    stats = time_query(q2)
    results['Q2: Filter & Count'] = stats
    if stats['avg']:
        print(f"Q2: Filter & Count (STATE > 0)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q2: Filter & Count - ERROR: {stats.get('error', 'Unknown')}")

    # Query 3: Aggregation with filter
    def q3():
        grouped = df.groupby('STATE_CODE_001').size().reset_index(name='count')
        grouped[grouped['count'] > 10000].sort_values('count', ascending=False)

    stats = time_query(q3)
    results['Q3: Aggregate HAVING'] = stats
    if stats['avg']:
        print(f"Q3: Aggregate with filter (count > 10000)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q3: Aggregate HAVING - ERROR: {stats.get('error', 'Unknown')}")

    # Summary
    print()
    print("=" * 80)
    print("PANDAS SUMMARY")
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
    run_pandas_benchmarks()
