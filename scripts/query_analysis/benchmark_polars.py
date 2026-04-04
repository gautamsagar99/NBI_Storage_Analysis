"""
Polars Query Benchmark - Using lazy evaluation efficiently
Safe columns: STATE_CODE_001 (Int64 - consistent across all files)
"""

import polars as pl
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


def run_polars_benchmarks():
    """Run all benchmarks with Polars"""
    print("=" * 80)
    print("POLARS BENCHMARK")
    print("=" * 80)
    print()

    files = get_parquet_files()
    results = {}

    # Note: Load ONLY STATE_CODE_001 column for fair comparison with Pandas
    # This shows true performance when accessing same data

    # Query 1: Count by STATE
    def q1():
        # Load only the STATE_CODE_001 column from all files
        df = pl.concat(
            [pl.read_parquet(f, columns=['STATE_CODE_001']) for f in files],
            how='vertical'
        )
        return df.group_by('STATE_CODE_001') \
            .agg(pl.len().alias('count')) \
            .sort('count', descending=True)

    stats = time_query(q1)
    results['Q1: Count by State'] = stats
    if stats['avg']:
        print(f"Q1: Count by State")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q1: Count by State - ERROR: {stats.get('error', 'Unknown')}")

    # Query 2: Filter and Count
    def q2():
        df = pl.concat(
            [pl.read_parquet(f, columns=['STATE_CODE_001']) for f in files],
            how='vertical'
        )
        return df.filter(pl.col('STATE_CODE_001') > 0) \
            .group_by('STATE_CODE_001') \
            .agg(pl.len().alias('count')) \
            .sort('count', descending=True)

    stats = time_query(q2)
    results['Q2: Filter & Count'] = stats
    if stats['avg']:
        print(f"Q2: Filter & Count (STATE > 0)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")
    else:
        print(f"Q2: Filter & Count - ERROR: {stats.get('error', 'Unknown')}")

    # Query 3: Aggregation with filter
    def q3():
        df = pl.concat(
            [pl.read_parquet(f, columns=['STATE_CODE_001']) for f in files],
            how='vertical'
        )
        return df.group_by('STATE_CODE_001') \
            .agg(pl.len().alias('count')) \
            .filter(pl.col('count') > 10000) \
            .sort('count', descending=True)

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
    print("POLARS SUMMARY")
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
    run_polars_benchmarks()
