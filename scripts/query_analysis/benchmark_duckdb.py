"""
DuckDB Query Benchmark - Using only STATE_CODE_001 (the only truly safe column)
Safe columns: STATE_CODE_001 (Int64 - consistent across all files)
"""

import duckdb
from pathlib import Path
import time
import statistics

# Work from project root regardless of execution directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_ROOT = PROJECT_ROOT / "data/storage/parquet_snappy"
NUM_RUNS = 3


def get_parquet_pattern():
    """Get glob pattern for all parquet files"""
    files = list(PARQUET_ROOT.rglob("*.parquet"))
    print(f"Found {len(files)} parquet files")
    if not files:
        print("ERROR: No parquet files found!")
        exit(1)
    return str(PARQUET_ROOT / "**/*.parquet")


def time_query(query_func, num_runs=NUM_RUNS):
    """Run query multiple times and return statistics"""
    times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        query_func()
        end = time.perf_counter()
        times.append(end - start)

    return {
        'best': min(times),
        'avg': statistics.mean(times),
        'median': statistics.median(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0
    }


def run_duckdb_benchmarks():
    """Run all benchmarks with DuckDB"""
    print("=" * 80)
    print("DUCKDB BENCHMARK")
    print("=" * 80)
    print()

    parquet_pattern = get_parquet_pattern()
    results = {}

    # Query 1: Count by STATE
    def q1():
        con = duckdb.connect()
        # DuckDB automatically prunes columns - select only what we need
        con.execute(f"""
            SELECT STATE_CODE_001, COUNT(*) as count
            FROM read_parquet('{parquet_pattern}')
            GROUP BY STATE_CODE_001
            ORDER BY count DESC
        """).fetchall()
        con.close()

    stats = time_query(q1)
    results['Q1: Count by State'] = stats
    print(f"Q1: Count by State")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")

    # Query 2: Filter and Count
    def q2():
        con = duckdb.connect()
        con.execute(f"""
            SELECT STATE_CODE_001, COUNT(*) as count
            FROM read_parquet('{parquet_pattern}')
            WHERE STATE_CODE_001 > 0
            GROUP BY STATE_CODE_001
            ORDER BY count DESC
        """).fetchall()
        con.close()

    stats = time_query(q2)
    results['Q2: Filter & Count'] = stats
    print(f"Q2: Filter & Count (STATE > 0)")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")

    # Query 3: Aggregation with HAVING
    def q3():
        con = duckdb.connect()
        con.execute(f"""
            SELECT STATE_CODE_001, COUNT(*) as count
            FROM read_parquet('{parquet_pattern}')
            GROUP BY STATE_CODE_001
            HAVING COUNT(*) > 10000
            ORDER BY count DESC
        """).fetchall()
        con.close()

    stats = time_query(q3)
    results['Q3: Aggregate HAVING'] = stats
    print(f"Q3: Aggregate with HAVING (count > 10000)")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s | Median: {stats['median']:.3f}s")

    # Summary
    print()
    print("=" * 80)
    print("DUCKDB SUMMARY")
    print("=" * 80)
    print(f"Total queries: {len(results)}")
    print(f"Overall avg time: {statistics.mean([r['avg'] for r in results.values()]):.3f}s")
    print()
    print("Ranking (by average time):")
    for rank, (query, stats) in enumerate(sorted(results.items(), key=lambda x: x[1]['avg']), 1):
        print(f"  {rank}. {query:25s} - {stats['avg']:.3f}s avg")

    return results


if __name__ == "__main__":
    run_duckdb_benchmarks()
