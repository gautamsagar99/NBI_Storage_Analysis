"""
Complex Polars Benchmark (LAZY MODE) - Real-world NBI analysis with multiple columns
Tests: Lazy loading with multi-column filtering, grouping, aggregation
Uses: 12 safe columns for infrastructure health analysis
Optimized with pl.scan_parquet() and lazy evaluation
"""

import polars as pl
from pathlib import Path
import time
import statistics

PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_ROOT = PROJECT_ROOT / "data/storage/parquet_snappy"
NUM_RUNS = 2

# Safe columns for complex analysis
TEST_COLUMNS = [
    'STATE_CODE_001',
    'DECK_COND_058',
    'SUPERSTRUCTURE_COND_059',
    'SUBSTRUCTURE_COND_060',
    'YEAR_BUILT_027',
    'STRUCTURE_LEN_MT_049',
    'ADT_029',
    'DECK_WIDTH_MT_052',
    'INVENTORY_RATING_066',
    'OPERATING_RATING_064',
    'OWNER_022',
    'HIGHWAY_SYSTEM_104'
]

# Define target schema to handle mixed types across files
schema = {
    'STATE_CODE_001': pl.Int64,
    'DECK_COND_058': pl.String,
    'SUPERSTRUCTURE_COND_059': pl.String,
    'SUBSTRUCTURE_COND_060': pl.String,
    'YEAR_BUILT_027': pl.Float64,
    'STRUCTURE_LEN_MT_049': pl.Float64,
    'ADT_029': pl.Float64,
    'DECK_WIDTH_MT_052': pl.Float64,
    'INVENTORY_RATING_066': pl.Float64,
    'OPERATING_RATING_064': pl.Float64,
    'OWNER_022': pl.Float64,
    'HIGHWAY_SYSTEM_104': pl.Float64
}


def get_parquet_files():
    """Get all parquet files"""
    files = list(PARQUET_ROOT.rglob("*.parquet"))
    print(f"Found {len(files)} parquet files")
    if not files:
        print("ERROR: No parquet files found!")
        exit(1)
    return sorted([str(f) for f in files])


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


def run_complex_polars_benchmarks():
    """Run complex benchmarks with Polars LAZY mode"""
    print("=" * 80)
    print("COMPLEX POLARS BENCHMARK (LAZY MODE) - OPTIMIZED")
    print(f"Columns: {len(TEST_COLUMNS)} | Files: ALL 1,728 | Using pl.scan_parquet()")
    print("=" * 80)
    print()

    files = get_parquet_files()
    results = {}

    # Query 1: Infrastructure Health by State and Condition
    def q1():
        try:
            # Use scan_parquet for lazy loading (no columns parameter at scan time)
            dfs = [pl.scan_parquet(f).select(TEST_COLUMNS).cast(schema) for f in files]
            # Build lazy query plan (not executed yet)
            df = pl.concat(dfs, how='vertical').lazy()
            result = df.filter(
                pl.col('DECK_COND_058').is_in(['6', '7', '8', '9'])
            ).group_by('STATE_CODE_001').agg([
                pl.len().alias('bridge_count'),
                pl.col('STRUCTURE_LEN_MT_049').mean().alias('avg_length'),
                pl.col('STRUCTURE_LEN_MT_049').sum().alias('total_length'),
                pl.col('ADT_029').mean().alias('avg_traffic'),
                pl.col('INVENTORY_RATING_066').mean().alias('avg_rating')
            ]).sort('avg_traffic', descending=True)
            # NOW execute the optimized plan
            return result.collect()
        except Exception as e:
            raise e

    stats = time_query(q1)
    results['Q1: Health by State'] = stats
    if stats['avg']:
        print(f"Q1: Infrastructure Health by State (good condition bridges)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")
    else:
        print(f"Q1: ERROR - {stats.get('error', 'Unknown')}")

    # Query 2: Aging Infrastructure Analysis
    def q2():
        try:
            dfs = [pl.scan_parquet(f).select(TEST_COLUMNS).cast(schema) for f in files]
            df = pl.concat(dfs, how='vertical').lazy()
            result = df.filter(
                pl.col('YEAR_BUILT_027') < 1950
            ).group_by(['STATE_CODE_001', 'DECK_COND_058']).agg([
                pl.len().alias('bridge_count'),
                pl.col('ADT_029').mean().alias('avg_traffic'),
                pl.col('YEAR_BUILT_027').mean().alias('avg_year'),
                pl.col('OPERATING_RATING_064').mean().alias('avg_operating_rating'),
                pl.col('OPERATING_RATING_064').min().alias('min_rating'),
                pl.col('OPERATING_RATING_064').max().alias('max_rating')
            ]).sort('bridge_count', descending=True).limit(100)
            return result.collect()
        except Exception as e:
            raise e

    stats = time_query(q2)
    results['Q2: Aging Infrastructure'] = stats
    if stats['avg']:
        print(f"Q2: Aging Infrastructure Analysis (built before 1950)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")
    else:
        print(f"Q2: ERROR - {stats.get('error', 'Unknown')}")

    # Query 3: Multi-condition Impact on Rating
    def q3():
        try:
            dfs = [pl.scan_parquet(f).select(TEST_COLUMNS).cast(schema) for f in files]
            df = pl.concat(dfs, how='vertical').lazy()
            result = df.filter(
                (pl.col('DECK_COND_058').is_not_null()) &
                (pl.col('SUPERSTRUCTURE_COND_059').is_not_null()) &
                (pl.col('INVENTORY_RATING_066').is_not_null())
            ).group_by(['DECK_COND_058', 'SUPERSTRUCTURE_COND_059']).agg([
                pl.len().alias('bridge_count'),
                pl.col('INVENTORY_RATING_066').mean().alias('avg_inv_rating'),
                pl.col('INVENTORY_RATING_066').min().alias('min_inv_rating'),
                pl.col('INVENTORY_RATING_066').max().alias('max_inv_rating'),
                pl.col('OPERATING_RATING_064').mean().alias('avg_op_rating'),
                pl.col('OPERATING_RATING_064').min().alias('min_op_rating'),
                pl.col('OPERATING_RATING_064').max().alias('max_op_rating'),
                pl.col('ADT_029').mean().alias('avg_traffic')
            ]).sort('bridge_count', descending=True).limit(50)
            return result.collect()
        except Exception as e:
            raise e

    stats = time_query(q3)
    results['Q3: Multi-condition Impact'] = stats
    if stats['avg']:
        print(f"Q3: Multi-condition Impact on Ratings")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")
    else:
        print(f"Q3: ERROR - {stats.get('error', 'Unknown')}")

    # Summary
    print()
    print("=" * 80)
    print("COMPLEX POLARS (LAZY MODE) SUMMARY")
    print("=" * 80)
    valid_results = {k: v for k, v in results.items() if v['avg']}
    if valid_results:
        print(f"Total queries completed: {len(valid_results)}")
        print(f"Overall avg time: {statistics.mean([r['avg'] for r in valid_results.values()]):.3f}s")
        print()
        print("Ranking (by average time):")
        for rank, (query, stats) in enumerate(sorted(valid_results.items(), key=lambda x: x[1]['avg']), 1):
            print(f"  {rank}. {query:30s} - {stats['avg']:.3f}s avg")
    else:
        print("No queries completed")

    return results


if __name__ == "__main__":
    run_complex_polars_benchmarks()
