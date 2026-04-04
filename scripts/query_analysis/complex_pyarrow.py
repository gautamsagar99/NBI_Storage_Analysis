"""
Complex PyArrow Benchmark - Real-world NBI analysis with multiple columns
Tests: PyArrow Dataset API (lazy loading) with filtered aggregation
Uses: 12 safe columns for infrastructure health analysis
Optimized with pa.dataset.dataset() for lazy evaluation and predicate pushdown (similar to Polars/DuckDB)
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds
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
TARGET_SCHEMA = pa.schema([
    ('STATE_CODE_001', pa.int64()),
    ('DECK_COND_058', pa.string()),
    ('SUPERSTRUCTURE_COND_059', pa.string()),
    ('SUBSTRUCTURE_COND_060', pa.string()),
    ('YEAR_BUILT_027', pa.float64()),
    ('STRUCTURE_LEN_MT_049', pa.float64()),
    ('ADT_029', pa.float64()),
    ('DECK_WIDTH_MT_052', pa.float64()),
    ('INVENTORY_RATING_066', pa.float64()),
    ('OPERATING_RATING_064', pa.float64()),
    ('OWNER_022', pa.float64()),
    ('HIGHWAY_SYSTEM_104', pa.float64()),
])


def get_parquet_files():
    """Get all parquet files"""
    files = sorted(list(PARQUET_ROOT.rglob("*.parquet")))
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


def run_complex_pyarrow_benchmarks():
    """Run complex benchmarks with PyArrow using Dataset API (lazy mode)"""
    print("=" * 80)
    print("COMPLEX PYARROW BENCHMARK (DATASET API - LAZY MODE)")
    print(f"Columns: {len(TEST_COLUMNS)} | Files: ALL 1,728 | Using pa.dataset.dataset()")
    print("=" * 80)
    print()

    files = get_parquet_files()
    results = {}

    # Query 1: Infrastructure Health by State and Condition
    def q1():
        try:
            # Use Dataset API for lazy loading with predicate pushdown
            dataset = ds.dataset(files, format='parquet')

            # Apply filter at dataset level (lazy - pushed to file reader)
            filtered = dataset.to_table(
                columns=TEST_COLUMNS,
                filter=pc.is_in(ds.field('DECK_COND_058'), pa.array(['6', '7', '8', '9']))
            )

            # Cast schema to handle mixed types
            filtered = filtered.cast(TARGET_SCHEMA)

            # Group by state and aggregate
            grouped = filtered.group_by('STATE_CODE_001').aggregate([
                ('STRUCTURE_LEN_MT_049', 'mean'),
                ('STRUCTURE_LEN_MT_049', 'sum'),
                ('ADT_029', 'mean'),
                ('INVENTORY_RATING_066', 'mean'),
            ])

            return grouped
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
            # Dataset API with filter pushdown
            dataset = ds.dataset(files, format='parquet')

            # Filter applied at dataset level (lazy)
            filtered = dataset.to_table(
                columns=TEST_COLUMNS,
                filter=pc.less(ds.field('YEAR_BUILT_027'), 1950)
            )

            # Cast schema
            filtered = filtered.cast(TARGET_SCHEMA)

            # Group by state and condition
            grouped = filtered.group_by(['STATE_CODE_001', 'DECK_COND_058']).aggregate([
                ('ADT_029', 'mean'),
                ('YEAR_BUILT_027', 'mean'),
                ('OPERATING_RATING_064', 'mean'),
                ('OPERATING_RATING_064', 'min'),
                ('OPERATING_RATING_064', 'max'),
            ])

            return grouped
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
            # Dataset API with complex filters
            dataset = ds.dataset(files, format='parquet')

            # Apply multiple filters (using & operator instead of pc.and_)
            filter_expr = (
                pc.is_valid(ds.field('DECK_COND_058')) &
                pc.is_valid(ds.field('SUPERSTRUCTURE_COND_059')) &
                pc.is_valid(ds.field('INVENTORY_RATING_066'))
            )

            filtered = dataset.to_table(
                columns=TEST_COLUMNS,
                filter=filter_expr
            )

            # Cast schema
            filtered = filtered.cast(TARGET_SCHEMA)

            # Group by deck and superstructure conditions
            grouped = filtered.group_by(['DECK_COND_058', 'SUPERSTRUCTURE_COND_059']).aggregate([
                ('INVENTORY_RATING_066', 'mean'),
                ('INVENTORY_RATING_066', 'min'),
                ('INVENTORY_RATING_066', 'max'),
                ('OPERATING_RATING_064', 'mean'),
                ('OPERATING_RATING_064', 'min'),
                ('OPERATING_RATING_064', 'max'),
                ('ADT_029', 'mean'),
            ])

            return grouped
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
    print("COMPLEX PYARROW (DATASET API - LAZY MODE) SUMMARY")
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
    run_complex_pyarrow_benchmarks()
