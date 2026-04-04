"""
Complex Pandas Benchmark - Real-world NBI analysis with multiple columns
Tests: multi-column filtering, grouping, aggregation, sorting
Uses: 12 safe columns for infrastructure health analysis
"""

import pandas as pd
from pathlib import Path
import time
import statistics

PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_ROOT = PROJECT_ROOT / "data/storage/parquet_snappy"
NUM_RUNS = 2

# Safe columns for complex analysis
TEST_COLUMNS = [
    'STATE_CODE_001',           # State identifier
    'DECK_COND_058',            # Deck condition
    'SUPERSTRUCTURE_COND_059',  # Structure condition
    'SUBSTRUCTURE_COND_060',    # Substructure condition
    'YEAR_BUILT_027',           # Year built
    'STRUCTURE_LEN_MT_049',     # Length in meters
    'ADT_029',                  # Average daily traffic
    'DECK_WIDTH_MT_052',        # Deck width
    'INVENTORY_RATING_066',     # Inventory rating
    'OPERATING_RATING_064',     # Operating rating
    'OWNER_022',                # Owner
    'HIGHWAY_SYSTEM_104'        # Highway system
]


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


def run_complex_pandas_benchmarks():
    """Run complex benchmarks with Pandas"""
    print("=" * 80)
    print("COMPLEX PANDAS BENCHMARK")
    print(f"Columns: {len(TEST_COLUMNS)} | Files: ALL 1,728")
    print("=" * 80)
    print()

    files = get_parquet_files()

    # Load data from all files
    print("Loading 12 columns from all 1,728 parquet files...")
    try:
        dfs = []
        for i, f in enumerate(files):
            if (i + 1) % 500 == 0:
                print(f"  Processed {i + 1}/{len(files)} files...")
            df = pd.read_parquet(f, columns=TEST_COLUMNS)
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(df):,} rows total")
        print()
    except Exception as e:
        print(f"ERROR: {str(e)[:100]}")
        return {}

    results = {}

    # Query 1: Infrastructure Health by State and Condition
    def q1():
        # Analyze bridge health by state
        filtered = df[df['DECK_COND_058'].isin(['6', '7', '8', '9'])]  # Good condition
        result = filtered.groupby('STATE_CODE_001').agg({
            'STRUCTURE_LEN_MT_049': ['count', 'mean', 'sum'],
            'ADT_029': 'mean',
            'INVENTORY_RATING_066': 'mean'
        })
        result.columns = ['_'.join(col).strip() for col in result.columns.values]
        result = result.reset_index()
        return result.sort_values('ADT_029_mean', ascending=False)

    stats = time_query(q1)
    results['Q1: Health by State'] = stats
    if stats['avg']:
        print(f"Q1: Infrastructure Health by State (good condition bridges)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")
    else:
        print(f"Q1: ERROR - {stats.get('error', 'Unknown')}")

    # Query 2: Aging Infrastructure Analysis
    def q2():
        # Analyze old bridges (built before 1950) with current condition
        old_bridges = df[df['YEAR_BUILT_027'] < 1950]
        result = old_bridges.groupby(['STATE_CODE_001', 'DECK_COND_058']).agg({
            'STRUCTURE_LEN_MT_049': 'count',
            'ADT_029': 'mean',
            'YEAR_BUILT_027': 'mean',
            'OPERATING_RATING_064': 'mean'
        }).reset_index()
        return result.sort_values('STRUCTURE_LEN_MT_049', ascending=False)

    stats = time_query(q2)
    results['Q2: Aging Infrastructure'] = stats
    if stats['avg']:
        print(f"Q2: Aging Infrastructure Analysis (built before 1950)")
        print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")
    else:
        print(f"Q2: ERROR - {stats.get('error', 'Unknown')}")

    # Query 3: Multi-condition Impact on Rating
    def q3():
        # Analyze how multiple conditions affect ratings
        filtered = df.dropna(subset=['DECK_COND_058', 'SUPERSTRUCTURE_COND_059', 'INVENTORY_RATING_066'])
        result = filtered.groupby(['DECK_COND_058', 'SUPERSTRUCTURE_COND_059']).agg({
            'INVENTORY_RATING_066': ['mean', 'min', 'max'],
            'OPERATING_RATING_064': ['mean', 'min', 'max'],
            'STRUCTURE_LEN_MT_049': 'count',
            'ADT_029': 'mean'
        }).reset_index()
        return result.sort_values(('STRUCTURE_LEN_MT_049', 'count'), ascending=False).head(50)

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
    print("COMPLEX PANDAS SUMMARY")
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
    run_complex_pandas_benchmarks()
