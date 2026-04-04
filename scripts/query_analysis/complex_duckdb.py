"""
Complex DuckDB Benchmark - Real-world NBI analysis with multiple columns
Tests: SQL queries with filtering, grouping, aggregation, multi-level analysis
Uses: 12 safe columns for infrastructure health analysis
"""

import duckdb
from pathlib import Path
import time
import statistics

PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_ROOT = PROJECT_ROOT / "data/storage/parquet_snappy"
NUM_RUNS = 2


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


def run_complex_duckdb_benchmarks():
    """Run complex benchmarks with DuckDB"""
    print("=" * 80)
    print("COMPLEX DUCKDB BENCHMARK")
    print("Columns: 12 | Files: ALL 1,728 | SQL-optimized queries")
    print("=" * 80)
    print()

    parquet_pattern = get_parquet_pattern()
    results = {}

    # Query 1: Infrastructure Health by State and Condition
    def q1():
        con = duckdb.connect()
        con.execute(f"""
            CREATE VIEW nbi_data AS
            SELECT * FROM read_parquet('{parquet_pattern}')
        """)
        con.execute("""
            SELECT
                STATE_CODE_001,
                COUNT(*) as bridge_count,
                AVG(STRUCTURE_LEN_MT_049) as avg_length,
                SUM(STRUCTURE_LEN_MT_049) as total_length,
                AVG(ADT_029) as avg_traffic,
                AVG(INVENTORY_RATING_066) as avg_rating
            FROM nbi_data
            WHERE DECK_COND_058 IN ('6', '7', '8', '9')
            GROUP BY STATE_CODE_001
            ORDER BY avg_traffic DESC
        """).fetchall()
        con.close()

    stats = time_query(q1)
    results['Q1: Health by State'] = stats
    print(f"Q1: Infrastructure Health by State (good condition bridges)")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")

    # Query 2: Aging Infrastructure Analysis
    def q2():
        con = duckdb.connect()
        con.execute(f"""
            CREATE VIEW nbi_data AS
            SELECT * FROM read_parquet('{parquet_pattern}')
        """)
        con.execute("""
            SELECT
                STATE_CODE_001,
                DECK_COND_058,
                COUNT(*) as bridge_count,
                AVG(ADT_029) as avg_traffic,
                AVG(YEAR_BUILT_027) as avg_year,
                AVG(OPERATING_RATING_064) as avg_operating_rating,
                MIN(OPERATING_RATING_064) as min_rating,
                MAX(OPERATING_RATING_064) as max_rating
            FROM nbi_data
            WHERE YEAR_BUILT_027 < 1950
            GROUP BY STATE_CODE_001, DECK_COND_058
            ORDER BY bridge_count DESC
            LIMIT 100
        """).fetchall()
        con.close()

    stats = time_query(q2)
    results['Q2: Aging Infrastructure'] = stats
    print(f"Q2: Aging Infrastructure Analysis (built before 1950)")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")

    # Query 3: Multi-condition Impact on Rating
    def q3():
        con = duckdb.connect()
        con.execute(f"""
            CREATE VIEW nbi_data AS
            SELECT * FROM read_parquet('{parquet_pattern}')
        """)
        con.execute("""
            SELECT
                DECK_COND_058,
                SUPERSTRUCTURE_COND_059,
                COUNT(*) as bridge_count,
                AVG(INVENTORY_RATING_066) as avg_inv_rating,
                MIN(INVENTORY_RATING_066) as min_inv_rating,
                MAX(INVENTORY_RATING_066) as max_inv_rating,
                AVG(OPERATING_RATING_064) as avg_op_rating,
                MIN(OPERATING_RATING_064) as min_op_rating,
                MAX(OPERATING_RATING_064) as max_op_rating,
                AVG(ADT_029) as avg_traffic
            FROM nbi_data
            WHERE DECK_COND_058 IS NOT NULL
                AND SUPERSTRUCTURE_COND_059 IS NOT NULL
                AND INVENTORY_RATING_066 IS NOT NULL
            GROUP BY DECK_COND_058, SUPERSTRUCTURE_COND_059
            ORDER BY bridge_count DESC
            LIMIT 50
        """).fetchall()
        con.close()

    stats = time_query(q3)
    results['Q3: Multi-condition Impact'] = stats
    print(f"Q3: Multi-condition Impact on Ratings")
    print(f"   Best: {stats['best']:.3f}s | Avg: {stats['avg']:.3f}s")

    # Summary
    print()
    print("=" * 80)
    print("COMPLEX DUCKDB SUMMARY")
    print("=" * 80)
    print(f"Total queries completed: {len(results)}")
    print(f"Overall avg time: {statistics.mean([r['avg'] for r in results.values()]):.3f}s")
    print()
    print("Ranking (by average time):")
    for rank, (query, stats) in enumerate(sorted(results.items(), key=lambda x: x[1]['avg']), 1):
        print(f"  {rank}. {query:30s} - {stats['avg']:.3f}s avg")

    return results


if __name__ == "__main__":
    run_complex_duckdb_benchmarks()
