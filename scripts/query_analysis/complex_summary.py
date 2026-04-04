"""
Complex Benchmark Summary - Compare all 4 approaches on complex queries
Tests real-world NBI queries with 12 columns and 1,728 files
ALL TOOLS OPTIMIZED: DuckDB (SQL), Pandas (eager), Polars (lazy eval), PyArrow (Dataset API lazy)
"""

import json
from pathlib import Path
from complex_pandas import run_complex_pandas_benchmarks
from complex_duckdb import run_complex_duckdb_benchmarks
from complex_polars import run_complex_polars_benchmarks
from complex_pyarrow import run_complex_pyarrow_benchmarks
import statistics


def save_complex_results(duckdb_results, polars_results, pandas_results, pyarrow_results):
    """Save complex benchmark results to JSON"""
    results_data = {
        'duckdb': duckdb_results,
        'polars': polars_results,
        'pandas': pandas_results,
        'pyarrow': pyarrow_results,
        'query_type': 'COMPLEX - 12 columns, multi-level aggregation, 1,728 files'
    }

    results_file = Path(__file__).parent / "complex_benchmark_results.json"
    with open(results_file, 'w') as f:
        json.dump(results_data, f, indent=2)

    print(f"\n[OK] Complex results saved to: {results_file}")
    return results_file


def run_all_complex_and_compare():
    """Run all complex benchmarks and create comparison"""
    print("\n" * 2)
    print("=" * 115)
    print("COMPLEX QUERY BENCHMARKING: All 4 Tools on Real-World NBI Analysis")
    print("=" * 115)
    print("\nQuery Type: Infrastructure Health Analysis")
    print("Columns: 12 (state, conditions, year, traffic, ratings, width, owner, highway)")
    print("Files: ALL 1,728 (complete dataset)")
    print("Query Complexity: HIGH (multi-level grouping, filtering, aggregation)")
    print("Optimizations:")
    print("  - DuckDB: SQL query planner + predicate pushdown")
    print("  - Polars: Lazy evaluation (pl.scan_parquet + .lazy())")
    print("  - PyArrow: Dataset API (lazy loading + filter pushdown)")
    print("  - Pandas: Simple eager in-memory operations")
    print("\n")

    # Run each benchmark
    print("1/4 Running PANDAS complex benchmark...")
    pandas_results = run_complex_pandas_benchmarks()

    print("\n\n2/4 Running DUCKDB complex benchmark...")
    duckdb_results = run_complex_duckdb_benchmarks()

    print("\n\n3/4 Running POLARS complex benchmark...")
    polars_results = run_complex_polars_benchmarks()

    print("\n\n4/4 Running PYARROW complex benchmark...")
    pyarrow_results = run_complex_pyarrow_benchmarks()

    # Create comparison
    print("\n\n")
    print("=" * 115)
    print("COMPLEX QUERY COMPARISON - QUERY BY QUERY")
    print("=" * 115)
    print("\n")

    # Tables side by side
    print(f"{'Query':<35} {'Pandas':>15} {'DuckDB':>15} {'Polars':>15} {'PyArrow':>15} {'Winner':>15}")
    print("-" * 115)

    query_names = sorted(set(
        duckdb_results.keys() if isinstance(duckdb_results, dict) else []
    ) & set(
        pandas_results.keys() if isinstance(pandas_results, dict) else []
    ))

    all_pandas = []
    all_duckdb = []
    all_polars = []
    all_pyarrow = []

    for q in query_names:
        p_avg = pandas_results[q].get('avg') if q in pandas_results else None
        d_avg = duckdb_results[q].get('avg') if q in duckdb_results else None
        pl_avg = polars_results[q].get('avg') if q in polars_results else None
        pa_avg = pyarrow_results[q].get('avg') if q in pyarrow_results else None

        if p_avg:
            all_pandas.append(p_avg)
        if d_avg:
            all_duckdb.append(d_avg)
        if pl_avg:
            all_polars.append(pl_avg)
        if pa_avg:
            all_pyarrow.append(pa_avg)

        # Determine fastest
        times = []
        if p_avg:
            times.append((p_avg, 'Pandas'))
        if d_avg:
            times.append((d_avg, 'DuckDB'))
        if pl_avg:
            times.append((pl_avg, 'Polars'))
        if pa_avg:
            times.append((pa_avg, 'PyArrow'))

        fastest = min(times, key=lambda x: x[0])[1] if times else 'ERROR'

        p_str = f"{p_avg:.3f}s" if p_avg else "ERROR"
        d_str = f"{d_avg:.3f}s" if d_avg else "ERROR"
        pl_str = f"{pl_avg:.3f}s" if pl_avg else "ERROR"
        pa_str = f"{pa_avg:.3f}s" if pa_avg else "ERROR"

        print(f"{q:<35} {p_str:>15} {d_str:>15} {pl_str:>15} {pa_str:>15} {fastest:>15}")

    # Overall stats
    print("\n")
    print("=" * 115)
    print("COMPLEX QUERY OVERALL PERFORMANCE")
    print("=" * 115)
    print()

    if all_pandas:
        pandas_overall = statistics.mean(all_pandas)
        print(f"Pandas   - Average: {pandas_overall:.3f}s | Best: {min(all_pandas):.3f}s | Worst: {max(all_pandas):.3f}s")

    if all_duckdb:
        duckdb_overall = statistics.mean(all_duckdb)
        print(f"DuckDB   - Average: {duckdb_overall:.3f}s | Best: {min(all_duckdb):.3f}s | Worst: {max(all_duckdb):.3f}s")

    if all_polars:
        polars_overall = statistics.mean(all_polars)
        print(f"Polars   - Average: {polars_overall:.3f}s | Best: {min(all_polars):.3f}s | Worst: {max(all_polars):.3f}s")

    if all_pyarrow:
        pyarrow_overall = statistics.mean(all_pyarrow)
        print(f"PyArrow  - Average: {pyarrow_overall:.3f}s | Best: {min(all_pyarrow):.3f}s | Worst: {max(all_pyarrow):.3f}s")

    # Winner
    print()
    print("=" * 115)
    print("FINAL RESULT - COMPLEX QUERY PERFORMANCE CHAMPION")
    print("=" * 115)
    print()

    options = []
    if all_pandas:
        options.append((statistics.mean(all_pandas), 'Pandas'))
    if all_duckdb:
        options.append((statistics.mean(all_duckdb), 'DuckDB'))
    if all_polars:
        options.append((statistics.mean(all_polars), 'Polars'))
    if all_pyarrow:
        options.append((statistics.mean(all_pyarrow), 'PyArrow'))

    if options:
        winner = min(options, key=lambda x: x[0])
        print(f"WINNER: {winner[1].upper()}")
        print(f"Average time on complex queries: {winner[0]:.3f}s")
        print()
        print("Full Ranking (by average time):")
        for rank, (time, tool) in enumerate(sorted(options, key=lambda x: x[0]), 1):
            medals = ['1st', '2nd', '3rd', '4th']
            medal = medals[rank-1]
            print(f"  {medal:3s}. {tool:10s} - {time:.3f}s avg")

    print()
    print("=" * 115)
    print()

    # Key insight
    print("KEY INSIGHT:")
    if all_duckdb and all_pandas:
        speedup = statistics.mean(all_pandas) / statistics.mean(all_duckdb)
        print(f"DuckDB is {speedup:.1f}x faster than Pandas on complex queries!")
        print(f"(Compare to simple queries where Pandas was 2.2x faster)")
    print()

    return pandas_results, duckdb_results, polars_results, pyarrow_results


if __name__ == "__main__":
    pandas_results, duckdb_results, polars_results, pyarrow_results = run_all_complex_and_compare()
    save_complex_results(duckdb_results, polars_results, pandas_results, pyarrow_results)
