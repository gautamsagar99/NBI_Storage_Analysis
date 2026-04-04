"""
Benchmark Summary - Compare all 4 approaches
Uses only safe columns: STATE_CODE_001
"""

import sys
import json
from pathlib import Path
from benchmark_duckdb import run_duckdb_benchmarks
from benchmark_polars import run_polars_benchmarks
from benchmark_pandas import run_pandas_benchmarks
from benchmark_pyarrow import run_pyarrow_benchmarks
import statistics


def save_results(duckdb_results, polars_results, pandas_results, pyarrow_results):
    """Save benchmark results to JSON for analysis"""
    results_data = {
        'duckdb': duckdb_results,
        'polars': polars_results,
        'pandas': pandas_results,
        'pyarrow': pyarrow_results
    }

    results_file = Path(__file__).parent / "benchmark_results.json"
    with open(results_file, 'w') as f:
        json.dump(results_data, f, indent=2)

    print(f"\n[OK] Results saved to: {results_file}")
    return results_file


def run_all_and_compare():
    """Run all benchmarks and create comparison"""
    print("\n" * 2)
    print("=" * 100)
    print("COMPARING: DuckDB vs Polars vs Pandas vs PyArrow")
    print("=" * 100)
    print("\n")

    # Run each benchmark
    print("1/4 Running DuckDB benchmark...")
    duckdb_results = run_duckdb_benchmarks()

    print("\n\n2/4 Running Polars benchmark...")
    polars_results = run_polars_benchmarks()

    print("\n\n3/4 Running Pandas benchmark...")
    pandas_results = run_pandas_benchmarks()

    print("\n\n4/4 Running PyArrow benchmark...")
    pyarrow_results = run_pyarrow_benchmarks()

    # Create comparison
    print("\n\n")
    print("=" * 115)
    print("COMPARISON - QUERY BY QUERY")
    print("=" * 115)
    print("\n")

    # Tables side by side
    print(f"{'Query':<30} {'DuckDB':>15} {'Polars':>15} {'Pandas':>15} {'PyArrow':>15} {'Winner':>15}")
    print("-" * 115)

    query_names = sorted(set(duckdb_results.keys()) & set(polars_results.keys()) & set(pandas_results.keys()) &
                                set(pyarrow_results.keys()))

    all_duckdb = []
    all_polars = []
    all_pandas = []
    all_pyarrow = []

    for q in query_names:
        d_avg = duckdb_results[q].get('avg')
        p_avg = polars_results[q].get('avg')
        pn_avg = pandas_results[q].get('avg')
        pa_avg = pyarrow_results[q].get('avg')

        if d_avg:
            all_duckdb.append(d_avg)
        if p_avg:
            all_polars.append(p_avg)
        if pn_avg:
            all_pandas.append(pn_avg)
        if pa_avg:
            all_pyarrow.append(pa_avg)

        # Determine fastest
        times = []
        if d_avg:
            times.append((d_avg, 'DuckDB'))
        if p_avg:
            times.append((p_avg, 'Polars'))
        if pn_avg:
            times.append((pn_avg, 'Pandas'))
        if pa_avg:
            times.append((pa_avg, 'PyArrow'))

        fastest = min(times, key=lambda x: x[0])[1] if times else 'ERROR'

        d_str = f"{d_avg:.3f}s" if d_avg else "ERROR"
        p_str = f"{p_avg:.3f}s" if p_avg else "ERROR"
        pn_str = f"{pn_avg:.3f}s" if pn_avg else "ERROR"
        pa_str = f"{pa_avg:.3f}s" if pa_avg else "ERROR"

        print(f"{q:<30} {d_str:>15} {p_str:>15} {pn_str:>15} {pa_str:>15} {fastest:>15}")

    # Overall stats
    print("\n")
    print("=" * 115)
    print("OVERALL PERFORMANCE")
    print("=" * 115)
    print()

    if all_duckdb:
        duckdb_overall = statistics.mean(all_duckdb)
        print(f"DuckDB  - Average: {duckdb_overall:.3f}s | Best: {min(all_duckdb):.3f}s | Worst: {max(all_duckdb):.3f}s")

    if all_polars:
        polars_overall = statistics.mean(all_polars)
        print(f"Polars  - Average: {polars_overall:.3f}s | Best: {min(all_polars):.3f}s | Worst: {max(all_polars):.3f}s")

    if all_pandas:
        pandas_overall = statistics.mean(all_pandas)
        print(f"Pandas  - Average: {pandas_overall:.3f}s | Best: {min(all_pandas):.3f}s | Worst: {max(all_pandas):.3f}s")

    if all_pyarrow:
        pyarrow_overall = statistics.mean(all_pyarrow)
        print(f"PyArrow - Average: {pyarrow_overall:.3f}s | Best: {min(all_pyarrow):.3f}s | Worst: {max(all_pyarrow):.3f}s")

    # Winner
    print()
    print("=" * 115)
    print("FINAL RESULT - OVERALL WINNER")
    print("=" * 115)
    print()

    options = []
    if all_duckdb:
        options.append((statistics.mean(all_duckdb), 'DuckDB'))
    if all_polars:
        options.append((statistics.mean(all_polars), 'Polars'))
    if all_pandas:
        options.append((statistics.mean(all_pandas), 'Pandas'))
    if all_pyarrow:
        options.append((statistics.mean(all_pyarrow), 'PyArrow'))

    if options:
        winner = min(options, key=lambda x: x[0])
        print(f"FASTEST TOOL: {winner[1].upper()}")
        print(f"Average query time: {winner[0]:.3f}s")
        print()
        print("Ranking (by average time):")
        for rank, (time, tool) in enumerate(sorted(options, key=lambda x: x[0]), 1):
            print(f"  {rank}. {tool:12s} - {time:.3f}s avg")

    print()
    print("=" * 115)

    return duckdb_results, polars_results, pandas_results, pyarrow_results


if __name__ == "__main__":
    duckdb_results, polars_results, pandas_results, pyarrow_results = run_all_and_compare()
    save_results(duckdb_results, polars_results, pandas_results, pyarrow_results)
