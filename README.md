# NBI Data Format Analysis Project

## Overview
This project analyzes National Bridge Inventory (NBI) data storage formats by converting datasets to various compressed formats and validating the results. The goal is to compare storage efficiency, read/write performance, and compression ratios across different formats.

## Requirements
- Python 3.8+
- Dependencies: `pip install -r requirements.txt`

## Scripts
All scripts are located in the `scripts/` directory:

### Conversion Scripts
- [`convert_to_csv_gzip.py`](scripts/convert_to_csv_gzip.py): CSV conversion with GZIP compression
- [`convert_to_orc_snappy.py`](scripts/convert_to_orc_snappy.py): ORC format with Snappy compression
- [`convert_to_parquet_snappy.py`](scripts/convert_to_parquet_snappy.py): Parquet format with Snappy compression
- [`convert_to_parquet_zstd.py`](scripts/convert_to_parquet_zstd.py): Parquet format with Zstandard compression

### Validation Scripts
- [`convert_to_csv_gzip_validation.py`](scripts/convert_to_csv_gzip_validation.py): Validates CSV+GZIP outputs
- [`convert_to_orc_snappy_validation.py`](scripts/convert_to_orc_snappy_validation.py): Validates ORC+Snappy outputs
- [`convert_to_parquet_snappy_validation.py`](scripts/convert_to_parquet_snappy_validation.py): Validates Parquet+Snappy outputs
- [`convert_to_parquet_zstd_validation.py`](scripts/convert_to_parquet_zstd_validation.py): Validates Parquet+Zstandard outputs

### Future Work
- [`convert_to_avro - future works(now its not working).py`](scripts/convert_to_avro%20-%20future%20works(now%20its%20not%20working).py): AVRO format implementation (WIP)

## Usage
1. Place NBI dataset in project root
2. Run conversion script:
   ```bash
   python scripts/convert_to_<format>_<compression>.py
   ```
3. Validate outputs:
   ```bash
   python scripts/convert_to_<format>_<compression>_validation.py
   ```

## Analysis

### Storage Format Analysis
See the analysis notebook: [`nbi_storage_analysis.ipynb`](nbi_storage_analysis.ipynb)

---

## Query Performance Analysis

Benchmark 4 different Python tools (DuckDB, Pandas, Polars, PyArrow) for querying parquet data. Tests both simple and complex query scenarios to determine which tool performs best for different use cases.

### Overview
- **Simple Queries**: 1 column, basic aggregation (STATE_CODE_001)
- **Complex Queries**: 12 columns, multi-level aggregation with filtering and grouping
- **Dataset**: 1,728 parquet files (~1.4 GB), 21.6M total rows
- **Tools**: DuckDB, Pandas, Polars (with lazy evaluation), PyArrow (with Dataset API)

### Simple Query Benchmarks

Run individual simple query benchmarks:

```bash
# Test individual tools
python scripts/query_analysis/benchmark_duckdb.py
python scripts/query_analysis/benchmark_pandas.py
python scripts/query_analysis/benchmark_polars.py
python scripts/query_analysis/benchmark_pyarrow.py

# Run all simple benchmarks + save results to JSON
python scripts/query_analysis/benchmark_summary.py
```

**Results saved to**: `scripts/query_analysis/benchmark_results.json`

### Complex Query Benchmarks

Run complex query benchmarks (12 columns, multi-level aggregation):

```bash
# Test individual tools on complex queries
python scripts/query_analysis/complex_duckdb.py
python scripts/query_analysis/complex_pandas.py
python scripts/query_analysis/complex_polars.py
python scripts/query_analysis/complex_pyarrow.py

# Run all complex benchmarks + save results to JSON
python scripts/query_analysis/complex_summary.py
```

**Results saved to**: `scripts/query_analysis/complex_benchmark_results.json`

### Analysis Notebooks

Interactive analysis with visualizations:

```bash
# Simple query analysis
jupyter notebook nbi_query_analysis.ipynb

# Complex query analysis
jupyter notebook nbi_complex_query_analysis.ipynb
```

**Features**:
- Performance comparison charts
- Query-by-query winner analysis
- Statistical summaries
- Tool recommendations by use case
- ⚠️ **Important disclaimer**: Results based on raw, uncleaned data with performance varying by query complexity

### Key Findings

| Scenario | Winner | Time | Notes |
|----------|--------|------|-------|
| **Simple Query** (1 column) | Pandas | 0.547s | No optimization overhead |
| **Complex Query** (12 columns) | DuckDB | 2.257s | SQL planner + predicate pushdown |
| **Lazy Evaluation** | Polars | 4.126s | Better than eager loading |
| **Dataset API** | PyArrow | 7.762s | 80% improvement with optimization |

### Performance Interpretation

1. **Data Quality Impact**: Results use raw, uncleaned NBI data
   - Type inconsistencies (mixed Int64/Float64)
   - Missing values may affect performance
   - Production cleaned data may show different results

2. **Query Type Matters**:
   - Simple queries: Pandas dominates (minimal overhead)
   - Complex queries: DuckDB wins (optimization benefits)
   - Selectivity: Filter effectiveness affects which tool performs best

3. **Tool Optimization Strategies**:
   - **DuckDB**: SQL query planner with predicate pushdown
   - **Polars**: Lazy evaluation (deferred execution)
   - **PyArrow**: Dataset API with filter pushdown
   - **Pandas**: Eager evaluation (no optimization)