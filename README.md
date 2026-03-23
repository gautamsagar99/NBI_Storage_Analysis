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
See the analysis notebook: [`nbi_storage_analysis.ipynb`](nbi_storage_analysis.ipynb)