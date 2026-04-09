## Reference Blogs

### Performance & cost: Parquet vs Avro vs ORC
- [“Performance and Cost Implications: Parquet, Avro, ORC.” DZone article with benchmarks and cost models.](https://dzone.com/articles/performance-and-cost-implications-parquet-avro-orc)

### Benchmark of different file formats
- [“Comparison of Different File Formats in Big Data.” Adaltas benchmark (CSV, Parquet, ORC, others).](https://www.adaltas.com/en/2020/07/23/benchmark-study-of-different-file-format/)

### Big Data file formats overview
- [“Big Data File Formats, Explained – Parquet vs ORC vs AVRO vs JSON.” Towards Data Science.](https://towardsdatascience.com/big-data-file-formats-explained-275876dc1fc9/)

### Choosing file formats for big data
- [“A Comparison of Parquet, JSON, ORC, Avro, and CSV: Choosing the Right File Format for Big Data.”](https://ghostinthedata.info/posts/2023/2023-02-12-choosing-the-right-file-format-for-big-data/)

### Evaluation framework for Avro, Parquet, ORC
- [Nexla whitepaper: “Understanding Avro, Parquet & ORC — An Evaluation Framework.”](https://nexla.com/new-whitepaper-understanding-avro-parquet-orc/)

### Parquet/ORC/Avro vs CSV and Data Lakes

### Parquet vs CSV for analytics
- [“Parquet vs CSV: Which Format Should You Choose?” Last9 – includes TPC‑H style benchmarks and size/performance comparisons.](https://last9.io/blog/parquet-vs-csv/)

### What is Parquet?
- [Databricks / MotherDuck style explainer on Parquet vs CSV/Avro/ORC and why it's standard in data lakes.](https://motherduck.com/learn-more/why-choose-parquet-table-file-format/)

### Data lake file formats overview
- [“Data Lake File Formats.” Blog explaining Parquet, ORC, Avro roles and when to pick each.](https://www.ssp.sh/brain/data-lake-file-formats/)

### Optimizing storage formats in data lakes
- [“Optimizing Storage Formats in Data Lakes: Parquet vs. ORC vs. Avro.” HashStudioz.](https://www.hashstudioz.com/blog/optimizing-storage-formats-in-data-lakes-parquet-vs-orc-vs-avro/)

### Understanding ORC, Parquet, and Avro in Azure Data Lake
- [Blog showing file size comparisons and guidance on choosing formats in ADLS.](https://www.certlibrary.com/blog/understanding-orc-parquet-and-avro-file-formats-in-azure-data-lake/)


## DuckDB Official Resources

### Querying Parquet with DuckDB
- [“Querying Parquet with Precision Using DuckDB.” DuckDB Blog, 2021.](https://duckdb.org/2021/06/25/querying-parquet.html)
- Direct Parquet querying, predicate pushdown, and analytical workload optimization.

### DuckDB Parquet Documentation
- [“Reading and Writing Parquet Files.” DuckDB Docs.](https://duckdb.org/docs/stable/data/parquet/overview.html)
- Native Parquet support and scan behavior.

- [“Querying Parquet Files.” DuckDB Guides.](https://duckdb.org/docs/stable/guides/file_formats/query_parquet.html)
- Direct querying without loading into a separate database.

- [“Tuning Workloads.” DuckDB Performance Guides.](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads.html)
- Performance and memory behavior for larger-than-memory workloads.

### DuckDB Benchmarks & Performance
- [“Benchmarking Ourselves over Time at DuckDB.” DuckDB Blog, 2024.](https://duckdb.org/2024/06/26/benchmarks-over-time.html)
- Detailed internal benchmarking methodology and results.

- [“DuckDB vs. Polars: Performance & Memory on Massive Parquet Data.” Codecentric, 2024.](https://www.codecentric.de/en/knowledge-hub/blog/duckdb-vs-polars-performance-and-memory-with-massive-parquet-data)
- Benchmark comparison on large Parquet datasets.

- [“DuckDB vs. Polars vs. Pandas: Benchmark & Comparison.” Codecentric.](https://www.codecentric.de/en/knowledge-hub/blog/duckdb-vs-dataframe-libraries)
- Three-way comparison including Pandas for analytical workloads.

- [“DuckDB vs Pandas vs Polars for Python Developers.” MotherDuck.](https://motherduck.com/blog/duckdb-versus-pandas-versus-polars/)
- Practical comparison supporting DuckDB's strength in analytical workloads.

- [“DuckDB benchmarking for analytics workloads.” DataIntellect.](https://dataintellect.com/blog/duckdb-benchmarking/)
- Applied benchmark results for analytics use cases.

- [“DuckDB Discussion: Parquet vs Native Format Performance.” GitHub Discussion.](https://github.com/duckdb/duckdb/discussions/10161)
- Community observations on Parquet versus native storage performance.


## Polars Documentation

### Parquet Support in Polars
- [“scan_parquet.” Polars API Reference.](https://docs.pola.rs/api/python/dev/reference/api/polars.scan_parquet.html)
- Lazy Parquet scanning and query pushdown.

- [“Parquet.” Polars User Guide.](https://docs.pola.rs/user-guide/io/parquet/)
- Comprehensive guide to Parquet support in Polars.


## Pandas Documentation

### Parquet Ingestion
- [“read_parquet.” Pandas API Reference.](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html)
- Pandas Parquet ingestion through PyArrow engine.



