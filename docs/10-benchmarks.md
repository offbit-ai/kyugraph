# Benchmarks

## What you'll learn

How KyuGraph performs across different workloads — from sequential scans to graph algorithms. All benchmarks run automatically on every push to `main` using Criterion.rs, with interactive reports published to GitHub Pages.

## Live benchmark reports

Browse the latest Criterion reports with interactive charts, statistical summaries, and regression detection:

**[View Benchmark Reports →](https://offbit-ai.github.io/kyugraph/bench/)**

## End-to-end benchmarks

These measure real query execution through the full pipeline (parse → bind → plan → execute):

### Sequential scans

| Benchmark | Query | Scales |
|-----------|-------|--------|
| [Fixed-size scan](https://offbit-ai.github.io/kyugraph/bench/fixed_size_seq_scan/report/index.html) | `MATCH (c:Comment) RETURN c.id, c.length, c.creationDate` | 1K, 10K, 100K |
| [Variable-size scan](https://offbit-ai.github.io/kyugraph/bench/var_size_seq_scan/report/index.html) | `MATCH (c:Comment) RETURN c.browserUsed` | 1K, 10K, 100K |
| [Scan after filter](https://offbit-ai.github.io/kyugraph/bench/scan_after_filter/report/index.html) | `MATCH (c:Comment) WHERE c.length < 10 RETURN c.id` | 1K, 10K, 100K |

### Filter selectivity

How selectivity affects performance — from 0.15% to broad disjunctions:

| Benchmark | Filter | Selectivity |
|-----------|--------|-------------|
| [Low selectivity](https://offbit-ai.github.io/kyugraph/bench/filter_selectivity/report/index.html) | `WHERE c.length < 3` | 0.15% |
| [Medium selectivity](https://offbit-ai.github.io/kyugraph/bench/filter_selectivity/report/index.html) | `WHERE c.length < 150` | 7.5% |
| [Disjunction](https://offbit-ai.github.io/kyugraph/bench/filter_selectivity/report/index.html) | `WHERE c.length < 5 OR c.length > 1900` | ~5% |
| [String prefix](https://offbit-ai.github.io/kyugraph/bench/filter_selectivity/report/index.html) | `WHERE c.browserUsed STARTS WITH 'Ch'` | varies |

### Expressions and aggregation

| Benchmark | Query | What it tests |
|-----------|-------|---------------|
| [Expression evaluator](https://offbit-ai.github.io/kyugraph/bench/fixed_size_expr_evaluator/report/index.html) | `RETURN c.length * 2 * 2 * 2 * 2` | Arithmetic pipeline |
| [Aggregation](https://offbit-ai.github.io/kyugraph/bench/aggregation/report/index.html) | `RETURN c.length % 10, count(c.id)` | GROUP BY + COUNT |
| [ORDER BY](https://offbit-ai.github.io/kyugraph/bench/order_by/report/index.html) | `RETURN c.id ORDER BY c.id LIMIT 5` | Sorting with limit |
| [Multi-operator](https://offbit-ai.github.io/kyugraph/bench/multi_operator_pipeline/report/index.html) | `WHERE p.gender = 'male' RETURN p.browserUsed, count(p.id)` | Filter + aggregate |

### DML (data manipulation)

| Benchmark | Operation | Scales |
|-----------|-----------|--------|
| [Single insert](https://offbit-ai.github.io/kyugraph/bench/dml/report/index.html) | One row insertion latency | single |
| [Bulk load](https://offbit-ai.github.io/kyugraph/bench/dml/report/index.html) | `insert_row()` throughput | 1K, 10K, 100K |

### Graph workloads

| Benchmark | Query | Scales |
|-----------|-------|--------|
| [Recursive join](https://offbit-ai.github.io/kyugraph/bench/recursive_join/report/index.html) | `MATCH (a)-[:KNOWS*1..2]->(b)` | 100, 1K, 10K nodes |
| [Scalar functions](https://offbit-ai.github.io/kyugraph/bench/scalar_functions/report/index.html) | `lower()`, `upper()`, `substring()`, `abs()`, `reverse()`, `typeof()` | 1K, 10K, 100K |
| [Graph algorithms](https://offbit-ai.github.io/kyugraph/bench/graph_algorithms/report/index.html) | PageRank, WCC, betweenness centrality | 100, 1K, 10K nodes |

## Storage micro-benchmarks

Low-level benchmarks measuring the columnar storage engine:

| Benchmark | What it measures | Scales |
|-----------|-----------------|--------|
| [ValueVector get](https://offbit-ai.github.io/kyugraph/bench/value_vector_get/report/index.html) | Flat vs owned vector element access | 10K, 100K |
| [DataChunk iteration](https://offbit-ai.github.io/kyugraph/bench/data_chunk_iteration/report/index.html) | Column-major iteration over 5-column chunks | 2K, 10K |
| [SelectionVector filter](https://offbit-ai.github.io/kyugraph/bench/selection_vector_filter/report/index.html) | Filtered iteration at 10%, 50%, 90% selectivity | 10K, 100K |

## Performance highlights

Results from the latest run on CI (Ubuntu, single-threaded):

| Workload | Result |
|----------|--------|
| Sequential scan (100K rows) | ~5 ms |
| Scan + filter | ~2.5 ms |
| Aggregation (GROUP BY + COUNT) | ~8 ms |
| Bulk load 100K rows | ~65 ms (1.54M rows/sec) |
| PageRank (10K nodes) | ~3.6 ms |

## Run benchmarks locally

```bash
# End-to-end benchmarks
cargo bench -p kyu-api --bench e2e_bench

# Storage micro-benchmarks
cargo bench -p kyu-executor --bench storage_bench

# Open the HTML report
open target/criterion/report/index.html
```

### Run a specific benchmark

```bash
# Only the aggregation benchmark
cargo bench -p kyu-api --bench e2e_bench -- aggregation

# Only PageRank
cargo bench -p kyu-api --bench e2e_bench -- graph_algorithms/pageRank
```

### Compare against a baseline

```bash
# Save current results as baseline
cargo bench -p kyu-api --bench e2e_bench -- --save-baseline main

# After making changes, compare
cargo bench -p kyu-api --bench e2e_bench -- --baseline main
```

## How benchmarks work

All benchmarks use **Criterion.rs** which provides:
- Statistical analysis (mean, median, standard deviation)
- Outlier detection and classification
- Regression detection against previous runs
- Interactive HTML reports with violin plots and time series

Data is generated programmatically using an LDBC Social Network Benchmark-inspired schema — no external datasets required. Each benchmark group measures throughput in elements/second across multiple scales.
