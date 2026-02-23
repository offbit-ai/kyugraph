# KyuGraph

[![CI](https://github.com/offbit-ai/kyugraph/actions/workflows/ci.yml/badge.svg)](https://github.com/offbit-ai/kyugraph/actions/workflows/ci.yml)
[![Benchmarks](https://github.com/offbit-ai/kyugraph/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/offbit-ai/kyugraph/actions/workflows/benchmarks.yml)
[![Rust](https://img.shields.io/badge/rust-stable-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

KyuGraph is a high-performance embedded property graph database written in pure Rust. It is a port of [RyuGraph](https://github.com/ryu-graph/ryugraph) — itself a C++ fork of [Kùzu](https://github.com/kuzudb/kuzu) — redesigned from the ground up to leverage Rust's ownership model, zero-cost abstractions, and modern concurrency primitives.

KyuGraph implements the openCypher query language and targets analytical graph workloads where low-latency traversals and columnar scans coexist.

## Key Features

- **Cypher Query Language** — Hand-written lexer with combinator-based parser, full expression binding, and query planning
- **Columnar Storage Engine** — Cache-friendly columnar layout with node groups, CSR-compressed adjacency lists, and a split read/write buffer pool
- **MVCC Transactions** — Serializable isolation via multi-version concurrency control with write-ahead logging and automatic checkpointing
- **Vectorized Execution** — Morsel-driven pipeline execution operating on typed value vectors with selection vector filtering
- **Cranelift JIT Compilation** — Filter predicates and arithmetic projections are compiled to native machine code at query time, delivering up to 22x speedup over tree-walking evaluation
- **Cost-Based Optimizer** — Statistics-driven cardinality estimation with selectivity heuristics for join reordering
- **Multi-Format Ingestion** — `COPY FROM` supports CSV, Parquet, Arrow IPC, and Kafka streaming sources
- **Arrow Flight Protocol** — gRPC-based network interface for remote clients and BI tool integration
- **Cloud-Native Storage** — S3-backed page store with NVMe write-through disk cache and cross-AZ WAL replication
- **Extension System** — Pluggable algorithm, full-text search, vector similarity, and JSON extensions
- **Language Bindings** — Python (PyO3) and Node.js (NAPI-rs) client libraries

## Performance

All benchmarks run on Apple M-series silicon using Criterion 0.5 with `opt-level = 3`, LTO, and single codegen unit. Datasets follow the LDBC Social Network Benchmark schema.

### Query Execution

| Operation | 1K rows | 10K rows | 100K rows |
|---|---:|---:|---:|
| Sequential scan (fixed-size) | 0.37 ms | 1.18 ms | 5.06 ms |
| Sequential scan (variable-size) | 0.38 ms | 1.48 ms | 2.74 ms |
| Scan + filter | 0.41 ms | 0.52 ms | 2.46 ms |
| Aggregation (GROUP BY + COUNT) | 0.34 ms | 1.02 ms | 8.11 ms |
| Order by + LIMIT | 0.32 ms | 1.32 ms | 12.19 ms |
| Multi-operator pipeline | 0.31 ms | 0.79 ms | 6.51 ms |
| Expression evaluation | 0.35 ms | 1.34 ms | 8.76 ms |

### JIT vs Batch vs Scalar Evaluation (100K rows)

| Expression | Scalar | Batch | JIT | Speedup (JIT vs Scalar) |
|---|---:|---:|---:|---:|
| Filter `col > N/2` | 2.33 ms | 0.15 ms | 0.10 ms | **22.3x** |
| Compound predicate | 12.24 ms | 0.66 ms | 0.40 ms | **30.8x** |
| Arithmetic projection | 3.75 ms | 2.22 ms | 0.18 ms | **21.0x** |

### Graph Algorithms

| Algorithm | 100 nodes | 1K nodes | 10K nodes |
|---|---:|---:|---:|
| PageRank | 0.03 ms | 0.33 ms | 3.58 ms |
| Weakly Connected Components | 0.04 ms | 0.40 ms | 4.43 ms |
| Betweenness Centrality | 0.81 ms | 67.94 ms | — |

### Data Ingestion

| Operation | Latency / Throughput |
|---|---:|
| Single row insert | 0.34 ms |
| Bulk load 1K rows | 0.77 ms |
| Bulk load 10K rows | 6.85 ms |
| Bulk load 100K rows | 65.04 ms (1.54M rows/sec) |

### Storage Micro-Benchmarks

| Operation | 10K elements | 100K elements |
|---|---:|---:|
| FlatVector random access | 39.0 µs | 380.1 µs |
| DataChunk iteration (5 cols) | 58.6 µs | 284.2 µs |
| SelectionVector filter (10%) | 4.8 µs | 53.8 µs |
| SelectionVector filter (50%) | 26.5 µs | 239.7 µs |

### Recursive Joins (Variable-Length Paths)

| Hops | 100 nodes | 1K nodes | 10K nodes |
|---|---:|---:|---:|
| 1–2 hops | 0.50 ms | 2.31 ms | 24.71 ms |
| 1–3 hops | 0.55 ms | 3.18 ms | 33.40 ms |

## Optimization Approach

KyuGraph employs a layered optimization strategy:

**Storage layer.** Data is organized in fixed-size node groups with columnar layout for cache-efficient scans. A split buffer pool separates read and write workloads, and a clock-based eviction policy keeps hot pages resident. The CSR-compressed adjacency structure enables O(1) neighbor lookups.

**Execution layer.** The vectorized engine processes data in batches of 2048 values through a morsel-driven pipeline. Selection vectors avoid materializing intermediate results during filtering. A three-tier expression evaluator routes predicates through tree-walking interpretation, pattern-matched batch evaluation, or Cranelift-compiled native code depending on query complexity and expected cardinality.

**Planning layer.** The cost-based optimizer derives table statistics from catalog metadata and applies selectivity heuristics (equality: 1/NDV, range: 1/3, conjunction: product, disjunction: inclusion-exclusion) to estimate cardinalities. Join reordering swaps build and probe sides of hash joins to minimize the size of hash tables built in memory.

**Cloud layer.** For distributed deployments, the storage layer abstracts over local and remote page stores. An S3-backed page store provides elastic capacity, a write-through NVMe disk cache reduces read latency, and a cloud WAL replicates committed segments to a remote sink for cross-AZ durability.

## Quick Start

### Rust

```rust
use kyu_api::Database;

fn main() {
    let db = Database::in_memory();
    let conn = db.connect();

    // Create schema
    conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    conn.query("CREATE NODE TABLE City (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    conn.query("CREATE REL TABLE LIVES_IN (FROM Person TO City)")
        .unwrap();

    // Insert data
    conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
    conn.query("CREATE (p:Person {id: 2, name: 'Bob', age: 25})").unwrap();
    conn.query("CREATE (c:City {id: 1, name: 'Tokyo'})").unwrap();

    // Query
    let result = conn.query("MATCH (p:Person) WHERE p.age > 20 RETURN p.name, p.age").unwrap();
    for row in result.iter_rows() {
        println!("{:?}", row);
    }

    // Persistent database (survives restart)
    let db = Database::open(std::path::Path::new("./my_graph")).unwrap();
    let conn = db.connect();
    conn.query("CREATE NODE TABLE Log (id INT64, msg STRING, PRIMARY KEY (id))").unwrap();
    // Data is checkpointed to disk on drop
}
```

### Python

```bash
pip install kyugraph
```

```python
import kyugraph

# In-memory database
db = kyugraph.Database()
conn = db.connect()

# Create schema
conn.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
conn.execute("CREATE NODE TABLE City (id INT64, name STRING, PRIMARY KEY (id))")
conn.execute("CREATE REL TABLE LIVES_IN (FROM Person TO City)")

# Insert data
conn.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 30})")
conn.execute("CREATE (p:Person {id: 2, name: 'Bob', age: 25})")
conn.execute("CREATE (c:City {id: 1, name: 'Tokyo'})")

# Query
result = conn.query("MATCH (p:Person) WHERE p.age > 20 RETURN p.name, p.age")
print(result.column_names())  # ['p.name', 'p.age']
for row in result:
    print(row)  # ['Alice', 30], ['Bob', 25]

# Bulk load from CSV / Parquet
conn.execute("COPY Person FROM 'people.csv'")

# Persistent database
db = kyugraph.Database("/path/to/my_graph")
```

### Node.js

```bash
npm install kyugraph
```

```javascript
const { Database } = require('kyugraph');

// In-memory database
const db = new Database();
const conn = db.connect();

// Create schema
conn.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))");
conn.execute("CREATE NODE TABLE City (id INT64, name STRING, PRIMARY KEY (id))");
conn.execute("CREATE REL TABLE LIVES_IN (FROM Person TO City)");

// Insert data
conn.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 30})");
conn.execute("CREATE (p:Person {id: 2, name: 'Bob', age: 25})");
conn.execute("CREATE (c:City {id: 1, name: 'Tokyo'})");

// Query
const result = conn.query("MATCH (p:Person) WHERE p.age > 20 RETURN p.name, p.age");
console.log(result.columnNames());  // ['p.name', 'p.age']
for (const row of result.toArray()) {
    console.log(row);  // ['Alice', 30], ['Bob', 25]
}

// Persistent database
const pdb = new Database("/path/to/my_graph");
```

## Examples

The [`crates/kyu-api/examples/`](crates/kyu-api/examples/) directory contains end-to-end examples showcasing KyuGraph's capabilities:

| Example | Description | Run |
|---|---|---|
| [knowledge_graph](crates/kyu-api/examples/knowledge_graph.rs) | Builds a research paper knowledge graph with CSV bulk ingestion, full-text search, vector similarity, and PageRank | `cargo run -p kyu-api --example knowledge_graph` |
| [filesystem_graph](crates/kyu-api/examples/filesystem_graph.rs) | Ingests a real codebase directory tree, extracts cross-crate Rust import dependencies, and runs content-aware analysis | `cargo run -p kyu-api --example filesystem_graph` |
| [agent_explorer](crates/kyu-api/examples/agent_explorer.rs) | Simulates an autonomous AI agent that incrementally discovers knowledge — follows imports, then uses FTS and vector similarity to find emergent connections not in any import chain | `cargo run -p kyu-api --example agent_explorer` |

## Building

```bash
cargo build --workspace
cargo test --workspace
cargo bench
```

## License

KyuGraph is licensed under the [MIT License](LICENSE).
