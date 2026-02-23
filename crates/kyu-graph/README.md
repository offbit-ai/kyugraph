# kyu-graph

[![Crates.io](https://img.shields.io/crates/v/kyu-graph.svg)](https://crates.io/crates/kyu-graph)
[![docs.rs](https://img.shields.io/docsrs/kyu-graph)](https://docs.rs/kyu-graph)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/offbit-ai/kyugraph/blob/main/LICENSE)

High-performance embedded property graph database for Rust.

KyuGraph is a pure-Rust embedded graph database implementing the [openCypher](https://opencypher.org/) query language. It uses columnar storage, vectorized execution, and optional Cranelift JIT compilation for analytical graph workloads.

## Features

- **openCypher queries** — `MATCH`, `CREATE`, `SET`, `DELETE`, `MERGE`, `WITH`, `ORDER BY`, `COPY FROM`, and more
- **In-memory or persistent** — zero-config in-memory mode, or durable on-disk storage with WAL and automatic checkpointing
- **Parameterized queries** — safe `$param` placeholders resolved at bind time, with JSON construction helpers
- **Columnar engine** — cache-friendly node-group layout with selection-vector filtering and morsel-driven pipelines
- **Cranelift JIT** — filter predicates and projections compiled to native code for up to 22x speedup
- **Bulk ingestion** — `COPY FROM` for CSV, Parquet, and Arrow IPC
- **Extension system** — pluggable graph algorithms, full-text search, and vector similarity
- **Arrow Flight** — gRPC interface for remote clients and BI tools

## Quick Start

Add `kyu-graph` to your `Cargo.toml`:

```toml
[dependencies]
kyu-graph = "0.1"
```

### In-Memory Database

```rust
use kyu_graph::{Database, TypedValue};

let db = Database::in_memory();
let conn = db.connect();

// Create schema
conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
    .unwrap();
conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)")
    .unwrap();

// Insert data
conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
conn.query("CREATE (p:Person {id: 2, name: 'Bob', age: 25})").unwrap();
conn.query("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b)")
    .unwrap();

// Query
let result = conn.query("MATCH (p:Person) WHERE p.age > 20 RETURN p.name, p.age").unwrap();
for row in result.iter_rows() {
    println!("{:?}", row);
}
```

### Persistent Database

```rust
use kyu_graph::Database;

let db = Database::open(std::path::Path::new("./my_graph")).unwrap();
let conn = db.connect();
conn.query("CREATE NODE TABLE Log (id INT64, msg STRING, PRIMARY KEY (id))").unwrap();
// Data is checkpointed to disk automatically.
```

## Parameterized Queries

Pass dynamic values safely via `$param` placeholders instead of string interpolation:

```rust
use kyu_graph::{Database, BindContext, json};

let db = Database::in_memory();
let conn = db.connect();
conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
    .unwrap();
conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();

// From the json! macro
let ctx = BindContext::with_params_json(json!({"min_age": 25}));

// From a JSON string (useful for config files or HTTP request bodies)
let ctx = BindContext::with_params_str(r#"{"min_age": 25}"#).unwrap();

// Or use HashMap<String, TypedValue> directly
use std::collections::HashMap;
use kyu_graph::TypedValue;
let mut params = HashMap::new();
params.insert("min_age".to_string(), TypedValue::Int64(25));
let result = conn.query_with_params(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
    params,
).unwrap();
```

### Environment Bindings

For automation pipelines, `env()` lookups resolve from an environment map at bind time:

```rust
use kyu_graph::{Database, json};
use std::collections::HashMap;
use kyu_graph::TypedValue;

let db = Database::in_memory();
let conn = db.connect();
conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();

let params = HashMap::new();
let mut env = HashMap::new();
env.insert("data_dir".to_string(), TypedValue::String("/data/imports".into()));

let result = conn.execute(
    "COPY Person FROM env('data_dir') + '/people.csv'",
    params,
    env,
);
```

## Bulk Data Import

```rust
use kyu_graph::Database;

let db = Database::in_memory();
let conn = db.connect();
conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
conn.query("COPY Person FROM 'people.csv'").unwrap();
```

## Extensions

Register pluggable extensions for graph algorithms, full-text search, and vector similarity:

```rust
use kyu_graph::{Database, Extension};

let mut db = Database::in_memory();
// db.register_extension(Box::new(my_extension));
let conn = db.connect();
// conn.query("CALL algo.pagerank('Person', 'KNOWS')").unwrap();
```

## API Overview

| Method | Description |
|---|---|
| `Database::in_memory()` | Create an in-memory database |
| `Database::open(path)` | Open or create a persistent database |
| `db.connect()` | Create a connection |
| `conn.query(cypher)` | Execute a Cypher statement |
| `conn.query_with_params(cypher, params)` | Execute with `$param` bindings |
| `conn.execute(cypher, params, env)` | Execute with params and `env()` bindings |

## Type System

KyuGraph maps Cypher values to Rust via `TypedValue`:

| Cypher | Rust (`TypedValue`) |
|---|---|
| `INT64` | `TypedValue::Int64(i64)` |
| `DOUBLE` | `TypedValue::Double(f64)` |
| `BOOL` | `TypedValue::Bool(bool)` |
| `STRING` | `TypedValue::String(SmolStr)` |
| `DATE` | `TypedValue::Date(i32)` |
| `TIMESTAMP` | `TypedValue::Timestamp(i64)` |
| `INTERVAL` | `TypedValue::Interval(Interval)` |
| `LIST` | `TypedValue::List(Vec<TypedValue>)` |
| `MAP` | `TypedValue::Map(Vec<(SmolStr, TypedValue)>)` |
| `NULL` | `TypedValue::Null` |

Bidirectional conversion with `serde_json::Value` is supported via `From` impls.

## License

MIT — see [LICENSE](https://github.com/offbit-ai/kyugraph/blob/main/LICENSE) for details.
