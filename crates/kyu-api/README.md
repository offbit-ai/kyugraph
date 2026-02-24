# kyu-api

Database and Connection API for [KyuGraph](https://crates.io/crates/kyu-graph) with Arrow Flight support.

This crate is the internal engine API. Most users should depend on [`kyu-graph`](https://crates.io/crates/kyu-graph) instead, which re-exports the public surface from this crate.

## Core Types

| Type | Description |
|------|-------------|
| `Database` | Top-level entry point owning catalog, storage, WAL, and transaction manager |
| `Connection` | Executes Cypher queries and DDL against a database |
| `NodeGroupStorage` | Columnar storage backed by NodeGroup/ColumnChunk |

## Usage

```rust
use kyu_api::{Database, Connection};

// In-memory
let db = Database::in_memory();
let conn = db.connect();
conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
conn.query("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();

let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
for row in result.iter_rows() {
    println!("{:?}", row);
}
```

### Persistent Database

```rust
use kyu_api::Database;

let db = Database::open(std::path::Path::new("./my_graph")).unwrap();
let conn = db.connect();
// Schema + data persisted to disk via WAL + checkpointing.
```

### Parameterized Queries

```rust
use std::collections::HashMap;
use kyu_types::TypedValue;

let mut params = HashMap::new();
params.insert("min_age".to_string(), TypedValue::Int64(25));
let result = conn.query_with_params(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
    params,
).unwrap();
```

### Full VM Execution (params + env)

```rust
use std::collections::HashMap;
use kyu_types::TypedValue;

let params = HashMap::new();
let env = HashMap::new();
let result = conn.execute("MATCH (n) RETURN n", params, env).unwrap();
```

### Delta Fast Path

Conflict-free idempotent upserts bypassing OCC for high-throughput ingestion:

```rust
use kyu_delta::{DeltaBatchBuilder, DeltaValue};

let batch = DeltaBatchBuilder::new("source:my-pipeline", 1000)
    .upsert_node("Person", "1", vec![], [("name", DeltaValue::String("Alice".into()))])
    .build();
let stats = conn.apply_delta(batch).unwrap();
```

### Extensions

```rust
use kyu_api::Database;
use kyu_extension::Extension;

let mut db = Database::in_memory();
// db.register_extension(Box::new(my_ext));
let conn = db.connect();
```

### Arrow Flight Server

Expose KyuGraph as an Arrow Flight gRPC endpoint:

```rust
use std::sync::Arc;
use kyu_api::{Database, serve_flight};

let db = Arc::new(Database::in_memory());
// serve_flight(db, "0.0.0.0", 50051).await.unwrap();
```

Convert query results to Arrow RecordBatch:

```rust
use kyu_api::to_record_batch;

let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();
if let Some(batch) = to_record_batch(&result) {
    println!("Arrow schema: {:?}", batch.schema());
}
```

## Architecture

```
Application
    │
    ▼
┌─────────┐     ┌────────────┐
│ Database │────▶│ Connection │  ← you are here (kyu-api)
└─────────┘     └────────────┘
    │                │
    ▼                ▼
┌────────┐    ┌──────────────┐
│Catalog │    │ kyu-executor │
└────────┘    └──────────────┘
    │                │
    ▼                ▼
┌──────────────────────────┐
│  kyu-storage (columnar)  │
└──────────────────────────┘
    │
    ▼
┌──────────────────────────┐
│  kyu-transaction (WAL)   │
└──────────────────────────┘
```

## Query Pipeline

1. **Parse** — `kyu-parser` lexes and parses Cypher into an AST
2. **Bind** — `kyu-binder` resolves names against the catalog
3. **Plan** — `kyu-planner` builds and optimizes a logical plan
4. **Execute** — `kyu-executor` runs the plan against storage
5. **Commit** — `kyu-transaction` persists via WAL + checkpoint

## License

MIT
