# Data Ingestion

## What you'll learn

How to get data into KyuGraph efficiently — from single-row inserts to bulk loading millions of rows, and the delta fast path for real-time streaming ingestion.

## Step 1: Single-row inserts with Cypher

The simplest way to add data:

```rust
conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
```

Good for small datasets and interactive use. For large datasets, use bulk loading.

## Step 2: Bulk loading with COPY FROM

`COPY FROM` streams data directly from files into columnar storage:

```rust
// CSV (auto-detected by .csv extension)
conn.query("COPY Person FROM '/data/people.csv'").unwrap();

// Parquet (auto-detected by .parquet extension)
conn.query("COPY Person FROM '/data/people.parquet'").unwrap();

// Arrow IPC (auto-detected by .arrow or .ipc extension)
conn.query("COPY Person FROM '/data/people.arrow'").unwrap();
```

File format is determined by extension:

| Extension | Format | Best for |
|-----------|--------|----------|
| `.csv`, `.tsv` | Comma/tab-separated | Universal compatibility |
| `.parquet` | Apache Parquet | Large analytical datasets, columnar compression |
| `.arrow`, `.ipc` | Arrow IPC | Zero-copy interop with Arrow-native tools |

### CSV format

Header row must match the table column names:

```csv
id,name,age
1,Alice,30
2,Bob,25
3,Charlie,28
```

### Using environment bindings for dynamic paths

```rust
use std::collections::HashMap;
use kyu_types::TypedValue;

let mut env = HashMap::new();
env.insert("data_dir".to_string(), TypedValue::String("/data/imports".into()));

conn.execute(
    "COPY Person FROM env('data_dir') + '/people.csv'",
    HashMap::new(),
    env,
).unwrap();
```

## Step 3: Delta fast path for streaming

For high-throughput ingestion (agentic code graphs, document pipelines), `apply_delta` provides conflict-free idempotent upserts that bypass OCC:

```rust
use kyu_graph::{Database, DeltaBatchBuilder, DeltaValue};

let db = Database::in_memory();
let conn = db.connect();
conn.query("CREATE NODE TABLE Function (
    name STRING, lines INT64, complexity INT64,
    PRIMARY KEY (name)
)").unwrap();
conn.query("CREATE REL TABLE CALLS (FROM Function TO Function)").unwrap();

// Build a batch of upserts
let batch = DeltaBatchBuilder::new("file:src/main.rs", 1000)
    .upsert_node("Function", "main", vec![], [
        ("lines", DeltaValue::Int64(42)),
        ("complexity", DeltaValue::Int64(5)),
    ])
    .upsert_node("Function", "helper", vec![], [
        ("lines", DeltaValue::Int64(10)),
    ])
    .upsert_edge("Function", "main", "CALLS", "Function", "helper", [])
    .build();

let stats = conn.apply_delta(batch).unwrap();
println!("{}", stats); // nodes: +2/~0/-0, edges: +1/~0/-0
```

### Delta semantics

- **Upsert** — creates if not exists, updates if exists (merge semantics: unmentioned properties unchanged)
- **Last-write-wins** on timestamp — concurrent writers resolve deterministically
- **Idempotent** — replaying the same batch produces the same result
- **Conflict-free** — no OCC validation overhead

### Batch operations

```rust
let batch = DeltaBatchBuilder::new("source:pipeline-1", timestamp)
    // Upsert nodes (label, primary_key, extra_labels, properties)
    .upsert_node("Person", "alice", vec![], [("age", DeltaValue::Int64(30))])

    // Upsert edges (src_label, src_pk, rel_type, dst_label, dst_pk, properties)
    .upsert_edge("Person", "alice", "FOLLOWS", "Person", "bob", [])

    // Delete nodes
    .delete_node("Person", "charlie")

    // Delete edges
    .delete_edge("Person", "alice", "FOLLOWS", "Person", "bob")
    .build();
```

### DeltaValue types

```rust
pub enum DeltaValue {
    Null,
    Bool(bool),
    Int64(i64),
    Double(f64),
    String(SmolStr),
    List(Vec<DeltaValue>),
}
```

### Causal ordering with VectorClock

For distributed ingestion with ordering guarantees:

```rust
let clock = VectorClock::new();
let batch = DeltaBatchBuilder::new("worker-1", timestamp)
    .upsert_node("Function", "main", vec![], [("lines", DeltaValue::Int64(42))])
    .build()
    .with_vector_clock(clock);
```

## Step 4: Verify your data

After ingestion, verify with queries:

```rust
let result = conn.query("MATCH (f:Function) RETURN f.name, f.lines").unwrap();
for row in result.iter_rows() {
    println!("{}: {} lines", row[0], row[1]);
}

let result = conn.query("
    MATCH (a:Function)-[:CALLS]->(b:Function)
    RETURN a.name, b.name
").unwrap();
```

## When to use which method

| Method | Throughput | Use case |
|--------|-----------|----------|
| `CREATE` (Cypher) | Low | Interactive use, small datasets |
| `COPY FROM` | High | Batch loading from files |
| `apply_delta` | Very high | Streaming, real-time ingestion, idempotent pipelines |
