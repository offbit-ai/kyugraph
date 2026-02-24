# Arrow Flight Server

## What you'll build

A gRPC endpoint that exposes your KyuGraph database to remote clients. Any Arrow Flight client (Python, Java, Go, Rust) can connect and execute Cypher queries, receiving results as zero-copy Arrow RecordBatches.

## Step 1: Start the server from the CLI

The fastest way to get a Flight server running:

```bash
# In-memory database with Flight server on port 50051
kyu-graph-cli --serve

# Persistent database
kyu-graph-cli --path ./my_graph --serve --port 50051

# Custom bind address
kyu-graph-cli --path ./my_graph --serve --host 0.0.0.0 --port 8080
```

| Flag | Default | Description |
|------|---------|-------------|
| `--serve` | off | Enable Arrow Flight server mode |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `50051` | gRPC port |

## Step 2: Start the server from Rust

For embedding in your own application:

```rust
use std::sync::Arc;
use kyu_graph::{Database, serve_flight};

#[tokio::main]
async fn main() {
    let mut db = Database::in_memory();
    // Register extensions, create schema, load data...

    let conn = db.connect();
    conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();

    let db = Arc::new(db);
    println!("Starting Flight server on port 50051...");
    serve_flight(db, "0.0.0.0", 50051).await.unwrap();
}
```

## Step 3: Connect from Python

Use PyArrow's Flight client to query the database:

```python
import pyarrow.flight as flight

# Connect to the server
client = flight.connect("grpc://localhost:50051")

# Execute a Cypher query
ticket = flight.Ticket(b"MATCH (p:Person) RETURN p.name, p.age")
reader = client.do_get(ticket)

# Read results as a Pandas DataFrame
table = reader.read_all()
df = table.to_pandas()
print(df)
#     p.name  p.age
# 0   Alice     30
# 1     Bob     25
```

## Step 4: Convert query results to Arrow locally

Even without a Flight server, you can convert query results to Arrow RecordBatch for interop with Arrow-native tools (Polars, DataFusion, DuckDB):

```rust
use kyu_graph::to_record_batch;

let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();

if let Some(batch) = to_record_batch(&result) {
    println!("Schema: {:?}", batch.schema());
    println!("Rows: {}", batch.num_rows());

    // Pass to any Arrow-compatible library
    // write_parquet(&batch, "people.parquet");
}
```

## How Flight works

The protocol is simple:

1. Client sends a **Ticket** containing a Cypher query string
2. Server executes the query against the database
3. Server streams **RecordBatches** back over gRPC
4. Client assembles the batches into a table

```
Client                          Server
  │                               │
  │── GetFlightInfo(query) ──────>│
  │<── FlightInfo(schema, ep) ────│
  │                               │
  │── DoGet(ticket) ─────────────>│
  │<── RecordBatch stream ────────│
  │<── RecordBatch stream ────────│
  │<── (end of stream) ──────────│
```

Arrow's columnar format means data transfers are zero-copy when both sides use Arrow — no serialization/deserialization overhead.

## What you've learned

- Starting a Flight server from the CLI or from Rust
- Querying KyuGraph from Python via PyArrow
- Converting query results to Arrow RecordBatch for local interop
- How the Flight protocol works under the hood
