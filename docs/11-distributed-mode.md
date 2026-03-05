# Distributed Mode

## What you'll learn

How KyuGraph scales beyond a single process using worker pools, delta batches, cloud storage, and multi-tenant isolation — without becoming a distributed database.

## Design philosophy

KyuGraph is not a distributed database. It does not shard data, elect leaders, or run distributed transactions. Instead, it acts as a **fast convergence point** for independent workers that operate in parallel and merge results through conflict-free delta batches.

```
                 ┌──────────┐
                 │  Client  │
                 └────┬─────┘
                      │
              ┌───────┴───────┐
              │  kyu-coord    │  Task queue + tenant registry
              │  WorkerPool   │
              └───┬───┬───┬───┘
                  │   │   │
           ┌──────┘   │   └──────┐
           ▼          ▼          ▼
      ┌─────────┐ ┌─────────┐ ┌─────────┐
      │ Worker 0│ │ Worker 1│ │ Worker 2│   Stateless threads
      └────┬────┘ └────┬────┘ └────┬────┘
           │          │          │
           └──────────┼──────────┘
                      ▼
              ┌───────────────┐
              │   Database    │  Single convergence point
              │  apply_delta  │
              └───────┬───────┘
                      │
              ┌───────┴───────┐
              │  S3 / NVMe   │  Cloud-native storage
              └───────────────┘
```

Workers are stateless and interchangeable. They never communicate with each other. Convergence happens at the database layer via timestamp-ordered delta batches.

## Step 1: Set up a worker pool

The `kyu-coord` crate provides a priority task queue and thread pool:

```rust
use std::sync::Arc;
use kyu_coord::{Task, TaskQueue, TaskPriority, TaskResult, WorkerPool, TenantId};
use kyu_api::Database;

let db = Arc::new(Database::in_memory());
let queue = Arc::new(TaskQueue::new());

// Each worker pulls tasks and executes queries.
let db_ref = Arc::clone(&db);
let handler = Arc::new(move |task: Task| {
    let conn = db_ref.connect();
    match conn.query(&task.query) {
        Ok(_) => TaskResult::Success {
            task_id: task.id,
            message: "ok".into(),
        },
        Err(e) => TaskResult::Error {
            task_id: task.id,
            error: e.to_string(),
        },
    }
});

// Spawn 4 worker threads.
let pool = WorkerPool::new(4, Arc::clone(&queue), handler);

// Submit work.
queue.push(Task::new(
    TenantId::new("tenant-1"),
    "CREATE (p:Person {id: 1, name: 'Alice'})".into(),
    TaskPriority::Normal,
));

// Graceful shutdown when done.
pool.shutdown();
```

Tasks have three priority levels — `High`, `Normal`, `Low` — with FIFO ordering within each level.

## Step 2: Use delta batches for conflict-free ingestion

When multiple workers ingest data concurrently, use `DeltaBatch` instead of raw Cypher. Delta batches bypass OCC and use last-write-wins semantics:

```rust
use kyu_delta::{DeltaBatchBuilder, DeltaValue};

// Worker 1: ingest file A
let batch_a = DeltaBatchBuilder::new("file:src/main.rs", 1000)
    .upsert_node("Function", "main", vec![], [
        ("lines", DeltaValue::Int64(42)),
    ])
    .upsert_node("Function", "helper", vec![], [
        ("lines", DeltaValue::Int64(10)),
    ])
    .upsert_edge(
        "Function", "main",
        "calls",
        "Function", "helper",
        Vec::<(&str, DeltaValue)>::new(),
    )
    .build();

let conn = db.connect();
let stats = conn.apply_delta(batch_a).unwrap();
println!("Created {} nodes, {} edges", stats.nodes_created, stats.edges_created);
```

### Conflict resolution

If two workers upsert the same node, the batch with the **higher timestamp** wins:

```rust
// Worker 1 (timestamp 100): sets lines = 42
let batch1 = DeltaBatchBuilder::new("worker-1", 100)
    .upsert_node("Function", "main", vec![], [
        ("lines", DeltaValue::Int64(42)),
    ])
    .build();

// Worker 2 (timestamp 200): sets lines = 50 — this wins
let batch2 = DeltaBatchBuilder::new("worker-2", 200)
    .upsert_node("Function", "main", vec![], [
        ("lines", DeltaValue::Int64(50)),
    ])
    .build();
```

Both batches can be applied in any order. The result is always `lines = 50` because timestamp 200 > 100.

### Vector clocks for causal ordering

For workloads where wall-clock timestamps are insufficient (e.g., workers processing dependent data), attach a vector clock:

```rust
use kyu_delta::VectorClock;

let mut clock = VectorClock::new();
clock.set("worker-1", 5);
clock.set("worker-2", 3);

let batch = DeltaBatch::with_vector_clock("worker-1", 1000, clock);
```

## Step 3: Configure multi-tenant isolation

KyuGraph supports two tenant isolation models:

### Per-tenant databases (strong isolation)

Each tenant gets a separate `Database` instance with its own storage, buffer pool, and WAL:

```rust
use kyu_coord::{TenantRegistry, TenantId, TenantConfig};
use std::path::PathBuf;

let registry = TenantRegistry::new();

registry.register(
    TenantId::new("acme-corp"),
    TenantConfig::new(
        "kyugraph-prod".into(),          // S3 bucket
        "tenants/acme-corp/db".into(),   // S3 prefix
        PathBuf::from("/nvme/cache/acme"), // local cache
    ),
);

registry.register(
    TenantId::new("globex"),
    TenantConfig::new(
        "kyugraph-prod".into(),
        "tenants/globex/db".into(),
        PathBuf::from("/nvme/cache/globex"),
    ),
);

// Each tenant can have custom resource limits.
let mut config = registry.get(&TenantId::new("acme-corp")).unwrap();
// config.max_memory_bytes = 1024 * 1024 * 1024;  // 1 GB
// config.max_connections = 128;
```

### Shared database with tenant filter (memory-efficient)

For many small tenants, share a single database and inject tenant predicates automatically at the binder layer. Every node table includes a `_tenant` column, and queries are rewritten:

```
-- User writes:
MATCH (f:Function) RETURN f.name

-- Binder rewrites to:
MATCH (f:Function) WHERE f._tenant = $tenant_id RETURN f.name
```

This is zero-overhead for single-tenant deployments (empty binder pass list).

## Step 4: Enable cloud storage

### S3-backed page store

Replace local disk with S3 for elastic, durable storage. NVMe acts as a write-through cache:

```
┌─────────────┐     ┌──────────────┐     ┌─────────┐
│  Buffer Pool│────>│  NVMe Cache  │────>│   S3    │
│  (hot pages)│     │ (write-thru) │     │ (truth) │
└─────────────┘     └──────────────┘     └─────────┘
```

Object layout in S3:

```
s3://bucket/tenants/{tenant_id}/pages/file_{id}/page_{idx}
s3://bucket/tenants/{tenant_id}/wal/segment_{id}
```

Pod death or reschedule means a cold cache, not data loss. Storage scales without configuration changes.

### Cloud WAL replication

The transaction layer supports dual-write to local WAL and a remote sink for cross-AZ durability:

```
Transaction commit:
  1. Append to local WAL (fsync)     ← fast, always succeeds
  2. Push segment to remote sink     ← durable across AZ
```

The `WalSink` trait abstracts the remote backend. If the remote push fails, the local WAL retains the data for background catch-up.

## Step 5: Stream data from Kafka

For real-time ingestion, use `COPY FROM` with a Kafka URL:

```cypher
COPY Person FROM 'kafka://broker:9092/person_events?group_id=kyugraph'
```

Each Kafka message is a flat JSON object with keys matching column names. Missing keys become `NULL`. Messages are consumed in configurable batches.

## Step 6: Expose via Arrow Flight

Remote clients connect over gRPC using the Arrow Flight protocol:

```rust
use std::sync::Arc;
use kyu_graph::{Database, serve_flight};

#[tokio::main]
async fn main() {
    let db = Arc::new(Database::in_memory());
    // ... setup schema, register extensions ...
    serve_flight(db, "0.0.0.0", 50051).await.unwrap();
}
```

Any Arrow Flight client (Python, Java, Go) can then query:

```python
import pyarrow.flight as flight

client = flight.connect("grpc://kyugraph-host:50051")
reader = client.do_get(flight.Ticket(b"MATCH (p:Person) RETURN p.name"))
df = reader.read_all().to_pandas()
```

## Architecture summary

| Layer | Crate | Role |
|-------|-------|------|
| Coordination | `kyu-coord` | Task queue, worker pool, tenant registry |
| Convergence | `kyu-delta` | Conflict-free delta batches, vector clocks |
| Network | `kyu-api` (flight.rs) | Arrow Flight gRPC server |
| Durability | `kyu-transaction` | Cloud WAL with pluggable remote sink |
| Storage | `kyu-storage` | S3-backed page store with NVMe cache |
| Ingestion | `kyu-copy` | Kafka consumer, CSV/Parquet/Arrow IPC |

## What this is not

KyuGraph's distributed mode is intentionally limited. The following are **not in scope**:

| Feature | Why not |
|---------|---------|
| Distributed joins | Workers converge results at the database, not across nodes |
| Automatic sharding | Application-level partitioning (e.g., by file, tenant, or batch) |
| Leader election / Raft | Timestamp-based last-write-wins eliminates coordination |
| 2-phase commit | Delta batches are atomic within a single node |
| Cross-node replication | S3 is the shared durable store; replicas read from the same bucket |

This keeps the database simple and predictable. Scale-out complexity lives in the coordinator and worker layers above `kyu-api`, not inside the storage engine.

## What you've learned

- Setting up a worker pool with prioritized task queues
- Using delta batches for conflict-free concurrent ingestion
- Configuring multi-tenant isolation (per-tenant or shared)
- Enabling S3 storage with NVMe caching and cloud WAL
- Streaming from Kafka and exposing the database via Arrow Flight
- Why KyuGraph is a convergence point, not a distributed database
