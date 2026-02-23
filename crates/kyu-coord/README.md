# kyu-coord

[![Crates.io](https://img.shields.io/crates/v/kyu-coord.svg)](https://crates.io/crates/kyu-coord)
[![docs.rs](https://img.shields.io/docsrs/kyu-coord)](https://docs.rs/kyu-coord)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/offbit-ai/kyugraph/blob/main/LICENSE)

Multi-tenant coordination layer for [KyuGraph](https://crates.io/crates/kyu-graph).

Provides a priority task queue, tenant registry, and worker pool for routing graph database queries across isolated tenants in cloud or server deployments.

```
Application / Agents
        |  submit Task
        v
   kyu-coord (Coordinator)
        |  routes by availability + tenant
   +----+----+
Worker Worker Worker     <-- stateless, scale by adding more
   +----+----+
        |  DeltaBatch / QueryResult
        v
   kyu-api (per-tenant Database)
        |
   kyu-storage / kyu-index / kyu-executor ...
```

## Quick Start

```toml
[dependencies]
kyu-coord = "0.1"
kyu-graph = "0.1"
```

### Register Tenants

```rust
use kyu_coord::{TenantRegistry, TenantConfig, TenantId};
use std::path::PathBuf;

let registry = TenantRegistry::new();

// Each tenant gets an isolated config: S3 bucket, cache dir, resource limits
registry.register(
    TenantId::new("acme-corp"),
    TenantConfig::new(
        "prod-bucket".into(),
        "acme-corp/db".into(),
        PathBuf::from("/nvme/cache/acme"),
    ),
);

// Update resource limits
let mut config = registry.get(&TenantId::new("acme-corp")).unwrap();
config.max_memory_bytes = 1024 * 1024 * 1024; // 1 GB
config.max_connections = 128;
registry.update(&TenantId::new("acme-corp"), config);
```

### Priority Task Queue

```rust
use kyu_coord::{TaskQueue, Task, TaskPriority, TenantId};
use std::sync::Arc;

let queue = Arc::new(TaskQueue::new());

// Submit queries with priority levels
queue.push(Task::new(
    TenantId::new("acme-corp"),
    "MATCH (n:User) RETURN count(n)".into(),
    TaskPriority::High,
));

queue.push(Task::new(
    TenantId::new("acme-corp"),
    "CALL algo.pageRank(0.85, 20, 0.000001)".into(),
    TaskPriority::Normal,
));

// High priority tasks are dequeued first; within the same priority, FIFO order
let next = queue.try_pop().unwrap();
assert_eq!(next.priority, TaskPriority::High);
```

### Worker Pool

```rust
use kyu_coord::{TaskQueue, WorkerPool, Task, TaskResult, TaskPriority, TenantId};
use std::sync::Arc;

let queue = Arc::new(TaskQueue::new());

// Spawn 4 worker threads that pull tasks from the shared queue
let pool = WorkerPool::new(4, queue.clone(), Arc::new(|task: Task| {
    // In practice: look up the tenant's Database, execute the query
    println!("[{}] executing: {}", task.tenant_id, task.query);
    TaskResult::Success {
        task_id: task.id,
        message: "done".into(),
    }
}));

// Submit work
queue.push(Task::new(
    TenantId::new("acme-corp"),
    "MATCH (n) RETURN n LIMIT 10".into(),
    TaskPriority::Normal,
));

// Graceful shutdown — drains in-flight tasks, then joins all threads
pool.shutdown();
```

## API Reference

### Tenant Management

| Type | Description |
|---|---|
| `TenantId` | String-wrapped tenant identifier |
| `TenantConfig` | Per-tenant config: S3 bucket/prefix, cache dir, memory budget, connection limit |
| `TenantRegistry` | Thread-safe registry (lock-free reads via DashMap) |

`TenantConfig` defaults: 512 MB memory budget, 64 max connections.

### Task Queue

| Type | Description |
|---|---|
| `TaskId` | Auto-incrementing unique task identifier |
| `TaskPriority` | `Low`, `Normal`, `High` — determines dequeue order |
| `Task` | Unit of work: tenant ID, query string, priority, submission timestamp |
| `TaskResult` | `Success { task_id, message }` or `Error { task_id, error }` |
| `TaskQueue` | Thread-safe priority queue with `push()`, `try_pop()`, `pop_blocking()` |

### Worker Pool

| Type | Description |
|---|---|
| `TaskHandler` | `dyn Fn(Task) -> TaskResult + Send + Sync` — per-task callback |
| `WorkerPool` | Spawns N named threads (`kyu-worker-0`, `kyu-worker-1`, ...) pulling from a shared queue |

## License

MIT — see [LICENSE](https://github.com/offbit-ai/kyugraph/blob/main/LICENSE) for details.
