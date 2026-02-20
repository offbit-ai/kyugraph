# KyuGraph — Final Architecture Recommendation

> *Inspired by Kyubi, the nine-tailed fox — nine distinct capabilities,
> one unified intelligence.*
>
> Pure Rust. No bottlenecks. Designed for agentic AI, high-throughput
> document workloads, and multi-tenant distributed computation.
> Embeddings stay external. Streaming is an ingestion concern, not a
> database concern.

---

## Project Origin

**KyuGraph is a pure Rust port of [RyuGraph](https://github.com/predictable-labs/ryugraph).**

RyuGraph is an embedded property graph database maintained by Predictable Labs —
itself a fork of [Kuzu](https://github.com/kuzudb/kuzu), originally developed at
the University of Waterloo and archived in October 2025. RyuGraph is the direct
upstream: ~450 KLOC of C++, CMake build system, ANTLR4 parser runtime, and
`third_party/` vendored C++ dependencies.

KyuGraph starts as a faithful, component-for-component port of that codebase.
Every architectural decision in this document — columnar CSR storage, vectorized
factorized execution, OCC transactions, HNSW vector index, FTS via tantivy,
the Cypher query language — is grounded in what RyuGraph already ships. We are
not redesigning the database; we are re-expressing it in a language that gives
us memory safety, a single cross-platform toolchain, and the Rust ecosystem,
then extending it with capabilities (delta fast path, cloud storage,
multi-tenant coordinator) that would be significantly harder to add cleanly
in C++.

Wherever this document refers to "the original codebase", "the C++ version",
or "unchanged from the Kuzu/RyuGraph design", it means **RyuGraph's C++ source**
at `github.com/predictable-labs/ryugraph`. All disk formats, on-wire protocols,
and public API semantics are preserved across the port — existing RyuGraph
databases open in KyuGraph without migration.

---

## Design Principles

Before the components: the decisions that shaped every choice below.

**1. The database owns the graph. Applications own the embeddings.**
KyuGraph stores, indexes, and queries vectors. It does not generate them.
Model selection is the application's concern — baking it into the engine
couples a data infrastructure decision to a rapidly-changing ML ecosystem.
The boundary is clean: the application writes `FLOAT[N]` columns; KyuGraph
indexes them with HNSW and queries them with `vector_similarity()`.

**2. Streaming is an ingestion architecture, not a database mode.**
KyuGraph does not become a streaming engine. It becomes a fast, delta-aware
sink that high-throughput ingestion pipelines write into. The database exposes
the right primitives (delta API, micro-batch writes, Arrow Flight output);
the streaming topology lives outside it.

**3. Every unsafe block is a deliberate, auditable decision.**
Only `kyu-storage` uses `unsafe`. Everything else — parser, planner, executor,
all bindings — is provably safe Rust. This is not a policy constraint; it is
an architectural boundary enforced by the crate graph.

**4. OCC is right for reads, wrong for high-frequency writes.**
The existing serializable OCC model is kept for analytical query transactions.
A separate upsert-optimised fast path bypasses OCC for idempotent delta writes,
where the semantics are "last write wins" rather than "abort on conflict."

**5. Distribution is a computation concern, not a storage concern.**
KyuGraph does not become a distributed database. It becomes a fast convergence
point for distributed workers. Multiple workers process independent work in
parallel — different files, document batches, agent sessions — and their outputs
converge into the unified graph via `apply_delta`. The coordinator/worker layer
sits entirely above the public API and requires no internal changes to the engine.

**6. The pod is a cache. Object storage is the database.**
In cloud deployments, pod-local disk (NVMe/SSD) is a write-through cache tier.
Object storage (S3/GCS/Azure Blob) is the durable source of truth. Pod death,
reschedule, or scale-out is a cache cold-start, not data loss. Storage capacity
is an S3 billing question — it expands infinitely with zero configuration.
The `PageStore` trait is the seam that makes this transparent to the engine.

---

## 1. Why Pure Rust Will Not Be Slower

| C++ mechanism | Rust equivalent | Notes |
|---|---|---|
| Manual allocator control | `tikv-jemallocator` as global allocator | Drop-in jemalloc; matches tcmalloc performance |
| Zero-cost abstractions | Zero-cost abstractions | Identical — same LLVM backend |
| SIMD intrinsics | `std::arch` / `safe_arch` | `_mm256_fmadd_ps` on AVX2, `vfmaq_f32` on NEON |
| Raw pointer arithmetic | `unsafe` in `kyu-storage` only | Isolated, auditable, same codegen |
| `mmap` | `memmap2` | Same OS syscall |
| Custom thread pool | `rayon` | Morsel-driven parallelism maps 1:1 |
| ANTLR4 C++ runtime | `chumsky` + `ariadne` | Combinator speed + compiler-quality diagnostics |
| `pybind11` | `pyo3` | Faster, no GIL management bugs |

RyuGraph's C++ codebase is ~450 KLOC. A faithful Rust port lands at
**~65–70% of the line count** while matching or beating analytical performance
due to Rust's stronger alias information enabling better autovectorization.

---

## 2. Crate Workspace

```
kyugraph/
├── Cargo.toml
│
├── crates/                         ← database engine (all shippable together)
│   ├── kyu-common/                 ← shared types, error taxonomy, IDs
│   ├── kyu-types/                  ← LogicalType, PhysicalType, Value, SSO strings
│   ├── kyu-catalog/                ← schema, table/property metadata, DDL
│   ├── kyu-storage/                ← buffer manager, columns, CSR, WAL  [unsafe boundary]
│   │   └── src/page_store/
│   │       ├── local.rs            ← LocalPageStore (default, embedded)
│   │       ├── remote.rs           ← RemotePageStore (S3/GCS/Azure backend)
│   │       └── cache.rs            ← DiskCache (write-through NVMe cache tier)
│   ├── kyu-transaction/            ← MVCC, shadow pages, undo log, checkpoint
│   ├── kyu-delta/                  ← GraphDelta, DeltaBatch, upsert fast path
│   ├── kyu-index/                  ← hash index, HNSW + staging buffer, FTS writer
│   ├── kyu-parser/                 ← chumsky lexer + Cypher parser → AST
│   ├── kyu-binder/                 ← semantic analysis, name → ID resolution
│   ├── kyu-planner/                ← logical plan, DP optimizer, vector-first rule
│   ├── kyu-executor/               ← physical operators, morsel scheduler
│   ├── kyu-expression/             ← expression bytecode, function registry
│   ├── kyu-copy/                   ← COPY FROM CSV/Parquet/Arrow/JSON + Kafka consumer
│   ├── kyu-extension/              ← dlopen-based extension loader
│   ├── kyu-api/                    ← public Database/Connection/QueryResult + Arrow Flight
│   ├── kyu-coord/                  ← coordinator, worker pool, task queue, tenant registry
│   └── kyu-cli/                    ← interactive shell (reedline)
│
├── extensions/                     ← pre-bundled, statically linked in release
│   ├── ext-algo/                   ← PageRank, BFS, betweenness, WCC
│   ├── ext-vector/                 ← HNSW vector index
│   ├── ext-fts/                    ← full-text search via tantivy
│   └── ext-json/                   ← JSON path functions
│
├── bindings/
│   ├── python/                     ← PyO3
│   ├── nodejs/                     ← napi-rs
│   └── java/                       ← jni-rs
│
└── ops/                            ← cloud deployment (not compiled into the binary)
    ├── helm/                       ← Helm chart for Kubernetes
    └── terraform/                  ← S3 bucket, IAM roles, Kinesis stream
```

`kyu-storage` is the only `unsafe` crate. The crate graph enforces this —
no other crate has a direct dependency on raw memory operations.
`kyu-coord` sits entirely on top of `kyu-api` and touches nothing below it.
The `PageStore` trait inside `kyu-storage` is the only boundary that changes
between embedded and cloud deployments — everything above it is unaffected.

---

## 3. Core Engine Components

### 3.1 Type System — `kyu-types`

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PhysicalType {
    Bool,
    Int8, Int16, Int32, Int64, Int128,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    Interval,       // 16 bytes: months + days + micros
    InternalId,     // (table_id: u64, offset: u64)
    String,         // SSO: inline if len ≤ 12, otherwise heap ptr
    List,           // (size: u32, child_offset: u64)
    FloatArray,     // FLOAT[N] — vector columns
    Struct,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalType {
    Any,
    Bool,
    Int8 | Int16 | Int32 | Int64 | Int128,
    UInt8 | UInt16 | UInt32 | UInt64,
    Float | Double,
    Date | Timestamp | TimestampNs | Interval,
    String | Blob,
    UUID, Serial,
    Node, Rel, InternalId,
    List(Box<LogicalType>),
    Array { element: Box<LogicalType>, size: u64 },   // fixed-size — used for embeddings
    Struct(Vec<(SmolStr, LogicalType)>),
    Map   { key: Box<LogicalType>, value: Box<LogicalType> },
    Union (Vec<(SmolStr, LogicalType)>),
    Decimal { precision: u8, scale: u8 },
}
```

`Value` is a 32-byte untagged union behind a `PhysicalType` discriminant.
No heap allocation for any scalar. Strings ≤12 bytes are stored inline.

---

### 3.2 Buffer Manager — `kyu-storage::buffer`

Split into two independently sized pools to prevent write-heavy ingestion
from evicting pages needed by concurrent analytical reads.

```rust
pub const PAGE_SIZE: usize = 4096;

pub struct Frame {
    data:      *mut u8,       // raw page bytes — unsafe boundary
    page_id:   AtomicU64,
    pin_count: AtomicU32,
    dirty:     AtomicBool,
    latch:     RwLatch,       // 8-byte atomic RW latch; readers spin, writers futex-park
}

pub struct BufferManager {
    read_pool:    Pool,       // 70% of configured memory — LRU, analytical queries
    write_pool:   Pool,       // 30% — WAL staging, shadow pages, HNSW construction
    page_table:   DashMap<PageId, (PoolId, FrameIdx)>,
    file_handles: Slab<FileHandle>,
}
```

The split pool is the single most important change for mixed workloads.
Without it, a 100K docs/hr ingestion run will thrash the read pool and
degrade concurrent agent queries.

---

### 3.3 PageStore — `kyu-storage::page_store`

The abstraction that makes embedded and cloud deployments identical from the
engine's perspective. Every page fetch and write goes through this trait —
the buffer manager never knows whether it is talking to a local file or S3.

```rust
pub trait PageStore: Send + Sync {
    fn read_page(&self, id: PageId) -> Result<Box<[u8; PAGE_SIZE]>>;
    fn write_page(&self, id: PageId, page: &[u8; PAGE_SIZE]) -> Result<()>;
    fn flush_wal(&self, records: &[WalRecord]) -> Result<Lsn>;
    fn read_wal_from(&self, lsn: Lsn) -> Result<impl Iterator<Item = WalRecord>>;
    fn write_checkpoint(&self, lsn: Lsn, manifest: &CheckpointManifest) -> Result<()>;
    fn latest_checkpoint(&self) -> Result<Option<(Lsn, CheckpointManifest)>>;
}

/// Default — embedded deployment, local disk.
pub struct LocalPageStore {
    root: PathBuf,
}

/// Cloud deployment — S3/GCS/Azure Blob as source of truth,
/// pod-local NVMe as write-through cache.
pub struct RemotePageStore {
    bucket:      String,
    prefix:      String,           // per-tenant: "tenants/{tenant_id}/"
    client:      Arc<aws_sdk_s3::Client>,
    local_cache: Arc<DiskCache>,
}

/// Write-through cache layer — sits in front of RemotePageStore.
/// Dirty pages are written to both local disk and object storage
/// before the write is acknowledged.
pub struct DiskCache {
    root:     PathBuf,             // pod-local NVMe mount
    capacity: u64,                 // evict LRU pages when full
    policy:   DurabilityPolicy,
}

pub enum DurabilityPolicy {
    /// Wait for S3 ACK before returning — no data loss on pod death.
    Strong,
    /// Return after local write; flush S3 asynchronously.
    /// Pod death risks losing the unflushed window.
    /// Acceptable when delta batches are cheaply replayable.
    Async { flush_interval: Duration },
}
```

`BufferManager` is generic over `PageStore`:

```rust
pub struct BufferManager<S: PageStore> {
    store:      S,
    read_pool:  Pool,
    write_pool: Pool,
    page_table: DashMap<PageId, (PoolId, FrameIdx)>,
    file_handles: Slab<FileHandle>,
}
```

Ship with `LocalPageStore`. Switch to `RemotePageStore` for cloud — the rest
of the engine, all the way up through the executor and API, does not change.

---

### 3.4 Columnar Storage — `kyu-storage::column`

```rust
pub struct ColumnChunk<T: PhysicalValue> {
    data:      PageRange,
    null_mask: NullMask,       // packed u64 — 64 rows per word, SIMD-checkable
    num_rows:  u32,
    stats:     ChunkStats<T>, // min/max for filter pushdown at scan time
}

pub struct NodeGroup {
    chunks:         Vec<ErasedColumnChunk>,  // one per property
    version_record: VersionRecord,           // MVCC delta chain head
}

pub struct CsrList {
    offset: ColumnChunk<u64>,
    nbr_id: ColumnChunk<InternalId>,
    rel_id: ColumnChunk<InternalId>,
}
```

Forward and backward CSR directions both stored — unchanged from Kuzu design.
`FLOAT[N]` properties get their own `ColumnChunk<f32>` with a fixed stride
of `N * 4` bytes per row — no indirection, cache-friendly HNSW distance scans.

---

### 3.5 WAL & Transactions — `kyu-transaction`

```rust
pub enum WalRecord {
    PageUpdate    { page_id: PageId, before: Box<[u8; PAGE_SIZE]>, after: Box<[u8; PAGE_SIZE]> },
    CatalogUpdate { old: CatalogEntry, new: CatalogEntry },
    Commit        { txn_id: u64 },
    Rollback      { txn_id: u64 },
    Checkpoint    { lsn: u64 },
}

pub struct Transaction {
    id:        u64,
    start_ts:  u64,
    write_set: Vec<(PageId, ShadowPage)>,  // copy-on-write shadow pages
    undo_log:  Vec<UndoRecord>,
    state:     TxnState,                   // Active | Committed | Aborted
}
```

MVCC design: reads snapshot at `start_ts`; writers copy-on-write into shadow
pages, merged on commit; serializable isolation via OCC validation.

This model is **kept for analytical transactions**. The delta fast path
(§3.6 below) bypasses it entirely for high-frequency upserts.

**Cloud WAL backend:**

In cloud deployments the WAL is replaced by a managed append-only log
(Kinesis, Google Pub/Sub, or self-hosted Redpanda) rather than a local file.
This decouples write acknowledgment from S3 page-flush latency entirely.

```rust
pub struct CloudWal {
    local_ring: MmapMut,          // fast local ring buffer — pod-local NVMe
    remote:     RemoteLogClient,  // Kinesis / Pub/Sub / Redpanda
    policy:     DurabilityPolicy, // Strong (wait for remote ACK) or Async
}

impl CloudWal {
    /// Strong: waits for remote log ACK — no data loss on pod death.
    /// Async: returns after local ring write; remote flush is background.
    /// Use Async when delta batches are cheaply replayable (most ingestion).
    pub fn append(&self, records: &[WalRecord]) -> Result<Lsn> { … }
}
```

**Checkpointer:**

Checkpoints bound WAL replay time on pod restart. Without them a cold pod
replays the entire log from scratch. With 5-minute checkpoints the worst-case
restart cost is 5 minutes of replay.

```rust
pub struct Checkpointer {
    store:        Arc<dyn PageStore>,
    wal:          Arc<CloudWal>,
    interval:     Duration,     // default: 5 minutes
    min_wal_size: u64,          // only checkpoint if WAL grown > 256 MB
}

impl Checkpointer {
    pub async fn run(self) {
        loop {
            tokio::time::sleep(self.interval).await;

            // 1. Flush all dirty pages from write pool to object storage
            let dirty = self.flush_dirty_pages().await?;

            // 2. Write checkpoint manifest to object storage
            let lsn = self.wal.current_lsn();
            self.store.write_checkpoint(lsn, &CheckpointManifest {
                lsn,
                dirty_page_count: dirty,
                catalog_version:  self.catalog.version(),
                timestamp:        SystemTime::now(),
            }).await?;

            // 3. Truncate WAL before this LSN — no longer needed for recovery
            self.wal.truncate_before(lsn).await?;
        }
    }
}
```

For the document ingestion workload (100K docs/hr ≈ 28 docs/sec), 5 minutes
of WAL is ~8,400 delta batches — trivially fast to replay on restart.

---

### 3.6 Delta Write Fast Path — `kyu-delta`

The critical addition for both target workloads. Provides conflict-free
idempotent upserts that bypass OCC validation when semantics are
"last write wins" — exactly what code graph diffs and document ingestion need.

```rust
pub enum GraphDelta {
    UpsertNode {
        key:    NodeKey,           // user-defined stable identifier
        labels: Vec<SmolStr>,
        props:  HashMap<SmolStr, Value>,
    },
    UpsertEdge {
        src: NodeKey, rel: SmolStr, dst: NodeKey,
        props: HashMap<SmolStr, Value>,
    },
    DeleteNode { key: NodeKey },
    DeleteEdge { src: NodeKey, rel: SmolStr, dst: NodeKey },
}

pub struct DeltaBatch {
    source:       SmolStr,    // "file:src/main.rs", "doc:invoice_12345", "commit:abc123"
    timestamp:    u64,        // used to resolve last-write-wins conflicts
    deltas:       Vec<GraphDelta>,
    vector_clock: Option<VectorClock>, // causal ordering between workers — None by default
}

impl Connection {
    /// Bypasses OCC. Safe only when upsert semantics are acceptable.
    /// All deltas in one batch commit atomically via WAL append.
    pub fn apply_delta(&self, batch: DeltaBatch) -> Result<DeltaStats> { … }
}
```

`vector_clock` is `None` by default — zero overhead for all single-tenant and
single-worker cases. Set it only when workers have causal dependencies (Worker B
must not apply its delta before Worker A's delta is visible). For the vast
majority of ingestion workloads — document batches, independent file diffs —
it is never needed. `timestamp` alone handles last-write-wins convergence.

`apply_delta` pins affected node groups, applies upserts directly to the
columnar store, and appends a single WAL record for the entire batch.
No shadow page copying, no validation phase. Throughput is limited only
by WAL append speed and buffer manager pin/unpin overhead.

**Hot tier for absorbing bursts:**

```rust
pub struct HotTier {
    queue:    ArrayQueue<GraphDelta>,   // crossbeam lock-free ring buffer
    capacity: usize,                   // e.g. 50_000 deltas
}
```

A background drain thread flushes the hot tier to `apply_delta` every
N events or T milliseconds (configurable). Queries union-scan both the
hot tier (linear scan, fast for small N) and the cold columnar store.

---

### 3.7 Cypher Parser — `kyu-parser`

Replaces ANTLR4 with **chumsky** — a combinator parser that compiles
directly into the call stack with no grammar VM overhead, paired with
**ariadne** for compiler-quality diagnostic rendering.

```toml
chumsky = { version = "0.9", features = ["label"] }
ariadne = "0.4"
```

**Comparison:**

| | pest | nom | chumsky |
|---|---|---|---|
| Runtime speed | PEG VM overhead | ✅ Fast | ✅ Fast |
| Error recovery | ✗ | Manual | ✅ `recover_with` built-in |
| Labelled spans | ✗ | Manual | ✅ `.labelled("node pattern")` |
| Rendered diagnostics | ✗ | ✗ | ✅ via ariadne |
| Multi-error reporting | ✗ | ✗ | ✅ single pass |

A hand-written `Lexer` produces `Vec<Spanned<Token>>`; the chumsky layer
works on the token stream. Separating lex from parse gives precise byte
positions for every error span.

```rust
pub fn statement_parser() -> impl Parser<Token, Statement, Error = Simple<Token>> {
    choice((
        match_clause(),
        create_clause(),
        merge_clause(),
        delete_clause(),
        set_clause(),
        call_clause(),
    ))
    .then_ignore(end())
}

fn match_clause() -> impl Parser<Token, Statement, Error = Simple<Token>> {
    just(Token::Match)
        .ignore_then(
            pattern_parser()
                .recover_with(skip_then_retry_until([
                    Token::Where, Token::Return, Token::With, Token::Semicolon,
                ]))
                .labelled("match pattern")
                .separated_by(just(Token::Comma))
                .at_least(1)
        )
        .then(where_clause().or_not())
        .map(|(patterns, where_)| Statement::Match(Match { patterns, where_ }))
}
```

Error output in the CLI:

```
Error: unexpected token
  --> query.cyp:1:28
   |
 1 | MATCH (a:Person)-[:KNOWS->(b) RETURN a.name
   |                         ^ expected ']' to close relationship type
```

---

### 3.8 Binder — `kyu-binder`

Semantic analysis: string names → integer IDs, type inference, semantic rewrites.

```rust
pub struct Binder<'cat> {
    catalog: &'cat Catalog,
    scope:   Vec<Scope>,            // lexical scopes for WITH/RETURN chains
    txn:     &'cat Transaction,
    passes:  Vec<Box<dyn BinderPass>>,  // extension point — empty by default
}
```

`passes` is empty in single-tenant embedded use — zero overhead. The coordinator
injects a `TenantFilter` pass when creating a multi-tenant connection, so every
query is automatically scoped to that tenant without the application needing to
add predicates manually.

```rust
/// Implemented by anything that rewrites a BoundStatement before planning.
pub trait BinderPass: Send + Sync {
    fn rewrite(&self, stmt: BoundStatement) -> BoundStatement;
}

/// Injected by kyu-coord for each tenant connection.
/// Rewrites every node/edge scan to include a _tenant predicate.
///
/// MATCH (f:Function) 
///   → MATCH (f:Function {_tenant: $tenant_id})
pub struct TenantFilter {
    tenant_id: TenantId,
}

impl BinderPass for TenantFilter {
    fn rewrite(&self, stmt: BoundStatement) -> BoundStatement {
        inject_tenant_predicate(stmt, self.tenant_id)
    }
}
```

`TenantFilter` is the only change required in `kyu-binder` for full multi-tenant
support. It is defined in `kyu-coord` and injected via the public API — the binder
itself has no knowledge of tenancy.

`Scope` is a stack-allocated `ArrayVec<(SmolStr, ExpressionId), 64>` —
no heap allocation for queries with ≤64 variables in scope.

---

### 3.9 Planner & Optimizer — `kyu-planner`

Dynamic-programming join optimizer (identical strategy to Kuzu) plus one
new rewrite rule for the agentic query shape.

**Vector-first rewrite rule:**

When a `Filter` contains `vector_similarity(col, $vec) > threshold` and
an HNSW index exists on `col`, rewrite the plan to run HNSW first rather
than full table scan + filter:

```rust
pub struct VectorFirstRewriteRule;

impl OptimizerRule for VectorFirstRewriteRule {
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        // Pattern: ScanNode → Filter(vector_similarity > t)
        // Rewrite: HnswScan(k=topK, ef=search_ef) → Filter(remaining predicates)
        match plan {
            LogicalPlan::Filter { input, predicate }
                if contains_vector_predicate(&predicate) =>
            {
                let (vec_pred, rest) = split_vector_predicate(predicate);
                Some(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::HnswScan {
                        table: extract_table(&input),
                        col:   vec_pred.col,
                        query: vec_pred.query_param,
                        k:     estimate_topk(&rest),
                    }),
                    predicate: rest,
                })
            }
            _ => None,
        }
    }
}
```

This is the enabling optimisation for hybrid queries like:

```cypher
MATCH (f:Function)-[:calls]->(g:Function)<-[:modifies]-(c:Commit)
WHERE c.timestamp > $since
  AND vector_similarity(f.embedding, $query_vec) > 0.82
RETURN f.name, f.file, g.name
ORDER BY vector_similarity(f.embedding, $query_vec) DESC
LIMIT 20
```

Without this rule the planner would scan all Function nodes first.
With it, HNSW narrows candidates to ~50–200, then graph traversal
filters them — three orders of magnitude difference on large graphs.

---

### 3.10 Executor — `kyu-executor`

Vectorized, factorized, morsel-driven execution — unchanged from the
Kuzu design, translated 1:1 to Rust.

```rust
pub const VECTOR_CAPACITY: usize = 2048;

pub struct ValueVector {
    data_type: PhysicalType,
    data:      *mut u8,         // VECTOR_CAPACITY * sizeof(type) — unsafe boundary
    null_mask: [u64; 32],       // 2048 bits; 64 rows per word
    state:     VectorState,     // Flat | Dictionary | Sequence | Constant
    len:       u16,
}

pub struct MorselScheduler {
    thread_pool: rayon::ThreadPool,
    task_queue:  SegQueue<Morsel>,  // lock-free MPMC
}

pub trait Operator: Send {
    fn init(&mut self, ctx: &ExecutionContext) -> Result<()>;
    fn next(&mut self, output: &mut DataChunk) -> Result<OperatorState>;
    fn finalize(&mut self) -> Result<()>;
}
```

Key physical operators: `ScanNodeTable`, `ScanRelTable`, `HashJoinBuild`,
`HashJoinProbe`, `AggregateHash`, `Intersect` (factorized multi-way),
`Filter`, `Projection`, `OrderBy`, `Limit`, `HnswScan`, `CopyTo`.

No `Box<dyn Operator>` in the pipeline — operators are concrete structs
in an enum, dispatched via `match` that the compiler inlines aggressively.

---

### 3.11 HNSW Index with Staging Buffer — `ext-vector`

Two-phase construction eliminates write amplification during bulk ingestion.

```rust
pub struct HnswIndex {
    ef_construction: usize,
    m:               usize,           // max connections per layer (default: 16)
    layers:          Vec<Layer>,
    entry_point:     NodeId,
    vector_store:    ColumnChunk<f32>,
    dim:             u32,
}

/// Absorbs inserts during bulk ingestion; flushed to HnswIndex in batches.
pub struct HnswStagingBuffer {
    pending:  Vec<(InternalId, Vec<f32>)>,
    capacity: usize,                        // e.g. 10_000 vectors
}

impl HnswStagingBuffer {
    /// Queries search both live index and staging buffer transparently.
    pub fn search_merged(
        &self,
        live: &HnswIndex,
        query: &[f32],
        k: usize,
    ) -> Vec<(InternalId, f32)> {
        let mut results = live.search(query, k + self.pending.len(), 100);
        // SIMD cosine over staging buffer — fits in L2 cache at 10K vectors
        for (id, vec) in &self.pending {
            results.push((*id, simd_cosine(query, vec)));
        }
        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        results.truncate(k);
        results
    }

    /// When full: bulk-insert into HNSW (3–5× faster than incremental).
    pub fn flush(&mut self, index: &mut HnswIndex) { … }
}
```

SIMD distance kernels use `_mm256_fmadd_ps` (AVX2) and `vfmaq_f32` (NEON)
via `safe_arch` — no `unsafe` required in the extension crate.

---

### 3.12 Full-Text Search — `ext-fts`

tantivy with tuned commit cadence for high-throughput ingestion.

```rust
pub struct FtsIngestionWriter {
    writer:           tantivy::IndexWriter,
    uncommitted:      usize,
    last_commit:      Instant,
    commit_interval:  Duration,   // default: 30 seconds
    commit_threshold: usize,      // default: 5_000 documents
}

impl FtsIngestionWriter {
    pub fn add_document(&mut self, id: InternalId, fields: &[(&str, &str)]) -> Result<()> {
        // … add to writer …
        self.uncommitted += 1;
        if self.uncommitted >= self.commit_threshold
            || self.last_commit.elapsed() >= self.commit_interval
        {
            self.writer.commit()?;
            self.uncommitted = 0;
            self.last_commit = Instant::now();
        }
        Ok(())
    }
}
```

At 100K docs/hr with 5K-document commit batches: ~20 commits/hr.
Each commit triggers one segment merge — well within tantivy's
performance envelope. Per-document commits would trigger 100K merges/hr
and saturate I/O within minutes.

---

### 3.13 Ingestion Pipeline — `kyu-copy`

`kyu-copy` gains a Kafka/Redpanda consumer alongside its existing
file-format readers. The `DataReader` trait is the extension point:

```rust
pub trait DataReader: Send {
    fn schema(&self) -> &Schema;
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

// Existing
pub struct CsvReader     { … }
pub struct ParquetReader { … }
pub struct ArrowReader   { … }
pub struct JsonReader    { … }

// New
pub struct KafkaReader {
    consumer:      rdkafka::consumer::StreamConsumer,
    topic:         String,
    batch_size:    usize,
    offset_table:  String,   // KyuGraph node table storing committed offsets
}
```

Exactly-once Kafka semantics: partition offsets are stored as properties
on a `_KafkaOffset` node table and committed atomically with each
`apply_delta` batch in a single WAL record.

---

### 3.14 Arrow Flight Output — `kyu-api`

Enables streaming result consumption without polling — critical for
downstream consumers (ML pipelines, dashboards, other services).

```rust
use arrow_flight::FlightServiceServer;

pub struct KyuFlightService {
    db: Arc<DatabaseInner>,
}

impl FlightService for KyuFlightService {
    async fn do_get(&self, request: Request<Ticket>) -> Result<FlightStream> {
        let cypher = decode_ticket(request.into_inner())?;
        let result = self.db.query(&cypher).await?;
        // Stream RecordBatches over gRPC — zero-copy from ValueVector to Flight
        Ok(result.into_flight_stream())
    }
}
```

Arrow Flight uses gRPC and transfers Arrow `RecordBatch`es with zero
serialization overhead — the same in-memory columnar layout used by
the executor is transmitted directly over the wire.

---

### 3.15 Multi-Tenant Coordinator — `kyu-coord`

A lightweight crate sitting entirely above `kyu-api`. It has no access to
any internal crate — it only calls the public `Database` and `Connection` API.
~800 lines, pure safe Rust, zero `unsafe`.

**Tenant registry — two isolation models:**

```rust
pub struct TenantRegistry {
    tenants: DashMap<TenantId, Arc<Database>>,
    config:  TenantConfig,
}

impl TenantRegistry {
    /// Per-tenant database files — strong isolation, best for large tenants.
    pub fn get_or_create(&self, id: TenantId) -> Arc<Database> {
        self.tenants.entry(id).or_insert_with(|| {
            let path = self.config.data_dir.join(id.to_string());
            Arc::new(Database::new(path, self.config.db_config.clone()).unwrap())
        }).clone()
    }

    /// Shared database with automatic tenant predicate injection —
    /// best for many small tenants where per-tenant buffer pools waste memory.
    pub fn connection_for(&self, db: &Database, id: TenantId) -> Connection {
        let mut conn = db.connect();
        conn.add_binder_pass(Box::new(TenantFilter { tenant_id: id }));
        conn
    }
}
```

**Coordinator and task queue:**

```rust
pub enum Task {
    IngestFiles {
        tenant_id:  TenantId,
        paths:      Vec<PathBuf>,
        source_tag: SmolStr,
    },
    IngestKafkaBatch {
        tenant_id:  TenantId,
        topic:      SmolStr,
        partition:  i32,
        offsets:    Range<i64>,
    },
    RunQuery {
        tenant_id:  TenantId,
        cypher:     String,
        params:     Vec<(SmolStr, Value)>,
        reply_to:   oneshot::Sender<QueryResult>,
    },
}

pub struct Coordinator {
    worker_pool: WorkerPool,
    tenant_reg:  Arc<TenantRegistry>,
    task_queue:  Arc<TaskQueue>,          // bounded mpsc — backpressure built in
}

impl Coordinator {
    pub async fn submit(&self, task: Task) -> TaskHandle { … }
    pub async fn submit_batch(&self, tasks: Vec<Task>) -> Vec<TaskHandle> { … }
}
```

**Workers — stateless, interchangeable:**

```rust
pub struct Worker {
    id:       WorkerId,
    task_rx:  mpsc::Receiver<Task>,
    reg:      Arc<TenantRegistry>,
}

impl Worker {
    pub async fn run(mut self) {
        while let Some(task) = self.task_rx.recv().await {
            match task {
                Task::IngestFiles { tenant_id, paths, source_tag } => {
                    let db = self.reg.get_or_create(tenant_id);
                    let conn = db.connect();
                    let batch = parse_files_to_delta(&paths, &source_tag).await;
                    conn.apply_delta(batch).unwrap();
                }
                Task::RunQuery { tenant_id, cypher, params, reply_to } => {
                    let db = self.reg.get_or_create(tenant_id);
                    let conn = self.reg.connection_for(&db, tenant_id);
                    let result = conn.execute_with_params(&cypher, &params).unwrap();
                    let _ = reply_to.send(result);
                }
                // …
            }
        }
    }
}
```

Workers never communicate with each other. The convergence protocol is
`DeltaBatch` with `timestamp`-based last-write-wins — any two workers can
process overlapping data and the database resolves it correctly without
coordination between them.

**System topology:**

```
Application / Agents
        │  submit Task
        ▼
   kyu-coord (Coordinator)
        │  routes by availability + tenant
   ┌────┼────┐
Worker Worker Worker     ← stateless, scale by adding more
   └────┼────┘
        │  DeltaBatch / QueryResult
        ▼
   kyu-api (per-tenant Database)
        │
   kyu-storage / kyu-index / kyu-executor …
```

---

### 3.16 Public API — `kyu-api`

```rust
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    pub fn new(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> { … }
    pub fn in_memory() -> Result<Self> { … }
}

pub struct Connection {
    db:  Arc<DatabaseInner>,
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl Connection {
    // Analytical queries — OCC transactions
    pub fn query(&self, cypher: &str) -> Result<QueryResult> { … }
    pub fn prepare(&self, cypher: &str) -> Result<PreparedStatement> { … }
    pub fn execute(&self, stmt: &PreparedStatement, params: &[(&str, Value)]) -> Result<QueryResult> { … }
    pub fn begin_transaction(&mut self, mode: TransactionMode) -> Result<()> { … }
    pub fn commit(&mut self) -> Result<()> { … }
    pub fn rollback(&mut self) -> Result<()> { … }

    // High-frequency writes — delta fast path, no OCC
    pub fn apply_delta(&self, batch: DeltaBatch) -> Result<DeltaStats> { … }
}

pub struct QueryResult {
    schema:  Schema,
    batches: Vec<RecordBatch>,   // Arrow in-memory format
}

impl IntoIterator for QueryResult {
    type Item = Vec<Value>;
    // …
}
```

---

## 4. Embedding Boundary

KyuGraph does not generate embeddings. It stores and queries them.

**What KyuGraph owns:**
- `FLOAT[N]` as a first-class column type
- HNSW index on any `FLOAT[N]` column
- `vector_similarity(col, $vec)` in Cypher
- Vector-first planner rewrite rule
- SIMD distance kernels (L2 and cosine)

**What lives outside KyuGraph:**
- Embedding model selection and inference
- Chunking strategy
- Entity extraction / NER
- Ingestion orchestration

The application writes precomputed vectors. How they were generated
is invisible to the database — the same position pgvector takes with Postgres.

**Optional future extension (v2, not now):**
A `CALL embed.generate(text, model)` loadable extension — strictly opt-in,
not in the core binary, for users who want the convenience.

---

## 5. Target Workload Fit

### Agentic Context Graph (Codebase)

The graph schema:
```
File → contains → Function
File → imports → File
Function → calls → Function
Function → references → Symbol
Chunk → embedding → FLOAT[1536]
Commit → modifies → File
```

Key requirements and how they're met:

| Requirement | Solution |
|---|---|
| Incremental file edits, not full re-ingest | `kyu-delta` upsert fast path |
| Low-latency agent queries (hybrid vector + graph) | Vector-first planner rule + HNSW staging buffer |
| OCC not appropriate for rapid file saves | `apply_delta` bypasses OCC |
| Multi-hop traversal + similarity in one query | Native in the executor — no joins across services |

### Document Analysis (100K docs/hr)

Numbers: 100K docs/hr = ~28 docs/sec = ~139 chunks/sec (5 chunks/doc avg).
The pipeline throughput is dominated by embedding generation upstream of
KyuGraph — the database itself is not the bottleneck at this scale.

| Requirement | Solution |
|---|---|
| Bulk vector inserts without HNSW write amplification | Two-phase HNSW staging buffer |
| FTS staying searchable without I/O saturation | Batched tantivy commits (30s / 5K docs) |
| Embedding ingestion from upstream pipeline | Arrow IPC or Kafka consumer via `kyu-copy` |
| Read pool not starved by write pressure | Split read/write buffer pools (70/30) |
| Exactly-once Kafka consumption | Offsets stored as graph properties, committed atomically |

---

## 6. Performance-Critical Decisions

**No `Arc` in the hot path.** Query execution uses scoped lifetimes (`'exec`)
to borrow from the buffer manager and catalog. No reference counting per tuple.

**Inline operator dispatch.** Physical operators are concrete structs in an
enum. Dispatch is a `match` that the compiler inlines — no vtable, no
`Box<dyn Operator>` indirection.

**SIMD null masks.** `[u64; 32]` in `ValueVector` enables 64-row-at-a-time
null checks with bitwise AND. `popcnt` counts non-null rows in one instruction
per word.

**Column-at-a-time filter evaluation.** `Filter` writes a selection vector
(`u16` indices) rather than copying rows. All downstream operators consume
the selection vector — no data movement until `Projection` materialises output.

**String interning.** All property names and labels are interned via
`string-interner` → `u32` IDs. Hot-path comparisons become integer compares.

**Prefetching.** `ScanNodeTable` issues `_mm_prefetch` for the next page
group while processing the current one — same strategy as Kuzu's `ColumnChunk::scan`.

**Split buffer pools.** Read pool (LRU, 70%) for analytical queries.
Write pool (30%) for WAL staging, shadow pages, and HNSW construction.
Bulk ingestion cannot evict pages needed by concurrent agent queries.

---

## 7. Third-Party Crates

| Crate | Purpose |
|---|---|
| `tikv-jemallocator` | Global allocator — replaces tcmalloc |
| `memmap2` | Memory-mapped I/O |
| `rayon` | Morsel-driven parallelism |
| `tokio` | Async I/O for Kafka consumer, Arrow Flight server |
| `dashmap` | Concurrent page table |
| `crossbeam` | Lock-free queues (hot tier ring buffer), epoch GC |
| `arrow` + `parquet` | RecordBatch, IPC, Parquet, CSV readers |
| `tantivy` | Full-text search |
| `chumsky` | Cypher parser combinators |
| `ariadne` | Diagnostic rendering |
| `rdkafka` | Kafka/Redpanda consumer |
| `arrow-flight` | Streaming query output over gRPC |
| `aws-sdk-s3` | S3/GCS-compatible object storage backend |
| `aws-config` | AWS credential and region resolution |
| `uuid` | Tenant IDs and task IDs (`v7` — time-sortable) |
| `libloading` | Extension loader (`dlopen`) |
| `reedline` | CLI readline |
| `pyo3` | Python bindings |
| `napi` | Node.js bindings |
| `jni` | Java bindings |
| `smol_str` | SSO strings for property names and labels |
| `bumpalo` | Arena allocator — query lifetime scoped |
| `roaring` | Compressed bitmaps for null masks and filters |
| `safe_arch` | SIMD distance kernels — no `unsafe` needed |
| `string-interner` | Label and property name interning |

---

## 8. Migration Strategy

KyuGraph is ported from **RyuGraph's C++ codebase**
(`github.com/predictable-labs/ryugraph`). RyuGraph already ships Rust
bindings (1.2% of the repo, `test-rust-local.sh`), so the FFI bridge exists
from day one. Migration is incremental — KyuGraph stays shippable and
RyuGraph-compatible at every phase. No existing RyuGraph database files
require migration at any point; the on-disk format is preserved throughout.

**Phase 1 — Types + Parser (Weeks 1–4)**
Port `kyu-types`, `kyu-common`, `kyu-parser`. Test the chumsky parser against
the full Cypher test corpus in `test/`. Replace ANTLR4 call sites via `extern "C"`.

**Phase 2 — Delta API + Catalog + Buffer Manager (Weeks 5–9)**
Port `kyu-delta`, `kyu-catalog`, `kyu-storage::buffer`. The split pool
and hot tier land here. Buffer manager replaces C++ behind a thin shim.

**Phase 3 — Storage Columns + Indexes (Weeks 10–15)**
Port column chunks, CSR lists, hash index, HNSW staging buffer.
Disk formats stay identical — no data migration.

**Phase 4 — Transaction Engine (Weeks 16–20)**
Port WAL + MVCC. Riskiest phase — run full ACID test suite continuously.
`apply_delta` fast path verified against the test corpus.

**Phase 5 — Expression + Executor (Weeks 21–30)**
Port the query pipeline. Vectorised operators are a direct translation.
Vector-first planner rule lands here. Morsel scheduling maps to rayon tasks.

**Phase 6 — Extensions + Bindings + Flight (Weeks 31–37)**
Port tantivy FTS (with commit cadence tuning), HNSW extension, graph algo.
Add Kafka consumer to `kyu-copy`. Add Arrow Flight server to `kyu-api`.
PyO3/napi-rs/jni-rs replace pybind11/node-addon-api/raw JNI.

**Phase 7 — Multi-Tenant Coordinator (Weeks 38–40)**
Build `kyu-coord` entirely on top of `kyu-api` — no internal access required.
`BinderPass` extension point in `kyu-binder` lands here (one field addition).
`TenantFilter` implemented and tested against multi-tenant query isolation suite.
`vector_clock` field on `DeltaBatch` verified in concurrent worker scenarios.

**Phase 8 — Cloud Storage Backend (Weeks 42–46)**
Implement `RemotePageStore` (S3 backend) and `DiskCache` (write-through NVMe
cache) inside `kyu-storage`. Implement `CloudWal` with Kinesis backend and
`DurabilityPolicy`. Implement `Checkpointer`. Ship Helm chart and Terraform
in `ops/`. All existing tests pass unchanged — `PageStore` trait is the only
interface that changed, and `LocalPageStore` is unmodified.

**Phase 9 — Delete C++ (Week 47)**
Remove CMakeLists.txt, ANTLR4 grammar runtime, all `third_party/` C++ vendoring.
Pure Cargo workspace. `cargo build --release` is the entire build system.

---

## 9. Cargo.toml Workspace Skeleton

```toml
[workspace]
resolver = "2"
members = [
    "crates/kyu-common",
    "crates/kyu-types",
    "crates/kyu-catalog",
    "crates/kyu-storage",
    "crates/kyu-transaction",
    "crates/kyu-delta",
    "crates/kyu-index",
    "crates/kyu-parser",
    "crates/kyu-binder",
    "crates/kyu-planner",
    "crates/kyu-executor",
    "crates/kyu-expression",
    "crates/kyu-copy",
    "crates/kyu-extension",
    "crates/kyu-api",
    "crates/kyu-coord",
    "crates/kyu-cli",
    "extensions/ext-algo",
    "extensions/ext-vector",
    "extensions/ext-fts",
    "extensions/ext-json",
    "bindings/python",
    "bindings/nodejs",
    "bindings/java",
]

[workspace.dependencies]
# Allocator
tikv-jemallocator  = "0.6"

# Storage / IO
memmap2            = "0.9"
bytes              = "1"

# Parallelism
rayon              = "1.10"
crossbeam          = "0.8"
dashmap            = "6"

# Arrow / Data
arrow              = { version = "53", features = ["simd"] }
parquet            = "53"
arrow-flight       = "53"

# Ingestion
rdkafka            = { version = "0.36", features = ["tokio"] }

# Cloud storage
aws-sdk-s3         = "1"
aws-config         = "1"

# Search
tantivy            = "0.22"

# Parser + Diagnostics
chumsky            = { version = "0.9", features = ["label"] }
ariadne            = "0.4"

# Async runtime
tokio              = { version = "1", features = ["full"] }

# Utilities
uuid               = { version = "1", features = ["v7"] }
smol_str           = "0.3"
bumpalo            = "3"
roaring            = "0.10"
hashbrown          = "0.15"
libloading         = "0.8"
string-interner    = "0.17"
safe_arch          = "0.7"
thiserror          = "2"
tracing            = "0.1"
serde              = { version = "1", features = ["derive"] }

# CLI
reedline           = "0.35"

# Bindings
pyo3               = { version = "0.22", features = ["extension-module"] }
napi               = "2"
jni                = "0.21"

[profile.release]
opt-level       = 3
lto             = "fat"
codegen-units   = 1
panic           = "abort"
strip           = "symbols"

[profile.release.build-override]
opt-level = 3
```

---

## 10. Cloud Deployment — Kubernetes

KyuGraph's cloud deployment model is built on one principle from design
principle §6: **the pod is a cache, object storage is the database**.
Pod death, reschedule, or scale-out is a cache cold-start — never data loss.

### Storage Architecture on k8s

```
Pod (StatefulSet replica)
│
├── BufferManager<RemotePageStore>
│   ├── read_pool  (70% of pod memory) ← hot pages, LRU eviction to S3
│   └── write_pool (30% of pod memory) ← WAL staging, shadow pages
│
├── DiskCache (pod-local NVMe PVC)     ← write-through cache to S3
│   └── /var/kyugraph/cache            ← ephemeral — can be lost safely
│
└── CloudWal                           ← Kinesis / Redpanda (durable)

Object Storage (S3 / GCS / Azure Blob)  ← source of truth, infinite capacity
│
├── tenants/{tenant_id}/pages/          ← column files, CSR pages
├── tenants/{tenant_id}/wal/            ← WAL segments (truncated after checkpoint)
└── tenants/{tenant_id}/checkpoints/    ← checkpoint manifests
```

### Kubernetes StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kyugraph
spec:
  replicas: 3                      # 1 writer + 2 read replicas per tenant group
  serviceName: kyugraph
  template:
    spec:
      containers:
      - name: kyugraph
        image: kyugraph:latest
        env:
        - name: KYUGRAPH_PAGE_STORE
          value: "s3://kyugraph-data/prod/"
        - name: KYUGRAPH_WAL_BACKEND
          value: "kinesis://kyugraph-wal-stream"
        - name: KYUGRAPH_DURABILITY
          value: "strong"          # or "async" for ingestion-only replicas
        - name: KYUGRAPH_CACHE_SIZE
          value: "32Gi"            # size to working set — determines hit rate
        - name: KYUGRAPH_CHECKPOINT_INTERVAL
          value: "300"             # seconds
        volumeMounts:
        - name: page-cache
          mountPath: /var/kyugraph/cache
        resources:
          requests:
            memory: "16Gi"
            cpu: "8"
          limits:
            memory: "64Gi"
            cpu: "32"
  volumeClaimTemplates:
  - metadata:
      name: page-cache
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-nvme  # local NVMe storage class
      resources:
        requests:
          storage: 500Gi           # cache only — not primary storage
```

The PVC is **cache only**. Its size determines cache hit rate, not data
capacity. If it fills, old pages are evicted to S3 — no data loss, just
more cache misses until the working set stabilises.

### Storage Expansion — Zero Downtime, Zero Data Loss

| Scenario | Action | Data loss risk |
|---|---|---|
| Need more storage capacity | S3 scales infinitely — no action needed | None |
| Improve cache hit rate | Resize PVC via `kubectl patch` | None — cache only |
| Add compute capacity | `kubectl scale statefulset --replicas=N` | None — new pod reads from S3 |
| Pod crashes / rescheduled | Restart, read latest checkpoint, replay WAL | None |
| PVC lost / corrupted | Restart, cold-read from S3 (slower, not lossy) | None |
| Tenant migration | Copy S3 prefix to new region/cluster | None |
| Tenant deletion | Delete S3 prefix | Intentional |

**PVC resize (zero downtime on most cloud providers):**

```bash
kubectl patch pvc page-cache-kyugraph-0 \
  -p '{"spec":{"resources":{"requests":{"storage":"1Ti"}}}}'
```

Requires `allowVolumeExpansion: true` on the StorageClass (default on EBS,
GCE PD, Azure Disk). The filesystem expands live — no pod restart, no
write interruption.

**Scale-out (read replicas):**

```bash
kubectl scale statefulset kyugraph --replicas=5
```

New pods start, read the latest checkpoint from S3 (~seconds for the
manifest, minutes for the working set to warm), and begin serving reads.
`kyu-coord`'s `TenantRegistry` routes new connections to the least-loaded pod.

### Multi-Tenant Object Storage Layout

```
s3://kyugraph-data/
  tenants/
    {tenant_a}/
      pages/       ← column files, one object per page group
      wal/         ← WAL segments, truncated after checkpoint
      checkpoints/ ← checkpoint manifests (LSN + page inventory)
    {tenant_b}/
      …
```

Per-tenant prefix isolation means:
- Storage cost attribution per tenant is exact (S3 billing by prefix)
- Tenant backup is a prefix copy — atomic, instant to initiate
- Tenant restore is a prefix copy to a new path — no downtime for other tenants
- Tenant deletion is a single `aws s3 rm --recursive` — complete and immediate

### WAL Durability Tradeoffs

| Policy | Write latency | Pod-death risk |
|---|---|---|
| `Strong` (wait for Kinesis ACK) | +5–15ms per commit | No data loss |
| `Async` (local ring only, background flush) | <1ms | Loses unflushed window on pod death |

Use `Strong` for analytical write transactions and agent graph mutations.
Use `Async` for bulk document ingestion where re-running the delta batch
from the upstream queue (Kafka/Kinesis) is cheaper than the latency cost.

### Terraform Provisioning

```hcl
# ops/terraform/main.tf

resource "aws_s3_bucket" "kyugraph_data" {
  bucket = "kyugraph-data-${var.env}"

  lifecycle_rule {
    id      = "archive-old-wal"
    enabled = true
    prefix  = "tenants/"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"   # move old WAL segments to cheaper tier
    }
  }
}

resource "aws_kinesis_stream" "kyugraph_wal" {
  name             = "kyugraph-wal-${var.env}"
  shard_count      = 4               # one shard per writer pod
  retention_period = 168             # 7 days — covers checkpoint intervals
}

resource "aws_iam_role" "kyugraph_pod" {
  # S3: GetObject, PutObject, DeleteObject on kyugraph-data/*
  # Kinesis: PutRecord, GetRecords on kyugraph-wal
}
```

---

## 11. What This Architecture Is and Is Not

**It is:**
- A pure Rust embedded property graph database with Cypher
- A fast, delta-aware convergence point for multi-tenant, multi-worker ingestion
- A vector + graph query engine — store precomputed embeddings, query them natively
- A streaming-adjacent system that accepts data from Kafka and serves results over Arrow Flight
- A multi-tenant platform via `kyu-coord` — per-tenant database isolation or shared database with automatic predicate injection
- A cloud-native database — pod-local disk is a cache, S3 is the truth, storage expands without configuration or data loss

**It is not:**
- An embedding generation system
- A streaming database (no continuous queries, no windowing, no watermarks)
- A distributed database — compute and data stay on one logical node; distribution happens in the coordinator/worker layer above the engine
- An HTAP system with a separate hot store — the delta fast path and split pools handle mixed workloads without that complexity

The boundary is deliberate. Everything outside it belongs in application code
or a separate service. Keeping KyuGraph focused is what makes it fast.

---

## The Nine Tails

The name Kyubi reflects nine distinct capabilities unified in one engine:

1. **Cypher query language** — expressive, familiar, full property graph model
2. **Disaggregated cloud storage** — S3 as source of truth, pod-local NVMe as write-through cache; capacity expands infinitely with zero configuration
3. **Vectorized factorized execution** — morsel-driven, SIMD, zero-copy
4. **HNSW vector index** — native embedding storage and similarity search
5. **Full-text search** — tantivy-backed BM25, batched commits
6. **Delta write fast path** — convergent upserts, bypasses OCC, powers multi-worker ingestion
7. **ACID transactions** — serializable OCC for analytical correctness
8. **Multi-tenant coordinator** — distributed computation converging into one graph
9. **Arrow Flight output** — zero-copy streaming results to any downstream consumer