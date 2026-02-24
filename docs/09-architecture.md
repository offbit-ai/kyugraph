# Understanding the Architecture

## What you'll learn

How KyuGraph processes a query from Cypher string to committed result, and how the 21 crates fit together. This helps you understand performance characteristics and debug issues.

## Step 1: Follow a query through the pipeline

When you call `conn.query("MATCH (p:Person) RETURN p.name")`, five stages execute:

### Parse

`kyu-parser` breaks the Cypher string into tokens (lexer) then builds an AST (parser):

```
"MATCH (p:Person) RETURN p.name"
    │
    ▼ Lexer
[MATCH, LPAREN, IDENT("p"), COLON, IDENT("Person"), RPAREN, RETURN, ...]
    │
    ▼ Parser (chumsky combinators)
Statement::Query {
    match_clause: Pattern { node: "p", label: "Person" },
    return_clause: [Expression::Property("p", "name")]
}
```

The parser uses error recovery — partial results with diagnostics rather than hard failures.

### Bind

`kyu-binder` resolves names against the catalog:

```
"Person" → table_id: 0
"p.name" → column_idx: 1 in table 0
```

This is where "unknown table" or "unknown property" errors surface.

### Plan

`kyu-planner` builds a logical plan and optimizes it:

```
LogicalPlan:
  Project [col_1]
    Scan table=0
```

The optimizer uses dynamic programming for join ordering and statistics-driven cardinality estimation.

### Execute

`kyu-executor` runs the physical plan using morsel-driven parallelism:

```
PhysicalPlan:
  ProjectionOperator [col_1]
    ScanOperator table=0, columns=[1]
```

Data flows through operators as columnar batches with selection vectors for predicate filtering.

### Commit

`kyu-transaction` handles durability:
- **Read queries**: no commit needed
- **Write queries**: WAL append → acknowledgment → periodic checkpoint

## Step 2: Understand the crate layers

The crates form a layered architecture — each layer depends only on layers below it:

```
┌─────────────────────────────────────┐
│  kyu-graph  kyu-cli                 │  User-facing
├─────────────────────────────────────┤
│  kyu-api                            │  Database + Connection
├─────────────────────────────────────┤
│  kyu-executor  kyu-planner          │  Query processing
│  kyu-binder    kyu-expression       │
├─────────────────────────────────────┤
│  kyu-parser                         │  Cypher parsing
├─────────────────────────────────────┤
│  kyu-catalog  kyu-copy  kyu-delta   │  Metadata + ingestion
│  kyu-extension  kyu-coord           │
├─────────────────────────────────────┤
│  kyu-storage  kyu-transaction       │  Storage engine
│  kyu-index                          │
├─────────────────────────────────────┤
│  kyu-types  kyu-common              │  Foundation
└─────────────────────────────────────┘
```

### Foundation layer

| Crate | What it provides |
|-------|------------------|
| `kyu-common` | Error types (`KyuError`), ID types, configuration |
| `kyu-types` | `LogicalType`, `PhysicalType`, `TypedValue`, `Interval` |

Every other crate depends on these two.

### Storage layer

| Crate | What it provides |
|-------|------------------|
| `kyu-storage` | Buffer manager, columnar NodeGroups, CSR adjacency lists |
| `kyu-transaction` | MVCC, write-ahead log, shadow pages, checkpointing |
| `kyu-index` | Hash index, HNSW vector index, FTS index writer |

This is the only layer with `unsafe` code — confined to memory-mapped I/O.

### Query processing layer

| Crate | What it provides |
|-------|------------------|
| `kyu-parser` | Lexer + chumsky combinator parser → AST |
| `kyu-binder` | Name resolution, semantic analysis |
| `kyu-planner` | Logical plan, DP join optimizer |
| `kyu-executor` | Physical operators, morsel-driven pipelines, optional JIT |
| `kyu-expression` | Expression bytecode, function registry |

## Step 3: Understand the type system

KyuGraph has a two-level type system matching the C++ upstream for on-disk compatibility:

### PhysicalType (storage layout)

```
Bool | Int8 | Int16 | Int32 | Int64 | Int128
UInt8 | UInt16 | UInt32 | UInt64
Float32 | Float64
Interval (16 bytes: months + days + micros)
InternalId (table_id: u64, offset: u64)
String (SSO: inline if ≤ 12 bytes, else heap pointer)
List | FloatArray | Struct
```

### LogicalType (query semantics)

Richer type hierarchy built on top of physical types:

```
Scalars: Bool, Int8..Int128, UInt8..UInt64, Float, Double
Temporal: Date, Timestamp, TimestampNs, Interval
Text: String, Blob, UUID
Graph: Node, Rel, InternalId
Compound: List(T), Array(T, size), Struct(fields), Map(K, V), Union(variants)
Numeric: Decimal(precision, scale), Serial
```

### TypedValue (runtime values)

32-byte untagged union — no heap allocation for scalars:

```rust
// Scalars: stack-allocated
TypedValue::Int64(42)
TypedValue::Double(3.14)
TypedValue::Bool(true)

// Strings: SSO (≤ 12 bytes inline, otherwise heap)
TypedValue::String(SmolStr::new("Alice"))

// Compound: heap-allocated
TypedValue::List(vec![TypedValue::Int64(1), TypedValue::Int64(2)])
```

## Step 4: Understand storage layout

On disk, each table is a **NodeGroup** containing **ColumnChunks**:

```
NodeGroup (Person table)
├── ColumnChunk 0: id     [1, 2, 3, ...]      (Int64)
├── ColumnChunk 1: name   ["Alice", "Bob", ...]  (String)
├── ColumnChunk 2: age    [30, 25, 28, ...]      (Int64)
└── SelectionVector: [true, true, false, ...]     (live/deleted)
```

- Columnar layout enables vectorized scans — process entire columns at once
- Selection vectors mark soft-deleted rows without rewriting data
- WAL captures mutations; checkpoints merge them into base data

## What you've learned

- The 5-stage query pipeline: Parse → Bind → Plan → Execute → Commit
- How the 21 crates layer on top of each other
- The two-level type system (Physical + Logical)
- Columnar storage with NodeGroups and selection vectors
