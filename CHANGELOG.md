# Changelog

All notable changes to KyuGraph are documented in this file.

## [Unreleased]

### Changed

- Upgrade all five Cranelift dependencies from 0.128 to 0.136.2. Adapt memory
  flags, frontend finalization, and explicit signed-immediate instruction APIs.
- Make Cranelift JIT unconditional in executor and public/API builds, including
  `--no-default-features`. Every execution context has a bounded compilation
  cache, and database connections share compiled expressions. The old `jit`
  feature is a compatibility alias. KyuGraph now requires Rust 1.96 or newer.
- Replace outdated Apple Silicon fork instructions with the upstream version
  verified by the native JIT tests.

### Added

- **kyu-binder / kyu-api: `IF NOT EXISTS` support** — `CREATE NODE TABLE IF NOT
  EXISTS` and `CREATE REL TABLE IF NOT EXISTS` now work end-to-end. The
  `if_not_exists` flag is propagated from parser → binder → executor; when the
  table already exists, the statement silently succeeds instead of erroring.

- **kyu-api: Idempotent `LOAD FROM`** — `exec_load_from()` now checks for
  existing tables before creating them. Running `LOAD FROM 'file.ttl'` twice on
  the same file silently reuses existing node/rel tables instead of crashing
  with "already exists".

- **ext-rdf: Explicit XSD date/time → String mapping** — `xsd_to_logical_type()`
  now explicitly maps `xsd:date`, `xsd:dateTime`, `xsd:time`, `xsd:gYear`,
  `xsd:gYearMonth`, `xsd:gMonthDay`, and `xsd:duration` to `LogicalType::String`
  (ISO 8601). Previously these fell through to the catch-all; now the intent is
  documented in code.

## [0.3.0] — 2026-02-26

### Added

- **ext-rdf** — New extension crate for RDF Linked Data import following
  Linked Data Principles. Supported formats: Turtle (`.ttl`), N-Triples
  (`.nt`), N-Quads (`.nq`), RDF/XML (`.rdf`/`.owl`). Schema is inferred
  automatically from triples:
  - `rdf:type` → node table (local name as table name)
  - Literal-valued predicates → node properties (XSD datatype → LogicalType)
  - URI-valued predicates → relationship tables (hyperlinks as edges)
  - Subject URI stored as `STRING` primary key on every node table
  - Untyped subjects assigned to a `Resource` default table
  - Extension procedures: `CALL rdf.stats(path)`, `CALL rdf.prefixes(path)`,
    `CALL rdf.types(path)`

- **kyu-parser: `LOAD FROM` statement** — New statement that imports an RDF
  file and auto-creates all tables. Distinct from `COPY FROM` (which requires
  a pre-existing table): `LOAD FROM 'file.ttl'` creates the full schema from
  scratch.

- **kyu-binder** — Added `BoundLoadFrom` and `bind_load_from()` to resolve the
  new `LOAD FROM` AST node.

- **kyu-api: `Connection::exec_load_from()`** — Orchestrates the full RDF
  import pipeline: parse → infer schema → create node/rel tables → insert rows.

- **kyu-visualizer: library target** — Added `src/lib.rs` exposing
  `pub fn launch(db: Database)` so examples can each pre-seed the database
  independently before opening the visualizer window. The default binary now
  starts with an empty database.

- **kyu-visualizer: `examples/rdf_kg`** — New visualizer example that imports
  the FOAF + schema.org research knowledge graph via `LOAD FROM` and opens the
  interactive explorer pre-loaded with researchers, institutions, and papers.

### Fixed

- **kyu-api: Multi-MATCH relationship creation** — `exec_match_dml` now supports
  chained `MATCH ... MATCH ... CREATE` queries. Previously, `CREATE` clauses inside
  MATCH-driven mutations were silently ignored (`_ => {}`), so relationship inserts
  via `MATCH (a) MATCH (b) CREATE (a)-[:REL]->(b)` appeared to succeed but stored
  nothing. The method now builds a cross-product of bindings across all MATCH
  clauses and executes CREATE patterns with correct source/destination PK resolution.

- **kyu-planner: Sentinel join key elimination** — `build_pattern` used `u32::MAX`
  and `u32::MAX - 1` as placeholder join key column indices that were never resolved
  to actual positions. Hash joins would attempt to access column 4294967295, fail
  silently, and produce zero matches for any multi-hop pattern. Replaced with
  computed absolute column indices tracked via `total_cols`, `last_node_pk_col`, and
  `last_rel_dst_abs`.

- **kyu-planner: Optimizer join-side swap guard** — The cost-based optimizer swapped
  build/probe sides of `HashJoin` when the probe had lower cardinality. This changed
  the combined output column layout, breaking downstream projections that relied on
  specific column positions. The swap is now restricted to keyless cross-product
  joins where column order is irrelevant.

### Added

- **kyu-visualizer** — New crate providing an interactive GPU-accelerated graph
  explorer built on the Blinc UI framework. Features:
  - Force-directed (Fruchterman-Reingold) graph layout
  - Pan, zoom, and node dragging with 1:1 cursor tracking
  - Schema browser sidebar with click-to-center navigation
  - Node/edge selection with property inspector
  - Cypher query bar for ad-hoc queries
  - Canvas rendering with arrowheads, labels, and selection highlights

- **kyu-binder** — Extended binder to support multi-MATCH clause collection for
  DML queries, enabling the `exec_match_dml` rewrite above.
