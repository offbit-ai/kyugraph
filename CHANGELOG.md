# Changelog

All notable changes to KyuGraph are documented in this file.

## [Unreleased]

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
