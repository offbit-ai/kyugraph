# Agentic Knowledge Discovery

## What you'll build

An autonomous agent that incrementally explores a codebase — starting from a single seed file, following import chains, then using FTS and vector similarity to discover files that are **related but not directly imported**. This simulates how an AI agent builds up a knowledge graph through emergent discovery.

This follows the `agent_explorer` example: `cargo run -p kyu-api --example agent_explorer`

## Step 1: Set up the agent's schema

The agent needs three relationship types — direct imports and emergent similarities:

```rust
let mut db = Database::in_memory();
db.register_extension(Box::new(ext_algo::AlgoExtension));
let conn = db.connect();

conn.query("CREATE NODE TABLE File (
    id INT64, path STRING, name STRING, ext STRING, lines INT64,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE REL TABLE IMPORTS (FROM File TO File)").unwrap();
conn.query("CREATE REL TABLE SIMILAR_TO (FROM File TO File)").unwrap();
```

The key difference from the codebase explorer: `SIMILAR_TO` edges represent connections discovered through semantic similarity, not explicit `use` statements.

## Step 2: Seed exploration

The agent starts from a single file and analyzes its imports:

```rust
// Start from the main connection handler
let seed = "crates/kyu-api/src/connection.rs";

// Ingest the seed file:
// 1. Insert as a File node
// 2. Index content in FTS
// 3. Compute bag-of-words vector and index in HNSW

let (seed_id, imports, modules) = ingest_file(&seed, &mut state);
// imports = ["kyu_common", "kyu_types", "kyu_parser", "kyu_binder", ...]
// modules = ["database.rs", "storage.rs", "persistence.rs", ...]

// Queue all imports and modules for exploration
for crate_name in &imports {
    if let Some(lib_path) = crate_map.get(crate_name) {
        queue.push_back(lib_path.clone());
    }
}
```

## Step 3: Import-driven expansion

The agent follows the import chain, exploring each file and discovering new imports:

```rust
let mut turn = 2;

while let Some(rel_path) = queue.pop_front() {
    if turn > 12 { break; }
    if explored.contains(&rel_path) { continue; }

    // Ingest the file (graph node + FTS + vector index)
    let (file_id, file_imports, file_modules) = ingest_file(&rel_path, &mut state);

    // Add IMPORTS edges to/from already-explored files
    for imp_crate in &file_imports {
        if let Some(&target_id) = path_to_id.get(crate_map[imp_crate]) {
            import_edges.push((file_id, target_id));
            storage.insert_row(imports_id, &[
                TypedValue::Int64(file_id),
                TypedValue::Int64(target_id),
            ]).unwrap();
        }
    }

    // Queue new discoveries
    for imp_crate in &file_imports {
        if let Some(lib_path) = crate_map.get(imp_crate) {
            if !explored.contains(lib_path) {
                queue.push_back(lib_path.clone());
            }
        }
    }

    // Periodically check what's most important
    if turn % 3 == 0 {
        let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();
        // Top 3: kyu-types (0.182), kyu-common (0.156), kyu-parser (0.134)
    }

    turn += 1;
}
```

Each turn the agent prints what it found:

```
[Turn 2] Exploring: crates/kyu-types/src/lib.rs (via import)
  Found 1 imports + 4 modules, queued 4 new. Graph: 3 files, 2 edges.

[Turn 3] Exploring: crates/kyu-common/src/lib.rs (via import)
  Found 0 imports + 3 modules, queued 3 new. Graph: 4 files, 3 edges.
  PageRank top 3: kyu-types (0.182), kyu-common (0.156), connection (0.134)
```

## Step 4: Emergent discovery

This is the key innovation. After exhausting import chains, the agent uses FTS and vector similarity to find files that are **semantically related but not in any import chain**:

```rust
// Strategy A: FTS → Vector pipeline
// 1. Search FTS for domain-specific terms
let fts_results = fts_ext.execute("search", &[
    "query execution pipeline".into(), "5".into()
], &empty_adj).unwrap();

// 2. Take the top FTS result and find its vector neighbors
if let Some(hub_id) = fts_results.first() {
    let hub_vector = vec_cache.get(&hub_id);
    let vec_results = vector_ext.execute("search", &[
        hub_vector_csv, "10".into()
    ], &empty_adj).unwrap();

    // 3. Look for unexplored files near the vector neighbors
    for candidate in &vec_results {
        let candidate_dir = Path::new(&candidate_path).parent();
        for rs_path in &all_rs_files {
            if !explored.contains(rs_path) && rs_path.starts_with(candidate_dir) {
                // Found an unexplored file near a similar explored file!
                let (new_id, _, _) = ingest_file(rs_path, &mut state);
                // Add a SIMILAR_TO edge (not IMPORTS — this is emergent)
                similar_edges.push((new_id, candidate_id));
                storage.insert_row(similar_table_id, &[
                    TypedValue::Int64(new_id),
                    TypedValue::Int64(candidate_id),
                ]).unwrap();
            }
        }
    }
}
```

Output:

```
[Turn 14] Emergent: binder.rs (similar to connection.rs, dist=0.1234)
  Added SIMILAR_TO edge. Not in any import chain!

[Turn 15] Emergent: planner.rs (near executor.rs)
```

## Step 5: Analyze the complete knowledge graph

After both import-driven and emergent exploration, analyze the full graph:

```rust
// PageRank on the combined graph (imports + SIMILAR_TO edges)
let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();
// Now includes emergent nodes alongside import-discovered ones

// Betweenness centrality — which files are bridges?
let result = conn.query("CALL algo.betweenness()").unwrap();
// Files that connect otherwise-separate modules rank highest

// WCC — how many connected clusters?
let result = conn.query("CALL algo.wcc()").unwrap();
// SIMILAR_TO edges may connect clusters that imports alone couldn't
```

## Summary output

```
=== Summary ===
Explored 18 files in 16 turns.
  1 seed, 13 via imports, 4 via emergent discovery.
  22 import edges, 4 SIMILAR_TO edges.
```

## What you've learned

- **Incremental graph building** — start small, expand based on discoveries
- **Two discovery modes** — explicit (import chains) and emergent (FTS + vector similarity)
- **FTS → vector pipeline** — use text search to find hubs, then vector search to find neighbors
- **SIMILAR_TO edges** — represent semantic relationships invisible to static analysis
- **Live graph analytics** — run PageRank during exploration to guide the agent's focus
