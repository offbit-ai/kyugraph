# Explore a Codebase

## What you'll build

A codebase exploration tool that ingests a Rust workspace into a graph — directories, files, and cross-crate import dependencies. You'll search source code with FTS, find files with similar code patterns via vector similarity, and use PageRank to find the most central files.

This follows the `filesystem_graph` example: `cargo run -p kyu-api --example filesystem_graph`

## Step 1: Set up schema for code structure

Model a codebase as directories containing files, with import edges between files:

```rust
let mut db = Database::in_memory();
db.register_extension(Box::new(ext_algo::AlgoExtension));
let conn = db.connect();

conn.query("CREATE NODE TABLE Directory (
    id INT64, path STRING, name STRING, depth INT64,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE NODE TABLE File (
    id INT64, path STRING, name STRING, ext STRING, size INT64, lines INT64,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE REL TABLE HAS_FILE (FROM Directory TO File)").unwrap();
conn.query("CREATE REL TABLE HAS_DIR (FROM Directory TO Directory)").unwrap();
conn.query("CREATE REL TABLE IMPORTS (FROM File TO File)").unwrap();
```

## Step 2: Walk the filesystem and extract dependencies

Scan the workspace, build CSV files, and extract Rust `use` statements to create import edges:

```rust
// Walk the directory tree, collecting directories and files
let (dirs, files) = walk_tree(&root);

// Build a map: crate_name (e.g. "kyu_types") -> lib.rs file ID
// by looking for lib.rs files under crates/ and extensions/
let crate_map = build_crate_map(&files);

// Extract import edges by parsing "use crate_name::" lines from .rs files
let imports = extract_imports(&root, &files, &crate_map);
```

The `extract_imports` function scans each `.rs` file for `use kyu_types::...` and similar lines, mapping them to the corresponding `lib.rs` file ID:

```rust
for line in content.lines() {
    let trimmed = line.trim();
    if !trimmed.starts_with("use ") { continue; }
    let crate_name = trimmed.strip_prefix("use ").unwrap()
        .split("::").next().unwrap().trim();
    if let Some(&target_id) = crate_map.get(crate_name) {
        edges.push((file_id, target_id));
    }
}
```

## Step 3: Load data via CSV bulk ingestion

```rust
// Write CSV files and load them
conn.query(&format!("COPY Directory FROM '{}'", dirs_csv_path)).unwrap();
conn.query(&format!("COPY File FROM '{}'", files_csv_path)).unwrap();

// Insert relationships via storage API
let mut storage = db.storage().write().unwrap();
for &(from_id, to_id) in &imports {
    storage.insert_row(imports_id, &[
        TypedValue::Int64(from_id),
        TypedValue::Int64(to_id),
    ]).unwrap();
}
```

## Step 4: Query code dependencies

Find the most-imported libraries:

```rust
// Which lib.rs files are imported most?
let mut import_counts: HashMap<i64, u32> = HashMap::new();
for &(_, to_id) in &imports {
    *import_counts.entry(to_id).or_default() += 1;
}
// Result: kyu-types/src/lib.rs — 15x, kyu-common/src/lib.rs — 12x, ...
```

Find all files that import a specific crate:

```rust
// Files importing kyu_types
let importers: Vec<_> = imports.iter()
    .filter(|(_, to)| *to == kyu_types_lib_id)
    .filter_map(|(from, _)| file_by_id.get(from))
    .collect();
println!("Files importing kyu_types: {}", importers.len());
```

Largest source files by line count:

```rust
let result = conn.query("
    MATCH (f:File)
    RETURN f.name, f.lines, f.path
    ORDER BY f.lines DESC
    LIMIT 10
").unwrap();
```

## Step 5: Search source code with FTS

Index the content of every `.rs` file and search across the codebase:

```rust
let fts_ext = ext_fts::FtsExtension::new();

// Index each Rust source file
for fi in &files {
    if fi.ext != "rs" { continue; }
    let content = std::fs::read_to_string(root.join(&fi.path)).unwrap_or_default();
    fts_ext.execute("add", &[content], &empty_adj).unwrap();
}

// Search for specific patterns
for query in ["parser combinator", "transaction commit", "HNSW nearest neighbor"] {
    let results = fts_ext.execute("search", &[query.into(), "5".into()], &empty_adj).unwrap();
    println!("Search: \"{}\"", query);
    for row in &results {
        // row[0] = doc_id, row[1] = score
        println!("  [score={:.2}] {}", row[1], file_paths[row[0] as usize]);
    }
}
```

## Step 6: Find similar files with vector search

Use bag-of-words embeddings to find files with similar code patterns:

```rust
let vector_ext = ext_vector::VectorExtension::new();

// Code-specific keywords for embeddings
let keywords = vec![
    "fn", "struct", "impl", "trait", "enum", "pub", "mod", "use",
    "async", "unsafe", "test", "error", "result", "option",
    "query", "parse", "execute", "table", "column", "row",
];

vector_ext.execute("build", &[keywords.len().to_string(), "cosine".into()], &empty_adj).unwrap();

// Index each file
for fi in &files {
    let content = std::fs::read_to_string(root.join(&fi.path)).unwrap_or_default();
    let vec = bag_of_words(&content, &keywords);
    let csv = vec.iter().map(|v| format!("{v}")).collect::<Vec<_>>().join(",");
    vector_ext.execute("add", &[fi.id.to_string(), csv], &empty_adj).unwrap();
}

// Find files similar to connection.rs
let query_vec = bag_of_words(&connection_rs_content, &keywords);
let results = vector_ext.execute("search", &[query_csv, "5".into()], &empty_adj).unwrap();
// Result: database.rs, executor.rs, storage.rs — structurally similar code
```

## Step 7: Analyze the dependency graph

PageRank reveals the most central files — the ones everything depends on:

```rust
let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();
// Top results: kyu-types/src/lib.rs, kyu-common/src/lib.rs (foundational crates)

// WCC finds isolated module clusters
let result = conn.query("CALL algo.wcc()").unwrap();
// Shows independent clusters of files with no shared dependencies
```

## What you've learned

- Modeling a codebase as a property graph (directories, files, imports)
- Extracting cross-crate dependencies from Rust `use` statements
- Full-text search across source code
- Vector similarity for finding structurally similar files
- Graph algorithms revealing architectural patterns (most-central files, module clusters)
