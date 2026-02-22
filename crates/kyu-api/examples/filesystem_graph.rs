//! Filesystem Graph Example — ingests a codebase into KyuGraph for content exploration.
//!
//! Scans the KyuGraph workspace (or a CLI path), builds a knowledge graph
//! capturing directory structure, file contents, and cross-file dependencies
//! extracted from Rust `use` statements. Demonstrates:
//! 1. Schema creation (directories, files, containment, imports)
//! 2. Filesystem walk & dependency extraction
//! 3. Content-aware queries (dependencies, importers, cross-crate usage)
//! 4. Full-text search across source code (ext-fts)
//! 5. Vector similarity between files by code patterns (ext-vector)
//! 6. Graph algorithms on the import graph (ext-algo)
//!
//! Run with: cargo run -p kyu-api --example filesystem_graph [-- /path/to/scan]

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use kyu_api::Database;
use kyu_extension::Extension;
use kyu_types::TypedValue;

fn main() {
    println!("=== KyuGraph Codebase Explorer ===\n");

    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    println!("Scanning: {}\n", root.display());

    let mut db = Database::in_memory();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    let conn = db.connect();

    // -----------------------------------------------------------------------
    // Stage 1: Schema Creation
    // -----------------------------------------------------------------------
    println!("--- Stage 1: Schema Creation ---");

    conn.query("CREATE NODE TABLE Directory (id INT64, path STRING, name STRING, depth INT64, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE NODE TABLE File (id INT64, path STRING, name STRING, ext STRING, size INT64, lines INT64, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE REL TABLE HAS_FILE (FROM Directory TO File)").unwrap();
    conn.query("CREATE REL TABLE HAS_DIR (FROM Directory TO Directory)").unwrap();
    conn.query("CREATE REL TABLE IMPORTS (FROM File TO File)").unwrap();

    println!("Created 2 node tables and 3 relationship tables.\n");

    // -----------------------------------------------------------------------
    // Stage 2: Filesystem Walk & Dependency Extraction
    // -----------------------------------------------------------------------
    println!("--- Stage 2: Filesystem Walk & Dependency Extraction ---");

    let (dirs, files) = walk_tree(&root);

    // Build a map: crate_name (e.g. "kyu_types") → lib.rs file id.
    let crate_lib_map = build_crate_map(&files);

    // Extract import edges from Rust source files.
    let imports = extract_imports(&root, &files, &crate_lib_map);

    // Write and load Directory CSV.
    let tmp = std::env::temp_dir().join("kyu_filesystem_graph_example");
    let _ = std::fs::create_dir_all(&tmp);

    {
        let mut f = std::fs::File::create(tmp.join("dirs.csv")).unwrap();
        writeln!(f, "id,path,name,depth").unwrap();
        for d in &dirs {
            writeln!(f, "{},\"{}\",\"{}\",{}", d.id, csv_escape(&d.path), csv_escape(&d.name), d.depth).unwrap();
        }
    }
    conn.query(&format!("COPY Directory FROM '{}'", tmp.join("dirs.csv").display())).unwrap();

    // Write and load File CSV.
    {
        let mut f = std::fs::File::create(tmp.join("files.csv")).unwrap();
        writeln!(f, "id,path,name,ext,size,lines").unwrap();
        for fi in &files {
            writeln!(
                f, "{},\"{}\",\"{}\",\"{}\",{},{}",
                fi.id, csv_escape(&fi.path), csv_escape(&fi.name), csv_escape(&fi.ext), fi.size, fi.lines
            ).unwrap();
        }
    }
    conn.query(&format!("COPY File FROM '{}'", tmp.join("files.csv").display())).unwrap();

    // Insert relationships via storage API.
    {
        let catalog = db.catalog().read();
        let has_file_id = catalog.find_by_name("HAS_FILE").unwrap().table_id();
        let has_dir_id = catalog.find_by_name("HAS_DIR").unwrap().table_id();
        let imports_id = catalog.find_by_name("IMPORTS").unwrap().table_id();
        drop(catalog);

        let mut storage = db.storage().write().unwrap();
        let mut has_dir_count = 0;
        for d in &dirs {
            if let Some(parent) = d.parent_id {
                storage
                    .insert_row(has_dir_id, &[TypedValue::Int64(parent), TypedValue::Int64(d.id)])
                    .unwrap();
                has_dir_count += 1;
            }
        }
        for fi in &files {
            storage
                .insert_row(has_file_id, &[TypedValue::Int64(fi.parent_dir_id), TypedValue::Int64(fi.id)])
                .unwrap();
        }
        for &(from_id, to_id) in &imports {
            storage
                .insert_row(imports_id, &[TypedValue::Int64(from_id), TypedValue::Int64(to_id)])
                .unwrap();
        }

        println!("Loaded {} directories, {} files", dirs.len(), files.len());
        println!(
            "Loaded {} containment edges, {} import edges\n",
            has_dir_count + files.len(),
            imports.len()
        );
    }

    // Build lookup maps used across stages.
    let file_by_id: HashMap<i64, &FileEntry> = files.iter().map(|f| (f.id, f)).collect();

    // -----------------------------------------------------------------------
    // Stage 3: Content-Aware Queries
    // -----------------------------------------------------------------------
    println!("--- Stage 3: Content-Aware Queries ---");

    // Most-imported libraries (which lib.rs files are imported most).
    let mut import_counts: HashMap<i64, u32> = HashMap::new();
    for &(_, to_id) in &imports {
        *import_counts.entry(to_id).or_default() += 1;
    }
    let mut sorted_imports: Vec<_> = import_counts.iter().collect();
    sorted_imports.sort_by(|a, b| b.1.cmp(a.1));
    println!("Most-imported libraries:");
    for (target_id, count) in sorted_imports.iter().take(10) {
        if let Some(f) = file_by_id.get(target_id) {
            println!("  {count:>3}x  {}", f.path);
        }
    }

    // Files that import a specific crate.
    let target_crate = "kyu_types";
    if let Some(&lib_id) = crate_lib_map.get(target_crate) {
        let importers: Vec<_> = imports
            .iter()
            .filter(|(_, to)| *to == lib_id)
            .filter_map(|(from, _)| file_by_id.get(from))
            .collect();
        println!("\nFiles importing {target_crate} ({} found):", importers.len());
        for f in importers.iter().take(10) {
            println!("  {}", f.path);
        }
        if importers.len() > 10 {
            println!("  ... and {} more", importers.len() - 10);
        }
    }

    // Direct dependencies of a specific file.
    let target_file = "connection.rs";
    if let Some(tf) = files.iter().find(|f| f.name == target_file) {
        let deps: Vec<_> = imports
            .iter()
            .filter(|(from, _)| *from == tf.id)
            .filter_map(|(_, to)| file_by_id.get(to))
            .collect();
        println!("\nDependencies of {target_file} ({} imports):", deps.len());
        for f in &deps {
            println!("  -> {}", f.path);
        }
    }

    // Largest files by line count.
    let result = conn.query("MATCH (f:File) RETURN f.name, f.lines, f.path").unwrap();
    let mut file_rows: Vec<_> = result.iter_rows().collect();
    file_rows.sort_by(|a, b| {
        let la = match &a[1] { TypedValue::Int64(v) => *v, _ => 0 };
        let lb = match &b[1] { TypedValue::Int64(v) => *v, _ => 0 };
        lb.cmp(&la)
    });
    println!("\nLargest source files:");
    for row in file_rows.iter().take(10) {
        println!("  {:>6} lines  {}", row[1], row[2]);
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 4: Full-Text Search
    // -----------------------------------------------------------------------
    println!("--- Stage 4: Full-Text Search ---");

    let fts_ext = ext_fts::FtsExtension::new();
    let empty_adj: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();

    // Index .rs file contents (FTS used directly — content has commas).
    let mut fts_file_ids: Vec<i64> = Vec::new();
    for fi in &files {
        if fi.ext != "rs" {
            continue;
        }
        let content = std::fs::read_to_string(root.join(&fi.path)).unwrap_or_default();
        if content.is_empty() {
            continue;
        }
        let truncated = if content.len() > 10_000 { &content[..10_000] } else { &content };
        fts_ext
            .execute("add", &[truncated.to_string()], &empty_adj)
            .unwrap();
        fts_file_ids.push(fi.id);
    }
    println!("Indexed {} Rust source files.\n", fts_file_ids.len());

    for query in ["parser combinator", "transaction commit", "HNSW nearest neighbor"] {
        let results = fts_ext
            .execute("search", &[query.into(), "5".into()], &empty_adj)
            .unwrap();
        println!("Search: \"{query}\"");
        if results.is_empty() {
            println!("  (no results)");
        }
        for row in &results {
            if let (TypedValue::Int64(doc_id), TypedValue::Double(score)) = (&row[0], &row[1]) {
                if let Some(&file_id) = fts_file_ids.get(*doc_id as usize) {
                    if let Some(f) = file_by_id.get(&file_id) {
                        println!("  [score={score:.2}] {}", f.path);
                    }
                }
            }
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Stage 5: Vector Similarity Search
    // -----------------------------------------------------------------------
    println!("--- Stage 5: Vector Similarity Search ---");

    let vector_ext = ext_vector::VectorExtension::new();
    let kw = keywords();

    vector_ext
        .execute("build", &[kw.len().to_string(), "cosine".into()], &empty_adj)
        .unwrap();

    let mut vec_file_ids: Vec<i64> = Vec::new();
    for fi in &files {
        if fi.ext != "rs" {
            continue;
        }
        let content = std::fs::read_to_string(root.join(&fi.path)).unwrap_or_default();
        if content.is_empty() {
            continue;
        }
        let vec = bag_of_words(&content, &kw);
        if vec.iter().all(|&v| v == 0.0) {
            continue;
        }
        let vec_csv = vec.iter().map(|v| format!("{v}")).collect::<Vec<_>>().join(",");
        vector_ext
            .execute("add", &[fi.id.to_string(), vec_csv], &empty_adj)
            .unwrap();
        vec_file_ids.push(fi.id);
    }
    println!("Indexed {} files for vector similarity.\n", vec_file_ids.len());

    // Find files similar to connection.rs by code patterns.
    let target_name = "connection.rs";
    if let Some(tf) = files.iter().find(|f| f.name == target_name) {
        let content = std::fs::read_to_string(root.join(&tf.path)).unwrap_or_default();
        let query_vec = bag_of_words(&content, &kw);
        let query_csv = query_vec.iter().map(|v| format!("{v}")).collect::<Vec<_>>().join(",");

        let results = vector_ext
            .execute("search", &[query_csv, "5".into()], &empty_adj)
            .unwrap();

        // HNSW returns 0-based insertion-order IDs.
        println!("Files with similar code patterns to {target_name}:");
        for row in &results {
            if let (TypedValue::Int64(idx), TypedValue::Double(dist)) = (&row[0], &row[1]) {
                if let Some(&file_id) = vec_file_ids.get(*idx as usize) {
                    if let Some(f) = file_by_id.get(&file_id) {
                        println!("  [dist={dist:.4}] {}", f.path);
                    }
                }
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 6: Graph Algorithms on Import Graph
    // -----------------------------------------------------------------------
    println!("--- Stage 6: Graph Algorithms ---");

    let mut name_map: HashMap<i64, String> = HashMap::new();
    for d in &dirs {
        name_map.insert(d.id, format!("{}/", d.path));
    }
    for f in &files {
        name_map.insert(f.id, f.path.clone());
    }

    // PageRank — find most central files in the dependency graph.
    let result = conn
        .query("CALL algo.pageRank(0.85, 20, 0.000001)")
        .unwrap();
    println!("PageRank — most central nodes (import + containment graph):");
    for (rank, row) in result.iter_rows().take(10).enumerate() {
        if let (TypedValue::Int64(node_id), TypedValue::Double(score)) = (&row[0], &row[1]) {
            let label = name_map.get(node_id).map_or("?", |s| s.as_str());
            println!("  #{:>2} rank={score:.6}  {label}", rank + 1);
        }
    }

    // WCC — find independent module clusters.
    let result = conn.query("CALL algo.wcc()").unwrap();
    let mut components: HashMap<i64, Vec<i64>> = HashMap::new();
    for row in result.iter_rows() {
        if let (TypedValue::Int64(node_id), TypedValue::Int64(comp)) = (&row[0], &row[1]) {
            components.entry(*comp).or_default().push(*node_id);
        }
    }
    let mut sorted_comps: Vec<_> = components.values().collect();
    sorted_comps.sort_by(|a, b| b.len().cmp(&a.len()));
    println!("\nWCC: {} cluster(s) found", sorted_comps.len());
    for (i, members) in sorted_comps.iter().take(5).enumerate() {
        let sample: Vec<_> = members
            .iter()
            .take(3)
            .filter_map(|id| name_map.get(id))
            .cloned()
            .collect();
        println!(
            "  Cluster {}: {} nodes (e.g., {})",
            i + 1,
            members.len(),
            sample.join(", ")
        );
    }

    let _ = std::fs::remove_dir_all(&tmp);
    println!("\nDone.");
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct DirEntry {
    id: i64,
    path: String,
    name: String,
    depth: i64,
    parent_id: Option<i64>,
}

struct FileEntry {
    id: i64,
    path: String,
    name: String,
    ext: String,
    size: i64,
    lines: i64,
    parent_dir_id: i64,
}

// ---------------------------------------------------------------------------
// Filesystem walk
// ---------------------------------------------------------------------------

fn walk_tree(root: &Path) -> (Vec<DirEntry>, Vec<FileEntry>) {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    let mut next_id: i64 = 0;

    let root_id = next_id;
    next_id += 1;
    dirs.push(DirEntry {
        id: root_id,
        path: ".".into(),
        name: root.file_name().unwrap_or_default().to_string_lossy().to_string(),
        depth: 0,
        parent_id: None,
    });

    let mut stack: Vec<(PathBuf, i64, i64)> = vec![(root.to_path_buf(), 0, root_id)];

    while let Some((dir_path, depth, parent_id)) = stack.pop() {
        let entries = match std::fs::read_dir(&dir_path) {
            Ok(e) => e,
            Err(_) => continue,
        };
        let mut children: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        children.sort_by_key(|e| e.file_name());

        for entry in children {
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let entry_path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden dirs, build artifacts, caches.
            if name.starts_with('.') || name == "target" || name == "node_modules" || name == "__pycache__" {
                continue;
            }

            let rel_path = entry_path
                .strip_prefix(root)
                .unwrap_or(&entry_path)
                .to_string_lossy()
                .to_string();

            if ft.is_dir() {
                let dir_id = next_id;
                next_id += 1;
                dirs.push(DirEntry {
                    id: dir_id,
                    path: rel_path,
                    name,
                    depth: depth + 1,
                    parent_id: Some(parent_id),
                });
                stack.push((entry_path, depth + 1, dir_id));
            } else if ft.is_file() {
                let file_id = next_id;
                next_id += 1;
                let ext = entry_path
                    .extension()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let size = entry.metadata().map_or(0, |m| m.len() as i64);
                let lines = count_lines(&entry_path);
                files.push(FileEntry {
                    id: file_id,
                    path: rel_path,
                    name,
                    ext,
                    size,
                    lines,
                    parent_dir_id: parent_id,
                });
            }
        }
    }

    (dirs, files)
}

fn count_lines(path: &Path) -> i64 {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return 0,
    };
    if meta.len() > 1_000_000 {
        return 0;
    }
    std::fs::read_to_string(path)
        .map(|s| s.lines().count() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Dependency extraction
// ---------------------------------------------------------------------------

/// Build a map from Rust crate names (e.g. "kyu_types") to their lib.rs file ID.
fn build_crate_map(files: &[FileEntry]) -> HashMap<String, i64> {
    let mut map = HashMap::new();
    for f in files {
        if f.name == "lib.rs" {
            // Extract crate name from path: "crates/kyu-types/src/lib.rs" → "kyu_types"
            // or "extensions/ext-algo/src/lib.rs" → "ext_algo"
            let parts: Vec<&str> = f.path.split('/').collect();
            if parts.len() >= 3 && parts[parts.len() - 1] == "lib.rs" && parts[parts.len() - 2] == "src" {
                let crate_dir = parts[parts.len() - 3];
                let crate_name = crate_dir.replace('-', "_");
                map.insert(crate_name, f.id);
            }
        }
    }
    map
}

/// Extract import edges by parsing `use crate_name::` from Rust source files.
fn extract_imports(
    root: &Path,
    files: &[FileEntry],
    crate_map: &HashMap<String, i64>,
) -> Vec<(i64, i64)> {
    let mut edges = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for fi in files {
        if fi.ext != "rs" {
            continue;
        }
        let content = std::fs::read_to_string(root.join(&fi.path)).unwrap_or_default();
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.starts_with("use ") {
                continue;
            }
            // Parse: "use kyu_types::TypedValue;" → crate_name = "kyu_types"
            let rest = trimmed.strip_prefix("use ").unwrap_or("");
            let crate_name = rest.split("::").next().unwrap_or("").trim();
            if let Some(&target_id) = crate_map.get(crate_name) {
                if target_id != fi.id {
                    let edge = (fi.id, target_id);
                    if seen.insert(edge) {
                        edges.push(edge);
                    }
                }
            }
        }
    }
    edges
}

// ---------------------------------------------------------------------------
// CSV helper
// ---------------------------------------------------------------------------

fn csv_escape(s: &str) -> String {
    s.replace('"', "\"\"")
}

// ---------------------------------------------------------------------------
// Bag-of-words embedding for code similarity
// ---------------------------------------------------------------------------

fn keywords() -> Vec<&'static str> {
    vec![
        "fn", "struct", "impl", "trait", "enum", "pub", "mod", "use", "async", "unsafe",
        "test", "error", "result", "option", "vec", "string", "hash", "map", "arc", "mutex",
        "query", "parse", "execute", "table", "column", "row", "index", "type", "value", "node",
    ]
}

fn bag_of_words(text: &str, keywords: &[&str]) -> Vec<f32> {
    let lower = text.to_lowercase();
    keywords
        .iter()
        .map(|kw| if lower.contains(kw) { 1.0 } else { 0.0 })
        .collect()
}
