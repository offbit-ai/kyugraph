//! Agent Explorer — simulates emergent knowledge discovery through incremental ingestion.
//!
//! Unlike the batch-loading `filesystem_graph` example, this simulates how an
//! autonomous AI agent explores a codebase: start from a seed file, follow imports,
//! use FTS and vector similarity to discover files that are related but NOT directly
//! imported (emergent connections), and iteratively build up a knowledge graph.
//!
//! Run with: cargo run -p kyu-api --example agent_explorer [-- /path/to/scan]

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use kyu_api::Database;
use kyu_extension::Extension;
use kyu_types::TypedValue;
use smol_str::SmolStr;

fn main() {
    println!("=== KyuGraph Agent Explorer — Emergent Knowledge Discovery ===\n");

    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    println!("Workspace: {}\n", root.display());

    // --- Initialize database + extensions ---
    let mut db = Database::in_memory();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    let conn = db.connect();

    // FTS and vector used directly (content has commas → breaks CALL parser).
    let fts_ext = ext_fts::FtsExtension::new();
    let vector_ext = ext_vector::VectorExtension::new();
    let empty_adj: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
    let kw = keywords();

    vector_ext
        .execute(
            "build",
            &[kw.len().to_string(), "cosine".into()],
            &empty_adj,
        )
        .unwrap();

    // --- Schema ---
    conn.query("CREATE NODE TABLE File (id INT64, path STRING, name STRING, ext STRING, lines INT64, PRIMARY KEY (id))")
        .unwrap();
    conn.query("CREATE REL TABLE IMPORTS (FROM File TO File)")
        .unwrap();
    conn.query("CREATE REL TABLE SIMILAR_TO (FROM File TO File)")
        .unwrap();

    let file_table_id = {
        let cat = db.catalog().read();
        cat.find_by_name("File").unwrap().table_id()
    };
    let imports_table_id = {
        let cat = db.catalog().read();
        cat.find_by_name("IMPORTS").unwrap().table_id()
    };
    let similar_table_id = {
        let cat = db.catalog().read();
        cat.find_by_name("SIMILAR_TO").unwrap().table_id()
    };

    // --- Build crate map (scan workspace for lib.rs files) ---
    let crate_map = build_crate_map(&root);

    // --- Agent state ---
    let mut next_id: i64 = 0;
    let mut explored: HashSet<String> = HashSet::new(); // rel paths already ingested
    let mut path_to_id: HashMap<String, i64> = HashMap::new();
    let mut id_to_path: HashMap<i64, String> = HashMap::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut import_edges: Vec<(i64, i64)> = Vec::new();
    let mut similar_edges: Vec<(i64, i64)> = Vec::new();
    let mut fts_file_ids: Vec<i64> = Vec::new(); // FTS doc_id → graph node id
    let mut vec_file_ids: Vec<i64> = Vec::new(); // HNSW insertion order → graph node id
    let mut vec_cache: HashMap<i64, Vec<f32>> = HashMap::new(); // id → bag-of-words
    let mut discovery_method: HashMap<i64, &str> = HashMap::new(); // id → "import" | "emergent"

    // Helper: ingest a single file into the graph + FTS + vector.
    let ingest_file = |rel_path: &str,
                       next_id: &mut i64,
                       explored: &mut HashSet<String>,
                       path_to_id: &mut HashMap<String, i64>,
                       id_to_path: &mut HashMap<i64, String>,
                       fts_file_ids: &mut Vec<i64>,
                       vec_file_ids: &mut Vec<i64>,
                       vec_cache: &mut HashMap<i64, Vec<f32>>,
                       db: &Database,
                       fts_ext: &ext_fts::FtsExtension,
                       vector_ext: &ext_vector::VectorExtension,
                       empty_adj: &HashMap<i64, Vec<(i64, f64)>>,
                       kw: &[&str]|
     -> Option<(i64, Vec<String>, Vec<String>)> {
        if explored.contains(rel_path) {
            return None;
        }

        let abs_path = root.join(rel_path);
        let content = std::fs::read_to_string(&abs_path).unwrap_or_default();
        if content.is_empty() {
            return None;
        }

        let id = *next_id;
        *next_id += 1;

        let name = Path::new(rel_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let ext = Path::new(rel_path)
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let lines = content.lines().count() as i64;

        // Insert File node.
        {
            let mut storage = db.storage().write().unwrap();
            storage
                .insert_row(
                    file_table_id,
                    &[
                        TypedValue::Int64(id),
                        TypedValue::String(SmolStr::new(rel_path)),
                        TypedValue::String(SmolStr::new(&name)),
                        TypedValue::String(SmolStr::new(&ext)),
                        TypedValue::Int64(lines),
                    ],
                )
                .unwrap();
        }

        // Index in FTS (truncate large files).
        let truncated = if content.len() > 10_000 {
            &content[..10_000]
        } else {
            &content
        };
        fts_ext
            .execute("add", &[truncated.to_string()], empty_adj)
            .unwrap();
        fts_file_ids.push(id);

        // Index in vector.
        let bow = bag_of_words(&content, kw);
        if bow.iter().any(|&v| v > 0.0) {
            let csv = bow
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join(",");
            vector_ext
                .execute("add", &[id.to_string(), csv], empty_adj)
                .unwrap();
            vec_file_ids.push(id);
            vec_cache.insert(id, bow);
        }

        explored.insert(rel_path.to_string());
        path_to_id.insert(rel_path.to_string(), id);
        id_to_path.insert(id, rel_path.to_string());

        // Extract imports and module declarations.
        let imports = extract_imports_from_content(&content);
        let parent_dir = Path::new(rel_path)
            .parent()
            .unwrap_or(Path::new(""))
            .to_string_lossy()
            .to_string();
        let modules = extract_modules(&content, &parent_dir);

        Some((id, imports, modules))
    };

    // =======================================================================
    // Phase 1: Seed Exploration
    // =======================================================================
    println!("--- Phase 1: Seed Exploration ---\n");

    let seed = "crates/kyu-api/src/connection.rs";
    let seed_path = if root.join(seed).exists() {
        seed.to_string()
    } else {
        // Fallback: find any lib.rs
        crate_map
            .values()
            .next()
            .cloned()
            .unwrap_or_else(|| "src/lib.rs".to_string())
    };

    let (seed_id, seed_imports, seed_modules) = ingest_file(
        &seed_path,
        &mut next_id,
        &mut explored,
        &mut path_to_id,
        &mut id_to_path,
        &mut fts_file_ids,
        &mut vec_file_ids,
        &mut vec_cache,
        &db,
        &fts_ext,
        &vector_ext,
        &empty_adj,
        &kw,
    )
    .expect("Seed file must exist");

    discovery_method.insert(seed_id, "seed");
    let lines = id_to_path
        .get(&seed_id)
        .and_then(|p| std::fs::read_to_string(root.join(p)).ok())
        .map_or(0, |c| c.lines().count());
    println!("[Turn 1] Seed: {} ({} lines)", seed_path, lines);

    // Resolve imports to lib.rs paths and queue them.
    let mut new_targets = Vec::new();
    for crate_name in &seed_imports {
        if let Some(lib_path) = crate_map.get(crate_name.as_str()) {
            if !explored.contains(lib_path) {
                queue.push_back(lib_path.clone());
                new_targets.push(crate_name.clone());
            }
        }
    }
    // Also queue sibling modules (pub mod declarations).
    for mod_path in &seed_modules {
        if root.join(mod_path).exists() && !explored.contains(mod_path) {
            queue.push_back(mod_path.clone());
        }
    }
    println!(
        "  Found {} imports: {}",
        seed_imports.len(),
        seed_imports.join(", ")
    );
    println!(
        "  Found {} modules. Queued {} new files.\n",
        seed_modules.len(),
        new_targets.len()
            + seed_modules
                .iter()
                .filter(|m| root.join(m).exists())
                .count()
    );

    // =======================================================================
    // Phase 2: Import-Driven Expansion
    // =======================================================================
    println!("--- Phase 2: Import-Driven Expansion ---\n");

    let mut turn = 2;
    let max_import_turns = 12;

    while let Some(rel_path) = queue.pop_front() {
        if turn > max_import_turns {
            break;
        }
        if explored.contains(&rel_path) {
            continue;
        }

        let result = ingest_file(
            &rel_path,
            &mut next_id,
            &mut explored,
            &mut path_to_id,
            &mut id_to_path,
            &mut fts_file_ids,
            &mut vec_file_ids,
            &mut vec_cache,
            &db,
            &fts_ext,
            &vector_ext,
            &empty_adj,
            &kw,
        );

        let (file_id, file_imports, file_modules) = match result {
            Some(r) => r,
            None => continue,
        };
        discovery_method.insert(file_id, "import");

        // Add IMPORTS edges from all files that import this crate.
        let crate_name = rel_path_to_crate_name(&rel_path);
        if let Some(cname) = &crate_name {
            for (existing_path, &existing_id) in &path_to_id {
                if existing_id == file_id {
                    continue;
                }
                let content = std::fs::read_to_string(root.join(existing_path)).unwrap_or_default();
                let their_imports = extract_imports_from_content(&content);
                if their_imports.contains(cname) {
                    let edge = (existing_id, file_id);
                    if !import_edges.contains(&edge) {
                        import_edges.push(edge);
                        let mut storage = db.storage().write().unwrap();
                        storage
                            .insert_row(
                                imports_table_id,
                                &[TypedValue::Int64(existing_id), TypedValue::Int64(file_id)],
                            )
                            .unwrap();
                    }
                }
            }
        }

        // Queue new imports from this file.
        let mut queued = 0;
        for imp_crate in &file_imports {
            if let Some(lib_path) = crate_map.get(imp_crate.as_str()) {
                if !explored.contains(lib_path) && !queue.contains(lib_path) {
                    queue.push_back(lib_path.clone());
                    queued += 1;
                }
            }
        }
        // Queue sibling modules (pub mod declarations).
        for mod_path in &file_modules {
            if root.join(mod_path).exists()
                && !explored.contains(mod_path)
                && !queue.contains(mod_path)
            {
                queue.push_back(mod_path.clone());
                queued += 1;
            }
        }

        // Also add edges from this file to its imports that are already explored.
        for imp_crate in &file_imports {
            if let Some(lib_path) = crate_map.get(imp_crate.as_str()) {
                if let Some(&target_id) = path_to_id.get(lib_path.as_str()) {
                    if target_id != file_id {
                        let edge = (file_id, target_id);
                        if !import_edges.contains(&edge) {
                            import_edges.push(edge);
                            let mut storage = db.storage().write().unwrap();
                            storage
                                .insert_row(
                                    imports_table_id,
                                    &[TypedValue::Int64(file_id), TypedValue::Int64(target_id)],
                                )
                                .unwrap();
                        }
                    }
                }
            }
        }

        println!("[Turn {}] Exploring: {} (via import)", turn, rel_path);
        println!(
            "  Found {} imports + {} modules, queued {} new. Graph: {} files, {} edges.",
            file_imports.len(),
            file_modules.len(),
            queued,
            explored.len(),
            import_edges.len()
        );

        // Every 3 turns, run PageRank on current graph.
        if turn % 3 == 0 && import_edges.len() >= 2 {
            let result = conn
                .query("CALL algo.pageRank(0.85, 20, 0.000001)")
                .unwrap();
            print!("  PageRank top 3: ");
            let top: Vec<String> = result
                .iter_rows()
                .take(3)
                .filter_map(|row| {
                    if let (TypedValue::Int64(nid), TypedValue::Double(rank)) = (&row[0], &row[1]) {
                        let name = id_to_path.get(nid).map_or("?", |s| s.as_str());
                        let short = short_name(name);
                        Some(format!("{short} ({rank:.3})"))
                    } else {
                        None
                    }
                })
                .collect();
            println!("{}", top.join(", "));
        }
        println!();

        turn += 1;
    }

    // =======================================================================
    // Phase 3: Emergent Discovery
    // =======================================================================
    println!("--- Phase 3: Emergent Discovery ---\n");

    // Collect all .rs files in workspace for potential discovery.
    let all_rs_files = find_all_rs_files(&root);

    let mut emergent_turn = turn;
    let max_emergent_turns = 4;
    let mut emergent_found = 0;

    // Strategy A: FTS — search for terms from the most-central file.
    let search_terms = [
        "query execution pipeline",
        "bind expression type",
        "storage column chunk",
    ];
    for query in search_terms {
        if emergent_found >= max_emergent_turns {
            break;
        }
        let results = fts_ext
            .execute("search", &[query.into(), "5".into()], &empty_adj)
            .unwrap();

        // The FTS results point to already-indexed files. Use the top result's content
        // to find *related but unexplored* files via vector similarity.
        if let Some(row) = results.first() {
            if let TypedValue::Int64(doc_id) = &row[0] {
                if let Some(&hub_id) = fts_file_ids.get(*doc_id as usize) {
                    if let Some(bow) = vec_cache.get(&hub_id) {
                        let csv = bow
                            .iter()
                            .map(|v| format!("{v}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        let vec_results = vector_ext
                            .execute("search", &[csv, "10".into()], &empty_adj)
                            .unwrap();

                        let hub_path = id_to_path.get(&hub_id).cloned().unwrap_or_default();

                        for vrow in &vec_results {
                            if emergent_found >= max_emergent_turns {
                                break;
                            }
                            if let (TypedValue::Int64(idx), TypedValue::Double(dist)) =
                                (&vrow[0], &vrow[1])
                            {
                                if let Some(&candidate_id) = vec_file_ids.get(*idx as usize) {
                                    let cand_path =
                                        id_to_path.get(&candidate_id).cloned().unwrap_or_default();
                                    // Already explored — but can we find a sibling file nearby?
                                    let dir = Path::new(&cand_path)
                                        .parent()
                                        .unwrap_or(Path::new(""))
                                        .to_string_lossy()
                                        .to_string();
                                    for rs_path in &all_rs_files {
                                        if emergent_found >= max_emergent_turns {
                                            break;
                                        }
                                        if explored.contains(rs_path) {
                                            continue;
                                        }
                                        if !rs_path.starts_with(&dir) {
                                            continue;
                                        }
                                        // Found an unexplored file near a similar explored file.
                                        let result = ingest_file(
                                            rs_path,
                                            &mut next_id,
                                            &mut explored,
                                            &mut path_to_id,
                                            &mut id_to_path,
                                            &mut fts_file_ids,
                                            &mut vec_file_ids,
                                            &mut vec_cache,
                                            &db,
                                            &fts_ext,
                                            &vector_ext,
                                            &empty_adj,
                                            &kw,
                                        );
                                        if let Some((new_id, _, _)) = result {
                                            discovery_method.insert(new_id, "emergent");
                                            similar_edges.push((new_id, candidate_id));
                                            let mut storage = db.storage().write().unwrap();
                                            storage
                                                .insert_row(
                                                    similar_table_id,
                                                    &[
                                                        TypedValue::Int64(new_id),
                                                        TypedValue::Int64(candidate_id),
                                                    ],
                                                )
                                                .unwrap();
                                            drop(storage);

                                            println!(
                                                "[Turn {}] Emergent: {} (similar to {}, dist={:.4})",
                                                emergent_turn,
                                                short_name(rs_path),
                                                short_name(&hub_path),
                                                dist
                                            );
                                            println!(
                                                "  Added SIMILAR_TO edge. Not in any import chain!"
                                            );
                                            emergent_found += 1;
                                            emergent_turn += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Strategy B: direct vector similarity from hub nodes to unexplored .rs files.
    // Collect candidates first (to avoid borrowing vec_file_ids during mutation).
    if emergent_found < max_emergent_turns && !vec_file_ids.is_empty() {
        let hub_ids: Vec<i64> = vec_file_ids.iter().take(3).copied().collect();
        let vec_snapshot: Vec<i64> = vec_file_ids.clone();
        let mut candidates: Vec<(String, i64)> = Vec::new(); // (rs_path, hub_id)

        for hub_id in &hub_ids {
            if candidates.len() >= max_emergent_turns - emergent_found {
                break;
            }
            if let Some(bow) = vec_cache.get(hub_id) {
                let csv = bow
                    .iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let vec_results = vector_ext
                    .execute("search", &[csv, "10".into()], &empty_adj)
                    .unwrap();

                'outer: for vrow in &vec_results {
                    if let TypedValue::Int64(idx) = &vrow[0] {
                        if let Some(&cand_id) = vec_snapshot.get(*idx as usize) {
                            if let Some(cand_path) = id_to_path.get(&cand_id) {
                                let dir = Path::new(cand_path)
                                    .parent()
                                    .unwrap_or(Path::new(""))
                                    .to_string_lossy()
                                    .to_string();
                                for rs_path in &all_rs_files {
                                    if explored.contains(rs_path) || !rs_path.starts_with(&dir) {
                                        continue;
                                    }
                                    candidates.push((rs_path.clone(), *hub_id));
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
        }

        for (rs_path, hub_id) in candidates {
            if emergent_found >= max_emergent_turns {
                break;
            }
            let result = ingest_file(
                &rs_path,
                &mut next_id,
                &mut explored,
                &mut path_to_id,
                &mut id_to_path,
                &mut fts_file_ids,
                &mut vec_file_ids,
                &mut vec_cache,
                &db,
                &fts_ext,
                &vector_ext,
                &empty_adj,
                &kw,
            );
            if let Some((new_id, _, _)) = result {
                discovery_method.insert(new_id, "emergent");
                similar_edges.push((new_id, hub_id));
                let mut storage = db.storage().write().unwrap();
                storage
                    .insert_row(
                        similar_table_id,
                        &[TypedValue::Int64(new_id), TypedValue::Int64(hub_id)],
                    )
                    .unwrap();
                drop(storage);

                let hub_name = id_to_path.get(&hub_id).map_or("?", |s| s.as_str());
                println!(
                    "[Turn {}] Emergent: {} (near {})",
                    emergent_turn,
                    short_name(&rs_path),
                    short_name(hub_name),
                );
                emergent_found += 1;
                emergent_turn += 1;
            }
        }
    }

    if emergent_found == 0 {
        println!("  (no emergent connections discovered — workspace may be too small)\n");
    } else {
        println!();
    }

    // =======================================================================
    // Phase 4: Knowledge Graph Analysis
    // =======================================================================
    println!("--- Phase 4: Knowledge Graph Analysis ---\n");

    // PageRank on full graph (imports + similar_to edges).
    if import_edges.len() + similar_edges.len() >= 2 {
        let result = conn
            .query("CALL algo.pageRank(0.85, 20, 0.000001)")
            .unwrap();
        println!("PageRank (import + emergent graph):");
        for (rank, row) in result.iter_rows().take(8).enumerate() {
            if let (TypedValue::Int64(nid), TypedValue::Double(score)) = (&row[0], &row[1]) {
                let path = id_to_path.get(nid).map_or("?", |s| s.as_str());
                let method = discovery_method.get(nid).unwrap_or(&"?");
                println!(
                    "  #{:>2} rank={score:.6}  {} [{}]",
                    rank + 1,
                    short_name(path),
                    method
                );
            }
        }
        println!();

        // Betweenness centrality — bridge files.
        let result = conn.query("CALL algo.betweenness()").unwrap();
        println!("Betweenness centrality (bridge files):");
        for (rank, row) in result.iter_rows().take(5).enumerate() {
            if let (TypedValue::Int64(nid), TypedValue::Double(cent)) = (&row[0], &row[1]) {
                let path = id_to_path.get(nid).map_or("?", |s| s.as_str());
                println!(
                    "  #{:>2} centrality={cent:.2}  {}",
                    rank + 1,
                    short_name(path)
                );
            }
        }
        println!();
    }

    // WCC — connected clusters.
    if explored.len() >= 2 {
        let result = conn.query("CALL algo.wcc()").unwrap();
        let mut components: HashMap<i64, Vec<i64>> = HashMap::new();
        for row in result.iter_rows() {
            if let (TypedValue::Int64(nid), TypedValue::Int64(comp)) = (&row[0], &row[1]) {
                components.entry(*comp).or_default().push(*nid);
            }
        }
        let mut sorted: Vec<_> = components.values().collect();
        sorted.sort_by(|a, b| b.len().cmp(&a.len()));
        println!("WCC: {} cluster(s)", sorted.len());
        for (i, members) in sorted.iter().take(3).enumerate() {
            let sample: Vec<_> = members
                .iter()
                .take(3)
                .filter_map(|id| id_to_path.get(id).map(|s| short_name(s)))
                .collect();
            println!(
                "  Cluster {}: {} files (e.g., {})",
                i + 1,
                members.len(),
                sample.join(", ")
            );
        }
        println!();
    }

    // Summary.
    let import_count = discovery_method
        .values()
        .filter(|&&m| m == "import")
        .count();
    let emergent_count = discovery_method
        .values()
        .filter(|&&m| m == "emergent")
        .count();
    println!("=== Summary ===");
    println!(
        "Explored {} files in {} turns.",
        explored.len(),
        emergent_turn - 1
    );
    println!(
        "  1 seed, {} via imports, {} via emergent discovery.",
        import_count, emergent_count
    );
    println!(
        "  {} import edges, {} SIMILAR_TO edges.",
        import_edges.len(),
        similar_edges.len()
    );
    println!("\nDone.");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a map: crate_name (e.g. "kyu_types") → relative path to lib.rs.
fn build_crate_map(root: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for dir_name in ["crates", "extensions"] {
        let dir = root.join(dir_name);
        if !dir.is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let lib_path = entry.path().join("src").join("lib.rs");
            if lib_path.exists() {
                let crate_dir = entry.file_name().to_string_lossy().to_string();
                let crate_name = crate_dir.replace('-', "_");
                let rel = lib_path
                    .strip_prefix(root)
                    .unwrap_or(&lib_path)
                    .to_string_lossy()
                    .to_string();
                map.insert(crate_name, rel);
            }
        }
    }
    map
}

/// Extract crate names from `use crate_name::...` lines in file content.
fn extract_imports_from_content(content: &str) -> Vec<String> {
    let mut imports = Vec::new();
    let mut seen = HashSet::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("use ") {
            continue;
        }
        let rest = trimmed.strip_prefix("use ").unwrap_or("");
        let crate_name = rest.split("::").next().unwrap_or("").trim();
        if !crate_name.is_empty() && seen.insert(crate_name.to_string()) {
            imports.push(crate_name.to_string());
        }
    }
    imports
}

/// Extract `pub mod foo` / `mod foo` declarations → sibling file paths.
fn extract_modules(content: &str, parent_dir: &str) -> Vec<String> {
    let mut modules = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        let mod_name = if let Some(rest) = trimmed.strip_prefix("pub mod ") {
            rest.trim_end_matches(';').trim()
        } else if let Some(rest) = trimmed.strip_prefix("mod ") {
            rest.trim_end_matches(';').trim()
        } else {
            continue;
        };
        // Skip inline modules (those with { ).
        if mod_name.contains('{') || mod_name.contains(' ') {
            continue;
        }
        // Sibling file: parent_dir/mod_name.rs
        let sibling = format!("{parent_dir}/{mod_name}.rs");
        modules.push(sibling);
    }
    modules
}

/// Extract crate name from a lib.rs path, e.g. "crates/kyu-types/src/lib.rs" → "kyu_types".
fn rel_path_to_crate_name(rel_path: &str) -> Option<String> {
    let parts: Vec<&str> = rel_path.split('/').collect();
    if parts.len() >= 3 && parts[parts.len() - 1] == "lib.rs" && parts[parts.len() - 2] == "src" {
        let crate_dir = parts[parts.len() - 3];
        Some(crate_dir.replace('-', "_"))
    } else {
        None
    }
}

/// Find all .rs files in the workspace (for emergent discovery candidates).
fn find_all_rs_files(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name == "target" || name == "node_modules" {
                continue;
            }
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() && name.ends_with(".rs") {
                let rel = entry
                    .path()
                    .strip_prefix(root)
                    .unwrap_or(&entry.path())
                    .to_string_lossy()
                    .to_string();
                files.push(rel);
            }
        }
    }
    files.sort();
    files
}

/// Shorten a path for display: "crates/kyu-types/src/lib.rs" → "kyu-types/src/lib.rs"
fn short_name(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("crates/") {
        rest.to_string()
    } else if let Some(rest) = path.strip_prefix("extensions/") {
        rest.to_string()
    } else {
        path.to_string()
    }
}

fn keywords() -> Vec<&'static str> {
    vec![
        "fn", "struct", "impl", "trait", "enum", "pub", "mod", "use", "async", "unsafe", "test",
        "error", "result", "option", "vec", "string", "hash", "map", "arc", "mutex", "query",
        "parse", "execute", "table", "column", "row", "index", "type", "value", "node",
    ]
}

fn bag_of_words(text: &str, keywords: &[&str]) -> Vec<f32> {
    let lower = text.to_lowercase();
    // Use log-scaled term frequency instead of binary presence for finer-grained similarity.
    keywords
        .iter()
        .map(|kw| {
            let count = lower.matches(kw).count();
            if count == 0 {
                0.0
            } else {
                1.0 + (count as f32).ln()
            }
        })
        .collect()
}
