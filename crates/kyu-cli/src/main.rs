//! kyu-cli: interactive Cypher shell for KyuGraph.

use std::path::PathBuf;
use std::time::Instant;

use kyu_api::Database;
use reedline::{
    DefaultPrompt, DefaultPromptSegment, FileBackedHistory, Reedline, Signal,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse --path <dir> argument for persistent database.
    let db_path = parse_db_path(&args);

    let db = match &db_path {
        Some(path) => match Database::open(path.as_path()) {
            Ok(db) => {
                println!(
                    "KyuGraph v0.1.0 — Interactive Cypher Shell ({})",
                    path.display()
                );
                db
            }
            Err(e) => {
                eprintln!("Error opening database at '{}': {e}", path.display());
                std::process::exit(1);
            }
        },
        None => {
            println!("KyuGraph v0.1.0 — Interactive Cypher Shell (in-memory)");
            Database::in_memory()
        }
    };

    println!("Type :help for help, :quit to exit.\n");
    let conn = db.connect();

    // Set up reedline with persistent history.
    let mut line_editor = match history_path() {
        Some(path) => {
            let history = FileBackedHistory::with_file(1000, path)
                .expect("cannot open history file");
            Reedline::create().with_history(Box::new(history))
        }
        None => Reedline::create(),
    };

    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("kyugraph".to_string()),
        DefaultPromptSegment::Empty,
    );

    let mut buffer = String::new();

    loop {
        let sig = line_editor.read_line(&prompt);
        match sig {
            Ok(Signal::Success(line)) => {
                let trimmed = line.trim();

                // Meta-commands (colon prefix).
                if trimmed.starts_with(':') {
                    let handled = handle_meta_command(trimmed, &db);
                    match handled {
                        MetaResult::Continue => continue,
                        MetaResult::Quit => break,
                        MetaResult::NotMeta => {} // fall through to query
                    }
                }

                if trimmed.is_empty() {
                    continue;
                }

                // Accumulate multi-line input until semicolon.
                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push_str(trimmed);

                if !buffer.ends_with(';') {
                    continue;
                }

                // Strip trailing semicolon.
                let query = buffer.trim_end_matches(';').trim();
                if query.is_empty() {
                    buffer.clear();
                    continue;
                }

                let start = Instant::now();
                match conn.query(query) {
                    Ok(result) => {
                        let elapsed = start.elapsed();
                        if result.num_columns() > 0 {
                            print!("{result}");
                        } else {
                            println!("OK");
                        }
                        println!("({:.1}ms)\n", elapsed.as_secs_f64() * 1000.0);
                    }
                    Err(e) => {
                        eprintln!("Error: {e}\n");
                    }
                }

                buffer.clear();
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Input error: {e}");
                break;
            }
        }
    }
}

// ---- Argument parsing ----

fn parse_db_path(args: &[String]) -> Option<PathBuf> {
    // Support: kyu-cli --path <dir>  or  kyu-cli <dir>
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--path" || args[i] == "-p" {
            if i + 1 < args.len() {
                return Some(PathBuf::from(&args[i + 1]));
            } else {
                eprintln!("Error: --path requires a directory argument");
                std::process::exit(1);
            }
        }
        if args[i] == "--help" || args[i] == "-h" {
            print_usage();
            std::process::exit(0);
        }
        // Bare positional argument = database path.
        if !args[i].starts_with('-') {
            return Some(PathBuf::from(&args[i]));
        }
        i += 1;
    }
    None
}

fn print_usage() {
    println!("Usage: kyu-cli [OPTIONS] [DATABASE_PATH]");
    println!();
    println!("Arguments:");
    println!("  [DATABASE_PATH]       Path to persistent database directory");
    println!();
    println!("Options:");
    println!("  -p, --path <DIR>      Path to persistent database directory");
    println!("  -h, --help            Print this help message");
    println!();
    println!("If no path is given, an in-memory database is used.");
}

// ---- History ----

fn history_path() -> Option<PathBuf> {
    dirs_or_home().map(|h| h.join(".kyu_history"))
}

fn dirs_or_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

// ---- Meta-commands ----

enum MetaResult {
    Continue,
    Quit,
    NotMeta,
}

fn handle_meta_command(cmd: &str, db: &Database) -> MetaResult {
    let lower = cmd.to_lowercase();
    let parts: Vec<&str> = lower.split_whitespace().collect();

    match parts[0] {
        ":quit" | ":exit" | ":q" => {
            println!("Bye!");
            MetaResult::Quit
        }
        ":help" | ":h" => {
            print_help();
            MetaResult::Continue
        }
        ":tables" => {
            print_tables(db);
            MetaResult::Continue
        }
        ":schema" => {
            let table_name = if parts.len() > 1 { Some(cmd.split_whitespace().nth(1).unwrap()) } else { None };
            print_schema(db, table_name);
            MetaResult::Continue
        }
        ":stats" => {
            print_stats(db);
            MetaResult::Continue
        }
        _ => MetaResult::NotMeta,
    }
}

fn print_help() {
    println!("Commands:");
    println!("  :help            Show this help");
    println!("  :quit            Exit the shell");
    println!("  :tables          List all tables");
    println!("  :schema [TABLE]  Show schema (all tables or specific table)");
    println!("  :stats           Show database statistics");
    println!();
    println!("Enter Cypher queries terminated with ';'.");
    println!("Multi-line input is supported — the shell accumulates");
    println!("lines until it sees a semicolon.");
    println!();
    println!("Examples:");
    println!("  CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id));");
    println!("  MATCH (p:Person) RETURN p.name;");
    println!("  COPY Person FROM '/path/to/data.parquet';");
    println!("  RETURN 1 + 2 AS sum;");
    println!("  CHECKPOINT;");
    println!();
}

fn print_tables(db: &Database) {
    let catalog = db.catalog().read();
    let nodes = catalog.node_tables();
    let rels = catalog.rel_tables();

    if nodes.is_empty() && rels.is_empty() {
        println!("No tables.\n");
        return;
    }

    if !nodes.is_empty() {
        println!("Node tables:");
        for entry in &nodes {
            println!(
                "  {} ({} properties, {} rows)",
                entry.name,
                entry.properties.len(),
                entry.num_rows
            );
        }
    }
    if !rels.is_empty() {
        println!("Relationship tables:");
        for entry in &rels {
            let from_name = catalog
                .find_by_id(entry.from_table_id)
                .map(|e| e.name().to_string())
                .unwrap_or_else(|| format!("{:?}", entry.from_table_id));
            let to_name = catalog
                .find_by_id(entry.to_table_id)
                .map(|e| e.name().to_string())
                .unwrap_or_else(|| format!("{:?}", entry.to_table_id));
            println!(
                "  {} (FROM {} TO {}, {} properties, {} rows)",
                entry.name,
                from_name,
                to_name,
                entry.properties.len(),
                entry.num_rows
            );
        }
    }
    println!();
}

fn print_schema(db: &Database, table_name: Option<&str>) {
    let catalog = db.catalog().read();

    match table_name {
        Some(name) => {
            let entry = catalog.find_by_name(name);
            match entry {
                Some(entry) => {
                    print_entry_schema(entry);
                }
                None => {
                    eprintln!("Table '{}' not found.\n", name);
                }
            }
        }
        None => {
            let nodes = catalog.node_tables();
            let rels = catalog.rel_tables();

            if nodes.is_empty() && rels.is_empty() {
                println!("No tables.\n");
                return;
            }

            for entry in &nodes {
                print_node_schema(entry);
            }
            for entry in &rels {
                print_rel_schema(entry, &catalog);
            }
        }
    }
}

fn print_entry_schema(entry: &kyu_catalog::CatalogEntry) {
    match entry {
        kyu_catalog::CatalogEntry::NodeTable(n) => print_node_schema(n),
        kyu_catalog::CatalogEntry::RelTable(r) => {
            // We need catalog for from/to names but don't have it here.
            // Just print what we can.
            println!("REL TABLE {} {{", r.name);
            for prop in &r.properties {
                println!("  {} {}", prop.name, prop.data_type.type_name());
            }
            println!("}}\n");
        }
    }
}

fn print_node_schema(entry: &kyu_catalog::NodeTableEntry) {
    println!("NODE TABLE {} {{", entry.name);
    for (i, prop) in entry.properties.iter().enumerate() {
        let pk = if i == entry.primary_key_idx {
            " [PRIMARY KEY]"
        } else {
            ""
        };
        println!("  {} {}{}", prop.name, prop.data_type.type_name(), pk);
    }
    println!("}}\n");
}

fn print_rel_schema(entry: &kyu_catalog::RelTableEntry, catalog: &kyu_catalog::CatalogContent) {
    let from_name = catalog
        .find_by_id(entry.from_table_id)
        .map(|e| e.name().to_string())
        .unwrap_or_else(|| "?".to_string());
    let to_name = catalog
        .find_by_id(entry.to_table_id)
        .map(|e| e.name().to_string())
        .unwrap_or_else(|| "?".to_string());
    println!("REL TABLE {} (FROM {} TO {}) {{", entry.name, from_name, to_name);
    for prop in &entry.properties {
        println!("  {} {}", prop.name, prop.data_type.type_name());
    }
    println!("}}\n");
}

fn print_stats(db: &Database) {
    let catalog = db.catalog().read();
    let nodes = catalog.node_tables();
    let rels = catalog.rel_tables();

    println!("Database statistics:");
    println!("  Node tables:         {}", nodes.len());
    println!("  Relationship tables: {}", rels.len());
    println!(
        "  Total node rows:     {}",
        nodes.iter().map(|n| n.num_rows).sum::<u64>()
    );
    println!(
        "  Total rel rows:      {}",
        rels.iter().map(|r| r.num_rows).sum::<u64>()
    );
    println!("  Catalog version:     {}", db.catalog().version());
    println!();
}
