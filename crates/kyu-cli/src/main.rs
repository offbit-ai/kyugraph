//! kyu-cli: interactive Cypher shell for KyuGraph.

use std::time::Instant;

use kyu_api::Database;
use reedline::{DefaultPrompt, DefaultPromptSegment, Reedline, Signal};

fn main() {
    println!("KyuGraph v0.1.0 — Interactive Cypher Shell");
    println!("Type :help for help, :quit to exit.\n");

    let db = Database::in_memory();
    let conn = db.connect();

    let mut line_editor = Reedline::create();
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

                // Meta-commands.
                if trimmed.eq_ignore_ascii_case(":quit") || trimmed.eq_ignore_ascii_case(":exit") {
                    println!("Bye!");
                    break;
                }
                if trimmed.eq_ignore_ascii_case(":help") {
                    print_help();
                    continue;
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

fn print_help() {
    println!("Commands:");
    println!("  :help   — Show this help");
    println!("  :quit   — Exit the shell");
    println!();
    println!("Enter Cypher queries terminated with ';'.");
    println!("Multi-line input is supported — the shell accumulates");
    println!("lines until it sees a semicolon.");
    println!();
    println!("Examples:");
    println!("  CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id));");
    println!("  MATCH (p:Person) RETURN p.name;");
    println!("  RETURN 1 + 2 AS sum;");
    println!();
}
