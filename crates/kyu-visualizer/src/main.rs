//! kyu-viz: Interactive graph visualizer for KyuGraph databases.

#![allow(dead_code)]

use blinc_app::WindowConfig;
use blinc_app::windowed::WindowedApp;
use kyu_api::Database;

mod app;
mod canvas;
mod graph;
mod state;
mod theme;
mod transitions;
mod ui;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // In-memory database, seeded with sample data.
    let mut db = Database::in_memory();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    db.register_extension(Box::new(ext_fts::FtsExtension::new()));
    db.register_extension(Box::new(ext_vector::VectorExtension::new()));

    // seed_database(&db); // uncomment to seed sample data for debugging

    let config = WindowConfig {
        title: "KyuGraph Visualizer".to_string(),
        width: 1280,
        height: 800,
        ..Default::default()
    };

    let db = std::sync::Arc::new(db);

    // ── Pre-load schema and initial graph before entering the render loop ──
    let initial = app::load_initial_data(&db);

    let mut css_loaded = false;
    WindowedApp::run(config, move |ctx| {
        if !css_loaded {
            ctx.add_css(include_str!("../style.css"));
            css_loaded = true;
        }
        let db = db.clone();
        app::build_ui(ctx, db, &initial)
    })?;
    Ok(())
}

// ── Seed database with sample graph data ──

fn seed_database(db: &Database) {
    let conn = db.connect();

    // Schema + nodes.
    let stmts = [
        "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))",
        "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY(name))",
        "CREATE REL TABLE LIVES_IN(FROM Person TO City)",
        "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)",
        "CREATE (p:Person {name: 'Alice', age: 30})",
        "CREATE (p:Person {name: 'Bob', age: 25})",
        "CREATE (p:Person {name: 'Carol', age: 35})",
        "CREATE (p:Person {name: 'Dave', age: 28})",
        "CREATE (p:Person {name: 'Eve', age: 32})",
        "CREATE (c:City {name: 'Tokyo', population: 14000000})",
        "CREATE (c:City {name: 'London', population: 9000000})",
        "CREATE (c:City {name: 'Paris', population: 2200000})",
    ];
    for q in &stmts {
        match conn.query(q) {
            Ok(_) => eprintln!("  OK: {q}"),
            Err(e) => eprintln!("  ERR: {q} — {e}"),
        }
    }

    // KNOWS relationships.
    for (from, to, since) in [
        ("Alice", "Bob", 2020),
        ("Alice", "Carol", 2019),
        ("Bob", "Dave", 2021),
        ("Carol", "Eve", 2022),
        ("Dave", "Eve", 2023),
    ] {
        let q = format!(
            "MATCH (a:Person) WHERE a.name = '{from}' \
             MATCH (b:Person) WHERE b.name = '{to}' \
             CREATE (a)-[:KNOWS {{since: {since}}}]->(b)"
        );
        match conn.query(&q) {
            Ok(_) => eprintln!("  OK: {from} -[KNOWS]-> {to}"),
            Err(e) => eprintln!("  ERR: KNOWS {from}->{to} — {e}"),
        }
    }

    // LIVES_IN relationships.
    for (person, city) in [
        ("Alice", "Tokyo"),
        ("Bob", "London"),
        ("Carol", "Paris"),
        ("Dave", "Tokyo"),
        ("Eve", "London"),
    ] {
        let q = format!(
            "MATCH (a:Person) WHERE a.name = '{person}' \
             MATCH (c:City) WHERE c.name = '{city}' \
             CREATE (a)-[:LIVES_IN]->(c)"
        );
        match conn.query(&q) {
            Ok(_) => eprintln!("  OK: {person} -[LIVES_IN]-> {city}"),
            Err(e) => eprintln!("  ERR: LIVES_IN {person}->{city} — {e}"),
        }
    }

    eprintln!("Seeding complete.");
}
