//! Seed a test database with sample graph data for the visualizer.

use std::path::Path;

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: seed_db <db_path>");
        std::process::exit(1);
    });

    let mut db = kyu_api::Database::open(Path::new(&path)).unwrap();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    db.register_extension(Box::new(ext_fts::FtsExtension::new()));
    db.register_extension(Box::new(ext_vector::VectorExtension::new()));

    let conn = db.connect();

    // Phase 1: Schema + nodes + cities.
    let schema_and_nodes = [
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

    for q in &schema_and_nodes {
        match conn.query(q) {
            Ok(_) => eprintln!("OK: {q}"),
            Err(e) => {
                eprintln!("ERR: {q} — {e}");
                std::process::exit(1);
            }
        }
    }

    // Phase 2: Relationships via chained MATCH clauses.
    seed_relationships(&conn);

    eprintln!("Done seeding {path}");
}

fn seed_relationships(conn: &kyu_api::Connection) {
    // KNOWS relationships.
    let knows = [
        ("Alice", "Bob", 2020),
        ("Alice", "Carol", 2019),
        ("Bob", "Dave", 2021),
        ("Carol", "Eve", 2022),
        ("Dave", "Eve", 2023),
    ];
    for (from, to, since) in &knows {
        let q = format!(
            "MATCH (a:Person) WHERE a.name = '{from}' \
             MATCH (b:Person) WHERE b.name = '{to}' \
             CREATE (a)-[:KNOWS {{since: {since}}}]->(b)"
        );
        match conn.query(&q) {
            Ok(_) => eprintln!("OK: {from} -[KNOWS {{since: {since}}}]-> {to}"),
            Err(e) => eprintln!("ERR: KNOWS {from}->{to} — {e}"),
        }
    }

    // LIVES_IN relationships.
    let lives_in = [
        ("Alice", "Tokyo"),
        ("Bob", "London"),
        ("Carol", "Paris"),
        ("Dave", "Tokyo"),
        ("Eve", "London"),
    ];
    for (person, city) in &lives_in {
        let q = format!(
            "MATCH (a:Person) WHERE a.name = '{person}' \
             MATCH (c:City) WHERE c.name = '{city}' \
             CREATE (a)-[:LIVES_IN]->(c)"
        );
        match conn.query(&q) {
            Ok(_) => eprintln!("OK: {person} -[LIVES_IN]-> {city}"),
            Err(e) => eprintln!("ERR: LIVES_IN {person}->{city} — {e}"),
        }
    }
}
