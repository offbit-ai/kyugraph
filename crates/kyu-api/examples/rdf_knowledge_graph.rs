//! RDF Knowledge Graph Example — demonstrates LOAD FROM with Linked Data.
//!
//! Writes a FOAF-style Turtle file describing researchers, organisations, and
//! publications, then uses `LOAD FROM` to auto-import it into KyuGraph —
//! no manual CREATE TABLE or COPY FROM needed.
//!
//! Showcases:
//! 1. RDF inspection with `CALL rdf.stats / rdf.types / rdf.prefixes`
//! 2. `LOAD FROM 'file.ttl'` — automatic schema + data import
//! 3. Cypher queries over the resulting property graph
//!
//! Run with: cargo run -p kyu-api --example rdf_knowledge_graph

use kyu_api::Database;

// ---------------------------------------------------------------------------
// Sample Turtle dataset — academic research community in FOAF + schema.org
// ---------------------------------------------------------------------------

const FOAF_TTL: &str = r#"
@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix foaf:   <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:     <https://example.org/> .

# ── People ───────────────────────────────────────────────────────────────────

ex:alice
    a foaf:Person ;
    foaf:name    "Alice Nakamura" ;
    foaf:mbox    "alice@mit.edu" ;
    schema:age   "34"^^xsd:integer ;
    foaf:knows   ex:bob, ex:carol .

ex:bob
    a foaf:Person ;
    foaf:name    "Bob Okonkwo" ;
    foaf:mbox    "bob@stanford.edu" ;
    schema:age   "41"^^xsd:integer ;
    foaf:knows   ex:alice, ex:diana .

ex:carol
    a foaf:Person ;
    foaf:name    "Carol Ferreira" ;
    foaf:mbox    "carol@eth.ch" ;
    schema:age   "29"^^xsd:integer ;
    foaf:knows   ex:alice .

ex:diana
    a foaf:Person ;
    foaf:name    "Diana Kowalski" ;
    foaf:mbox    "diana@oxford.ac.uk" ;
    schema:age   "38"^^xsd:integer ;
    foaf:knows   ex:bob, ex:carol .

# ── Organisations ─────────────────────────────────────────────────────────────

ex:mit
    a schema:CollegeOrUniversity ;
    schema:name "MIT" ;
    schema:location "Cambridge, MA" .

ex:stanford
    a schema:CollegeOrUniversity ;
    schema:name "Stanford University" ;
    schema:location "Stanford, CA" .

ex:eth
    a schema:CollegeOrUniversity ;
    schema:name "ETH Zürich" ;
    schema:location "Zürich, Switzerland" .

ex:oxford
    a schema:CollegeOrUniversity ;
    schema:name "University of Oxford" ;
    schema:location "Oxford, UK" .

# ── Affiliations (Person → Organisation) ─────────────────────────────────────

ex:alice  schema:affiliation ex:mit .
ex:bob    schema:affiliation ex:stanford .
ex:carol  schema:affiliation ex:eth .
ex:diana  schema:affiliation ex:oxford .

# ── Publications ─────────────────────────────────────────────────────────────

ex:paper1
    a schema:ScholarlyArticle ;
    schema:name "Graph Neural Networks for Knowledge Representation" ;
    schema:datePublished "2023"^^xsd:integer .

ex:paper2
    a schema:ScholarlyArticle ;
    schema:name "Scalable RDF Processing with Property Graph Semantics" ;
    schema:datePublished "2024"^^xsd:integer .

ex:paper3
    a schema:ScholarlyArticle ;
    schema:name "Linked Data Principles in Distributed Graph Databases" ;
    schema:datePublished "2022"^^xsd:integer .

# ── Authorship (Person → Publication) ────────────────────────────────────────

ex:alice  schema:author ex:paper1, ex:paper3 .
ex:bob    schema:author ex:paper1, ex:paper2 .
ex:carol  schema:author ex:paper2 .
ex:diana  schema:author ex:paper3 .
"#;

fn main() {
    println!("=== KyuGraph RDF Knowledge Graph Example ===\n");

    // Write the Turtle file to a temp path.
    let tmp = std::env::temp_dir().join("kyu_rdf_example");
    let _ = std::fs::create_dir_all(&tmp);
    let ttl_path = tmp.join("research.ttl");
    std::fs::write(&ttl_path, FOAF_TTL).unwrap();
    let ttl_str = ttl_path.to_str().unwrap();

    // Create a database and register the RDF extension.
    let mut db = Database::in_memory();
    db.register_extension(Box::new(ext_rdf::RdfExtension::new()));
    let conn = db.connect();

    // -----------------------------------------------------------------------
    // Stage 1: Inspect the RDF file before importing
    // -----------------------------------------------------------------------
    println!("--- Stage 1: RDF Inspection ---");

    let stats = conn.query(&format!("CALL rdf.stats('{ttl_str}')")).unwrap();
    for row in stats.iter_rows() {
        println!(
            "  triples: {}  subjects: {}  predicates: {}  rdf:type triples: {}",
            row[0], row[1], row[2], row[3]
        );
    }
    println!();

    let types = conn.query(&format!("CALL rdf.types('{ttl_str}')")).unwrap();
    println!("RDF types found:");
    for row in types.iter_rows() {
        println!("  {:45} → {}", row[0], row[1]);
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 2: Import — LOAD FROM auto-creates all tables
    // -----------------------------------------------------------------------
    println!("--- Stage 2: LOAD FROM (auto-schema import) ---");

    conn.query(&format!("LOAD FROM '{ttl_str}'")).unwrap();

    println!("Imported! Tables created automatically from RDF schema.\n");

    // -----------------------------------------------------------------------
    // Stage 3: Explore the imported schema
    // -----------------------------------------------------------------------
    println!("--- Stage 3: Imported Node Tables ---");

    for table in &["Person", "CollegeOrUniversity", "ScholarlyArticle"] {
        let result = conn
            .query(&format!("MATCH (n:{table}) RETURN n.uri"))
            .unwrap();
        println!("  {table}: {} nodes", result.num_rows());
        for row in result.iter_rows() {
            // Print just the local name for readability.
            let uri = row[0].to_string();
            let local = uri.rsplit_once('/').map_or(uri.as_str(), |(_, n)| n);
            println!("    <{local}>");
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 4: Cypher queries
    // -----------------------------------------------------------------------
    println!("--- Stage 4: Cypher Queries ---");

    // Who knows whom?
    let result = conn
        .query(
            "MATCH (a:Person)-[:knows]->(b:Person)
             RETURN a.name, b.name",
        )
        .unwrap();
    println!(
        "Social network — knows relationships ({} edges):",
        result.num_rows()
    );
    for row in result.iter_rows() {
        println!("  {} → {}", row[0], row[1]);
    }
    println!();

    // Researchers and their institutions.
    let result = conn
        .query(
            "MATCH (p:Person)-[:affiliation]->(o:CollegeOrUniversity)
             RETURN p.name, o.name, o.location",
        )
        .unwrap();
    println!("Researcher affiliations:");
    for row in result.iter_rows() {
        println!("  {} @ {} ({})", row[0], row[1], row[2]);
    }
    println!();

    // Papers and their authors.
    let result = conn
        .query(
            "MATCH (p:Person)-[:author]->(a:ScholarlyArticle)
             RETURN a.name, p.name, a.datePublished",
        )
        .unwrap();
    println!("Publications ({} author–paper links):", result.num_rows());
    for row in result.iter_rows() {
        println!("  \"{}\" — {} ({})", row[0], row[1], row[2]);
    }
    println!();

    // Two-hop: co-authors — collect per-paper author lists in Rust.
    let result = conn
        .query(
            "MATCH (a:Person)-[:author]->(paper:ScholarlyArticle)
             RETURN a.name, paper.name",
        )
        .unwrap();
    let mut paper_authors: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for row in result.iter_rows() {
        paper_authors
            .entry(row[1].to_string())
            .or_default()
            .push(row[0].to_string());
    }
    println!("Co-author pairs by paper:");
    for (paper, authors) in &paper_authors {
        if authors.len() > 1 {
            println!("  \"{paper}\"");
            println!("    authors: {}", authors.join(", "));
        }
    }
    println!();

    // People reachable via knows (direct + indirect).
    let result = conn
        .query(
            "MATCH (a:Person {name: 'Alice Nakamura'})-[:knows*1..2]->(b:Person)
             RETURN DISTINCT b.name",
        )
        .unwrap();
    println!("Reachable from Alice (1–2 hops via knows):");
    for row in result.iter_rows() {
        println!("  {}", row[0]);
    }
    println!();

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------
    let _ = std::fs::remove_dir_all(&tmp);
    println!("Done.");
}
