//! RDF Knowledge Graph visualizer example.
//!
//! Seeds an in-memory KyuGraph database from a FOAF + schema.org Turtle file
//! and opens the interactive visualizer. No manual schema creation needed —
//! `LOAD FROM` auto-infers all node tables and relationship tables from the
//! RDF triples following Linked Data Principles.
//!
//! Run with: cargo run -p kyu-visualizer --example rdf_kg

const RESEARCH_TTL: &str = r#"
@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix foaf:   <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:     <https://example.org/> .

# ── Researchers ──────────────────────────────────────────────────────────────

ex:alice  a foaf:Person ; foaf:name "Alice Nakamura" ; schema:age "34"^^xsd:integer .
ex:bob    a foaf:Person ; foaf:name "Bob Okonkwo"    ; schema:age "41"^^xsd:integer .
ex:carol  a foaf:Person ; foaf:name "Carol Ferreira" ; schema:age "29"^^xsd:integer .
ex:diana  a foaf:Person ; foaf:name "Diana Kowalski" ; schema:age "38"^^xsd:integer .

ex:alice foaf:knows ex:bob, ex:carol .
ex:bob   foaf:knows ex:alice, ex:diana .
ex:carol foaf:knows ex:alice .
ex:diana foaf:knows ex:bob, ex:carol .

# ── Institutions ─────────────────────────────────────────────────────────────

ex:mit      a schema:CollegeOrUniversity ; schema:name "MIT"                  ; schema:location "Cambridge, MA" .
ex:stanford a schema:CollegeOrUniversity ; schema:name "Stanford University"  ; schema:location "Stanford, CA" .
ex:eth      a schema:CollegeOrUniversity ; schema:name "ETH Zürich"           ; schema:location "Zürich, Switzerland" .
ex:oxford   a schema:CollegeOrUniversity ; schema:name "University of Oxford" ; schema:location "Oxford, UK" .

ex:alice schema:affiliation ex:mit .
ex:bob   schema:affiliation ex:stanford .
ex:carol schema:affiliation ex:eth .
ex:diana schema:affiliation ex:oxford .

# ── Publications ─────────────────────────────────────────────────────────────

ex:paper1 a schema:ScholarlyArticle ; schema:name "Graph Neural Networks for Knowledge Representation"    ; schema:datePublished "2023"^^xsd:integer .
ex:paper2 a schema:ScholarlyArticle ; schema:name "Scalable RDF Processing with Property Graph Semantics" ; schema:datePublished "2024"^^xsd:integer .
ex:paper3 a schema:ScholarlyArticle ; schema:name "Linked Data Principles in Distributed Graph Databases" ; schema:datePublished "2022"^^xsd:integer .

ex:alice schema:author ex:paper1, ex:paper3 .
ex:bob   schema:author ex:paper1, ex:paper2 .
ex:carol schema:author ex:paper2 .
ex:diana schema:author ex:paper3 .
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut db = kyu_api::Database::in_memory();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    db.register_extension(Box::new(ext_fts::FtsExtension::new()));
    db.register_extension(Box::new(ext_rdf::RdfExtension::new()));
    db.register_extension(Box::new(ext_vector::VectorExtension::new()));

    // Write Turtle to a temp file and import via LOAD FROM.
    let tmp = std::env::temp_dir().join("kyu_viz_rdf");
    let _ = std::fs::create_dir_all(&tmp);
    let path = tmp.join("research.ttl");
    std::fs::write(&path, RESEARCH_TTL)?;

    let conn = db.connect();
    conn.query(&format!("LOAD FROM '{}'", path.display()))?;
    tracing::info!("RDF knowledge graph loaded — opening visualizer");
    drop(conn);

    kyu_visualizer::launch(db)
}
