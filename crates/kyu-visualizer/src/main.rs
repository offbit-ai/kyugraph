//! kyu-viz binary: interactive graph visualizer for KyuGraph databases.
//!
//! Starts with an empty in-memory database. Use one of the seed examples to
//! pre-load data, or type Cypher in the query bar to build the graph live.
//!
//! Run with: cargo run -p kyu-visualizer
//! RDF example: cargo run -p kyu-visualizer --example rdf_kg

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut db = kyu_api::Database::in_memory();
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    db.register_extension(Box::new(ext_fts::FtsExtension::new()));
    db.register_extension(Box::new(ext_vector::VectorExtension::new()));

    kyu_visualizer::launch(db)
}
