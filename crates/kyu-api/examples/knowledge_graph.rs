//! Knowledge Graph Example — demonstrates KyuGraph's capabilities in a single workflow.
//!
//! Builds a research paper knowledge graph and showcases:
//! 1. Schema creation via Cypher DDL
//! 2. CSV bulk ingestion via COPY FROM
//! 3. Graph queries with filtering
//! 4. Full-text search (ext-fts)
//! 5. Vector similarity search (ext-vector)
//! 6. Graph algorithms — PageRank (ext-algo)
//!
//! Run with: cargo run -p kyu-api --example knowledge_graph

use std::collections::HashMap;
use std::io::Write;

use kyu_api::Database;
use kyu_extension::Extension;
use kyu_types::TypedValue;

fn main() {
    println!("=== KyuGraph Knowledge Graph Example ===\n");

    // Create an in-memory database with fts and algo extensions.
    // Vector extension is used directly (its CSV arguments conflict with the
    // simple CALL argument parser), demonstrating both usage patterns.
    let mut db = Database::in_memory();
    db.register_extension(Box::new(ext_fts::FtsExtension::new()));
    db.register_extension(Box::new(ext_algo::AlgoExtension));
    let conn = db.connect();

    // -----------------------------------------------------------------------
    // Stage 1: Schema Creation
    // -----------------------------------------------------------------------
    println!("--- Stage 1: Schema Creation ---");

    conn.query("CREATE NODE TABLE Article (id INT64, title STRING, abstract STRING, year INT64, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE NODE TABLE Author (id INT64, name STRING, field STRING, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE NODE TABLE Topic (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    conn.query("CREATE REL TABLE AUTHORED (FROM Author TO Article)").unwrap();
    conn.query("CREATE REL TABLE COVERS (FROM Article TO Topic)").unwrap();

    println!("Created 3 node tables and 2 relationship tables.\n");

    // -----------------------------------------------------------------------
    // Stage 2: Data Ingestion
    // -----------------------------------------------------------------------
    println!("--- Stage 2: Data Ingestion ---");

    let tmp = std::env::temp_dir().join("kyu_knowledge_graph_example");
    let _ = std::fs::create_dir_all(&tmp);

    // Write and load Article CSV.
    let articles = articles();
    write_csv(&tmp.join("articles.csv"), "id,title,abstract,year", &articles, |f, a| {
        writeln!(f, "{},{},{},{}", a.0, a.1, a.2, a.3)
    });
    conn.query(&format!("COPY Article FROM '{}'", tmp.join("articles.csv").display())).unwrap();

    // Write and load Author CSV.
    let authors = authors();
    write_csv(&tmp.join("authors.csv"), "id,name,field", &authors, |f, a| {
        writeln!(f, "{},{},{}", a.0, a.1, a.2)
    });
    conn.query(&format!("COPY Author FROM '{}'", tmp.join("authors.csv").display())).unwrap();

    // Write and load Topic CSV.
    let topics = topics();
    write_csv(&tmp.join("topics.csv"), "id,name", &topics, |f, t| {
        writeln!(f, "{},{}", t.0, t.1)
    });
    conn.query(&format!("COPY Topic FROM '{}'", tmp.join("topics.csv").display())).unwrap();

    // Insert relationships via storage API (CREATE relationship not yet in Cypher).
    let authored = authored_rels();
    let covers = covers_rels();
    {
        let catalog = db.catalog().read();
        let authored_id = catalog.find_by_name("AUTHORED").unwrap().table_id();
        let covers_id = catalog.find_by_name("COVERS").unwrap().table_id();
        drop(catalog);

        let mut storage = db.storage().write().unwrap();
        for &(author_id, article_id) in &authored {
            storage
                .insert_row(authored_id, &[TypedValue::Int64(author_id), TypedValue::Int64(article_id)])
                .unwrap();
        }
        for &(article_id, topic_id) in &covers {
            storage
                .insert_row(covers_id, &[TypedValue::Int64(article_id), TypedValue::Int64(topic_id)])
                .unwrap();
        }
    }

    println!(
        "Loaded {} articles, {} authors, {} topics",
        articles.len(),
        authors.len(),
        topics.len()
    );
    println!(
        "Loaded {} AUTHORED and {} COVERS relationships\n",
        authored.len(),
        covers.len()
    );

    // -----------------------------------------------------------------------
    // Stage 3: Graph Queries
    // -----------------------------------------------------------------------
    println!("--- Stage 3: Graph Queries ---");

    let result = conn
        .query("MATCH (a:Article) WHERE a.year > 2022 RETURN a.id, a.title, a.year")
        .unwrap();
    println!("Articles published after 2022 ({} found):", result.num_rows());
    for row in result.iter_rows() {
        println!("  [{}] {} ({})", row[0], row[1], row[2]);
    }

    let result = conn.query("MATCH (a:Article) RETURN a.year").unwrap();
    let mut year_counts = std::collections::BTreeMap::new();
    for row in result.iter_rows() {
        if let TypedValue::Int64(y) = &row[0] {
            *year_counts.entry(*y).or_insert(0u32) += 1;
        }
    }
    println!("\nArticles per year:");
    for (year, count) in &year_counts {
        println!("  {year}: {count} articles");
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 4: Full-Text Search
    // -----------------------------------------------------------------------
    println!("--- Stage 4: Full-Text Search ---");

    // Index all article abstracts (doc_id = insertion order, 0-based).
    for (_, _, abst, _) in &articles {
        conn.query(&format!("CALL fts.add('{abst}')")).unwrap();
    }

    for query in ["neural network optimization", "graph database"] {
        let result = conn
            .query(&format!("CALL fts.search('{query}', 5)"))
            .unwrap();
        println!("Search: \"{query}\"");
        for row in result.iter_rows() {
            if let (TypedValue::Int64(doc_id), TypedValue::Double(score)) = (&row[0], &row[1]) {
                let title = &articles[*doc_id as usize].1;
                println!("  [score={score:.2}] \"{title}\"");
            }
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Stage 5: Vector Similarity Search
    // -----------------------------------------------------------------------
    println!("--- Stage 5: Vector Similarity Search ---");

    // Use the vector extension directly to avoid CALL parser comma conflicts.
    let vector_ext = ext_vector::VectorExtension::new();
    let empty_adj = HashMap::new();
    let keywords = keywords();

    vector_ext
        .execute("build", &[keywords.len().to_string(), "cosine".into()], &empty_adj)
        .unwrap();

    // Compute bag-of-words embeddings and index each article.
    for (id, _, abst, _) in &articles {
        let vec_csv = bag_of_words(abst, &keywords)
            .iter()
            .map(|v| format!("{v}"))
            .collect::<Vec<_>>()
            .join(",");
        vector_ext
            .execute("add", &[id.to_string(), vec_csv], &empty_adj)
            .unwrap();
    }

    // Search for articles similar to a query phrase.
    let query_text = "distributed graph neural networks";
    let query_csv = bag_of_words(query_text, &keywords)
        .iter()
        .map(|v| format!("{v}"))
        .collect::<Vec<_>>()
        .join(",");

    let results = vector_ext
        .execute("search", &[query_csv, "5".into()], &empty_adj)
        .unwrap();

    // The HNSW index returns 0-based insertion-order IDs (not the ext_id
    // passed to vector.add), so map by insertion index.
    let titles: Vec<&str> = articles.iter().map(|(_, t, _, _)| t.as_str()).collect();
    println!("Query: \"{query_text}\"");
    for row in &results {
        if let (TypedValue::Int64(idx), TypedValue::Double(dist)) = (&row[0], &row[1]) {
            let title = titles.get(*idx as usize).unwrap_or(&"?");
            println!("  [dist={dist:.4}] \"{title}\"");
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Stage 6: Graph Algorithms
    // -----------------------------------------------------------------------
    println!("--- Stage 6: Graph Algorithms ---");

    let result = conn
        .query("CALL algo.pageRank(0.85, 20, 0.000001)")
        .unwrap();

    // Build node ID -> name lookup.
    let mut name_map: HashMap<i64, String> = HashMap::new();
    for (id, title, _, _) in &articles {
        name_map.insert(*id, format!("Article: {title}"));
    }
    for (id, name, _) in &authors {
        name_map.insert(*id, format!("Author: {name}"));
    }
    for (id, name) in &topics {
        name_map.insert(*id, format!("Topic: {name}"));
    }

    println!("PageRank (top 5):");
    for (rank, row) in result.iter_rows().take(5).enumerate() {
        if let (TypedValue::Int64(node_id), TypedValue::Double(score)) = (&row[0], &row[1]) {
            let label = name_map.get(node_id).map_or("unknown", |s| s.as_str());
            println!("  #{} node={node_id} rank={score:.6}  ({label})", rank + 1);
        }
    }
    println!();

    // Clean up temp files.
    let _ = std::fs::remove_dir_all(&tmp);

    println!("Done.");
}

// ---------------------------------------------------------------------------
// Helper: write a CSV file from a slice of items.
// ---------------------------------------------------------------------------

fn write_csv<T>(
    path: &std::path::Path,
    header: &str,
    items: &[T],
    mut row_fn: impl FnMut(&mut std::fs::File, &T) -> std::io::Result<()>,
) {
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, "{header}").unwrap();
    for item in items {
        row_fn(&mut f, item).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

type Article = (i64, String, String, i64);
type Author = (i64, String, String);
type Topic = (i64, String);

fn articles() -> Vec<Article> {
    vec![
        (1, "Scalable Graph Neural Networks for Large-Scale Recommendations".into(),
         "A novel approach to scalable graph neural network architectures for distributed learning on large recommendation graphs".into(), 2023),
        (2, "Optimizing Deep Learning Inference on Edge Devices".into(),
         "Techniques for optimization of deep learning neural network inference on resource-constrained edge devices".into(), 2022),
        (3, "Knowledge Graph Embeddings for Link Prediction".into(),
         "Learning knowledge graph embedding representations using advanced algorithms for link prediction tasks".into(), 2023),
        (4, "Efficient Query Processing in Distributed Databases".into(),
         "A scalable approach to query processing and index optimization in parallel distributed database systems".into(), 2022),
        (5, "Transformer Architectures for Natural Language Understanding".into(),
         "Deep learning with transformer attention mechanisms for neural network language understanding".into(), 2023),
        (6, "Graph Database Index Structures: A Survey".into(),
         "Survey of graph database storage and index structures for efficient query processing".into(), 2021),
        (7, "Parallel Algorithms for Large-Scale Graph Analytics".into(),
         "Scalable parallel algorithms for distributed graph analytics on large-scale networks".into(), 2023),
        (8, "Neural Network Optimization via Gradient Descent Variants".into(),
         "Comparing deep learning neural network optimization algorithms including gradient descent variants".into(), 2022),
        (9, "Scalable Machine Learning Pipelines in the Cloud".into(),
         "Building scalable machine learning pipelines with parallel distributed inference in cloud environments".into(), 2023),
        (10, "Embedding-Based Retrieval for Knowledge Graphs".into(),
         "Using embedding techniques for knowledge graph retrieval with learned index structures".into(), 2022),
    ]
}

fn authors() -> Vec<Author> {
    vec![
        (101, "Dr. Sarah Chen".into(), "Machine Learning".into()),
        (102, "Prof. James Wilson".into(), "Databases".into()),
        (103, "Dr. Aisha Patel".into(), "Graph Systems".into()),
        (104, "Prof. Michael Zhang".into(), "Deep Learning".into()),
        (105, "Dr. Emily Rodriguez".into(), "Distributed Systems".into()),
        (106, "Prof. David Kim".into(), "NLP".into()),
    ]
}

fn topics() -> Vec<Topic> {
    vec![
        (201, "Machine Learning".into()),
        (202, "Databases".into()),
        (203, "Graph Systems".into()),
        (204, "Natural Language Processing".into()),
        (205, "Distributed Systems".into()),
    ]
}

fn authored_rels() -> Vec<(i64, i64)> {
    vec![
        (101, 1), (101, 9),               // Chen: GNN, ML pipelines
        (102, 4), (102, 6),               // Wilson: query processing, graph DB survey
        (103, 1), (103, 7),               // Patel: GNN, parallel graph
        (104, 2), (104, 5), (104, 8),     // Zhang: deep learning, transformers, NN optim
        (105, 4), (105, 7),               // Rodriguez: query processing, parallel graph
        (106, 3), (106, 5), (106, 10),    // Kim: KG embeddings, transformers, embedding retrieval
    ]
}

fn covers_rels() -> Vec<(i64, i64)> {
    vec![
        (1, 201), (1, 203),     // GNN: ML, Graph Systems
        (2, 201),               // Deep learning inference: ML
        (3, 203), (3, 201),     // KG embeddings: Graph Systems, ML
        (4, 202), (4, 205),     // Query processing: Databases, Distributed Systems
        (5, 201), (5, 204),     // Transformers: ML, NLP
        (6, 202), (6, 203),     // Graph DB survey: Databases, Graph Systems
        (7, 203), (7, 205),     // Parallel graph: Graph Systems, Distributed Systems
        (8, 201),               // NN optimization: ML
        (9, 201), (9, 205),     // ML pipelines: ML, Distributed Systems
        (10, 203), (10, 201),   // Embedding retrieval: Graph Systems, ML
    ]
}

// ---------------------------------------------------------------------------
// Bag-of-words embedding for vector similarity search
// ---------------------------------------------------------------------------

fn keywords() -> Vec<&'static str> {
    vec![
        "neural", "network", "graph", "distributed", "learning",
        "optimization", "database", "query", "index", "storage",
        "machine", "deep", "transformer", "attention", "embedding",
        "knowledge", "algorithm", "scalable", "parallel", "inference",
    ]
}

fn bag_of_words(text: &str, keywords: &[&str]) -> Vec<f32> {
    let lower = text.to_lowercase();
    keywords
        .iter()
        .map(|kw| if lower.contains(kw) { 1.0 } else { 0.0 })
        .collect()
}
