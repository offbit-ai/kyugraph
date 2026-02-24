# Build a Knowledge Graph

## What you'll build

A research paper knowledge graph with articles, authors, and topics — connected by AUTHORED and COVERS relationships. You'll use CSV bulk ingestion, full-text search to find papers by content, vector similarity to find related papers, and PageRank to identify the most influential nodes.

This follows the `knowledge_graph` example: `cargo run -p kyu-api --example knowledge_graph`

## Step 1: Set up the database with extensions

Register the extensions you'll need — algorithms, full-text search, and vector search:

```rust
use kyu_api::Database;
use kyu_extension::Extension;

let mut db = Database::in_memory();
db.register_extension(Box::new(ext_fts::FtsExtension::new()));
db.register_extension(Box::new(ext_algo::AlgoExtension));
let conn = db.connect();
```

## Step 2: Create the schema

Three node types and two relationship types form the knowledge graph:

```rust
conn.query("CREATE NODE TABLE Article (
    id INT64, title STRING, abstract STRING, year INT64,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE NODE TABLE Author (
    id INT64, name STRING, field STRING,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE NODE TABLE Topic (
    id INT64, name STRING,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE REL TABLE AUTHORED (FROM Author TO Article)").unwrap();
conn.query("CREATE REL TABLE COVERS (FROM Article TO Topic)").unwrap();
```

## Step 3: Bulk load data from CSV

Write your data to CSV files and use `COPY FROM` for fast ingestion:

```rust
// articles.csv:
// id,title,abstract,year
// 1,Scalable Graph Neural Networks,A novel approach to...,2023
// 2,Optimizing Deep Learning,Techniques for...,2022

conn.query("COPY Article FROM '/tmp/articles.csv'").unwrap();
conn.query("COPY Author FROM '/tmp/authors.csv'").unwrap();
conn.query("COPY Topic FROM '/tmp/topics.csv'").unwrap();
```

KyuGraph auto-detects file format by extension — `.csv`, `.parquet`, and `.arrow` are all supported.

## Step 4: Query the graph

Find articles published after 2022:

```rust
let result = conn.query("
    MATCH (a:Article)
    WHERE a.year > 2022
    RETURN a.id, a.title, a.year
").unwrap();

for row in result.iter_rows() {
    println!("[{}] {} ({})", row[0], row[1], row[2]);
}
```

Count articles per year:

```rust
let result = conn.query("
    MATCH (a:Article)
    RETURN a.year, count(a) AS count
    ORDER BY a.year
").unwrap();
```

## Step 5: Add full-text search

Index all article abstracts and search by content:

```rust
// Index each abstract
for abstract_text in &abstracts {
    conn.query(&format!("CALL fts.add('{}')", abstract_text)).unwrap();
}

// Search — returns doc_id, BM25 score, and snippet
let result = conn.query("CALL fts.search('neural network optimization', 5)").unwrap();
for row in result.iter_rows() {
    // row[0] = doc_id (insertion order), row[1] = score, row[2] = snippet
    println!("[score={:.2}] {}", row[1], row[2]);
}
```

FTS uses Tantivy's BM25 ranking — the same algorithm behind Elasticsearch.

## Step 6: Add vector similarity search

Compute bag-of-words embeddings and find semantically similar articles:

```rust
let vector_ext = ext_vector::VectorExtension::new();
let empty_adj = HashMap::new();

// Build a 20-dimensional HNSW index with cosine similarity
vector_ext.execute("build", &["20".into(), "cosine".into()], &empty_adj).unwrap();

// Index each article's abstract as a bag-of-words vector
let keywords = vec![
    "neural", "network", "graph", "distributed", "learning",
    "optimization", "database", "query", "index", "storage",
    "machine", "deep", "transformer", "attention", "embedding",
    "knowledge", "algorithm", "scalable", "parallel", "inference",
];

for (id, abstract_text) in &articles {
    let vec: Vec<f32> = keywords.iter()
        .map(|kw| if abstract_text.to_lowercase().contains(kw) { 1.0 } else { 0.0 })
        .collect();
    let csv = vec.iter().map(|v| format!("{v}")).collect::<Vec<_>>().join(",");
    vector_ext.execute("add", &[id.to_string(), csv], &empty_adj).unwrap();
}

// Search for similar articles
let query_csv = bag_of_words("distributed graph neural networks", &keywords);
let results = vector_ext.execute("search", &[query_csv, "5".into()], &empty_adj).unwrap();

for row in &results {
    // row[0] = id, row[1] = cosine distance
    println!("[dist={:.4}] Article {}", row[1], row[0]);
}
```

## Step 7: Run PageRank

Find the most influential nodes in the knowledge graph:

```rust
let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();

println!("Most influential nodes:");
for (rank, row) in result.iter_rows().take(5).enumerate() {
    // row[0] = node_id, row[1] = rank score
    println!("  #{} node={} rank={:.6}", rank + 1, row[0], row[1]);
}
```

PageRank considers both the AUTHORED and COVERS relationships — authors who write many papers on popular topics rank higher.

## What you've learned

- Bulk CSV ingestion with `COPY FROM`
- Full-text search with BM25 ranking via `fts.add()` and `fts.search()`
- Vector similarity search via HNSW index with `vector.build()`, `vector.add()`, `vector.search()`
- Graph algorithms with `algo.pageRank()`
- Combining all of these in a single knowledge graph application
