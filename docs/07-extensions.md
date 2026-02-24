# Using Extensions

## What you'll learn

How to use KyuGraph's four bundled extensions — graph algorithms, full-text search, vector similarity search, and JSON functions. Each section walks through a hands-on example.

## Step 1: Register extensions

Extensions are registered on the database before creating connections:

```rust
use kyu_graph::Database;

let mut db = Database::in_memory();
db.register_extension(Box::new(ext_algo::AlgoExtension));
db.register_extension(Box::new(ext_fts::FtsExtension::new()));
db.register_extension(Box::new(ext_vector::VectorExtension::new()));
let conn = db.connect();
```

The CLI automatically registers all bundled extensions — no setup needed there.

## Step 2: Graph algorithms

All algorithms are invoked via `CALL` and operate on the entire graph.

### Try PageRank

Create a small graph and find the most central node:

```rust
// Set up a citation network
conn.query("CREATE NODE TABLE Paper (id INT64, title STRING, PRIMARY KEY (id))").unwrap();
conn.query("CREATE REL TABLE CITES (FROM Paper TO Paper)").unwrap();

conn.query("CREATE (p:Paper {id: 1, title: 'Foundations'})").unwrap();
conn.query("CREATE (p:Paper {id: 2, title: 'Extension A'})").unwrap();
conn.query("CREATE (p:Paper {id: 3, title: 'Extension B'})").unwrap();
conn.query("CREATE (p:Paper {id: 4, title: 'Survey'})").unwrap();

// Papers 2, 3, 4 all cite Paper 1
// Paper 4 also cites Papers 2 and 3
// ... insert relationships ...

// Run PageRank
let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();
for row in result.iter_rows() {
    println!("node={} rank={:.6}", row[0], row[1]);
}
// Paper 1 ("Foundations") ranks highest — it's cited by everything
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| damping | DOUBLE | 0.85 | Probability of following a link vs. random jump |
| max_iter | INT64 | 20 | Maximum iterations |
| tolerance | DOUBLE | 1e-6 | Convergence threshold |

### Try Weakly Connected Components

Find disconnected clusters in your graph:

```cypher
CALL algo.wcc();
```

Returns `node_id INT64, component INT64` — nodes in the same component share the same component ID.

### Try Betweenness Centrality

Find bridge nodes that connect different parts of the graph:

```cypher
CALL algo.betweenness();
```

Returns `node_id INT64, centrality DOUBLE` — higher centrality means the node appears on more shortest paths.

## Step 3: Full-text search

Build a searchable index over text content using Tantivy's BM25 ranking.

### Index documents

```cypher
CALL fts.add('KyuGraph is a high-performance graph database written in Rust');
CALL fts.add('Cypher is a declarative query language for property graphs');
CALL fts.add('Arrow Flight enables zero-copy data transfer over gRPC');
```

Each call returns a `doc_id` (0-based insertion order).

### Search

```cypher
CALL fts.search('graph database', 5);
```

Returns up to 5 results sorted by BM25 relevance:

| Column | Type | Description |
|--------|------|-------------|
| doc_id | INT64 | Document index (insertion order) |
| score | DOUBLE | BM25 relevance score |
| snippet | STRING | Matching text excerpt |

### Reset the index

```cypher
CALL fts.clear();
```

## Step 4: Vector similarity search

HNSW-based approximate nearest neighbor search with cosine or L2 distance.

### Create an index

```cypher
-- 3-dimensional index with cosine similarity
CALL vector.build(3, 'cosine');
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| dimension | INT64 | required | Vector dimensionality |
| metric | STRING | `'l2'` | `'l2'` (Euclidean) or `'cosine'` |

### Add vectors

```cypher
CALL vector.add(1, '0.1,0.8,0.3');
CALL vector.add(2, '0.9,0.1,0.2');
CALL vector.add(3, '0.15,0.75,0.35');
```

### Search for nearest neighbors

```cypher
CALL vector.search('0.1,0.7,0.3', 2);
```

Returns the 2 closest vectors:

| Column | Type | Description |
|--------|------|-------------|
| id | INT64 | Vector identifier |
| distance | DOUBLE | Distance from query vector |

Vector 3 and Vector 1 will be closest to the query (similar to the `[0.1, 0.7, 0.3]` direction).

## Step 5: JSON functions

JSON functions work as scalar expressions in any query — no `CALL` needed:

```cypher
-- Extract a field
RETURN json_extract('{"name": "Alice", "age": 30}', '$.name');

-- Validate JSON
RETURN json_valid('{"valid": true}');  -- true
RETURN json_valid('not json');          -- false

-- Get type
RETURN json_type('{"key": "value"}');  -- "object"

-- Get keys
RETURN json_keys('{"a": 1, "b": 2}');  -- ["a", "b"]

-- Count array elements
RETURN json_array_length('[1, 2, 3]');  -- 3

-- Check containment
RETURN json_contains('[1, 2, 3]', '2');  -- true

-- Set a value
RETURN json_set('{"name": "Alice"}', '$.age', '30');
```

### Using JSON with graph data

```cypher
MATCH (d:Document)
WHERE json_valid(d.metadata)
RETURN d.id,
       json_extract(d.metadata, '$.author') AS author,
       json_extract(d.metadata, '$.tags') AS tags;
```

## Extension reference

| Extension | Procedures / Functions | Use case |
|-----------|----------------------|----------|
| ext-algo | `algo.pageRank`, `algo.wcc`, `algo.betweenness` | Graph analytics, centrality, clustering |
| ext-fts | `fts.add`, `fts.search`, `fts.clear` | Full-text search across documents |
| ext-vector | `vector.build`, `vector.add`, `vector.search` | Nearest neighbor search, RAG, embeddings |
| ext-json | `json_extract`, `json_valid`, `json_type`, etc. | JSON document processing |
