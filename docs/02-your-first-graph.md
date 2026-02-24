# Your First Graph

## What you'll build

A social network graph with people and cities, connected by relationships. You'll create a schema, insert data, and run Cypher queries — all in about 20 lines of Rust.

## Step 1: Create a database

Start with an in-memory database. No files, no config — just a function call:

```rust
use kyu_graph::Database;

fn main() {
    let db = Database::in_memory();
    let conn = db.connect();
}
```

## Step 2: Define the schema

KyuGraph uses typed node tables (like SQL tables) and relationship tables that connect them. Every node table needs a primary key:

```rust
conn.query("CREATE NODE TABLE Person (
    id INT64,
    name STRING,
    age INT64,
    PRIMARY KEY (id)
)").unwrap();

conn.query("CREATE NODE TABLE City (
    name STRING,
    population INT64,
    PRIMARY KEY (name)
)").unwrap();

conn.query("CREATE REL TABLE LIVES_IN (FROM Person TO City)").unwrap();
conn.query("CREATE REL TABLE FOLLOWS (FROM Person TO Person, since STRING)").unwrap();
```

## Step 3: Insert data

Use Cypher `CREATE` statements to add nodes and relationships:

```rust
// Create people
conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
conn.query("CREATE (p:Person {id: 2, name: 'Bob', age: 25})").unwrap();
conn.query("CREATE (p:Person {id: 3, name: 'Charlie', age: 35})").unwrap();

// Create cities
conn.query("CREATE (c:City {name: 'Berlin', population: 3700000})").unwrap();
conn.query("CREATE (c:City {name: 'Tokyo', population: 14000000})").unwrap();

// Connect people to cities
conn.query("MATCH (p:Person {id: 1}), (c:City {name: 'Berlin'})
    CREATE (p)-[:LIVES_IN]->(c)").unwrap();
conn.query("MATCH (p:Person {id: 2}), (c:City {name: 'Tokyo'})
    CREATE (p)-[:LIVES_IN]->(c)").unwrap();

// Create follow relationships
conn.query("MATCH (a:Person {id: 1}), (b:Person {id: 2})
    CREATE (a)-[:FOLLOWS {since: '2024-01'}]->(b)").unwrap();
conn.query("MATCH (a:Person {id: 2}), (b:Person {id: 3})
    CREATE (a)-[:FOLLOWS {since: '2023-06'}]->(b)").unwrap();
```

## Step 4: Query the graph

Now the fun part — reading data back with pattern matching:

```rust
// Find all people
let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();
for row in result.iter_rows() {
    println!("{} (age {})", row[0], row[1]);
}

// Who lives where?
let result = conn.query("
    MATCH (p:Person)-[:LIVES_IN]->(c:City)
    RETURN p.name, c.name
").unwrap();
for row in result.iter_rows() {
    println!("{} lives in {}", row[0], row[1]);
}

// Who does Alice follow?
let result = conn.query("
    MATCH (a:Person {name: 'Alice'})-[:FOLLOWS]->(b:Person)
    RETURN b.name
").unwrap();
for row in result.iter_rows() {
    println!("Alice follows {}", row[0]);
}
```

## Step 5: Filter and aggregate

Cypher supports WHERE, ORDER BY, LIMIT, and aggregation functions:

```rust
// People older than 28
let result = conn.query("
    MATCH (p:Person)
    WHERE p.age > 28
    RETURN p.name, p.age
    ORDER BY p.age DESC
").unwrap();

// Count people per city
let result = conn.query("
    MATCH (p:Person)-[:LIVES_IN]->(c:City)
    RETURN c.name, count(p) AS residents
").unwrap();
```

## Step 6: Use parameterized queries

Avoid string interpolation — use `$param` placeholders:

```rust
use std::collections::HashMap;
use kyu_types::TypedValue;

let mut params = HashMap::new();
params.insert("min_age".to_string(), TypedValue::Int64(28));

let result = conn.query_with_params(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age",
    params,
).unwrap();
```

## Step 7: Persist to disk

Switch from `in_memory()` to `open()` and your data survives restarts:

```rust
use std::path::Path;

let db = Database::open(Path::new("./my_social_graph")).unwrap();
let conn = db.connect();
// Everything you query or create is now persisted via WAL + checkpointing.
```

## What's next?

You've built a complete graph application. Next tutorials:
- **Build a Knowledge Graph** — CSV ingestion, full-text search, vector similarity, PageRank
- **Data Ingestion** — bulk loading and the delta fast path for streaming
