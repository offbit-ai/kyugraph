//! KyuGraph — high-performance embedded property graph database.
//!
//! KyuGraph is a pure-Rust embedded graph database implementing the openCypher
//! query language. It uses columnar storage, vectorized execution, and optional
//! Cranelift JIT compilation for analytical graph workloads.
//!
//! # Quick Start
//!
//! ```no_run
//! use kyu_graph::{Database, TypedValue};
//!
//! // In-memory database
//! let db = Database::in_memory();
//! let conn = db.connect();
//!
//! // Create schema
//! conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
//!     .unwrap();
//! conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)")
//!     .unwrap();
//!
//! // Insert data
//! conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
//! conn.query("CREATE (p:Person {id: 2, name: 'Bob', age: 25})").unwrap();
//!
//! // Query
//! let result = conn.query("MATCH (p:Person) WHERE p.age > 20 RETURN p.name, p.age").unwrap();
//! for row in result.iter_rows() {
//!     println!("{:?}", row);
//! }
//! ```
//!
//! # Persistent Database
//!
//! ```no_run
//! use kyu_graph::Database;
//!
//! let db = Database::open(std::path::Path::new("./my_graph")).unwrap();
//! let conn = db.connect();
//! conn.query("CREATE NODE TABLE Log (id INT64, msg STRING, PRIMARY KEY (id))").unwrap();
//! // Data is checkpointed to disk automatically.
//! ```
//!
//! # Bulk Data Import
//!
//! ```no_run
//! use kyu_graph::Database;
//!
//! let db = Database::in_memory();
//! let conn = db.connect();
//! conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
//! conn.query("COPY Person FROM 'people.csv'").unwrap();
//! ```
//!
//! # Parameterized Queries
//!
//! Pass dynamic values safely via `$param` placeholders instead of string
//! interpolation. Use the re-exported `json!` macro and `BindContext` for
//! ergonomic construction from JSON:
//!
//! ```no_run
//! use kyu_graph::{Database, BindContext, json};
//!
//! let db = Database::in_memory();
//! let conn = db.connect();
//! conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
//!     .unwrap();
//! conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30})").unwrap();
//!
//! // From json! macro
//! let ctx = BindContext::with_params_json(json!({"min_age": 25}));
//!
//! // From a JSON string (e.g. read from a config file or HTTP request body)
//! let ctx = BindContext::with_params_str(r#"{"min_age": 25}"#).unwrap();
//!
//! // Or use HashMap<String, TypedValue> directly
//! use std::collections::HashMap;
//! use kyu_graph::TypedValue;
//! let mut params = HashMap::new();
//! params.insert("min_age".to_string(), TypedValue::Int64(25));
//! let result = conn.query_with_params(
//!     "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
//!     params,
//! ).unwrap();
//! ```
//!
//! For full VM-style execution with both parameters and environment bindings,
//! use [`Connection::execute`].
//!
//! # Extensions
//!
//! KyuGraph supports pluggable extensions for graph algorithms, full-text search,
//! vector similarity, and more. Extensions are registered on the database instance
//! before creating connections.
//!
//! ```no_run
//! use kyu_graph::{Database, Extension};
//!
//! let mut db = Database::in_memory();
//! // db.register_extension(Box::new(my_extension));
//! let conn = db.connect();
//! // conn.query("CALL ext.procedure(args)").unwrap();
//! ```

// ---- Core API ----

pub use kyu_api::{Connection, Database};

// ---- Bind-Time Context ----

pub use kyu_binder::BindContext;

// ---- JSON ----

/// Re-export `serde_json::json!` for ergonomic construction of params and env.
pub use serde_json::json;

// ---- Query Results ----

pub use kyu_executor::QueryResult;

// ---- Storage ----

pub use kyu_api::NodeGroupStorage;

// ---- Data Import ----

pub use kyu_copy::{ArrowIpcReader, CsvReader, DataReader, KafkaReader, ParquetReader, open_reader};

// ---- Delta Fast Path ----

pub use kyu_delta::{
    DeltaBatch, DeltaBatchBuilder, DeltaStats, DeltaValue, GraphDelta, NodeKey, VectorClock,
};

// ---- Type System ----

pub use kyu_types::{LogicalType, TypedValue};

// ---- Error Handling ----

pub use kyu_common::{KyuError, KyuResult};

// ---- Configuration ----

pub use kyu_common::DatabaseConfig;

// ---- Extensions ----

pub use kyu_extension::{Extension, ProcColumn, ProcParam, ProcRow, ProcedureSignature};

// ---- Arrow Flight ----

pub use kyu_api::{serve_flight, to_record_batch};

/// Re-exports of types used less frequently but needed for advanced usage.
pub mod types {
    pub use kyu_types::{Interval, LogicalType, PhysicalType, TypedValue};
}
