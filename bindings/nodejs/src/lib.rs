//! kyugraph-nodejs: NAPI-rs bindings for KyuGraph.
//!
//! Provides `Database`, `Connection`, and `QueryResult` classes to Node.js.
//!
//! Usage (JavaScript/TypeScript):
//! ```js
//! const { Database } = require('kyugraph');
//! const db = new Database();                     // in-memory
//! const db = new Database('/path/to/dir');        // persistent
//! const conn = db.connect();
//! conn.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))");
//! const result = conn.query("MATCH (p:Person) RETURN p.id, p.name");
//! console.log(result.columnNames());
//! for (const row of result.toArray()) {
//!     console.log(row);
//! }
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use kyu_api::Database;
use kyu_executor::QueryResult;
use kyu_types::TypedValue;
use napi::bindgen_prelude::*;
use napi_derive::napi;

// ---------------------------------------------------------------------------
// JsDatabase
// ---------------------------------------------------------------------------

/// An embedded KyuGraph database.
#[napi(js_name = "Database")]
pub struct JsDatabase {
    inner: Arc<Database>,
}

#[napi]
impl JsDatabase {
    /// Create a new database.
    ///
    /// Pass a path string for a persistent database, or omit for in-memory.
    #[napi(constructor)]
    pub fn new(path: Option<String>) -> Result<Self> {
        let db = match path {
            Some(p) => Database::open(&PathBuf::from(p)).map_err(|e| {
                Error::new(Status::GenericFailure, format!("cannot open database: {e}"))
            })?,
            None => Database::in_memory(),
        };
        Ok(JsDatabase {
            inner: Arc::new(db),
        })
    }

    /// Create a new connection to this database.
    #[napi]
    pub fn connect(&self) -> JsConnection {
        JsConnection {
            conn: self.inner.connect(),
        }
    }
}

// ---------------------------------------------------------------------------
// JsConnection
// ---------------------------------------------------------------------------

/// A connection to a KyuGraph database.
#[napi(js_name = "Connection")]
pub struct JsConnection {
    conn: kyu_api::Connection,
}

#[napi]
impl JsConnection {
    /// Execute a Cypher query and return the result.
    #[napi]
    pub fn query(&self, cypher: String) -> Result<JsQueryResult> {
        let result = self
            .conn
            .query(&cypher)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
        Ok(JsQueryResult { inner: result })
    }

    /// Execute a Cypher statement (DDL/DML). Returns nothing.
    #[napi]
    pub fn execute(&self, cypher: String) -> Result<()> {
        self.conn
            .query(&cypher)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JsQueryResult
// ---------------------------------------------------------------------------

/// The result of a Cypher query.
#[napi(js_name = "QueryResult")]
pub struct JsQueryResult {
    inner: QueryResult,
}

#[napi]
impl JsQueryResult {
    /// Get column names as an array of strings.
    #[napi]
    pub fn column_names(&self) -> Vec<String> {
        self.inner
            .column_names
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Number of result rows.
    #[napi]
    pub fn num_rows(&self) -> u32 {
        self.inner.num_rows() as u32
    }

    /// Number of result columns.
    #[napi]
    pub fn num_columns(&self) -> u32 {
        self.inner.num_columns() as u32
    }

    /// Convert the entire result to an array of arrays.
    ///
    /// Each inner array is a row of values.
    #[napi]
    pub fn to_array(&self, env: Env) -> Result<Vec<Vec<napi::JsUnknown>>> {
        let mut rows = Vec::with_capacity(self.inner.num_rows());
        for i in 0..self.inner.num_rows() {
            let row = self.inner.row(i);
            let js_row: Vec<napi::JsUnknown> = row
                .iter()
                .map(|v| typed_value_to_js(&env, v))
                .collect::<Result<Vec<_>>>()?;
            rows.push(js_row);
        }
        Ok(rows)
    }

    /// Get a single row by index as an array of values.
    #[napi]
    pub fn get_row(&self, index: u32, env: Env) -> Result<Vec<napi::JsUnknown>> {
        let idx = index as usize;
        if idx >= self.inner.num_rows() {
            return Err(Error::new(
                Status::GenericFailure,
                format!(
                    "row index {idx} out of range (num_rows={})",
                    self.inner.num_rows()
                ),
            ));
        }
        let row = self.inner.row(idx);
        row.iter()
            .map(|v| typed_value_to_js(&env, v))
            .collect::<Result<Vec<_>>>()
    }

    /// String representation.
    #[napi]
    pub fn to_string_repr(&self) -> String {
        format!(
            "QueryResult(columns={}, rows={})",
            self.inner.num_columns(),
            self.inner.num_rows()
        )
    }
}

// ---------------------------------------------------------------------------
// TypedValue → JS conversion
// ---------------------------------------------------------------------------

fn typed_value_to_js(env: &Env, value: &TypedValue) -> Result<napi::JsUnknown> {
    match value {
        TypedValue::Null => env.get_null().map(|v| v.into_unknown()),
        TypedValue::Bool(v) => env.get_boolean(*v).map(|v| v.into_unknown()),
        TypedValue::Int8(v) => env.create_int32(*v as i32).map(|v| v.into_unknown()),
        TypedValue::Int16(v) => env.create_int32(*v as i32).map(|v| v.into_unknown()),
        TypedValue::Int32(v) => env.create_int32(*v).map(|v| v.into_unknown()),
        TypedValue::Int64(v) => env.create_int64(*v).map(|v| v.into_unknown()),
        TypedValue::Float(v) => env.create_double(*v as f64).map(|v| v.into_unknown()),
        TypedValue::Double(v) => env.create_double(*v).map(|v| v.into_unknown()),
        TypedValue::String(s) => env.create_string(s.as_str()).map(|v| v.into_unknown()),
        other => env
            .create_string(&format!("{other:?}"))
            .map(|v| v.into_unknown()),
    }
}
