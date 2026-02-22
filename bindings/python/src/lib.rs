// PyO3 macro expansion triggers this lint in generated code.
#![allow(clippy::useless_conversion)]

//! kyugraph-python: PyO3 bindings for KyuGraph.
//!
//! Provides `Database`, `Connection`, and `QueryResult` classes to Python.
//!
//! Usage:
//! ```python
//! import kyugraph
//! db = kyugraph.Database()                    # in-memory
//! db = kyugraph.Database("/path/to/dir")      # persistent
//! conn = db.connect()
//! conn.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
//! result = conn.query("MATCH (p:Person) RETURN p.id, p.name")
//! for row in result:
//!     print(row)
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use kyu_api::Database;
use kyu_executor::QueryResult;
use kyu_types::TypedValue;

// ---------------------------------------------------------------------------
// PyDatabase
// ---------------------------------------------------------------------------

/// An embedded KyuGraph database.
///
/// Create an in-memory database with `Database()` or a persistent one
/// with `Database("/path/to/dir")`.
#[pyclass(name = "Database")]
struct PyDatabase {
    inner: Arc<Database>,
}

#[pymethods]
impl PyDatabase {
    /// Create a new database.
    ///
    /// Args:
    ///     path: Optional filesystem path for persistence. If omitted,
    ///           creates an in-memory database.
    #[new]
    #[pyo3(signature = (path=None))]
    fn new(path: Option<&str>) -> PyResult<Self> {
        let db = match path {
            Some(p) => Database::open(&PathBuf::from(p))
                .map_err(|e| PyRuntimeError::new_err(format!("cannot open database: {e}")))?,
            None => Database::in_memory(),
        };
        Ok(PyDatabase {
            inner: Arc::new(db),
        })
    }

    /// Create a new connection to this database.
    fn connect(&self) -> PyConnection {
        PyConnection {
            conn: self.inner.connect(),
        }
    }
}

// ---------------------------------------------------------------------------
// PyConnection
// ---------------------------------------------------------------------------

/// A connection to a KyuGraph database.
///
/// Use `query()` to execute Cypher queries that return results,
/// or `execute()` for DDL/DML statements.
#[pyclass(name = "Connection")]
struct PyConnection {
    conn: kyu_api::Connection,
}

#[pymethods]
impl PyConnection {
    /// Execute a Cypher query and return the result.
    fn query(&self, cypher: &str) -> PyResult<PyQueryResult> {
        let result = self
            .conn
            .query(cypher)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyQueryResult { inner: result })
    }

    /// Execute a Cypher statement (DDL/DML). Returns nothing.
    fn execute(&self, cypher: &str) -> PyResult<()> {
        self.conn
            .query(cypher)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PyQueryResult
// ---------------------------------------------------------------------------

/// The result of a Cypher query.
///
/// Iterable: yields rows as Python lists.
/// Supports `column_names()`, `num_rows()`, `num_columns()`, and `to_list()`.
#[pyclass(name = "QueryResult")]
struct PyQueryResult {
    inner: QueryResult,
}

#[pymethods]
impl PyQueryResult {
    /// Get column names as a list of strings.
    fn column_names(&self) -> Vec<String> {
        self.inner
            .column_names
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Number of result rows.
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of result columns.
    fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Convert the entire result to a list of lists.
    fn to_list(&self, py: Python<'_>) -> PyResult<PyObject> {
        let rows: Vec<PyObject> = (0..self.inner.num_rows())
            .map(|r| {
                let row = self.inner.row(r);
                let py_row: Vec<PyObject> = row.iter().map(|v| typed_value_to_py(py, v)).collect();
                PyList::new_bound(py, &py_row).into()
            })
            .collect();
        Ok(PyList::new_bound(py, &rows).into())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyQueryResultIter {
        PyQueryResultIter {
            result: slf.into(),
            index: 0,
        }
    }

    fn __len__(&self) -> usize {
        self.inner.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "<QueryResult columns={} rows={}>",
            self.inner.num_columns(),
            self.inner.num_rows()
        )
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

#[pyclass]
struct PyQueryResultIter {
    result: Py<PyQueryResult>,
    index: usize,
}

#[pymethods]
impl PyQueryResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> Option<PyObject> {
        let result = self.result.borrow(py);
        if self.index >= result.inner.num_rows() {
            return None;
        }
        let row = result.inner.row(self.index);
        self.index += 1;
        let py_row: Vec<PyObject> = row.iter().map(|v| typed_value_to_py(py, v)).collect();
        Some(PyList::new_bound(py, &py_row).into())
    }
}

// ---------------------------------------------------------------------------
// TypedValue → Python conversion
// ---------------------------------------------------------------------------

fn typed_value_to_py(py: Python<'_>, value: &TypedValue) -> PyObject {
    match value {
        TypedValue::Null => py.None(),
        TypedValue::Bool(v) => v.into_py(py),
        TypedValue::Int8(v) => v.into_py(py),
        TypedValue::Int16(v) => v.into_py(py),
        TypedValue::Int32(v) => v.into_py(py),
        TypedValue::Int64(v) => v.into_py(py),
        TypedValue::Float(v) => (*v as f64).into_py(py),
        TypedValue::Double(v) => v.into_py(py),
        TypedValue::String(s) => s.as_str().into_py(py),
        other => format!("{other:?}").into_py(py),
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

#[pymodule]
fn kyugraph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyConnection>()?;
    m.add_class::<PyQueryResult>()?;
    Ok(())
}
