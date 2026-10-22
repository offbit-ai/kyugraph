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
    #[cfg(feature = "vector")]
    vector: Arc<ext_vector::VectorExtension>,
    /// Dimension given to `vector_build` (0 until built). Checked here so a wrong-length vector raises
    /// instead of panicking inside the extension while it holds its lock.
    #[cfg(feature = "vector")]
    vector_dim: std::sync::atomic::AtomicUsize,
}

/// Lets the database own the vector extension while Python keeps a handle to call it directly.
/// `CALL vector.*` splits its arguments on every comma, quoted or not, which cuts a vector given as one
/// quoted CSV string down to its first element; the direct methods below pass the string through intact.
#[cfg(feature = "vector")]
struct SharedVector(Arc<ext_vector::VectorExtension>);

#[cfg(feature = "vector")]
impl kyu_extension::Extension for SharedVector {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn procedures(&self) -> Vec<kyu_extension::ProcedureSignature> {
        self.0.procedures()
    }
    fn needs_graph(&self) -> bool {
        false
    }
    fn execute(
        &self,
        procedure: &str,
        args: &[String],
        adjacency: &std::collections::HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<kyu_extension::ProcRow>, String> {
        self.0.execute(procedure, args, adjacency)
    }
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
        #[allow(unused_mut)] // only mutated when an extension feature is enabled
        let mut db = match path {
            Some(p) => Database::open(&PathBuf::from(p))
                .map_err(|e| PyRuntimeError::new_err(format!("cannot open database: {e}")))?,
            None => Database::in_memory(),
        };
        // Extensions must be registered before the database is shared or connected to.
        #[cfg(feature = "fts")]
        db.register_extension(Box::new(ext_fts::FtsExtension::new()));
        #[cfg(feature = "vector")]
        let vector = Arc::new(ext_vector::VectorExtension::new());
        #[cfg(feature = "vector")]
        db.register_extension(Box::new(SharedVector(Arc::clone(&vector))));
        Ok(PyDatabase {
            inner: Arc::new(db),
            #[cfg(feature = "vector")]
            vector,
            #[cfg(feature = "vector")]
            vector_dim: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Create a new connection to this database.
    fn connect(&self) -> PyConnection {
        PyConnection {
            conn: self.inner.connect(),
        }
    }

    /// Initialise the vector index: `dim` components per vector, metric "l2" (default) or "cosine".
    #[cfg(feature = "vector")]
    #[pyo3(signature = (dim, metric="l2"))]
    fn vector_build(&self, dim: usize, metric: &str) -> PyResult<()> {
        self.call_vector("build", vec![dim.to_string(), metric.to_string()])?;
        self.vector_dim
            .store(dim, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Insert one vector. The index reports positions in insertion order, so keep `id` equal to that order.
    #[cfg(feature = "vector")]
    fn vector_add(&self, id: usize, vector: Vec<f32>) -> PyResult<()> {
        self.check_dim(vector.len())?;
        let csv = vector
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.call_vector("add", vec![id.to_string(), csv])
            .map(|_| ())
    }

    /// Nearest `k` vectors to `query` as `(insertion_position, distance)`, nearest first.
    #[cfg(feature = "vector")]
    #[pyo3(signature = (query, k=10))]
    fn vector_search(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<(i64, f64)>> {
        self.check_dim(query.len())?;
        let csv = query
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let rows = self.call_vector("search", vec![csv, k.to_string()])?;
        Ok(rows
            .into_iter()
            .filter_map(|r| match (r.first(), r.get(1)) {
                (Some(TypedValue::Int64(i)), Some(TypedValue::Double(d))) => Some((*i, *d)),
                _ => None,
            })
            .collect())
    }
}

#[cfg(feature = "vector")]
impl PyDatabase {
    fn check_dim(&self, len: usize) -> PyResult<()> {
        let dim = self.vector_dim.load(std::sync::atomic::Ordering::SeqCst);
        if dim == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "call vector_build first",
            ));
        }
        if len != dim {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "vector has {len} components, index was built for {dim}"
            )));
        }
        Ok(())
    }

    fn call_vector(
        &self,
        procedure: &str,
        args: Vec<String>,
    ) -> PyResult<Vec<kyu_extension::ProcRow>> {
        use kyu_extension::Extension;
        // A malformed vector must raise, never panic across the FFI boundary.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.vector
                .execute(procedure, &args, &std::collections::HashMap::new())
        }))
        .map_err(|_| {
            PyRuntimeError::new_err("vector extension rejected the input (check dimensions)")
        })?
        .map_err(|e| PyRuntimeError::new_err(format!("vector.{procedure}: {e}")))
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
