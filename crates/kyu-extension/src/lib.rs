//! kyu-extension: Extension trait and procedure signatures.
//!
//! Extensions register named procedures (e.g., `algo.pageRank`) that can be
//! invoked via `CALL algo.pageRank(...) YIELD node_id, rank`. Each procedure
//! receives typed arguments and returns a table of named, typed columns.

use std::collections::HashMap;

/// A procedure parameter: name + type description.
#[derive(Clone, Debug)]
pub struct ProcParam {
    pub name: String,
    pub type_desc: String,
}

/// A procedure result column: name + type description.
#[derive(Clone, Debug)]
pub struct ProcColumn {
    pub name: String,
    pub type_desc: String,
}

/// Signature of an extension procedure.
#[derive(Clone, Debug)]
pub struct ProcedureSignature {
    pub name: String,
    pub params: Vec<ProcParam>,
    pub columns: Vec<ProcColumn>,
}

/// A row of results: column_name -> string value.
pub type ProcRow = HashMap<String, String>;

/// Trait that every extension must implement.
pub trait Extension: Send + Sync {
    /// Extension name (e.g., "algo").
    fn name(&self) -> &str;

    /// List all procedures this extension provides.
    fn procedures(&self) -> Vec<ProcedureSignature>;

    /// Execute a named procedure with string arguments.
    /// Returns rows of column_name -> value pairs.
    fn execute(
        &self,
        procedure: &str,
        args: &[String],
        adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<ProcRow>, String>;
}
