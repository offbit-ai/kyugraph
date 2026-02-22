//! kyu-extension: Extension trait and procedure signatures.
//!
//! Extensions register named procedures (e.g., `algo.pageRank`) that can be
//! invoked via `CALL algo.pageRank(...) YIELD node_id, rank`. Each procedure
//! receives typed arguments and returns a table of named, typed columns.

use std::collections::HashMap;

use kyu_types::{LogicalType, TypedValue};

/// A procedure parameter: name + type description.
#[derive(Clone, Debug)]
pub struct ProcParam {
    pub name: String,
    pub type_desc: String,
}

/// A procedure result column: name + logical type.
#[derive(Clone, Debug)]
pub struct ProcColumn {
    pub name: String,
    pub data_type: LogicalType,
}

/// Signature of an extension procedure.
#[derive(Clone, Debug)]
pub struct ProcedureSignature {
    pub name: String,
    pub params: Vec<ProcParam>,
    pub columns: Vec<ProcColumn>,
}

/// A row of typed results, ordered by column index (matching ProcedureSignature.columns).
pub type ProcRow = Vec<TypedValue>;

/// Trait that every extension must implement.
pub trait Extension: Send + Sync {
    /// Extension name (e.g., "algo").
    fn name(&self) -> &str;

    /// List all procedures this extension provides.
    fn procedures(&self) -> Vec<ProcedureSignature>;

    /// Execute a named procedure with string arguments.
    /// Returns rows of typed values, column-ordered per the procedure signature.
    fn execute(
        &self,
        procedure: &str,
        args: &[String],
        adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<ProcRow>, String>;
}
