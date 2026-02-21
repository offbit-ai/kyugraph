//! QueryResult — final output of query execution.

use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;
use std::fmt;

/// The result of executing a query: column metadata + rows of typed values.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub column_names: Vec<SmolStr>,
    pub column_types: Vec<LogicalType>,
    pub rows: Vec<Vec<TypedValue>>,
}

impl QueryResult {
    pub fn new(column_names: Vec<SmolStr>, column_types: Vec<LogicalType>) -> Self {
        Self {
            column_names,
            column_types,
            rows: Vec::new(),
        }
    }

    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn push_row(&mut self, row: Vec<TypedValue>) {
        debug_assert_eq!(row.len(), self.column_names.len());
        self.rows.push(row);
    }

    /// Batch-append all rows from a DataChunk.
    pub fn push_chunk(&mut self, chunk: &crate::data_chunk::DataChunk) {
        let n = chunk.num_rows();
        let num_cols = self.column_names.len();
        self.rows.reserve(n);
        for row_idx in 0..n {
            let row: Vec<TypedValue> =
                (0..num_cols).map(|col| chunk.get_value(row_idx, col)).collect();
            self.rows.push(row);
        }
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Header.
        let headers: Vec<&str> = self.column_names.iter().map(|n| n.as_str()).collect();
        writeln!(f, "| {} |", headers.join(" | "))?;
        writeln!(
            f,
            "|{}|",
            headers
                .iter()
                .map(|h| "-".repeat(h.len() + 2))
                .collect::<Vec<_>>()
                .join("|")
        )?;
        // Rows.
        for row in &self.rows {
            let cells: Vec<String> = row
                .iter()
                .zip(&self.column_names)
                .map(|(val, name)| format!("{:>width$}", format_value(val), width = name.len()))
                .collect();
            writeln!(f, "| {} |", cells.join(" | "))?;
        }
        writeln!(f, "({} row{})", self.rows.len(), if self.rows.len() == 1 { "" } else { "s" })
    }
}

fn format_value(val: &TypedValue) -> String {
    match val {
        TypedValue::Null => "NULL".to_string(),
        TypedValue::Bool(b) => b.to_string(),
        TypedValue::Int8(v) => v.to_string(),
        TypedValue::Int16(v) => v.to_string(),
        TypedValue::Int32(v) => v.to_string(),
        TypedValue::Int64(v) => v.to_string(),
        TypedValue::Float(v) => format!("{v:.1}"),
        TypedValue::Double(v) => format!("{v:.1}"),
        TypedValue::String(s) => s.to_string(),
        _ => format!("{val:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_result() {
        let result = QueryResult::new(
            vec![SmolStr::new("x")],
            vec![LogicalType::Int64],
        );
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 1);
    }

    #[test]
    fn push_rows() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("a"), SmolStr::new("b")],
            vec![LogicalType::Int64, LogicalType::String],
        );
        result.push_row(vec![
            TypedValue::Int64(1),
            TypedValue::String(SmolStr::new("hello")),
        ]);
        result.push_row(vec![
            TypedValue::Int64(2),
            TypedValue::String(SmolStr::new("world")),
        ]);
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn display_format() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("x")],
            vec![LogicalType::Int64],
        );
        result.push_row(vec![TypedValue::Int64(42)]);
        let output = format!("{result}");
        assert!(output.contains("42"));
        assert!(output.contains("1 row"));
    }

    #[test]
    fn display_plural() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("x")],
            vec![LogicalType::Int64],
        );
        result.push_row(vec![TypedValue::Int64(1)]);
        result.push_row(vec![TypedValue::Int64(2)]);
        let output = format!("{result}");
        assert!(output.contains("2 rows"));
    }
}
