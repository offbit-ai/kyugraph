//! QueryResult — final output of query execution.
//!
//! Uses flat `Vec<TypedValue>` storage (row-major) instead of `Vec<Vec<TypedValue>>`
//! to eliminate per-row heap allocations. For 100K rows this avoids 100K small
//! Vec allocations, giving ~2-3x speedup on the result-collection path.

use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;
use std::fmt;

use crate::value_vector::{FlatVector, ValueVector};

/// The result of executing a query: column metadata + rows of typed values.
///
/// Values are stored in a single flat `Vec<TypedValue>` in row-major order
/// (row0_col0, row0_col1, ..., row1_col0, row1_col1, ...). Access via
/// `row(i)` returns a `&[TypedValue]` slice for row `i`.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub column_names: Vec<SmolStr>,
    pub column_types: Vec<LogicalType>,
    data: Vec<TypedValue>,
    num_rows_count: usize,
}

impl QueryResult {
    pub fn new(column_names: Vec<SmolStr>, column_types: Vec<LogicalType>) -> Self {
        Self {
            column_names,
            column_types,
            data: Vec::new(),
            num_rows_count: 0,
        }
    }

    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows_count
    }

    /// Access row `idx` as a typed-value slice.
    pub fn row(&self, idx: usize) -> &[TypedValue] {
        let nc = self.column_names.len();
        let start = idx * nc;
        &self.data[start..start + nc]
    }

    /// Iterate over all rows as slices.
    pub fn iter_rows(&self) -> impl Iterator<Item = &[TypedValue]> {
        let nc = self.column_names.len();
        (0..self.num_rows_count).map(move |i| {
            let start = i * nc;
            &self.data[start..start + nc]
        })
    }

    pub fn push_row(&mut self, row: Vec<TypedValue>) {
        debug_assert_eq!(row.len(), self.column_names.len());
        self.data.extend(row);
        self.num_rows_count += 1;
    }

    /// Batch-append all rows from a DataChunk.
    ///
    /// Uses typed fast-paths for FlatVector columns (direct slice access) to
    /// avoid per-element byte parsing and type dispatch. The flat storage
    /// eliminates per-row Vec allocations entirely.
    pub fn push_chunk(&mut self, chunk: &crate::data_chunk::DataChunk) {
        let n = chunk.num_rows();
        if n == 0 {
            return;
        }
        let num_cols = self.column_names.len();

        // Single allocation for all values in this chunk.
        self.data.reserve(n * num_cols);

        if chunk.selection().is_identity() {
            push_chunk_identity(&mut self.data, chunk, n, num_cols);
        } else {
            for row_idx in 0..n {
                for col_idx in 0..num_cols {
                    self.data.push(chunk.get_value(row_idx, col_idx));
                }
            }
        }
        self.num_rows_count += n;
    }
}

/// Fast path for identity selection: read each column via typed accessors,
/// write row-major into the flat output buffer.
fn push_chunk_identity(
    data: &mut Vec<TypedValue>,
    chunk: &crate::data_chunk::DataChunk,
    n: usize,
    num_cols: usize,
) {
    // Pre-fill with Null, then overwrite column-by-column.
    let base = data.len();
    data.resize(base + n * num_cols, TypedValue::Null);
    let dest = &mut data[base..];

    for col_idx in 0..num_cols {
        let col = chunk.column(col_idx);
        match col {
            ValueVector::Flat(flat) => {
                push_flat_strided(dest, col_idx, num_cols, flat, n);
            }
            ValueVector::String(sv) => {
                let sdata = sv.data();
                for i in 0..n {
                    dest[i * num_cols + col_idx] = match &sdata[i] {
                        Some(s) => TypedValue::String(s.clone()),
                        None => TypedValue::Null,
                    };
                }
            }
            ValueVector::Bool(bv) => {
                for i in 0..n {
                    dest[i * num_cols + col_idx] = bv.get_value(i);
                }
            }
            ValueVector::Owned(v) => {
                for i in 0..n {
                    dest[i * num_cols + col_idx] = v[i].clone();
                }
            }
        }
    }
}

/// Write a FlatVector column into strided flat storage using typed slice accessors.
fn push_flat_strided(
    dest: &mut [TypedValue],
    col_idx: usize,
    stride: usize,
    flat: &FlatVector,
    n: usize,
) {
    let nm = flat.null_mask();
    match flat.logical_type() {
        LogicalType::Int64 | LogicalType::Serial => {
            let slice = flat.data_as_i64_slice();
            for i in 0..n {
                if !nm.is_null(i as u64) {
                    dest[i * stride + col_idx] = TypedValue::Int64(slice[i]);
                }
            }
        }
        LogicalType::Int32 => {
            let slice = flat.data_as_i32_slice();
            for i in 0..n {
                if !nm.is_null(i as u64) {
                    dest[i * stride + col_idx] = TypedValue::Int32(slice[i]);
                }
            }
        }
        LogicalType::Double => {
            let slice = flat.data_as_f64_slice();
            for i in 0..n {
                if !nm.is_null(i as u64) {
                    dest[i * stride + col_idx] = TypedValue::Double(slice[i]);
                }
            }
        }
        LogicalType::Float => {
            let slice = flat.data_as_f32_slice();
            for i in 0..n {
                if !nm.is_null(i as u64) {
                    dest[i * stride + col_idx] = TypedValue::Float(slice[i]);
                }
            }
        }
        _ => {
            for i in 0..n {
                dest[i * stride + col_idx] = flat.get_value(i);
            }
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
        for row in self.iter_rows() {
            let cells: Vec<String> = row
                .iter()
                .zip(&self.column_names)
                .map(|(val, name)| format!("{:>width$}", format_value(val), width = name.len()))
                .collect();
            writeln!(f, "| {} |", cells.join(" | "))?;
        }
        writeln!(f, "({} row{})", self.num_rows_count, if self.num_rows_count == 1 { "" } else { "s" })
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
    fn row_access() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("a"), SmolStr::new("b")],
            vec![LogicalType::Int64, LogicalType::String],
        );
        result.push_row(vec![
            TypedValue::Int64(1),
            TypedValue::String(SmolStr::new("hello")),
        ]);
        assert_eq!(result.row(0)[0], TypedValue::Int64(1));
        assert_eq!(result.row(0)[1], TypedValue::String(SmolStr::new("hello")));
    }

    #[test]
    fn iter_rows_works() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("x")],
            vec![LogicalType::Int64],
        );
        result.push_row(vec![TypedValue::Int64(1)]);
        result.push_row(vec![TypedValue::Int64(2)]);
        let rows: Vec<_> = result.iter_rows().collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], TypedValue::Int64(1));
        assert_eq!(rows[1][0], TypedValue::Int64(2));
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
