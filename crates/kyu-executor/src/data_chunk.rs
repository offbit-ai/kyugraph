//! DataChunk — columnar batch of rows flowing between physical operators.
//!
//! Backed by `ValueVector` columns (flat byte buffers, packed bits, or owned
//! TypedValues) with a `SelectionVector` for zero-copy filtering.

use kyu_types::TypedValue;

use crate::value_vector::{SelectionVector, ValueVector};

/// A batch of rows in column-major format.
///
/// Columns are `ValueVector`s — either flat byte buffers from storage or
/// owned TypedValue vecs from operators. A `SelectionVector` maps logical
/// row indices to physical positions, enabling zero-copy filtering.
#[derive(Clone, Debug)]
pub struct DataChunk {
    columns: Vec<ValueVector>,
    selection: SelectionVector,
}

impl DataChunk {
    /// Create from ValueVector columns + SelectionVector (scan path).
    pub fn from_vectors(columns: Vec<ValueVector>, selection: SelectionVector) -> Self {
        Self { columns, selection }
    }

    /// Create a new DataChunk from owned TypedValue columns (backward compat).
    pub fn new(columns: Vec<Vec<TypedValue>>) -> Self {
        let num_rows = columns.first().map_or(0, |c| c.len());
        debug_assert!(columns.iter().all(|c| c.len() == num_rows));
        let vectors = columns.into_iter().map(ValueVector::Owned).collect();
        Self {
            columns: vectors,
            selection: SelectionVector::identity(num_rows),
        }
    }

    /// Create a DataChunk with a specific row count (for zero-column chunks).
    pub fn new_with_row_count(columns: Vec<Vec<TypedValue>>, num_rows: usize) -> Self {
        let vectors = columns.into_iter().map(ValueVector::Owned).collect();
        Self {
            columns: vectors,
            selection: SelectionVector::identity(num_rows),
        }
    }

    /// Create an empty DataChunk with the given number of columns.
    pub fn empty(num_columns: usize) -> Self {
        Self {
            columns: (0..num_columns)
                .map(|_| ValueVector::Owned(Vec::new()))
                .collect(),
            selection: SelectionVector::identity(0),
        }
    }

    /// Create an empty DataChunk with pre-allocated column capacity.
    pub fn with_capacity(num_columns: usize, row_capacity: usize) -> Self {
        Self {
            columns: (0..num_columns)
                .map(|_| ValueVector::Owned(Vec::with_capacity(row_capacity)))
                .collect(),
            selection: SelectionVector::identity(0),
        }
    }

    /// Create a DataChunk with a single row of Null values.
    pub fn single_empty_row(num_columns: usize) -> Self {
        Self {
            columns: (0..num_columns)
                .map(|_| ValueVector::Owned(vec![TypedValue::Null]))
                .collect(),
            selection: SelectionVector::identity(1),
        }
    }

    /// Create a DataChunk from rows (row-major → column-major).
    pub fn from_rows(rows: &[Vec<TypedValue>], num_columns: usize) -> Self {
        let mut columns: Vec<Vec<TypedValue>> = (0..num_columns).map(|_| Vec::new()).collect();
        for row in rows {
            for (col_idx, val) in row.iter().enumerate() {
                if col_idx < num_columns {
                    columns[col_idx].push(val.clone());
                }
            }
        }
        let num_rows = rows.len();
        Self {
            columns: columns.into_iter().map(ValueVector::Owned).collect(),
            selection: SelectionVector::identity(num_rows),
        }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.selection.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// Get a single value by (logical) row and column index.
    #[inline]
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> TypedValue {
        let physical = self.selection.get(row_idx);
        self.columns[col_idx].get_value(physical)
    }

    /// Get a row as a vector across all columns.
    pub fn get_row(&self, row_idx: usize) -> Vec<TypedValue> {
        (0..self.columns.len())
            .map(|col| self.get_value(row_idx, col))
            .collect()
    }

    /// Append a single row. Only works when columns are Owned.
    pub fn append_row(&mut self, row: &[TypedValue]) {
        debug_assert_eq!(row.len(), self.columns.len());
        for (col_idx, val) in row.iter().enumerate() {
            self.columns[col_idx].push(val.clone());
        }
        self.selection = SelectionVector::identity(self.selection.len() + 1);
    }

    /// Append all rows from another chunk.
    /// Materializes self into Owned columns if needed (e.g., when self has
    /// Flat columns from storage).
    pub fn append(&mut self, other: &DataChunk) {
        debug_assert_eq!(self.num_columns(), other.num_columns());
        self.ensure_owned();
        for row_idx in 0..other.num_rows() {
            self.append_row_from_chunk(other, row_idx);
        }
    }

    /// Convert all non-Owned columns to Owned by materializing values.
    fn ensure_owned(&mut self) {
        let needs_materialize = self
            .columns
            .iter()
            .any(|c| !matches!(c, ValueVector::Owned(_)));
        if !needs_materialize {
            return;
        }
        let num_rows = self.num_rows();
        let num_cols = self.num_columns();
        let owned_columns: Vec<Vec<TypedValue>> = (0..num_cols)
            .map(|col| (0..num_rows).map(|row| self.get_value(row, col)).collect())
            .collect();
        self.columns = owned_columns.into_iter().map(ValueVector::Owned).collect();
        self.selection = SelectionVector::identity(num_rows);
    }

    /// Access the selection vector.
    #[inline]
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Direct access to a column's ValueVector.
    #[inline]
    pub fn column(&self, col_idx: usize) -> &ValueVector {
        &self.columns[col_idx]
    }

    /// Extract a column compacted through the selection vector.
    /// When the selection is identity, clones the column directly (cheap for
    /// FlatVector/Owned). When non-identity, materializes selected values.
    pub fn compact_column(&self, col_idx: usize) -> ValueVector {
        if self.selection.is_identity() {
            self.columns[col_idx].clone()
        } else {
            let n = self.selection.len();
            let mut result = Vec::with_capacity(n);
            for i in 0..n {
                result.push(self.columns[col_idx].get_value(self.selection.get(i)));
            }
            ValueVector::Owned(result)
        }
    }

    /// Create a new DataChunk containing only the specified columns (by move).
    pub fn select_columns(mut self, indices: &[usize]) -> Self {
        let columns = indices
            .iter()
            .map(|&i| std::mem::replace(&mut self.columns[i], ValueVector::Owned(Vec::new())))
            .collect();
        Self {
            columns,
            selection: self.selection,
        }
    }

    /// Move a column out of the chunk (replacing it with an empty Owned vec).
    /// For identity selection, avoids cloning the underlying data.
    /// For non-identity, materializes selected values.
    pub fn take_column(&mut self, col_idx: usize) -> ValueVector {
        if self.selection.is_identity() {
            std::mem::replace(&mut self.columns[col_idx], ValueVector::Owned(Vec::new()))
        } else {
            let n = self.selection.len();
            let mut result = Vec::with_capacity(n);
            for i in 0..n {
                result.push(self.columns[col_idx].get_value(self.selection.get(i)));
            }
            ValueVector::Owned(result)
        }
    }

    /// Replace selection, keeping columns intact. Consumes self.
    #[inline]
    pub fn with_selection(self, selection: SelectionVector) -> Self {
        Self {
            columns: self.columns,
            selection,
        }
    }

    /// Get a zero-copy row reference for expression evaluation.
    #[inline]
    pub fn row_ref(&self, row_idx: usize) -> RowRef<'_> {
        RowRef {
            chunk: self,
            row_idx,
        }
    }

    /// Copy a single row from a source chunk. Target must use Owned columns.
    pub fn append_row_from_chunk(&mut self, source: &DataChunk, row_idx: usize) {
        debug_assert_eq!(self.num_columns(), source.num_columns());
        for col_idx in 0..self.columns.len() {
            let val = source.get_value(row_idx, col_idx);
            self.columns[col_idx].push(val);
        }
        self.selection = SelectionVector::identity(self.selection.len() + 1);
    }

    /// Copy a row from source chunk and append an extra value as the last column.
    pub fn append_row_from_chunk_with_extra(
        &mut self,
        source: &DataChunk,
        row_idx: usize,
        extra: TypedValue,
    ) {
        debug_assert_eq!(self.num_columns(), source.num_columns() + 1);
        for col_idx in 0..source.num_columns() {
            let val = source.get_value(row_idx, col_idx);
            self.columns[col_idx].push(val);
        }
        self.columns[source.num_columns()].push(extra);
        self.selection = SelectionVector::identity(self.selection.len() + 1);
    }
}

/// Row reference into a DataChunk for expression evaluation.
///
/// Values are extracted on-demand from the underlying ValueVector columns
/// via the SelectionVector, avoiding upfront materialization.
pub struct RowRef<'a> {
    chunk: &'a DataChunk,
    row_idx: usize,
}

impl kyu_expression::Tuple for RowRef<'_> {
    #[inline]
    fn value_at(&self, col_idx: usize) -> Option<TypedValue> {
        if col_idx < self.chunk.num_columns() {
            Some(self.chunk.get_value(self.row_idx, col_idx))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    #[test]
    fn new_chunk() {
        let chunk = DataChunk::new(vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(2)],
            vec![
                TypedValue::String(SmolStr::new("a")),
                TypedValue::String(SmolStr::new("b")),
            ],
        ]);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.num_columns(), 2);
    }

    #[test]
    fn empty_chunk() {
        let chunk = DataChunk::empty(3);
        assert_eq!(chunk.num_rows(), 0);
        assert_eq!(chunk.num_columns(), 3);
        assert!(chunk.is_empty());
    }

    #[test]
    fn single_empty_row() {
        let chunk = DataChunk::single_empty_row(2);
        assert_eq!(chunk.num_rows(), 1);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Null);
        assert_eq!(chunk.get_value(0, 1), TypedValue::Null);
    }

    #[test]
    fn from_rows() {
        let rows = vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(10)],
            vec![TypedValue::Int64(2), TypedValue::Int64(20)],
        ];
        let chunk = DataChunk::from_rows(&rows, 2);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(1));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(2));
        assert_eq!(chunk.get_value(0, 1), TypedValue::Int64(10));
        assert_eq!(chunk.get_value(1, 1), TypedValue::Int64(20));
    }

    #[test]
    fn get_row() {
        let chunk = DataChunk::new(vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(2)],
            vec![TypedValue::Int64(10), TypedValue::Int64(20)],
        ]);
        assert_eq!(
            chunk.get_row(0),
            vec![TypedValue::Int64(1), TypedValue::Int64(10)]
        );
        assert_eq!(
            chunk.get_row(1),
            vec![TypedValue::Int64(2), TypedValue::Int64(20)]
        );
    }

    #[test]
    fn append_row() {
        let mut chunk = DataChunk::empty(2);
        chunk.append_row(&[TypedValue::Int64(1), TypedValue::Int64(2)]);
        assert_eq!(chunk.num_rows(), 1);
        chunk.append_row(&[TypedValue::Int64(3), TypedValue::Int64(4)]);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(
            chunk.get_row(1),
            vec![TypedValue::Int64(3), TypedValue::Int64(4)]
        );
    }

    #[test]
    fn row_ref_value_at() {
        use kyu_expression::Tuple;
        let chunk = DataChunk::new(vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(2)],
            vec![TypedValue::Int64(10), TypedValue::Int64(20)],
        ]);
        let row = chunk.row_ref(0);
        assert_eq!(row.value_at(0), Some(TypedValue::Int64(1)));
        assert_eq!(row.value_at(1), Some(TypedValue::Int64(10)));
        assert_eq!(row.value_at(2), None);

        let row1 = chunk.row_ref(1);
        assert_eq!(row1.value_at(0), Some(TypedValue::Int64(2)));
        assert_eq!(row1.value_at(1), Some(TypedValue::Int64(20)));
    }

    #[test]
    fn append_row_from_chunk() {
        let src = DataChunk::new(vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(2)],
            vec![TypedValue::Int64(10), TypedValue::Int64(20)],
        ]);
        let mut dst = DataChunk::with_capacity(2, 2);
        dst.append_row_from_chunk(&src, 1);
        assert_eq!(dst.num_rows(), 1);
        assert_eq!(
            dst.get_row(0),
            vec![TypedValue::Int64(2), TypedValue::Int64(20)]
        );
    }

    #[test]
    fn append_chunks() {
        let mut chunk1 = DataChunk::new(vec![vec![TypedValue::Int64(1)]]);
        let chunk2 = DataChunk::new(vec![vec![TypedValue::Int64(2), TypedValue::Int64(3)]]);
        chunk1.append(&chunk2);
        assert_eq!(chunk1.num_rows(), 3);
        assert_eq!(chunk1.get_value(0, 0), TypedValue::Int64(1));
        assert_eq!(chunk1.get_value(1, 0), TypedValue::Int64(2));
        assert_eq!(chunk1.get_value(2, 0), TypedValue::Int64(3));
    }

    #[test]
    fn with_selection_filters() {
        let chunk = DataChunk::new(vec![vec![
            TypedValue::Int64(10),
            TypedValue::Int64(20),
            TypedValue::Int64(30),
        ]]);
        let filtered = chunk.with_selection(SelectionVector::from_indices(vec![0, 2]));
        assert_eq!(filtered.num_rows(), 2);
        assert_eq!(filtered.get_value(0, 0), TypedValue::Int64(10));
        assert_eq!(filtered.get_value(1, 0), TypedValue::Int64(30));
    }
}
