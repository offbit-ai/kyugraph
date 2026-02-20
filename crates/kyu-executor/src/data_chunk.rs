//! DataChunk — columnar batch of rows flowing between physical operators.

use kyu_types::TypedValue;

/// A batch of rows in column-major format.
///
/// Each inner `Vec<TypedValue>` is one column. All columns have the same length.
#[derive(Clone, Debug)]
pub struct DataChunk {
    columns: Vec<Vec<TypedValue>>,
    num_rows: usize,
}

impl DataChunk {
    /// Create a new DataChunk from columns. All columns must have equal length.
    pub fn new(columns: Vec<Vec<TypedValue>>) -> Self {
        let num_rows = columns.first().map_or(0, |c| c.len());
        debug_assert!(columns.iter().all(|c| c.len() == num_rows));
        Self { columns, num_rows }
    }

    /// Create a DataChunk with a specific row count (for zero-column chunks).
    pub fn new_with_row_count(columns: Vec<Vec<TypedValue>>, num_rows: usize) -> Self {
        Self { columns, num_rows }
    }

    /// Create an empty DataChunk with the given number of columns.
    pub fn empty(num_columns: usize) -> Self {
        Self {
            columns: vec![Vec::new(); num_columns],
            num_rows: 0,
        }
    }

    /// Create a DataChunk with a single row of Null values.
    pub fn single_empty_row(num_columns: usize) -> Self {
        Self {
            columns: vec![vec![TypedValue::Null]; num_columns],
            num_rows: 1,
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
        Self { columns, num_rows }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Get a column by index.
    pub fn column(&self, idx: usize) -> &[TypedValue] {
        &self.columns[idx]
    }

    /// Get a row as a vector across all columns.
    pub fn get_row(&self, row_idx: usize) -> Vec<TypedValue> {
        self.columns.iter().map(|col| col[row_idx].clone()).collect()
    }

    /// Append a single row.
    pub fn append_row(&mut self, row: &[TypedValue]) {
        debug_assert_eq!(row.len(), self.columns.len());
        for (col_idx, val) in row.iter().enumerate() {
            self.columns[col_idx].push(val.clone());
        }
        self.num_rows += 1;
    }

    /// Append all rows from another chunk.
    pub fn append(&mut self, other: &DataChunk) {
        debug_assert_eq!(self.num_columns(), other.num_columns());
        for (col_idx, col) in other.columns.iter().enumerate() {
            self.columns[col_idx].extend_from_slice(col);
        }
        self.num_rows += other.num_rows;
    }

    /// Get the underlying columns.
    pub fn columns(&self) -> &[Vec<TypedValue>] {
        &self.columns
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
        assert_eq!(chunk.column(0)[0], TypedValue::Null);
        assert_eq!(chunk.column(1)[0], TypedValue::Null);
    }

    #[test]
    fn from_rows() {
        let rows = vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(10)],
            vec![TypedValue::Int64(2), TypedValue::Int64(20)],
        ];
        let chunk = DataChunk::from_rows(&rows, 2);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.column(0), &[TypedValue::Int64(1), TypedValue::Int64(2)]);
        assert_eq!(chunk.column(1), &[TypedValue::Int64(10), TypedValue::Int64(20)]);
    }

    #[test]
    fn get_row() {
        let chunk = DataChunk::new(vec![
            vec![TypedValue::Int64(1), TypedValue::Int64(2)],
            vec![TypedValue::Int64(10), TypedValue::Int64(20)],
        ]);
        assert_eq!(chunk.get_row(0), vec![TypedValue::Int64(1), TypedValue::Int64(10)]);
        assert_eq!(chunk.get_row(1), vec![TypedValue::Int64(2), TypedValue::Int64(20)]);
    }

    #[test]
    fn append_row() {
        let mut chunk = DataChunk::empty(2);
        chunk.append_row(&[TypedValue::Int64(1), TypedValue::Int64(2)]);
        assert_eq!(chunk.num_rows(), 1);
        chunk.append_row(&[TypedValue::Int64(3), TypedValue::Int64(4)]);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_row(1), vec![TypedValue::Int64(3), TypedValue::Int64(4)]);
    }

    #[test]
    fn append_chunks() {
        let mut chunk1 = DataChunk::new(vec![vec![TypedValue::Int64(1)]]);
        let chunk2 = DataChunk::new(vec![vec![TypedValue::Int64(2), TypedValue::Int64(3)]]);
        chunk1.append(&chunk2);
        assert_eq!(chunk1.num_rows(), 3);
        assert_eq!(
            chunk1.column(0),
            &[TypedValue::Int64(1), TypedValue::Int64(2), TypedValue::Int64(3)]
        );
    }
}
