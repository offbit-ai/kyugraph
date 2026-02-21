//! ChunkedNodeGroup: groups column chunks for a batch of rows.
//!
//! Each ChunkedNodeGroup holds up to `CHUNKED_NODE_GROUP_CAPACITY` (2,048) rows
//! across all columns.

use kyu_types::LogicalType;

use crate::column_chunk::ColumnChunk;
use crate::constants::CHUNKED_NODE_GROUP_CAPACITY;
use crate::storage_types::{NodeGroupFormat, ResidencyState};

/// A group of column chunks, one per column, holding up to
/// `CHUNKED_NODE_GROUP_CAPACITY` rows.
pub struct ChunkedNodeGroup {
    format: NodeGroupFormat,
    residency_state: ResidencyState,
    start_row_idx: u64,
    capacity: u64,
    num_rows: u64,
    chunks: Vec<ColumnChunk>,
}

impl ChunkedNodeGroup {
    /// Create a new ChunkedNodeGroup with the given column types.
    pub fn new(data_types: &[LogicalType], start_row_idx: u64) -> Self {
        Self::with_capacity(data_types, start_row_idx, CHUNKED_NODE_GROUP_CAPACITY)
    }

    /// Create a ChunkedNodeGroup with a custom capacity.
    pub fn with_capacity(
        data_types: &[LogicalType],
        start_row_idx: u64,
        capacity: u64,
    ) -> Self {
        let chunks = data_types
            .iter()
            .map(|dt| ColumnChunk::new(dt.clone(), capacity))
            .collect();
        Self {
            format: NodeGroupFormat::Regular,
            residency_state: ResidencyState::InMemory,
            start_row_idx,
            capacity,
            num_rows: 0,
            chunks,
        }
    }

    pub fn format(&self) -> NodeGroupFormat {
        self.format
    }

    pub fn set_format(&mut self, format: NodeGroupFormat) {
        self.format = format;
    }

    pub fn residency_state(&self) -> ResidencyState {
        self.residency_state
    }

    pub fn start_row_idx(&self) -> u64 {
        self.start_row_idx
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn num_columns(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_full(&self) -> bool {
        self.num_rows >= self.capacity
    }

    pub fn remaining_capacity(&self) -> u64 {
        self.capacity.saturating_sub(self.num_rows)
    }

    pub fn column(&self, idx: usize) -> &ColumnChunk {
        &self.chunks[idx]
    }

    pub fn column_mut(&mut self, idx: usize) -> &mut ColumnChunk {
        &mut self.chunks[idx]
    }

    /// Append a row of raw byte values across all columns.
    /// `values[i]` is `Some(bytes)` for a non-null value, `None` for null.
    /// Returns the local row index within this chunked group.
    pub fn append_row(&mut self, values: &[Option<&[u8]>]) -> u64 {
        debug_assert!(!self.is_full());
        debug_assert_eq!(values.len(), self.chunks.len());

        let row = self.num_rows;
        for (col_idx, value) in values.iter().enumerate() {
            match value {
                Some(bytes) => {
                    let chunk = &mut self.chunks[col_idx];
                    match chunk {
                        ColumnChunk::Fixed(c) => {
                            c.set_raw(row, bytes);
                            c.set_num_values(row + 1);
                        }
                        ColumnChunk::Bool(c) => {
                            let val = bytes[0] != 0;
                            c.set_bool(row, val);
                            c.set_num_values(row + 1);
                        }
                        ColumnChunk::String(c) => {
                            let s = std::str::from_utf8(bytes).unwrap_or("");
                            c.set_string(row, smol_str::SmolStr::new(s));
                            c.set_num_values(row + 1);
                        }
                    }
                }
                None => {
                    let chunk = &mut self.chunks[col_idx];
                    chunk.set_null(row, true);
                    chunk.set_num_values(row + 1);
                }
            }
        }
        self.num_rows += 1;
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_chunked_node_group() {
        let types = vec![LogicalType::Int64, LogicalType::String];
        let group = ChunkedNodeGroup::new(&types, 0);
        assert_eq!(group.num_rows(), 0);
        assert_eq!(group.capacity(), CHUNKED_NODE_GROUP_CAPACITY);
        assert_eq!(group.num_columns(), 2);
        assert!(!group.is_full());
        assert_eq!(group.remaining_capacity(), CHUNKED_NODE_GROUP_CAPACITY);
    }

    #[test]
    fn append_row_increments_count() {
        let types = vec![LogicalType::Int32, LogicalType::Int64];
        let mut group = ChunkedNodeGroup::new(&types, 0);

        let val_i32: i32 = 42;
        let val_i64: i64 = 100;
        let row = group.append_row(&[
            Some(&val_i32.to_ne_bytes()),
            Some(&val_i64.to_ne_bytes()),
        ]);

        assert_eq!(row, 0);
        assert_eq!(group.num_rows(), 1);
    }

    #[test]
    fn append_row_with_null() {
        let types = vec![LogicalType::Int32, LogicalType::Int64];
        let mut group = ChunkedNodeGroup::new(&types, 0);

        let val_i32: i32 = 42;
        group.append_row(&[Some(&val_i32.to_ne_bytes()), None]);

        assert_eq!(group.num_rows(), 1);
        assert!(!group.column(0).is_null(0));
        assert!(group.column(1).is_null(0));
    }

    #[test]
    fn append_multiple_rows() {
        let types = vec![LogicalType::Int64];
        let mut group = ChunkedNodeGroup::new(&types, 0);

        for i in 0..10u64 {
            let val = i as i64;
            group.append_row(&[Some(&val.to_ne_bytes())]);
        }
        assert_eq!(group.num_rows(), 10);
        assert_eq!(group.remaining_capacity(), CHUNKED_NODE_GROUP_CAPACITY - 10);
    }

    #[test]
    fn with_custom_capacity() {
        let types = vec![LogicalType::Int32];
        let group = ChunkedNodeGroup::with_capacity(&types, 100, 16);
        assert_eq!(group.capacity(), 16);
        assert_eq!(group.start_row_idx(), 100);
    }

    #[test]
    fn is_full_at_capacity() {
        let types = vec![LogicalType::Int32];
        let mut group = ChunkedNodeGroup::with_capacity(&types, 0, 2);

        let v1: i32 = 1;
        let v2: i32 = 2;
        group.append_row(&[Some(&v1.to_ne_bytes())]);
        assert!(!group.is_full());
        group.append_row(&[Some(&v2.to_ne_bytes())]);
        assert!(group.is_full());
    }

    #[test]
    fn bool_column() {
        let types = vec![LogicalType::Bool];
        let mut group = ChunkedNodeGroup::new(&types, 0);

        group.append_row(&[Some(&[1u8])]);
        group.append_row(&[Some(&[0u8])]);
        group.append_row(&[None]);

        match group.column(0) {
            ColumnChunk::Bool(c) => {
                assert!(c.get_bool(0));
                assert!(!c.get_bool(1));
                assert!(c.is_null(2));
            }
            _ => panic!("expected Bool chunk"),
        }
    }

    #[test]
    fn column_access() {
        let types = vec![LogicalType::Int32, LogicalType::Double];
        let mut group = ChunkedNodeGroup::new(&types, 0);

        let v1: i32 = 42;
        let v2: f64 = 3.14;
        group.append_row(&[Some(&v1.to_ne_bytes()), Some(&v2.to_ne_bytes())]);

        assert_eq!(*group.column(0).data_type(), LogicalType::Int32);
        assert_eq!(*group.column(1).data_type(), LogicalType::Double);
    }

    #[test]
    fn format_default() {
        let group = ChunkedNodeGroup::new(&[LogicalType::Int32], 0);
        assert_eq!(group.format(), NodeGroupFormat::Regular);
    }

    #[test]
    fn residency_state_default() {
        let group = ChunkedNodeGroup::new(&[LogicalType::Int32], 0);
        assert_eq!(group.residency_state(), ResidencyState::InMemory);
    }
}
