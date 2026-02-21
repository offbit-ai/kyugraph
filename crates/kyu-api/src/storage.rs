//! NodeGroupStorage — bridges kyu-storage's columnar engine with kyu-executor's DataChunk.

use hashbrown::HashMap;
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_executor::value_vector::{BoolVector, FlatVector, StringVector};
use kyu_executor::{DataChunk, SelectionVector, Storage, ValueVector};
use kyu_storage::{ChunkedNodeGroup, ColumnChunk, NodeGroup, NodeGroupIdx, NullMask};
use kyu_types::{LogicalType, TypedValue};

struct TableData {
    schema: Vec<LogicalType>,
    node_group: NodeGroup,
    /// Soft-delete bitset: bit=1 at position i means row i is deleted.
    /// Uses the same NullMask from kyu-storage (packed u64, O(1) set/check).
    deleted: NullMask,
}

/// Real columnar storage backed by NodeGroup/ColumnChunk.
#[derive(Debug)]
pub struct NodeGroupStorage {
    tables: HashMap<TableId, TableData>,
}

impl std::fmt::Debug for TableData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableData")
            .field("schema", &self.schema)
            .field("num_rows", &self.node_group.num_rows())
            .finish()
    }
}

impl Default for NodeGroupStorage {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

impl NodeGroupStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a table with the given column types.
    pub fn create_table(&mut self, table_id: TableId, schema: Vec<LogicalType>) {
        self.tables.insert(
            table_id,
            TableData {
                node_group: NodeGroup::new(NodeGroupIdx(table_id.0), schema.clone()),
                schema,
                deleted: NullMask::new(0),
            },
        );
    }

    /// Drop a table.
    pub fn drop_table(&mut self, table_id: TableId) {
        self.tables.remove(&table_id);
    }

    /// Check if a table exists.
    pub fn has_table(&self, table_id: TableId) -> bool {
        self.tables.contains_key(&table_id)
    }

    /// Get the schema (column types) for a table.
    pub fn table_schema(&self, table_id: TableId) -> Option<&[LogicalType]> {
        self.tables.get(&table_id).map(|t| t.schema.as_slice())
    }

    /// Get the number of rows in a table.
    pub fn num_rows(&self, table_id: TableId) -> u64 {
        self.tables
            .get(&table_id)
            .map_or(0, |t| t.node_group.num_rows())
    }

    /// Insert a row of TypedValues into a table's NodeGroup.
    pub fn insert_row(&mut self, table_id: TableId, values: &[TypedValue]) -> KyuResult<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| KyuError::Storage(format!("table {:?} not found", table_id)))?;

        let raw_values: Vec<Option<Vec<u8>>> = values
            .iter()
            .zip(&table.schema)
            .map(|(val, ty)| typed_value_to_bytes(val, ty))
            .collect();

        let refs: Vec<Option<&[u8]>> = raw_values.iter().map(|opt| opt.as_deref()).collect();
        table.node_group.append_row(&refs);

        // Grow the deleted mask to cover the new row.
        let num_rows = table.node_group.num_rows();
        table.deleted = NullMask::new(num_rows);

        Ok(())
    }

    /// Update a single cell in-place.
    pub fn update_cell(
        &mut self,
        table_id: TableId,
        row_idx: u64,
        col_idx: usize,
        value: &TypedValue,
    ) -> KyuResult<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| KyuError::Storage(format!("table {:?} not found", table_id)))?;

        let (chunk_idx, local_row) = table.node_group.global_row_to_chunked_group(row_idx);
        let chunk = table.node_group.chunked_group_mut(chunk_idx);
        let col = chunk.column_mut(col_idx);

        match (col, value) {
            (ColumnChunk::Fixed(c), TypedValue::Null) => {
                c.set_null(local_row, true);
            }
            (ColumnChunk::Fixed(c), TypedValue::Int8(v)) => {
                c.set_value::<i8>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Fixed(c), TypedValue::Int16(v)) => {
                c.set_value::<i16>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Fixed(c), TypedValue::Int32(v)) => {
                c.set_value::<i32>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Fixed(c), TypedValue::Int64(v)) => {
                c.set_value::<i64>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Fixed(c), TypedValue::Float(v)) => {
                c.set_value::<f32>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Fixed(c), TypedValue::Double(v)) => {
                c.set_value::<f64>(local_row, *v);
                c.set_null(local_row, false);
            }
            (ColumnChunk::Bool(c), TypedValue::Null) => {
                c.set_null(local_row, true);
            }
            (ColumnChunk::Bool(c), TypedValue::Bool(v)) => {
                c.set_bool(local_row, *v);
            }
            (ColumnChunk::String(c), TypedValue::Null) => {
                c.set_null(local_row, true);
            }
            (ColumnChunk::String(c), TypedValue::String(s)) => {
                c.set_string(local_row, s.clone());
            }
            _ => {
                return Err(KyuError::Storage(format!(
                    "type mismatch: cannot write {:?} to column {}",
                    value, col_idx
                )));
            }
        }
        Ok(())
    }

    /// Scan all non-deleted rows with their global row indices.
    /// Returns (global_row_idx, row_values) for each live row.
    pub fn scan_rows(&self, table_id: TableId) -> KyuResult<Vec<(u64, Vec<TypedValue>)>> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or_else(|| KyuError::Storage(format!("table {:?} not found", table_id)))?;

        let num_chunks = table.node_group.num_chunked_groups();
        let has_deletions = !table.deleted.has_no_nulls_guarantee();
        let mut rows = Vec::new();

        for chunk_idx in 0..num_chunks {
            let cng = table.node_group.chunked_group(chunk_idx);
            let base_row = chunk_idx as u64 * kyu_storage::CHUNKED_NODE_GROUP_CAPACITY;
            let num_rows = cng.num_rows() as usize;

            let columns: Vec<ValueVector> = (0..cng.num_columns())
                .map(|col| column_chunk_to_value_vector(cng.column(col), num_rows))
                .collect();

            for local_row in 0..num_rows {
                let global_row = base_row + local_row as u64;
                if has_deletions && table.deleted.is_null(global_row) {
                    continue;
                }
                let row: Vec<TypedValue> =
                    columns.iter().map(|col| col.get_value(local_row)).collect();
                rows.push((global_row, row));
            }
        }

        Ok(rows)
    }

    /// Soft-delete a row (mark as deleted; skipped during scans).
    pub fn delete_row(&mut self, table_id: TableId, row_idx: u64) -> KyuResult<()> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| KyuError::Storage(format!("table {:?} not found", table_id)))?;

        table.deleted.set_null(row_idx, true);
        Ok(())
    }
}

impl Storage for NodeGroupStorage {
    fn scan_table(&self, table_id: TableId) -> Box<dyn Iterator<Item = DataChunk> + '_> {
        let table = match self.tables.get(&table_id) {
            Some(t) if t.node_group.num_rows() > 0 => t,
            _ => return Box::new(std::iter::empty()),
        };
        let num_chunks = table.node_group.num_chunked_groups();
        let has_deletions = !table.deleted.has_no_nulls_guarantee();
        Box::new((0..num_chunks).filter_map(move |idx| {
            let cng = table.node_group.chunked_group(idx);
            let base_row = idx as u64 * kyu_storage::CHUNKED_NODE_GROUP_CAPACITY;
            let chunk = if has_deletions {
                chunked_group_to_data_chunk_filtered(cng, &table.deleted, base_row)
            } else {
                chunked_group_to_data_chunk(cng)
            };
            if chunk.num_rows() == 0 {
                None
            } else {
                Some(chunk)
            }
        }))
    }
}

/// Convert a ChunkedNodeGroup to a DataChunk, skipping rows marked deleted.
fn chunked_group_to_data_chunk_filtered(
    cng: &ChunkedNodeGroup,
    deleted: &NullMask,
    base_row: u64,
) -> DataChunk {
    let num_rows = cng.num_rows() as usize;
    let columns: Vec<ValueVector> = (0..cng.num_columns())
        .map(|i| column_chunk_to_value_vector(cng.column(i), num_rows))
        .collect();
    let live_indices: Vec<u32> = (0..num_rows)
        .filter(|&i| !deleted.is_null(base_row + i as u64))
        .map(|i| i as u32)
        .collect();
    let sel = if live_indices.len() == num_rows {
        SelectionVector::identity(num_rows)
    } else {
        SelectionVector::from_indices(live_indices)
    };
    DataChunk::from_vectors(columns, sel)
}

/// Convert a ChunkedNodeGroup to a DataChunk.
fn chunked_group_to_data_chunk(cng: &ChunkedNodeGroup) -> DataChunk {
    let num_rows = cng.num_rows() as usize;
    let columns: Vec<ValueVector> = (0..cng.num_columns())
        .map(|i| column_chunk_to_value_vector(cng.column(i), num_rows))
        .collect();
    DataChunk::from_vectors(columns, SelectionVector::identity(num_rows))
}

/// Convert a ColumnChunk to a ValueVector.
fn column_chunk_to_value_vector(chunk: &ColumnChunk, num_rows: usize) -> ValueVector {
    match chunk {
        ColumnChunk::Fixed(c) => ValueVector::Flat(FlatVector::from_column_chunk(c, num_rows)),
        ColumnChunk::Bool(c) => ValueVector::Bool(BoolVector::from_bool_chunk(c, num_rows)),
        ColumnChunk::String(c) => ValueVector::String(StringVector::from_string_chunk(c, num_rows)),
    }
}

/// Convert a TypedValue to raw bytes for storage in a NodeGroup.
fn typed_value_to_bytes(val: &TypedValue, _ty: &LogicalType) -> Option<Vec<u8>> {
    match val {
        TypedValue::Null => None,
        TypedValue::Bool(b) => Some(vec![if *b { 1u8 } else { 0u8 }]),
        TypedValue::Int8(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::Int16(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::Int32(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::Int64(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::Float(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::Double(v) => Some(v.to_ne_bytes().to_vec()),
        TypedValue::String(s) => Some(s.as_bytes().to_vec()),
        _ => None, // unsupported types stored as null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    #[test]
    fn create_and_scan_empty_table() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        assert!(storage.has_table(TableId(0)));

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert!(chunks.is_empty()); // no rows yet
    }

    #[test]
    fn insert_and_scan_int64() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);

        storage.insert_row(TableId(0), &[TypedValue::Int64(42)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Int64(100)]).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Int64(42));
        assert_eq!(chunks[0].get_value(1, 0), TypedValue::Int64(100));
    }

    #[test]
    fn insert_and_scan_string() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64, LogicalType::String]);

        storage
            .insert_row(
                TableId(0),
                &[TypedValue::Int64(1), TypedValue::String(SmolStr::new("Alice"))],
            )
            .unwrap();
        storage
            .insert_row(
                TableId(0),
                &[TypedValue::Int64(2), TypedValue::String(SmolStr::new("Bob"))],
            )
            .unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Int64(1));
        assert_eq!(chunks[0].get_value(0, 1), TypedValue::String(SmolStr::new("Alice")));
        assert_eq!(chunks[0].get_value(1, 1), TypedValue::String(SmolStr::new("Bob")));
    }

    #[test]
    fn insert_and_scan_bool() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Bool]);

        storage.insert_row(TableId(0), &[TypedValue::Bool(true)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Bool(false)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Null]).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Bool(true));
        assert_eq!(chunks[0].get_value(1, 0), TypedValue::Bool(false));
        assert_eq!(chunks[0].get_value(2, 0), TypedValue::Null);
    }

    #[test]
    fn drop_table() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        assert!(storage.has_table(TableId(0)));
        storage.drop_table(TableId(0));
        assert!(!storage.has_table(TableId(0)));
    }

    #[test]
    fn insert_into_missing_table_errors() {
        let mut storage = NodeGroupStorage::new();
        let result = storage.insert_row(TableId(99), &[TypedValue::Int64(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn scan_missing_table_returns_empty() {
        let storage = NodeGroupStorage::new();
        let chunks: Vec<DataChunk> = storage.scan_table(TableId(99)).collect();
        assert!(chunks.is_empty());
    }

    #[test]
    fn update_cell_int64() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        storage.insert_row(TableId(0), &[TypedValue::Int64(42)]).unwrap();

        storage.update_cell(TableId(0), 0, 0, &TypedValue::Int64(99)).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Int64(99));
    }

    #[test]
    fn update_cell_string() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::String]);
        storage.insert_row(TableId(0), &[TypedValue::String(SmolStr::new("old"))]).unwrap();

        storage.update_cell(TableId(0), 0, 0, &TypedValue::String(SmolStr::new("new"))).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::String(SmolStr::new("new")));
    }

    #[test]
    fn update_cell_to_null() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        storage.insert_row(TableId(0), &[TypedValue::Int64(42)]).unwrap();

        storage.update_cell(TableId(0), 0, 0, &TypedValue::Null).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Null);
    }

    #[test]
    fn delete_row_skips_in_scan() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        storage.insert_row(TableId(0), &[TypedValue::Int64(1)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Int64(2)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Int64(3)]).unwrap();

        storage.delete_row(TableId(0), 1).unwrap(); // delete row with value 2

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(chunks[0].get_value(0, 0), TypedValue::Int64(1));
        assert_eq!(chunks[0].get_value(1, 0), TypedValue::Int64(3));
    }

    #[test]
    fn delete_all_rows_returns_empty_scan() {
        let mut storage = NodeGroupStorage::new();
        storage.create_table(TableId(0), vec![LogicalType::Int64]);
        storage.insert_row(TableId(0), &[TypedValue::Int64(1)]).unwrap();
        storage.insert_row(TableId(0), &[TypedValue::Int64(2)]).unwrap();

        storage.delete_row(TableId(0), 0).unwrap();
        storage.delete_row(TableId(0), 1).unwrap();

        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert!(chunks.is_empty());
    }
}
