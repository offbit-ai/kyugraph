//! NodeGroupStorage — bridges kyu-storage's columnar engine with kyu-executor's DataChunk.

use hashbrown::HashMap;
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_executor::{DataChunk, Storage};
use kyu_storage::{ChunkedNodeGroup, ColumnChunk, ColumnChunkData, FixedSizeValue, NodeGroup, NodeGroupIdx};
use kyu_types::{LogicalType, TypedValue};

struct TableData {
    schema: Vec<LogicalType>,
    node_group: NodeGroup,
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
        Box::new((0..num_chunks).map(move |idx| {
            let cng = table.node_group.chunked_group(idx);
            chunked_group_to_data_chunk(cng, &table.schema)
        }))
    }
}

/// Convert a ChunkedNodeGroup to a DataChunk.
fn chunked_group_to_data_chunk(cng: &ChunkedNodeGroup, schema: &[LogicalType]) -> DataChunk {
    let num_rows = cng.num_rows() as usize;
    let columns: Vec<Vec<TypedValue>> = (0..cng.num_columns())
        .map(|col_idx| column_chunk_to_typed_values(cng.column(col_idx), num_rows, &schema[col_idx]))
        .collect();
    DataChunk::new(columns)
}

/// Convert a ColumnChunk to a Vec<TypedValue>.
fn column_chunk_to_typed_values(
    chunk: &ColumnChunk,
    num_rows: usize,
    logical_type: &LogicalType,
) -> Vec<TypedValue> {
    let n = num_rows as u64;
    match chunk {
        ColumnChunk::Bool(c) => c
            .scan_range(0, n)
            .into_iter()
            .map(|opt| opt.map_or(TypedValue::Null, TypedValue::Bool))
            .collect(),
        ColumnChunk::String(c) => c
            .scan_range(0, n)
            .iter()
            .map(|opt| match opt {
                Some(s) => TypedValue::String(s.clone()),
                None => TypedValue::Null,
            })
            .collect(),
        ColumnChunk::Fixed(c) => match logical_type {
            LogicalType::Int8 => convert_fixed::<i8>(c, n, TypedValue::Int8),
            LogicalType::Int16 => convert_fixed::<i16>(c, n, TypedValue::Int16),
            LogicalType::Int32 => convert_fixed::<i32>(c, n, TypedValue::Int32),
            LogicalType::Int64 | LogicalType::Serial => {
                convert_fixed::<i64>(c, n, TypedValue::Int64)
            }
            LogicalType::Float => convert_fixed::<f32>(c, n, TypedValue::Float),
            LogicalType::Double => convert_fixed::<f64>(c, n, TypedValue::Double),
            _ => vec![TypedValue::Null; num_rows],
        },
    }
}

fn convert_fixed<T: FixedSizeValue>(
    c: &ColumnChunkData,
    n: u64,
    wrap: impl Fn(T) -> TypedValue,
) -> Vec<TypedValue> {
    c.scan_range::<T>(0, n)
        .into_iter()
        .map(|opt| opt.map_or(TypedValue::Null, &wrap))
        .collect()
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
        assert_eq!(chunks[0].column(0)[0], TypedValue::Int64(42));
        assert_eq!(chunks[0].column(0)[1], TypedValue::Int64(100));
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
        assert_eq!(chunks[0].column(0)[0], TypedValue::Int64(1));
        assert_eq!(chunks[0].column(1)[0], TypedValue::String(SmolStr::new("Alice")));
        assert_eq!(chunks[0].column(1)[1], TypedValue::String(SmolStr::new("Bob")));
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
        assert_eq!(chunks[0].column(0)[0], TypedValue::Bool(true));
        assert_eq!(chunks[0].column(0)[1], TypedValue::Bool(false));
        assert_eq!(chunks[0].column(0)[2], TypedValue::Null);
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
}
