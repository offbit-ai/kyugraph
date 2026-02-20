//! Execution context — mock storage for Phase 6 testing.

use hashbrown::HashMap;
use kyu_catalog::CatalogContent;
use kyu_common::id::TableId;
use kyu_types::TypedValue;

/// Mock in-memory storage: table_id → rows (each row is a Vec<TypedValue>).
///
/// In Phase 7+, this will be replaced by real NodeGroup/ColumnChunk iteration
/// through the buffer manager.
#[derive(Clone, Debug)]
pub struct MockStorage {
    tables: HashMap<TableId, Vec<Vec<TypedValue>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Insert rows for a table.
    pub fn insert_table(&mut self, table_id: TableId, rows: Vec<Vec<TypedValue>>) {
        self.tables.insert(table_id, rows);
    }

    /// Get all rows for a table.
    pub fn scan_table(&self, table_id: TableId) -> Option<&[Vec<TypedValue>]> {
        self.tables.get(&table_id).map(|v| v.as_slice())
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution context holding catalog and storage references.
#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub catalog: CatalogContent,
    pub storage: MockStorage,
}

impl ExecutionContext {
    pub fn new(catalog: CatalogContent, storage: MockStorage) -> Self {
        Self { catalog, storage }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    #[test]
    fn mock_storage_insert_and_scan() {
        let mut storage = MockStorage::new();
        let rows = vec![
            vec![TypedValue::Int64(1), TypedValue::String(SmolStr::new("Alice"))],
            vec![TypedValue::Int64(2), TypedValue::String(SmolStr::new("Bob"))],
        ];
        storage.insert_table(TableId(0), rows);
        let result = storage.scan_table(TableId(0)).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn mock_storage_missing_table() {
        let storage = MockStorage::new();
        assert!(storage.scan_table(TableId(99)).is_none());
    }
}
