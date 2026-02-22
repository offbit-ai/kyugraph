//! Execution context — storage trait and execution context.

use hashbrown::HashMap;
use kyu_catalog::CatalogContent;
use kyu_common::id::TableId;
use kyu_types::TypedValue;

use crate::data_chunk::DataChunk;

/// Abstraction over table storage backends.
///
/// kyu-executor depends only on this trait. Concrete implementations
/// (MockStorage for tests, NodeGroupStorage for real storage) live
/// in their respective crates.
pub trait Storage: std::fmt::Debug {
    /// Iterate DataChunk batches for a table. Returns empty iterator if table missing.
    fn scan_table(&self, table_id: TableId) -> Box<dyn Iterator<Item = DataChunk> + '_>;
}

/// Mock in-memory storage: table_id → rows (each row is a Vec<TypedValue>).
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
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MockStorage {
    fn scan_table(&self, table_id: TableId) -> Box<dyn Iterator<Item = DataChunk> + '_> {
        match self.tables.get(&table_id) {
            Some(rows) if !rows.is_empty() => {
                let num_cols = rows[0].len();
                Box::new(std::iter::once(DataChunk::from_rows(rows, num_cols)))
            }
            _ => Box::new(std::iter::empty()),
        }
    }
}

/// Execution context holding catalog and storage references.
pub struct ExecutionContext<'a> {
    pub catalog: CatalogContent,
    pub storage: &'a dyn Storage,
    #[cfg(feature = "jit")]
    jit_cache: Option<std::sync::Arc<crate::jit::ExpressionCache>>,
}

impl std::fmt::Debug for ExecutionContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("catalog", &self.catalog)
            .field("storage", &self.storage)
            .finish()
    }
}

impl<'a> ExecutionContext<'a> {
    pub fn new(catalog: CatalogContent, storage: &'a dyn Storage) -> Self {
        Self {
            catalog,
            storage,
            #[cfg(feature = "jit")]
            jit_cache: None,
        }
    }

    /// Create an execution context with a JIT expression cache.
    #[cfg(feature = "jit")]
    pub fn with_jit_cache(
        catalog: CatalogContent,
        storage: &'a dyn Storage,
        cache: std::sync::Arc<crate::jit::ExpressionCache>,
    ) -> Self {
        Self {
            catalog,
            storage,
            jit_cache: Some(cache),
        }
    }

    /// Get the JIT expression cache, if available.
    #[cfg(feature = "jit")]
    pub fn jit_cache(&self) -> Option<&std::sync::Arc<crate::jit::ExpressionCache>> {
        self.jit_cache.as_ref()
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
        let chunks: Vec<DataChunk> = storage.scan_table(TableId(0)).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
    }

    #[test]
    fn mock_storage_missing_table() {
        let storage = MockStorage::new();
        let chunks: Vec<DataChunk> = storage.scan_table(TableId(99)).collect();
        assert!(chunks.is_empty());
    }
}
