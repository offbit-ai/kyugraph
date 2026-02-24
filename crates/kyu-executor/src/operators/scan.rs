//! Scan operator — reads rows from Storage trait.
//!
//! Streams chunks one-at-a-time from storage, preserving native
//! FlatVector/BoolVector/StringVector column formats (no materialization).

use std::collections::VecDeque;

use kyu_common::KyuResult;
use kyu_common::id::TableId;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;

pub struct ScanNodeOp {
    pub table_id: TableId,
    /// Optional column indices to project during scan. When set, only these
    /// columns are kept, avoiding copies of unused columns (especially strings).
    pub column_indices: Option<Vec<usize>>,
    chunks: Option<VecDeque<DataChunk>>,
}

impl ScanNodeOp {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            column_indices: None,
            chunks: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.chunks.is_none() {
            let col_indices = self.column_indices.clone();
            self.chunks = Some(
                ctx.storage
                    .scan_table(self.table_id)
                    .map(move |chunk| {
                        if let Some(ref indices) = col_indices {
                            chunk.select_columns(indices)
                        } else {
                            chunk
                        }
                    })
                    .collect(),
            );
        }
        Ok(self.chunks.as_mut().unwrap().pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::TypedValue;
    use smol_str::SmolStr;

    fn make_storage() -> MockStorage {
        let mut storage = MockStorage::new();
        storage.insert_table(
            TableId(0),
            vec![
                vec![
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::Int64(25),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(30),
                ],
            ],
        );
        storage
    }

    #[test]
    fn scan_returns_all_rows() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let mut op = ScanNodeOp::new(TableId(0));
        let chunk = op.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.num_columns(), 2);
    }

    #[test]
    fn scan_exhausts_after_one_call() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let mut op = ScanNodeOp::new(TableId(0));
        assert!(op.next(&ctx).unwrap().is_some());
        assert!(op.next(&ctx).unwrap().is_none());
    }

    #[test]
    fn scan_missing_table_returns_none() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let mut op = ScanNodeOp::new(TableId(99));
        assert!(op.next(&ctx).unwrap().is_none());
    }
}
