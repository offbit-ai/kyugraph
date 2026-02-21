//! Scan operator — reads rows from Storage trait.

use kyu_common::id::TableId;
use kyu_common::KyuResult;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;

pub struct ScanNodeOp {
    pub table_id: TableId,
    exhausted: bool,
}

impl ScanNodeOp {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            exhausted: false,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.exhausted {
            return Ok(None);
        }
        self.exhausted = true;

        let mut merged: Option<DataChunk> = None;
        for chunk in ctx.storage.scan_table(self.table_id) {
            match merged {
                None => merged = Some(chunk),
                Some(ref mut m) => m.append(&chunk),
            }
        }
        Ok(merged)
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
                vec![TypedValue::String(SmolStr::new("Alice")), TypedValue::Int64(25)],
                vec![TypedValue::String(SmolStr::new("Bob")), TypedValue::Int64(30)],
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
