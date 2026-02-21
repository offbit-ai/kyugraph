//! Empty operator — produces a single row with no columns, then exhausts.

use kyu_common::KyuResult;
use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;

pub struct EmptyOp {
    pub num_columns: usize,
    done: bool,
}

impl EmptyOp {
    pub fn new(num_columns: usize) -> Self {
        Self {
            num_columns,
            done: false,
        }
    }

    pub fn next(&mut self, _ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        // Produce a single row. If num_columns == 0, it's still one row with no columns.
        if self.num_columns == 0 {
            Ok(Some(DataChunk::new_with_row_count(Vec::new(), 1)))
        } else {
            Ok(Some(DataChunk::single_empty_row(self.num_columns)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_types::TypedValue;

    #[test]
    fn empty_produces_one_row() {
        let storage = crate::context::MockStorage::new();
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            &storage,
        );
        let mut op = EmptyOp::new(2);
        let chunk = op.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 1);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Null);
        assert!(op.next(&ctx).unwrap().is_none());
    }

    #[test]
    fn empty_zero_columns() {
        let storage = crate::context::MockStorage::new();
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            &storage,
        );
        let mut op = EmptyOp::new(0);
        let chunk = op.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_columns(), 0);
        assert!(op.next(&ctx).unwrap().is_none());
    }
}
