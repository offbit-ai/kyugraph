//! Limit operator — SKIP + LIMIT row counting.

use kyu_common::KyuResult;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct LimitOp {
    pub child: Box<PhysicalOperator>,
    pub skip: u64,
    pub limit: u64,
    skipped: u64,
    emitted: u64,
}

impl LimitOp {
    pub fn new(child: PhysicalOperator, skip: u64, limit: u64) -> Self {
        Self {
            child: Box::new(child),
            skip,
            limit,
            skipped: 0,
            emitted: 0,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
        if self.emitted >= self.limit {
            return Ok(None);
        }

        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let num_cols = chunk.num_columns();
            let mut result = DataChunk::empty(num_cols);

            for row_idx in 0..chunk.num_rows() {
                if self.emitted >= self.limit {
                    break;
                }
                if self.skipped < self.skip {
                    self.skipped += 1;
                    continue;
                }
                result.append_row(&chunk.get_row(row_idx));
                self.emitted += 1;
            }

            if !result.is_empty() {
                return Ok(Some(result));
            }

            if self.emitted >= self.limit {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::TypedValue;

    fn make_ctx() -> ExecutionContext {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(1)],
                vec![TypedValue::Int64(2)],
                vec![TypedValue::Int64(3)],
                vec![TypedValue::Int64(4)],
                vec![TypedValue::Int64(5)],
            ],
        );
        ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage)
    }

    #[test]
    fn limit_only() {
        let ctx = make_ctx();
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut limit = LimitOp::new(scan, 0, 3);
        let chunk = limit.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 3);
        assert!(limit.next(&ctx).unwrap().is_none());
    }

    #[test]
    fn skip_and_limit() {
        let ctx = make_ctx();
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut limit = LimitOp::new(scan, 2, 2);
        let chunk = limit.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.column(0)[0], TypedValue::Int64(3));
        assert_eq!(chunk.column(0)[1], TypedValue::Int64(4));
    }

    #[test]
    fn skip_all() {
        let ctx = make_ctx();
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut limit = LimitOp::new(scan, 10, 5);
        assert!(limit.next(&ctx).unwrap().is_none());
    }
}
