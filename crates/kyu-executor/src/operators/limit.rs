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

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.emitted >= self.limit {
            return Ok(None);
        }

        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let n = chunk.num_rows();

            // Fast skip: entire chunk can be skipped
            let skip_remaining = (self.skip - self.skipped) as usize;
            if skip_remaining >= n {
                self.skipped += n as u64;
                continue;
            }

            let start = skip_remaining;
            self.skipped = self.skip;

            let remaining = (self.limit - self.emitted) as usize;
            let take = remaining.min(n - start);

            if take == 0 {
                return Ok(None);
            }

            // Build selection vector for the slice [start..start+take]
            let sel = chunk.selection();
            let indices: Vec<u32> = (start..start + take)
                .map(|i| sel.get(i) as u32)
                .collect();
            self.emitted += take as u64;

            return Ok(Some(
                chunk.with_selection(crate::value_vector::SelectionVector::from_indices(indices)),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::TypedValue;

    fn make_storage() -> MockStorage {
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
        storage
    }

    #[test]
    fn limit_only() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
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
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut limit = LimitOp::new(scan, 2, 2);
        let chunk = limit.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(3));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(4));
    }

    #[test]
    fn skip_all() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut limit = LimitOp::new(scan, 10, 5);
        assert!(limit.next(&ctx).unwrap().is_none());
    }
}
