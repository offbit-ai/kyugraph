//! Distinct operator — hash-based deduplication.

use hashbrown::HashSet;
use kyu_common::KyuResult;
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct DistinctOp {
    pub child: Box<PhysicalOperator>,
    seen: HashSet<Vec<TypedValue>>,
}

impl DistinctOp {
    pub fn new(child: PhysicalOperator) -> Self {
        Self {
            child: Box::new(child),
            seen: HashSet::new(),
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let num_cols = chunk.num_columns();
            let mut result = DataChunk::with_capacity(num_cols, chunk.num_rows());

            for row_idx in 0..chunk.num_rows() {
                let row = chunk.get_row(row_idx);
                if self.seen.insert(row) {
                    result.append_row_from_chunk(&chunk, row_idx);
                }
            }

            if !result.is_empty() {
                return Ok(Some(result));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::TypedValue;

    #[test]
    fn distinct_dedup() {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(1)],
                vec![TypedValue::Int64(2)],
                vec![TypedValue::Int64(1)],
                vec![TypedValue::Int64(3)],
                vec![TypedValue::Int64(2)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage);

        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut distinct = DistinctOp::new(scan);
        let chunk = distinct.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 3);
    }
}
