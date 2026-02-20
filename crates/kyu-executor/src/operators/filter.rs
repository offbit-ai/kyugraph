//! Filter operator — evaluates predicate per row, keeps passing rows.

use kyu_common::KyuResult;
use kyu_expression::{evaluate, BoundExpression};
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct FilterOp {
    pub child: Box<PhysicalOperator>,
    pub predicate: BoundExpression,
}

impl FilterOp {
    pub fn new(child: PhysicalOperator, predicate: BoundExpression) -> Self {
        Self {
            child: Box::new(child),
            predicate,
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
                let val = evaluate(&self.predicate, &chunk.row_ref(row_idx))?;
                if val == TypedValue::Bool(true) {
                    result.append_row_from_chunk(&chunk, row_idx);
                }
            }

            if !result.is_empty() {
                return Ok(Some(result));
            }
            // If all rows filtered out, pull next chunk from child.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_parser::ast::ComparisonOp;
    use kyu_types::LogicalType;

    #[test]
    fn filter_keeps_matching_rows() {
        // Create a scan that returns rows, then filter.
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(10)],
                vec![TypedValue::Int64(20)],
                vec![TypedValue::Int64(30)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage);

        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));

        // Filter: variable[0] > 15
        let predicate = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(15),
                result_type: LogicalType::Int64,
            }),
        };

        let mut filter = FilterOp::new(scan, predicate);
        let chunk = filter.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.column(0)[0], TypedValue::Int64(20));
        assert_eq!(chunk.column(0)[1], TypedValue::Int64(30));
    }
}
