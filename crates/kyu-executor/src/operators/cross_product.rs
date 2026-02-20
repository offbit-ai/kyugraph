//! Cross product operator — Cartesian product of left × right.

use kyu_common::KyuResult;
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct CrossProductOp {
    pub left: Box<PhysicalOperator>,
    pub right: Box<PhysicalOperator>,
    /// Materialized right-side rows.
    right_rows: Option<Vec<Vec<TypedValue>>>,
}

impl CrossProductOp {
    pub fn new(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            right_rows: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
        // Materialize right side on first call.
        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while let Some(chunk) = self.right.next(ctx)? {
                for row_idx in 0..chunk.num_rows() {
                    rows.push(chunk.get_row(row_idx));
                }
            }
            self.right_rows = Some(rows);
        }

        let right_rows = self.right_rows.as_ref().unwrap();
        if right_rows.is_empty() {
            return Ok(None);
        }

        let left_chunk = match self.left.next(ctx)? {
            Some(c) => c,
            None => return Ok(None),
        };

        let left_ncols = left_chunk.num_columns();
        let right_ncols = right_rows[0].len();
        let total_cols = left_ncols + right_ncols;
        let mut result = DataChunk::with_capacity(total_cols, left_chunk.num_rows() * right_rows.len());

        let mut combined = Vec::with_capacity(total_cols);
        for row_idx in 0..left_chunk.num_rows() {
            for right_row in right_rows {
                combined.clear();
                for col_idx in 0..left_ncols {
                    combined.push(left_chunk.column(col_idx)[row_idx].clone());
                }
                combined.extend_from_slice(right_row);
                result.append_row(&combined);
            }
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::TypedValue;

    #[test]
    fn cross_product_2x2() {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![vec![TypedValue::Int64(1)], vec![TypedValue::Int64(2)]],
        );
        storage.insert_table(
            kyu_common::id::TableId(1),
            vec![vec![TypedValue::Int64(10)], vec![TypedValue::Int64(20)]],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage);

        let left = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let right = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(1),
        ));

        let mut cp = CrossProductOp::new(left, right);
        let chunk = cp.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 4); // 2 × 2
        assert_eq!(chunk.num_columns(), 2);
    }
}
