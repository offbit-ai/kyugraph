//! Projection operator — evaluates expression list per row.

use kyu_common::KyuResult;
use kyu_expression::{evaluate, BoundExpression};

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct ProjectionOp {
    pub child: Box<PhysicalOperator>,
    pub expressions: Vec<BoundExpression>,
}

impl ProjectionOp {
    pub fn new(child: PhysicalOperator, expressions: Vec<BoundExpression>) -> Self {
        Self {
            child: Box::new(child),
            expressions,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        let chunk = match self.child.next(ctx)? {
            Some(c) => c,
            None => return Ok(None),
        };

        let num_out_cols = self.expressions.len();
        let mut result = DataChunk::with_capacity(num_out_cols, chunk.num_rows());

        for row_idx in 0..chunk.num_rows() {
            let row_ref = chunk.row_ref(row_idx);
            let mut out_row = Vec::with_capacity(num_out_cols);
            for expr in &self.expressions {
                out_row.push(evaluate(expr, &row_ref)?);
            }
            result.append_row(&out_row);
        }

        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_parser::ast::BinaryOp;
    use kyu_types::{LogicalType, TypedValue};

    #[test]
    fn project_literal() {
        let storage = MockStorage::new();
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            &storage,
        );
        let empty = PhysicalOperator::Empty(crate::operators::empty::EmptyOp::new(0));
        let mut proj = ProjectionOp::new(
            empty,
            vec![BoundExpression::Literal {
                value: TypedValue::Int64(42),
                result_type: LogicalType::Int64,
            }],
        );
        let chunk = proj.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 1);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(42));
    }

    #[test]
    fn project_expression() {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(10)],
                vec![TypedValue::Int64(20)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);

        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));

        // Project: variable[0] + 1
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(1),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };

        let mut proj = ProjectionOp::new(scan, vec![expr]);
        let chunk = proj.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(11));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(21));
    }
}
