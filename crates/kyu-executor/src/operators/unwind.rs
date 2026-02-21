//! Unwind operator — flattens a list expression into individual rows.

use kyu_common::KyuResult;
use kyu_expression::{evaluate, BoundExpression};
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct UnwindOp {
    pub child: Box<PhysicalOperator>,
    pub expression: BoundExpression,
}

impl UnwindOp {
    pub fn new(child: PhysicalOperator, expression: BoundExpression) -> Self {
        Self {
            child: Box::new(child),
            expression,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let parent_cols = chunk.num_columns();
            let total_cols = parent_cols + 1; // Add one column for the unwound element.
            let mut result = DataChunk::with_capacity(total_cols, chunk.num_rows() * 2);

            for row_idx in 0..chunk.num_rows() {
                let row_ref = chunk.row_ref(row_idx);
                let list_val = evaluate(&self.expression, &row_ref)?;

                match list_val {
                    TypedValue::List(elements) => {
                        for elem in elements {
                            result.append_row_from_chunk_with_extra(&chunk, row_idx, elem);
                        }
                    }
                    TypedValue::Null => {
                        // UNWIND null produces no rows.
                    }
                    other => {
                        // Single value — treat as single-element list.
                        result.append_row_from_chunk_with_extra(&chunk, row_idx, other);
                    }
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
    use kyu_types::LogicalType;

    #[test]
    fn unwind_list() {
        let storage = MockStorage::new();
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            &storage,
        );

        let empty = PhysicalOperator::Empty(crate::operators::empty::EmptyOp::new(0));
        let expr = BoundExpression::ListLiteral {
            elements: vec![
                BoundExpression::Literal {
                    value: TypedValue::Int64(1),
                    result_type: LogicalType::Int64,
                },
                BoundExpression::Literal {
                    value: TypedValue::Int64(2),
                    result_type: LogicalType::Int64,
                },
                BoundExpression::Literal {
                    value: TypedValue::Int64(3),
                    result_type: LogicalType::Int64,
                },
            ],
            result_type: LogicalType::Any,
        };

        let mut unwind = UnwindOp::new(empty, expr);
        let chunk = unwind.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 3);
        // The unwound element is in column index 0 (parent had 0 cols, so element is col 0).
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(1));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(2));
        assert_eq!(chunk.get_value(2, 0), TypedValue::Int64(3));
    }
}
