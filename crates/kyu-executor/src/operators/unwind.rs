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

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let parent_cols = chunk.num_columns();
            let total_cols = parent_cols + 1; // Add one column for the unwound element.
            let mut result = DataChunk::empty(total_cols);

            for row_idx in 0..chunk.num_rows() {
                let row = chunk.get_row(row_idx);
                let list_val = evaluate(&self.expression, &row)?;

                match list_val {
                    TypedValue::List(elements) => {
                        for elem in &elements {
                            let mut new_row = row.clone();
                            new_row.push(elem.clone());
                            result.append_row(&new_row);
                        }
                    }
                    TypedValue::Null => {
                        // UNWIND null produces no rows.
                    }
                    other => {
                        // Single value — treat as single-element list.
                        let mut new_row = row.clone();
                        new_row.push(other);
                        result.append_row(&new_row);
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
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            MockStorage::new(),
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
        assert_eq!(chunk.column(0)[0], TypedValue::Int64(1));
        assert_eq!(chunk.column(0)[1], TypedValue::Int64(2));
        assert_eq!(chunk.column(0)[2], TypedValue::Int64(3));
    }
}
