//! Projection operator — evaluates expression list per row.
//!
//! Evaluation cascade per expression:
//! 1. JIT compiled (if available) — native code writing to flat buffers
//! 2. Batch column evaluation — pattern-matched common patterns
//! 3. Scalar fallback — tree-walking `evaluate()` per row

use kyu_common::KyuResult;
use kyu_expression::{BoundExpression, evaluate};

use crate::batch_eval::evaluate_column;
use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;
use crate::value_vector::{SelectionVector, ValueVector};

pub struct ProjectionOp {
    pub child: Box<PhysicalOperator>,
    pub expressions: Vec<BoundExpression>,
    #[cfg(feature = "jit")]
    jit_states: Vec<Option<crate::jit::JitState>>,
}

impl ProjectionOp {
    pub fn new(child: PhysicalOperator, expressions: Vec<BoundExpression>) -> Self {
        #[cfg(feature = "jit")]
        let jit_states = expressions
            .iter()
            .map(|e| crate::jit::JitState::new_projection(e, 100_000))
            .collect();

        Self {
            child: Box::new(child),
            expressions,
            #[cfg(feature = "jit")]
            jit_states,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        let mut chunk = match self.child.next(ctx)? {
            Some(c) => c,
            None => return Ok(None),
        };

        let n = chunk.num_rows();
        let is_identity = chunk.selection().is_identity();
        let mut out_columns = Vec::with_capacity(self.expressions.len());

        #[cfg(feature = "jit")]
        let jit_cache = ctx.jit_cache();

        #[allow(unused_variables)]
        for (i, expr) in self.expressions.iter().enumerate() {
            // Fast path: Variable ref with identity selection — move column, no clone.
            if let BoundExpression::Variable { index, .. } = expr
                && is_identity
            {
                out_columns.push(chunk.take_column(*index as usize));
                continue;
            }

            // Tier 1: JIT compiled projection
            #[cfg(feature = "jit")]
            {
                if let Some(ref jit) = self.jit_states[i] {
                    if let Some(cache) = jit_cache {
                        jit.observe_rows(n as u64, cache);
                    }
                    if let Some(col) = jit.try_eval_projection(&chunk) {
                        out_columns.push(col);
                        continue;
                    }
                }
            }

            // Tier 2: Batch column evaluation
            if let Some(result) = evaluate_column(expr, &chunk) {
                out_columns.push(result?);
            } else {
                // Tier 3: Scalar fallback — evaluate per-row, collect into Owned
                let mut col = Vec::with_capacity(n);
                for row_idx in 0..n {
                    col.push(evaluate(expr, &chunk.row_ref(row_idx))?);
                }
                out_columns.push(ValueVector::Owned(col));
            }
        }

        Ok(Some(DataChunk::from_vectors(
            out_columns,
            SelectionVector::identity(n),
        )))
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
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
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
            vec![vec![TypedValue::Int64(10)], vec![TypedValue::Int64(20)]],
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
