//! Filter operator — evaluates predicate per row, keeps passing rows.
//!
//! Evaluation cascade:
//! 1. JIT compiled (if available) — native code on flat buffers
//! 2. Batch evaluation — pattern-matched common predicates
//! 3. Scalar fallback — tree-walking `evaluate()` per row

use kyu_common::KyuResult;
use kyu_expression::{BoundExpression, evaluate};
use kyu_types::TypedValue;

use crate::batch_eval::evaluate_filter_batch;
use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;
use crate::value_vector::SelectionVector;

pub struct FilterOp {
    pub child: Box<PhysicalOperator>,
    pub predicate: BoundExpression,
    #[cfg(feature = "jit")]
    jit_state: Option<crate::jit::JitState>,
}

impl FilterOp {
    pub fn new(child: PhysicalOperator, predicate: BoundExpression) -> Self {
        #[cfg(feature = "jit")]
        let jit_state = crate::jit::JitState::new_filter(&predicate, 100_000);

        Self {
            child: Box::new(child),
            predicate,
            #[cfg(feature = "jit")]
            jit_state,
        }
    }

    /// Construct with a custom JIT threshold (for testing).
    #[cfg(feature = "jit")]
    pub fn with_jit_threshold(
        child: PhysicalOperator,
        predicate: BoundExpression,
        threshold: u64,
    ) -> Self {
        let jit_state = crate::jit::JitState::new_filter(&predicate, threshold);
        Self {
            child: Box::new(child),
            predicate,
            jit_state,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        loop {
            let chunk = match self.child.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            // Tier 1: JIT compiled filter
            #[cfg(feature = "jit")]
            if let Some(ref jit) = self.jit_state {
                if let Some(cache) = ctx.jit_cache() {
                    jit.observe_rows(chunk.num_rows() as u64, cache);
                }
                if let Some(sel) = jit.try_eval_filter(&chunk) {
                    if !sel.is_empty() {
                        return Ok(Some(chunk.with_selection(sel)));
                    }
                    continue;
                }
            }

            // Tier 2: Batch evaluation (no per-row TypedValue creation).
            if let Some(result) = evaluate_filter_batch(&self.predicate, &chunk) {
                let sel = result?;
                if !sel.is_empty() {
                    return Ok(Some(chunk.with_selection(sel)));
                }
                continue;
            }

            // Tier 3: Scalar fallback — evaluate predicate per row.
            let mut selected = Vec::with_capacity(chunk.num_rows());
            for row_idx in 0..chunk.num_rows() {
                let val = evaluate(&self.predicate, &chunk.row_ref(row_idx))?;
                if val == TypedValue::Bool(true) {
                    selected.push(chunk.selection().get(row_idx) as u32);
                }
            }

            if !selected.is_empty() {
                return Ok(Some(
                    chunk.with_selection(SelectionVector::from_indices(selected)),
                ));
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
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);

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
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(20));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(30));
    }
}
