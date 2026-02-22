//! Per-expression JIT state — tracks evaluation count and manages tiered
//! promotion from interpreted to compiled execution.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::cache::{expr_hash, ExpressionCache};
use super::compiler::{compile_filter, compile_projection, CompiledFilter, CompiledProjection};
use super::is_jit_eligible;
use crate::data_chunk::DataChunk;
use crate::value_vector::{FlatVector, SelectionVector, ValueVector};
use kyu_expression::BoundExpression;
use kyu_storage::NullMask;
use kyu_types::LogicalType;

/// Evaluation strategy — loaded atomically via `ArcSwap`.
pub enum EvalStrategy {
    /// Not yet compiled — fall through to batch_eval / scalar.
    Interpreted,
    /// Compiled filter: predicate → selection vector.
    CompiledFilter(Arc<CompiledFilter>),
    /// Compiled projection: expression → flat value vector.
    CompiledProjection(Arc<CompiledProjection>),
}

/// Whether we compile a filter or projection.
#[derive(Clone, Copy)]
enum CompileMode {
    Filter,
    Projection,
}

/// Per-expression JIT state, tracking evaluation count and managing
/// background compilation.
pub struct JitState {
    strategy: Arc<ArcSwap<EvalStrategy>>,
    eval_count: AtomicU64,
    compiling: AtomicBool,
    threshold: u64,
    hash: u64,
    expr: BoundExpression,
    mode: CompileMode,
}

impl JitState {
    /// Create a new JitState for a filter expression.
    /// Returns `None` if the expression is not JIT-eligible.
    pub fn new_filter(expr: &BoundExpression, threshold: u64) -> Option<Self> {
        if !is_jit_eligible(expr) {
            return None;
        }
        Some(Self {
            strategy: Arc::new(ArcSwap::from_pointee(EvalStrategy::Interpreted)),
            eval_count: AtomicU64::new(0),
            compiling: AtomicBool::new(false),
            threshold,
            hash: expr_hash(expr),
            expr: expr.clone(),
            mode: CompileMode::Filter,
        })
    }

    /// Create a new JitState for a projection expression.
    pub fn new_projection(expr: &BoundExpression, threshold: u64) -> Option<Self> {
        if !is_jit_eligible(expr) {
            return None;
        }
        Some(Self {
            strategy: Arc::new(ArcSwap::from_pointee(EvalStrategy::Interpreted)),
            eval_count: AtomicU64::new(0),
            compiling: AtomicBool::new(false),
            threshold,
            hash: expr_hash(expr),
            expr: expr.clone(),
            mode: CompileMode::Projection,
        })
    }

    /// Record that `n` rows were evaluated. If the threshold is crossed,
    /// trigger background compilation.
    pub fn observe_rows(&self, n: u64, cache: &Arc<ExpressionCache>) {
        let prev = self.eval_count.fetch_add(n, Ordering::Relaxed);
        let total = prev + n;

        if prev < self.threshold && total >= self.threshold {
            self.maybe_compile(cache);
        }
    }

    /// Trigger background compilation if not already in progress.
    fn maybe_compile(&self, cache: &Arc<ExpressionCache>) {
        if self.compiling.swap(true, Ordering::AcqRel) {
            return;
        }

        let hash = self.hash;
        let expr = self.expr.clone();
        let cache = Arc::clone(cache);
        let strategy = Arc::clone(&self.strategy);
        let mode = self.mode;

        rayon::spawn(move || {
            compile_and_install(hash, &expr, &cache, &strategy, mode);
        });
    }

    /// Compile immediately (synchronous). Used when threshold == 0.
    pub fn compile_now(&self, cache: &Arc<ExpressionCache>) {
        compile_and_install(self.hash, &self.expr, cache, &self.strategy, self.mode);
    }

    /// Try to evaluate as a JIT-compiled filter.
    /// Returns `Some(SelectionVector)` if JIT is active, `None` to fall through.
    #[inline]
    pub fn try_eval_filter(&self, chunk: &DataChunk) -> Option<SelectionVector> {
        let guard = self.strategy.load();
        match guard.as_ref() {
            EvalStrategy::CompiledFilter(compiled) => {
                Some(execute_jit_filter(compiled, chunk))
            }
            _ => None,
        }
    }

    /// Try to evaluate as a JIT-compiled projection.
    /// Returns `Some(ValueVector)` if JIT is active, `None` to fall through.
    #[inline]
    pub fn try_eval_projection(&self, chunk: &DataChunk) -> Option<ValueVector> {
        let guard = self.strategy.load();
        match guard.as_ref() {
            EvalStrategy::CompiledProjection(compiled) => {
                Some(execute_jit_projection(compiled, chunk))
            }
            _ => None,
        }
    }
}

/// Compile an expression and install it into the strategy + cache.
fn compile_and_install(
    hash: u64,
    expr: &BoundExpression,
    cache: &ExpressionCache,
    strategy: &ArcSwap<EvalStrategy>,
    mode: CompileMode,
) {
    match mode {
        CompileMode::Filter => {
            if let Some(compiled) = cache.get_filter(hash) {
                strategy.store(Arc::new(EvalStrategy::CompiledFilter(compiled)));
                return;
            }
            match compile_filter(expr) {
                Ok(compiled) => {
                    let arc = Arc::new(compiled);
                    cache.insert_filter(hash, Arc::clone(&arc));
                    strategy.store(Arc::new(EvalStrategy::CompiledFilter(arc)));
                }
                Err(e) => {
                    tracing::warn!("JIT filter compilation failed for hash {hash}: {e}");
                }
            }
        }
        CompileMode::Projection => {
            if let Some(compiled) = cache.get_projection(hash) {
                strategy.store(Arc::new(EvalStrategy::CompiledProjection(compiled)));
                return;
            }
            match compile_projection(expr) {
                Ok(compiled) => {
                    let arc = Arc::new(compiled);
                    cache.insert_projection(hash, Arc::clone(&arc));
                    strategy.store(Arc::new(EvalStrategy::CompiledProjection(arc)));
                }
                Err(e) => {
                    tracing::warn!("JIT projection compilation failed for hash {hash}: {e}");
                }
            }
        }
    }
}

/// Execute a compiled filter on a DataChunk.
fn execute_jit_filter(compiled: &CompiledFilter, chunk: &DataChunk) -> SelectionVector {
    let n = chunk.selection().len();
    if n == 0 {
        return SelectionVector::identity(0);
    }

    let mut col_ptrs = Vec::with_capacity(compiled.col_indices.len());
    let mut null_ptrs = Vec::with_capacity(compiled.col_indices.len());

    for &col_idx in &compiled.col_indices {
        match chunk.column(col_idx as usize) {
            ValueVector::Flat(flat) => {
                col_ptrs.push(flat.data_ptr());
                null_ptrs.push(flat.null_mask().data().as_ptr());
            }
            _ => return SelectionVector::identity(0),
        }
    }

    let sel = chunk.selection();
    let sel_ptr = sel.indices_ptr();
    let mut out = vec![0u32; n];

    let count = unsafe {
        compiled.execute(&col_ptrs, &null_ptrs, sel_ptr, n as u32, &mut out)
    };
    out.truncate(count as usize);
    SelectionVector::from_indices(out)
}

/// Execute a compiled projection on a DataChunk.
fn execute_jit_projection(compiled: &CompiledProjection, chunk: &DataChunk) -> ValueVector {
    let n = chunk.selection().len();
    if n == 0 {
        return ValueVector::Owned(Vec::new());
    }

    let stride = match &compiled.output_type {
        LogicalType::Int64 | LogicalType::Serial | LogicalType::Double => 8,
        LogicalType::Int32 | LogicalType::Float => 4,
        LogicalType::Int16 => 2,
        LogicalType::Int8 | LogicalType::Bool => 1,
        _ => return ValueVector::Owned(Vec::new()),
    };

    let mut col_ptrs = Vec::with_capacity(compiled.col_indices.len());
    let mut null_ptrs = Vec::with_capacity(compiled.col_indices.len());

    for &col_idx in &compiled.col_indices {
        match chunk.column(col_idx as usize) {
            ValueVector::Flat(flat) => {
                col_ptrs.push(flat.data_ptr());
                null_ptrs.push(flat.null_mask().data().as_ptr());
            }
            _ => return ValueVector::Owned(Vec::new()),
        }
    }

    let sel = chunk.selection();
    let sel_ptr = sel.indices_ptr();
    let mut out_data = vec![0u8; n * stride];
    let null_entries = n.div_ceil(64);
    let mut out_nulls = vec![0u64; null_entries];

    unsafe {
        compiled.execute(
            &col_ptrs,
            &null_ptrs,
            sel_ptr,
            n as u32,
            &mut out_data,
            &mut out_nulls,
        );
    }

    let null_mask = NullMask::from_raw(out_nulls, n as u64);
    ValueVector::Flat(FlatVector::from_raw(
        out_data,
        null_mask,
        compiled.output_type.clone(),
        n,
        stride,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value_vector::SelectionVector;
    use kyu_expression::BoundExpression;
    use kyu_parser::ast::{BinaryOp, ComparisonOp};
    use kyu_storage::ColumnChunkData;
    use kyu_types::{LogicalType, TypedValue};

    fn make_i64_chunk(values: &[i64]) -> DataChunk {
        let mut col = ColumnChunkData::new(LogicalType::Int64, values.len() as u64);
        for &v in values {
            col.append_value::<i64>(v);
        }
        let flat = FlatVector::from_column_chunk(&col, values.len());
        DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::identity(values.len()),
        )
    }

    fn gt_expr(threshold: i64) -> BoundExpression {
        BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(threshold),
                result_type: LogicalType::Int64,
            }),
        }
    }

    fn mul_expr(factor: i64) -> BoundExpression {
        BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(factor),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        }
    }

    #[test]
    fn jit_state_filter_immediate() {
        // threshold = 0 → compile immediately.
        let expr = gt_expr(10);
        let cache = Arc::new(ExpressionCache::new(16));
        let state = JitState::new_filter(&expr, 0).unwrap();

        // Before compilation: try_eval_filter returns None.
        let chunk = make_i64_chunk(&[5, 15, 20]);
        assert!(state.try_eval_filter(&chunk).is_none());

        // Compile now.
        state.compile_now(&cache);

        // After compilation: returns selection.
        let sel = state.try_eval_filter(&chunk).unwrap();
        assert_eq!(sel.len(), 2);
        assert_eq!(sel.get(0), 1); // 15
        assert_eq!(sel.get(1), 2); // 20
    }

    #[test]
    fn jit_state_projection_immediate() {
        let expr = mul_expr(3);
        let cache = Arc::new(ExpressionCache::new(16));
        let state = JitState::new_projection(&expr, 0).unwrap();

        state.compile_now(&cache);

        let chunk = make_i64_chunk(&[2, 5, 10]);
        let col = state.try_eval_projection(&chunk).unwrap();
        assert_eq!(col.get_value(0), TypedValue::Int64(6));
        assert_eq!(col.get_value(1), TypedValue::Int64(15));
        assert_eq!(col.get_value(2), TypedValue::Int64(30));
    }

    #[test]
    fn jit_state_threshold_promotion() {
        // threshold = 10 → first chunk (5 rows) stays interpreted,
        // after second chunk (5 rows) crosses threshold.
        let expr = gt_expr(10);
        let cache = Arc::new(ExpressionCache::new(16));
        let state = JitState::new_filter(&expr, 10).unwrap();

        let chunk = make_i64_chunk(&[5, 15, 20, 3, 25]);

        // First batch: 5 rows, threshold not met.
        state.observe_rows(5, &cache);
        // Give rayon a chance to compile in background.
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Should still be interpreted.
        assert!(state.try_eval_filter(&chunk).is_none());

        // Second batch: crosses threshold (10 total).
        state.observe_rows(5, &cache);
        // Wait for background compilation.
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Now should be compiled.
        let sel = state.try_eval_filter(&chunk);
        assert!(sel.is_some());
        let sel = sel.unwrap();
        assert_eq!(sel.len(), 3); // 15, 20, 25
    }

    #[test]
    fn jit_state_not_eligible() {
        // String variable — not JIT-eligible.
        let expr = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::String,
        };
        assert!(JitState::new_filter(&expr, 0).is_none());
    }

    #[test]
    fn cache_sharing_across_states() {
        // Two JitStates for the same expression share cache.
        let expr = gt_expr(42);
        let cache = Arc::new(ExpressionCache::new(16));

        let state1 = JitState::new_filter(&expr, 0).unwrap();
        state1.compile_now(&cache);

        let state2 = JitState::new_filter(&expr, 0).unwrap();
        state2.compile_now(&cache); // Should get cache hit.

        let chunk = make_i64_chunk(&[10, 50, 100]);
        let sel1 = state1.try_eval_filter(&chunk).unwrap();
        let sel2 = state2.try_eval_filter(&chunk).unwrap();
        assert_eq!(sel1.len(), sel2.len());
    }

    #[test]
    fn concurrent_jit_state() {
        // 4 threads sharing a JitState, all observe rows.
        // Verify no panics and all threads eventually see the JIT strategy.
        let expr = gt_expr(10);
        let cache = Arc::new(ExpressionCache::new(16));
        let state = Arc::new(JitState::new_filter(&expr, 100).unwrap());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let state = Arc::clone(&state);
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    for _ in 0..10 {
                        state.observe_rows(10, &cache);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Wait for compilation.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let chunk = make_i64_chunk(&[5, 15, 20, 3, 25]);
        let sel = state.try_eval_filter(&chunk);
        assert!(sel.is_some());
        assert_eq!(sel.unwrap().len(), 3);
    }
}
