//! Expression cache — concurrent map of compiled JIT artifacts keyed by
//! structural expression hash. Backed by `DashMap` for lock-free reads.

use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use kyu_expression::BoundExpression;
use kyu_parser::ast::{BinaryOp, ComparisonOp, UnaryOp};
use kyu_types::{LogicalType, TypedValue};

use super::compiler::{CompiledFilter, CompiledProjection};

/// Concurrent cache for compiled JIT artifacts.
///
/// Uses `DashMap` for O(1) concurrent reads. Insertion order is tracked
/// in a separate `Mutex<VecDeque>` for LRU eviction when the cache is full.
pub struct ExpressionCache {
    filters: DashMap<u64, Arc<CompiledFilter>>,
    projections: DashMap<u64, Arc<CompiledProjection>>,
    /// Insertion order for eviction.
    order: Mutex<VecDeque<(u64, bool)>>, // (hash, is_filter)
    capacity: usize,
}

impl ExpressionCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            filters: DashMap::with_capacity(capacity),
            projections: DashMap::with_capacity(capacity),
            order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Look up a cached compiled filter.
    pub fn get_filter(&self, hash: u64) -> Option<Arc<CompiledFilter>> {
        self.filters.get(&hash).map(|r| Arc::clone(r.value()))
    }

    /// Look up a cached compiled projection.
    pub fn get_projection(&self, hash: u64) -> Option<Arc<CompiledProjection>> {
        self.projections.get(&hash).map(|r| Arc::clone(r.value()))
    }

    /// Insert a compiled filter into the cache.
    pub fn insert_filter(&self, hash: u64, compiled: Arc<CompiledFilter>) {
        self.evict_if_full();
        self.filters.insert(hash, compiled);
        if let Ok(mut order) = self.order.lock() {
            order.push_back((hash, true));
        }
    }

    /// Insert a compiled projection into the cache.
    pub fn insert_projection(&self, hash: u64, compiled: Arc<CompiledProjection>) {
        self.evict_if_full();
        self.projections.insert(hash, compiled);
        if let Ok(mut order) = self.order.lock() {
            order.push_back((hash, false));
        }
    }

    fn evict_if_full(&self) {
        let total = self.filters.len() + self.projections.len();
        if total < self.capacity {
            return;
        }
        if let Ok(mut order) = self.order.lock() {
            // Evict oldest entries until under capacity.
            while self.filters.len() + self.projections.len() >= self.capacity {
                if let Some((hash, is_filter)) = order.pop_front() {
                    if is_filter {
                        self.filters.remove(&hash);
                    } else {
                        self.projections.remove(&hash);
                    }
                } else {
                    break;
                }
            }
        }
    }
}

/// Compute a structural hash of a `BoundExpression` tree.
///
/// Two expressions that are structurally identical will hash the same,
/// enabling cache lookups across different plan instances.
pub fn expr_hash(expr: &BoundExpression) -> u64 {
    let mut hasher = DefaultHasher::new();
    hash_expr(expr, &mut hasher);
    hasher.finish()
}

fn hash_expr(expr: &BoundExpression, h: &mut DefaultHasher) {
    // Hash the variant discriminant first.
    std::mem::discriminant(expr).hash(h);

    match expr {
        BoundExpression::Literal { value, result_type } => {
            hash_typed_value(value, h);
            hash_logical_type(result_type, h);
        }
        BoundExpression::Variable { index, result_type } => {
            index.hash(h);
            hash_logical_type(result_type, h);
        }
        BoundExpression::UnaryOp {
            op,
            operand,
            result_type,
        } => {
            hash_unary_op(op, h);
            hash_expr(operand, h);
            hash_logical_type(result_type, h);
        }
        BoundExpression::BinaryOp {
            op,
            left,
            right,
            result_type,
        } => {
            hash_binary_op(op, h);
            hash_expr(left, h);
            hash_expr(right, h);
            hash_logical_type(result_type, h);
        }
        BoundExpression::Comparison { op, left, right } => {
            hash_cmp_op(op, h);
            hash_expr(left, h);
            hash_expr(right, h);
        }
        BoundExpression::IsNull { expr, negated } => {
            hash_expr(expr, h);
            negated.hash(h);
        }
        BoundExpression::Cast { expr, target_type } => {
            hash_expr(expr, h);
            hash_logical_type(target_type, h);
        }
        // Non-JIT-eligible expressions — hash enough to be unique.
        _ => {
            // Use Debug representation as a fallback for non-JIT types.
            format!("{expr:?}").hash(h);
        }
    }
}

fn hash_typed_value(v: &TypedValue, h: &mut DefaultHasher) {
    std::mem::discriminant(v).hash(h);
    match v {
        TypedValue::Null => {}
        TypedValue::Bool(b) => b.hash(h),
        TypedValue::Int8(n) => n.hash(h),
        TypedValue::Int16(n) => n.hash(h),
        TypedValue::Int32(n) => n.hash(h),
        TypedValue::Int64(n) => n.hash(h),
        TypedValue::Float(f) => f.to_bits().hash(h),
        TypedValue::Double(f) => f.to_bits().hash(h),
        TypedValue::String(s) => s.hash(h),
        _ => format!("{v:?}").hash(h),
    }
}

fn hash_logical_type(ty: &LogicalType, h: &mut DefaultHasher) {
    std::mem::discriminant(ty).hash(h);
}

fn hash_binary_op(op: &BinaryOp, h: &mut DefaultHasher) {
    std::mem::discriminant(op).hash(h);
}

fn hash_unary_op(op: &UnaryOp, h: &mut DefaultHasher) {
    std::mem::discriminant(op).hash(h);
}

fn hash_cmp_op(op: &ComparisonOp, h: &mut DefaultHasher) {
    std::mem::discriminant(op).hash(h);
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_types::{LogicalType, TypedValue};

    fn var_i64(idx: u32) -> BoundExpression {
        BoundExpression::Variable {
            index: idx,
            result_type: LogicalType::Int64,
        }
    }

    fn lit_i64(v: i64) -> BoundExpression {
        BoundExpression::Literal {
            value: TypedValue::Int64(v),
            result_type: LogicalType::Int64,
        }
    }

    #[test]
    fn same_expression_same_hash() {
        let e1 = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        let e2 = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        assert_eq!(expr_hash(&e1), expr_hash(&e2));
    }

    #[test]
    fn different_literal_different_hash() {
        let e1 = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        let e2 = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(99)),
        };
        assert_ne!(expr_hash(&e1), expr_hash(&e2));
    }

    #[test]
    fn different_op_different_hash() {
        let e1 = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        let e2 = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        assert_ne!(expr_hash(&e1), expr_hash(&e2));
    }

    #[test]
    fn cache_insert_and_get() {
        let cache = ExpressionCache::new(16);
        assert!(cache.get_filter(123).is_none());
        // We can't easily construct a CompiledFilter in tests without Cranelift,
        // but we can verify the DashMap mechanics.
        assert_eq!(cache.filters.len(), 0);
    }
}
