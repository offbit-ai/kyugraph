//! Batch expression evaluation — operates on columns/DataChunks instead of
//! per-row TypedValue evaluation. Falls back to `None` for unsupported patterns
//! so callers can use the scalar `evaluate()` path.

use kyu_common::KyuResult;
use kyu_expression::BoundExpression;
use kyu_parser::ast::{BinaryOp, ComparisonOp};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::data_chunk::DataChunk;
use crate::value_vector::{SelectionVector, ValueVector};

// ---------------------------------------------------------------------------
// Filter batch evaluation
// ---------------------------------------------------------------------------

/// Try to evaluate a predicate on a DataChunk in batch mode.
/// Returns `Some(Ok(SelectionVector))` if handled, `None` to fall back to scalar.
pub fn evaluate_filter_batch(
    predicate: &BoundExpression,
    chunk: &DataChunk,
) -> Option<KyuResult<SelectionVector>> {
    match predicate {
        // Pattern: variable <cmp> literal (Int64)
        BoundExpression::Comparison { op, left, right } => {
            match (left.as_ref(), right.as_ref()) {
                (
                    BoundExpression::Variable {
                        index,
                        result_type: LogicalType::Int64 | LogicalType::Serial,
                    },
                    BoundExpression::Literal { value, .. },
                ) => {
                    if let TypedValue::Int64(lit) = value {
                        Some(Ok(filter_cmp_var_lit_i64(
                            chunk, *index as usize, *op, *lit,
                        )))
                    } else {
                        None
                    }
                }
                // Pattern: literal <cmp> variable — flip the comparison
                (
                    BoundExpression::Literal { value, .. },
                    BoundExpression::Variable {
                        index,
                        result_type: LogicalType::Int64 | LogicalType::Serial,
                    },
                ) => {
                    if let TypedValue::Int64(lit) = value {
                        Some(Ok(filter_cmp_var_lit_i64(
                            chunk,
                            *index as usize,
                            flip_cmp(*op),
                            *lit,
                        )))
                    } else {
                        None
                    }
                }
                // Pattern: variable <cmp> literal (String)
                (
                    BoundExpression::Variable { index, result_type },
                    BoundExpression::Literal { value, .. },
                ) if *result_type == LogicalType::String => {
                    if let TypedValue::String(lit) = value {
                        Some(Ok(filter_cmp_var_lit_string(
                            chunk, *index as usize, *op, lit,
                        )))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        // Pattern: expr OR expr
        BoundExpression::BinaryOp {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let left_sel = evaluate_filter_batch(left, chunk)?;
            let right_sel = evaluate_filter_batch(right, chunk)?;
            Some(match (left_sel, right_sel) {
                (Ok(l), Ok(r)) => Ok(union_selections(&l, &r)),
                (Err(e), _) | (_, Err(e)) => Err(e),
            })
        }
        // Pattern: expr AND expr
        BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let left_sel = evaluate_filter_batch(left, chunk)?;
            let right_sel = evaluate_filter_batch(right, chunk)?;
            Some(match (left_sel, right_sel) {
                (Ok(l), Ok(r)) => Ok(intersect_selections(&l, &r)),
                (Err(e), _) | (_, Err(e)) => Err(e),
            })
        }
        // Pattern: variable STARTS WITH literal
        BoundExpression::StringOp {
            op: kyu_parser::ast::StringOp::StartsWith,
            left,
            right,
        } => {
            if let (
                BoundExpression::Variable { index, .. },
                BoundExpression::Literal {
                    value: TypedValue::String(prefix),
                    ..
                },
            ) = (left.as_ref(), right.as_ref())
            {
                Some(Ok(filter_starts_with(chunk, *index as usize, prefix)))
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Column expression evaluation
// ---------------------------------------------------------------------------

/// Try to evaluate an expression on a DataChunk, producing an output ValueVector.
/// Returns `None` for unsupported patterns (caller falls back to scalar).
pub fn evaluate_column(
    expr: &BoundExpression,
    chunk: &DataChunk,
) -> Option<KyuResult<ValueVector>> {
    match expr {
        // Variable reference — pass through the column
        BoundExpression::Variable { index, .. } => {
            Some(Ok(chunk.compact_column(*index as usize)))
        }
        // Literal — broadcast to N copies
        BoundExpression::Literal { value, .. } => {
            let n = chunk.num_rows();
            Some(Ok(ValueVector::Owned(vec![value.clone(); n])))
        }
        // BinaryOp on column data
        BoundExpression::BinaryOp {
            op,
            left,
            right,
            result_type,
        } => eval_binop_column(op, left, right, result_type, chunk),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Filter helpers
// ---------------------------------------------------------------------------

fn filter_cmp_var_lit_i64(
    chunk: &DataChunk,
    col_idx: usize,
    op: ComparisonOp,
    literal: i64,
) -> SelectionVector {
    let sel = chunk.selection();
    let n = sel.len();
    let col = chunk.column(col_idx);

    // Fast path: FlatVector — direct i64 slice access
    if let ValueVector::Flat(flat) = col
        && matches!(flat.logical_type(), LogicalType::Int64 | LogicalType::Serial)
    {
        let data = flat.data_as_i64_slice();
        let mut selected = Vec::with_capacity(n);
        for i in 0..n {
            let phys = sel.get(i);
            if !flat.is_null(phys) && cmp_i64(data[phys], literal, op) {
                selected.push(phys as u32);
            }
        }
        return SelectionVector::from_indices(selected);
    }

    // Fallback: Owned or other vector types
    let mut selected = Vec::with_capacity(n);
    for i in 0..n {
        let phys = sel.get(i);
        if let TypedValue::Int64(v) = col.get_value(phys)
            && cmp_i64(v, literal, op)
        {
            selected.push(phys as u32);
        }
    }
    SelectionVector::from_indices(selected)
}

#[inline]
fn cmp_i64(val: i64, lit: i64, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Lt => val < lit,
        ComparisonOp::Le => val <= lit,
        ComparisonOp::Gt => val > lit,
        ComparisonOp::Ge => val >= lit,
        ComparisonOp::Eq => val == lit,
        ComparisonOp::Neq => val != lit,
        ComparisonOp::RegexMatch => false,
    }
}

fn flip_cmp(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Le => ComparisonOp::Ge,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Ge => ComparisonOp::Le,
        ComparisonOp::Eq => ComparisonOp::Eq,
        ComparisonOp::Neq => ComparisonOp::Neq,
        ComparisonOp::RegexMatch => ComparisonOp::RegexMatch,
    }
}

fn filter_cmp_var_lit_string(
    chunk: &DataChunk,
    col_idx: usize,
    op: ComparisonOp,
    literal: &SmolStr,
) -> SelectionVector {
    let sel = chunk.selection();
    let n = sel.len();
    let col = chunk.column(col_idx);

    // Fast path: StringVector — direct data access
    if let ValueVector::String(sv) = col {
        let data = sv.data();
        let mut selected = Vec::with_capacity(n);
        for i in 0..n {
            let phys = sel.get(i);
            if let Some(ref s) = data[phys] {
                let pass = match op {
                    ComparisonOp::Eq => s == literal,
                    ComparisonOp::Neq => s != literal,
                    ComparisonOp::Lt => s < literal,
                    ComparisonOp::Le => s <= literal,
                    ComparisonOp::Gt => s > literal,
                    ComparisonOp::Ge => s >= literal,
                    ComparisonOp::RegexMatch => false,
                };
                if pass {
                    selected.push(phys as u32);
                }
            }
        }
        return SelectionVector::from_indices(selected);
    }

    // Fallback
    let mut selected = Vec::with_capacity(n);
    for i in 0..n {
        let phys = sel.get(i);
        if let TypedValue::String(ref s) = col.get_value(phys) {
            let pass = match op {
                ComparisonOp::Eq => s == literal,
                ComparisonOp::Neq => s != literal,
                _ => false,
            };
            if pass {
                selected.push(phys as u32);
            }
        }
    }
    SelectionVector::from_indices(selected)
}

fn filter_starts_with(
    chunk: &DataChunk,
    col_idx: usize,
    prefix: &SmolStr,
) -> SelectionVector {
    let sel = chunk.selection();
    let n = sel.len();
    let col = chunk.column(col_idx);

    if let ValueVector::String(sv) = col {
        let data = sv.data();
        let mut selected = Vec::with_capacity(n);
        for i in 0..n {
            let phys = sel.get(i);
            if let Some(ref s) = data[phys]
                && s.starts_with(prefix.as_str())
            {
                selected.push(phys as u32);
            }
        }
        return SelectionVector::from_indices(selected);
    }

    // Fallback
    let mut selected = Vec::with_capacity(n);
    for i in 0..n {
        let phys = sel.get(i);
        if let TypedValue::String(ref s) = col.get_value(phys)
            && s.starts_with(prefix.as_str())
        {
            selected.push(phys as u32);
        }
    }
    SelectionVector::from_indices(selected)
}

fn union_selections(a: &SelectionVector, b: &SelectionVector) -> SelectionVector {
    use std::collections::BTreeSet;
    let mut set = BTreeSet::new();
    for i in 0..a.len() {
        set.insert(a.get(i) as u32);
    }
    for i in 0..b.len() {
        set.insert(b.get(i) as u32);
    }
    SelectionVector::from_indices(set.into_iter().collect())
}

fn intersect_selections(a: &SelectionVector, b: &SelectionVector) -> SelectionVector {
    use std::collections::BTreeSet;
    let set_a: BTreeSet<u32> = (0..a.len()).map(|i| a.get(i) as u32).collect();
    let set_b: BTreeSet<u32> = (0..b.len()).map(|i| b.get(i) as u32).collect();
    let intersection: Vec<u32> = set_a.intersection(&set_b).copied().collect();
    SelectionVector::from_indices(intersection)
}

// ---------------------------------------------------------------------------
// Column expression helpers
// ---------------------------------------------------------------------------

fn eval_binop_column(
    op: &BinaryOp,
    left: &BoundExpression,
    right: &BoundExpression,
    _result_type: &LogicalType,
    chunk: &DataChunk,
) -> Option<KyuResult<ValueVector>> {
    // Pattern: Variable{i64} <op> Literal{i64}
    if let (
        BoundExpression::Variable { index, result_type },
        BoundExpression::Literal {
            value: TypedValue::Int64(lit),
            ..
        },
    ) = (left, right)
        && matches!(result_type, LogicalType::Int64 | LogicalType::Serial)
    {
        return Some(Ok(binop_col_lit_i64(chunk, *index as usize, *op, *lit)));
    }

    // Pattern: Literal{i64} <op> Variable{i64}
    if let (
        BoundExpression::Literal {
            value: TypedValue::Int64(lit),
            ..
        },
        BoundExpression::Variable { index, result_type },
    ) = (left, right)
        && matches!(result_type, LogicalType::Int64 | LogicalType::Serial)
    {
        return Some(Ok(binop_lit_col_i64(*lit, chunk, *index as usize, *op)));
    }

    // Pattern: nested BinaryOp — evaluate children as columns, then combine
    let left_col = evaluate_column(left, chunk)?;
    let right_col = evaluate_column(right, chunk)?;
    Some(match (left_col, right_col) {
        (Ok(lv), Ok(rv)) => Ok(binop_vec_vec_i64(&lv, &rv, *op, chunk.num_rows())),
        (Err(e), _) | (_, Err(e)) => Err(e),
    })
}

fn binop_col_lit_i64(
    chunk: &DataChunk,
    col_idx: usize,
    op: BinaryOp,
    literal: i64,
) -> ValueVector {
    let sel = chunk.selection();
    let n = sel.len();
    let col = chunk.column(col_idx);

    if let ValueVector::Flat(flat) = col
        && matches!(flat.logical_type(), LogicalType::Int64 | LogicalType::Serial)
    {
        let data = flat.data_as_i64_slice();
        let mut result = Vec::with_capacity(n);
        for i in 0..n {
            let phys = sel.get(i);
            if flat.is_null(phys) {
                result.push(TypedValue::Null);
            } else {
                result.push(TypedValue::Int64(apply_i64_op(data[phys], literal, op)));
            }
        }
        return ValueVector::Owned(result);
    }

    // Fallback: Owned
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        let phys = sel.get(i);
        match col.get_value(phys) {
            TypedValue::Int64(v) => {
                result.push(TypedValue::Int64(apply_i64_op(v, literal, op)));
            }
            _ => result.push(TypedValue::Null),
        }
    }
    ValueVector::Owned(result)
}

fn binop_lit_col_i64(
    literal: i64,
    chunk: &DataChunk,
    col_idx: usize,
    op: BinaryOp,
) -> ValueVector {
    let sel = chunk.selection();
    let n = sel.len();
    let col = chunk.column(col_idx);
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        let phys = sel.get(i);
        match col.get_value(phys) {
            TypedValue::Int64(v) => {
                result.push(TypedValue::Int64(apply_i64_op(literal, v, op)));
            }
            _ => result.push(TypedValue::Null),
        }
    }
    ValueVector::Owned(result)
}

fn binop_vec_vec_i64(
    left: &ValueVector,
    right: &ValueVector,
    op: BinaryOp,
    n: usize,
) -> ValueVector {
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        match (left.get_value(i), right.get_value(i)) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => {
                result.push(TypedValue::Int64(apply_i64_op(a, b, op)));
            }
            _ => result.push(TypedValue::Null),
        }
    }
    ValueVector::Owned(result)
}

#[inline]
fn apply_i64_op(a: i64, b: i64, op: BinaryOp) -> i64 {
    match op {
        BinaryOp::Add => a.wrapping_add(b),
        BinaryOp::Sub => a.wrapping_sub(b),
        BinaryOp::Mul => a.wrapping_mul(b),
        BinaryOp::Div => {
            if b == 0 {
                0
            } else {
                a / b
            }
        }
        BinaryOp::Mod => {
            if b == 0 {
                0
            } else {
                a % b
            }
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_storage::ColumnChunkData;
    use kyu_types::LogicalType;

    fn make_i64_chunk(values: &[i64]) -> DataChunk {
        let mut col = ColumnChunkData::new(LogicalType::Int64, values.len() as u64);
        for &v in values {
            col.append_value::<i64>(v);
        }
        let flat = crate::value_vector::FlatVector::from_column_chunk(&col, values.len());
        DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::identity(values.len()),
        )
    }

    #[test]
    fn batch_filter_lt() {
        let chunk = make_i64_chunk(&[10, 20, 5, 30, 3]);
        let pred = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(15),
                result_type: LogicalType::Int64,
            }),
        };
        let sel = evaluate_filter_batch(&pred, &chunk).unwrap().unwrap();
        assert_eq!(sel.len(), 3); // 10, 5, 3
    }

    #[test]
    fn batch_filter_or() {
        let chunk = make_i64_chunk(&[1, 50, 3, 100, 2]);
        let left = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(3),
                result_type: LogicalType::Int64,
            }),
        };
        let right = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(90),
                result_type: LogicalType::Int64,
            }),
        };
        let pred = BoundExpression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(left),
            right: Box::new(right),
            result_type: LogicalType::Bool,
        };
        let sel = evaluate_filter_batch(&pred, &chunk).unwrap().unwrap();
        assert_eq!(sel.len(), 3); // 1, 100, 2
    }

    #[test]
    fn batch_column_variable() {
        let chunk = make_i64_chunk(&[10, 20, 30]);
        let expr = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        };
        let col = evaluate_column(&expr, &chunk).unwrap().unwrap();
        assert_eq!(col.get_value(0), TypedValue::Int64(10));
        assert_eq!(col.get_value(2), TypedValue::Int64(30));
    }

    #[test]
    fn batch_column_mul() {
        let chunk = make_i64_chunk(&[5, 10, 15]);
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(2),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };
        let col = evaluate_column(&expr, &chunk).unwrap().unwrap();
        assert_eq!(col.get_value(0), TypedValue::Int64(10));
        assert_eq!(col.get_value(1), TypedValue::Int64(20));
        assert_eq!(col.get_value(2), TypedValue::Int64(30));
    }

    #[test]
    fn batch_column_nested_mul() {
        // c.length * 2 * 2 * 2 * 2  (benchmark query pattern)
        let chunk = make_i64_chunk(&[1, 3]);
        let var = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        };
        let lit2 = || BoundExpression::Literal {
            value: TypedValue::Int64(2),
            result_type: LogicalType::Int64,
        };
        let mul = |l, r| BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(l),
            right: Box::new(r),
            result_type: LogicalType::Int64,
        };
        let expr = mul(mul(mul(mul(var, lit2()), lit2()), lit2()), lit2());
        let col = evaluate_column(&expr, &chunk).unwrap().unwrap();
        assert_eq!(col.get_value(0), TypedValue::Int64(16));
        assert_eq!(col.get_value(1), TypedValue::Int64(48));
    }
}
