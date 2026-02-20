//! Scalar evaluator — evaluate a BoundExpression against a single tuple.
//!
//! Used for constant folding during binding, default value evaluation, and
//! validation. NOT the vectorized batch evaluator (that belongs in the executor).

use kyu_common::{KyuError, KyuResult};
use kyu_parser::ast::{BinaryOp, ComparisonOp, StringOp, UnaryOp};
use kyu_types::{LogicalType, TypedValue};

use crate::bound_expr::BoundExpression;

/// Trait for accessing values by column index during expression evaluation.
///
/// Implemented for `[TypedValue]` (backward-compatible slice access) and for
/// `RowRef` in the executor (zero-copy column-major access from DataChunk).
pub trait Tuple {
    fn value_at(&self, idx: usize) -> Option<&TypedValue>;
}

impl Tuple for [TypedValue] {
    #[inline]
    fn value_at(&self, idx: usize) -> Option<&TypedValue> {
        self.get(idx)
    }
}

impl Tuple for Vec<TypedValue> {
    #[inline]
    fn value_at(&self, idx: usize) -> Option<&TypedValue> {
        self.get(idx)
    }
}

/// Evaluate a bound expression against a tuple of values.
///
/// `tuple[i]` corresponds to a variable with `index = i`.
/// Generic over any `Tuple` implementation — `&[TypedValue]` for backward compat,
/// `&RowRef` for zero-copy columnar access.
pub fn evaluate<T: Tuple + ?Sized>(expr: &BoundExpression, tuple: &T) -> KyuResult<TypedValue> {
    match expr {
        BoundExpression::Literal { value, .. } => Ok(value.clone()),

        BoundExpression::Variable { index, .. } => {
            tuple
                .value_at(*index as usize)
                .cloned()
                .ok_or_else(|| KyuError::Runtime(format!("variable index {index} out of range")))
        }

        BoundExpression::Property { object, .. } => {
            // In the scalar evaluator, property access just evaluates the object.
            // Full property resolution happens in the executor with actual storage.
            evaluate(object, tuple)
        }

        BoundExpression::Parameter { index, .. } => {
            tuple
                .value_at(*index as usize)
                .cloned()
                .ok_or_else(|| KyuError::Runtime(format!("parameter index {index} out of range")))
        }

        BoundExpression::UnaryOp { op, operand, .. } => {
            let val = evaluate(operand, tuple)?;
            eval_unary(*op, &val)
        }

        BoundExpression::BinaryOp { op, left, right, .. } => {
            let lv = evaluate(left, tuple)?;
            let rv = evaluate(right, tuple)?;
            eval_binary(*op, &lv, &rv)
        }

        BoundExpression::Comparison { op, left, right } => {
            let lv = evaluate(left, tuple)?;
            let rv = evaluate(right, tuple)?;
            eval_comparison(*op, &lv, &rv)
        }

        BoundExpression::IsNull { expr, negated } => {
            let val = evaluate(expr, tuple)?;
            let is_null = val.is_null();
            Ok(TypedValue::Bool(if *negated { !is_null } else { is_null }))
        }

        BoundExpression::InList { expr, list, negated } => {
            let val = evaluate(expr, tuple)?;
            if val.is_null() {
                return Ok(TypedValue::Null);
            }
            let mut found = false;
            let mut has_null = false;
            for item in list {
                let item_val = evaluate(item, tuple)?;
                if item_val.is_null() {
                    has_null = true;
                    continue;
                }
                if val == item_val {
                    found = true;
                    break;
                }
            }
            if found {
                Ok(TypedValue::Bool(!*negated))
            } else if has_null {
                Ok(TypedValue::Null)
            } else {
                Ok(TypedValue::Bool(*negated))
            }
        }

        BoundExpression::FunctionCall { .. } => {
            // Built-in function evaluation is not implemented in the scalar
            // evaluator. This is handled by the executor's vectorized evaluator.
            Err(KyuError::NotImplemented(
                "function evaluation in scalar evaluator".into(),
            ))
        }

        BoundExpression::CountStar => {
            Err(KyuError::NotImplemented(
                "COUNT(*) in scalar evaluator".into(),
            ))
        }

        BoundExpression::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(op) = operand {
                // Simple CASE: CASE expr WHEN val THEN result ...
                let op_val = evaluate(op, tuple)?;
                for (when_expr, then_expr) in whens {
                    let when_val = evaluate(when_expr, tuple)?;
                    if op_val == when_val {
                        return evaluate(then_expr, tuple);
                    }
                }
            } else {
                // Searched CASE: CASE WHEN cond THEN result ...
                for (when_expr, then_expr) in whens {
                    let cond = evaluate(when_expr, tuple)?;
                    if cond == TypedValue::Bool(true) {
                        return evaluate(then_expr, tuple);
                    }
                }
            }
            if let Some(else_e) = else_expr {
                evaluate(else_e, tuple)
            } else {
                Ok(TypedValue::Null)
            }
        }

        BoundExpression::ListLiteral { elements, .. } => {
            let values: Vec<TypedValue> = elements
                .iter()
                .map(|e| evaluate(e, tuple))
                .collect::<KyuResult<_>>()?;
            Ok(TypedValue::List(values))
        }

        BoundExpression::MapLiteral { entries, .. } => {
            let values: Vec<(TypedValue, TypedValue)> = entries
                .iter()
                .map(|(k, v)| Ok((evaluate(k, tuple)?, evaluate(v, tuple)?)))
                .collect::<KyuResult<_>>()?;
            Ok(TypedValue::Map(values))
        }

        BoundExpression::Subscript { expr, index, .. } => {
            let val = evaluate(expr, tuple)?;
            let idx = evaluate(index, tuple)?;
            match (&val, &idx) {
                (TypedValue::Null, _) | (_, TypedValue::Null) => Ok(TypedValue::Null),
                (TypedValue::List(list), TypedValue::Int64(i)) => {
                    // 1-indexed as per Cypher spec.
                    let i = *i as usize;
                    if i == 0 || i > list.len() {
                        Ok(TypedValue::Null)
                    } else {
                        Ok(list[i - 1].clone())
                    }
                }
                _ => Err(KyuError::Runtime("subscript requires list and integer".into())),
            }
        }

        BoundExpression::Slice { expr, from, to, .. } => {
            let val = evaluate(expr, tuple)?;
            match val {
                TypedValue::Null => Ok(TypedValue::Null),
                TypedValue::List(list) => {
                    let len = list.len() as i64;
                    let start = match from {
                        Some(e) => match evaluate(e, tuple)? {
                            TypedValue::Int64(v) => (v.max(1) - 1) as usize,
                            TypedValue::Null => return Ok(TypedValue::Null),
                            _ => return Err(KyuError::Runtime("slice index must be integer".into())),
                        },
                        None => 0,
                    };
                    let end = match to {
                        Some(e) => match evaluate(e, tuple)? {
                            TypedValue::Int64(v) => v.min(len) as usize,
                            TypedValue::Null => return Ok(TypedValue::Null),
                            _ => return Err(KyuError::Runtime("slice index must be integer".into())),
                        },
                        None => list.len(),
                    };
                    if start >= end || start >= list.len() {
                        Ok(TypedValue::List(Vec::new()))
                    } else {
                        Ok(TypedValue::List(list[start..end].to_vec()))
                    }
                }
                _ => Err(KyuError::Runtime("slice requires list".into())),
            }
        }

        BoundExpression::StringOp { op, left, right } => {
            let lv = evaluate(left, tuple)?;
            let rv = evaluate(right, tuple)?;
            eval_string_op(*op, &lv, &rv)
        }

        BoundExpression::Cast { expr, target_type } => {
            let val = evaluate(expr, tuple)?;
            eval_cast(&val, target_type)
        }

        BoundExpression::HasLabel { .. } => {
            // Label checks require runtime table metadata — not available here.
            Err(KyuError::NotImplemented(
                "HasLabel in scalar evaluator".into(),
            ))
        }
    }
}

/// Evaluate a constant expression (no variable references allowed).
pub fn evaluate_constant(expr: &BoundExpression) -> KyuResult<TypedValue> {
    let empty: &[TypedValue] = &[];
    evaluate(expr, empty)
}

fn eval_unary(op: UnaryOp, val: &TypedValue) -> KyuResult<TypedValue> {
    if val.is_null() {
        return Ok(TypedValue::Null);
    }
    match op {
        UnaryOp::Not => match val {
            TypedValue::Bool(b) => Ok(TypedValue::Bool(!b)),
            _ => Err(KyuError::Runtime("NOT requires boolean".into())),
        },
        UnaryOp::Minus => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int8(-v)),
            TypedValue::Int16(v) => Ok(TypedValue::Int16(-v)),
            TypedValue::Int32(v) => Ok(TypedValue::Int32(-v)),
            TypedValue::Int64(v) => Ok(TypedValue::Int64(-v)),
            TypedValue::Float(v) => Ok(TypedValue::Float(-v)),
            TypedValue::Double(v) => Ok(TypedValue::Double(-v)),
            _ => Err(KyuError::Runtime("unary minus requires numeric".into())),
        },
        UnaryOp::BitwiseNot => match val {
            TypedValue::Int64(v) => Ok(TypedValue::Int64(!v)),
            _ => Err(KyuError::Runtime("bitwise NOT requires integer".into())),
        },
    }
}

fn eval_binary(op: BinaryOp, left: &TypedValue, right: &TypedValue) -> KyuResult<TypedValue> {
    // NULL propagation for arithmetic/bitwise ops.
    if left.is_null() || right.is_null() {
        // Special three-valued logic for boolean operators.
        return match op {
            BinaryOp::And => eval_and(left, right),
            BinaryOp::Or => eval_or(left, right),
            _ => Ok(TypedValue::Null),
        };
    }

    match op {
        BinaryOp::Add => numeric_binop(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_binop(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_binop(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            // Check division by zero.
            match right {
                TypedValue::Int64(0) | TypedValue::Int32(0) => {
                    return Err(KyuError::Runtime("division by zero".into()));
                }
                TypedValue::Double(v) if *v == 0.0 => {
                    return Err(KyuError::Runtime("division by zero".into()));
                }
                _ => {}
            }
            numeric_binop(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOp::Mod => {
            match right {
                TypedValue::Int64(0) | TypedValue::Int32(0) => {
                    return Err(KyuError::Runtime("modulo by zero".into()));
                }
                _ => {}
            }
            numeric_binop(left, right, |a, b| a % b, |a, b| a % b)
        }
        BinaryOp::Pow => {
            match (left, right) {
                (TypedValue::Int64(a), TypedValue::Int64(b)) => {
                    Ok(TypedValue::Double((*a as f64).powf(*b as f64)))
                }
                (TypedValue::Double(a), TypedValue::Double(b)) => {
                    Ok(TypedValue::Double(a.powf(*b)))
                }
                _ => Err(KyuError::Runtime("pow requires numeric".into())),
            }
        }
        BinaryOp::And => eval_and(left, right),
        BinaryOp::Or => eval_or(left, right),
        BinaryOp::Xor => match (left, right) {
            (TypedValue::Bool(a), TypedValue::Bool(b)) => Ok(TypedValue::Bool(a ^ b)),
            _ => Err(KyuError::Runtime("XOR requires boolean".into())),
        },
        BinaryOp::Concat => match (left, right) {
            (TypedValue::String(a), TypedValue::String(b)) => {
                Ok(TypedValue::String(SmolStr::new(format!("{a}{b}"))))
            }
            _ => Err(KyuError::Runtime("concat requires strings".into())),
        },
        BinaryOp::BitwiseAnd => match (left, right) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(TypedValue::Int64(a & b)),
            _ => Err(KyuError::Runtime("bitwise AND requires integers".into())),
        },
        BinaryOp::BitwiseOr => match (left, right) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(TypedValue::Int64(a | b)),
            _ => Err(KyuError::Runtime("bitwise OR requires integers".into())),
        },
        BinaryOp::ShiftLeft => match (left, right) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(TypedValue::Int64(a << b)),
            _ => Err(KyuError::Runtime("shift requires integers".into())),
        },
        BinaryOp::ShiftRight => match (left, right) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(TypedValue::Int64(a >> b)),
            _ => Err(KyuError::Runtime("shift requires integers".into())),
        },
    }
}

use smol_str::SmolStr;

fn eval_and(left: &TypedValue, right: &TypedValue) -> KyuResult<TypedValue> {
    // Three-valued logic: false AND anything = false.
    match (left, right) {
        (TypedValue::Bool(false), _) | (_, TypedValue::Bool(false)) => {
            Ok(TypedValue::Bool(false))
        }
        (TypedValue::Bool(true), TypedValue::Bool(true)) => Ok(TypedValue::Bool(true)),
        _ => Ok(TypedValue::Null), // At least one is NULL, neither is false.
    }
}

fn eval_or(left: &TypedValue, right: &TypedValue) -> KyuResult<TypedValue> {
    // Three-valued logic: true OR anything = true.
    match (left, right) {
        (TypedValue::Bool(true), _) | (_, TypedValue::Bool(true)) => {
            Ok(TypedValue::Bool(true))
        }
        (TypedValue::Bool(false), TypedValue::Bool(false)) => Ok(TypedValue::Bool(false)),
        _ => Ok(TypedValue::Null), // At least one is NULL, neither is true.
    }
}

fn numeric_binop(
    left: &TypedValue,
    right: &TypedValue,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> KyuResult<TypedValue> {
    match (left, right) {
        (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(TypedValue::Int64(int_op(*a, *b))),
        (TypedValue::Int32(a), TypedValue::Int32(b)) => {
            Ok(TypedValue::Int32(int_op(*a as i64, *b as i64) as i32))
        }
        (TypedValue::Double(a), TypedValue::Double(b)) => {
            Ok(TypedValue::Double(float_op(*a, *b)))
        }
        (TypedValue::Float(a), TypedValue::Float(b)) => {
            Ok(TypedValue::Float(float_op(*a as f64, *b as f64) as f32))
        }
        _ => Err(KyuError::Runtime(format!(
            "arithmetic not defined for {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_comparison(
    op: ComparisonOp,
    left: &TypedValue,
    right: &TypedValue,
) -> KyuResult<TypedValue> {
    if left.is_null() || right.is_null() {
        return Ok(TypedValue::Null);
    }

    let ord = compare_values(left, right)?;

    let result = match op {
        ComparisonOp::Eq => ord == std::cmp::Ordering::Equal,
        ComparisonOp::Neq => ord != std::cmp::Ordering::Equal,
        ComparisonOp::Lt => ord == std::cmp::Ordering::Less,
        ComparisonOp::Le => ord != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ord == std::cmp::Ordering::Greater,
        ComparisonOp::Ge => ord != std::cmp::Ordering::Less,
        ComparisonOp::RegexMatch => {
            // Simple regex match fallback — just equality for now.
            // Full regex is an executor concern.
            ord == std::cmp::Ordering::Equal
        }
    };

    Ok(TypedValue::Bool(result))
}

fn compare_values(
    left: &TypedValue,
    right: &TypedValue,
) -> KyuResult<std::cmp::Ordering> {
    match (left, right) {
        (TypedValue::Int64(a), TypedValue::Int64(b)) => Ok(a.cmp(b)),
        (TypedValue::Int32(a), TypedValue::Int32(b)) => Ok(a.cmp(b)),
        (TypedValue::Double(a), TypedValue::Double(b)) => {
            Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        (TypedValue::Float(a), TypedValue::Float(b)) => {
            Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        (TypedValue::String(a), TypedValue::String(b)) => Ok(a.cmp(b)),
        (TypedValue::Bool(a), TypedValue::Bool(b)) => Ok(a.cmp(b)),
        _ => Err(KyuError::Runtime(format!(
            "cannot compare {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_string_op(
    op: StringOp,
    left: &TypedValue,
    right: &TypedValue,
) -> KyuResult<TypedValue> {
    if left.is_null() || right.is_null() {
        return Ok(TypedValue::Null);
    }

    match (left, right) {
        (TypedValue::String(a), TypedValue::String(b)) => {
            let result = match op {
                StringOp::StartsWith => a.starts_with(b.as_str()),
                StringOp::EndsWith => a.ends_with(b.as_str()),
                StringOp::Contains => a.contains(b.as_str()),
            };
            Ok(TypedValue::Bool(result))
        }
        _ => Err(KyuError::Runtime("string operations require strings".into())),
    }
}

fn eval_cast(val: &TypedValue, target: &LogicalType) -> KyuResult<TypedValue> {
    if val.is_null() {
        return Ok(TypedValue::Null);
    }

    match target {
        LogicalType::Int64 => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Int16(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Int32(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Int64(_) => Ok(val.clone()),
            TypedValue::UInt8(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::UInt16(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::UInt32(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Float(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Double(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::String(s) => s
                .parse::<i64>()
                .map(TypedValue::Int64)
                .map_err(|_| KyuError::Runtime(format!("cannot cast '{s}' to INT64"))),
            TypedValue::Bool(b) => Ok(TypedValue::Int64(if *b { 1 } else { 0 })),
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to INT64"
            ))),
        },
        LogicalType::Double => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Int16(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Int32(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Int64(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::UInt8(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::UInt16(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::UInt32(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::UInt64(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Float(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Double(_) => Ok(val.clone()),
            TypedValue::String(s) => s
                .parse::<f64>()
                .map(TypedValue::Double)
                .map_err(|_| KyuError::Runtime(format!("cannot cast '{s}' to DOUBLE"))),
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to DOUBLE"
            ))),
        },
        LogicalType::Float => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int16(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int32(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int64(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Float(_) => Ok(val.clone()),
            TypedValue::Double(v) => Ok(TypedValue::Float(*v as f32)),
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to FLOAT"
            ))),
        },
        LogicalType::String => Ok(TypedValue::String(SmolStr::new(format!("{val}")))),
        LogicalType::Bool => match val {
            TypedValue::Bool(_) => Ok(val.clone()),
            TypedValue::Int64(v) => Ok(TypedValue::Bool(*v != 0)),
            TypedValue::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(TypedValue::Bool(true)),
                "false" => Ok(TypedValue::Bool(false)),
                _ => Err(KyuError::Runtime(format!("cannot cast '{s}' to BOOL"))),
            },
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to BOOL"
            ))),
        },
        LogicalType::Int32 => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int32(*v as i32)),
            TypedValue::Int16(v) => Ok(TypedValue::Int32(*v as i32)),
            TypedValue::Int32(_) => Ok(val.clone()),
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to INT32"
            ))),
        },
        LogicalType::Int16 => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int16(*v as i16)),
            TypedValue::Int16(_) => Ok(val.clone()),
            _ => Err(KyuError::Runtime(format!(
                "cannot cast {val:?} to INT16"
            ))),
        },
        _ => Err(KyuError::Runtime(format!(
            "cast to {} not supported in scalar evaluator",
            target.type_name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_parser::ast::ComparisonOp;

    fn lit(value: TypedValue, result_type: LogicalType) -> BoundExpression {
        BoundExpression::Literal { value, result_type }
    }

    fn lit_int(v: i64) -> BoundExpression {
        lit(TypedValue::Int64(v), LogicalType::Int64)
    }

    fn lit_str(s: &str) -> BoundExpression {
        lit(TypedValue::String(SmolStr::new(s)), LogicalType::String)
    }

    fn lit_bool(v: bool) -> BoundExpression {
        lit(TypedValue::Bool(v), LogicalType::Bool)
    }

    fn lit_null() -> BoundExpression {
        lit(TypedValue::Null, LogicalType::Any)
    }

    #[test]
    fn evaluate_literal() {
        assert_eq!(evaluate_constant(&lit_int(42)).unwrap(), TypedValue::Int64(42));
    }

    #[test]
    fn evaluate_variable() {
        let expr = BoundExpression::Variable {
            index: 1,
            result_type: LogicalType::String,
        };
        let tuple = vec![TypedValue::Int64(1), TypedValue::String(SmolStr::new("hello"))];
        assert_eq!(evaluate(&expr, &tuple).unwrap(), TypedValue::String(SmolStr::new("hello")));
    }

    #[test]
    fn evaluate_add() {
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(lit_int(10)),
            right: Box::new(lit_int(32)),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(42));
    }

    #[test]
    fn evaluate_sub_mul() {
        let sub = BoundExpression::BinaryOp {
            op: BinaryOp::Sub,
            left: Box::new(lit_int(50)),
            right: Box::new(lit_int(8)),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&sub).unwrap(), TypedValue::Int64(42));

        let mul = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(lit_int(6)),
            right: Box::new(lit_int(7)),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&mul).unwrap(), TypedValue::Int64(42));
    }

    #[test]
    fn evaluate_division_by_zero() {
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit_int(10)),
            right: Box::new(lit_int(0)),
            result_type: LogicalType::Int64,
        };
        assert!(evaluate_constant(&expr).is_err());
    }

    #[test]
    fn evaluate_null_propagation() {
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(lit_int(10)),
            right: Box::new(lit_null()),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Null);
    }

    #[test]
    fn evaluate_three_valued_and() {
        // false AND null = false
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(lit_bool(false)),
            right: Box::new(lit_null()),
            result_type: LogicalType::Bool,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(false));

        // true AND null = null
        let expr2 = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(lit_bool(true)),
            right: Box::new(lit_null()),
            result_type: LogicalType::Bool,
        };
        assert_eq!(evaluate_constant(&expr2).unwrap(), TypedValue::Null);
    }

    #[test]
    fn evaluate_three_valued_or() {
        // true OR null = true
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(lit_bool(true)),
            right: Box::new(lit_null()),
            result_type: LogicalType::Bool,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(true));

        // false OR null = null
        let expr2 = BoundExpression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(lit_bool(false)),
            right: Box::new(lit_null()),
            result_type: LogicalType::Bool,
        };
        assert_eq!(evaluate_constant(&expr2).unwrap(), TypedValue::Null);
    }

    #[test]
    fn evaluate_comparison() {
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(lit_int(10)),
            right: Box::new(lit_int(5)),
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(true));

        let expr2 = BoundExpression::Comparison {
            op: ComparisonOp::Eq,
            left: Box::new(lit_str("a")),
            right: Box::new(lit_str("a")),
        };
        assert_eq!(evaluate_constant(&expr2).unwrap(), TypedValue::Bool(true));
    }

    #[test]
    fn evaluate_comparison_null() {
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Eq,
            left: Box::new(lit_int(1)),
            right: Box::new(lit_null()),
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Null);
    }

    #[test]
    fn evaluate_is_null() {
        let expr = BoundExpression::IsNull {
            expr: Box::new(lit_null()),
            negated: false,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(true));

        let expr2 = BoundExpression::IsNull {
            expr: Box::new(lit_int(1)),
            negated: true,
        };
        assert_eq!(evaluate_constant(&expr2).unwrap(), TypedValue::Bool(true));
    }

    #[test]
    fn evaluate_string_ops() {
        let sw = BoundExpression::StringOp {
            op: StringOp::StartsWith,
            left: Box::new(lit_str("hello")),
            right: Box::new(lit_str("hel")),
        };
        assert_eq!(evaluate_constant(&sw).unwrap(), TypedValue::Bool(true));

        let ew = BoundExpression::StringOp {
            op: StringOp::EndsWith,
            left: Box::new(lit_str("hello")),
            right: Box::new(lit_str("lo")),
        };
        assert_eq!(evaluate_constant(&ew).unwrap(), TypedValue::Bool(true));

        let ct = BoundExpression::StringOp {
            op: StringOp::Contains,
            left: Box::new(lit_str("hello")),
            right: Box::new(lit_str("ell")),
        };
        assert_eq!(evaluate_constant(&ct).unwrap(), TypedValue::Bool(true));
    }

    #[test]
    fn evaluate_concat() {
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Concat,
            left: Box::new(lit_str("hello")),
            right: Box::new(lit_str(" world")),
            result_type: LogicalType::String,
        };
        assert_eq!(
            evaluate_constant(&expr).unwrap(),
            TypedValue::String(SmolStr::new("hello world"))
        );
    }

    #[test]
    fn evaluate_not() {
        let expr = BoundExpression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(lit_bool(true)),
            result_type: LogicalType::Bool,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(false));
    }

    #[test]
    fn evaluate_unary_minus() {
        let expr = BoundExpression::UnaryOp {
            op: UnaryOp::Minus,
            operand: Box::new(lit_int(42)),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(-42));
    }

    #[test]
    fn evaluate_case_searched() {
        let expr = BoundExpression::Case {
            operand: None,
            whens: vec![
                (lit_bool(false), lit_int(1)),
                (lit_bool(true), lit_int(2)),
            ],
            else_expr: Some(Box::new(lit_int(3))),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(2));
    }

    #[test]
    fn evaluate_case_else() {
        let expr = BoundExpression::Case {
            operand: None,
            whens: vec![(lit_bool(false), lit_int(1))],
            else_expr: Some(Box::new(lit_int(99))),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(99));
    }

    #[test]
    fn evaluate_list_literal() {
        let expr = BoundExpression::ListLiteral {
            elements: vec![lit_int(1), lit_int(2), lit_int(3)],
            result_type: LogicalType::List(Box::new(LogicalType::Int64)),
        };
        assert_eq!(
            evaluate_constant(&expr).unwrap(),
            TypedValue::List(vec![TypedValue::Int64(1), TypedValue::Int64(2), TypedValue::Int64(3)])
        );
    }

    #[test]
    fn evaluate_subscript() {
        let expr = BoundExpression::Subscript {
            expr: Box::new(BoundExpression::ListLiteral {
                elements: vec![lit_int(10), lit_int(20), lit_int(30)],
                result_type: LogicalType::List(Box::new(LogicalType::Int64)),
            }),
            index: Box::new(lit_int(2)), // 1-indexed
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(20));
    }

    #[test]
    fn evaluate_cast_int_to_double() {
        let expr = BoundExpression::Cast {
            expr: Box::new(lit_int(42)),
            target_type: LogicalType::Double,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Double(42.0));
    }

    #[test]
    fn evaluate_cast_to_string() {
        let expr = BoundExpression::Cast {
            expr: Box::new(lit_int(42)),
            target_type: LogicalType::String,
        };
        assert_eq!(
            evaluate_constant(&expr).unwrap(),
            TypedValue::String(SmolStr::new("42"))
        );
    }

    #[test]
    fn evaluate_in_list() {
        let expr = BoundExpression::InList {
            expr: Box::new(lit_int(2)),
            list: vec![lit_int(1), lit_int(2), lit_int(3)],
            negated: false,
        };
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(true));

        let expr_not = BoundExpression::InList {
            expr: Box::new(lit_int(5)),
            list: vec![lit_int(1), lit_int(2), lit_int(3)],
            negated: false,
        };
        assert_eq!(evaluate_constant(&expr_not).unwrap(), TypedValue::Bool(false));
    }

    #[test]
    fn evaluate_nested_expression() {
        // (1 + 2) * 3 = 9
        let add = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(lit_int(1)),
            right: Box::new(lit_int(2)),
            result_type: LogicalType::Int64,
        };
        let mul = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(add),
            right: Box::new(lit_int(3)),
            result_type: LogicalType::Int64,
        };
        assert_eq!(evaluate_constant(&mul).unwrap(), TypedValue::Int64(9));
    }
}
