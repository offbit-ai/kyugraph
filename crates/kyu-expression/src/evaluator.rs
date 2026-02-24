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
    fn value_at(&self, idx: usize) -> Option<TypedValue>;
}

impl Tuple for [TypedValue] {
    #[inline]
    fn value_at(&self, idx: usize) -> Option<TypedValue> {
        self.get(idx).cloned()
    }
}

impl Tuple for Vec<TypedValue> {
    #[inline]
    fn value_at(&self, idx: usize) -> Option<TypedValue> {
        self.get(idx).cloned()
    }
}

/// Evaluate a bound expression against a tuple of values.
///
/// `tuple[i]` corresponds to a variable with `index = i`.
/// Generic over any `Tuple` implementation — `&[TypedValue]` for backward compat,
/// `&RowRef` for zero-copy columnar access.
#[inline]
pub fn evaluate<T: Tuple + ?Sized>(expr: &BoundExpression, tuple: &T) -> KyuResult<TypedValue> {
    match expr {
        BoundExpression::Literal { value, .. } => Ok(value.clone()),

        BoundExpression::Variable { index, .. } => tuple
            .value_at(*index as usize)
            .ok_or_else(|| KyuError::Runtime(format!("variable index {index} out of range"))),

        BoundExpression::Property { object, .. } => {
            // In the scalar evaluator, property access just evaluates the object.
            // Full property resolution happens in the executor with actual storage.
            evaluate(object, tuple)
        }

        BoundExpression::Parameter { index, .. } => tuple
            .value_at(*index as usize)
            .ok_or_else(|| KyuError::Runtime(format!("parameter index {index} out of range"))),

        BoundExpression::UnaryOp { op, operand, .. } => {
            let val = evaluate(operand, tuple)?;
            eval_unary(*op, &val)
        }

        BoundExpression::BinaryOp {
            op, left, right, ..
        } => {
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

        BoundExpression::InList {
            expr,
            list,
            negated,
        } => {
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

        BoundExpression::FunctionCall {
            function_name,
            args,
            ..
        } => {
            let evaluated: Vec<TypedValue> = args
                .iter()
                .map(|a| evaluate(a, tuple))
                .collect::<KyuResult<_>>()?;
            eval_scalar_function(function_name, &evaluated)
        }

        BoundExpression::CountStar => Err(KyuError::NotImplemented(
            "COUNT(*) in scalar evaluator".into(),
        )),

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
                _ => Err(KyuError::Runtime(
                    "subscript requires list and integer".into(),
                )),
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
                            _ => {
                                return Err(KyuError::Runtime(
                                    "slice index must be integer".into(),
                                ));
                            }
                        },
                        None => 0,
                    };
                    let end = match to {
                        Some(e) => match evaluate(e, tuple)? {
                            TypedValue::Int64(v) => v.min(len) as usize,
                            TypedValue::Null => return Ok(TypedValue::Null),
                            _ => {
                                return Err(KyuError::Runtime(
                                    "slice index must be integer".into(),
                                ));
                            }
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
        BinaryOp::Pow => match (left, right) {
            (TypedValue::Int64(a), TypedValue::Int64(b)) => {
                Ok(TypedValue::Double((*a as f64).powf(*b as f64)))
            }
            (TypedValue::Double(a), TypedValue::Double(b)) => Ok(TypedValue::Double(a.powf(*b))),
            _ => Err(KyuError::Runtime("pow requires numeric".into())),
        },
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
        (TypedValue::Bool(false), _) | (_, TypedValue::Bool(false)) => Ok(TypedValue::Bool(false)),
        (TypedValue::Bool(true), TypedValue::Bool(true)) => Ok(TypedValue::Bool(true)),
        _ => Ok(TypedValue::Null), // At least one is NULL, neither is false.
    }
}

fn eval_or(left: &TypedValue, right: &TypedValue) -> KyuResult<TypedValue> {
    // Three-valued logic: true OR anything = true.
    match (left, right) {
        (TypedValue::Bool(true), _) | (_, TypedValue::Bool(true)) => Ok(TypedValue::Bool(true)),
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
        (TypedValue::Double(a), TypedValue::Double(b)) => Ok(TypedValue::Double(float_op(*a, *b))),
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

fn compare_values(left: &TypedValue, right: &TypedValue) -> KyuResult<std::cmp::Ordering> {
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

fn eval_string_op(op: StringOp, left: &TypedValue, right: &TypedValue) -> KyuResult<TypedValue> {
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
        _ => Err(KyuError::Runtime(
            "string operations require strings".into(),
        )),
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
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to INT64"))),
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
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to DOUBLE"))),
        },
        LogicalType::Float => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int16(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int32(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Int64(v) => Ok(TypedValue::Float(*v as f32)),
            TypedValue::Float(_) => Ok(val.clone()),
            TypedValue::Double(v) => Ok(TypedValue::Float(*v as f32)),
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to FLOAT"))),
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
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to BOOL"))),
        },
        LogicalType::Int32 => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int32(*v as i32)),
            TypedValue::Int16(v) => Ok(TypedValue::Int32(*v as i32)),
            TypedValue::Int32(_) => Ok(val.clone()),
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to INT32"))),
        },
        LogicalType::Int16 => match val {
            TypedValue::Int8(v) => Ok(TypedValue::Int16(*v as i16)),
            TypedValue::Int16(_) => Ok(val.clone()),
            _ => Err(KyuError::Runtime(format!("cannot cast {val:?} to INT16"))),
        },
        _ => Err(KyuError::Runtime(format!(
            "cast to {} not supported in scalar evaluator",
            target.type_name()
        ))),
    }
}

fn eval_scalar_function(name: &str, args: &[TypedValue]) -> KyuResult<TypedValue> {
    use std::hash::{DefaultHasher, Hash, Hasher};

    // Handle NULL propagation: most functions return NULL if any arg is NULL.
    // Exceptions: coalesce, greatest, least.
    let null_transparent = !matches!(
        name.to_lowercase().as_str(),
        "coalesce" | "greatest" | "least" | "typeof"
    );
    if null_transparent && args.iter().any(|a| a.is_null()) {
        return Ok(TypedValue::Null);
    }

    match name.to_lowercase().as_str() {
        // -- Numeric functions --
        "abs" => match &args[0] {
            TypedValue::Int64(v) => Ok(TypedValue::Int64(v.abs())),
            TypedValue::Double(v) => Ok(TypedValue::Double(v.abs())),
            TypedValue::Int32(v) => Ok(TypedValue::Int32(v.abs())),
            _ => Err(KyuError::Runtime("abs requires numeric".into())),
        },
        "floor" => as_f64(&args[0]).map(|v| TypedValue::Double(v.floor())),
        "ceil" => as_f64(&args[0]).map(|v| TypedValue::Double(v.ceil())),
        "round" => as_f64(&args[0]).map(|v| TypedValue::Double(v.round())),
        "sqrt" => as_f64(&args[0]).map(|v| TypedValue::Double(v.sqrt())),
        "log" => as_f64(&args[0]).map(|v| TypedValue::Double(v.ln())),
        "log2" => as_f64(&args[0]).map(|v| TypedValue::Double(v.log2())),
        "log10" => as_f64(&args[0]).map(|v| TypedValue::Double(v.log10())),
        "sin" => as_f64(&args[0]).map(|v| TypedValue::Double(v.sin())),
        "cos" => as_f64(&args[0]).map(|v| TypedValue::Double(v.cos())),
        "tan" => as_f64(&args[0]).map(|v| TypedValue::Double(v.tan())),
        "sign" => match &args[0] {
            TypedValue::Int64(v) => Ok(TypedValue::Int64(v.signum())),
            TypedValue::Double(v) => Ok(TypedValue::Int64(if *v > 0.0 {
                1
            } else if *v < 0.0 {
                -1
            } else {
                0
            })),
            _ => Err(KyuError::Runtime("sign requires numeric".into())),
        },

        // -- String functions --
        "lower" => as_str(&args[0]).map(|s| TypedValue::String(SmolStr::new(s.to_lowercase()))),
        "upper" => as_str(&args[0]).map(|s| TypedValue::String(SmolStr::new(s.to_uppercase()))),
        "length" | "size" => match &args[0] {
            TypedValue::String(s) => Ok(TypedValue::Int64(s.len() as i64)),
            TypedValue::List(l) => Ok(TypedValue::Int64(l.len() as i64)),
            _ => Err(KyuError::Runtime("length requires string or list".into())),
        },
        "trim" => as_str(&args[0]).map(|s| TypedValue::String(SmolStr::new(s.trim()))),
        "ltrim" => as_str(&args[0]).map(|s| TypedValue::String(SmolStr::new(s.trim_start()))),
        "rtrim" => as_str(&args[0]).map(|s| TypedValue::String(SmolStr::new(s.trim_end()))),
        "reverse" => as_str(&args[0]).map(|s| {
            TypedValue::String(SmolStr::new(
                s.chars().rev().collect::<std::string::String>(),
            ))
        }),
        "substring" => {
            let s = as_str(&args[0])?;
            let start = as_i64(&args[1])? as usize;
            let len = as_i64(&args[2])? as usize;
            let start = if start > 0 { start - 1 } else { 0 }; // 1-indexed
            let result: std::string::String = s.chars().skip(start).take(len).collect();
            Ok(TypedValue::String(SmolStr::new(result)))
        }
        "left" => {
            let s = as_str(&args[0])?;
            let n = as_i64(&args[1])? as usize;
            let result: std::string::String = s.chars().take(n).collect();
            Ok(TypedValue::String(SmolStr::new(result)))
        }
        "right" => {
            let s = as_str(&args[0])?;
            let n = as_i64(&args[1])? as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            let result: std::string::String = chars[start..].iter().collect();
            Ok(TypedValue::String(SmolStr::new(result)))
        }
        "replace" => {
            let s = as_str(&args[0])?;
            let from = as_str(&args[1])?;
            let to = as_str(&args[2])?;
            Ok(TypedValue::String(SmolStr::new(s.replace(from, to))))
        }
        "concat" => {
            let mut result = std::string::String::new();
            for arg in args {
                match arg {
                    TypedValue::String(s) => result.push_str(s),
                    TypedValue::Null => {} // skip nulls in concat
                    other => result.push_str(&format!("{other}")),
                }
            }
            Ok(TypedValue::String(SmolStr::new(result)))
        }
        "lpad" => {
            let s = as_str(&args[0])?;
            let target_len = as_i64(&args[1])? as usize;
            let pad = as_str(&args[2])?;
            let mut result = std::string::String::from(s);
            while result.len() < target_len {
                let remaining = target_len - result.len();
                let take: std::string::String = pad.chars().take(remaining).collect();
                result = format!("{take}{result}");
            }
            Ok(TypedValue::String(SmolStr::new(result)))
        }
        "rpad" => {
            let s = as_str(&args[0])?;
            let target_len = as_i64(&args[1])? as usize;
            let pad = as_str(&args[2])?;
            let mut result = std::string::String::from(s);
            while result.len() < target_len {
                let remaining = target_len - result.len();
                let take: std::string::String = pad.chars().take(remaining).collect();
                result.push_str(&take);
            }
            Ok(TypedValue::String(SmolStr::new(result)))
        }

        // -- Conversion functions --
        "tostring" => Ok(TypedValue::String(SmolStr::new(format!("{}", args[0])))),
        "tointeger" => match &args[0] {
            TypedValue::Int64(_) => Ok(args[0].clone()),
            TypedValue::Int32(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Double(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::Float(v) => Ok(TypedValue::Int64(*v as i64)),
            TypedValue::String(s) => s
                .parse::<i64>()
                .map(TypedValue::Int64)
                .map_err(|_| KyuError::Runtime(format!("cannot convert '{s}' to integer"))),
            TypedValue::Bool(b) => Ok(TypedValue::Int64(if *b { 1 } else { 0 })),
            _ => Err(KyuError::Runtime("tointeger: unsupported type".into())),
        },
        "tofloat" => match &args[0] {
            TypedValue::Double(_) => Ok(args[0].clone()),
            TypedValue::Int64(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Int32(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::Float(v) => Ok(TypedValue::Double(*v as f64)),
            TypedValue::String(s) => s
                .parse::<f64>()
                .map(TypedValue::Double)
                .map_err(|_| KyuError::Runtime(format!("cannot convert '{s}' to float"))),
            _ => Err(KyuError::Runtime("tofloat: unsupported type".into())),
        },
        "toboolean" => match &args[0] {
            TypedValue::Bool(_) => Ok(args[0].clone()),
            TypedValue::Int64(v) => Ok(TypedValue::Bool(*v != 0)),
            TypedValue::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(TypedValue::Bool(true)),
                "false" => Ok(TypedValue::Bool(false)),
                _ => Err(KyuError::Runtime(format!(
                    "cannot convert '{s}' to boolean"
                ))),
            },
            _ => Err(KyuError::Runtime("toboolean: unsupported type".into())),
        },

        // -- Utility functions --
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(TypedValue::Null)
        }
        "typeof" => {
            let type_name = match &args[0] {
                TypedValue::Null => "NULL",
                TypedValue::Bool(_) => "BOOL",
                TypedValue::Int8(_) => "INT8",
                TypedValue::Int16(_) => "INT16",
                TypedValue::Int32(_) => "INT32",
                TypedValue::Int64(_) => "INT64",
                TypedValue::UInt8(_) => "UINT8",
                TypedValue::UInt16(_) => "UINT16",
                TypedValue::UInt32(_) => "UINT32",
                TypedValue::UInt64(_) => "UINT64",
                TypedValue::Float(_) => "FLOAT",
                TypedValue::Double(_) => "DOUBLE",
                TypedValue::String(_) => "STRING",
                TypedValue::List(_) => "LIST",
                TypedValue::Map(_) => "MAP",
                TypedValue::Interval(_) => "INTERVAL",
                TypedValue::InternalId(_) => "INTERNAL_ID",
                _ => "UNKNOWN",
            };
            Ok(TypedValue::String(SmolStr::new(type_name)))
        }
        "hash" => {
            let mut hasher = DefaultHasher::new();
            // Hash the debug representation since TypedValue doesn't impl Hash
            // (contains f32/f64 which don't impl Hash).
            format!("{:?}", args[0]).hash(&mut hasher);
            Ok(TypedValue::Int64(hasher.finish() as i64))
        }
        "greatest" => {
            let mut best: Option<&TypedValue> = None;
            for arg in args {
                if arg.is_null() {
                    continue;
                }
                match best {
                    None => best = Some(arg),
                    Some(b) => {
                        if let Ok(std::cmp::Ordering::Greater) = compare_values(arg, b) {
                            best = Some(arg);
                        }
                    }
                }
            }
            Ok(best.cloned().unwrap_or(TypedValue::Null))
        }
        "least" => {
            let mut best: Option<&TypedValue> = None;
            for arg in args {
                if arg.is_null() {
                    continue;
                }
                match best {
                    None => best = Some(arg),
                    Some(b) => {
                        if let Ok(std::cmp::Ordering::Less) = compare_values(arg, b) {
                            best = Some(arg);
                        }
                    }
                }
            }
            Ok(best.cloned().unwrap_or(TypedValue::Null))
        }

        // -- JSON functions (simd-json accelerated) --
        "json_extract" => {
            let s = as_str(&args[0])?;
            let path = as_str(&args[1])?;
            json_extract(s, path)
        }
        "json_valid" => {
            let s = as_str(&args[0])?;
            let mut bytes = s.as_bytes().to_vec();
            Ok(TypedValue::Bool(
                simd_json::to_borrowed_value(&mut bytes).is_ok(),
            ))
        }
        "json_type" => {
            let s = as_str(&args[0])?;
            json_type_fn(s)
        }
        "json_keys" => {
            let s = as_str(&args[0])?;
            json_keys(s)
        }
        "json_array_length" => {
            let s = as_str(&args[0])?;
            json_array_length(s)
        }
        "json_contains" => {
            let s = as_str(&args[0])?;
            let path = as_str(&args[1])?;
            json_contains(s, path)
        }
        "json_set" => {
            let s = as_str(&args[0])?;
            let path = as_str(&args[1])?;
            let value_str = as_str(&args[2])?;
            json_set(s, path, value_str)
        }

        // -- List functions --
        "range" => {
            let start = as_i64(&args[0])?;
            let end = as_i64(&args[1])?;
            let list: Vec<TypedValue> = (start..=end).map(TypedValue::Int64).collect();
            Ok(TypedValue::List(list))
        }

        _ => Err(KyuError::NotImplemented(format!(
            "function '{name}' not implemented"
        ))),
    }
}

/// Helper: extract f64 from a TypedValue.
fn as_f64(val: &TypedValue) -> KyuResult<f64> {
    match val {
        TypedValue::Int64(v) => Ok(*v as f64),
        TypedValue::Int32(v) => Ok(*v as f64),
        TypedValue::Double(v) => Ok(*v),
        TypedValue::Float(v) => Ok(*v as f64),
        _ => Err(KyuError::Runtime("expected numeric value".into())),
    }
}

/// Helper: extract i64 from a TypedValue.
fn as_i64(val: &TypedValue) -> KyuResult<i64> {
    match val {
        TypedValue::Int64(v) => Ok(*v),
        TypedValue::Int32(v) => Ok(*v as i64),
        _ => Err(KyuError::Runtime("expected integer value".into())),
    }
}

/// Helper: extract &str from a TypedValue.
fn as_str(val: &TypedValue) -> KyuResult<&str> {
    match val {
        TypedValue::String(s) => Ok(s.as_str()),
        _ => Err(KyuError::Runtime("expected string value".into())),
    }
}

// ---- JSON helpers (simd-json) ----

// Import simd-json traits, renaming TypedValue to avoid collision with kyu_types::TypedValue.
use simd_json::prelude::{
    TypedScalarValue, ValueAsArray, ValueAsMutArray, ValueAsMutObject, ValueAsObject,
    ValueAsScalar, ValueObjectAccess, Writable,
};

/// Navigate a dot-delimited path (e.g., "$.address.city" or "address.city") into a
/// simd-json BorrowedValue. Returns None if path not found.
fn json_navigate<'a>(
    val: &'a simd_json::BorrowedValue<'a>,
    path: &str,
) -> Option<&'a simd_json::BorrowedValue<'a>> {
    let path = path
        .strip_prefix("$.")
        .unwrap_or(path.strip_prefix('$').unwrap_or(path));
    if path.is_empty() {
        return Some(val);
    }

    let mut current = val;
    for key in path.split('.') {
        // Try array index: key could be "[0]" or just "0".
        let key = key.trim_matches(|c| c == '[' || c == ']');
        if let Ok(idx) = key.parse::<usize>() {
            current = current.as_array()?.get(idx)?;
        } else {
            current = ValueObjectAccess::get(current, key)?;
        }
    }
    Some(current)
}

/// Convert a simd-json BorrowedValue to a TypedValue.
fn json_value_to_typed(val: &simd_json::BorrowedValue<'_>) -> TypedValue {
    if val.is_null() {
        TypedValue::Null
    } else if let Some(b) = val.as_bool() {
        TypedValue::Bool(b)
    } else if let Some(n) = val.as_i64() {
        TypedValue::Int64(n)
    } else if let Some(n) = val.as_f64() {
        TypedValue::Double(n)
    } else if let Some(s) = val.as_str() {
        TypedValue::String(SmolStr::new(s))
    } else {
        // For objects and arrays, serialize back to JSON string.
        TypedValue::String(SmolStr::new(json_serialize(val)))
    }
}

/// Serialize a simd-json value to a JSON string.
fn json_serialize(val: &impl Writable) -> std::string::String {
    let mut buf = Vec::new();
    let _ = val.write(&mut buf);
    // SAFETY: simd-json always produces valid UTF-8 JSON.
    unsafe { std::string::String::from_utf8_unchecked(buf) }
}

fn json_extract(json_str: &str, path: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let parsed = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    match json_navigate(&parsed, path) {
        Some(val) => Ok(json_value_to_typed(val)),
        None => Ok(TypedValue::Null),
    }
}

fn json_type_fn(json_str: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let parsed = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    let type_name = if parsed.is_null() {
        "null"
    } else if parsed.as_bool().is_some() {
        "boolean"
    } else if parsed.as_i64().is_some() || parsed.as_f64().is_some() {
        "number"
    } else if parsed.as_str().is_some() {
        "string"
    } else if parsed.as_array().is_some() {
        "array"
    } else if parsed.as_object().is_some() {
        "object"
    } else {
        "unknown"
    };
    Ok(TypedValue::String(SmolStr::new(type_name)))
}

fn json_keys(json_str: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let parsed = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    match parsed.as_object() {
        Some(obj) => {
            let keys: Vec<TypedValue> = obj
                .keys()
                .map(|k| TypedValue::String(SmolStr::new(k.as_ref())))
                .collect();
            Ok(TypedValue::List(keys))
        }
        None => Ok(TypedValue::Null),
    }
}

fn json_array_length(json_str: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let parsed = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    match parsed.as_array() {
        Some(arr) => Ok(TypedValue::Int64(arr.len() as i64)),
        None => Ok(TypedValue::Null),
    }
}

fn json_contains(json_str: &str, path: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let parsed = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    Ok(TypedValue::Bool(json_navigate(&parsed, path).is_some()))
}

fn json_set(json_str: &str, path: &str, value_str: &str) -> KyuResult<TypedValue> {
    let mut bytes = json_str.as_bytes().to_vec();
    let mut doc = simd_json::to_owned_value(&mut bytes)
        .map_err(|e| KyuError::Runtime(format!("invalid JSON: {e}")))?;

    // Parse the value to set.
    let mut val_bytes = value_str.as_bytes().to_vec();
    let new_val = simd_json::to_owned_value(&mut val_bytes).unwrap_or_else(|_| {
        // If not valid JSON, treat as string literal.
        simd_json::OwnedValue::from(value_str.to_string())
    });

    let path = path
        .strip_prefix("$.")
        .unwrap_or(path.strip_prefix('$').unwrap_or(path));

    if path.is_empty() {
        return Ok(TypedValue::String(SmolStr::new(json_serialize(&new_val))));
    }

    let keys: Vec<&str> = path.split('.').collect();

    // Navigate to parent, set the last key.
    let mut current = &mut doc;
    for &key in &keys[..keys.len() - 1] {
        let key = key.trim_matches(|c| c == '[' || c == ']');
        if let Ok(idx) = key.parse::<usize>() {
            current = current
                .as_array_mut()
                .and_then(|a| a.get_mut(idx))
                .ok_or_else(|| KyuError::Runtime(format!("path not found: {path}")))?;
        } else {
            current = current
                .as_object_mut()
                .and_then(|o| o.get_mut(key))
                .ok_or_else(|| KyuError::Runtime(format!("path not found: {path}")))?;
        }
    }

    let last_key = keys[keys.len() - 1].trim_matches(|c| c == '[' || c == ']');
    if let Some(obj) = current.as_object_mut() {
        obj.insert(last_key.to_string(), new_val);
    } else if let Ok(idx) = last_key.parse::<usize>() {
        if let Some(arr) = current.as_array_mut() {
            if idx < arr.len() {
                arr[idx] = new_val;
            } else {
                return Err(KyuError::Runtime(format!(
                    "array index {idx} out of bounds"
                )));
            }
        } else {
            return Err(KyuError::Runtime(
                "cannot set on non-object/non-array".into(),
            ));
        }
    } else {
        return Err(KyuError::Runtime("cannot set on non-object".into()));
    }

    Ok(TypedValue::String(SmolStr::new(json_serialize(&doc))))
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
        assert_eq!(
            evaluate_constant(&lit_int(42)).unwrap(),
            TypedValue::Int64(42)
        );
    }

    #[test]
    fn evaluate_variable() {
        let expr = BoundExpression::Variable {
            index: 1,
            result_type: LogicalType::String,
        };
        let tuple = vec![
            TypedValue::Int64(1),
            TypedValue::String(SmolStr::new("hello")),
        ];
        assert_eq!(
            evaluate(&expr, &tuple).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );
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
            whens: vec![(lit_bool(false), lit_int(1)), (lit_bool(true), lit_int(2))],
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
            TypedValue::List(vec![
                TypedValue::Int64(1),
                TypedValue::Int64(2),
                TypedValue::Int64(3)
            ])
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
        assert_eq!(
            evaluate_constant(&expr_not).unwrap(),
            TypedValue::Bool(false)
        );
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

    // ---- Scalar function tests ----

    use crate::bound_expr::FunctionId;

    fn func(name: &str, args: Vec<BoundExpression>, ret: LogicalType) -> BoundExpression {
        BoundExpression::FunctionCall {
            function_id: FunctionId(0),
            function_name: SmolStr::new(name),
            args,
            distinct: false,
            result_type: ret,
        }
    }

    fn lit_f64(v: f64) -> BoundExpression {
        lit(TypedValue::Double(v), LogicalType::Double)
    }

    #[test]
    fn func_abs_int() {
        let expr = func("abs", vec![lit_int(-42)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(42));
    }

    #[test]
    fn func_abs_double() {
        let expr = func("abs", vec![lit_f64(-3.14)], LogicalType::Double);
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Double(3.14));
    }

    #[test]
    fn func_floor_ceil_round() {
        let floor = func("floor", vec![lit_f64(3.7)], LogicalType::Double);
        assert_eq!(evaluate_constant(&floor).unwrap(), TypedValue::Double(3.0));

        let ceil = func("ceil", vec![lit_f64(3.2)], LogicalType::Double);
        assert_eq!(evaluate_constant(&ceil).unwrap(), TypedValue::Double(4.0));

        let round = func("round", vec![lit_f64(3.5)], LogicalType::Double);
        assert_eq!(evaluate_constant(&round).unwrap(), TypedValue::Double(4.0));
    }

    #[test]
    fn func_sqrt() {
        let expr = func("sqrt", vec![lit_f64(9.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Double(3.0));
    }

    #[test]
    fn func_log_family() {
        let ln = func(
            "log",
            vec![lit_f64(std::f64::consts::E)],
            LogicalType::Double,
        );
        let result = evaluate_constant(&ln).unwrap();
        if let TypedValue::Double(v) = result {
            assert!((v - 1.0).abs() < 1e-10);
        }

        let log2 = func("log2", vec![lit_f64(8.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&log2).unwrap(), TypedValue::Double(3.0));

        let log10 = func("log10", vec![lit_f64(100.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&log10).unwrap(), TypedValue::Double(2.0));
    }

    #[test]
    fn func_trig() {
        let sin = func("sin", vec![lit_f64(0.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&sin).unwrap(), TypedValue::Double(0.0));

        let cos = func("cos", vec![lit_f64(0.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&cos).unwrap(), TypedValue::Double(1.0));

        let tan = func("tan", vec![lit_f64(0.0)], LogicalType::Double);
        assert_eq!(evaluate_constant(&tan).unwrap(), TypedValue::Double(0.0));
    }

    #[test]
    fn func_sign() {
        let pos = func("sign", vec![lit_int(42)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&pos).unwrap(), TypedValue::Int64(1));

        let neg = func("sign", vec![lit_int(-5)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&neg).unwrap(), TypedValue::Int64(-1));

        let zero = func("sign", vec![lit_int(0)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&zero).unwrap(), TypedValue::Int64(0));

        let neg_dbl = func("sign", vec![lit_f64(-3.14)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&neg_dbl).unwrap(), TypedValue::Int64(-1));
    }

    #[test]
    fn func_lower_upper() {
        let lower = func("lower", vec![lit_str("Hello")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&lower).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );

        let upper = func("upper", vec![lit_str("Hello")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&upper).unwrap(),
            TypedValue::String(SmolStr::new("HELLO"))
        );
    }

    #[test]
    fn func_length_size() {
        let len_str = func("length", vec![lit_str("hello")], LogicalType::Int64);
        assert_eq!(evaluate_constant(&len_str).unwrap(), TypedValue::Int64(5));

        let size_list = func(
            "size",
            vec![BoundExpression::ListLiteral {
                elements: vec![lit_int(1), lit_int(2), lit_int(3)],
                result_type: LogicalType::List(Box::new(LogicalType::Int64)),
            }],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&size_list).unwrap(), TypedValue::Int64(3));
    }

    #[test]
    fn func_trim() {
        let trim = func("trim", vec![lit_str("  hello  ")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&trim).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );

        let ltrim = func("ltrim", vec![lit_str("  hello")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&ltrim).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );

        let rtrim = func("rtrim", vec![lit_str("hello  ")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&rtrim).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );
    }

    #[test]
    fn func_reverse() {
        let rev = func("reverse", vec![lit_str("hello")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&rev).unwrap(),
            TypedValue::String(SmolStr::new("olleh"))
        );
    }

    #[test]
    fn func_substring() {
        let sub = func(
            "substring",
            vec![lit_str("hello world"), lit_int(1), lit_int(5)],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&sub).unwrap(),
            TypedValue::String(SmolStr::new("hello"))
        );

        let sub2 = func(
            "substring",
            vec![lit_str("hello"), lit_int(3), lit_int(2)],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&sub2).unwrap(),
            TypedValue::String(SmolStr::new("ll"))
        );
    }

    #[test]
    fn func_left_right() {
        let left = func(
            "left",
            vec![lit_str("hello"), lit_int(3)],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&left).unwrap(),
            TypedValue::String(SmolStr::new("hel"))
        );

        let right = func(
            "right",
            vec![lit_str("hello"), lit_int(3)],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&right).unwrap(),
            TypedValue::String(SmolStr::new("llo"))
        );
    }

    #[test]
    fn func_replace() {
        let rep = func(
            "replace",
            vec![lit_str("hello world"), lit_str("world"), lit_str("rust")],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&rep).unwrap(),
            TypedValue::String(SmolStr::new("hello rust"))
        );
    }

    #[test]
    fn func_concat() {
        let cat = func(
            "concat",
            vec![lit_str("hello"), lit_str(" "), lit_str("world")],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&cat).unwrap(),
            TypedValue::String(SmolStr::new("hello world"))
        );
    }

    #[test]
    fn func_lpad_rpad() {
        let lpad = func(
            "lpad",
            vec![lit_str("hi"), lit_int(5), lit_str("x")],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&lpad).unwrap(),
            TypedValue::String(SmolStr::new("xxxhi"))
        );

        let rpad = func(
            "rpad",
            vec![lit_str("hi"), lit_int(5), lit_str("x")],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&rpad).unwrap(),
            TypedValue::String(SmolStr::new("hixxx"))
        );
    }

    #[test]
    fn func_tostring() {
        let ts = func("tostring", vec![lit_int(42)], LogicalType::String);
        assert_eq!(
            evaluate_constant(&ts).unwrap(),
            TypedValue::String(SmolStr::new("42"))
        );
    }

    #[test]
    fn func_tointeger() {
        let ti = func("tointeger", vec![lit_f64(3.7)], LogicalType::Int64);
        assert_eq!(evaluate_constant(&ti).unwrap(), TypedValue::Int64(3));

        let ti2 = func("tointeger", vec![lit_str("42")], LogicalType::Int64);
        assert_eq!(evaluate_constant(&ti2).unwrap(), TypedValue::Int64(42));
    }

    #[test]
    fn func_tofloat() {
        let tf = func("tofloat", vec![lit_int(42)], LogicalType::Double);
        assert_eq!(evaluate_constant(&tf).unwrap(), TypedValue::Double(42.0));

        let tf2 = func("tofloat", vec![lit_str("3.14")], LogicalType::Double);
        assert_eq!(evaluate_constant(&tf2).unwrap(), TypedValue::Double(3.14));
    }

    #[test]
    fn func_toboolean() {
        let tb = func("toboolean", vec![lit_str("true")], LogicalType::Bool);
        assert_eq!(evaluate_constant(&tb).unwrap(), TypedValue::Bool(true));

        let tb2 = func("toboolean", vec![lit_int(0)], LogicalType::Bool);
        assert_eq!(evaluate_constant(&tb2).unwrap(), TypedValue::Bool(false));
    }

    #[test]
    fn func_coalesce() {
        let co = func(
            "coalesce",
            vec![lit_null(), lit_null(), lit_int(42)],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&co).unwrap(), TypedValue::Int64(42));

        let co2 = func("coalesce", vec![lit_null(), lit_null()], LogicalType::Any);
        assert_eq!(evaluate_constant(&co2).unwrap(), TypedValue::Null);
    }

    #[test]
    fn func_typeof() {
        let ty = func("typeof", vec![lit_int(42)], LogicalType::String);
        assert_eq!(
            evaluate_constant(&ty).unwrap(),
            TypedValue::String(SmolStr::new("INT64"))
        );

        let ty2 = func("typeof", vec![lit_str("hello")], LogicalType::String);
        assert_eq!(
            evaluate_constant(&ty2).unwrap(),
            TypedValue::String(SmolStr::new("STRING"))
        );

        let ty3 = func("typeof", vec![lit_null()], LogicalType::String);
        assert_eq!(
            evaluate_constant(&ty3).unwrap(),
            TypedValue::String(SmolStr::new("NULL"))
        );
    }

    #[test]
    fn func_hash() {
        let h1 = func("hash", vec![lit_int(42)], LogicalType::Int64);
        let result = evaluate_constant(&h1).unwrap();
        assert!(matches!(result, TypedValue::Int64(_)));

        // Same input -> same hash.
        let h2 = func("hash", vec![lit_int(42)], LogicalType::Int64);
        assert_eq!(
            evaluate_constant(&h1).unwrap(),
            evaluate_constant(&h2).unwrap()
        );
    }

    #[test]
    fn func_greatest_least() {
        let g = func(
            "greatest",
            vec![lit_int(1), lit_int(5), lit_int(3)],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&g).unwrap(), TypedValue::Int64(5));

        let l = func(
            "least",
            vec![lit_int(1), lit_int(5), lit_int(3)],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&l).unwrap(), TypedValue::Int64(1));

        // With nulls: skip nulls.
        let gn = func(
            "greatest",
            vec![lit_null(), lit_int(3), lit_null()],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&gn).unwrap(), TypedValue::Int64(3));
    }

    #[test]
    fn func_range() {
        let r = func(
            "range",
            vec![lit_int(1), lit_int(4)],
            LogicalType::List(Box::new(LogicalType::Int64)),
        );
        assert_eq!(
            evaluate_constant(&r).unwrap(),
            TypedValue::List(vec![
                TypedValue::Int64(1),
                TypedValue::Int64(2),
                TypedValue::Int64(3),
                TypedValue::Int64(4)
            ])
        );
    }

    #[test]
    fn func_null_propagation() {
        // Most functions return NULL if any arg is NULL.
        let abs_null = func("abs", vec![lit_null()], LogicalType::Int64);
        assert_eq!(evaluate_constant(&abs_null).unwrap(), TypedValue::Null);

        let lower_null = func("lower", vec![lit_null()], LogicalType::String);
        assert_eq!(evaluate_constant(&lower_null).unwrap(), TypedValue::Null);
    }

    // ---- JSON function tests (simd-json) ----

    #[test]
    fn json_extract_nested_path() {
        let expr = func(
            "json_extract",
            vec![
                lit_str(r#"{"address":{"city":"Tokyo","zip":"100"}}"#),
                lit_str("$.address.city"),
            ],
            LogicalType::String,
        );
        assert_eq!(
            evaluate_constant(&expr).unwrap(),
            TypedValue::String(SmolStr::new("Tokyo"))
        );
    }

    #[test]
    fn json_extract_missing_path() {
        let expr = func(
            "json_extract",
            vec![lit_str(r#"{"a":1}"#), lit_str("$.b")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Null);
    }

    #[test]
    fn json_extract_array_index() {
        let expr = func(
            "json_extract",
            vec![lit_str(r#"{"items":[10,20,30]}"#), lit_str("$.items.1")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(20));
    }

    #[test]
    fn json_extract_scalar_values() {
        // Boolean
        let b = func(
            "json_extract",
            vec![lit_str(r#"{"ok":true}"#), lit_str("ok")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&b).unwrap(), TypedValue::Bool(true));

        // Integer
        let i = func(
            "json_extract",
            vec![lit_str(r#"{"n":42}"#), lit_str("n")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&i).unwrap(), TypedValue::Int64(42));

        // Double
        let d = func(
            "json_extract",
            vec![lit_str(r#"{"pi":3.14}"#), lit_str("pi")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&d).unwrap(), TypedValue::Double(3.14));

        // Null value in JSON
        let n = func(
            "json_extract",
            vec![lit_str(r#"{"x":null}"#), lit_str("x")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&n).unwrap(), TypedValue::Null);
    }

    #[test]
    fn json_extract_root() {
        // Extracting "$" returns the whole thing as a string
        let expr = func(
            "json_extract",
            vec![lit_str(r#"{"a":1}"#), lit_str("$")],
            LogicalType::String,
        );
        let result = evaluate_constant(&expr).unwrap();
        // Root object is serialized back to JSON string
        if let TypedValue::String(s) = &result {
            assert!(s.contains("\"a\""));
        } else {
            panic!("expected String, got {result:?}");
        }
    }

    #[test]
    fn json_valid_ok() {
        let valid_obj = func("json_valid", vec![lit_str(r#"{"a":1}"#)], LogicalType::Bool);
        assert_eq!(
            evaluate_constant(&valid_obj).unwrap(),
            TypedValue::Bool(true)
        );

        let valid_arr = func("json_valid", vec![lit_str("[1,2,3]")], LogicalType::Bool);
        assert_eq!(
            evaluate_constant(&valid_arr).unwrap(),
            TypedValue::Bool(true)
        );
    }

    #[test]
    fn json_valid_invalid() {
        let invalid = func("json_valid", vec![lit_str("{not json}")], LogicalType::Bool);
        assert_eq!(
            evaluate_constant(&invalid).unwrap(),
            TypedValue::Bool(false)
        );

        let empty = func("json_valid", vec![lit_str("")], LogicalType::Bool);
        assert_eq!(evaluate_constant(&empty).unwrap(), TypedValue::Bool(false));
    }

    #[test]
    fn json_type_variants() {
        let cases = vec![
            (r#"{"a":1}"#, "object"),
            ("[1,2]", "array"),
            (r#""hello""#, "string"),
            ("42", "number"),
            ("3.14", "number"),
            ("true", "boolean"),
            ("null", "null"),
        ];
        for (input, expected) in cases {
            let expr = func("json_type", vec![lit_str(input)], LogicalType::String);
            assert_eq!(
                evaluate_constant(&expr).unwrap(),
                TypedValue::String(SmolStr::new(expected)),
                "json_type({input}) should be {expected}"
            );
        }
    }

    #[test]
    fn json_keys_object() {
        let expr = func(
            "json_keys",
            vec![lit_str(r#"{"b":2,"a":1}"#)],
            LogicalType::List(Box::new(LogicalType::String)),
        );
        let result = evaluate_constant(&expr).unwrap();
        if let TypedValue::List(keys) = result {
            let key_strs: Vec<&str> = keys
                .iter()
                .map(|k| match k {
                    TypedValue::String(s) => s.as_str(),
                    _ => panic!("expected string key"),
                })
                .collect();
            assert!(key_strs.contains(&"a"));
            assert!(key_strs.contains(&"b"));
            assert_eq!(key_strs.len(), 2);
        } else {
            panic!("expected List, got {result:?}");
        }
    }

    #[test]
    fn json_keys_non_object() {
        let expr = func(
            "json_keys",
            vec![lit_str("[1,2,3]")],
            LogicalType::List(Box::new(LogicalType::String)),
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Null);
    }

    #[test]
    fn json_array_length_ok() {
        let expr = func(
            "json_array_length",
            vec![lit_str("[1,2,3,4,5]")],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Int64(5));
    }

    #[test]
    fn json_array_length_non_array() {
        let expr = func(
            "json_array_length",
            vec![lit_str(r#"{"a":1}"#)],
            LogicalType::Int64,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Null);
    }

    #[test]
    fn json_contains_existing() {
        let expr = func(
            "json_contains",
            vec![lit_str(r#"{"a":{"b":1}}"#), lit_str("$.a.b")],
            LogicalType::Bool,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(true));
    }

    #[test]
    fn json_contains_missing() {
        let expr = func(
            "json_contains",
            vec![lit_str(r#"{"a":1}"#), lit_str("$.x.y")],
            LogicalType::Bool,
        );
        assert_eq!(evaluate_constant(&expr).unwrap(), TypedValue::Bool(false));
    }

    #[test]
    fn json_set_existing_key() {
        let expr = func(
            "json_set",
            vec![lit_str(r#"{"a":1,"b":2}"#), lit_str("$.a"), lit_str("99")],
            LogicalType::String,
        );
        let result = evaluate_constant(&expr).unwrap();
        // Re-extract to verify the set took effect.
        if let TypedValue::String(s) = &result {
            let check = func(
                "json_extract",
                vec![lit_str(s.as_str()), lit_str("a")],
                LogicalType::String,
            );
            assert_eq!(evaluate_constant(&check).unwrap(), TypedValue::Int64(99));
        } else {
            panic!("expected String, got {result:?}");
        }
    }

    #[test]
    fn json_set_nested() {
        let expr = func(
            "json_set",
            vec![lit_str(r#"{"a":{"x":1}}"#), lit_str("$.a.x"), lit_str("42")],
            LogicalType::String,
        );
        let result = evaluate_constant(&expr).unwrap();
        if let TypedValue::String(s) = &result {
            let check = func(
                "json_extract",
                vec![lit_str(s.as_str()), lit_str("a.x")],
                LogicalType::String,
            );
            assert_eq!(evaluate_constant(&check).unwrap(), TypedValue::Int64(42));
        } else {
            panic!("expected String, got {result:?}");
        }
    }

    #[test]
    fn json_null_propagation() {
        // All JSON functions return NULL if arg is NULL.
        let extract_null = func(
            "json_extract",
            vec![lit_null(), lit_str("$.a")],
            LogicalType::String,
        );
        assert_eq!(evaluate_constant(&extract_null).unwrap(), TypedValue::Null);

        let valid_null = func("json_valid", vec![lit_null()], LogicalType::Bool);
        assert_eq!(evaluate_constant(&valid_null).unwrap(), TypedValue::Null);
    }
}
