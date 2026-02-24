//! Type coercion — implicit cast insertion for expression binding.
//!
//! Wraps `kyu_types::type_utils` functions and inserts `BoundExpression::Cast`
//! nodes when implicit casts are needed.

use kyu_common::{KyuError, KyuResult};
use kyu_types::LogicalType;
use kyu_types::type_utils::{are_comparable, arithmetic_result_type, implicit_cast_cost};

use crate::bound_expr::BoundExpression;

/// Try to coerce an expression to the target type.
///
/// Returns the original expression if types already match, wraps in a `Cast`
/// node if an implicit cast is possible, or returns an error.
pub fn try_coerce(expr: BoundExpression, target: &LogicalType) -> KyuResult<BoundExpression> {
    let from = expr.result_type();
    if from == target {
        return Ok(expr);
    }
    // Any can be cast to anything (e.g., NULL literal).
    if implicit_cast_cost(from, target).is_some() {
        Ok(BoundExpression::Cast {
            expr: Box::new(expr),
            target_type: target.clone(),
        })
    } else {
        Err(KyuError::Binder(format!(
            "cannot implicitly cast {} to {}",
            from.type_name(),
            target.type_name(),
        )))
    }
}

/// Find the common type for a list of types.
///
/// All types must be implicitly castable to the result. Used for CASE
/// branches, UNION columns, and IN list elements.
pub fn common_type(types: &[LogicalType]) -> KyuResult<LogicalType> {
    if types.is_empty() {
        return Ok(LogicalType::Any);
    }

    let mut result = types[0].clone();
    for ty in &types[1..] {
        if *ty == result {
            continue;
        }
        // Try casting left to right, or right to left, pick the cheaper direction.
        let cost_lr = implicit_cast_cost(&result, ty);
        let cost_rl = implicit_cast_cost(ty, &result);

        match (cost_lr, cost_rl) {
            (Some(_), Some(0)) => {
                // result can be cast to ty, but ty is exactly result — keep result.
            }
            (Some(cl), Some(cr)) => {
                if cl <= cr {
                    result = ty.clone();
                }
                // else keep result
            }
            (Some(_), None) => {
                result = ty.clone();
            }
            (None, Some(_)) => {
                // ty can be cast to result — keep result.
            }
            (None, None) => {
                return Err(KyuError::Binder(format!(
                    "incompatible types: {} and {}",
                    result.type_name(),
                    ty.type_name(),
                )));
            }
        }
    }

    Ok(result)
}

/// Determine the result type for a binary arithmetic operation and coerce
/// both operands if needed. Returns (coerced_left, coerced_right, result_type).
pub fn coerce_binary_arithmetic(
    left: BoundExpression,
    right: BoundExpression,
) -> KyuResult<(BoundExpression, BoundExpression, LogicalType)> {
    let lt = left.result_type().clone();
    let rt = right.result_type().clone();

    let result = arithmetic_result_type(&lt, &rt).ok_or_else(|| {
        KyuError::Binder(format!(
            "arithmetic not defined for {} and {}",
            lt.type_name(),
            rt.type_name(),
        ))
    })?;

    let left = try_coerce(left, &result)?;
    let right = try_coerce(right, &result)?;

    Ok((left, right, result))
}

/// Coerce both operands of a comparison to a common comparable type.
/// Returns (coerced_left, coerced_right).
pub fn coerce_comparison(
    left: BoundExpression,
    right: BoundExpression,
) -> KyuResult<(BoundExpression, BoundExpression)> {
    let lt = left.result_type().clone();
    let rt = right.result_type().clone();

    if !are_comparable(&lt, &rt) {
        return Err(KyuError::Binder(format!(
            "cannot compare {} and {}",
            lt.type_name(),
            rt.type_name(),
        )));
    }

    // Find common type and coerce both sides.
    let target = common_type(&[lt, rt])?;
    let left = try_coerce(left, &target)?;
    let right = try_coerce(right, &target)?;

    Ok((left, right))
}

/// Coerce both sides of a string concatenation to String.
pub fn coerce_concat(
    left: BoundExpression,
    right: BoundExpression,
) -> KyuResult<(BoundExpression, BoundExpression)> {
    let left = try_coerce(left, &LogicalType::String)?;
    let right = try_coerce(right, &LogicalType::String)?;
    Ok((left, right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_types::TypedValue;
    use smol_str::SmolStr;

    fn lit(value: TypedValue, result_type: LogicalType) -> BoundExpression {
        BoundExpression::Literal { value, result_type }
    }

    fn lit_int32(v: i32) -> BoundExpression {
        lit(TypedValue::Int32(v), LogicalType::Int32)
    }

    fn lit_int64(v: i64) -> BoundExpression {
        lit(TypedValue::Int64(v), LogicalType::Int64)
    }

    fn lit_double(v: f64) -> BoundExpression {
        lit(TypedValue::Double(v), LogicalType::Double)
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
    fn coerce_same_type_noop() {
        let expr = lit_int64(42);
        let result = try_coerce(expr, &LogicalType::Int64).unwrap();
        assert!(matches!(result, BoundExpression::Literal { .. }));
    }

    #[test]
    fn coerce_int32_to_int64() {
        let expr = lit_int32(42);
        let result = try_coerce(expr, &LogicalType::Int64).unwrap();
        assert!(matches!(
            result,
            BoundExpression::Cast {
                target_type: LogicalType::Int64,
                ..
            }
        ));
    }

    #[test]
    fn coerce_int_to_double() {
        let expr = lit_int64(42);
        let result = try_coerce(expr, &LogicalType::Double).unwrap();
        assert!(matches!(
            result,
            BoundExpression::Cast {
                target_type: LogicalType::Double,
                ..
            }
        ));
    }

    #[test]
    fn coerce_incompatible_error() {
        let expr = lit_bool(true);
        let result = try_coerce(expr, &LogicalType::Int64);
        assert!(result.is_err());
    }

    #[test]
    fn coerce_null_to_any_type() {
        let expr = lit_null();
        let result = try_coerce(expr, &LogicalType::Int64).unwrap();
        assert!(matches!(
            result,
            BoundExpression::Cast {
                target_type: LogicalType::Int64,
                ..
            }
        ));
    }

    #[test]
    fn common_type_same() {
        let result = common_type(&[LogicalType::Int64, LogicalType::Int64]).unwrap();
        assert_eq!(result, LogicalType::Int64);
    }

    #[test]
    fn common_type_widening() {
        let result = common_type(&[LogicalType::Int32, LogicalType::Int64]).unwrap();
        assert_eq!(result, LogicalType::Int64);
    }

    #[test]
    fn common_type_int_float() {
        let result =
            common_type(&[LogicalType::Int32, LogicalType::Int64, LogicalType::Double]).unwrap();
        assert_eq!(result, LogicalType::Double);
    }

    #[test]
    fn common_type_incompatible() {
        let result = common_type(&[LogicalType::Bool, LogicalType::Int64]);
        assert!(result.is_err());
    }

    #[test]
    fn binary_arithmetic_coercion() {
        let (l, r, rt) = coerce_binary_arithmetic(lit_int32(1), lit_int64(2)).unwrap();
        assert_eq!(rt, LogicalType::Int64);
        assert!(matches!(l, BoundExpression::Cast { .. }));
        assert!(matches!(r, BoundExpression::Literal { .. }));
    }

    #[test]
    fn comparison_coercion() {
        let (l, r) = coerce_comparison(lit_int32(1), lit_double(2.0)).unwrap();
        assert_eq!(l.result_type(), &LogicalType::Double);
        assert_eq!(r.result_type(), &LogicalType::Double);
    }

    #[test]
    fn concat_coercion() {
        let (l, r) = coerce_concat(lit_str("a"), lit_int64(1)).unwrap();
        assert_eq!(l.result_type(), &LogicalType::String);
        assert_eq!(r.result_type(), &LogicalType::String);
    }
}
