//! Tiered JIT compilation for hot expressions via Cranelift.
//!
//! Expressions start interpreted (scalar evaluator). After evaluating
//! `jit_threshold` rows, eligible expressions are compiled to native code
//! via Cranelift and swapped in atomically. Non-eligible expressions
//! (strings, CASE, functions, lists) stay at the scalar tier permanently.

mod cache;
mod compiler;
mod state;

pub use cache::{expr_hash, ExpressionCache};
pub use compiler::{compile_filter, compile_projection, CompiledFilter, CompiledProjection, JitError};
pub use state::{EvalStrategy, JitState};

use kyu_expression::BoundExpression;
use kyu_parser::ast::{BinaryOp, ComparisonOp, UnaryOp};
use kyu_types::LogicalType;

/// Check whether an expression tree is fully JIT-compilable.
///
/// Returns `true` only if every node in the tree is in the supported set:
/// numeric literals, variable refs on fixed-size columns, arithmetic,
/// comparisons, boolean logic, unary ops, IsNull, and numeric casts.
pub fn is_jit_eligible(expr: &BoundExpression) -> bool {
    match expr {
        BoundExpression::Literal { result_type, .. } => is_jit_type(result_type),

        BoundExpression::Variable { result_type, .. } => is_jit_type(result_type),

        BoundExpression::UnaryOp { op, operand, result_type } => {
            matches!(op, UnaryOp::Not | UnaryOp::Minus)
                && is_jit_type(result_type)
                && is_jit_eligible(operand)
        }

        BoundExpression::BinaryOp { op, left, right, result_type } => {
            is_jit_binop(op)
                && is_jit_type(result_type)
                && is_jit_eligible(left)
                && is_jit_eligible(right)
        }

        BoundExpression::Comparison { op, left, right } => {
            is_jit_cmpop(op) && is_jit_eligible(left) && is_jit_eligible(right)
        }

        BoundExpression::IsNull { expr, .. } => is_jit_eligible(expr),

        BoundExpression::Cast { expr, target_type } => {
            is_jit_type(target_type) && is_jit_eligible(expr)
        }

        // Everything else stays scalar.
        _ => false,
    }
}

fn is_jit_type(ty: &LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Int64
            | LogicalType::Int32
            | LogicalType::Int16
            | LogicalType::Int8
            | LogicalType::Serial
            | LogicalType::Double
            | LogicalType::Float
            | LogicalType::Bool
    )
}

fn is_jit_binop(op: &BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::BitwiseAnd
            | BinaryOp::BitwiseOr
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight
    )
}

fn is_jit_cmpop(op: &ComparisonOp) -> bool {
    matches!(
        op,
        ComparisonOp::Eq
            | ComparisonOp::Neq
            | ComparisonOp::Lt
            | ComparisonOp::Le
            | ComparisonOp::Gt
            | ComparisonOp::Ge
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_types::TypedValue;
    use smol_str::SmolStr;

    fn lit_i64(v: i64) -> BoundExpression {
        BoundExpression::Literal {
            value: TypedValue::Int64(v),
            result_type: LogicalType::Int64,
        }
    }

    fn var_i64(idx: u32) -> BoundExpression {
        BoundExpression::Variable {
            index: idx,
            result_type: LogicalType::Int64,
        }
    }

    #[test]
    fn simple_comparison_eligible() {
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(42)),
        };
        assert!(is_jit_eligible(&expr));
    }

    #[test]
    fn compound_predicate_eligible() {
        let left = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(10)),
        };
        let right = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(var_i64(1)),
            right: Box::new(lit_i64(20)),
        };
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
            result_type: LogicalType::Bool,
        };
        assert!(is_jit_eligible(&expr));
    }

    #[test]
    fn arithmetic_eligible() {
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(2)),
            result_type: LogicalType::Int64,
        };
        assert!(is_jit_eligible(&expr));
    }

    #[test]
    fn string_not_eligible() {
        let expr = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::String,
        };
        assert!(!is_jit_eligible(&expr));
    }

    #[test]
    fn function_call_not_eligible() {
        let expr = BoundExpression::FunctionCall {
            function_id: kyu_expression::FunctionId(0),
            function_name: SmolStr::new("abs"),
            args: vec![lit_i64(-5)],
            distinct: false,
            result_type: LogicalType::Int64,
        };
        assert!(!is_jit_eligible(&expr));
    }

    #[test]
    fn is_null_eligible() {
        let expr = BoundExpression::IsNull {
            expr: Box::new(var_i64(0)),
            negated: false,
        };
        assert!(is_jit_eligible(&expr));
    }

    #[test]
    fn nested_with_string_not_eligible() {
        // a > 10 AND name = 'foo' — the string part poisons the whole tree.
        let left = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(var_i64(0)),
            right: Box::new(lit_i64(10)),
        };
        let right = BoundExpression::Comparison {
            op: ComparisonOp::Eq,
            left: Box::new(BoundExpression::Variable {
                index: 1,
                result_type: LogicalType::String,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::String(SmolStr::new("foo")),
                result_type: LogicalType::String,
            }),
        };
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
            result_type: LogicalType::Bool,
        };
        assert!(!is_jit_eligible(&expr));
    }
}
