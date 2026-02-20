//! kyu-expression: bound expressions, function registry, type coercion, scalar evaluation.

pub mod bound_expr;
pub mod coercion;
pub mod evaluator;
pub mod function_registry;

pub use bound_expr::{BoundExpression, FunctionId};
pub use coercion::{
    coerce_binary_arithmetic, coerce_comparison, coerce_concat, common_type, try_coerce,
};
pub use evaluator::{evaluate, evaluate_constant, Tuple};
pub use function_registry::{FunctionKind, FunctionRegistry, FunctionSignature};
