//! Bound expression types — resolved, typed expressions ready for planning/execution.
//!
//! Every variant carries its pre-computed `result_type: LogicalType` so type
//! queries are O(1) field access. Variables are referenced by u32 index (not
//! string) for zero string comparison on the hot path.

use kyu_common::id::{PropertyId, TableId};
use kyu_parser::ast::{BinaryOp, ComparisonOp, StringOp, UnaryOp};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

/// Unique function identifier for O(1) registry lookup after resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FunctionId(pub u32);

/// A resolved, typed expression. All names are resolved to IDs,
/// all types are pre-computed. Consumed by the planner and executor.
#[derive(Clone, Debug)]
pub enum BoundExpression {
    Literal {
        value: TypedValue,
        result_type: LogicalType,
    },
    Variable {
        index: u32,
        result_type: LogicalType,
    },
    Property {
        object: Box<BoundExpression>,
        property_id: PropertyId,
        property_name: SmolStr,
        result_type: LogicalType,
    },
    Parameter {
        name: SmolStr,
        index: u32,
        result_type: LogicalType,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<BoundExpression>,
        result_type: LogicalType,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<BoundExpression>,
        right: Box<BoundExpression>,
        result_type: LogicalType,
    },
    Comparison {
        op: ComparisonOp,
        left: Box<BoundExpression>,
        right: Box<BoundExpression>,
    },
    IsNull {
        expr: Box<BoundExpression>,
        negated: bool,
    },
    InList {
        expr: Box<BoundExpression>,
        list: Vec<BoundExpression>,
        negated: bool,
    },
    FunctionCall {
        function_id: FunctionId,
        function_name: SmolStr,
        args: Vec<BoundExpression>,
        distinct: bool,
        result_type: LogicalType,
    },
    CountStar,
    Case {
        operand: Option<Box<BoundExpression>>,
        whens: Vec<(BoundExpression, BoundExpression)>,
        else_expr: Option<Box<BoundExpression>>,
        result_type: LogicalType,
    },
    ListLiteral {
        elements: Vec<BoundExpression>,
        result_type: LogicalType,
    },
    MapLiteral {
        entries: Vec<(BoundExpression, BoundExpression)>,
        result_type: LogicalType,
    },
    Subscript {
        expr: Box<BoundExpression>,
        index: Box<BoundExpression>,
        result_type: LogicalType,
    },
    Slice {
        expr: Box<BoundExpression>,
        from: Option<Box<BoundExpression>>,
        to: Option<Box<BoundExpression>>,
        result_type: LogicalType,
    },
    StringOp {
        op: StringOp,
        left: Box<BoundExpression>,
        right: Box<BoundExpression>,
    },
    Cast {
        expr: Box<BoundExpression>,
        target_type: LogicalType,
    },
    HasLabel {
        expr: Box<BoundExpression>,
        table_ids: Vec<TableId>,
    },
}

impl BoundExpression {
    /// Get the pre-computed result type of this expression.
    pub fn result_type(&self) -> &LogicalType {
        match self {
            Self::Literal { result_type, .. }
            | Self::Variable { result_type, .. }
            | Self::Property { result_type, .. }
            | Self::Parameter { result_type, .. }
            | Self::UnaryOp { result_type, .. }
            | Self::BinaryOp { result_type, .. }
            | Self::FunctionCall { result_type, .. }
            | Self::Case { result_type, .. }
            | Self::ListLiteral { result_type, .. }
            | Self::MapLiteral { result_type, .. }
            | Self::Subscript { result_type, .. }
            | Self::Slice { result_type, .. } => result_type,

            Self::Comparison { .. }
            | Self::IsNull { .. }
            | Self::InList { .. }
            | Self::StringOp { .. }
            | Self::HasLabel { .. } => &LogicalType::Bool,

            Self::CountStar => &LogicalType::Int64,

            Self::Cast { target_type, .. } => target_type,
        }
    }

    /// Whether this expression is a constant (literal or constant-folded).
    pub fn is_constant(&self) -> bool {
        matches!(self, Self::Literal { .. })
    }

    /// Whether this expression is an aggregate function call or COUNT(*).
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::CountStar | Self::FunctionCall { distinct: true, .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit_int(v: i64) -> BoundExpression {
        BoundExpression::Literal {
            value: TypedValue::Int64(v),
            result_type: LogicalType::Int64,
        }
    }

    fn lit_str(s: &str) -> BoundExpression {
        BoundExpression::Literal {
            value: TypedValue::String(SmolStr::new(s)),
            result_type: LogicalType::String,
        }
    }

    fn lit_bool(v: bool) -> BoundExpression {
        BoundExpression::Literal {
            value: TypedValue::Bool(v),
            result_type: LogicalType::Bool,
        }
    }

    #[test]
    fn literal_result_type() {
        assert_eq!(lit_int(42).result_type(), &LogicalType::Int64);
        assert_eq!(lit_str("hello").result_type(), &LogicalType::String);
        assert_eq!(lit_bool(true).result_type(), &LogicalType::Bool);
    }

    #[test]
    fn variable_result_type() {
        let var = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Node,
        };
        assert_eq!(var.result_type(), &LogicalType::Node);
    }

    #[test]
    fn comparison_always_bool() {
        let cmp = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(lit_int(1)),
            right: Box::new(lit_int(2)),
        };
        assert_eq!(cmp.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn is_null_always_bool() {
        let expr = BoundExpression::IsNull {
            expr: Box::new(lit_int(1)),
            negated: false,
        };
        assert_eq!(expr.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn string_op_always_bool() {
        let expr = BoundExpression::StringOp {
            op: StringOp::StartsWith,
            left: Box::new(lit_str("hello")),
            right: Box::new(lit_str("he")),
        };
        assert_eq!(expr.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn count_star_always_int64() {
        assert_eq!(BoundExpression::CountStar.result_type(), &LogicalType::Int64);
    }

    #[test]
    fn cast_result_type() {
        let expr = BoundExpression::Cast {
            expr: Box::new(lit_int(42)),
            target_type: LogicalType::Double,
        };
        assert_eq!(expr.result_type(), &LogicalType::Double);
    }

    #[test]
    fn is_constant() {
        assert!(lit_int(1).is_constant());
        assert!(!BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        }
        .is_constant());
    }

    #[test]
    fn has_label_always_bool() {
        let expr = BoundExpression::HasLabel {
            expr: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Node,
            }),
            table_ids: vec![TableId(1)],
        };
        assert_eq!(expr.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn function_call_result_type() {
        let expr = BoundExpression::FunctionCall {
            function_id: FunctionId(0),
            function_name: SmolStr::new("abs"),
            args: vec![lit_int(-5)],
            distinct: false,
            result_type: LogicalType::Int64,
        };
        assert_eq!(expr.result_type(), &LogicalType::Int64);
    }
}
