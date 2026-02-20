//! Expression binder — resolves parser AST expressions to bound expressions.
//!
//! Transforms `kyu_parser::ast::Expression` → `BoundExpression` using scope
//! and catalog for name resolution and type inference.

use kyu_catalog::CatalogContent;
use kyu_common::{KyuError, KyuResult};
use kyu_expression::bound_expr::BoundExpression;
use kyu_expression::{
    coerce_binary_arithmetic, coerce_comparison, coerce_concat, common_type, try_coerce,
    FunctionRegistry,
};
use kyu_parser::ast::{
    BinaryOp, ComparisonOp, Expression, Literal,
};
use kyu_parser::span::Spanned;
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::scope::BinderScope;

/// Bind a parser expression to a resolved BoundExpression.
pub fn bind_expression(
    expr: &Spanned<Expression>,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    match &expr.0 {
        Expression::Literal(lit) => bind_literal(lit),

        Expression::Variable(name) => bind_variable(name, scope),

        Expression::Parameter(name) => Ok(BoundExpression::Parameter {
            name: name.clone(),
            index: 0, // Parameters are resolved at execution time.
            result_type: LogicalType::Any,
        }),

        Expression::Property { object, key } => {
            bind_property(object, key, scope, catalog, registry)
        }

        Expression::FunctionCall {
            name,
            distinct,
            args,
        } => bind_function_call(name, *distinct, args, scope, catalog, registry),

        Expression::CountStar => Ok(BoundExpression::CountStar),

        Expression::UnaryOp { op, operand } => {
            bind_unary_op(*op, operand, scope, catalog, registry)
        }

        Expression::BinaryOp { left, op, right } => {
            bind_binary_op(*op, left, right, scope, catalog, registry)
        }

        Expression::Comparison { left, ops } => {
            bind_comparison(left, ops, scope, catalog, registry)
        }

        Expression::IsNull { expr: inner, negated } => {
            let bound = bind_expression(inner, scope, catalog, registry)?;
            Ok(BoundExpression::IsNull {
                expr: Box::new(bound),
                negated: *negated,
            })
        }

        Expression::InList {
            expr: inner,
            list,
            negated,
        } => bind_in_list(inner, list, *negated, scope, catalog, registry),

        Expression::ListLiteral(elements) => {
            bind_list_literal(elements, scope, catalog, registry)
        }

        Expression::MapLiteral(entries) => {
            bind_map_literal(entries, scope, catalog, registry)
        }

        Expression::Subscript { expr: inner, index } => {
            let bound_expr = bind_expression(inner, scope, catalog, registry)?;
            let bound_index = bind_expression(index, scope, catalog, registry)?;
            let result_type = match bound_expr.result_type() {
                LogicalType::List(elem) => *elem.clone(),
                _ => LogicalType::Any,
            };
            Ok(BoundExpression::Subscript {
                expr: Box::new(bound_expr),
                index: Box::new(bound_index),
                result_type,
            })
        }

        Expression::Slice {
            expr: inner,
            from,
            to,
        } => {
            let bound_expr = bind_expression(inner, scope, catalog, registry)?;
            let bound_from = from
                .as_ref()
                .map(|e| bind_expression(e, scope, catalog, registry))
                .transpose()?
                .map(Box::new);
            let bound_to = to
                .as_ref()
                .map(|e| bind_expression(e, scope, catalog, registry))
                .transpose()?
                .map(Box::new);
            let result_type = bound_expr.result_type().clone();
            Ok(BoundExpression::Slice {
                expr: Box::new(bound_expr),
                from: bound_from,
                to: bound_to,
                result_type,
            })
        }

        Expression::Case {
            operand,
            whens,
            else_expr,
        } => bind_case(operand, whens, else_expr, scope, catalog, registry),

        Expression::StringOp { left, op, right } => {
            let bound_left = bind_expression(left, scope, catalog, registry)?;
            let bound_right = bind_expression(right, scope, catalog, registry)?;
            let bound_left = try_coerce(bound_left, &LogicalType::String)?;
            let bound_right = try_coerce(bound_right, &LogicalType::String)?;
            Ok(BoundExpression::StringOp {
                op: *op,
                left: Box::new(bound_left),
                right: Box::new(bound_right),
            })
        }

        Expression::HasLabel { expr: inner, labels } => {
            let bound = bind_expression(inner, scope, catalog, registry)?;
            let mut table_ids = Vec::with_capacity(labels.len());
            for label in labels {
                let entry = catalog.find_by_name(&label.0).ok_or_else(|| {
                    KyuError::Binder(format!("label '{}' not found", label.0))
                })?;
                table_ids.push(entry.table_id());
            }
            Ok(BoundExpression::HasLabel {
                expr: Box::new(bound),
                table_ids,
            })
        }

        Expression::ExistsSubquery(_)
        | Expression::CountSubquery(_)
        | Expression::Quantifier { .. }
        | Expression::ListComprehension { .. } => Err(KyuError::NotImplemented(
            "subqueries and quantifiers not yet supported in binder".into(),
        )),
    }
}

fn bind_literal(lit: &Literal) -> KyuResult<BoundExpression> {
    let (value, result_type) = match lit {
        Literal::Integer(v) => (TypedValue::Int64(*v), LogicalType::Int64),
        Literal::Float(v) => (TypedValue::Double(*v), LogicalType::Double),
        Literal::String(s) => (TypedValue::String(s.clone()), LogicalType::String),
        Literal::Bool(b) => (TypedValue::Bool(*b), LogicalType::Bool),
        Literal::Null => (TypedValue::Null, LogicalType::Any),
    };
    Ok(BoundExpression::Literal { value, result_type })
}

fn bind_variable(name: &SmolStr, scope: &BinderScope) -> KyuResult<BoundExpression> {
    let info = scope.resolve(name).ok_or_else(|| {
        KyuError::Binder(format!("variable '{name}' is not defined"))
    })?;
    Ok(BoundExpression::Variable {
        index: info.index,
        result_type: info.data_type.clone(),
    })
}

fn bind_property(
    object: &Spanned<Expression>,
    key: &Spanned<SmolStr>,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound_object = bind_expression(object, scope, catalog, registry)?;

    // If the object is a variable bound to a table, resolve the property from catalog.
    if let BoundExpression::Variable { index, .. } = &bound_object {
        // Look up the variable in scope to get its table_id.
        let var_info = scope
            .current_variables()
            .iter()
            .chain(std::iter::empty()) // Just to have an iterator
            .find(|(_, info)| info.index == *index)
            .map(|(_, info)| info);

        // Also search all frames via resolve with the variable name.
        let var_info = var_info.or_else(|| {
            // We need the name — search scope by index.
            find_variable_by_index(scope, *index)
        });

        if let Some(info) = var_info
            && let Some(table_id) = info.table_id
        {
            let entry = catalog.find_by_id(table_id).ok_or_else(|| {
                KyuError::Binder(format!("table id {table_id:?} not found in catalog"))
            })?;
            let prop_name = &key.0;
            let prop = find_property_on_entry(entry, prop_name)?;
            return Ok(BoundExpression::Property {
                object: Box::new(bound_object),
                property_id: prop.id,
                property_name: prop.name.clone(),
                result_type: prop.data_type.clone(),
            });
        }
    }

    // Fallback: if object is Node or Rel type but we couldn't resolve via catalog,
    // return Any type.
    Ok(BoundExpression::Property {
        object: Box::new(bound_object),
        property_id: kyu_common::id::PropertyId(0),
        property_name: key.0.clone(),
        result_type: LogicalType::Any,
    })
}

fn find_variable_by_index(scope: &BinderScope, index: u32) -> Option<&crate::scope::VariableInfo> {
    scope
        .current_variables()
        .iter()
        .find(|(_, info)| info.index == index)
        .map(|(_, info)| info)
}

fn find_property_on_entry<'a>(
    entry: &'a kyu_catalog::CatalogEntry,
    name: &str,
) -> KyuResult<&'a kyu_catalog::Property> {
    let lower = name.to_lowercase();
    entry
        .properties()
        .iter()
        .find(|p| p.name.to_lowercase() == lower)
        .ok_or_else(|| {
            KyuError::Binder(format!(
                "property '{}' not found on table '{}'",
                name,
                entry.name()
            ))
        })
}

fn bind_function_call(
    name: &[Spanned<SmolStr>],
    distinct: bool,
    args: &[Spanned<Expression>],
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    // Join multi-part name with '.'.
    let func_name: String = name
        .iter()
        .map(|(s, _)| s.as_str())
        .collect::<Vec<_>>()
        .join(".");

    // Bind all arguments.
    let bound_args: Vec<BoundExpression> = args
        .iter()
        .map(|a| bind_expression(a, scope, catalog, registry))
        .collect::<KyuResult<_>>()?;

    let arg_types: Vec<LogicalType> = bound_args.iter().map(|a| a.result_type().clone()).collect();

    // Resolve function.
    let sig = registry.resolve(&func_name, &arg_types)?;

    Ok(BoundExpression::FunctionCall {
        function_id: sig.id,
        function_name: sig.name.clone(),
        args: bound_args,
        distinct,
        result_type: sig.return_type.clone(),
    })
}

fn bind_unary_op(
    op: kyu_parser::ast::UnaryOp,
    operand: &Spanned<Expression>,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound = bind_expression(operand, scope, catalog, registry)?;
    let result_type = match op {
        kyu_parser::ast::UnaryOp::Not => {
            let bound = try_coerce(bound, &LogicalType::Bool)?;
            return Ok(BoundExpression::UnaryOp {
                op,
                operand: Box::new(bound),
                result_type: LogicalType::Bool,
            });
        }
        kyu_parser::ast::UnaryOp::Minus => bound.result_type().clone(),
        kyu_parser::ast::UnaryOp::BitwiseNot => bound.result_type().clone(),
    };
    Ok(BoundExpression::UnaryOp {
        op,
        operand: Box::new(bound),
        result_type,
    })
}

fn bind_binary_op(
    op: BinaryOp,
    left: &Spanned<Expression>,
    right: &Spanned<Expression>,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound_left = bind_expression(left, scope, catalog, registry)?;
    let bound_right = bind_expression(right, scope, catalog, registry)?;

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
        | BinaryOp::Pow => {
            let (l, r, result_type) = coerce_binary_arithmetic(bound_left, bound_right)?;
            Ok(BoundExpression::BinaryOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
                result_type,
            })
        }
        BinaryOp::And | BinaryOp::Or | BinaryOp::Xor => {
            let l = try_coerce(bound_left, &LogicalType::Bool)?;
            let r = try_coerce(bound_right, &LogicalType::Bool)?;
            Ok(BoundExpression::BinaryOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
                result_type: LogicalType::Bool,
            })
        }
        BinaryOp::Concat => {
            let (l, r) = coerce_concat(bound_left, bound_right)?;
            Ok(BoundExpression::BinaryOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
                result_type: LogicalType::String,
            })
        }
        BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => {
            Ok(BoundExpression::BinaryOp {
                op,
                left: Box::new(bound_left),
                right: Box::new(bound_right),
                result_type: LogicalType::Int64,
            })
        }
    }
}

/// Desugar chained comparison `a < b < c` into `a < b AND b < c`.
fn bind_comparison(
    left: &Spanned<Expression>,
    ops: &[(ComparisonOp, Spanned<Expression>)],
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    if ops.is_empty() {
        return bind_expression(left, scope, catalog, registry);
    }

    // Single comparison: a op b
    if ops.len() == 1 {
        let (op, ref right_expr) = ops[0];
        let bound_left = bind_expression(left, scope, catalog, registry)?;
        let bound_right = bind_expression(right_expr, scope, catalog, registry)?;
        let (l, r) = coerce_comparison(bound_left, bound_right)?;
        return Ok(BoundExpression::Comparison {
            op,
            left: Box::new(l),
            right: Box::new(r),
        });
    }

    // Chained: a op1 b op2 c → (a op1 b) AND (b op2 c)
    let mut conjuncts = Vec::new();
    let mut prev = bind_expression(left, scope, catalog, registry)?;

    for (op, right_expr) in ops {
        let right = bind_expression(right_expr, scope, catalog, registry)?;
        let (l, r) = coerce_comparison(prev.clone(), right.clone())?;
        conjuncts.push(BoundExpression::Comparison {
            op: *op,
            left: Box::new(l),
            right: Box::new(r),
        });
        prev = right;
    }

    // Fold into AND chain.
    let mut result = conjuncts.pop().unwrap();
    while let Some(cmp) = conjuncts.pop() {
        result = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(cmp),
            right: Box::new(result),
            result_type: LogicalType::Bool,
        };
    }

    Ok(result)
}

fn bind_in_list(
    expr: &Spanned<Expression>,
    list: &Spanned<Expression>,
    negated: bool,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound_expr = bind_expression(expr, scope, catalog, registry)?;
    let bound_list = bind_expression(list, scope, catalog, registry)?;

    // The list expression should be a list literal; flatten it.
    let list_items = match bound_list {
        BoundExpression::ListLiteral { elements, .. } => elements,
        other => vec![other],
    };

    Ok(BoundExpression::InList {
        expr: Box::new(bound_expr),
        list: list_items,
        negated,
    })
}

fn bind_list_literal(
    elements: &[Spanned<Expression>],
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound: Vec<BoundExpression> = elements
        .iter()
        .map(|e| bind_expression(e, scope, catalog, registry))
        .collect::<KyuResult<_>>()?;

    let elem_types: Vec<LogicalType> = bound.iter().map(|e| e.result_type().clone()).collect();
    let elem_type = if elem_types.is_empty() {
        LogicalType::Any
    } else {
        common_type(&elem_types)?
    };

    Ok(BoundExpression::ListLiteral {
        elements: bound,
        result_type: LogicalType::List(Box::new(elem_type)),
    })
}

fn bind_map_literal(
    entries: &[(Spanned<SmolStr>, Spanned<Expression>)],
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound: Vec<(BoundExpression, BoundExpression)> = entries
        .iter()
        .map(|(k, v)| {
            let key = BoundExpression::Literal {
                value: TypedValue::String(k.0.clone()),
                result_type: LogicalType::String,
            };
            let val = bind_expression(v, scope, catalog, registry)?;
            Ok((key, val))
        })
        .collect::<KyuResult<_>>()?;

    let val_types: Vec<LogicalType> = bound.iter().map(|(_, v)| v.result_type().clone()).collect();
    let val_type = if val_types.is_empty() {
        LogicalType::Any
    } else {
        common_type(&val_types)?
    };

    Ok(BoundExpression::MapLiteral {
        entries: bound,
        result_type: LogicalType::Map {
            key: Box::new(LogicalType::String),
            value: Box::new(val_type),
        },
    })
}

fn bind_case(
    operand: &Option<Box<Spanned<Expression>>>,
    whens: &[(Spanned<Expression>, Spanned<Expression>)],
    else_expr: &Option<Box<Spanned<Expression>>>,
    scope: &BinderScope,
    catalog: &CatalogContent,
    registry: &FunctionRegistry,
) -> KyuResult<BoundExpression> {
    let bound_operand = operand
        .as_ref()
        .map(|e| bind_expression(e, scope, catalog, registry))
        .transpose()?
        .map(Box::new);

    let mut bound_whens = Vec::with_capacity(whens.len());
    let mut result_types = Vec::new();

    for (when_expr, then_expr) in whens {
        let w = bind_expression(when_expr, scope, catalog, registry)?;
        let t = bind_expression(then_expr, scope, catalog, registry)?;
        result_types.push(t.result_type().clone());
        bound_whens.push((w, t));
    }

    let bound_else = else_expr
        .as_ref()
        .map(|e| bind_expression(e, scope, catalog, registry))
        .transpose()?;

    if let Some(ref e) = bound_else {
        result_types.push(e.result_type().clone());
    }

    let result_type = if result_types.is_empty() {
        LogicalType::Any
    } else {
        common_type(&result_types)?
    };

    Ok(BoundExpression::Case {
        operand: bound_operand,
        whens: bound_whens,
        else_expr: bound_else.map(Box::new),
        result_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_catalog::{CatalogContent, NodeTableEntry, Property, RelTableEntry};
    use kyu_common::id::{PropertyId, TableId};
    use kyu_expression::FunctionRegistry;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        catalog.add_node_table(NodeTableEntry {
            table_id: TableId(0),
            name: SmolStr::new("Person"),
            properties: vec![
                Property::new(PropertyId(0), "name", LogicalType::String, true),
                Property::new(PropertyId(1), "age", LogicalType::Int64, false),
            ],
            primary_key_idx: 0,
            num_rows: 0,
            comment: None,
        }).unwrap();
        catalog.add_rel_table(RelTableEntry {
            table_id: TableId(1),
            name: SmolStr::new("KNOWS"),
            from_table_id: TableId(0),
            to_table_id: TableId(0),
            properties: vec![
                Property::new(PropertyId(2), "since", LogicalType::Int64, false),
            ],
            num_rows: 0,
            comment: None,
        }).unwrap();
        catalog
    }

    fn parse_expr(s: &str) -> Spanned<Expression> {
        // Parse "RETURN <expr>" and extract the expression.
        let result = kyu_parser::parse(&format!("RETURN {s}"));
        let stmt = result.ast.expect("parse failed");
        match stmt {
            kyu_parser::ast::Statement::Query(q) => {
                let proj = q.parts[0].projection.as_ref().unwrap();
                match &proj.items {
                    kyu_parser::ast::ProjectionItems::Expressions(exprs) => {
                        exprs[0].0.clone()
                    }
                    _ => panic!("expected expressions"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn bind_integer_literal() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("42");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Int64);
        assert!(bound.is_constant());
    }

    #[test]
    fn bind_string_literal() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("'hello'");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::String);
    }

    #[test]
    fn bind_bool_literal() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("true");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn bind_null_literal() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("null");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Any);
    }

    #[test]
    fn bind_variable_found() {
        let catalog = make_catalog();
        let mut scope = BinderScope::new();
        scope.define("p", LogicalType::Node, Some(TableId(0))).unwrap();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("p");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert!(matches!(bound, BoundExpression::Variable { index: 0, .. }));
    }

    #[test]
    fn bind_variable_not_found() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("unknown_var");
        let result = bind_expression(&expr, &scope, &catalog, &registry);
        assert!(result.is_err());
    }

    #[test]
    fn bind_property_access() {
        let catalog = make_catalog();
        let mut scope = BinderScope::new();
        scope.define("p", LogicalType::Node, Some(TableId(0))).unwrap();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("p.name");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::String);
        if let BoundExpression::Property { property_id, .. } = &bound {
            assert_eq!(*property_id, PropertyId(0));
        } else {
            panic!("expected Property");
        }
    }

    #[test]
    fn bind_property_not_found() {
        let catalog = make_catalog();
        let mut scope = BinderScope::new();
        scope.define("p", LogicalType::Node, Some(TableId(0))).unwrap();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("p.nonexistent");
        let result = bind_expression(&expr, &scope, &catalog, &registry);
        assert!(result.is_err());
    }

    #[test]
    fn bind_binary_add_coercion() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("1 + 2.0");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Double);
    }

    #[test]
    fn bind_comparison_gt() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("1 > 2");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn bind_function_call() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("upper('hello')");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::String);
    }

    #[test]
    fn bind_count_star() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("count(*)");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Int64);
    }

    #[test]
    fn bind_is_null() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("null IS NULL");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn bind_case_expression() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("CASE WHEN true THEN 1 ELSE 2 END");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Int64);
    }

    #[test]
    fn bind_string_starts_with() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("'hello' STARTS WITH 'he'");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Bool);
    }

    #[test]
    fn bind_list_literal() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("[1, 2, 3]");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(
            bound.result_type(),
            &LogicalType::List(Box::new(LogicalType::Int64))
        );
    }

    #[test]
    fn bind_arithmetic_type_error() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("'hello' + 42");
        let result = bind_expression(&expr, &scope, &catalog, &registry);
        assert!(result.is_err());
    }

    #[test]
    fn bind_unary_not() {
        let catalog = make_catalog();
        let scope = BinderScope::new();
        let registry = FunctionRegistry::with_builtins();
        let expr = parse_expr("NOT true");
        let bound = bind_expression(&expr, &scope, &catalog, &registry).unwrap();
        assert_eq!(bound.result_type(), &LogicalType::Bool);
    }
}
