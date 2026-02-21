//! Plan builder — transforms `BoundStatement` into a `LogicalPlan` tree.
//!
//! Single-pass walk over the bound statement, producing scan, join, filter,
//! projection, aggregate, and other logical operators.

use kyu_binder::*;
use kyu_catalog::CatalogContent;
use kyu_common::{KyuError, KyuResult};
use kyu_expression::BoundExpression;
use kyu_types::LogicalType;
use smol_str::SmolStr;

use crate::logical_plan::*;

// Known aggregate function names.
const AGGREGATE_FUNCTIONS: &[&str] = &["count", "sum", "avg", "min", "max", "collect"];

/// Build a logical plan from a bound statement.
pub fn build_plan(stmt: &BoundStatement, catalog: &CatalogContent) -> KyuResult<LogicalPlan> {
    match stmt {
        BoundStatement::Query(query) => build_query(query, catalog),
        _ => Err(KyuError::NotImplemented(
            "only query statements are supported in the planner".into(),
        )),
    }
}

/// Build a logical plan directly from a bound query (avoids BoundStatement wrapping/cloning).
pub fn build_query_plan(query: &BoundQuery, catalog: &CatalogContent) -> KyuResult<LogicalPlan> {
    build_query(query, catalog)
}

fn build_query(query: &BoundQuery, catalog: &CatalogContent) -> KyuResult<LogicalPlan> {
    let mut plan = None;

    for part in &query.parts {
        plan = Some(build_query_part(part, plan, catalog)?);
    }

    let mut result = plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }));

    // Handle UNION ALL chains.
    if !query.union_all.is_empty() {
        let mut children = vec![result];
        for (all, sub_query) in &query.union_all {
            let sub_plan = build_query(sub_query, catalog)?;
            children.push(sub_plan);
            // Note: *all indicates UNION ALL (true) vs UNION (false).
            // For now we only handle UNION ALL; UNION adds DISTINCT.
            if !all {
                // Wrap in Distinct.
                let union_plan = LogicalPlan::Union(Box::new(LogicalUnion {
                    children,
                    all: false,
                }));
                return Ok(LogicalPlan::Distinct(Box::new(LogicalDistinct {
                    child: union_plan,
                })));
            }
        }
        result = LogicalPlan::Union(Box::new(LogicalUnion {
            children,
            all: true,
        }));
    }

    Ok(result)
}

fn build_query_part(
    part: &BoundQueryPart,
    input: Option<LogicalPlan>,
    catalog: &CatalogContent,
) -> KyuResult<LogicalPlan> {
    // Start with input from previous part (WITH chaining) or nothing.
    let mut plan = input;

    // Process reading clauses.
    for clause in &part.reading_clauses {
        let clause_plan = match clause {
            BoundReadingClause::Match(m) => build_match(m, plan.clone(), catalog)?,
            BoundReadingClause::Unwind(u) => {
                let child = plan
                    .clone()
                    .unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }));
                LogicalPlan::Unwind(Box::new(LogicalUnwind {
                    child,
                    expression: u.expression.clone(),
                    variable_index: u.variable_index,
                    element_type: u.element_type.clone(),
                    alias: SmolStr::new(format!("_unwind_{}", u.variable_index)),
                }))
            }
        };
        plan = Some(clause_plan);
    }

    // Process updating clauses.
    for clause in &part.updating_clauses {
        let child = plan
            .clone()
            .unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }));
        plan = Some(build_updating(child, clause)?);
    }

    // Resolve Property expressions → Variable column references.
    // Build property map from scan nodes in the plan tree.
    let prop_map = if let Some(ref p) = plan {
        build_property_map(p)
    } else {
        HashMap::new()
    };

    // Process projection (RETURN or WITH).
    if let Some(ref proj) = part.projection {
        let child = plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }));
        let child = resolve_plan_properties(child, &prop_map);
        plan = Some(build_projection(child, proj, &prop_map)?);
    }

    Ok(plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 })))
}

fn build_match(
    match_clause: &BoundMatchClause,
    input: Option<LogicalPlan>,
    catalog: &CatalogContent,
) -> KyuResult<LogicalPlan> {
    let mut plan: Option<LogicalPlan> = input;

    for pattern in &match_clause.patterns {
        let pattern_plan = build_pattern(pattern, catalog)?;
        plan = Some(match plan {
            None => pattern_plan,
            Some(existing) => {
                // Multiple patterns or input from previous clause: cross product.
                LogicalPlan::CrossProduct(Box::new(LogicalCrossProduct {
                    left: existing,
                    right: pattern_plan,
                }))
            }
        });
    }

    // Apply WHERE clause as a Filter.
    if let Some(ref predicate) = match_clause.where_clause {
        let child = plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }));
        plan = Some(LogicalPlan::Filter(Box::new(LogicalFilter {
            child,
            predicate: predicate.clone(),
        })));
    }

    Ok(plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 })))
}

fn build_pattern(pattern: &BoundPattern, catalog: &CatalogContent) -> KyuResult<LogicalPlan> {
    let mut plan: Option<LogicalPlan> = None;
    let mut last_node_var: Option<u32> = None;

    for element in &pattern.elements {
        match element {
            BoundPatternElement::Node(node) => {
                let columns = build_node_columns(node, catalog);
                let scan = LogicalPlan::ScanNode(LogicalScanNode {
                    table_id: node.table_id,
                    variable_index: node.variable_index,
                    output_columns: columns.clone(),
                });

                plan = Some(match plan {
                    None => scan,
                    Some(existing) => {
                        // Join with previous relationship: rel.dst = node.id
                        // The join key is synthesized — use variable indices as column refs.
                        LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
                            build: existing,
                            probe: scan,
                            // Build key: last column of rel scan (dst_id)
                            // Probe key: first column of node scan (node_id)
                            build_keys: vec![BoundExpression::Variable {
                                index: u32::MAX, // sentinel: "last column of build"
                                result_type: LogicalType::InternalId,
                            }],
                            probe_keys: vec![BoundExpression::Variable {
                                index: node.variable_index.unwrap_or(0),
                                result_type: LogicalType::InternalId,
                            }],
                        }))
                    }
                });

                last_node_var = node.variable_index;
            }
            BoundPatternElement::Relationship(rel) => {
                let columns = build_rel_columns(rel, catalog);
                let scan = LogicalPlan::ScanRel(LogicalScanRel {
                    table_id: rel.table_id,
                    variable_index: rel.variable_index,
                    direction: rel.direction,
                    bound_node_var: last_node_var.unwrap_or(0),
                    output_columns: columns,
                });

                plan = Some(match plan {
                    None => LogicalPlan::ScanRel(LogicalScanRel {
                        table_id: rel.table_id,
                        variable_index: rel.variable_index,
                        direction: rel.direction,
                        bound_node_var: 0,
                        output_columns: build_rel_columns(rel, catalog),
                    }),
                    Some(existing) => {
                        // Join previous node with this relationship: node.id = rel.src_id
                        LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
                            build: existing,
                            probe: scan,
                            build_keys: vec![BoundExpression::Variable {
                                index: last_node_var.unwrap_or(0),
                                result_type: LogicalType::InternalId,
                            }],
                            probe_keys: vec![BoundExpression::Variable {
                                index: u32::MAX - 1, // sentinel: "src column of rel"
                                result_type: LogicalType::InternalId,
                            }],
                        }))
                    }
                });
            }
        }
    }

    Ok(plan.unwrap_or(LogicalPlan::Empty(LogicalEmpty { num_columns: 0 })))
}

fn build_node_columns(
    node: &BoundNodePattern,
    catalog: &CatalogContent,
) -> Vec<(SmolStr, LogicalType)> {
    let mut columns = Vec::new();

    // Property columns from the catalog (in catalog order = physical column order).
    // Note: no synthetic _var column — the physical scan produces only property columns.
    if let Some(entry) = catalog.find_by_id(node.table_id) {
        for prop in entry.properties() {
            columns.push((prop.name.clone(), prop.data_type.clone()));
        }
    }

    columns
}

fn build_rel_columns(
    rel: &BoundRelPattern,
    catalog: &CatalogContent,
) -> Vec<(SmolStr, LogicalType)> {
    let mut columns = Vec::new();

    // Relationship variable.
    if let Some(var_idx) = rel.variable_index {
        columns.push((SmolStr::new(format!("_var{var_idx}")), LogicalType::Rel));
    }

    // src and dst columns for join purposes.
    columns.push((SmolStr::new("_src"), LogicalType::InternalId));
    columns.push((SmolStr::new("_dst"), LogicalType::InternalId));

    // Property columns.
    if let Some(entry) = catalog.find_by_id(rel.table_id) {
        for prop in entry.properties() {
            columns.push((prop.name.clone(), prop.data_type.clone()));
        }
    }

    columns
}

fn build_updating(child: LogicalPlan, clause: &BoundUpdatingClause) -> KyuResult<LogicalPlan> {
    match clause {
        BoundUpdatingClause::Create(patterns) => {
            let mut plan = child;
            for pattern in patterns {
                for element in &pattern.elements {
                    match element {
                        BoundPatternElement::Node(node) => {
                            plan = LogicalPlan::CreateNode(Box::new(LogicalCreate {
                                child: plan,
                                table_id: node.table_id,
                                is_node: true,
                                properties: node.properties.clone(),
                                variable_index: node.variable_index,
                            }));
                        }
                        BoundPatternElement::Relationship(rel) => {
                            plan = LogicalPlan::CreateNode(Box::new(LogicalCreate {
                                child: plan,
                                table_id: rel.table_id,
                                is_node: false,
                                properties: rel.properties.clone(),
                                variable_index: rel.variable_index,
                            }));
                        }
                    }
                }
            }
            Ok(plan)
        }
        BoundUpdatingClause::Set(items) => {
            let set_items = items
                .iter()
                .map(|item| LogicalSetItem {
                    object: item.object.clone(),
                    property_id: item.property_id,
                    value: item.value.clone(),
                })
                .collect();
            Ok(LogicalPlan::SetProperty(Box::new(LogicalSet {
                child,
                items: set_items,
            })))
        }
        BoundUpdatingClause::Delete(del) => Ok(LogicalPlan::Delete(Box::new(LogicalDelete {
            child,
            expressions: del.expressions.clone(),
            detach: del.detach,
        }))),
    }
}

fn build_projection(
    child: LogicalPlan,
    proj: &BoundProjection,
    prop_map: &HashMap<(u32, SmolStr), u32>,
) -> KyuResult<LogicalPlan> {
    // Separate aggregates from non-aggregates.
    let has_aggregates = proj
        .items
        .iter()
        .any(|item| contains_aggregate(&item.expression));

    let mut plan = child;

    if has_aggregates {
        let mut group_by = Vec::new();
        let mut group_by_aliases = Vec::new();
        let mut aggregates = Vec::new();

        for item in &proj.items {
            if contains_aggregate(&item.expression) {
                // Extract aggregate spec — resolve properties in arguments.
                let resolved_expr = resolve_properties(&item.expression, prop_map);
                let spec = extract_aggregate(&resolved_expr, &item.alias);
                aggregates.push(spec);
            } else {
                // Non-aggregate expression is an implicit group-by key.
                group_by.push(resolve_properties(&item.expression, prop_map));
                group_by_aliases.push(item.alias.clone());
            }
        }

        plan = LogicalPlan::Aggregate(Box::new(LogicalAggregate {
            child: plan,
            group_by,
            aggregates,
            group_by_aliases,
        }));

        // After aggregation, we don't need a separate projection —
        // the aggregate node produces the final columns directly.
    } else {
        // Simple projection — resolve property accesses to column indices.
        let expressions: Vec<_> = proj
            .items
            .iter()
            .map(|i| resolve_properties(&i.expression, prop_map))
            .collect();
        let aliases: Vec<_> = proj.items.iter().map(|i| i.alias.clone()).collect();
        plan = LogicalPlan::Projection(Box::new(LogicalProjection {
            child: plan,
            expressions,
            aliases,
        }));
    }

    // Apply DISTINCT.
    if proj.distinct {
        plan = LogicalPlan::Distinct(Box::new(LogicalDistinct { child: plan }));
    }

    // Apply ORDER BY — resolve properties in sort expressions.
    if !proj.order_by.is_empty() {
        let order_by = proj
            .order_by
            .iter()
            .map(|(e, ord)| (resolve_properties(e, prop_map), *ord))
            .collect();
        plan = LogicalPlan::OrderBy(Box::new(LogicalOrderBy {
            child: plan,
            order_by,
        }));
    }

    // Apply SKIP and LIMIT.
    let skip = proj.skip.as_ref().and_then(eval_constant_u64);
    let limit = proj.limit.as_ref().and_then(eval_constant_u64);
    if skip.is_some() || limit.is_some() {
        plan = LogicalPlan::Limit(Box::new(LogicalLimit {
            child: plan,
            skip,
            limit,
        }));
    }

    Ok(plan)
}

/// Check if an expression contains an aggregate function call or COUNT(*).
fn contains_aggregate(expr: &BoundExpression) -> bool {
    match expr {
        BoundExpression::CountStar => true,
        BoundExpression::FunctionCall { function_name, .. } => {
            AGGREGATE_FUNCTIONS.contains(&function_name.to_lowercase().as_str())
        }
        _ => false,
    }
}

/// Extract an AggregateSpec from an aggregate expression.
fn extract_aggregate(expr: &BoundExpression, alias: &SmolStr) -> AggregateSpec {
    match expr {
        BoundExpression::CountStar => AggregateSpec {
            function_name: SmolStr::new("count"),
            resolved_func: AggFunc::Count,
            arg: None,
            distinct: false,
            result_type: LogicalType::Int64,
            alias: alias.clone(),
        },
        BoundExpression::FunctionCall {
            function_name,
            args,
            distinct,
            result_type,
            ..
        } => AggregateSpec {
            function_name: function_name.clone(),
            resolved_func: AggFunc::from_name(function_name).unwrap_or(AggFunc::Count),
            arg: args.first().cloned(),
            distinct: *distinct,
            result_type: result_type.clone(),
            alias: alias.clone(),
        },
        _ => AggregateSpec {
            function_name: SmolStr::new("unknown"),
            resolved_func: AggFunc::Count,
            arg: Some(expr.clone()),
            distinct: false,
            result_type: expr.result_type().clone(),
            alias: alias.clone(),
        },
    }
}

/// Try to evaluate a constant expression as a u64 (for SKIP/LIMIT).
fn eval_constant_u64(expr: &BoundExpression) -> Option<u64> {
    if let Ok(val) = kyu_expression::evaluate_constant(expr) {
        match val {
            kyu_types::TypedValue::Int64(v) if v >= 0 => Some(v as u64),
            kyu_types::TypedValue::Int32(v) if v >= 0 => Some(v as u64),
            _ => None,
        }
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Property resolution — resolve Property expressions to Variable column refs
// ---------------------------------------------------------------------------

use std::collections::HashMap;

/// Build a property map from a logical plan tree.
/// Maps (variable_index, property_name) → physical column index.
fn build_property_map(plan: &LogicalPlan) -> HashMap<(u32, SmolStr), u32> {
    let mut map = HashMap::new();
    let mut offset = 0u32;
    collect_scan_properties(plan, &mut map, &mut offset);
    map
}

fn collect_scan_properties(
    plan: &LogicalPlan,
    map: &mut HashMap<(u32, SmolStr), u32>,
    offset: &mut u32,
) {
    match plan {
        LogicalPlan::ScanNode(scan) => {
            if let Some(var_idx) = scan.variable_index {
                for (i, (name, _)) in scan.output_columns.iter().enumerate() {
                    map.insert((var_idx, name.clone()), *offset + i as u32);
                }
            }
            *offset += scan.output_columns.len() as u32;
        }
        LogicalPlan::ScanRel(scan) => {
            if let Some(var_idx) = scan.variable_index {
                for (i, (name, _)) in scan.output_columns.iter().enumerate() {
                    map.insert((var_idx, name.clone()), *offset + i as u32);
                }
            }
            *offset += scan.output_columns.len() as u32;
        }
        LogicalPlan::CrossProduct(cp) => {
            collect_scan_properties(&cp.left, map, offset);
            collect_scan_properties(&cp.right, map, offset);
        }
        LogicalPlan::HashJoin(j) => {
            collect_scan_properties(&j.build, map, offset);
            collect_scan_properties(&j.probe, map, offset);
        }
        LogicalPlan::Filter(f) => {
            collect_scan_properties(&f.child, map, offset);
        }
        LogicalPlan::Unwind(u) => {
            collect_scan_properties(&u.child, map, offset);
            // Unwind adds one column (the unwound variable).
            *offset += 1;
        }
        _ => {}
    }
}

/// Recursively resolve Property expressions to Variable column references.
pub fn resolve_properties(
    expr: &BoundExpression,
    map: &HashMap<(u32, SmolStr), u32>,
) -> BoundExpression {
    match expr {
        BoundExpression::Property {
            object,
            property_name,
            result_type,
            ..
        } => {
            if let BoundExpression::Variable { index, .. } = object.as_ref()
                && let Some(&col_idx) = map.get(&(*index, property_name.clone()))
            {
                return BoundExpression::Variable {
                    index: col_idx,
                    result_type: result_type.clone(),
                };
            }
            expr.clone()
        }
        BoundExpression::BinaryOp {
            op,
            left,
            right,
            result_type,
        } => BoundExpression::BinaryOp {
            op: *op,
            left: Box::new(resolve_properties(left, map)),
            right: Box::new(resolve_properties(right, map)),
            result_type: result_type.clone(),
        },
        BoundExpression::UnaryOp {
            op,
            operand,
            result_type,
        } => BoundExpression::UnaryOp {
            op: *op,
            operand: Box::new(resolve_properties(operand, map)),
            result_type: result_type.clone(),
        },
        BoundExpression::Comparison { op, left, right } => BoundExpression::Comparison {
            op: *op,
            left: Box::new(resolve_properties(left, map)),
            right: Box::new(resolve_properties(right, map)),
        },
        BoundExpression::IsNull { expr, negated } => BoundExpression::IsNull {
            expr: Box::new(resolve_properties(expr, map)),
            negated: *negated,
        },
        BoundExpression::InList {
            expr,
            list,
            negated,
        } => BoundExpression::InList {
            expr: Box::new(resolve_properties(expr, map)),
            list: list.iter().map(|e| resolve_properties(e, map)).collect(),
            negated: *negated,
        },
        BoundExpression::FunctionCall {
            function_id,
            function_name,
            args,
            distinct,
            result_type,
        } => BoundExpression::FunctionCall {
            function_id: *function_id,
            function_name: function_name.clone(),
            args: args.iter().map(|a| resolve_properties(a, map)).collect(),
            distinct: *distinct,
            result_type: result_type.clone(),
        },
        BoundExpression::Case {
            operand,
            whens,
            else_expr,
            result_type,
        } => BoundExpression::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(resolve_properties(o, map))),
            whens: whens
                .iter()
                .map(|(w, t)| (resolve_properties(w, map), resolve_properties(t, map)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(resolve_properties(e, map))),
            result_type: result_type.clone(),
        },
        BoundExpression::Cast { expr, target_type } => BoundExpression::Cast {
            expr: Box::new(resolve_properties(expr, map)),
            target_type: target_type.clone(),
        },
        BoundExpression::StringOp { op, left, right } => BoundExpression::StringOp {
            op: *op,
            left: Box::new(resolve_properties(left, map)),
            right: Box::new(resolve_properties(right, map)),
        },
        BoundExpression::Subscript {
            expr,
            index,
            result_type,
        } => BoundExpression::Subscript {
            expr: Box::new(resolve_properties(expr, map)),
            index: Box::new(resolve_properties(index, map)),
            result_type: result_type.clone(),
        },
        // Leaf expressions (Literal, Variable, Parameter, CountStar, etc.)
        _ => expr.clone(),
    }
}

/// Walk a logical plan tree and resolve all Property expressions using the map.
fn resolve_plan_properties(
    plan: LogicalPlan,
    map: &HashMap<(u32, SmolStr), u32>,
) -> LogicalPlan {
    if map.is_empty() {
        return plan;
    }
    match plan {
        LogicalPlan::Filter(f) => LogicalPlan::Filter(Box::new(LogicalFilter {
            child: resolve_plan_properties(f.child, map),
            predicate: resolve_properties(&f.predicate, map),
        })),
        LogicalPlan::Projection(p) => LogicalPlan::Projection(Box::new(LogicalProjection {
            child: resolve_plan_properties(p.child, map),
            expressions: p
                .expressions
                .iter()
                .map(|e| resolve_properties(e, map))
                .collect(),
            aliases: p.aliases,
        })),
        LogicalPlan::Aggregate(a) => LogicalPlan::Aggregate(Box::new(LogicalAggregate {
            child: resolve_plan_properties(a.child, map),
            group_by: a
                .group_by
                .iter()
                .map(|e| resolve_properties(e, map))
                .collect(),
            aggregates: a
                .aggregates
                .into_iter()
                .map(|mut agg| {
                    agg.arg = agg.arg.map(|a| resolve_properties(&a, map));
                    agg
                })
                .collect(),
            group_by_aliases: a.group_by_aliases,
        })),
        LogicalPlan::OrderBy(o) => LogicalPlan::OrderBy(Box::new(LogicalOrderBy {
            child: resolve_plan_properties(o.child, map),
            order_by: o
                .order_by
                .iter()
                .map(|(e, ord)| (resolve_properties(e, map), *ord))
                .collect(),
        })),
        LogicalPlan::Limit(l) => LogicalPlan::Limit(Box::new(LogicalLimit {
            child: resolve_plan_properties(l.child, map),
            skip: l.skip,
            limit: l.limit,
        })),
        LogicalPlan::Distinct(d) => LogicalPlan::Distinct(Box::new(LogicalDistinct {
            child: resolve_plan_properties(d.child, map),
        })),
        LogicalPlan::CrossProduct(cp) => {
            LogicalPlan::CrossProduct(Box::new(LogicalCrossProduct {
                left: resolve_plan_properties(cp.left, map),
                right: resolve_plan_properties(cp.right, map),
            }))
        }
        LogicalPlan::HashJoin(j) => LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: resolve_plan_properties(j.build, map),
            probe: resolve_plan_properties(j.probe, map),
            build_keys: j
                .build_keys
                .iter()
                .map(|e| resolve_properties(e, map))
                .collect(),
            probe_keys: j
                .probe_keys
                .iter()
                .map(|e| resolve_properties(e, map))
                .collect(),
        })),
        // Leaf/other nodes pass through
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_binder::Binder;
    use kyu_catalog::{NodeTableEntry, Property, RelTableEntry};
    use kyu_common::id::{PropertyId, TableId};
    use kyu_expression::FunctionRegistry;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: TableId(0),
                name: SmolStr::new("Person"),
                properties: vec![
                    Property::new(PropertyId(0), "name", LogicalType::String, true),
                    Property::new(PropertyId(1), "age", LogicalType::Int64, false),
                ],
                primary_key_idx: 0,
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
            .add_rel_table(RelTableEntry {
                table_id: TableId(1),
                name: SmolStr::new("KNOWS"),
                from_table_id: TableId(0),
                to_table_id: TableId(0),
                properties: vec![Property::new(
                    PropertyId(2),
                    "since",
                    LogicalType::Int64,
                    false,
                )],
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
    }

    fn plan_query(cypher: &str, catalog: &CatalogContent) -> KyuResult<LogicalPlan> {
        let result = kyu_parser::parse(cypher);
        let stmt = result
            .ast
            .ok_or_else(|| KyuError::Binder(format!("parse failed: {:?}", result.errors)))?;
        let mut binder = Binder::new(catalog.clone(), FunctionRegistry::with_builtins());
        let bound = binder.bind(&stmt)?;
        build_plan(&bound, catalog)
    }

    #[test]
    fn plan_standalone_return() {
        let catalog = make_catalog();
        let plan = plan_query("RETURN 1 AS x", &catalog).unwrap();
        assert!(matches!(plan, LogicalPlan::Projection(_)));
    }

    #[test]
    fn plan_standalone_return_multiple() {
        let catalog = make_catalog();
        let plan = plan_query("RETURN 1 AS x, 'hello' AS y", &catalog).unwrap();
        if let LogicalPlan::Projection(p) = &plan {
            assert_eq!(p.expressions.len(), 2);
            assert_eq!(p.aliases.len(), 2);
        } else {
            panic!("expected Projection");
        }
    }

    #[test]
    fn plan_match_return() {
        let catalog = make_catalog();
        let plan = plan_query("MATCH (p:Person) RETURN p.name", &catalog).unwrap();
        // Should be Projection(ScanNode)
        assert!(matches!(plan, LogicalPlan::Projection(_)));
        if let LogicalPlan::Projection(p) = &plan {
            assert!(matches!(p.child, LogicalPlan::ScanNode(_)));
        }
    }

    #[test]
    fn plan_match_where_return() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            &catalog,
        )
        .unwrap();
        // Should be Projection(Filter(ScanNode))
        assert!(matches!(plan, LogicalPlan::Projection(_)));
        if let LogicalPlan::Projection(p) = &plan {
            assert!(matches!(p.child, LogicalPlan::Filter(_)));
        }
    }

    #[test]
    fn plan_match_relationship() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, b.name",
            &catalog,
        )
        .unwrap();
        // Should have joins in the tree.
        assert!(matches!(plan, LogicalPlan::Projection(_)));
    }

    #[test]
    fn plan_count_star() {
        let catalog = make_catalog();
        let plan = plan_query("MATCH (p:Person) RETURN count(*) AS cnt", &catalog).unwrap();
        // Should be Aggregate(ScanNode)
        assert!(matches!(plan, LogicalPlan::Aggregate(_)));
        if let LogicalPlan::Aggregate(a) = &plan {
            assert_eq!(a.aggregates.len(), 1);
            assert_eq!(a.aggregates[0].function_name.as_str(), "count");
            assert!(a.group_by.is_empty());
        }
    }

    #[test]
    fn plan_group_by_aggregate() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) RETURN p.name, count(*) AS cnt",
            &catalog,
        )
        .unwrap();
        assert!(matches!(plan, LogicalPlan::Aggregate(_)));
        if let LogicalPlan::Aggregate(a) = &plan {
            assert_eq!(a.group_by.len(), 1);
            assert_eq!(a.aggregates.len(), 1);
        }
    }

    #[test]
    fn plan_order_by_limit() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 5",
            &catalog,
        )
        .unwrap();
        // Should be Limit(OrderBy(Projection(ScanNode)))
        assert!(matches!(plan, LogicalPlan::Limit(_)));
        if let LogicalPlan::Limit(l) = &plan {
            assert_eq!(l.limit, Some(5));
            assert!(matches!(l.child, LogicalPlan::OrderBy(_)));
        }
    }

    #[test]
    fn plan_distinct() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) RETURN DISTINCT p.name",
            &catalog,
        )
        .unwrap();
        // Distinct should wrap the projection.
        assert!(matches!(plan, LogicalPlan::Distinct(_)));
    }

    #[test]
    fn plan_unwind() {
        let catalog = make_catalog();
        let plan = plan_query("UNWIND [1, 2, 3] AS x RETURN x", &catalog).unwrap();
        // Should be Projection(Unwind(Empty))
        assert!(matches!(plan, LogicalPlan::Projection(_)));
        if let LogicalPlan::Projection(p) = &plan {
            assert!(matches!(p.child, LogicalPlan::Unwind(_)));
        }
    }

    #[test]
    fn plan_with_chaining() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) WITH p.name AS name RETURN name",
            &catalog,
        )
        .unwrap();
        // The result should be a Projection.
        assert!(matches!(plan, LogicalPlan::Projection(_)));
    }

    #[test]
    fn plan_skip_limit() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) RETURN p.name SKIP 1 LIMIT 2",
            &catalog,
        )
        .unwrap();
        assert!(matches!(plan, LogicalPlan::Limit(_)));
        if let LogicalPlan::Limit(l) = &plan {
            assert_eq!(l.skip, Some(1));
            assert_eq!(l.limit, Some(2));
        }
    }

    #[test]
    fn plan_sum_aggregate() {
        let catalog = make_catalog();
        let plan = plan_query(
            "MATCH (p:Person) RETURN sum(p.age) AS total",
            &catalog,
        )
        .unwrap();
        assert!(matches!(plan, LogicalPlan::Aggregate(_)));
        if let LogicalPlan::Aggregate(a) = &plan {
            assert_eq!(a.aggregates.len(), 1);
            assert_eq!(a.aggregates[0].function_name.as_str(), "sum");
            assert!(a.aggregates[0].arg.is_some());
        }
    }

    #[test]
    fn plan_create() {
        let catalog = make_catalog();
        let plan = plan_query("CREATE (p:Person {name: 'Alice'})", &catalog).unwrap();
        assert!(matches!(plan, LogicalPlan::CreateNode(_)));
    }

    #[test]
    fn plan_return_arithmetic() {
        let catalog = make_catalog();
        let plan = plan_query("RETURN 1 + 2 AS sum", &catalog).unwrap();
        assert!(matches!(plan, LogicalPlan::Projection(_)));
        if let LogicalPlan::Projection(p) = &plan {
            assert_eq!(p.aliases[0].as_str(), "sum");
        }
    }

    #[test]
    fn contains_aggregate_countstar() {
        assert!(contains_aggregate(&BoundExpression::CountStar));
    }

    #[test]
    fn contains_aggregate_literal_false() {
        assert!(!contains_aggregate(&BoundExpression::Literal {
            value: kyu_types::TypedValue::Int64(1),
            result_type: LogicalType::Int64,
        }));
    }

    #[test]
    fn contains_aggregate_function_call() {
        assert!(contains_aggregate(&BoundExpression::FunctionCall {
            function_id: kyu_expression::FunctionId(0),
            function_name: SmolStr::new("count"),
            args: vec![],
            distinct: false,
            result_type: LogicalType::Int64,
        }));
    }
}
