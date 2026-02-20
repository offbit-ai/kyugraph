//! Logical plan types — relational operator tree produced by the plan builder.
//!
//! Each variant represents a logical operation. The tree is built from
//! `BoundStatement` and consumed by the physical plan mapper.

use kyu_common::id::{PropertyId, TableId};
use kyu_expression::BoundExpression;
use kyu_parser::ast::{Direction, SortOrder};
use kyu_types::LogicalType;
use smol_str::SmolStr;

/// A logical plan node — tree of relational operators.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    ScanNode(LogicalScanNode),
    ScanRel(LogicalScanRel),
    Filter(Box<LogicalFilter>),
    Projection(Box<LogicalProjection>),
    HashJoin(Box<LogicalHashJoin>),
    CrossProduct(Box<LogicalCrossProduct>),
    Aggregate(Box<LogicalAggregate>),
    OrderBy(Box<LogicalOrderBy>),
    Limit(Box<LogicalLimit>),
    Distinct(Box<LogicalDistinct>),
    Unwind(Box<LogicalUnwind>),
    CreateNode(Box<LogicalCreate>),
    SetProperty(Box<LogicalSet>),
    Delete(Box<LogicalDelete>),
    Union(Box<LogicalUnion>),
    Empty(LogicalEmpty),
}

// ---------------------------------------------------------------------------
// Scan operators
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogicalScanNode {
    pub table_id: TableId,
    pub variable_index: Option<u32>,
    /// Columns produced by this scan: (name, type) pairs.
    /// Typically the variable itself (Node type) plus all properties.
    pub output_columns: Vec<(SmolStr, LogicalType)>,
}

#[derive(Clone, Debug)]
pub struct LogicalScanRel {
    pub table_id: TableId,
    pub variable_index: Option<u32>,
    pub direction: Direction,
    /// Variable index of the bound node this relationship connects from.
    pub bound_node_var: u32,
    pub output_columns: Vec<(SmolStr, LogicalType)>,
}

// ---------------------------------------------------------------------------
// Relational operators
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogicalFilter {
    pub child: LogicalPlan,
    pub predicate: BoundExpression,
}

#[derive(Clone, Debug)]
pub struct LogicalProjection {
    pub child: LogicalPlan,
    pub expressions: Vec<BoundExpression>,
    pub aliases: Vec<SmolStr>,
}

#[derive(Clone, Debug)]
pub struct LogicalHashJoin {
    pub build: LogicalPlan,
    pub probe: LogicalPlan,
    pub build_keys: Vec<BoundExpression>,
    pub probe_keys: Vec<BoundExpression>,
}

#[derive(Clone, Debug)]
pub struct LogicalCrossProduct {
    pub left: LogicalPlan,
    pub right: LogicalPlan,
}

#[derive(Clone, Debug)]
pub struct LogicalAggregate {
    pub child: LogicalPlan,
    pub group_by: Vec<BoundExpression>,
    pub aggregates: Vec<AggregateSpec>,
    pub group_by_aliases: Vec<SmolStr>,
}

/// Resolved aggregate function for O(1) dispatch in the executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

impl AggFunc {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "collect" => Some(Self::Collect),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AggregateSpec {
    pub function_name: SmolStr,
    pub resolved_func: AggFunc,
    pub arg: Option<BoundExpression>,
    pub distinct: bool,
    pub result_type: LogicalType,
    pub alias: SmolStr,
}

#[derive(Clone, Debug)]
pub struct LogicalOrderBy {
    pub child: LogicalPlan,
    pub order_by: Vec<(BoundExpression, SortOrder)>,
}

#[derive(Clone, Debug)]
pub struct LogicalLimit {
    pub child: LogicalPlan,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct LogicalDistinct {
    pub child: LogicalPlan,
}

#[derive(Clone, Debug)]
pub struct LogicalUnwind {
    pub child: LogicalPlan,
    pub expression: BoundExpression,
    pub variable_index: u32,
    pub element_type: LogicalType,
    pub alias: SmolStr,
}

// ---------------------------------------------------------------------------
// Mutation operators
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogicalCreate {
    pub child: LogicalPlan,
    pub table_id: TableId,
    pub is_node: bool,
    pub properties: Vec<(PropertyId, BoundExpression)>,
    pub variable_index: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct LogicalSet {
    pub child: LogicalPlan,
    pub items: Vec<LogicalSetItem>,
}

#[derive(Clone, Debug)]
pub struct LogicalSetItem {
    pub object: BoundExpression,
    pub property_id: PropertyId,
    pub value: BoundExpression,
}

#[derive(Clone, Debug)]
pub struct LogicalDelete {
    pub child: LogicalPlan,
    pub expressions: Vec<BoundExpression>,
    pub detach: bool,
}

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogicalUnion {
    pub children: Vec<LogicalPlan>,
    pub all: bool,
}

// ---------------------------------------------------------------------------
// Empty source
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogicalEmpty {
    /// Number of columns in the empty row.
    pub num_columns: usize,
}

// ---------------------------------------------------------------------------
// Output schema derivation
// ---------------------------------------------------------------------------

impl LogicalPlan {
    /// Derive the output schema (column name + type pairs) for this plan node.
    pub fn output_schema(&self) -> Vec<(SmolStr, LogicalType)> {
        match self {
            LogicalPlan::ScanNode(scan) => scan.output_columns.clone(),
            LogicalPlan::ScanRel(scan) => scan.output_columns.clone(),

            LogicalPlan::Filter(f) => f.child.output_schema(),

            LogicalPlan::Projection(p) => p
                .expressions
                .iter()
                .zip(&p.aliases)
                .map(|(expr, alias)| (alias.clone(), expr.result_type().clone()))
                .collect(),

            LogicalPlan::HashJoin(j) => {
                let mut schema = j.build.output_schema();
                schema.extend(j.probe.output_schema());
                schema
            }

            LogicalPlan::CrossProduct(cp) => {
                let mut schema = cp.left.output_schema();
                schema.extend(cp.right.output_schema());
                schema
            }

            LogicalPlan::Aggregate(a) => {
                let mut schema: Vec<_> = a
                    .group_by
                    .iter()
                    .zip(&a.group_by_aliases)
                    .map(|(expr, alias)| (alias.clone(), expr.result_type().clone()))
                    .collect();
                for agg in &a.aggregates {
                    schema.push((agg.alias.clone(), agg.result_type.clone()));
                }
                schema
            }

            LogicalPlan::OrderBy(o) => o.child.output_schema(),
            LogicalPlan::Limit(l) => l.child.output_schema(),
            LogicalPlan::Distinct(d) => d.child.output_schema(),

            LogicalPlan::Unwind(u) => {
                let mut schema = u.child.output_schema();
                schema.push((u.alias.clone(), u.element_type.clone()));
                schema
            }

            LogicalPlan::CreateNode(c) => c.child.output_schema(),
            LogicalPlan::SetProperty(s) => s.child.output_schema(),
            LogicalPlan::Delete(d) => d.child.output_schema(),

            LogicalPlan::Union(u) => {
                if let Some(first) = u.children.first() {
                    first.output_schema()
                } else {
                    Vec::new()
                }
            }

            LogicalPlan::Empty(e) => {
                (0..e.num_columns)
                    .map(|i| (SmolStr::new(format!("_col{i}")), LogicalType::Any))
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_node_output_schema() {
        let scan = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![
                (SmolStr::new("p"), LogicalType::Node),
                (SmolStr::new("p.name"), LogicalType::String),
            ],
        });
        let schema = scan.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].0.as_str(), "p");
        assert_eq!(schema[1].1, LogicalType::String);
    }

    #[test]
    fn filter_preserves_child_schema() {
        let child = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        });
        let plan = LogicalPlan::Filter(Box::new(LogicalFilter {
            child,
            predicate: BoundExpression::Literal {
                value: kyu_types::TypedValue::Bool(true),
                result_type: LogicalType::Bool,
            },
        }));
        assert_eq!(plan.output_schema().len(), 1);
    }

    #[test]
    fn projection_output_schema() {
        let child = LogicalPlan::Empty(LogicalEmpty { num_columns: 0 });
        let plan = LogicalPlan::Projection(Box::new(LogicalProjection {
            child,
            expressions: vec![
                BoundExpression::Literal {
                    value: kyu_types::TypedValue::Int64(1),
                    result_type: LogicalType::Int64,
                },
                BoundExpression::Literal {
                    value: kyu_types::TypedValue::String(SmolStr::new("hi")),
                    result_type: LogicalType::String,
                },
            ],
            aliases: vec![SmolStr::new("a"), SmolStr::new("b")],
        }));
        let schema = plan.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0], (SmolStr::new("a"), LogicalType::Int64));
        assert_eq!(schema[1], (SmolStr::new("b"), LogicalType::String));
    }

    #[test]
    fn hash_join_combines_schemas() {
        let left = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("a"), LogicalType::Node)],
        });
        let right = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(1),
            variable_index: Some(1),
            output_columns: vec![(SmolStr::new("b"), LogicalType::Node)],
        });
        let plan = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: left,
            probe: right,
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let schema = plan.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].0.as_str(), "a");
        assert_eq!(schema[1].0.as_str(), "b");
    }

    #[test]
    fn cross_product_combines_schemas() {
        let left = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        });
        let right = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(1),
            variable_index: Some(1),
            output_columns: vec![(SmolStr::new("y"), LogicalType::String)],
        });
        let plan = LogicalPlan::CrossProduct(Box::new(LogicalCrossProduct { left, right }));
        assert_eq!(plan.output_schema().len(), 2);
    }

    #[test]
    fn aggregate_output_schema() {
        let child = LogicalPlan::Empty(LogicalEmpty { num_columns: 0 });
        let plan = LogicalPlan::Aggregate(Box::new(LogicalAggregate {
            child,
            group_by: vec![BoundExpression::Literal {
                value: kyu_types::TypedValue::String(SmolStr::new("x")),
                result_type: LogicalType::String,
            }],
            aggregates: vec![AggregateSpec {
                function_name: SmolStr::new("count"),
                resolved_func: AggFunc::Count,
                arg: None,
                distinct: false,
                result_type: LogicalType::Int64,
                alias: SmolStr::new("cnt"),
            }],
            group_by_aliases: vec![SmolStr::new("key")],
        }));
        let schema = plan.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].0.as_str(), "key");
        assert_eq!(schema[1].0.as_str(), "cnt");
    }

    #[test]
    fn limit_preserves_schema() {
        let child = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        });
        let plan = LogicalPlan::Limit(Box::new(LogicalLimit {
            child,
            skip: None,
            limit: Some(10),
        }));
        assert_eq!(plan.output_schema().len(), 1);
    }

    #[test]
    fn distinct_preserves_schema() {
        let child = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        });
        let plan = LogicalPlan::Distinct(Box::new(LogicalDistinct { child }));
        assert_eq!(plan.output_schema().len(), 1);
    }

    #[test]
    fn unwind_extends_schema() {
        let child = LogicalPlan::Empty(LogicalEmpty { num_columns: 0 });
        let plan = LogicalPlan::Unwind(Box::new(LogicalUnwind {
            child,
            expression: BoundExpression::Literal {
                value: kyu_types::TypedValue::Null,
                result_type: LogicalType::Any,
            },
            variable_index: 0,
            element_type: LogicalType::Int64,
            alias: SmolStr::new("elem"),
        }));
        let schema = plan.output_schema();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].0.as_str(), "elem");
    }

    #[test]
    fn empty_schema() {
        let plan = LogicalPlan::Empty(LogicalEmpty { num_columns: 3 });
        assert_eq!(plan.output_schema().len(), 3);
    }
}
