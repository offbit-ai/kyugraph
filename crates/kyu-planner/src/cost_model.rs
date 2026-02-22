//! Cost model for logical plan nodes.
//!
//! Estimates cardinality (number of output rows) and cost (abstract units
//! combining CPU and I/O) for each logical operator. Used by the optimizer
//! to compare alternative plans.

use kyu_common::TableId;

use crate::logical_plan::*;
use crate::statistics::{row_count, StatsMap};

/// Estimated cost of a logical plan node.
#[derive(Clone, Debug)]
pub struct PlanCost {
    /// Estimated number of output rows.
    pub cardinality: f64,
    /// Estimated total cost (abstract units).
    pub total_cost: f64,
}

// ---------------------------------------------------------------------------
// Cost constants
// ---------------------------------------------------------------------------

/// Cost of reading one row from a table scan.
const SEQ_SCAN_COST_PER_ROW: f64 = 1.0;
/// Cost of a hash table lookup (probe side).
const HASH_PROBE_COST_PER_ROW: f64 = 1.2;
/// Cost of inserting one row into a hash table (build side).
const HASH_BUILD_COST_PER_ROW: f64 = 1.5;
/// Cost of evaluating a filter predicate per row.
const FILTER_COST_PER_ROW: f64 = 0.1;
/// Cost of sorting per row (n log n factor applied separately).
const SORT_COST_PER_ROW: f64 = 2.0;
/// Cost of hashing for aggregate per row.
const AGG_COST_PER_ROW: f64 = 1.5;

// ---------------------------------------------------------------------------
// Selectivity heuristics
// ---------------------------------------------------------------------------

/// Default selectivity for an equality predicate: 1/NDV.
/// When NDV is unknown, assume 10%.
const DEFAULT_EQUALITY_SELECTIVITY: f64 = 0.1;

/// Default selectivity for a range predicate (>, <, BETWEEN).
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;

/// Default selectivity for other filters (LIKE, CONTAINS, etc.).
const DEFAULT_OTHER_SELECTIVITY: f64 = 0.25;

/// Estimate selectivity of a filter predicate.
///
/// This is a heuristic-based estimation. With real column statistics,
/// we could do much better (e.g., histogram-based estimates).
fn estimate_filter_selectivity(predicate: &kyu_expression::BoundExpression) -> f64 {
    use kyu_expression::BoundExpression;
    use kyu_parser::ast::{BinaryOp, UnaryOp};
    match predicate {
        BoundExpression::Comparison { .. } => DEFAULT_EQUALITY_SELECTIVITY,
        BoundExpression::BinaryOp { op, left, right, .. } => match op {
            BinaryOp::And => {
                estimate_filter_selectivity(left) * estimate_filter_selectivity(right)
            }
            BinaryOp::Or => {
                let s1 = estimate_filter_selectivity(left);
                let s2 = estimate_filter_selectivity(right);
                1.0 - (1.0 - s1) * (1.0 - s2)
            }
            _ => DEFAULT_OTHER_SELECTIVITY,
        },
        BoundExpression::UnaryOp { op: UnaryOp::Not, operand, .. } => {
            1.0 - estimate_filter_selectivity(operand)
        }
        BoundExpression::IsNull { .. } => 0.05,
        BoundExpression::InList { list, .. } => {
            (list.len() as f64 * DEFAULT_EQUALITY_SELECTIVITY).min(0.9)
        }
        BoundExpression::StringOp { .. } => DEFAULT_RANGE_SELECTIVITY,
        BoundExpression::Literal { value, .. } => {
            use kyu_types::TypedValue;
            match value {
                TypedValue::Bool(true) => 1.0,
                TypedValue::Bool(false) => 0.0,
                _ => DEFAULT_OTHER_SELECTIVITY,
            }
        }
        _ => DEFAULT_OTHER_SELECTIVITY,
    }
}

// ---------------------------------------------------------------------------
// Cardinality and cost estimation
// ---------------------------------------------------------------------------

/// Estimate the cardinality and cost of a logical plan tree.
pub fn estimate(plan: &LogicalPlan, stats: &StatsMap) -> PlanCost {
    match plan {
        LogicalPlan::ScanNode(scan) => estimate_scan_node(scan, stats),
        LogicalPlan::ScanRel(scan) => estimate_scan_rel(scan, stats),
        LogicalPlan::Filter(f) => estimate_filter(f, stats),
        LogicalPlan::Projection(p) => estimate_projection(p, stats),
        LogicalPlan::HashJoin(j) => estimate_hash_join(j, stats),
        LogicalPlan::CrossProduct(cp) => estimate_cross_product(cp, stats),
        LogicalPlan::Aggregate(a) => estimate_aggregate(a, stats),
        LogicalPlan::OrderBy(o) => estimate_order_by(o, stats),
        LogicalPlan::Limit(l) => estimate_limit(l, stats),
        LogicalPlan::Distinct(d) => estimate_distinct(d, stats),
        LogicalPlan::Unwind(u) => estimate_unwind(u, stats),
        LogicalPlan::RecursiveJoin(rj) => estimate_recursive_join(rj, stats),
        LogicalPlan::Union(u) => estimate_union(u, stats),
        LogicalPlan::CreateNode(_) | LogicalPlan::SetProperty(_) | LogicalPlan::Delete(_) => {
            PlanCost {
                cardinality: 0.0,
                total_cost: 0.0,
            }
        }
        LogicalPlan::Empty(_) => PlanCost {
            cardinality: 1.0,
            total_cost: 0.0,
        },
    }
}

fn estimate_scan_node(scan: &LogicalScanNode, stats: &StatsMap) -> PlanCost {
    let rows = row_count(stats, scan.table_id);
    PlanCost {
        cardinality: rows,
        total_cost: rows * SEQ_SCAN_COST_PER_ROW,
    }
}

fn estimate_scan_rel(scan: &LogicalScanRel, stats: &StatsMap) -> PlanCost {
    let rows = row_count(stats, scan.table_id);
    PlanCost {
        cardinality: rows,
        total_cost: rows * SEQ_SCAN_COST_PER_ROW,
    }
}

fn estimate_filter(f: &LogicalFilter, stats: &StatsMap) -> PlanCost {
    let child = estimate(&f.child, stats);
    let sel = estimate_filter_selectivity(&f.predicate);
    let out_rows = (child.cardinality * sel).max(1.0);
    PlanCost {
        cardinality: out_rows,
        total_cost: child.total_cost + child.cardinality * FILTER_COST_PER_ROW,
    }
}

fn estimate_projection(p: &LogicalProjection, stats: &StatsMap) -> PlanCost {
    let child = estimate(&p.child, stats);
    PlanCost {
        cardinality: child.cardinality,
        total_cost: child.total_cost, // projection is essentially free
    }
}

fn estimate_hash_join(j: &LogicalHashJoin, stats: &StatsMap) -> PlanCost {
    let build = estimate(&j.build, stats);
    let probe = estimate(&j.probe, stats);
    // For equi-join on a key, output ≈ larger side (graph joins typically match most rows).
    // Use a conservative estimate: min(build * probe / max(build, probe), build + probe).
    let max_card = build.cardinality.max(probe.cardinality).max(1.0);
    let out_rows = (build.cardinality * probe.cardinality / max_card).max(1.0);
    let cost = build.total_cost
        + probe.total_cost
        + build.cardinality * HASH_BUILD_COST_PER_ROW
        + probe.cardinality * HASH_PROBE_COST_PER_ROW;
    PlanCost {
        cardinality: out_rows,
        total_cost: cost,
    }
}

fn estimate_cross_product(cp: &LogicalCrossProduct, stats: &StatsMap) -> PlanCost {
    let left = estimate(&cp.left, stats);
    let right = estimate(&cp.right, stats);
    PlanCost {
        cardinality: left.cardinality * right.cardinality,
        total_cost: left.total_cost + right.total_cost + left.cardinality * right.cardinality,
    }
}

fn estimate_aggregate(a: &LogicalAggregate, stats: &StatsMap) -> PlanCost {
    let child = estimate(&a.child, stats);
    // Output rows ≈ number of groups. Heuristic: min(child rows, product of group-by NDVs).
    let num_groups = if a.group_by.is_empty() {
        1.0
    } else {
        // Assume each group-by column has moderate selectivity.
        (child.cardinality / 10.0).max(1.0)
    };
    PlanCost {
        cardinality: num_groups,
        total_cost: child.total_cost + child.cardinality * AGG_COST_PER_ROW,
    }
}

fn estimate_order_by(o: &LogicalOrderBy, stats: &StatsMap) -> PlanCost {
    let child = estimate(&o.child, stats);
    let n = child.cardinality.max(1.0);
    let sort_cost = n * n.log2().max(1.0) * SORT_COST_PER_ROW;
    PlanCost {
        cardinality: child.cardinality,
        total_cost: child.total_cost + sort_cost,
    }
}

fn estimate_limit(l: &LogicalLimit, stats: &StatsMap) -> PlanCost {
    let child = estimate(&l.child, stats);
    let skip = l.skip.unwrap_or(0) as f64;
    let limit = l.limit.map(|l| l as f64).unwrap_or(child.cardinality);
    let out_rows = (child.cardinality - skip).max(0.0).min(limit);
    PlanCost {
        cardinality: out_rows,
        total_cost: child.total_cost, // still must produce child rows up to skip+limit
    }
}

fn estimate_distinct(d: &LogicalDistinct, stats: &StatsMap) -> PlanCost {
    let child = estimate(&d.child, stats);
    // Assume ~80% of rows are distinct.
    let out_rows = (child.cardinality * 0.8).max(1.0);
    PlanCost {
        cardinality: out_rows,
        total_cost: child.total_cost + child.cardinality * AGG_COST_PER_ROW,
    }
}

fn estimate_unwind(u: &LogicalUnwind, stats: &StatsMap) -> PlanCost {
    let child = estimate(&u.child, stats);
    // Assume average list length of 5.
    PlanCost {
        cardinality: child.cardinality * 5.0,
        total_cost: child.total_cost + child.cardinality * 5.0,
    }
}

fn estimate_recursive_join(rj: &LogicalRecursiveJoin, stats: &StatsMap) -> PlanCost {
    let child = estimate(&rj.child, stats);
    let rel_rows = row_count(stats, rj.rel_table_id);
    let dest_rows = row_count(stats, rj.dest_table_id);
    // Heuristic: average fan-out per hop ≈ rel_rows / dest_rows, clamped.
    let fan_out = (rel_rows / dest_rows.max(1.0)).clamp(1.0, 100.0);
    let avg_hops = ((rj.min_hops + rj.max_hops) as f64) / 2.0;
    let out_rows = child.cardinality * fan_out.powf(avg_hops);
    let cost = child.total_cost + out_rows * HASH_PROBE_COST_PER_ROW;
    PlanCost {
        cardinality: out_rows.min(1e12), // cap at 1 trillion
        total_cost: cost,
    }
}

fn estimate_union(u: &LogicalUnion, stats: &StatsMap) -> PlanCost {
    let mut total_card = 0.0;
    let mut total_cost = 0.0;
    for child in &u.children {
        let c = estimate(child, stats);
        total_card += c.cardinality;
        total_cost += c.total_cost;
    }
    if !u.all {
        // UNION (not ALL) deduplicates.
        total_card *= 0.8;
        total_cost += total_card * AGG_COST_PER_ROW;
    }
    PlanCost {
        cardinality: total_card.max(0.0),
        total_cost,
    }
}

/// Convenience: estimate just the cardinality.
pub fn estimate_cardinality(plan: &LogicalPlan, stats: &StatsMap) -> f64 {
    estimate(plan, stats).cardinality
}

/// Convenience: estimate just the total cost.
pub fn estimate_cost(plan: &LogicalPlan, stats: &StatsMap) -> f64 {
    estimate(plan, stats).total_cost
}

/// Extract the table ID from a leaf scan node, if any.
pub fn scan_table_id(plan: &LogicalPlan) -> Option<TableId> {
    match plan {
        LogicalPlan::ScanNode(s) => Some(s.table_id),
        LogicalPlan::ScanRel(s) => Some(s.table_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::{ColumnStatistics, StatsMap, TableStatistics};
    use kyu_expression::BoundExpression;
    use kyu_parser::ast::{BinaryOp, ComparisonOp};
    use kyu_types::{LogicalType, TypedValue};
    use smol_str::SmolStr;

    fn test_stats() -> StatsMap {
        let mut m = StatsMap::new();
        m.insert(
            TableId(0),
            TableStatistics {
                num_rows: 1000,
                column_stats: vec![
                    ColumnStatistics {
                        distinct_count: 1000,
                        null_count: 0,
                    },
                    ColumnStatistics {
                        distinct_count: 100,
                        null_count: 0,
                    },
                ],
            },
        );
        m.insert(
            TableId(1),
            TableStatistics {
                num_rows: 5000,
                column_stats: vec![ColumnStatistics {
                    distinct_count: 500,
                    null_count: 0,
                }],
            },
        );
        m.insert(
            TableId(2),
            TableStatistics {
                num_rows: 100,
                column_stats: vec![ColumnStatistics {
                    distinct_count: 100,
                    null_count: 0,
                }],
            },
        );
        m
    }

    fn scan_node(table_id: u64) -> LogicalPlan {
        LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(table_id),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        })
    }

    #[test]
    fn scan_cost() {
        let stats = test_stats();
        let plan = scan_node(0);
        let cost = estimate(&plan, &stats);
        assert_eq!(cost.cardinality, 1000.0);
        assert_eq!(cost.total_cost, 1000.0);
    }

    #[test]
    fn filter_reduces_cardinality() {
        let stats = test_stats();
        let plan = LogicalPlan::Filter(Box::new(LogicalFilter {
            child: scan_node(0),
            predicate: BoundExpression::Comparison {
                op: ComparisonOp::Eq,
                left: Box::new(BoundExpression::Variable {
                    index: 0,
                    result_type: LogicalType::Int64,
                }),
                right: Box::new(BoundExpression::Literal {
                    value: TypedValue::Int64(42),
                    result_type: LogicalType::Int64,
                }),
            },
        }));
        let cost = estimate(&plan, &stats);
        assert!(cost.cardinality < 1000.0);
        assert!(cost.total_cost > 1000.0); // scan + filter overhead
    }

    #[test]
    fn hash_join_cost() {
        let stats = test_stats();
        let plan = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan_node(0),
            probe: scan_node(1),
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let cost = estimate(&plan, &stats);
        // Join output should be > 0
        assert!(cost.cardinality > 0.0);
        // Cost should include build + probe overhead
        assert!(cost.total_cost > 6000.0);
    }

    #[test]
    fn cross_product_is_expensive() {
        let stats = test_stats();
        let join = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan_node(0),
            probe: scan_node(2),
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let cross = LogicalPlan::CrossProduct(Box::new(LogicalCrossProduct {
            left: scan_node(0),
            right: scan_node(2),
        }));
        let join_cost = estimate(&join, &stats);
        let cross_cost = estimate(&cross, &stats);
        // Cross product should be much more expensive
        assert!(cross_cost.total_cost > join_cost.total_cost);
    }

    #[test]
    fn limit_reduces_cardinality() {
        let stats = test_stats();
        let plan = LogicalPlan::Limit(Box::new(LogicalLimit {
            child: scan_node(0),
            skip: None,
            limit: Some(10),
        }));
        let cost = estimate(&plan, &stats);
        assert_eq!(cost.cardinality, 10.0);
    }

    #[test]
    fn order_by_adds_sort_cost() {
        let stats = test_stats();
        let child_cost = estimate(&scan_node(0), &stats);
        let plan = LogicalPlan::OrderBy(Box::new(LogicalOrderBy {
            child: scan_node(0),
            order_by: vec![],
        }));
        let cost = estimate(&plan, &stats);
        assert!(cost.total_cost > child_cost.total_cost);
    }

    #[test]
    fn aggregate_reduces_rows() {
        let stats = test_stats();
        let plan = LogicalPlan::Aggregate(Box::new(LogicalAggregate {
            child: scan_node(0),
            group_by: vec![BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::String,
            }],
            aggregates: vec![],
            group_by_aliases: vec![SmolStr::new("key")],
        }));
        let cost = estimate(&plan, &stats);
        assert!(cost.cardinality < 1000.0);
    }

    #[test]
    fn empty_plan_cost() {
        let stats = test_stats();
        let plan = LogicalPlan::Empty(LogicalEmpty { num_columns: 1 });
        let cost = estimate(&plan, &stats);
        assert_eq!(cost.cardinality, 1.0);
        assert_eq!(cost.total_cost, 0.0);
    }

    #[test]
    fn and_selectivity() {
        let s = estimate_filter_selectivity(&BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(BoundExpression::Comparison {
                op: ComparisonOp::Eq,
                left: Box::new(BoundExpression::Variable {
                    index: 0,
                    result_type: LogicalType::Int64,
                }),
                right: Box::new(BoundExpression::Literal {
                    value: TypedValue::Int64(1),
                    result_type: LogicalType::Int64,
                }),
            }),
            right: Box::new(BoundExpression::Comparison {
                op: ComparisonOp::Eq,
                left: Box::new(BoundExpression::Variable {
                    index: 1,
                    result_type: LogicalType::Int64,
                }),
                right: Box::new(BoundExpression::Literal {
                    value: TypedValue::Int64(2),
                    result_type: LogicalType::Int64,
                }),
            }),
            result_type: LogicalType::Bool,
        });
        assert!((s - 0.01).abs() < 0.001);
    }
}
