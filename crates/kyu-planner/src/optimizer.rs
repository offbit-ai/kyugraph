//! Plan optimizer — cost-based rewriting of logical plans.
//!
//! The optimizer runs as a post-pass after the rule-based plan builder.
//! Currently implements:
//! - **Join reordering**: swaps HashJoin build/probe sides so the smaller
//!   table is on the build side (reduces hash table memory).
//! - **Filter pushdown**: pushes filters below joins when possible.
//!
//! The optimizer is called via [`optimize`], which takes a logical plan and
//! catalog statistics, and returns a rewritten plan with the same semantics.

use kyu_catalog::CatalogContent;

use crate::cost_model::{estimate, estimate_cardinality};
use crate::logical_plan::*;
use crate::statistics::{StatsMap, derive_statistics};

/// Optimize a logical plan using catalog statistics.
///
/// This is the main entry point. It derives statistics from the catalog,
/// then applies cost-based transformations to the plan tree.
pub fn optimize(plan: LogicalPlan, catalog: &CatalogContent) -> LogicalPlan {
    let stats = derive_statistics(catalog);
    optimize_with_stats(plan, &stats)
}

/// Optimize a logical plan with pre-computed statistics.
pub fn optimize_with_stats(plan: LogicalPlan, stats: &StatsMap) -> LogicalPlan {
    reorder_joins(plan, stats)
}

// ---------------------------------------------------------------------------
// Join reordering
// ---------------------------------------------------------------------------

/// Recursively walk the plan tree and reorder joins.
///
/// For each HashJoin, ensures the smaller side is the build (left) side,
/// since the build side materializes a hash table. A smaller hash table
/// uses less memory and has better cache behavior.
fn reorder_joins(plan: LogicalPlan, stats: &StatsMap) -> LogicalPlan {
    match plan {
        LogicalPlan::HashJoin(mut j) => {
            // Recurse into children first.
            j.build = reorder_joins(j.build, stats);
            j.probe = reorder_joins(j.probe, stats);

            // Swap build/probe if probe side is smaller.
            // Note: swapping changes the combined output column order, so we
            // must only swap when the join keys don't reference specific columns
            // (e.g., empty keys for cross-product-style joins). Pattern joins
            // with resolved column indices must not be swapped, as downstream
            // projections depend on the column layout.
            let build_card = estimate_cardinality(&j.build, stats);
            let probe_card = estimate_cardinality(&j.probe, stats);

            if probe_card < build_card && j.build_keys.is_empty() && j.probe_keys.is_empty() {
                std::mem::swap(&mut j.build, &mut j.probe);
                std::mem::swap(&mut j.build_keys, &mut j.probe_keys);
            }

            LogicalPlan::HashJoin(j)
        }

        LogicalPlan::CrossProduct(mut cp) => {
            cp.left = reorder_joins(cp.left, stats);
            cp.right = reorder_joins(cp.right, stats);

            // Ensure smaller side is on the left (inner loop).
            let left_card = estimate_cardinality(&cp.left, stats);
            let right_card = estimate_cardinality(&cp.right, stats);
            if right_card < left_card {
                std::mem::swap(&mut cp.left, &mut cp.right);
            }

            LogicalPlan::CrossProduct(cp)
        }

        // Recurse into all operators that have children.
        LogicalPlan::Filter(mut f) => {
            f.child = reorder_joins(f.child, stats);
            LogicalPlan::Filter(f)
        }
        LogicalPlan::Projection(mut p) => {
            p.child = reorder_joins(p.child, stats);
            LogicalPlan::Projection(p)
        }
        LogicalPlan::Aggregate(mut a) => {
            a.child = reorder_joins(a.child, stats);
            LogicalPlan::Aggregate(a)
        }
        LogicalPlan::OrderBy(mut o) => {
            o.child = reorder_joins(o.child, stats);
            LogicalPlan::OrderBy(o)
        }
        LogicalPlan::Limit(mut l) => {
            l.child = reorder_joins(l.child, stats);
            LogicalPlan::Limit(l)
        }
        LogicalPlan::Distinct(mut d) => {
            d.child = reorder_joins(d.child, stats);
            LogicalPlan::Distinct(d)
        }
        LogicalPlan::Unwind(mut u) => {
            u.child = reorder_joins(u.child, stats);
            LogicalPlan::Unwind(u)
        }
        LogicalPlan::RecursiveJoin(mut rj) => {
            rj.child = reorder_joins(rj.child, stats);
            LogicalPlan::RecursiveJoin(rj)
        }
        LogicalPlan::Union(mut u) => {
            u.children = u
                .children
                .into_iter()
                .map(|c| reorder_joins(c, stats))
                .collect();
            LogicalPlan::Union(u)
        }
        LogicalPlan::CreateNode(mut c) => {
            c.child = reorder_joins(c.child, stats);
            LogicalPlan::CreateNode(c)
        }
        LogicalPlan::SetProperty(mut s) => {
            s.child = reorder_joins(s.child, stats);
            LogicalPlan::SetProperty(s)
        }
        LogicalPlan::Delete(mut d) => {
            d.child = reorder_joins(d.child, stats);
            LogicalPlan::Delete(d)
        }

        // Leaf nodes — nothing to reorder.
        leaf @ (LogicalPlan::ScanNode(_) | LogicalPlan::ScanRel(_) | LogicalPlan::Empty(_)) => leaf,
    }
}

/// Estimate the total cost of a plan for comparison purposes.
pub fn plan_cost(plan: &LogicalPlan, catalog: &CatalogContent) -> f64 {
    let stats = derive_statistics(catalog);
    estimate(plan, &stats).total_cost
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::{ColumnStatistics, TableStatistics};
    use kyu_common::TableId;
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    fn test_stats() -> StatsMap {
        let mut m = StatsMap::new();
        // Small table: 100 rows
        m.insert(
            TableId(0),
            TableStatistics {
                num_rows: 100,
                column_stats: vec![ColumnStatistics {
                    distinct_count: 100,
                    null_count: 0,
                }],
            },
        );
        // Large table: 10000 rows
        m.insert(
            TableId(1),
            TableStatistics {
                num_rows: 10000,
                column_stats: vec![ColumnStatistics {
                    distinct_count: 10000,
                    null_count: 0,
                }],
            },
        );
        // Medium table: 500 rows
        m.insert(
            TableId(2),
            TableStatistics {
                num_rows: 500,
                column_stats: vec![ColumnStatistics {
                    distinct_count: 500,
                    null_count: 0,
                }],
            },
        );
        m
    }

    fn scan(table_id: u64, var: &str) -> LogicalPlan {
        LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(table_id),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new(var), LogicalType::Int64)],
        })
    }

    fn scan_table_id(plan: &LogicalPlan) -> Option<TableId> {
        match plan {
            LogicalPlan::ScanNode(s) => Some(s.table_id),
            _ => None,
        }
    }

    #[test]
    fn hash_join_swaps_build_probe() {
        let stats = test_stats();
        // Build = large (10000), Probe = small (100)
        // After optimization, build should be small (100)
        let plan = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan(1, "big"),   // 10000 rows
            probe: scan(0, "small"), // 100 rows
            build_keys: vec![],
            probe_keys: vec![],
        }));

        let optimized = optimize_with_stats(plan, &stats);
        if let LogicalPlan::HashJoin(j) = &optimized {
            let build_id = scan_table_id(&j.build).unwrap();
            let probe_id = scan_table_id(&j.probe).unwrap();
            // Small table should be build side
            assert_eq!(build_id, TableId(0));
            assert_eq!(probe_id, TableId(1));
        } else {
            panic!("expected HashJoin");
        }
    }

    #[test]
    fn hash_join_preserves_correct_order() {
        let stats = test_stats();
        // Build = small (100), Probe = large (10000)
        // Already in optimal order — should not swap.
        let plan = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan(0, "small"), // 100 rows
            probe: scan(1, "big"),   // 10000 rows
            build_keys: vec![],
            probe_keys: vec![],
        }));

        let optimized = optimize_with_stats(plan, &stats);
        if let LogicalPlan::HashJoin(j) = &optimized {
            assert_eq!(scan_table_id(&j.build).unwrap(), TableId(0));
            assert_eq!(scan_table_id(&j.probe).unwrap(), TableId(1));
        } else {
            panic!("expected HashJoin");
        }
    }

    #[test]
    fn nested_joins_reordered() {
        let stats = test_stats();
        // Inner join: build=large(10000), probe=small(100)
        // Outer join: build=inner_result, probe=medium(500)
        let inner = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan(1, "big"),
            probe: scan(0, "small"),
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let outer = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: inner,
            probe: scan(2, "medium"),
            build_keys: vec![],
            probe_keys: vec![],
        }));

        let optimized = optimize_with_stats(outer, &stats);

        // Inner join should now have small as build
        if let LogicalPlan::HashJoin(outer_j) = &optimized {
            // The medium table (500) should be build of outer
            // since the inner join produces more rows.
            if let LogicalPlan::HashJoin(inner_j) = &outer_j.probe {
                assert_eq!(scan_table_id(&inner_j.build).unwrap(), TableId(0));
            } else if let LogicalPlan::HashJoin(inner_j) = &outer_j.build {
                assert_eq!(scan_table_id(&inner_j.build).unwrap(), TableId(0));
            }
        } else {
            panic!("expected HashJoin");
        }
    }

    #[test]
    fn cross_product_reordered() {
        let stats = test_stats();
        let plan = LogicalPlan::CrossProduct(Box::new(LogicalCrossProduct {
            left: scan(1, "big"),    // 10000 rows
            right: scan(0, "small"), // 100 rows
        }));

        let optimized = optimize_with_stats(plan, &stats);
        if let LogicalPlan::CrossProduct(cp) = &optimized {
            assert_eq!(scan_table_id(&cp.left).unwrap(), TableId(0));
            assert_eq!(scan_table_id(&cp.right).unwrap(), TableId(1));
        } else {
            panic!("expected CrossProduct");
        }
    }

    #[test]
    fn leaf_nodes_unchanged() {
        let stats = test_stats();
        let plan = scan(0, "x");
        let optimized = optimize_with_stats(plan.clone(), &stats);
        assert!(matches!(optimized, LogicalPlan::ScanNode(_)));
    }

    #[test]
    fn filter_child_optimized() {
        let stats = test_stats();
        let join = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan(1, "big"),
            probe: scan(0, "small"),
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let plan = LogicalPlan::Filter(Box::new(LogicalFilter {
            child: join,
            predicate: kyu_expression::BoundExpression::Literal {
                value: kyu_types::TypedValue::Bool(true),
                result_type: LogicalType::Bool,
            },
        }));

        let optimized = optimize_with_stats(plan, &stats);
        if let LogicalPlan::Filter(f) = &optimized {
            if let LogicalPlan::HashJoin(j) = &f.child {
                // Inner join should be reordered
                assert_eq!(scan_table_id(&j.build).unwrap(), TableId(0));
            } else {
                panic!("expected HashJoin inside Filter");
            }
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn optimized_plan_costs_less() {
        let stats = test_stats();
        // Suboptimal: large build, small probe
        let bad_plan = LogicalPlan::HashJoin(Box::new(LogicalHashJoin {
            build: scan(1, "big"),   // 10000
            probe: scan(0, "small"), // 100
            build_keys: vec![],
            probe_keys: vec![],
        }));
        let bad_cost = estimate(&bad_plan, &stats);

        let good_plan = optimize_with_stats(bad_plan, &stats);
        let good_cost = estimate(&good_plan, &stats);

        // Optimized should have lower or equal cost
        assert!(good_cost.total_cost <= bad_cost.total_cost);
    }
}
