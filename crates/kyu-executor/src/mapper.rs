//! Plan mapper — translates LogicalPlan → PhysicalOperator (1:1 in Phase 6).

use kyu_common::{KyuError, KyuResult};
use kyu_planner::*;

use crate::operators::*;
use crate::physical_plan::PhysicalOperator;

/// If all expressions are simple Variable references with unique indices,
/// return the column indices in projection order. Otherwise return None.
fn all_variable_indices(exprs: &[kyu_expression::BoundExpression]) -> Option<Vec<usize>> {
    let mut indices = Vec::with_capacity(exprs.len());
    for expr in exprs {
        match expr {
            kyu_expression::BoundExpression::Variable { index, .. } => {
                indices.push(*index as usize);
            }
            _ => return None,
        }
    }
    // Check uniqueness (duplicate column refs need projection to clone)
    let mut seen = std::collections::HashSet::new();
    if !indices.iter().all(|i| seen.insert(*i)) {
        return None;
    }
    Some(indices)
}

/// Map a logical plan tree to a physical operator tree.
pub fn map_plan(logical: &LogicalPlan) -> KyuResult<PhysicalOperator> {
    match logical {
        LogicalPlan::ScanNode(scan) => Ok(PhysicalOperator::ScanNode(ScanNodeOp::new(
            scan.table_id,
        ))),

        LogicalPlan::ScanRel(scan) => {
            // For now, relationship scans use the same ScanNodeOp with the rel table id.
            Ok(PhysicalOperator::ScanNode(ScanNodeOp::new(scan.table_id)))
        }

        LogicalPlan::Filter(f) => {
            let child = map_plan(&f.child)?;
            Ok(PhysicalOperator::Filter(FilterOp::new(
                child,
                f.predicate.clone(),
            )))
        }

        LogicalPlan::Projection(p) => {
            let child = map_plan(&p.child)?;
            // Column pruning: when projecting only Variable refs over a ScanNode,
            // push column selection into the scan and eliminate the projection.
            if let PhysicalOperator::ScanNode(mut scan) = child {
                if let Some(indices) = all_variable_indices(&p.expressions) {
                    scan.column_indices = Some(indices);
                    return Ok(PhysicalOperator::ScanNode(scan));
                }
                // Not all variables — keep projection
                return Ok(PhysicalOperator::Projection(ProjectionOp::new(
                    PhysicalOperator::ScanNode(scan),
                    p.expressions.clone(),
                )));
            }
            Ok(PhysicalOperator::Projection(ProjectionOp::new(
                child,
                p.expressions.clone(),
            )))
        }

        LogicalPlan::HashJoin(j) => {
            let build = map_plan(&j.build)?;
            let probe = map_plan(&j.probe)?;
            Ok(PhysicalOperator::HashJoin(HashJoinOp::new(
                build,
                probe,
                j.build_keys.clone(),
                j.probe_keys.clone(),
            )))
        }

        LogicalPlan::CrossProduct(cp) => {
            let left = map_plan(&cp.left)?;
            let right = map_plan(&cp.right)?;
            Ok(PhysicalOperator::CrossProduct(CrossProductOp::new(
                left, right,
            )))
        }

        LogicalPlan::Aggregate(a) => {
            let child = map_plan(&a.child)?;
            Ok(PhysicalOperator::Aggregate(AggregateOp::new(
                child,
                a.group_by.clone(),
                a.aggregates.clone(),
            )))
        }

        LogicalPlan::OrderBy(o) => {
            let child = map_plan(&o.child)?;
            Ok(PhysicalOperator::OrderBy(OrderByOp::new(
                child,
                o.order_by.clone(),
            )))
        }

        LogicalPlan::Limit(l) => {
            let child = map_plan(&l.child)?;
            Ok(PhysicalOperator::Limit(LimitOp::new(
                child,
                l.skip.unwrap_or(0),
                l.limit.unwrap_or(u64::MAX),
            )))
        }

        LogicalPlan::Distinct(d) => {
            let child = map_plan(&d.child)?;
            Ok(PhysicalOperator::Distinct(DistinctOp::new(child)))
        }

        LogicalPlan::Unwind(u) => {
            let child = map_plan(&u.child)?;
            Ok(PhysicalOperator::Unwind(UnwindOp::new(
                child,
                u.expression.clone(),
            )))
        }

        LogicalPlan::Empty(e) => Ok(PhysicalOperator::Empty(EmptyOp::new(e.num_columns))),

        LogicalPlan::Union(u) => {
            // Execute union as: first child, then second child, etc.
            // For now, return NotImplemented for multi-child union.
            if u.children.len() == 1 {
                map_plan(&u.children[0])
            } else {
                Err(KyuError::NotImplemented("UNION execution".into()))
            }
        }

        LogicalPlan::CreateNode(_) | LogicalPlan::SetProperty(_) | LogicalPlan::Delete(_) => {
            Err(KyuError::NotImplemented(
                "mutation operators not yet executable".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_common::id::TableId;
    use kyu_expression::BoundExpression;
    use kyu_types::{LogicalType, TypedValue};
    use smol_str::SmolStr;

    #[test]
    fn map_empty() {
        let plan = LogicalPlan::Empty(LogicalEmpty { num_columns: 2 });
        let op = map_plan(&plan).unwrap();
        assert!(matches!(op, PhysicalOperator::Empty(_)));
    }

    #[test]
    fn map_scan_node() {
        let plan = LogicalPlan::ScanNode(LogicalScanNode {
            table_id: TableId(0),
            variable_index: Some(0),
            output_columns: vec![(SmolStr::new("x"), LogicalType::Int64)],
        });
        let op = map_plan(&plan).unwrap();
        assert!(matches!(op, PhysicalOperator::ScanNode(_)));
    }

    #[test]
    fn map_projection() {
        let plan = LogicalPlan::Projection(Box::new(LogicalProjection {
            child: LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }),
            expressions: vec![BoundExpression::Literal {
                value: TypedValue::Int64(1),
                result_type: LogicalType::Int64,
            }],
            aliases: vec![SmolStr::new("x")],
        }));
        let op = map_plan(&plan).unwrap();
        assert!(matches!(op, PhysicalOperator::Projection(_)));
    }

    #[test]
    fn map_filter() {
        let plan = LogicalPlan::Filter(Box::new(LogicalFilter {
            child: LogicalPlan::Empty(LogicalEmpty { num_columns: 1 }),
            predicate: BoundExpression::Literal {
                value: TypedValue::Bool(true),
                result_type: LogicalType::Bool,
            },
        }));
        let op = map_plan(&plan).unwrap();
        assert!(matches!(op, PhysicalOperator::Filter(_)));
    }

    #[test]
    fn map_limit() {
        let plan = LogicalPlan::Limit(Box::new(LogicalLimit {
            child: LogicalPlan::Empty(LogicalEmpty { num_columns: 0 }),
            skip: Some(1),
            limit: Some(10),
        }));
        let op = map_plan(&plan).unwrap();
        assert!(matches!(op, PhysicalOperator::Limit(_)));
    }
}
