//! ext-algo: Graph algorithm extension providing PageRank, WCC, and Betweenness Centrality.
//!
//! Registers procedures under the `algo` namespace:
//! - `algo.pageRank(damping, max_iterations, tolerance)` → `{node_id, rank}`
//! - `algo.wcc()` → `{node_id, component}`
//! - `algo.betweenness()` → `{node_id, centrality}`

pub mod betweenness;
pub mod pagerank;
pub mod wcc;

use std::collections::HashMap;

use kyu_extension::{Extension, ProcColumn, ProcParam, ProcRow, ProcedureSignature};
use kyu_types::{LogicalType, TypedValue};

/// The graph algorithm extension.
pub struct AlgoExtension;

impl Extension for AlgoExtension {
    fn name(&self) -> &str {
        "algo"
    }

    fn needs_graph(&self) -> bool {
        true
    }

    fn procedures(&self) -> Vec<ProcedureSignature> {
        vec![
            ProcedureSignature {
                name: "pageRank".into(),
                params: vec![
                    ProcParam {
                        name: "damping".into(),
                        type_desc: "DOUBLE".into(),
                    },
                    ProcParam {
                        name: "max_iterations".into(),
                        type_desc: "INT64".into(),
                    },
                    ProcParam {
                        name: "tolerance".into(),
                        type_desc: "DOUBLE".into(),
                    },
                ],
                columns: vec![
                    ProcColumn {
                        name: "node_id".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "rank".into(),
                        data_type: LogicalType::Double,
                    },
                ],
            },
            ProcedureSignature {
                name: "wcc".into(),
                params: vec![],
                columns: vec![
                    ProcColumn {
                        name: "node_id".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "component".into(),
                        data_type: LogicalType::Int64,
                    },
                ],
            },
            ProcedureSignature {
                name: "betweenness".into(),
                params: vec![],
                columns: vec![
                    ProcColumn {
                        name: "node_id".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "centrality".into(),
                        data_type: LogicalType::Double,
                    },
                ],
            },
        ]
    }

    fn execute(
        &self,
        procedure: &str,
        args: &[String],
        adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<ProcRow>, String> {
        match procedure {
            "pageRank" => {
                let damping = args
                    .first()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.85);
                let max_iter = args
                    .get(1)
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(20);
                let tolerance = args
                    .get(2)
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1e-6);

                let ranks = pagerank::pagerank(adjacency, damping, max_iter, tolerance);
                let mut rows: Vec<ProcRow> = ranks
                    .into_iter()
                    .map(|(node_id, rank)| {
                        vec![TypedValue::Int64(node_id), TypedValue::Double(rank)]
                    })
                    .collect();
                rows.sort_by(|a, b| match (&a[1], &b[1]) {
                    (TypedValue::Double(ra), TypedValue::Double(rb)) => {
                        rb.partial_cmp(ra).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => std::cmp::Ordering::Equal,
                });
                Ok(rows)
            }
            "wcc" => {
                let components = wcc::wcc(adjacency);
                let mut rows: Vec<ProcRow> = components
                    .into_iter()
                    .map(|(node_id, component)| {
                        vec![TypedValue::Int64(node_id), TypedValue::Int64(component)]
                    })
                    .collect();
                rows.sort_by_key(|r| match r[0] {
                    TypedValue::Int64(id) => id,
                    _ => 0,
                });
                Ok(rows)
            }
            "betweenness" => {
                let scores = betweenness::betweenness_centrality(adjacency);
                let mut rows: Vec<ProcRow> = scores
                    .into_iter()
                    .map(|(node_id, centrality)| {
                        vec![TypedValue::Int64(node_id), TypedValue::Double(centrality)]
                    })
                    .collect();
                rows.sort_by(|a, b| match (&a[1], &b[1]) {
                    (TypedValue::Double(ca), TypedValue::Double(cb)) => {
                        cb.partial_cmp(ca).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => std::cmp::Ordering::Equal,
                });
                Ok(rows)
            }
            _ => Err(format!("unknown procedure: {procedure}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_graph() -> HashMap<i64, Vec<(i64, f64)>> {
        let mut adj = HashMap::new();
        // 1 -> 2, 2 -> 3, 3 -> 1 (cycle)
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![(1, 1.0)]);
        adj
    }

    #[test]
    fn extension_metadata() {
        let ext = AlgoExtension;
        assert_eq!(ext.name(), "algo");
        assert_eq!(ext.procedures().len(), 3);
    }

    #[test]
    fn execute_pagerank() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let rows = ext
            .execute(
                "pageRank",
                &["0.85".into(), "20".into(), "1e-6".into()],
                &adj,
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        // All rows should have 2 columns: node_id (Int64) and rank (Double).
        for row in &rows {
            assert_eq!(row.len(), 2);
            assert!(matches!(row[0], TypedValue::Int64(_)));
            assert!(matches!(row[1], TypedValue::Double(_)));
        }
    }

    #[test]
    fn execute_pagerank_defaults() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let rows = ext.execute("pageRank", &[], &adj).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn execute_wcc() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let rows = ext.execute("wcc", &[], &adj).unwrap();
        assert_eq!(rows.len(), 3);
        // All nodes in same component.
        let comp = &rows[0][1];
        assert!(rows.iter().all(|r| &r[1] == comp));
    }

    #[test]
    fn execute_betweenness() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let rows = ext.execute("betweenness", &[], &adj).unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(row[1], TypedValue::Double(_)));
        }
    }

    #[test]
    fn execute_unknown_procedure() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let result = ext.execute("nonexistent", &[], &adj);
        assert!(result.is_err());
    }

    #[test]
    fn wcc_two_components() {
        let ext = AlgoExtension;
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(1, 1.0)]);
        adj.insert(10, vec![(11, 1.0)]);
        adj.insert(11, vec![(10, 1.0)]);
        let rows = ext.execute("wcc", &[], &adj).unwrap();
        assert_eq!(rows.len(), 4);
        // Build a map of node_id -> component.
        let comp_map: HashMap<i64, i64> = rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (TypedValue::Int64(id), TypedValue::Int64(comp)) => (*id, *comp),
                _ => panic!("unexpected types"),
            })
            .collect();
        assert_eq!(comp_map[&1], comp_map[&2]);
        assert_eq!(comp_map[&10], comp_map[&11]);
        assert_ne!(comp_map[&1], comp_map[&10]);
    }
}
