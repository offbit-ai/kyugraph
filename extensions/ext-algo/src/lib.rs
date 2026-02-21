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

/// The graph algorithm extension.
pub struct AlgoExtension;

impl Extension for AlgoExtension {
    fn name(&self) -> &str {
        "algo"
    }

    fn procedures(&self) -> Vec<ProcedureSignature> {
        vec![
            ProcedureSignature {
                name: "pageRank".into(),
                params: vec![
                    ProcParam { name: "damping".into(), type_desc: "DOUBLE".into() },
                    ProcParam { name: "max_iterations".into(), type_desc: "INT64".into() },
                    ProcParam { name: "tolerance".into(), type_desc: "DOUBLE".into() },
                ],
                columns: vec![
                    ProcColumn { name: "node_id".into(), type_desc: "INT64".into() },
                    ProcColumn { name: "rank".into(), type_desc: "DOUBLE".into() },
                ],
            },
            ProcedureSignature {
                name: "wcc".into(),
                params: vec![],
                columns: vec![
                    ProcColumn { name: "node_id".into(), type_desc: "INT64".into() },
                    ProcColumn { name: "component".into(), type_desc: "INT64".into() },
                ],
            },
            ProcedureSignature {
                name: "betweenness".into(),
                params: vec![],
                columns: vec![
                    ProcColumn { name: "node_id".into(), type_desc: "INT64".into() },
                    ProcColumn { name: "centrality".into(), type_desc: "DOUBLE".into() },
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
                let damping = args.first()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.85);
                let max_iter = args.get(1)
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(20);
                let tolerance = args.get(2)
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1e-6);

                let ranks = pagerank::pagerank(adjacency, damping, max_iter, tolerance);
                let mut rows: Vec<ProcRow> = ranks
                    .into_iter()
                    .map(|(node_id, rank)| {
                        let mut row = HashMap::new();
                        row.insert("node_id".into(), node_id.to_string());
                        row.insert("rank".into(), format!("{rank:.10}"));
                        row
                    })
                    .collect();
                rows.sort_by(|a, b| {
                    let ra: f64 = a["rank"].parse().unwrap_or(0.0);
                    let rb: f64 = b["rank"].parse().unwrap_or(0.0);
                    rb.partial_cmp(&ra).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(rows)
            }
            "wcc" => {
                let components = wcc::wcc(adjacency);
                let mut rows: Vec<ProcRow> = components
                    .into_iter()
                    .map(|(node_id, component)| {
                        let mut row = HashMap::new();
                        row.insert("node_id".into(), node_id.to_string());
                        row.insert("component".into(), component.to_string());
                        row
                    })
                    .collect();
                rows.sort_by_key(|r| r["node_id"].parse::<i64>().unwrap_or(0));
                Ok(rows)
            }
            "betweenness" => {
                let scores = betweenness::betweenness_centrality(adjacency);
                let mut rows: Vec<ProcRow> = scores
                    .into_iter()
                    .map(|(node_id, centrality)| {
                        let mut row = HashMap::new();
                        row.insert("node_id".into(), node_id.to_string());
                        row.insert("centrality".into(), format!("{centrality:.10}"));
                        row
                    })
                    .collect();
                rows.sort_by(|a, b| {
                    let ca: f64 = a["centrality"].parse().unwrap_or(0.0);
                    let cb: f64 = b["centrality"].parse().unwrap_or(0.0);
                    cb.partial_cmp(&ca).unwrap_or(std::cmp::Ordering::Equal)
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
        let rows = ext.execute("pageRank", &["0.85".into(), "20".into(), "1e-6".into()], &adj).unwrap();
        assert_eq!(rows.len(), 3);
        // All rows should have node_id and rank columns.
        for row in &rows {
            assert!(row.contains_key("node_id"));
            assert!(row.contains_key("rank"));
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
        let comp = &rows[0]["component"];
        assert!(rows.iter().all(|r| &r["component"] == comp));
    }

    #[test]
    fn execute_betweenness() {
        let ext = AlgoExtension;
        let adj = sample_graph();
        let rows = ext.execute("betweenness", &[], &adj).unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(row.contains_key("centrality"));
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
        // Nodes 1,2 in one component; 10,11 in another.
        let comp_1: HashMap<_, _> = rows.iter().map(|r| (r["node_id"].clone(), r["component"].clone())).collect();
        assert_eq!(comp_1["1"], comp_1["2"]);
        assert_eq!(comp_1["10"], comp_1["11"]);
        assert_ne!(comp_1["1"], comp_1["10"]);
    }
}
