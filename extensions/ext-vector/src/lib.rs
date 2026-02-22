//! ext-vector: HNSW vector index with SIMD distance kernels.
//!
//! Provides approximate nearest neighbor search through three procedures:
//! - `vector.build(dim, metric?)` → initializes HNSW index (metric: "l2" or "cosine")
//! - `vector.add(id, vector_csv)` → inserts a vector, returns `{status: STRING}`
//! - `vector.search(query_csv, k)` → ANN results `{id: INT64, distance: DOUBLE}`
//!
//! Uses NEON intrinsics on aarch64 and scalar fallback (auto-vectorized to AVX2) elsewhere.
//! Staging buffer accumulates vectors before bulk-inserting into the live HNSW index.

pub mod distance;
pub mod hnsw;
pub mod staging;

use std::collections::HashMap;
use std::sync::Mutex;

use kyu_extension::{Extension, ProcColumn, ProcParam, ProcRow, ProcedureSignature};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::distance::DistanceMetric;
use crate::hnsw::{HnswConfig, HnswIndex};
use crate::staging::StagingBuffer;

/// Vector search state: optional HNSW index + staging buffer.
struct VectorState {
    index: Option<HnswIndex>,
    staging: StagingBuffer,
    metric: DistanceMetric,
    next_id: usize,
}

impl Default for VectorState {
    fn default() -> Self {
        Self {
            index: None,
            staging: StagingBuffer::new(),
            metric: DistanceMetric::L2,
            next_id: 0,
        }
    }
}

/// Vector search extension.
pub struct VectorExtension {
    state: Mutex<VectorState>,
}

impl VectorExtension {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(VectorState::default()),
        }
    }
}

impl Default for VectorExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Extension for VectorExtension {
    fn name(&self) -> &str {
        "vector"
    }

    fn needs_graph(&self) -> bool {
        false
    }

    fn procedures(&self) -> Vec<ProcedureSignature> {
        vec![
            ProcedureSignature {
                name: "build".into(),
                params: vec![
                    ProcParam {
                        name: "dim".into(),
                        type_desc: "INT64".into(),
                    },
                    ProcParam {
                        name: "metric".into(),
                        type_desc: "STRING".into(),
                    },
                ],
                columns: vec![ProcColumn {
                    name: "status".into(),
                    data_type: LogicalType::String,
                }],
            },
            ProcedureSignature {
                name: "add".into(),
                params: vec![
                    ProcParam {
                        name: "id".into(),
                        type_desc: "INT64".into(),
                    },
                    ProcParam {
                        name: "vector_csv".into(),
                        type_desc: "STRING".into(),
                    },
                ],
                columns: vec![ProcColumn {
                    name: "status".into(),
                    data_type: LogicalType::String,
                }],
            },
            ProcedureSignature {
                name: "search".into(),
                params: vec![
                    ProcParam {
                        name: "query_csv".into(),
                        type_desc: "STRING".into(),
                    },
                    ProcParam {
                        name: "k".into(),
                        type_desc: "INT64".into(),
                    },
                ],
                columns: vec![
                    ProcColumn {
                        name: "id".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "distance".into(),
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
        _adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<ProcRow>, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| format!("lock error: {e}"))?;

        match procedure {
            "build" => {
                let dim: usize = args
                    .first()
                    .ok_or("vector.build requires dim argument")?
                    .parse()
                    .map_err(|_| "dim must be a positive integer")?;
                if dim == 0 {
                    return Err("dim must be > 0".into());
                }

                let metric = match args.get(1).map(|s| s.to_lowercase()).as_deref() {
                    Some("cosine") => DistanceMetric::Cosine,
                    _ => DistanceMetric::L2,
                };

                state.index = Some(HnswIndex::new(dim, HnswConfig {
                    metric,
                    ..HnswConfig::default()
                }));
                state.staging = StagingBuffer::new();
                state.metric = metric;
                state.next_id = 0;

                Ok(vec![vec![TypedValue::String(SmolStr::new(format!(
                    "built dim={dim} metric={metric:?}"
                )))]])
            }

            "add" => {
                let VectorState { index, staging, next_id, .. } = &mut *state;
                let index = index.as_mut().ok_or("call vector.build first")?;

                let ext_id: usize = args
                    .first()
                    .ok_or("vector.add requires id argument")?
                    .parse()
                    .map_err(|_| "id must be a non-negative integer")?;

                let csv = args.get(1).ok_or("vector.add requires vector_csv argument")?;
                let vector: Vec<f32> = csv
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<f32>()
                            .map_err(|_| format!("invalid float in vector: '{}'", s.trim()))
                    })
                    .collect::<Result<_, _>>()?;

                let needs_flush = staging.add(ext_id, vector);
                if needs_flush {
                    staging.flush(index);
                }

                *next_id = (*next_id).max(ext_id + 1);

                Ok(vec![vec![TypedValue::String(SmolStr::new("ok"))]])
            }

            "search" => {
                let VectorState { index, staging, .. } = &mut *state;
                let index = index.as_mut().ok_or("call vector.build first")?;

                let csv = args.first().ok_or("vector.search requires query_csv argument")?;
                let query: Vec<f32> = csv
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<f32>()
                            .map_err(|_| format!("invalid float in query: '{}'", s.trim()))
                    })
                    .collect::<Result<_, _>>()?;

                let k: usize = args
                    .get(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);

                // Flush pending before search for consistency.
                if staging.pending_count() > 0 {
                    staging.flush(index);
                }

                let results = index.search(&query, k, k.max(50));

                Ok(results
                    .into_iter()
                    .map(|(id, dist)| {
                        vec![
                            TypedValue::Int64(id as i64),
                            TypedValue::Double(dist as f64),
                        ]
                    })
                    .collect())
            }

            _ => Err(format!("unknown procedure: {procedure}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_adj() -> HashMap<i64, Vec<(i64, f64)>> {
        HashMap::new()
    }

    #[test]
    fn extension_metadata() {
        let ext = VectorExtension::new();
        assert_eq!(ext.name(), "vector");
        assert!(!ext.needs_graph());
        assert_eq!(ext.procedures().len(), 3);
    }

    #[test]
    fn build_add_search() {
        let ext = VectorExtension::new();
        let adj = empty_adj();

        // Build index.
        let result = ext.execute("build", &["3".into(), "l2".into()], &adj).unwrap();
        assert_eq!(result.len(), 1);

        // Add vectors.
        ext.execute("add", &["0".into(), "1.0,0.0,0.0".into()], &adj).unwrap();
        ext.execute("add", &["1".into(), "0.0,1.0,0.0".into()], &adj).unwrap();
        ext.execute("add", &["2".into(), "0.9,0.1,0.0".into()], &adj).unwrap();

        // Search.
        let results = ext.execute("search", &["1.0,0.0,0.0".into(), "2".into()], &adj).unwrap();
        assert!(!results.is_empty());
        // Nearest should be vector 0 (identical).
        assert_eq!(results[0][0], TypedValue::Int64(0));
    }

    #[test]
    fn search_without_build() {
        let ext = VectorExtension::new();
        let adj = empty_adj();
        let result = ext.execute("search", &["1.0,0.0".into(), "5".into()], &adj);
        assert!(result.is_err());
    }

    #[test]
    fn add_without_build() {
        let ext = VectorExtension::new();
        let adj = empty_adj();
        let result = ext.execute("add", &["0".into(), "1.0,0.0".into()], &adj);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_procedure() {
        let ext = VectorExtension::new();
        let adj = empty_adj();
        assert!(ext.execute("nonexistent", &[], &adj).is_err());
    }

    #[test]
    fn cosine_search() {
        let ext = VectorExtension::new();
        let adj = empty_adj();

        ext.execute("build", &["3".into(), "cosine".into()], &adj).unwrap();
        ext.execute("add", &["0".into(), "1.0,0.0,0.0".into()], &adj).unwrap();
        ext.execute("add", &["1".into(), "0.0,1.0,0.0".into()], &adj).unwrap();
        ext.execute("add", &["2".into(), "0.9,0.1,0.0".into()], &adj).unwrap();

        let results = ext.execute("search", &["1.0,0.0,0.0".into(), "3".into()], &adj).unwrap();
        assert_eq!(results.len(), 3);
        // Vector 0 should be closest (cosine distance ~0).
        assert_eq!(results[0][0], TypedValue::Int64(0));
        if let TypedValue::Double(d) = results[0][1] {
            assert!(d < 0.01, "cosine distance to identical = {d}");
        }
    }
}
