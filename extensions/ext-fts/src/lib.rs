//! ext-fts: Full-text search extension via tantivy.
//!
//! Provides BM25-ranked full-text search through three procedures:
//! - `fts.add(content)` → indexes a document, returns `{doc_id: INT64}`
//! - `fts.search(query, limit?)` → BM25 ranked results `{doc_id, score, snippet}`
//! - `fts.clear()` → resets the index
//!
//! Uses an in-memory tantivy index with batched commits (every 1000 documents).
//! The index is thread-safe via `Mutex<FtsIndex>`.

pub mod index;

use std::collections::HashMap;
use std::sync::Mutex;

use kyu_extension::{Extension, ProcColumn, ProcParam, ProcRow, ProcedureSignature};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::index::FtsIndex;

/// Full-text search extension.
pub struct FtsExtension {
    state: Mutex<FtsIndex>,
}

impl FtsExtension {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FtsIndex::new()),
        }
    }
}

impl Default for FtsExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Extension for FtsExtension {
    fn name(&self) -> &str {
        "fts"
    }

    fn needs_graph(&self) -> bool {
        false
    }

    fn procedures(&self) -> Vec<ProcedureSignature> {
        vec![
            ProcedureSignature {
                name: "add".into(),
                params: vec![ProcParam {
                    name: "content".into(),
                    type_desc: "STRING".into(),
                }],
                columns: vec![ProcColumn {
                    name: "doc_id".into(),
                    data_type: LogicalType::Int64,
                }],
            },
            ProcedureSignature {
                name: "search".into(),
                params: vec![
                    ProcParam {
                        name: "query".into(),
                        type_desc: "STRING".into(),
                    },
                    ProcParam {
                        name: "limit".into(),
                        type_desc: "INT64".into(),
                    },
                ],
                columns: vec![
                    ProcColumn {
                        name: "doc_id".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "score".into(),
                        data_type: LogicalType::Double,
                    },
                    ProcColumn {
                        name: "snippet".into(),
                        data_type: LogicalType::String,
                    },
                ],
            },
            ProcedureSignature {
                name: "clear".into(),
                params: vec![],
                columns: vec![ProcColumn {
                    name: "status".into(),
                    data_type: LogicalType::String,
                }],
            },
        ]
    }

    fn execute(
        &self,
        procedure: &str,
        args: &[String],
        _adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    ) -> Result<Vec<ProcRow>, String> {
        let mut index = self
            .state
            .lock()
            .map_err(|e| format!("lock error: {e}"))?;

        match procedure {
            "add" => {
                let content = args
                    .first()
                    .ok_or("fts.add requires a content argument")?;
                let doc_id = index.add_document(content).map_err(|e| e.to_string())?;
                Ok(vec![vec![TypedValue::Int64(doc_id as i64)]])
            }
            "search" => {
                let query = args
                    .first()
                    .ok_or("fts.search requires a query argument")?;
                let limit = args
                    .get(1)
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(10);
                let results = index.search(query, limit).map_err(|e| e.to_string())?;
                Ok(results
                    .into_iter()
                    .map(|(doc_id, score, snippet)| {
                        vec![
                            TypedValue::Int64(doc_id as i64),
                            TypedValue::Double(score as f64),
                            TypedValue::String(SmolStr::new(snippet)),
                        ]
                    })
                    .collect())
            }
            "clear" => {
                index.clear().map_err(|e| e.to_string())?;
                Ok(vec![vec![TypedValue::String(SmolStr::new("ok"))]])
            }
            _ => Err(format!("unknown procedure: {procedure}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_metadata() {
        let ext = FtsExtension::new();
        assert_eq!(ext.name(), "fts");
        assert!(!ext.needs_graph());
        assert_eq!(ext.procedures().len(), 3);
    }

    #[test]
    fn execute_add_and_search() {
        let ext = FtsExtension::new();
        let empty = HashMap::new();

        // Add a document.
        let rows = ext
            .execute("add", &["the quick brown fox".into()], &empty)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], TypedValue::Int64(0));

        // Search for it.
        let results = ext
            .execute("search", &["fox".into(), "10".into()], &empty)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], TypedValue::Int64(0)); // doc_id
        assert!(matches!(results[0][1], TypedValue::Double(s) if s > 0.0)); // score > 0
    }

    #[test]
    fn execute_search_no_results() {
        let ext = FtsExtension::new();
        let empty = HashMap::new();

        ext.execute("add", &["hello world".into()], &empty)
            .unwrap();

        let results = ext
            .execute("search", &["quantum".into(), "10".into()], &empty)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn execute_clear() {
        let ext = FtsExtension::new();
        let empty = HashMap::new();

        ext.execute("add", &["testing clear".into()], &empty)
            .unwrap();
        let clear_result = ext.execute("clear", &[], &empty).unwrap();
        assert_eq!(clear_result[0][0], TypedValue::String(SmolStr::new("ok")));

        // After clear, search returns nothing.
        let results = ext
            .execute("search", &["testing".into(), "10".into()], &empty)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn execute_unknown_procedure() {
        let ext = FtsExtension::new();
        let empty = HashMap::new();
        assert!(ext.execute("nonexistent", &[], &empty).is_err());
    }

    #[test]
    fn multiple_documents_ranked() {
        let ext = FtsExtension::new();
        let empty = HashMap::new();

        ext.execute("add", &["python for data science".into()], &empty)
            .unwrap();
        ext.execute("add", &["rust systems programming language".into()], &empty)
            .unwrap();
        ext.execute("add", &["rust rust rust all about rust".into()], &empty)
            .unwrap();

        let results = ext
            .execute("search", &["rust".into(), "10".into()], &empty)
            .unwrap();
        // At least 2 rust documents should match.
        assert!(results.len() >= 2);
        // Python doc should not be in results.
        let doc_ids: Vec<i64> = results
            .iter()
            .map(|r| match r[0] {
                TypedValue::Int64(id) => id,
                _ => panic!("expected Int64"),
            })
            .collect();
        assert!(!doc_ids.contains(&0)); // python doc
    }
}
