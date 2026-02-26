//! ext-rdf: RDF Linked Data import extension for KyuGraph.
//!
//! Provides RDF file parsing (Turtle, N-Triples, N-Quads, RDF/XML) and
//! automatic schema inference following Linked Data Principles:
//!
//! - `rdf:type` → node table assignment
//! - Literal-valued predicates → node properties
//! - URI-valued predicates → relationships (hyperlinks as edges)
//! - URI serves as primary key on every node table
//!
//! ## Library API
//!
//! ```ignore
//! let triples = ext_rdf::parse_triples("foaf.ttl")?;
//! let schema = ext_rdf::infer_schema(&triples)?;
//! ```
//!
//! ## Extension procedures
//!
//! ```cypher
//! CALL rdf.stats('file.ttl')
//! CALL rdf.prefixes('file.ttl')
//! CALL rdf.types('file.ttl')
//! ```

pub mod model;
pub mod parser;
pub mod schema;

use std::collections::HashMap;

use kyu_extension::{Extension, ProcColumn, ProcParam, ProcRow, ProcedureSignature};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

pub use model::{RdfNodeTable, RdfObject, RdfRelTable, RdfSchema, Triple};
pub use parser::parse_triples;
pub use schema::infer_schema;

/// RDF extension providing inspection procedures.
pub struct RdfExtension;

impl RdfExtension {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RdfExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Extension for RdfExtension {
    fn name(&self) -> &str {
        "rdf"
    }

    fn procedures(&self) -> Vec<ProcedureSignature> {
        vec![
            ProcedureSignature {
                name: "stats".into(),
                params: vec![ProcParam {
                    name: "path".into(),
                    type_desc: "STRING".into(),
                }],
                columns: vec![
                    ProcColumn {
                        name: "triple_count".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "subject_count".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "predicate_count".into(),
                        data_type: LogicalType::Int64,
                    },
                    ProcColumn {
                        name: "type_count".into(),
                        data_type: LogicalType::Int64,
                    },
                ],
            },
            ProcedureSignature {
                name: "prefixes".into(),
                params: vec![ProcParam {
                    name: "path".into(),
                    type_desc: "STRING".into(),
                }],
                columns: vec![
                    ProcColumn {
                        name: "prefix".into(),
                        data_type: LogicalType::String,
                    },
                    ProcColumn {
                        name: "namespace".into(),
                        data_type: LogicalType::String,
                    },
                ],
            },
            ProcedureSignature {
                name: "types".into(),
                params: vec![ProcParam {
                    name: "path".into(),
                    type_desc: "STRING".into(),
                }],
                columns: vec![
                    ProcColumn {
                        name: "type_uri".into(),
                        data_type: LogicalType::String,
                    },
                    ProcColumn {
                        name: "local_name".into(),
                        data_type: LogicalType::String,
                    },
                    ProcColumn {
                        name: "count".into(),
                        data_type: LogicalType::Int64,
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
        let path = args.first().ok_or("rdf.* requires a file path argument")?;

        match procedure {
            "stats" => exec_stats(path),
            "prefixes" => exec_prefixes(path),
            "types" => exec_types(path),
            _ => Err(format!("unknown procedure: {procedure}")),
        }
    }
}

fn exec_stats(path: &str) -> Result<Vec<ProcRow>, String> {
    let triples = parse_triples(path).map_err(|e| e.to_string())?;

    let mut subjects = std::collections::HashSet::new();
    let mut predicates = std::collections::HashSet::new();
    let mut type_count = 0u64;

    for t in &triples {
        subjects.insert(&t.subject);
        predicates.insert(&t.predicate);
        if t.predicate == model::RDF_TYPE {
            type_count += 1;
        }
    }

    Ok(vec![vec![
        TypedValue::Int64(triples.len() as i64),
        TypedValue::Int64(subjects.len() as i64),
        TypedValue::Int64(predicates.len() as i64),
        TypedValue::Int64(type_count as i64),
    ]])
}

fn exec_prefixes(path: &str) -> Result<Vec<ProcRow>, String> {
    let prefixes = parser::parse_prefixes(path).map_err(|e| e.to_string())?;
    Ok(prefixes
        .into_iter()
        .map(|(prefix, ns)| {
            vec![
                TypedValue::String(SmolStr::new(prefix)),
                TypedValue::String(SmolStr::new(ns)),
            ]
        })
        .collect())
}

fn exec_types(path: &str) -> Result<Vec<ProcRow>, String> {
    let triples = parse_triples(path).map_err(|e| e.to_string())?;

    let mut type_counts: HashMap<String, i64> = HashMap::new();
    for t in &triples {
        if t.predicate == model::RDF_TYPE
            && let RdfObject::Uri(ref uri) = t.object
        {
            *type_counts.entry(uri.clone()).or_insert(0) += 1;
        }
    }

    let mut rows: Vec<ProcRow> = type_counts
        .into_iter()
        .map(|(uri, count)| {
            let name = model::local_name(&uri).to_string();
            vec![
                TypedValue::String(SmolStr::new(&uri)),
                TypedValue::String(SmolStr::new(name)),
                TypedValue::Int64(count),
            ]
        })
        .collect();
    rows.sort_by(|a, b| {
        let a_count = match &a[2] {
            TypedValue::Int64(n) => *n,
            _ => 0,
        };
        let b_count = match &b[2] {
            TypedValue::Int64(n) => *n,
            _ => 0,
        };
        b_count.cmp(&a_count)
    });

    Ok(rows)
}
