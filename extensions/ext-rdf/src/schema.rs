//! Schema inference: two-pass algorithm mapping RDF triples to property graph tables.
//!
//! Pass 1: Classify triples (types, literal properties, URI links).
//! Pass 2: Build node tables with columns, rel tables with edges, assemble row data.

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use kyu_common::KyuResult;
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::model::{
    RDF_TYPE, RdfNodeTable, RdfObject, RdfRelTable, RdfSchema, Triple, local_name,
    xsd_to_logical_type,
};

type PropVec<'a> = Vec<(&'a str, &'a str, Option<&'a str>)>;
type RelKey = (String, String, String);

/// Infer a property graph schema from a set of RDF triples.
///
/// Follows Linked Data Principles:
/// - `rdf:type` → node table assignment
/// - Literal-valued predicates → node properties (columns)
/// - URI-valued predicates → relationships (edges)
/// - URI serves as primary key on every node table
pub fn infer_schema(triples: &[Triple]) -> KyuResult<RdfSchema> {
    // ── Pass 1: classify triples ──

    // subject URI → type local name (e.g., "Person")
    let mut type_map: HashMap<&str, String> = HashMap::new();
    // subject URI → [(predicate_local_name, literal_value, datatype)]
    let mut subject_props: HashMap<&str, PropVec<'_>> = HashMap::new();
    // (subject, predicate_local_name, predicate_uri, object_uri)
    let mut links: Vec<(&str, String, &str, &str)> = Vec::new();
    // Collect all subjects that are referenced as objects (to ensure they get tables).
    let mut referenced_subjects: HashSet<&str> = HashSet::new();

    for triple in triples {
        if triple.predicate == RDF_TYPE {
            if let RdfObject::Uri(ref type_uri) = triple.object {
                type_map.insert(&triple.subject, local_name(type_uri).to_string());
            }
        } else {
            match &triple.object {
                RdfObject::Literal {
                    value,
                    datatype,
                    lang: _,
                } => {
                    subject_props.entry(&triple.subject).or_default().push((
                        local_name(&triple.predicate),
                        value.as_str(),
                        datatype.as_deref(),
                    ));
                }
                RdfObject::Uri(obj_uri) => {
                    links.push((
                        &triple.subject,
                        local_name(&triple.predicate).to_string(),
                        &triple.predicate,
                        obj_uri.as_str(),
                    ));
                    referenced_subjects.insert(obj_uri.as_str());
                }
            }
        }
    }

    // Assign untyped subjects that appear in links to "Resource" default table.
    let all_link_subjects: HashSet<&str> = links.iter().flat_map(|(s, _, _, o)| [*s, *o]).collect();
    for subj in &all_link_subjects {
        if !type_map.contains_key(*subj) {
            type_map.insert(*subj, "Resource".to_string());
        }
    }
    // Also assign untyped subjects with properties.
    for subj in subject_props.keys() {
        if !type_map.contains_key(*subj) {
            type_map.insert(*subj, "Resource".to_string());
        }
    }

    // ── Pass 2: build schema ──

    // Collect property columns per table type.
    // table_name → IndexMap<prop_name, LogicalType>
    let mut table_columns: HashMap<&str, IndexMap<String, LogicalType>> = HashMap::new();
    // table_name → type_uri
    let mut table_type_uris: HashMap<&str, String> = HashMap::new();

    // Register type URIs from rdf:type triples.
    for triple in triples {
        if triple.predicate == RDF_TYPE
            && let RdfObject::Uri(ref type_uri) = triple.object
        {
            let name = local_name(type_uri);
            table_type_uris
                .entry(
                    type_map
                        .get(triple.subject.as_str())
                        .map(|s| s.as_str())
                        .unwrap_or(name),
                )
                .or_insert_with(|| type_uri.clone());
        }
    }

    // Infer column types from literal properties.
    for (subj, props) in &subject_props {
        let table_name = match type_map.get(*subj) {
            Some(name) => name.as_str(),
            None => continue,
        };
        let columns = table_columns.entry(table_name).or_default();
        for (prop_name, _value, datatype) in props {
            let inferred = xsd_to_logical_type(*datatype);
            columns.entry(prop_name.to_string()).or_insert(inferred);
        }
    }

    // Build node tables with row data.
    let mut node_table_map: HashMap<&str, RdfNodeTable> = HashMap::new();

    // First, ensure all table names exist.
    let all_table_names: HashSet<&str> = type_map.values().map(|s| s.as_str()).collect();
    for table_name in &all_table_names {
        node_table_map
            .entry(table_name)
            .or_insert_with(|| RdfNodeTable {
                name: table_name.to_string(),
                type_uri: table_type_uris.get(table_name).cloned().unwrap_or_default(),
                properties: table_columns.get(table_name).cloned().unwrap_or_default(),
                rows: Vec::new(),
            });
    }

    // Assemble rows: group subjects by table, fill property values.
    let mut subjects_by_table: HashMap<&str, Vec<&str>> = HashMap::new();
    for (subj, table_name) in &type_map {
        subjects_by_table
            .entry(table_name.as_str())
            .or_default()
            .push(*subj);
    }

    for (table_name, subjects) in &subjects_by_table {
        let node_table = node_table_map.get_mut(table_name).unwrap();
        let col_names: Vec<String> = node_table.properties.keys().cloned().collect();
        let col_types: Vec<LogicalType> = node_table.properties.values().cloned().collect();

        for subj in subjects {
            let mut values = vec![TypedValue::Null; col_names.len()];
            if let Some(props) = subject_props.get(subj) {
                for (prop_name, value, datatype) in props {
                    if let Some(col_idx) = col_names.iter().position(|n| n == prop_name) {
                        values[col_idx] = parse_literal(value, &col_types[col_idx], *datatype);
                    }
                }
            }
            node_table.rows.push((subj.to_string(), values));
        }
    }

    // Build rel tables: group links by (from_table, predicate, to_table).
    // Key: (pred_local_name, from_table, to_table)
    let mut rel_map: HashMap<RelKey, (String, Vec<(String, String)>)> = HashMap::new();

    for (src, pred_name, pred_uri, dst) in &links {
        let src_table = match type_map.get(*src) {
            Some(t) => t.clone(),
            None => continue,
        };
        let dst_table = match type_map.get(*dst) {
            Some(t) => t.clone(),
            None => continue,
        };
        let key = (pred_name.clone(), src_table, dst_table);
        let entry = rel_map
            .entry(key)
            .or_insert_with(|| (pred_uri.to_string(), Vec::new()));
        entry.1.push((src.to_string(), dst.to_string()));
    }

    let mut rel_tables: Vec<RdfRelTable> = rel_map
        .into_iter()
        .map(|((name, from, to), (pred_uri, edges))| RdfRelTable {
            name,
            predicate_uri: pred_uri,
            from_table: from,
            to_table: to,
            edges,
        })
        .collect();
    rel_tables.sort_by(|a, b| a.name.cmp(&b.name));

    let mut node_tables: Vec<RdfNodeTable> = node_table_map.into_values().collect();
    node_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(RdfSchema {
        node_tables,
        rel_tables,
        prefixes: Vec::new(),
    })
}

/// Parse a literal string into a TypedValue based on the target LogicalType.
fn parse_literal(value: &str, ty: &LogicalType, _datatype: Option<&str>) -> TypedValue {
    match ty {
        LogicalType::Int64 => value
            .parse::<i64>()
            .map(TypedValue::Int64)
            .unwrap_or(TypedValue::String(SmolStr::new(value))),
        LogicalType::Float => value
            .parse::<f32>()
            .map(TypedValue::Float)
            .unwrap_or(TypedValue::String(SmolStr::new(value))),
        LogicalType::Double => value
            .parse::<f64>()
            .map(TypedValue::Double)
            .unwrap_or(TypedValue::String(SmolStr::new(value))),
        LogicalType::Bool => match value.to_lowercase().as_str() {
            "true" | "1" => TypedValue::Bool(true),
            "false" | "0" => TypedValue::Bool(false),
            _ => TypedValue::String(SmolStr::new(value)),
        },
        _ => TypedValue::String(SmolStr::new(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_triple(s: &str, p: &str, o: RdfObject) -> Triple {
        Triple {
            subject: s.to_string(),
            predicate: p.to_string(),
            object: o,
        }
    }

    fn uri(s: &str) -> RdfObject {
        RdfObject::Uri(s.to_string())
    }

    fn lit(v: &str) -> RdfObject {
        RdfObject::Literal {
            value: v.to_string(),
            datatype: None,
            lang: None,
        }
    }

    fn typed_lit(v: &str, dt: &str) -> RdfObject {
        RdfObject::Literal {
            value: v.to_string(),
            datatype: Some(dt.to_string()),
            lang: None,
        }
    }

    #[test]
    fn test_basic_schema_inference() {
        let triples = vec![
            make_triple("http://ex.org/alice", RDF_TYPE, uri("http://ex.org/Person")),
            make_triple("http://ex.org/bob", RDF_TYPE, uri("http://ex.org/Person")),
            make_triple("http://ex.org/alice", "http://ex.org/name", lit("Alice")),
            make_triple("http://ex.org/bob", "http://ex.org/name", lit("Bob")),
            make_triple(
                "http://ex.org/alice",
                "http://ex.org/age",
                typed_lit("30", "http://www.w3.org/2001/XMLSchema#integer"),
            ),
            make_triple(
                "http://ex.org/alice",
                "http://ex.org/knows",
                uri("http://ex.org/bob"),
            ),
        ];

        let schema = infer_schema(&triples).unwrap();

        assert_eq!(schema.node_tables.len(), 1);
        assert_eq!(schema.node_tables[0].name, "Person");
        assert_eq!(schema.node_tables[0].properties.len(), 2); // name, age
        assert_eq!(schema.node_tables[0].rows.len(), 2);

        assert_eq!(schema.rel_tables.len(), 1);
        assert_eq!(schema.rel_tables[0].name, "knows");
        assert_eq!(schema.rel_tables[0].from_table, "Person");
        assert_eq!(schema.rel_tables[0].to_table, "Person");
        assert_eq!(schema.rel_tables[0].edges.len(), 1);
    }

    #[test]
    fn test_multi_type_schema() {
        let triples = vec![
            make_triple("http://ex.org/alice", RDF_TYPE, uri("http://ex.org/Person")),
            make_triple("http://ex.org/tokyo", RDF_TYPE, uri("http://ex.org/City")),
            make_triple("http://ex.org/alice", "http://ex.org/name", lit("Alice")),
            make_triple("http://ex.org/tokyo", "http://ex.org/name", lit("Tokyo")),
            make_triple(
                "http://ex.org/alice",
                "http://ex.org/livesIn",
                uri("http://ex.org/tokyo"),
            ),
        ];

        let schema = infer_schema(&triples).unwrap();

        assert_eq!(schema.node_tables.len(), 2); // City, Person (sorted)
        assert_eq!(schema.rel_tables.len(), 1);
        assert_eq!(schema.rel_tables[0].name, "livesIn");
        assert_eq!(schema.rel_tables[0].from_table, "Person");
        assert_eq!(schema.rel_tables[0].to_table, "City");
    }
}
