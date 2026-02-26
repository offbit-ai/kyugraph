//! RDF data model types for the Linked Data → Property Graph mapping.

use indexmap::IndexMap;
use kyu_types::{LogicalType, TypedValue};

/// A parsed RDF triple.
#[derive(Clone, Debug)]
pub struct Triple {
    /// Subject URI.
    pub subject: String,
    /// Predicate URI.
    pub predicate: String,
    /// Object (URI or literal).
    pub object: RdfObject,
}

/// The object of an RDF triple.
#[derive(Clone, Debug)]
pub enum RdfObject {
    /// A URI reference (hyperlink to another entity).
    Uri(String),
    /// A literal value with optional XSD datatype and language tag.
    Literal {
        value: String,
        datatype: Option<String>,
        lang: Option<String>,
    },
}

/// Inferred property graph schema from RDF triples.
#[derive(Clone, Debug)]
pub struct RdfSchema {
    pub node_tables: Vec<RdfNodeTable>,
    pub rel_tables: Vec<RdfRelTable>,
    pub prefixes: Vec<(String, String)>,
}

/// A node table inferred from rdf:type + literal properties.
#[derive(Clone, Debug)]
pub struct RdfNodeTable {
    /// Table name (local name of rdf:type, e.g., "Person").
    pub name: String,
    /// Full type URI.
    pub type_uri: String,
    /// Ordered property columns: predicate local name → LogicalType.
    pub properties: IndexMap<String, LogicalType>,
    /// Rows: (subject_uri, property_values ordered by `properties` keys).
    pub rows: Vec<(String, Vec<TypedValue>)>,
}

/// A relationship table inferred from URI-to-URI predicates.
#[derive(Clone, Debug)]
pub struct RdfRelTable {
    /// Relationship name (predicate local name, e.g., "knows").
    pub name: String,
    /// Full predicate URI.
    pub predicate_uri: String,
    /// Source node table name.
    pub from_table: String,
    /// Target node table name.
    pub to_table: String,
    /// Edges: (source_uri, target_uri).
    pub edges: Vec<(String, String)>,
}

/// Extract the local name from a URI (after last `/` or `#`).
pub fn local_name(uri: &str) -> &str {
    uri.rsplit_once('#')
        .or_else(|| uri.rsplit_once('/'))
        .map(|(_, name)| name)
        .unwrap_or(uri)
}

/// Well-known RDF namespace.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// XSD namespace prefix.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// Infer a LogicalType from an XSD datatype URI.
pub fn xsd_to_logical_type(datatype: Option<&str>) -> LogicalType {
    match datatype {
        Some(dt) => {
            let local = dt.strip_prefix(XSD).unwrap_or(dt);
            match local {
                "integer" | "int" | "long" | "short" | "byte" | "nonNegativeInteger"
                | "positiveInteger" | "nonPositiveInteger" | "negativeInteger" | "unsignedLong"
                | "unsignedInt" | "unsignedShort" | "unsignedByte" => LogicalType::Int64,
                "float" => LogicalType::Float,
                "double" | "decimal" => LogicalType::Double,
                "boolean" => LogicalType::Bool,
                _ => LogicalType::String,
            }
        }
        None => LogicalType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_name() {
        assert_eq!(local_name("http://xmlns.com/foaf/0.1/Person"), "Person");
        assert_eq!(
            local_name("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "type"
        );
        assert_eq!(local_name("Person"), "Person");
    }

    #[test]
    fn test_xsd_to_logical_type() {
        assert_eq!(
            xsd_to_logical_type(Some("http://www.w3.org/2001/XMLSchema#integer")),
            LogicalType::Int64
        );
        assert_eq!(
            xsd_to_logical_type(Some("http://www.w3.org/2001/XMLSchema#double")),
            LogicalType::Double
        );
        assert_eq!(
            xsd_to_logical_type(Some("http://www.w3.org/2001/XMLSchema#boolean")),
            LogicalType::Bool
        );
        assert_eq!(xsd_to_logical_type(None), LogicalType::String);
    }
}
