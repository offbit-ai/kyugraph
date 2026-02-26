//! RDF file parsing using oxttl (Turtle/N-Triples/N-Quads) and oxrdfxml (RDF/XML).

use std::fs;
use std::io::BufReader;

use kyu_common::{KyuError, KyuResult};
use oxrdf::vocab::rdf;
use oxttl::{NQuadsParser, NTriplesParser, TurtleParser};

use crate::model::{RdfObject, Triple};

/// Parse an RDF file into a list of triples. Format is auto-detected by extension.
///
/// Supported: `.ttl` (Turtle), `.nt` (N-Triples), `.nq` (N-Quads), `.rdf`/`.owl` (RDF/XML).
pub fn parse_triples(path: &str) -> KyuResult<Vec<Triple>> {
    let lower = path.to_lowercase();
    if lower.ends_with(".nt") {
        parse_ntriples(path)
    } else if lower.ends_with(".nq") {
        parse_nquads(path)
    } else if lower.ends_with(".rdf") || lower.ends_with(".owl") || lower.ends_with(".xml") {
        parse_rdfxml(path)
    } else {
        // Default to Turtle (.ttl and anything else).
        parse_turtle(path)
    }
}

/// Extract namespace prefixes from a Turtle file.
pub fn parse_prefixes(path: &str) -> KyuResult<Vec<(String, String)>> {
    let data = fs::read(path).map_err(|e| KyuError::Copy(format!("cannot read '{path}': {e}")))?;
    let parser = TurtleParser::new().for_reader(data.as_slice());
    let mut prefixes = Vec::new();
    for (prefix, iri) in parser.prefixes() {
        prefixes.push((prefix.to_string(), iri.to_string()));
    }
    // Also parse to collect any prefixes declared later.
    let parser2 = TurtleParser::new().for_reader(data.as_slice());
    for result in parser2 {
        let _ = result; // consume to register prefixes
    }
    Ok(prefixes)
}

fn parse_turtle(path: &str) -> KyuResult<Vec<Triple>> {
    let file =
        fs::File::open(path).map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;
    let reader = BufReader::new(file);
    let parser = TurtleParser::new().for_reader(reader);
    collect_triples(parser, path)
}

fn parse_ntriples(path: &str) -> KyuResult<Vec<Triple>> {
    let file =
        fs::File::open(path).map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;
    let reader = BufReader::new(file);
    let parser = NTriplesParser::new().for_reader(reader);
    collect_triples(parser, path)
}

fn parse_nquads(path: &str) -> KyuResult<Vec<Triple>> {
    let file =
        fs::File::open(path).map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;
    let reader = BufReader::new(file);
    let parser = NQuadsParser::new().for_reader(reader);
    let mut triples = Vec::new();
    for result in parser {
        let quad =
            result.map_err(|e| KyuError::Copy(format!("N-Quads parse error in '{path}': {e}")))?;
        if let Some(triple) = convert_triple(quad.subject, quad.predicate, quad.object) {
            triples.push(triple);
        }
    }
    Ok(triples)
}

fn parse_rdfxml(path: &str) -> KyuResult<Vec<Triple>> {
    let file =
        fs::File::open(path).map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;
    let reader = BufReader::new(file);
    let parser = oxrdfxml::RdfXmlParser::new().for_reader(reader);
    collect_triples(parser, path)
}

/// Collect triples from any parser that yields `Result<oxrdf::Triple, _>`.
fn collect_triples<I, E>(parser: I, path: &str) -> KyuResult<Vec<Triple>>
where
    I: Iterator<Item = Result<oxrdf::Triple, E>>,
    E: std::fmt::Display,
{
    let mut triples = Vec::new();
    for result in parser {
        let t = result.map_err(|e| KyuError::Copy(format!("RDF parse error in '{path}': {e}")))?;
        if let Some(triple) = convert_triple(t.subject, t.predicate, t.object) {
            triples.push(triple);
        }
    }
    Ok(triples)
}

/// Convert an oxrdf triple to our model. Includes blank nodes as `_:id` URIs.
fn convert_triple(
    subject: oxrdf::NamedOrBlankNode,
    predicate: oxrdf::NamedNode,
    object: oxrdf::Term,
) -> Option<Triple> {
    let subj = match subject {
        oxrdf::NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
        oxrdf::NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
    };

    let pred = predicate.as_str().to_string();

    let obj = match object {
        oxrdf::Term::NamedNode(n) => RdfObject::Uri(n.as_str().to_string()),
        oxrdf::Term::BlankNode(b) => RdfObject::Uri(format!("_:{}", b.as_str())),
        oxrdf::Term::Literal(lit) => {
            let dt = lit.datatype();
            let datatype = if dt == rdf::LANG_STRING || dt == oxrdf::vocab::xsd::STRING {
                None
            } else {
                Some(dt.as_str().to_string())
            };
            RdfObject::Literal {
                value: lit.value().to_string(),
                datatype,
                lang: lit.language().map(|l| l.to_string()),
            }
        }
    };

    Some(Triple {
        subject: subj,
        predicate: pred,
        object: obj,
    })
}
