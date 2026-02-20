//! Node and relationship pattern parsers.

use chumsky::prelude::*;
use smol_str::SmolStr;

use crate::ast::*;
use crate::span::Spanned;
use crate::token::Token;

use super::expression::expression_parser;

type ParserError = Simple<Token>;

/// Parse an identifier (unquoted, escaped, or keyword used in identifier position).
///
/// Cypher allows most keywords to be used as identifiers in non-ambiguous contexts
/// (variable names, labels, property names, map keys, relationship types).
///
/// Uses `filter_map` instead of `select!` to avoid monomorphization bloat from 70+ arms,
/// which otherwise causes linker failures from excessively long symbol names.
pub fn ident() -> impl Parser<Token, SmolStr, Error = ParserError> + Clone {
    filter_map(|span, token: Token| {
        match token_to_ident(&token) {
            Some(name) => Ok(name),
            None => Err(Simple::expected_input_found(span, [], Some(token))),
        }
    })
}

/// Convert a token to an identifier name if it can be used in identifier position.
fn token_to_ident(token: &Token) -> Option<SmolStr> {
    match token {
        Token::Ident(name) | Token::EscapedIdent(name) => Some(name.clone()),
        // DDL keywords
        Token::Node => Some(SmolStr::new("NODE")),
        Token::Rel => Some(SmolStr::new("REL")),
        Token::Table => Some(SmolStr::new("TABLE")),
        Token::Group => Some(SmolStr::new("GROUP")),
        Token::Rdf => Some(SmolStr::new("RDF")),
        Token::Graph => Some(SmolStr::new("GRAPH")),
        Token::From => Some(SmolStr::new("FROM")),
        Token::To => Some(SmolStr::new("TO")),
        Token::Primary => Some(SmolStr::new("PRIMARY")),
        Token::Key => Some(SmolStr::new("KEY")),
        Token::Add => Some(SmolStr::new("ADD")),
        Token::Column => Some(SmolStr::new("COLUMN")),
        Token::Rename => Some(SmolStr::new("RENAME")),
        Token::Comment => Some(SmolStr::new("COMMENT")),
        Token::Default => Some(SmolStr::new("DEFAULT")),
        Token::Copy => Some(SmolStr::new("COPY")),
        Token::Load => Some(SmolStr::new("LOAD")),
        Token::Attach => Some(SmolStr::new("ATTACH")),
        Token::Use => Some(SmolStr::new("USE")),
        Token::Database => Some(SmolStr::new("DATABASE")),
        Token::Export => Some(SmolStr::new("EXPORT")),
        Token::Import => Some(SmolStr::new("IMPORT")),
        Token::Install => Some(SmolStr::new("INSTALL")),
        Token::Extension => Some(SmolStr::new("EXTENSION")),
        // Transaction keywords
        Token::Begin => Some(SmolStr::new("BEGIN")),
        Token::Commit => Some(SmolStr::new("COMMIT")),
        Token::Rollback => Some(SmolStr::new("ROLLBACK")),
        Token::Transaction => Some(SmolStr::new("TRANSACTION")),
        Token::Read => Some(SmolStr::new("READ")),
        Token::Write => Some(SmolStr::new("WRITE")),
        Token::Only => Some(SmolStr::new("ONLY")),
        // Type keywords
        Token::ListType => Some(SmolStr::new("LIST")),
        Token::MapType => Some(SmolStr::new("MAP")),
        Token::StructType => Some(SmolStr::new("STRUCT")),
        Token::UnionType => Some(SmolStr::new("UNION")),
        Token::BoolType => Some(SmolStr::new("BOOL")),
        Token::StringType => Some(SmolStr::new("STRING")),
        Token::DateType => Some(SmolStr::new("DATE")),
        Token::TimestampType => Some(SmolStr::new("TIMESTAMP")),
        Token::IntervalType => Some(SmolStr::new("INTERVAL")),
        Token::BlobType => Some(SmolStr::new("BLOB")),
        Token::UuidType => Some(SmolStr::new("UUID")),
        Token::SerialType => Some(SmolStr::new("SERIAL")),
        Token::FloatType => Some(SmolStr::new("FLOAT")),
        Token::DoubleType => Some(SmolStr::new("DOUBLE")),
        Token::Int8Type => Some(SmolStr::new("INT8")),
        Token::Int16Type => Some(SmolStr::new("INT16")),
        Token::Int32Type => Some(SmolStr::new("INT32")),
        Token::Int64Type => Some(SmolStr::new("INT64")),
        Token::Int128Type => Some(SmolStr::new("INT128")),
        Token::UInt8Type => Some(SmolStr::new("UINT8")),
        Token::UInt16Type => Some(SmolStr::new("UINT16")),
        Token::UInt32Type => Some(SmolStr::new("UINT32")),
        Token::UInt64Type => Some(SmolStr::new("UINT64")),
        // Cypher keywords usable as identifiers
        Token::Count => Some(SmolStr::new("count")),
        Token::Exists => Some(SmolStr::new("exists")),
        Token::All => Some(SmolStr::new("all")),
        Token::Any => Some(SmolStr::new("any")),
        Token::Single => Some(SmolStr::new("single")),
        Token::None => Some(SmolStr::new("none")),
        Token::On => Some(SmolStr::new("ON")),
        Token::Yield => Some(SmolStr::new("YIELD")),
        Token::End => Some(SmolStr::new("END")),
        Token::Call => Some(SmolStr::new("CALL")),
        Token::If => Some(SmolStr::new("IF")),
        Token::Macro => Some(SmolStr::new("MACRO")),
        Token::Shortest => Some(SmolStr::new("SHORTEST")),
        Token::Asc => Some(SmolStr::new("ASC")),
        Token::Desc => Some(SmolStr::new("DESC")),
        Token::In => Some(SmolStr::new("IN")),
        Token::Is => Some(SmolStr::new("IS")),
        Token::Contains => Some(SmolStr::new("CONTAINS")),
        Token::Starts => Some(SmolStr::new("STARTS")),
        Token::Ends => Some(SmolStr::new("ENDS")),
        Token::Union => Some(SmolStr::new("UNION")),
        Token::Drop => Some(SmolStr::new("DROP")),
        Token::Alter => Some(SmolStr::new("ALTER")),
        Token::Remove => Some(SmolStr::new("REMOVE")),
        Token::Profile => Some(SmolStr::new("PROFILE")),
        Token::Explain => Some(SmolStr::new("EXPLAIN")),
        _ => Option::None,
    }
}

/// Parse one or more node labels: `:Label1:Label2`
fn node_labels() -> impl Parser<Token, Vec<Spanned<SmolStr>>, Error = ParserError> + Clone {
    just(Token::Colon)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .repeated()
        .at_least(1)
}

/// Parse map properties: `{key: expr, key: expr, ...}`
pub fn map_properties(
) -> impl Parser<Token, Vec<(Spanned<SmolStr>, Spanned<Expression>)>, Error = ParserError> + Clone
{
    let entry = ident()
        .map_with_span(|n, s| (n, s))
        .then_ignore(just(Token::Colon))
        .then(expression_parser());

    entry
        .separated_by(just(Token::Comma))
        .allow_trailing()
        .delimited_by(just(Token::LeftBrace), just(Token::RightBrace))
}

/// Parse a node pattern: `(variable:Label {props})`
pub fn node_pattern() -> impl Parser<Token, NodePattern, Error = ParserError> + Clone {
    let variable = ident().map_with_span(|n, s| (n, s)).or_not();
    let labels = node_labels().or_not().map(|l| l.unwrap_or_default());
    let props = map_properties().or_not();

    variable
        .then(labels)
        .then(props)
        .delimited_by(just(Token::LeftParen), just(Token::RightParen))
        .map_with_span(|((variable, labels), properties), span| NodePattern {
            variable,
            labels,
            properties,
            span,
        })
        .labelled("node pattern")
}

/// Parse a relationship pattern including direction arrows.
///
/// Handles:
/// - `-[r:TYPE]->` (right)
/// - `<-[r:TYPE]-` (left)
/// - `-[r:TYPE]-`  (both)
type RelDetail = (
    Option<Spanned<SmolStr>>,
    Vec<Spanned<SmolStr>>,
    Option<(Option<u32>, Option<u32>)>,
    Option<Vec<(Spanned<SmolStr>, Spanned<Expression>)>>,
);

fn relationship_detail() -> impl Parser<Token, RelDetail, Error = ParserError> + Clone {
    let variable = ident().map_with_span(|n, s| (n, s)).or_not();

    // Relationship types: `:TYPE1|TYPE2` or `:TYPE1|:TYPE2` (TCK allows colon after pipe)
    let rel_types = just(Token::Colon)
        .ignore_then(
            ident()
                .map_with_span(|n, s| (n, s))
                .then(
                    just(Token::Pipe)
                        .ignore_then(just(Token::Colon).or_not())
                        .ignore_then(ident().map_with_span(|n, s| (n, s)))
                        .repeated(),
                )
                .map(|(first, rest)| {
                    let mut types = vec![first];
                    types.extend(rest);
                    types
                }),
        )
        .or_not()
        .map(|t| t.unwrap_or_default());

    // Variable-length: *min..max
    let range = just(Token::Star)
        .ignore_then(
            select! { Token::Integer(n) => n as u32 }
                .or_not()
                .then(
                    just(Token::DoubleDot)
                        .ignore_then(select! { Token::Integer(n) => n as u32 }.or_not())
                        .or_not(),
                ),
        )
        .map(|(min, max_opt)| match max_opt {
            Some(max) => (min, max),
            None => (min, min),
        })
        .or_not();

    let props = map_properties().or_not();

    variable
        .then(rel_types)
        .then(range)
        .then(props)
        .delimited_by(just(Token::LeftBracket), just(Token::RightBracket))
        .map(|(((variable, rel_types), range), properties)| {
            (variable, rel_types, range, properties)
        })
}

/// Parse a complete relationship segment with arrows/dashes.
fn relationship_pattern() -> impl Parser<Token, RelationshipPattern, Error = ParserError> + Clone {
    // Case 1: <-[...]-  (left arrow)
    let left = just(Token::LeftArrow)
        .ignore_then(relationship_detail())
        .then_ignore(just(Token::Dash))
        .map_with_span(|detail: RelDetail, span| {
            let (variable, rel_types, range, properties) = detail;
            RelationshipPattern { variable, rel_types, direction: Direction::Left, range, properties, span }
        });

    // Case 2: -[...]->  (right arrow)
    let right = just(Token::Dash)
        .ignore_then(relationship_detail())
        .then_ignore(just(Token::Arrow))
        .map_with_span(|detail: RelDetail, span| {
            let (variable, rel_types, range, properties) = detail;
            RelationshipPattern { variable, rel_types, direction: Direction::Right, range, properties, span }
        });

    // Case 3: -[...]-  (both directions / undirected)
    let both = just(Token::Dash)
        .ignore_then(relationship_detail())
        .then_ignore(just(Token::Dash))
        .map_with_span(|detail: RelDetail, span| {
            let (variable, rel_types, range, properties) = detail;
            RelationshipPattern { variable, rel_types, direction: Direction::Both, range, properties, span }
        });

    // Case 4: simple dashes without brackets: --> or <-- or --
    let simple_right = just(Token::Dash)
        .then_ignore(just(Token::Arrow))
        .map_with_span(|_, span| RelationshipPattern {
            variable: None,
            rel_types: vec![],
            direction: Direction::Right,
            range: None,
            properties: None,
            span,
        });

    let simple_left = just(Token::LeftArrow)
        .then_ignore(just(Token::Dash))
        .map_with_span(|_, span| RelationshipPattern {
            variable: None,
            rel_types: vec![],
            direction: Direction::Left,
            range: None,
            properties: None,
            span,
        });

    // Case 6: simple undirected `--` without brackets
    let simple_both = just(Token::Dash)
        .then_ignore(just(Token::Dash))
        .map_with_span(|_, span| RelationshipPattern {
            variable: None,
            rel_types: vec![],
            direction: Direction::Both,
            range: None,
            properties: None,
            span,
        });

    // Case 7: `<-->` shorthand (left-arrow followed by right-arrow)
    let simple_both_arrow = just(Token::LeftArrow)
        .then_ignore(just(Token::Arrow))
        .map_with_span(|_, span| RelationshipPattern {
            variable: None,
            rel_types: vec![],
            direction: Direction::Both,
            range: None,
            properties: None,
            span,
        });

    choice((left, right, both, simple_right, simple_left, simple_both_arrow, simple_both))
        .labelled("relationship pattern")
}

/// Parse a chain of node-rel-node-rel-... pattern elements.
pub fn pattern_element_chain() -> impl Parser<Token, Vec<PatternElement>, Error = ParserError> + Clone
{
    node_pattern()
        .map(PatternElement::Node)
        .then(
            relationship_pattern()
                .map(PatternElement::Relationship)
                .then(node_pattern().map(PatternElement::Node))
                .repeated(),
        )
        .map(|(first, rest)| {
            let mut elements = vec![first];
            for (rel, node) in rest {
                elements.push(rel);
                elements.push(node);
            }
            elements
        })
}

/// Parse a full pattern with optional variable assignment: `p = (a)-[:R]->(b)`
pub fn pattern_parser() -> impl Parser<Token, Pattern, Error = ParserError> + Clone {
    let variable_assignment = ident()
        .map_with_span(|n, s| (n, s))
        .then_ignore(just(Token::Eq))
        .or_not();

    variable_assignment
        .then(pattern_element_chain())
        .map(|(variable, elements)| Pattern { variable, elements })
        .labelled("pattern")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn parse_pattern_from(src: &str) -> Option<Pattern> {
        let (tokens, lex_errors) = Lexer::new(src).lex();
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");

        let len = src.len();
        let stream = chumsky::Stream::from_iter(
            len..len + 1,
            tokens
                .into_iter()
                .filter(|(tok, _)| !matches!(tok, Token::Eof)),
        );

        let (result, errors) = pattern_parser().then_ignore(end()).parse_recovery(stream);
        if !errors.is_empty() {
            eprintln!("parse errors: {errors:?}");
        }
        result
    }

    #[test]
    fn simple_node() {
        let p = parse_pattern_from("(n)").unwrap();
        assert_eq!(p.elements.len(), 1);
        if let PatternElement::Node(node) = &p.elements[0] {
            assert_eq!(node.variable.as_ref().unwrap().0.as_str(), "n");
            assert!(node.labels.is_empty());
        } else {
            panic!("expected node");
        }
    }

    #[test]
    fn node_with_label() {
        let p = parse_pattern_from("(n:Person)").unwrap();
        if let PatternElement::Node(node) = &p.elements[0] {
            assert_eq!(node.labels.len(), 1);
            assert_eq!(node.labels[0].0.as_str(), "Person");
        } else {
            panic!("expected node");
        }
    }

    #[test]
    fn node_with_multiple_labels() {
        let p = parse_pattern_from("(n:Person:Employee)").unwrap();
        if let PatternElement::Node(node) = &p.elements[0] {
            assert_eq!(node.labels.len(), 2);
        } else {
            panic!("expected node");
        }
    }

    #[test]
    fn node_with_properties() {
        let p = parse_pattern_from("(n:Person {name: 'Alice', age: 30})").unwrap();
        if let PatternElement::Node(node) = &p.elements[0] {
            assert!(node.properties.is_some());
            assert_eq!(node.properties.as_ref().unwrap().len(), 2);
        } else {
            panic!("expected node");
        }
    }

    #[test]
    fn right_relationship() {
        let p = parse_pattern_from("(a)-[:KNOWS]->(b)").unwrap();
        assert_eq!(p.elements.len(), 3);
        if let PatternElement::Relationship(rel) = &p.elements[1] {
            assert_eq!(rel.direction, Direction::Right);
            assert_eq!(rel.rel_types.len(), 1);
            assert_eq!(rel.rel_types[0].0.as_str(), "KNOWS");
        } else {
            panic!("expected relationship");
        }
    }

    #[test]
    fn left_relationship() {
        let p = parse_pattern_from("(a)<-[:LIKES]-(b)").unwrap();
        if let PatternElement::Relationship(rel) = &p.elements[1] {
            assert_eq!(rel.direction, Direction::Left);
        } else {
            panic!("expected relationship");
        }
    }

    #[test]
    fn undirected_relationship() {
        let p = parse_pattern_from("(a)-[:FRIENDS]-(b)").unwrap();
        if let PatternElement::Relationship(rel) = &p.elements[1] {
            assert_eq!(rel.direction, Direction::Both);
        } else {
            panic!("expected relationship");
        }
    }

    #[test]
    fn variable_on_relationship() {
        let p = parse_pattern_from("(a)-[r:KNOWS]->(b)").unwrap();
        if let PatternElement::Relationship(rel) = &p.elements[1] {
            assert_eq!(rel.variable.as_ref().unwrap().0.as_str(), "r");
        } else {
            panic!("expected relationship");
        }
    }

    #[test]
    fn multi_hop() {
        let p = parse_pattern_from("(a)-[:R1]->(b)-[:R2]->(c)").unwrap();
        assert_eq!(p.elements.len(), 5); // node-rel-node-rel-node
    }

    #[test]
    fn pattern_with_variable_assignment() {
        let p = parse_pattern_from("p = (a)-[:R]->(b)").unwrap();
        assert_eq!(p.variable.as_ref().unwrap().0.as_str(), "p");
    }

    #[test]
    fn anonymous_node() {
        let p = parse_pattern_from("()").unwrap();
        if let PatternElement::Node(node) = &p.elements[0] {
            assert!(node.variable.is_none());
        } else {
            panic!("expected node");
        }
    }
}
