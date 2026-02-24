//! MATCH, RETURN, WITH, WHERE, CREATE, SET, DELETE and other clause parsers.

use chumsky::prelude::*;
use smol_str::SmolStr;

use crate::ast::*;
use crate::span::Spanned;
use crate::token::Token;

use super::expression::expression_parser;
use super::pattern::{ident, pattern_parser};

type ParserError = Simple<Token>;

// =============================================================================
// Reading Clauses
// =============================================================================

/// Parse a MATCH clause: `[OPTIONAL] MATCH pattern [WHERE expr]`
pub fn match_clause() -> impl Parser<Token, ReadingClause, Error = ParserError> + Clone {
    let optional = just(Token::Optional).or_not().map(|o| o.is_some());

    optional
        .then_ignore(just(Token::Match))
        .then(
            pattern_parser()
                .separated_by(just(Token::Comma))
                .at_least(1)
                .labelled("match pattern"),
        )
        .then(where_clause().or_not())
        .map(|((is_optional, patterns), where_clause)| {
            ReadingClause::Match(MatchClause {
                is_optional,
                patterns,
                where_clause,
            })
        })
        .labelled("match clause")
}

/// Parse a WHERE clause: `WHERE expr`
fn where_clause() -> impl Parser<Token, Spanned<Expression>, Error = ParserError> + Clone {
    just(Token::Where)
        .ignore_then(expression_parser())
        .labelled("where clause")
}

/// Parse an UNWIND clause: `UNWIND expr AS alias`
pub fn unwind_clause() -> impl Parser<Token, ReadingClause, Error = ParserError> + Clone {
    just(Token::Unwind)
        .ignore_then(expression_parser())
        .then_ignore(just(Token::As))
        .then(ident().map_with_span(|n, s| (n, s)))
        .map(|(expression, alias)| ReadingClause::Unwind(UnwindClause { expression, alias }))
        .labelled("unwind clause")
}

// =============================================================================
// Updating Clauses
// =============================================================================

/// Parse a CREATE clause: `CREATE pattern, pattern, ...`
pub fn create_clause() -> impl Parser<Token, UpdatingClause, Error = ParserError> + Clone {
    just(Token::Create)
        .ignore_then(
            pattern_parser()
                .separated_by(just(Token::Comma))
                .at_least(1),
        )
        .map(UpdatingClause::Create)
        .labelled("create clause")
}

/// Parse a SET clause: `SET item, item, ...`
pub fn set_clause() -> impl Parser<Token, UpdatingClause, Error = ParserError> + Clone {
    just(Token::Set)
        .ignore_then(set_item().separated_by(just(Token::Comma)).at_least(1))
        .map(UpdatingClause::Set)
        .labelled("set clause")
}

fn set_item() -> impl Parser<Token, SetItem, Error = ParserError> + Clone {
    // Parse left side as a property chain: n.prop or n.a.b
    // We can't use expression_parser() here because it would consume `=` as comparison.
    let property_chain = ident()
        .map_with_span(|name, span| (Expression::Variable(name), span))
        .then(
            just(Token::Dot)
                .ignore_then(ident().map_with_span(|n, s| (n, s)))
                .repeated()
                .at_least(1),
        )
        .foldl(|base, key| {
            let span = base.1.start..key.1.end;
            (
                Expression::Property {
                    object: Box::new(base),
                    key,
                },
                span,
            )
        });

    let set_property = property_chain
        .then_ignore(just(Token::Eq))
        .then(expression_parser())
        .map(|(entity, value)| SetItem::Property { entity, value });

    // SET n:Label1:Label2 — setting labels on a node
    let set_labels = ident()
        .map_with_span(|n, s| (n, s))
        .then(
            just(Token::Colon)
                .ignore_then(ident().map_with_span(|n, s| (n, s)))
                .repeated()
                .at_least(1),
        )
        .map(|(entity, labels)| SetItem::Labels { entity, labels });

    set_property.or(set_labels)
}

/// Parse a DELETE clause: `[DETACH] DELETE expr, expr, ...`
pub fn delete_clause() -> impl Parser<Token, UpdatingClause, Error = ParserError> + Clone {
    let detach = just(Token::Detach).or_not().map(|d| d.is_some());

    detach
        .then_ignore(just(Token::Delete))
        .then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .at_least(1),
        )
        .map(|(detach, expressions)| {
            UpdatingClause::Delete(DeleteClause {
                detach,
                expressions,
            })
        })
        .labelled("delete clause")
}

/// Parse a MERGE clause: `MERGE pattern [ON MATCH SET ...] [ON CREATE SET ...]`
pub fn merge_clause() -> impl Parser<Token, UpdatingClause, Error = ParserError> + Clone {
    let on_match = just(Token::On)
        .then_ignore(just(Token::Match))
        .then_ignore(just(Token::Set))
        .ignore_then(set_item().separated_by(just(Token::Comma)).at_least(1))
        .or_not()
        .map(|o| o.unwrap_or_default());

    let on_create = just(Token::On)
        .then_ignore(just(Token::Create))
        .then_ignore(just(Token::Set))
        .ignore_then(set_item().separated_by(just(Token::Comma)).at_least(1))
        .or_not()
        .map(|o| o.unwrap_or_default());

    just(Token::Merge)
        .ignore_then(pattern_parser())
        .then(on_match)
        .then(on_create)
        .map(|((pattern, on_match), on_create)| {
            UpdatingClause::Merge(MergeClause {
                pattern,
                on_match,
                on_create,
            })
        })
        .labelled("merge clause")
}

// =============================================================================
// Projection: RETURN and WITH
// =============================================================================

/// Parse a projection body (shared by RETURN and WITH).
pub fn projection_body() -> impl Parser<Token, ProjectionBody, Error = ParserError> + Clone {
    let distinct = just(Token::Distinct).or_not().map(|d| d.is_some());

    let alias = just(Token::As)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .or_not();

    let item = expression_parser().then(alias);

    let items = just(Token::Star).to(ProjectionItems::All).or(item
        .separated_by(just(Token::Comma))
        .at_least(1)
        .map(ProjectionItems::Expressions));

    let sort_order = choice((
        just(Token::Asc).to(SortOrder::Ascending),
        just(Token::Desc).to(SortOrder::Descending),
    ))
    .or_not()
    .map(|o| o.unwrap_or(SortOrder::Ascending));

    let order_by = just(Token::Order)
        .ignore_then(just(Token::By))
        .ignore_then(
            expression_parser()
                .then(sort_order)
                .separated_by(just(Token::Comma))
                .at_least(1),
        )
        .or_not()
        .map(|o| o.unwrap_or_default());

    let skip = just(Token::Skip).ignore_then(expression_parser()).or_not();

    let limit = just(Token::Limit).ignore_then(expression_parser()).or_not();

    distinct
        .then(items)
        .then(order_by)
        .then(skip)
        .then(limit)
        .map(
            |((((distinct, items), order_by), skip), limit)| ProjectionBody {
                distinct,
                items,
                order_by,
                skip,
                limit,
            },
        )
}

/// Parse RETURN clause.
pub fn return_clause() -> impl Parser<Token, ProjectionBody, Error = ParserError> + Clone {
    just(Token::Return)
        .ignore_then(projection_body())
        .labelled("return clause")
}

/// Parse WITH clause.
pub fn with_clause()
-> impl Parser<Token, (ProjectionBody, Option<Spanned<Expression>>), Error = ParserError> + Clone {
    just(Token::With)
        .ignore_then(projection_body())
        .then(where_clause().or_not())
        .labelled("with clause")
}

// =============================================================================
// Reading clause dispatcher
// =============================================================================

/// Parse any reading clause.
pub fn reading_clause() -> impl Parser<Token, ReadingClause, Error = ParserError> + Clone {
    choice((match_clause(), unwind_clause()))
}

/// Parse any updating clause.
pub fn updating_clause() -> impl Parser<Token, UpdatingClause, Error = ParserError> + Clone {
    choice((
        create_clause(),
        merge_clause(),
        set_clause(),
        delete_clause(),
    ))
}

// =============================================================================
// Standalone CALL
// =============================================================================

/// Parse a standalone CALL statement: `CALL procedure(args...)` or `CALL db.schema`
pub fn standalone_call() -> impl Parser<Token, StandaloneCall, Error = ParserError> + Clone {
    // Dotted procedure name: `db.schema` or just `table_info`
    let procedure_name = ident()
        .map_with_span(|n, s| (n, s))
        .then(
            just(Token::Dot)
                .ignore_then(ident().map_with_span(|n, s| (n, s)))
                .repeated(),
        )
        .map(|(first, rest)| {
            let mut full_name = first.0.to_string();
            let start = first.1.start;
            let mut end = first.1.end;
            for part in &rest {
                full_name.push('.');
                full_name.push_str(&part.0);
                end = part.1.end;
            }
            (SmolStr::new(&full_name), start..end)
        });

    let args = expression_parser()
        .separated_by(just(Token::Comma))
        .delimited_by(just(Token::LeftParen), just(Token::RightParen))
        .or_not()
        .map(|a| a.unwrap_or_default());

    just(Token::Call)
        .ignore_then(procedure_name)
        .then(args)
        .map(|(procedure, args)| StandaloneCall { procedure, args })
        .labelled("call statement")
}

// =============================================================================
// Transaction
// =============================================================================

/// Parse transaction statements: BEGIN [READ ONLY | READ WRITE], COMMIT, ROLLBACK
pub fn transaction_statement()
-> impl Parser<Token, TransactionStatement, Error = ParserError> + Clone {
    let mode = choice((
        just(Token::Read)
            .then_ignore(just(Token::Only))
            .to(TransactionMode::ReadOnly),
        just(Token::Read)
            .then_ignore(just(Token::Write))
            .to(TransactionMode::ReadWrite),
    ))
    .or_not()
    .map(|m| m.unwrap_or(TransactionMode::ReadWrite));

    let begin = just(Token::Begin)
        .ignore_then(just(Token::Transaction).or_not())
        .ignore_then(mode)
        .map(TransactionStatement::Begin);

    let commit = just(Token::Commit).to(TransactionStatement::Commit);
    let rollback = just(Token::Rollback).to(TransactionStatement::Rollback);

    choice((begin, commit, rollback)).labelled("transaction statement")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn tokens(src: &str) -> Vec<Spanned<Token>> {
        let (tokens, errors) = Lexer::new(src).lex();
        assert!(errors.is_empty());
        tokens
    }

    fn parse_with<T>(parser: impl Parser<Token, T, Error = ParserError>, src: &str) -> Option<T> {
        let toks = tokens(src);
        let len = src.len();
        let stream = chumsky::Stream::from_iter(
            len..len + 1,
            toks.into_iter()
                .filter(|(tok, _)| !matches!(tok, Token::Eof)),
        );
        let (result, errors) = parser.then_ignore(end()).parse_recovery(stream);
        if !errors.is_empty() {
            eprintln!("parse errors: {errors:?}");
        }
        result
    }

    #[test]
    fn simple_match() {
        let clause = parse_with(match_clause(), "MATCH (n:Person)").unwrap();
        if let ReadingClause::Match(m) = clause {
            assert!(!m.is_optional);
            assert_eq!(m.patterns.len(), 1);
        } else {
            panic!("expected match clause");
        }
    }

    #[test]
    fn optional_match() {
        let clause = parse_with(match_clause(), "OPTIONAL MATCH (n)").unwrap();
        if let ReadingClause::Match(m) = clause {
            assert!(m.is_optional);
        } else {
            panic!("expected match clause");
        }
    }

    #[test]
    fn match_with_where() {
        let clause = parse_with(match_clause(), "MATCH (n:Person) WHERE n.age > 30").unwrap();
        if let ReadingClause::Match(m) = clause {
            assert!(m.where_clause.is_some());
        } else {
            panic!("expected match clause");
        }
    }

    #[test]
    fn return_star() {
        let proj = parse_with(return_clause(), "RETURN *").unwrap();
        assert!(matches!(proj.items, ProjectionItems::All));
    }

    #[test]
    fn return_with_alias() {
        let proj = parse_with(return_clause(), "RETURN n.name AS name").unwrap();
        if let ProjectionItems::Expressions(items) = &proj.items {
            assert_eq!(items.len(), 1);
            assert!(items[0].1.is_some());
        } else {
            panic!("expected expressions");
        }
    }

    #[test]
    fn return_with_order_by() {
        let proj = parse_with(return_clause(), "RETURN n ORDER BY n.age DESC").unwrap();
        assert_eq!(proj.order_by.len(), 1);
        assert_eq!(proj.order_by[0].1, SortOrder::Descending);
    }

    #[test]
    fn return_with_limit_skip() {
        let proj = parse_with(return_clause(), "RETURN n SKIP 10 LIMIT 5").unwrap();
        assert!(proj.skip.is_some());
        assert!(proj.limit.is_some());
    }

    #[test]
    fn create_node() {
        let clause = parse_with(
            create_clause(),
            "CREATE (n:Person {name: 'Alice', age: 30})",
        )
        .unwrap();
        if let UpdatingClause::Create(patterns) = clause {
            assert_eq!(patterns.len(), 1);
        } else {
            panic!("expected create clause");
        }
    }

    #[test]
    fn delete_detach() {
        let clause = parse_with(delete_clause(), "DETACH DELETE n").unwrap();
        if let UpdatingClause::Delete(d) = clause {
            assert!(d.detach);
        } else {
            panic!("expected delete clause");
        }
    }

    #[test]
    fn set_property() {
        let clause = parse_with(set_clause(), "SET n.age = 31").unwrap();
        assert!(matches!(clause, UpdatingClause::Set(_)));
    }

    #[test]
    fn unwind() {
        let clause = parse_with(unwind_clause(), "UNWIND [1, 2, 3] AS x").unwrap();
        assert!(matches!(clause, ReadingClause::Unwind(_)));
    }

    #[test]
    fn transaction_begin() {
        let stmt = parse_with(transaction_statement(), "BEGIN TRANSACTION READ ONLY").unwrap();
        assert!(matches!(
            stmt,
            TransactionStatement::Begin(TransactionMode::ReadOnly)
        ));
    }

    #[test]
    fn transaction_commit() {
        let stmt = parse_with(transaction_statement(), "COMMIT").unwrap();
        assert!(matches!(stmt, TransactionStatement::Commit));
    }
}
