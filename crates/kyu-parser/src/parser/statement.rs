//! Top-level statement parser dispatching to clause and DDL parsers.

use chumsky::prelude::*;

use crate::ast::*;
use crate::span::Spanned;
use crate::token::Token;

use super::clause::{
    reading_clause, return_clause, standalone_call, transaction_statement, updating_clause,
    with_clause,
};
use super::ddl::{alter_table, copy_from, create_node_table, create_rel_table, drop_statement};

type ParserError = Simple<Token>;

/// Parse a complete query (reading/updating clauses + RETURN).
fn query_parser() -> impl Parser<Token, Query, Error = ParserError> + Clone {
    // A query part is: reading clauses + updating clauses + (WITH | RETURN)
    // Multiple parts are chained by WITH clauses.

    let query_part = reading_clause()
        .repeated()
        .then(updating_clause().repeated())
        .then(
            return_clause()
                .map(|proj| (proj, true))
                .or(with_clause().map(|(proj, _where)| (proj, false))),
        )
        .map(|((reading, updating), (projection, is_return))| QueryPart {
            reading_clauses: reading,
            updating_clauses: updating,
            projection: Some(projection),
            is_return,
        });

    // A single-clause query with no RETURN (e.g., just CREATE)
    let update_only = reading_clause()
        .repeated()
        .then(updating_clause().repeated().at_least(1))
        .map(|(reading, updating)| QueryPart {
            reading_clauses: reading,
            updating_clauses: updating,
            projection: None,
            is_return: false,
        });

    let parts = query_part.repeated().at_least(1).or(update_only.map(|p| vec![p]));

    parts.map(|parts| Query {
        parts,
        union_all: vec![],
    })
}

/// Parse a top-level statement.
pub fn statement_parser() -> impl Parser<Token, Spanned<Statement>, Error = ParserError> {
    let explain = just(Token::Explain)
        .ignore_then(
            query_parser()
                .map(Statement::Query)
                .map_with_span(|s, span| (s, span)),
        )
        .map_with_span(|(inner, _), span| (Statement::Explain(Box::new(inner)), span));

    let profile = just(Token::Profile)
        .ignore_then(
            query_parser()
                .map(Statement::Query)
                .map_with_span(|s, span| (s, span)),
        )
        .map_with_span(|(inner, _), span| (Statement::Profile(Box::new(inner)), span));

    let query = query_parser()
        .map(Statement::Query)
        .map_with_span(|s, span| (s, span));

    let create_node = create_node_table()
        .map(Statement::CreateNodeTable)
        .map_with_span(|s, span| (s, span));

    let create_rel = create_rel_table()
        .map(Statement::CreateRelTable)
        .map_with_span(|s, span| (s, span));

    let drop = drop_statement()
        .map(Statement::Drop)
        .map_with_span(|s, span| (s, span));

    let alter = alter_table()
        .map(Statement::AlterTable)
        .map_with_span(|s, span| (s, span));

    let copy = copy_from()
        .map(Statement::CopyFrom)
        .map_with_span(|s, span| (s, span));

    let call = standalone_call()
        .map(Statement::StandaloneCall)
        .map_with_span(|s, span| (s, span));

    let txn = transaction_statement()
        .map(Statement::Transaction)
        .map_with_span(|s, span| (s, span));

    choice((
        explain,
        profile,
        create_node,
        create_rel,
        drop,
        alter,
        copy,
        call,
        txn,
        query,
    ))
    .then_ignore(just(Token::Semicolon).or_not())
    .then_ignore(end())
}
