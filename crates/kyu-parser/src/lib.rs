//! kyu-parser: chumsky lexer + Cypher parser -> AST.

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod span;
pub mod token;

use chumsky::Parser;

/// Parse a Cypher query string into an AST.
///
/// Returns the AST (if parsing succeeded or error recovery produced a partial
/// result) and all collected errors.
pub fn parse(source: &str) -> error::ParseResult<ast::Statement> {
    let (tokens, lex_errors) = lexer::Lexer::new(source).lex();

    let mut errors: Vec<error::ParseError> = lex_errors
        .into_iter()
        .map(|e| error::ParseError {
            span: e.span,
            message: e.message,
            expected: vec![],
            found: None,
            label: None,
        })
        .collect();

    let len = source.len();
    let stream = chumsky::Stream::from_iter(
        len..len + 1,
        tokens
            .into_iter()
            .filter(|(tok, _)| !matches!(tok, token::Token::Eof)),
    );

    let (ast, parse_errors) = parser::statement::statement_parser().parse_recovery(stream);

    errors.extend(
        parse_errors
            .into_iter()
            .map(error::ParseError::from_chumsky),
    );

    error::ParseResult {
        ast: ast.map(|(stmt, _span)| stmt),
        errors,
    }
}
