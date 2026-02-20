//! Parse error types and ariadne-based diagnostic rendering.

use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::Simple;

use crate::span::Span;
use crate::token::Token;

/// Result of parsing: an optional AST plus any collected errors.
/// Error recovery may produce both an AST and errors simultaneously.
pub struct ParseResult<T> {
    pub ast: Option<T>,
    pub errors: Vec<ParseError>,
}

/// A located parse error with context.
#[derive(Debug, Clone)]
pub struct ParseError {
    pub span: Span,
    pub message: String,
    pub expected: Vec<String>,
    pub found: Option<String>,
    pub label: Option<&'static str>,
}

impl ParseError {
    /// Convert a chumsky `Simple<Token>` error into our error type.
    pub fn from_chumsky(err: Simple<Token>) -> Self {
        let span = err.span();
        let message = format!("{err}");
        let expected: Vec<String> = err
            .expected()
            .map(|e| match e {
                Some(tok) => format!("{tok}"),
                None => "end of input".to_string(),
            })
            .collect();
        let found = err.found().map(|t| format!("{t}"));
        let label = err.label();

        Self {
            span,
            message,
            expected,
            found,
            label,
        }
    }

    /// Render this error as a rich diagnostic string using ariadne.
    pub fn render(&self, source_name: &str, source: &str) -> String {
        let mut buf = Vec::new();

        let msg = if !self.expected.is_empty() {
            let expected_str = self.expected.join(", ");
            match &self.found {
                Some(found) => format!("expected {expected_str}, found {found}"),
                None => format!("expected {expected_str}"),
            }
        } else {
            self.message.clone()
        };

        let label_msg = match self.label {
            Some(label) => format!("in {label}"),
            None => msg.clone(),
        };

        Report::build(ReportKind::Error, source_name, self.span.start)
            .with_message(&msg)
            .with_label(
                Label::new((source_name, self.span.clone()))
                    .with_message(label_msg)
                    .with_color(Color::Red),
            )
            .finish()
            .write((source_name, Source::from(source)), &mut buf)
            .unwrap();

        String::from_utf8(buf).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_error() {
        let err = ParseError {
            span: 6..7,
            message: "unexpected token".to_string(),
            expected: vec!["(".to_string()],
            found: Some(")".to_string()),
            label: None,
        };
        let rendered = err.render("test.cyp", "MATCH ) foo");
        assert!(rendered.contains("test.cyp"));
        assert!(rendered.contains("expected"));
    }

    #[test]
    fn render_with_label() {
        let err = ParseError {
            span: 0..5,
            message: "unexpected".to_string(),
            expected: vec![],
            found: None,
            label: Some("match pattern"),
        };
        let rendered = err.render("query.cyp", "XXXXX (n)");
        assert!(rendered.contains("match pattern"));
    }
}
