use smol_str::SmolStr;

use crate::span::{Span, Spanned};
use crate::token::{lookup_keyword, Token};

/// Lexical analysis error.
#[derive(Debug, Clone)]
pub struct LexError {
    pub span: Span,
    pub message: String,
}

/// Hand-written Cypher lexer producing a flat token stream.
///
/// Separating lex from parse gives precise byte positions for every
/// error span and avoids running combinators on individual characters.
pub struct Lexer<'src> {
    source: &'src [u8],
    pos: usize,
    tokens: Vec<Spanned<Token>>,
    errors: Vec<LexError>,
}

impl<'src> Lexer<'src> {
    pub fn new(source: &'src str) -> Self {
        Self {
            source: source.as_bytes(),
            pos: 0,
            tokens: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub fn lex(mut self) -> (Vec<Spanned<Token>>, Vec<LexError>) {
        while !self.is_at_end() {
            self.skip_whitespace_and_comments();
            if self.is_at_end() {
                break;
            }

            let start = self.pos;
            let ch = self.advance();

            match ch {
                b'(' => self.push(Token::LeftParen, start),
                b')' => self.push(Token::RightParen, start),
                b'[' => self.push(Token::LeftBracket, start),
                b']' => self.push(Token::RightBracket, start),
                b'{' => self.push(Token::LeftBrace, start),
                b'}' => self.push(Token::RightBrace, start),
                b',' => self.push(Token::Comma, start),
                b';' => self.push(Token::Semicolon, start),
                b':' => self.push(Token::Colon, start),
                b'|' => self.push(Token::Pipe, start),
                b'*' => self.push(Token::Star, start),
                b'%' => self.push(Token::Percent, start),
                b'^' => self.push(Token::Caret, start),
                b'&' => self.push(Token::Ampersand, start),
                b'~' => self.push(Token::Tilde, start),
                b'!' => self.push(Token::Exclaim, start),

                b'.' => {
                    if self.peek() == Some(b'.') {
                        self.advance();
                        self.push(Token::DoubleDot, start);
                    } else {
                        self.push(Token::Dot, start);
                    }
                }

                b'+' => {
                    if self.peek() == Some(b'=') {
                        self.advance();
                        self.push(Token::PlusEq, start);
                    } else {
                        self.push(Token::Plus, start);
                    }
                }

                b'=' => {
                    if self.peek() == Some(b'~') {
                        self.advance();
                        self.push(Token::RegexMatch, start);
                    } else {
                        self.push(Token::Eq, start);
                    }
                }

                b'<' => {
                    match self.peek() {
                        Some(b'=') => {
                            self.advance();
                            self.push(Token::Le, start);
                        }
                        Some(b'>') => {
                            self.advance();
                            self.push(Token::Neq, start);
                        }
                        Some(b'<') => {
                            self.advance();
                            self.push(Token::ShiftLeft, start);
                        }
                        Some(b'-') => {
                            self.advance();
                            self.push(Token::LeftArrow, start);
                        }
                        _ => self.push(Token::Lt, start),
                    }
                }

                b'>' => {
                    match self.peek() {
                        Some(b'=') => {
                            self.advance();
                            self.push(Token::Ge, start);
                        }
                        Some(b'>') => {
                            self.advance();
                            self.push(Token::ShiftRight, start);
                        }
                        _ => self.push(Token::Gt, start),
                    }
                }

                b'-' => {
                    if self.peek() == Some(b'>') {
                        self.advance();
                        self.push(Token::Arrow, start);
                    } else {
                        self.push(Token::Dash, start);
                    }
                }

                b'/' => {
                    // Forward slash as division — comments already handled in skip_whitespace
                    self.push(Token::Slash, start);
                }

                b'\'' | b'"' => self.lex_string(ch, start),

                b'`' => self.lex_escaped_ident(start),

                b'$' => self.lex_parameter(start),

                b'0'..=b'9' => self.lex_number(start),

                b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.lex_ident_or_keyword(start),

                _ => {
                    self.errors.push(LexError {
                        span: start..self.pos,
                        message: format!("unexpected character '{}'", ch as char),
                    });
                }
            }
        }

        self.tokens.push((Token::Eof, self.pos..self.pos));
        (self.tokens, self.errors)
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.source.len()
    }

    fn peek(&self) -> Option<u8> {
        self.source.get(self.pos).copied()
    }

    fn advance(&mut self) -> u8 {
        let ch = self.source[self.pos];
        self.pos += 1;
        ch
    }

    fn push(&mut self, token: Token, start: usize) {
        self.tokens.push((token, start..self.pos));
    }

    fn skip_whitespace_and_comments(&mut self) {
        while !self.is_at_end() {
            let ch = self.source[self.pos];
            match ch {
                b' ' | b'\t' | b'\n' | b'\r' => {
                    self.pos += 1;
                }
                b'/' => {
                    if self.pos + 1 < self.source.len() {
                        match self.source[self.pos + 1] {
                            b'/' => {
                                // Line comment: skip to end of line
                                self.pos += 2;
                                while !self.is_at_end() && self.source[self.pos] != b'\n' {
                                    self.pos += 1;
                                }
                            }
                            b'*' => {
                                // Block comment: skip to */
                                let start = self.pos;
                                self.pos += 2;
                                let mut depth = 1;
                                while !self.is_at_end() && depth > 0 {
                                    if self.source[self.pos] == b'*'
                                        && self.pos + 1 < self.source.len()
                                        && self.source[self.pos + 1] == b'/'
                                    {
                                        depth -= 1;
                                        self.pos += 2;
                                    } else if self.source[self.pos] == b'/'
                                        && self.pos + 1 < self.source.len()
                                        && self.source[self.pos + 1] == b'*'
                                    {
                                        depth += 1;
                                        self.pos += 2;
                                    } else {
                                        self.pos += 1;
                                    }
                                }
                                if depth > 0 {
                                    self.errors.push(LexError {
                                        span: start..self.pos,
                                        message: "unterminated block comment".to_string(),
                                    });
                                }
                            }
                            _ => break,
                        }
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }
    }

    fn lex_string(&mut self, quote: u8, start: usize) {
        let mut value = String::new();
        loop {
            if self.is_at_end() {
                self.errors.push(LexError {
                    span: start..self.pos,
                    message: "unterminated string literal".to_string(),
                });
                break;
            }
            let ch = self.advance();
            if ch == quote {
                // Check for escaped quote (doubled): '' or ""
                if self.peek() == Some(quote) {
                    self.advance();
                    value.push(quote as char);
                } else {
                    break;
                }
            } else if ch == b'\\' {
                // Backslash escape
                if self.is_at_end() {
                    self.errors.push(LexError {
                        span: start..self.pos,
                        message: "unterminated string escape".to_string(),
                    });
                    break;
                }
                let esc = self.advance();
                match esc {
                    b'n' => value.push('\n'),
                    b't' => value.push('\t'),
                    b'r' => value.push('\r'),
                    b'\\' => value.push('\\'),
                    b'\'' => value.push('\''),
                    b'"' => value.push('"'),
                    b'0' => value.push('\0'),
                    _ => {
                        value.push('\\');
                        value.push(esc as char);
                    }
                }
            } else {
                value.push(ch as char);
            }
        }
        self.push(Token::StringLiteral(SmolStr::new(&value)), start);
    }

    fn lex_escaped_ident(&mut self, start: usize) {
        let mut value = String::new();
        loop {
            if self.is_at_end() {
                self.errors.push(LexError {
                    span: start..self.pos,
                    message: "unterminated escaped identifier".to_string(),
                });
                break;
            }
            let ch = self.advance();
            if ch == b'`' {
                // Doubled backtick = literal backtick
                if self.peek() == Some(b'`') {
                    self.advance();
                    value.push('`');
                } else {
                    break;
                }
            } else {
                value.push(ch as char);
            }
        }
        self.push(Token::EscapedIdent(SmolStr::new(&value)), start);
    }

    fn lex_parameter(&mut self, start: usize) {
        let name_start = self.pos;
        while !self.is_at_end() && is_ident_continue(self.source[self.pos]) {
            self.pos += 1;
        }
        let name = std::str::from_utf8(&self.source[name_start..self.pos]).unwrap_or("");
        if name.is_empty() {
            self.errors.push(LexError {
                span: start..self.pos,
                message: "expected parameter name after '$'".to_string(),
            });
        } else {
            self.push(Token::Parameter(SmolStr::new(name)), start);
        }
    }

    fn lex_number(&mut self, start: usize) {
        // Consume integer part
        while !self.is_at_end() && self.source[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        let mut is_float = false;

        // Check for fractional part
        if self.peek() == Some(b'.')
            && self
                .source
                .get(self.pos + 1)
                .is_some_and(|c| c.is_ascii_digit())
        {
            is_float = true;
            self.pos += 1; // consume '.'
            while !self.is_at_end() && self.source[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        // Check for exponent
        if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            is_float = true;
            self.pos += 1;
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                self.pos += 1;
            }
            while !self.is_at_end() && self.source[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        let text = std::str::from_utf8(&self.source[start..self.pos]).unwrap_or("0");

        if is_float {
            self.push(Token::Float(SmolStr::new(text)), start);
        } else {
            match text.parse::<i64>() {
                Ok(n) => self.push(Token::Integer(n), start),
                Err(_) => {
                    self.errors.push(LexError {
                        span: start..self.pos,
                        message: format!("integer literal too large: {text}"),
                    });
                }
            }
        }
    }

    fn lex_ident_or_keyword(&mut self, start: usize) {
        while !self.is_at_end() && is_ident_continue(self.source[self.pos]) {
            self.pos += 1;
        }

        let text = std::str::from_utf8(&self.source[start..self.pos]).unwrap_or("");

        if let Some(kw) = lookup_keyword(text) {
            self.push(kw, start);
        } else {
            self.push(Token::Ident(SmolStr::new(text)), start);
        }
    }
}

fn is_ident_continue(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Vec<Token> {
        let (tokens, errors) = Lexer::new(src).lex();
        assert!(errors.is_empty(), "unexpected lex errors: {errors:?}");
        tokens.into_iter().map(|(tok, _)| tok).collect()
    }

    fn lex_with_errors(src: &str) -> (Vec<Token>, Vec<LexError>) {
        let (tokens, errors) = Lexer::new(src).lex();
        let toks = tokens.into_iter().map(|(tok, _)| tok).collect();
        (toks, errors)
    }

    #[test]
    fn empty_input() {
        let tokens = lex("");
        assert_eq!(tokens, vec![Token::Eof]);
    }

    #[test]
    fn single_char_tokens() {
        let tokens = lex("( ) [ ] { } , ; : | * % ^ & ~");
        assert_eq!(
            tokens,
            vec![
                Token::LeftParen,
                Token::RightParen,
                Token::LeftBracket,
                Token::RightBracket,
                Token::LeftBrace,
                Token::RightBrace,
                Token::Comma,
                Token::Semicolon,
                Token::Colon,
                Token::Pipe,
                Token::Star,
                Token::Percent,
                Token::Caret,
                Token::Ampersand,
                Token::Tilde,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn multi_char_operators() {
        let tokens = lex("-> <- .. << >> =~ += <= >= <>");
        assert_eq!(
            tokens,
            vec![
                Token::Arrow,
                Token::LeftArrow,
                Token::DoubleDot,
                Token::ShiftLeft,
                Token::ShiftRight,
                Token::RegexMatch,
                Token::PlusEq,
                Token::Le,
                Token::Ge,
                Token::Neq,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn integer_literals() {
        let tokens = lex("0 42 123456789");
        assert_eq!(
            tokens,
            vec![
                Token::Integer(0),
                Token::Integer(42),
                Token::Integer(123456789),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn float_literals() {
        let tokens = lex("3.14 1.0e10 2.5E-3");
        assert_eq!(
            tokens,
            vec![
                Token::Float(SmolStr::new("3.14")),
                Token::Float(SmolStr::new("1.0e10")),
                Token::Float(SmolStr::new("2.5E-3")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn string_literals() {
        let tokens = lex("'hello' \"world\"");
        assert_eq!(
            tokens,
            vec![
                Token::StringLiteral(SmolStr::new("hello")),
                Token::StringLiteral(SmolStr::new("world")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn string_escape_sequences() {
        let tokens = lex(r#"'he\'s' "tab\there""#);
        assert_eq!(
            tokens,
            vec![
                Token::StringLiteral(SmolStr::new("he's")),
                Token::StringLiteral(SmolStr::new("tab\there")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn string_doubled_quotes() {
        let tokens = lex("'it''s'");
        assert_eq!(
            tokens,
            vec![Token::StringLiteral(SmolStr::new("it's")), Token::Eof]
        );
    }

    #[test]
    fn identifiers() {
        let tokens = lex("foo _bar baz123");
        assert_eq!(
            tokens,
            vec![
                Token::Ident(SmolStr::new("foo")),
                Token::Ident(SmolStr::new("_bar")),
                Token::Ident(SmolStr::new("baz123")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn escaped_identifiers() {
        let tokens = lex("`my column` `has``backtick`");
        assert_eq!(
            tokens,
            vec![
                Token::EscapedIdent(SmolStr::new("my column")),
                Token::EscapedIdent(SmolStr::new("has`backtick")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn parameters() {
        let tokens = lex("$param1 $since");
        assert_eq!(
            tokens,
            vec![
                Token::Parameter(SmolStr::new("param1")),
                Token::Parameter(SmolStr::new("since")),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn keywords_case_insensitive() {
        let tokens = lex("MATCH Match match WHERE where");
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::Match,
                Token::Match,
                Token::Where,
                Token::Where,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn boolean_and_null() {
        let tokens = lex("TRUE false NULL");
        assert_eq!(
            tokens,
            vec![Token::True, Token::False, Token::Null, Token::Eof]
        );
    }

    #[test]
    fn line_comments() {
        let tokens = lex("MATCH // this is a comment\n(n)");
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::LeftParen,
                Token::Ident(SmolStr::new("n")),
                Token::RightParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn block_comments() {
        let tokens = lex("MATCH /* comment */ (n)");
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::LeftParen,
                Token::Ident(SmolStr::new("n")),
                Token::RightParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn full_query() {
        let tokens = lex("MATCH (n:Person) WHERE n.age > 30 RETURN n.name");
        // Check that we get the right sequence of tokens
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LeftParen);
        assert_eq!(tokens[2], Token::Ident(SmolStr::new("n")));
        assert_eq!(tokens[3], Token::Colon);
        assert_eq!(tokens[4], Token::Ident(SmolStr::new("Person")));
        assert_eq!(tokens[5], Token::RightParen);
        assert_eq!(tokens[6], Token::Where);
        assert_eq!(tokens[7], Token::Ident(SmolStr::new("n")));
        assert_eq!(tokens[8], Token::Dot);
        assert_eq!(tokens[9], Token::Ident(SmolStr::new("age")));
        assert_eq!(tokens[10], Token::Gt);
        assert_eq!(tokens[11], Token::Integer(30));
        assert_eq!(tokens[12], Token::Return);
        assert_eq!(tokens[13], Token::Ident(SmolStr::new("n")));
        assert_eq!(tokens[14], Token::Dot);
        assert_eq!(tokens[15], Token::Ident(SmolStr::new("name")));
        assert_eq!(tokens[16], Token::Eof);
    }

    #[test]
    fn relationship_arrows() {
        let tokens = lex("(a)-[:KNOWS]->(b)<-[:LIKES]-(c)");
        assert!(tokens.contains(&Token::Arrow));
        assert!(tokens.contains(&Token::LeftArrow));
        assert!(tokens.contains(&Token::Dash));
    }

    #[test]
    fn unexpected_char_reports_error() {
        let (tokens, errors) = lex_with_errors("MATCH @invalid");
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("unexpected character"));
        // Lexing continues past the error
        assert!(tokens.len() > 1);
    }

    #[test]
    fn unterminated_string_reports_error() {
        let (_tokens, errors) = lex_with_errors("'unterminated");
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("unterminated string"));
    }

    #[test]
    fn spans_are_correct() {
        let (tokens, _) = Lexer::new("MATCH (n)").lex();
        assert_eq!(tokens[0].1, 0..5); // MATCH
        assert_eq!(tokens[1].1, 6..7); // (
        assert_eq!(tokens[2].1, 7..8); // n
        assert_eq!(tokens[3].1, 8..9); // )
    }

    #[test]
    fn dash_vs_negative_number() {
        // In Cypher, `-` before a number is a dash (operator), not a negative sign.
        // The parser handles unary minus. The lexer always emits Dash.
        let tokens = lex("-42");
        assert_eq!(tokens[0], Token::Dash);
        assert_eq!(tokens[1], Token::Integer(42));
    }

    #[test]
    fn dot_after_integer_not_float() {
        // `n.age` — the dot is property access, not a decimal point
        let tokens = lex("n.age");
        assert_eq!(
            tokens,
            vec![
                Token::Ident(SmolStr::new("n")),
                Token::Dot,
                Token::Ident(SmolStr::new("age")),
                Token::Eof,
            ]
        );
    }
}
