//! Recursive expression parser with full operator precedence.
//!
//! Precedence (lowest to highest):
//! 1. OR
//! 2. XOR
//! 3. AND
//! 4. NOT (unary prefix)
//! 5. Comparison (=, <>, <, <=, >, >=, =~)
//! 6. String/List ops (STARTS WITH, ENDS WITH, CONTAINS, IN, IS NULL)
//! 7. Add / Subtract (+, -)
//! 8. Multiply / Divide / Modulo (*, /, %)
//! 9. Power (^)
//! 10. Unary (-, ~)
//! 11. Postfix (property access, subscript, function call)
//! 12. Primary (literals, variables, parenthesized, list literal, case, etc.)

use chumsky::prelude::*;
use smol_str::SmolStr;

use crate::ast::*;
use crate::span::Spanned;
use crate::token::Token;

use super::pattern::ident;

type ParserInput = Token;
type ParserError = Simple<Token>;

/// Parse a full expression.
pub fn expression_parser() -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone
{
    // Strategic .boxed() calls erase concrete types at key points to prevent
    // exponential monomorphization growth that OOMs rustc. Without these,
    // 12 nested precedence layers create a type ~2^12 nodes deep.
    recursive(|expr| {
        let primary = primary_expr(expr.clone());
        let postfix = postfix_expr(primary, expr.clone()).boxed();
        let unary = unary_expr(postfix);
        let power = power_expr(unary);
        let mul_div = mul_div_expr(power).boxed();
        let add_sub = add_sub_expr(mul_div);
        let string_list = string_list_expr(add_sub, expr.clone());
        let comparison = comparison_expr(string_list).boxed();
        let not = not_expr(comparison);
        let and = and_expr(not);
        let xor = xor_expr(and);
        or_expr(xor).boxed()
    })
}

// === Primary ===

fn primary_expr(
    expr: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone + 'static,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    let integer = select! { Token::Integer(n) => Expression::Literal(Literal::Integer(n)) };
    let float = select! { Token::Float(s) => {
        let f: f64 = s.parse().unwrap_or(0.0);
        Expression::Literal(Literal::Float(f))
    }};
    let string_lit =
        select! { Token::StringLiteral(s) => Expression::Literal(Literal::String(s)) };
    let bool_true = just(Token::True).to(Expression::Literal(Literal::Bool(true)));
    let bool_false = just(Token::False).to(Expression::Literal(Literal::Bool(false)));
    let null = just(Token::Null).to(Expression::Literal(Literal::Null));

    // Keywords that can also be used as function names or identifiers.
    // Reuses ident() from pattern.rs to stay in sync.
    let variable = ident().map(Expression::Variable);

    let parameter = select! { Token::Parameter(name) => Expression::Parameter(name) };

    // count(*) special form — uses try_map to backtrack if not matched.
    let count_star = just(Token::Count)
        .then(just(Token::LeftParen))
        .then(just(Token::Star))
        .then(just(Token::RightParen))
        .to(Expression::CountStar);

    // List comprehension: [x IN list WHERE pred | projection]
    // Must be attempted before list_literal since both start with `[`.
    let list_comprehension = just(Token::LeftBracket)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .then_ignore(just(Token::In))
        .then(expr.clone())
        .then(
            just(Token::Where)
                .ignore_then(expr.clone())
                .or_not(),
        )
        .then(
            just(Token::Pipe)
                .ignore_then(expr.clone())
                .or_not(),
        )
        .then_ignore(just(Token::RightBracket))
        .map(|(((variable, list), filter), projection)| {
            Expression::ListComprehension {
                variable,
                list: Box::new(list),
                filter: filter.map(Box::new),
                projection: projection.map(Box::new),
            }
        });

    // List literal: [expr, expr, ...]
    let list_literal = expr
        .clone()
        .separated_by(just(Token::Comma))
        .allow_trailing()
        .delimited_by(just(Token::LeftBracket), just(Token::RightBracket))
        .map(Expression::ListLiteral);

    // Map literal: {key: expr, key: expr, ...}
    let map_entry = ident()
        .map_with_span(|n, s| (n, s))
        .then_ignore(just(Token::Colon))
        .then(expr.clone());

    let map_literal = map_entry
        .separated_by(just(Token::Comma))
        .allow_trailing()
        .delimited_by(just(Token::LeftBrace), just(Token::RightBrace))
        .map(Expression::MapLiteral);

    // CASE expression
    let case_expr = case_expression(expr.clone());

    // Parenthesized expression — extract Expression from Spanned
    let paren = expr
        .clone()
        .delimited_by(just(Token::LeftParen), just(Token::RightParen))
        .map(|(e, _span)| e);

    // Use boxed() to erase types so the two choice groups can be combined with or().
    let literals = choice((
        count_star,
        integer,
        float,
        string_lit,
        bool_true,
        bool_false,
        null,
    ))
    .boxed();

    let compound = choice((
        parameter,
        case_expr,
        list_comprehension,
        list_literal,
        map_literal,
        paren,
        variable,
    ))
    .boxed();

    literals.or(compound).map_with_span(|e, s| (e, s))
}

// === Postfix: property access, subscript, function call ===

fn postfix_expr(
    primary: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone + 'static,
    expr: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone + 'static,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    // We handle postfix operations by parsing primary then folding left
    // through chains of `.property`, `[index]`, and `(args...)`.

    // Function call args: need to handle `func(DISTINCT expr, expr, ...)`
    let distinct = just(Token::Distinct).or_not().map(|d| d.is_some());

    let func_args = distinct
        .then(
            expr.clone()
                .separated_by(just(Token::Comma))
                .allow_trailing(),
        )
        .delimited_by(just(Token::LeftParen), just(Token::RightParen));

    // Subscript: [expr]
    let subscript = expr
        .clone()
        .delimited_by(just(Token::LeftBracket), just(Token::RightBracket));

    enum Postfix {
        Property(Spanned<SmolStr>),
        Subscript(Spanned<Expression>),
        FuncCall(bool, Vec<Spanned<Expression>>),
    }

    let property = just(Token::Dot)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .map(Postfix::Property);

    let sub = subscript.map(Postfix::Subscript);

    let call = func_args.map(|(d, args)| Postfix::FuncCall(d, args));

    primary
        .then(choice((property, call, sub)).repeated())
        .foldl(|base, postfix| {
            let span_start = base.1.start;
            match postfix {
                Postfix::Property(key) => {
                    let span_end = key.1.end;
                    (
                        Expression::Property {
                            object: Box::new(base),
                            key,
                        },
                        span_start..span_end,
                    )
                }
                Postfix::Subscript(index) => {
                    let span_end = index.1.end;
                    (
                        Expression::Subscript {
                            expr: Box::new(base),
                            index: Box::new(index),
                        },
                        span_start..span_end,
                    )
                }
                Postfix::FuncCall(distinct, args) => {
                    // The base must be a Variable (function name)
                    let name = match &base.0 {
                        Expression::Variable(n) => vec![(n.clone(), base.1.clone())],
                        Expression::Property { object, key } => {
                            // namespace.function_name
                            let mut names = Vec::new();
                            if let Expression::Variable(n) = &object.0 {
                                names.push((n.clone(), object.1.clone()));
                            }
                            names.push(key.clone());
                            names
                        }
                        _ => vec![(SmolStr::new("<unknown>"), base.1.clone())],
                    };
                    let span_end = args.last().map(|a| a.1.end).unwrap_or(base.1.end) + 1;
                    (
                        Expression::FunctionCall {
                            name,
                            distinct,
                            args,
                        },
                        span_start..span_end,
                    )
                }
            }
        })
}

// === Unary: -, ~ ===

fn unary_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    let minus = just(Token::Dash).to(UnaryOp::Minus);
    let bitwise_not = just(Token::Tilde).to(UnaryOp::BitwiseNot);

    let op = minus.or(bitwise_not);

    op.map_with_span(|op, s: std::ops::Range<usize>| (op, s))
        .repeated()
        .then(inner)
        .foldr(|(op, op_span), operand| {
            let span = op_span.start..operand.1.end;
            (
                Expression::UnaryOp {
                    op,
                    operand: Box::new(operand),
                },
                span,
            )
        })
}

// === Power: ^ ===

fn power_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    inner
        .clone()
        .then(just(Token::Caret).ignore_then(inner).repeated())
        .foldl(|left, right| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOp::Pow,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === Mul / Div / Mod ===

fn mul_div_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    let op = choice((
        just(Token::Star).to(BinaryOp::Mul),
        just(Token::Slash).to(BinaryOp::Div),
        just(Token::Percent).to(BinaryOp::Mod),
    ));

    inner
        .clone()
        .then(op.then(inner).repeated())
        .foldl(|left, (op, right)| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === Add / Sub ===

fn add_sub_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    let op = choice((
        just(Token::Plus).to(BinaryOp::Add),
        just(Token::Dash).to(BinaryOp::Sub),
    ));

    inner
        .clone()
        .then(op.then(inner).repeated())
        .foldl(|left, (op, right)| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === String/List operators: STARTS WITH, ENDS WITH, CONTAINS, IN, IS NULL ===

fn string_list_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone + 'static,
    _full_expr: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError>
        + Clone
        + 'static,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    enum PostfixStringOp {
        StartsWith(Spanned<Expression>),
        EndsWith(Spanned<Expression>),
        Contains(Spanned<Expression>),
        In(Spanned<Expression>),
        IsNull(bool),
        HasLabel(Vec<Spanned<SmolStr>>),
    }

    let starts_with = just(Token::Starts)
        .ignore_then(just(Token::With))
        .ignore_then(inner.clone())
        .map(PostfixStringOp::StartsWith);

    let ends_with = just(Token::Ends)
        .ignore_then(just(Token::With))
        .ignore_then(inner.clone())
        .map(PostfixStringOp::EndsWith);

    let contains = just(Token::Contains)
        .ignore_then(inner.clone())
        .map(PostfixStringOp::Contains);

    let in_list = just(Token::In)
        .ignore_then(inner.clone())
        .map(PostfixStringOp::In);

    let is_null = just(Token::Is)
        .ignore_then(just(Token::Not).or_not())
        .then_ignore(just(Token::Null))
        .map(|not| PostfixStringOp::IsNull(not.is_some()));

    // Label predicate: `a:Label` or `a:Label1:Label2`
    let has_label = just(Token::Colon)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .repeated()
        .at_least(1)
        .map(PostfixStringOp::HasLabel);

    inner
        .then(
            choice((starts_with, ends_with, contains, in_list, is_null, has_label)).repeated(),
        )
        .foldl(|left, op| match op {
            PostfixStringOp::StartsWith(right) => {
                let span = left.1.start..right.1.end;
                (
                    Expression::StringOp {
                        left: Box::new(left),
                        op: StringOp::StartsWith,
                        right: Box::new(right),
                    },
                    span,
                )
            }
            PostfixStringOp::EndsWith(right) => {
                let span = left.1.start..right.1.end;
                (
                    Expression::StringOp {
                        left: Box::new(left),
                        op: StringOp::EndsWith,
                        right: Box::new(right),
                    },
                    span,
                )
            }
            PostfixStringOp::Contains(right) => {
                let span = left.1.start..right.1.end;
                (
                    Expression::StringOp {
                        left: Box::new(left),
                        op: StringOp::Contains,
                        right: Box::new(right),
                    },
                    span,
                )
            }
            PostfixStringOp::In(right) => {
                let span = left.1.start..right.1.end;
                (
                    Expression::InList {
                        expr: Box::new(left),
                        list: Box::new(right),
                        negated: false,
                    },
                    span,
                )
            }
            PostfixStringOp::IsNull(negated) => {
                let span = left.1.clone();
                (
                    Expression::IsNull {
                        expr: Box::new(left),
                        negated,
                    },
                    span,
                )
            }
            PostfixStringOp::HasLabel(labels) => {
                let span_end = labels.last().map(|l| l.1.end).unwrap_or(left.1.end);
                let span = left.1.start..span_end;
                (
                    Expression::HasLabel {
                        expr: Box::new(left),
                        labels,
                    },
                    span,
                )
            }
        })
}

// === Comparison ===

fn comparison_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    let op = choice((
        just(Token::Eq).to(ComparisonOp::Eq),
        just(Token::Neq).to(ComparisonOp::Neq),
        just(Token::Le).to(ComparisonOp::Le),
        just(Token::Lt).to(ComparisonOp::Lt),
        just(Token::Ge).to(ComparisonOp::Ge),
        just(Token::Gt).to(ComparisonOp::Gt),
        just(Token::RegexMatch).to(ComparisonOp::RegexMatch),
    ));

    inner
        .clone()
        .then(op.then(inner).repeated())
        .map_with_span(|(left, ops), span| {
            if ops.is_empty() {
                left
            } else {
                (
                    Expression::Comparison {
                        left: Box::new(left),
                        ops,
                    },
                    span,
                )
            }
        })
}

// === NOT ===

fn not_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    just(Token::Not)
        .map_with_span(|_, s: std::ops::Range<usize>| s)
        .repeated()
        .then(inner)
        .foldr(|op_span, operand| {
            let span = op_span.start..operand.1.end;
            (
                Expression::UnaryOp {
                    op: UnaryOp::Not,
                    operand: Box::new(operand),
                },
                span,
            )
        })
}

// === AND ===

fn and_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    inner
        .clone()
        .then(just(Token::And).ignore_then(inner).repeated())
        .foldl(|left, right| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOp::And,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === XOR ===

fn xor_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    inner
        .clone()
        .then(just(Token::Xor).ignore_then(inner).repeated())
        .foldl(|left, right| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOp::Xor,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === OR ===

fn or_expr(
    inner: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone,
) -> impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone {
    inner
        .clone()
        .then(just(Token::Or).ignore_then(inner).repeated())
        .foldl(|left, right| {
            let span = left.1.start..right.1.end;
            (
                Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOp::Or,
                    right: Box::new(right),
                },
                span,
            )
        })
}

// === CASE ===

fn case_expression(
    expr: impl Parser<ParserInput, Spanned<Expression>, Error = ParserError> + Clone + 'static,
) -> impl Parser<ParserInput, Expression, Error = ParserError> + Clone {
    let when_clause = just(Token::When)
        .ignore_then(expr.clone())
        .then_ignore(just(Token::Then))
        .then(expr.clone());

    let else_clause = just(Token::Else).ignore_then(expr.clone());

    just(Token::Case)
        .ignore_then(expr.clone().map(Box::new).or_not())
        .then(when_clause.repeated().at_least(1))
        .then(else_clause.or_not())
        .then_ignore(just(Token::End))
        .map(|((operand, whens), else_expr)| Expression::Case {
            operand,
            whens,
            else_expr: else_expr.map(Box::new),
        })
}

// Note: we use `With` as a keyword token but `with` as a lowercase keyword
// in the `STARTS WITH` / `ENDS WITH` context. The lexer maps `with` to
// Token::With, so we just use `just(Token::With)` in the parsers above.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn parse_expr(src: &str) -> Option<Expression> {
        let (tokens, lex_errors) = Lexer::new(src).lex();
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");

        let len = src.len();
        let stream = chumsky::Stream::from_iter(
            len..len + 1,
            tokens
                .into_iter()
                .filter(|(tok, _)| !matches!(tok, Token::Eof)),
        );

        let (result, errors) = expression_parser().then_ignore(end()).parse_recovery(stream);
        if !errors.is_empty() {
            eprintln!("parse errors: {errors:?}");
        }
        result.map(|(expr, _)| expr)
    }

    #[test]
    fn integer_literal() {
        let expr = parse_expr("42").unwrap();
        assert!(matches!(expr, Expression::Literal(Literal::Integer(42))));
    }

    #[test]
    fn float_literal() {
        let expr = parse_expr("3.14").unwrap();
        if let Expression::Literal(Literal::Float(f)) = expr {
            assert!((f - 3.14).abs() < 1e-10);
        } else {
            panic!("expected float literal");
        }
    }

    #[test]
    fn string_literal() {
        let expr = parse_expr("'hello'").unwrap();
        assert!(matches!(
            expr,
            Expression::Literal(Literal::String(ref s)) if s == "hello"
        ));
    }

    #[test]
    fn boolean_literals() {
        assert!(matches!(
            parse_expr("TRUE").unwrap(),
            Expression::Literal(Literal::Bool(true))
        ));
        assert!(matches!(
            parse_expr("FALSE").unwrap(),
            Expression::Literal(Literal::Bool(false))
        ));
    }

    #[test]
    fn null_literal() {
        assert!(matches!(
            parse_expr("NULL").unwrap(),
            Expression::Literal(Literal::Null)
        ));
    }

    #[test]
    fn variable() {
        let expr = parse_expr("n").unwrap();
        assert!(matches!(expr, Expression::Variable(ref name) if name == "n"));
    }

    #[test]
    fn parameter() {
        let expr = parse_expr("$since").unwrap();
        assert!(matches!(expr, Expression::Parameter(ref name) if name == "since"));
    }

    #[test]
    fn property_access() {
        let expr = parse_expr("n.age").unwrap();
        assert!(matches!(expr, Expression::Property { .. }));
    }

    #[test]
    fn binary_arithmetic() {
        let expr = parse_expr("1 + 2 * 3").unwrap();
        // Should be 1 + (2 * 3) due to precedence
        if let Expression::BinaryOp { op, .. } = &expr {
            assert_eq!(*op, BinaryOp::Add);
        } else {
            panic!("expected binary op, got {expr:?}");
        }
    }

    #[test]
    fn comparison() {
        let expr = parse_expr("n.age > 30").unwrap();
        assert!(matches!(expr, Expression::Comparison { .. }));
    }

    #[test]
    fn logical_and_or() {
        let expr = parse_expr("a AND b OR c").unwrap();
        // Should be (a AND b) OR c
        if let Expression::BinaryOp { op, .. } = &expr {
            assert_eq!(*op, BinaryOp::Or);
        } else {
            panic!("expected OR");
        }
    }

    #[test]
    fn not_expression() {
        let expr = parse_expr("NOT x").unwrap();
        assert!(matches!(
            expr,
            Expression::UnaryOp {
                op: UnaryOp::Not,
                ..
            }
        ));
    }

    #[test]
    fn is_null() {
        let expr = parse_expr("x IS NULL").unwrap();
        assert!(matches!(
            expr,
            Expression::IsNull {
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn is_not_null() {
        let expr = parse_expr("x IS NOT NULL").unwrap();
        assert!(matches!(
            expr,
            Expression::IsNull {
                negated: true,
                ..
            }
        ));
    }

    #[test]
    fn in_list() {
        let expr = parse_expr("x IN [1, 2, 3]").unwrap();
        assert!(matches!(expr, Expression::InList { .. }));
    }

    #[test]
    fn function_call() {
        let expr = parse_expr("count(n)").unwrap();
        assert!(matches!(expr, Expression::FunctionCall { .. }));
    }

    #[test]
    fn count_star() {
        let expr = parse_expr("count(*)").unwrap();
        assert!(matches!(expr, Expression::CountStar));
    }

    #[test]
    fn list_literal() {
        let expr = parse_expr("[1, 2, 3]").unwrap();
        if let Expression::ListLiteral(items) = expr {
            assert_eq!(items.len(), 3);
        } else {
            panic!("expected list literal");
        }
    }

    #[test]
    fn case_expression_simple() {
        let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END").unwrap();
        assert!(matches!(expr, Expression::Case { .. }));
    }

    #[test]
    fn nested_parentheses() {
        let expr = parse_expr("((1 + 2))").unwrap();
        // Should unwrap to just the binary add
        assert!(matches!(expr, Expression::BinaryOp { .. }));
    }

    #[test]
    fn unary_minus() {
        let expr = parse_expr("-42").unwrap();
        assert!(matches!(
            expr,
            Expression::UnaryOp {
                op: UnaryOp::Minus,
                ..
            }
        ));
    }

    #[test]
    fn starts_with() {
        let expr = parse_expr("name STARTS WITH 'Al'").unwrap();
        assert!(matches!(
            expr,
            Expression::StringOp {
                op: StringOp::StartsWith,
                ..
            }
        ));
    }

    #[test]
    fn contains() {
        let expr = parse_expr("name CONTAINS 'foo'").unwrap();
        assert!(matches!(
            expr,
            Expression::StringOp {
                op: StringOp::Contains,
                ..
            }
        ));
    }

    #[test]
    fn map_literal() {
        let expr = parse_expr("{name: 'Alice', age: 30}").unwrap();
        assert!(matches!(expr, Expression::MapLiteral(_)));
    }

    #[test]
    fn chained_property_access() {
        let expr = parse_expr("a.b.c").unwrap();
        // Should be Property { Property { Variable("a"), "b" }, "c" }
        if let Expression::Property { object, key } = &expr {
            assert_eq!(key.0.as_str(), "c");
            assert!(matches!(object.0, Expression::Property { .. }));
        } else {
            panic!("expected property chain");
        }
    }
}
