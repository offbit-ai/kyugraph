//! DDL statement parsers: CREATE NODE TABLE, CREATE REL TABLE, DROP, ALTER, COPY.

use chumsky::prelude::*;
use smol_str::SmolStr;

use crate::ast::*;
use crate::span::Spanned;
use crate::token::Token;

use super::expression::expression_parser;
use super::pattern::ident;

type ParserError = Simple<Token>;

/// Parse a type name token as a SmolStr (handles type keywords + identifiers).
fn type_name() -> impl Parser<Token, Spanned<SmolStr>, Error = ParserError> + Clone {
    select! {
        Token::BoolType => SmolStr::new("BOOL"),
        Token::Int8Type => SmolStr::new("INT8"),
        Token::Int16Type => SmolStr::new("INT16"),
        Token::Int32Type => SmolStr::new("INT32"),
        Token::Int64Type => SmolStr::new("INT64"),
        Token::Int128Type => SmolStr::new("INT128"),
        Token::UInt8Type => SmolStr::new("UINT8"),
        Token::UInt16Type => SmolStr::new("UINT16"),
        Token::UInt32Type => SmolStr::new("UINT32"),
        Token::UInt64Type => SmolStr::new("UINT64"),
        Token::FloatType => SmolStr::new("FLOAT"),
        Token::DoubleType => SmolStr::new("DOUBLE"),
        Token::StringType => SmolStr::new("STRING"),
        Token::DateType => SmolStr::new("DATE"),
        Token::TimestampType => SmolStr::new("TIMESTAMP"),
        Token::IntervalType => SmolStr::new("INTERVAL"),
        Token::BlobType => SmolStr::new("BLOB"),
        Token::UuidType => SmolStr::new("UUID"),
        Token::SerialType => SmolStr::new("SERIAL"),
        Token::Ident(name) => name,
    }
    .map_with_span(|n, s| (n, s))
}

/// Parse a column definition: `name TYPE [DEFAULT expr]`
fn column_definition() -> impl Parser<Token, ColumnDefinition, Error = ParserError> + Clone {
    let name = ident().map_with_span(|n, s| (n, s));
    let data_type = type_name();
    let default = just(Token::Default)
        .ignore_then(expression_parser())
        .or_not();

    name.then(data_type)
        .then(default)
        .map(|((name, data_type), default_value)| ColumnDefinition {
            name,
            data_type,
            default_value,
        })
}

/// Parse IF NOT EXISTS.
fn if_not_exists() -> impl Parser<Token, bool, Error = ParserError> + Clone {
    just(Token::If)
        .then_ignore(just(Token::Not))
        .then_ignore(just(Token::Exists))
        .or_not()
        .map(|o| o.is_some())
}

/// Parse IF EXISTS.
fn if_exists() -> impl Parser<Token, bool, Error = ParserError> + Clone {
    just(Token::If)
        .then_ignore(just(Token::Exists))
        .or_not()
        .map(|o| o.is_some())
}

/// Parse CREATE NODE TABLE statement.
pub fn create_node_table() -> impl Parser<Token, CreateNodeTable, Error = ParserError> + Clone {
    let primary_key = just(Token::Primary)
        .ignore_then(just(Token::Key))
        .ignore_then(
            ident()
                .map_with_span(|n, s| (n, s))
                .delimited_by(just(Token::LeftParen), just(Token::RightParen)),
        );

    just(Token::Create)
        .ignore_then(just(Token::Node))
        .ignore_then(just(Token::Table))
        .ignore_then(if_not_exists())
        .then(ident().map_with_span(|n, s| (n, s)))
        .then(
            column_definition()
                .separated_by(just(Token::Comma))
                .then_ignore(just(Token::Comma).or_not())
                .then(primary_key)
                .delimited_by(just(Token::LeftParen), just(Token::RightParen)),
        )
        .map(|((if_not_exists, name), (columns, primary_key))| CreateNodeTable {
            name,
            if_not_exists,
            columns,
            primary_key,
        })
        .labelled("create node table")
}

/// Parse CREATE REL TABLE statement.
pub fn create_rel_table() -> impl Parser<Token, CreateRelTable, Error = ParserError> + Clone {
    let from_to = just(Token::From)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .then_ignore(just(Token::To))
        .then(ident().map_with_span(|n, s| (n, s)));

    let columns = just(Token::Comma)
        .ignore_then(column_definition().separated_by(just(Token::Comma)))
        .or_not()
        .map(|c| c.unwrap_or_default());

    just(Token::Create)
        .ignore_then(just(Token::Rel))
        .ignore_then(just(Token::Table))
        .ignore_then(if_not_exists())
        .then(ident().map_with_span(|n, s| (n, s)))
        .then(
            from_to
                .then(columns)
                .delimited_by(just(Token::LeftParen), just(Token::RightParen)),
        )
        .map(
            |((if_not_exists, name), ((from_table, to_table), columns))| CreateRelTable {
                name,
                if_not_exists,
                from_table,
                to_table,
                columns,
            },
        )
        .labelled("create rel table")
}

/// Parse DROP statement.
pub fn drop_statement() -> impl Parser<Token, DropStatement, Error = ParserError> + Clone {
    just(Token::Drop)
        .ignore_then(just(Token::Table).to(DropObjectType::Table))
        .then(if_exists())
        .then(ident().map_with_span(|n, s| (n, s)))
        .map(|((object_type, if_exists), name)| DropStatement {
            object_type,
            name,
            if_exists,
        })
        .labelled("drop statement")
}

/// Parse ALTER TABLE statement.
pub fn alter_table() -> impl Parser<Token, AlterTable, Error = ParserError> + Clone {
    let add_column = just(Token::Add)
        .ignore_then(just(Token::Column).or_not())
        .ignore_then(column_definition())
        .map(AlterAction::AddColumn);

    let drop_column = just(Token::Drop)
        .ignore_then(just(Token::Column).or_not())
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .map(AlterAction::DropColumn);

    let rename_column = just(Token::Rename)
        .ignore_then(just(Token::Column).or_not())
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .then_ignore(just(Token::To))
        .then(ident().map_with_span(|n, s| (n, s)))
        .map(|(old_name, new_name)| AlterAction::RenameColumn { old_name, new_name });

    let rename_table = just(Token::Rename)
        .ignore_then(just(Token::To))
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .map(AlterAction::RenameTable);

    let action = choice((add_column, drop_column, rename_column, rename_table));

    just(Token::Alter)
        .ignore_then(just(Token::Table))
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .then(action)
        .map(|(table_name, action)| AlterTable { table_name, action })
        .labelled("alter table")
}

/// Parse COPY FROM statement.
pub fn copy_from() -> impl Parser<Token, CopyFrom, Error = ParserError> + Clone {
    let option = ident()
        .map_with_span(|n, s| (n, s))
        .then_ignore(just(Token::Eq).or(just(Token::Colon)))
        .then(expression_parser());

    let options = option
        .separated_by(just(Token::Comma))
        .delimited_by(just(Token::LeftParen), just(Token::RightParen))
        .or_not()
        .map(|o| o.unwrap_or_default());

    just(Token::Copy)
        .ignore_then(ident().map_with_span(|n, s| (n, s)))
        .then_ignore(just(Token::From))
        .then(expression_parser())
        .then(options)
        .map(|((table_name, source), options)| CopyFrom {
            table_name,
            source,
            options,
        })
        .labelled("copy from")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn parse_with<T>(
        parser: impl Parser<Token, T, Error = ParserError>,
        src: &str,
    ) -> Option<T> {
        let (toks, errors) = Lexer::new(src).lex();
        assert!(errors.is_empty(), "lex errors: {errors:?}");
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
    fn create_node_table_basic() {
        let stmt = parse_with(
            create_node_table(),
            "CREATE NODE TABLE Person (name STRING, age INT64, PRIMARY KEY (name))",
        )
        .unwrap();
        assert_eq!(stmt.name.0.as_str(), "Person");
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.primary_key.0.as_str(), "name");
        assert!(!stmt.if_not_exists);
    }

    #[test]
    fn create_node_table_if_not_exists() {
        let stmt = parse_with(
            create_node_table(),
            "CREATE NODE TABLE IF NOT EXISTS Person (id INT64, PRIMARY KEY (id))",
        )
        .unwrap();
        assert!(stmt.if_not_exists);
    }

    #[test]
    fn create_rel_table_basic() {
        let stmt = parse_with(
            create_rel_table(),
            "CREATE REL TABLE Knows (FROM Person TO Person, since INT64)",
        )
        .unwrap();
        assert_eq!(stmt.name.0.as_str(), "Knows");
        assert_eq!(stmt.from_table.0.as_str(), "Person");
        assert_eq!(stmt.to_table.0.as_str(), "Person");
        assert_eq!(stmt.columns.len(), 1);
    }

    #[test]
    fn drop_table() {
        let stmt = parse_with(drop_statement(), "DROP TABLE Person").unwrap();
        assert_eq!(stmt.name.0.as_str(), "Person");
        assert!(!stmt.if_exists);
    }

    #[test]
    fn drop_table_if_exists() {
        let stmt = parse_with(drop_statement(), "DROP TABLE IF EXISTS Person").unwrap();
        assert!(stmt.if_exists);
    }

    #[test]
    fn alter_add_column() {
        let stmt = parse_with(
            alter_table(),
            "ALTER TABLE Person ADD email STRING",
        )
        .unwrap();
        assert!(matches!(stmt.action, AlterAction::AddColumn(_)));
    }

    #[test]
    fn alter_drop_column() {
        let stmt = parse_with(alter_table(), "ALTER TABLE Person DROP age").unwrap();
        assert!(matches!(stmt.action, AlterAction::DropColumn(_)));
    }

    #[test]
    fn alter_rename_column() {
        let stmt = parse_with(
            alter_table(),
            "ALTER TABLE Person RENAME name TO full_name",
        )
        .unwrap();
        assert!(matches!(
            stmt.action,
            AlterAction::RenameColumn { .. }
        ));
    }

    #[test]
    fn copy_from_basic() {
        let stmt = parse_with(
            copy_from(),
            "COPY Person FROM 'persons.csv'",
        )
        .unwrap();
        assert_eq!(stmt.table_name.0.as_str(), "Person");
    }
}
