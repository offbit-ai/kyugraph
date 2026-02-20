use smol_str::SmolStr;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Lexical token for the Cypher query language.
/// Produced by the hand-written lexer and consumed by the chumsky parser.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Token {
    // === Literals ===
    Integer(i64),
    Float(SmolStr), // stored as string to preserve exact representation; parsed to f64 later
    StringLiteral(SmolStr),
    True,
    False,
    Null,

    // === Identifiers ===
    Ident(SmolStr),
    EscapedIdent(SmolStr), // backtick-escaped `identifier`
    Parameter(SmolStr),    // $paramName

    // === Punctuation ===
    LeftParen,    // (
    RightParen,   // )
    LeftBracket,  // [
    RightBracket, // ]
    LeftBrace,    // {
    RightBrace,   // }
    Comma,        // ,
    Dot,          // .
    Colon,        // :
    Semicolon,    // ;
    Pipe,         // |
    DoubleDot,    // ..
    Arrow,        // ->
    LeftArrow,    // <-
    Dash,         // -
    Underscore,   // _

    // === Operators ===
    Eq,         // =
    Neq,        // <>
    Lt,         // <
    Le,         // <=
    Gt,         // >
    Ge,         // >=
    Plus,       // +
    Star,       // *
    Slash,      // /
    Percent,    // %
    Caret,      // ^
    Ampersand,  // &
    Tilde,      // ~
    RegexMatch, // =~
    ShiftLeft,  // <<
    ShiftRight, // >>
    Exclaim,    // !
    PlusEq,     // +=

    // === Cypher Keywords ===
    Match,
    Optional,
    Where,
    Return,
    With,
    Unwind,
    Create,
    Merge,
    Set,
    Delete,
    Detach,
    Remove,
    Order,
    By,
    Limit,
    Skip,
    Asc,
    Desc,
    Distinct,
    As,
    And,
    Or,
    Not,
    Xor,
    In,
    Is,
    Starts,
    Ends,
    Contains,
    Case,
    When,
    Then,
    Else,
    End,
    Union,
    All,
    Any,
    None,
    Single,
    Exists,
    Count,
    Call,
    Yield,
    On,

    // === DDL Keywords ===
    Node,
    Rel,
    Table,
    Group,
    Rdf,
    Graph,
    From,
    To,
    Primary,
    Key,
    Drop,
    Alter,
    Add,
    Column,
    Rename,
    Comment,
    Default,
    Copy,
    Load,
    Attach,
    Use,
    Database,
    Export,
    Import,
    Install,
    Extension,

    // === Type Keywords ===
    BoolType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Int128Type,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    FloatType,
    DoubleType,
    StringType,
    DateType,
    TimestampType,
    IntervalType,
    BlobType,
    UuidType,
    SerialType,
    ListType,
    MapType,
    StructType,
    UnionType,

    // === Transaction Keywords ===
    Begin,
    Commit,
    Rollback,
    Transaction,
    Read,
    Write,
    Only,

    // === Special ===
    If,
    NotKw, // NOT as distinct from Not (logical op) — used in "IF NOT EXISTS" etc.
    Macro,
    Shortest,
    Profile,
    Explain,

    // === EOF sentinel ===
    Eof,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(s) => write!(f, "{s}"),
            Self::StringLiteral(s) => write!(f, "'{s}'"),
            Self::True => write!(f, "TRUE"),
            Self::False => write!(f, "FALSE"),
            Self::Null => write!(f, "NULL"),
            Self::Ident(s) => write!(f, "{s}"),
            Self::EscapedIdent(s) => write!(f, "`{s}`"),
            Self::Parameter(s) => write!(f, "${s}"),
            Self::LeftParen => write!(f, "("),
            Self::RightParen => write!(f, ")"),
            Self::LeftBracket => write!(f, "["),
            Self::RightBracket => write!(f, "]"),
            Self::LeftBrace => write!(f, "{{"),
            Self::RightBrace => write!(f, "}}"),
            Self::Comma => write!(f, ","),
            Self::Dot => write!(f, "."),
            Self::Colon => write!(f, ":"),
            Self::Semicolon => write!(f, ";"),
            Self::Pipe => write!(f, "|"),
            Self::DoubleDot => write!(f, ".."),
            Self::Arrow => write!(f, "->"),
            Self::LeftArrow => write!(f, "<-"),
            Self::Dash => write!(f, "-"),
            Self::Underscore => write!(f, "_"),
            Self::Eq => write!(f, "="),
            Self::Neq => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::Le => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Ge => write!(f, ">="),
            Self::Plus => write!(f, "+"),
            Self::Star => write!(f, "*"),
            Self::Slash => write!(f, "/"),
            Self::Percent => write!(f, "%"),
            Self::Caret => write!(f, "^"),
            Self::Ampersand => write!(f, "&"),
            Self::Tilde => write!(f, "~"),
            Self::RegexMatch => write!(f, "=~"),
            Self::ShiftLeft => write!(f, "<<"),
            Self::ShiftRight => write!(f, ">>"),
            Self::Exclaim => write!(f, "!"),
            Self::PlusEq => write!(f, "+="),
            Self::Eof => write!(f, "<EOF>"),
            other => write!(f, "{}", keyword_name(other)),
        }
    }
}

fn keyword_name(tok: &Token) -> &'static str {
    match tok {
        Token::Match => "MATCH",
        Token::Optional => "OPTIONAL",
        Token::Where => "WHERE",
        Token::Return => "RETURN",
        Token::With => "WITH",
        Token::Unwind => "UNWIND",
        Token::Create => "CREATE",
        Token::Merge => "MERGE",
        Token::Set => "SET",
        Token::Delete => "DELETE",
        Token::Detach => "DETACH",
        Token::Remove => "REMOVE",
        Token::Order => "ORDER",
        Token::By => "BY",
        Token::Limit => "LIMIT",
        Token::Skip => "SKIP",
        Token::Asc => "ASC",
        Token::Desc => "DESC",
        Token::Distinct => "DISTINCT",
        Token::As => "AS",
        Token::And => "AND",
        Token::Or => "OR",
        Token::Not => "NOT",
        Token::Xor => "XOR",
        Token::In => "IN",
        Token::Is => "IS",
        Token::Starts => "STARTS",
        Token::Ends => "ENDS",
        Token::Contains => "CONTAINS",
        Token::Case => "CASE",
        Token::When => "WHEN",
        Token::Then => "THEN",
        Token::Else => "ELSE",
        Token::End => "END",
        Token::Union => "UNION",
        Token::All => "ALL",
        Token::Any => "ANY",
        Token::None => "NONE",
        Token::Single => "SINGLE",
        Token::Exists => "EXISTS",
        Token::Count => "COUNT",
        Token::Call => "CALL",
        Token::Yield => "YIELD",
        Token::On => "ON",
        Token::Node => "NODE",
        Token::Rel => "REL",
        Token::Table => "TABLE",
        Token::Group => "GROUP",
        Token::Rdf => "RDF",
        Token::Graph => "GRAPH",
        Token::From => "FROM",
        Token::To => "TO",
        Token::Primary => "PRIMARY",
        Token::Key => "KEY",
        Token::Drop => "DROP",
        Token::Alter => "ALTER",
        Token::Add => "ADD",
        Token::Column => "COLUMN",
        Token::Rename => "RENAME",
        Token::Comment => "COMMENT",
        Token::Default => "DEFAULT",
        Token::Copy => "COPY",
        Token::Load => "LOAD",
        Token::Attach => "ATTACH",
        Token::Use => "USE",
        Token::Database => "DATABASE",
        Token::Export => "EXPORT",
        Token::Import => "IMPORT",
        Token::Install => "INSTALL",
        Token::Extension => "EXTENSION",
        Token::BoolType => "BOOL",
        Token::Int8Type => "INT8",
        Token::Int16Type => "INT16",
        Token::Int32Type => "INT32",
        Token::Int64Type => "INT64",
        Token::Int128Type => "INT128",
        Token::UInt8Type => "UINT8",
        Token::UInt16Type => "UINT16",
        Token::UInt32Type => "UINT32",
        Token::UInt64Type => "UINT64",
        Token::FloatType => "FLOAT",
        Token::DoubleType => "DOUBLE",
        Token::StringType => "STRING",
        Token::DateType => "DATE",
        Token::TimestampType => "TIMESTAMP",
        Token::IntervalType => "INTERVAL",
        Token::BlobType => "BLOB",
        Token::UuidType => "UUID",
        Token::SerialType => "SERIAL",
        Token::ListType => "LIST",
        Token::MapType => "MAP",
        Token::StructType => "STRUCT",
        Token::UnionType => "UNION",
        Token::Begin => "BEGIN",
        Token::Commit => "COMMIT",
        Token::Rollback => "ROLLBACK",
        Token::Transaction => "TRANSACTION",
        Token::Read => "READ",
        Token::Write => "WRITE",
        Token::Only => "ONLY",
        Token::If => "IF",
        Token::NotKw => "NOT",
        Token::Macro => "MACRO",
        Token::Shortest => "SHORTEST",
        Token::Profile => "PROFILE",
        Token::Explain => "EXPLAIN",
        _ => "<unknown>",
    }
}

/// Case-insensitive keyword lookup table.
static KEYWORDS: LazyLock<HashMap<&'static str, Token>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    // Cypher keywords
    m.insert("match", Token::Match);
    m.insert("optional", Token::Optional);
    m.insert("where", Token::Where);
    m.insert("return", Token::Return);
    m.insert("with", Token::With);
    m.insert("unwind", Token::Unwind);
    m.insert("create", Token::Create);
    m.insert("merge", Token::Merge);
    m.insert("set", Token::Set);
    m.insert("delete", Token::Delete);
    m.insert("detach", Token::Detach);
    m.insert("remove", Token::Remove);
    m.insert("order", Token::Order);
    m.insert("by", Token::By);
    m.insert("limit", Token::Limit);
    m.insert("skip", Token::Skip);
    m.insert("asc", Token::Asc);
    m.insert("ascending", Token::Asc);
    m.insert("desc", Token::Desc);
    m.insert("descending", Token::Desc);
    m.insert("distinct", Token::Distinct);
    m.insert("as", Token::As);
    m.insert("and", Token::And);
    m.insert("or", Token::Or);
    m.insert("not", Token::Not);
    m.insert("xor", Token::Xor);
    m.insert("in", Token::In);
    m.insert("is", Token::Is);
    m.insert("starts", Token::Starts);
    m.insert("ends", Token::Ends);
    m.insert("contains", Token::Contains);
    m.insert("case", Token::Case);
    m.insert("when", Token::When);
    m.insert("then", Token::Then);
    m.insert("else", Token::Else);
    m.insert("end", Token::End);
    m.insert("union", Token::Union);
    m.insert("all", Token::All);
    m.insert("any", Token::Any);
    m.insert("none", Token::None);
    m.insert("single", Token::Single);
    m.insert("exists", Token::Exists);
    m.insert("count", Token::Count);
    m.insert("call", Token::Call);
    m.insert("yield", Token::Yield);
    m.insert("on", Token::On);
    m.insert("true", Token::True);
    m.insert("false", Token::False);
    m.insert("null", Token::Null);

    // DDL keywords
    m.insert("node", Token::Node);
    m.insert("rel", Token::Rel);
    m.insert("table", Token::Table);
    m.insert("group", Token::Group);
    m.insert("rdf", Token::Rdf);
    m.insert("graph", Token::Graph);
    m.insert("from", Token::From);
    m.insert("to", Token::To);
    m.insert("primary", Token::Primary);
    m.insert("key", Token::Key);
    m.insert("drop", Token::Drop);
    m.insert("alter", Token::Alter);
    m.insert("add", Token::Add);
    m.insert("column", Token::Column);
    m.insert("rename", Token::Rename);
    m.insert("comment", Token::Comment);
    m.insert("default", Token::Default);
    m.insert("copy", Token::Copy);
    m.insert("load", Token::Load);
    m.insert("attach", Token::Attach);
    m.insert("use", Token::Use);
    m.insert("database", Token::Database);
    m.insert("export", Token::Export);
    m.insert("import", Token::Import);
    m.insert("install", Token::Install);
    m.insert("extension", Token::Extension);

    // Type keywords
    m.insert("bool", Token::BoolType);
    m.insert("boolean", Token::BoolType);
    m.insert("int8", Token::Int8Type);
    m.insert("int16", Token::Int16Type);
    m.insert("int32", Token::Int32Type);
    m.insert("int", Token::Int32Type);
    m.insert("integer", Token::Int32Type);
    m.insert("int64", Token::Int64Type);
    m.insert("int128", Token::Int128Type);
    m.insert("uint8", Token::UInt8Type);
    m.insert("uint16", Token::UInt16Type);
    m.insert("uint32", Token::UInt32Type);
    m.insert("uint64", Token::UInt64Type);
    m.insert("float", Token::FloatType);
    m.insert("double", Token::DoubleType);
    m.insert("string", Token::StringType);
    m.insert("date", Token::DateType);
    m.insert("timestamp", Token::TimestampType);
    m.insert("interval", Token::IntervalType);
    m.insert("blob", Token::BlobType);
    m.insert("uuid", Token::UuidType);
    m.insert("serial", Token::SerialType);
    m.insert("list", Token::ListType);
    m.insert("map", Token::MapType);
    m.insert("struct", Token::StructType);

    // Transaction keywords
    m.insert("begin", Token::Begin);
    m.insert("commit", Token::Commit);
    m.insert("rollback", Token::Rollback);
    m.insert("transaction", Token::Transaction);
    m.insert("read", Token::Read);
    m.insert("write", Token::Write);
    m.insert("only", Token::Only);

    // Special
    m.insert("if", Token::If);
    m.insert("macro", Token::Macro);
    m.insert("shortest", Token::Shortest);
    m.insert("profile", Token::Profile);
    m.insert("explain", Token::Explain);

    m
});

/// Look up whether an identifier is a keyword.
/// Cypher keywords are case-insensitive.
pub fn lookup_keyword(ident: &str) -> Option<Token> {
    let lower = ident.to_ascii_lowercase();
    KEYWORDS.get(lower.as_str()).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_case_insensitive() {
        assert_eq!(lookup_keyword("MATCH"), Some(Token::Match));
        assert_eq!(lookup_keyword("match"), Some(Token::Match));
        assert_eq!(lookup_keyword("Match"), Some(Token::Match));
    }

    #[test]
    fn non_keyword_returns_none() {
        assert_eq!(lookup_keyword("foobar"), None);
        assert_eq!(lookup_keyword("x"), None);
    }

    #[test]
    fn type_keywords() {
        assert_eq!(lookup_keyword("INT64"), Some(Token::Int64Type));
        assert_eq!(lookup_keyword("string"), Some(Token::StringType));
        assert_eq!(lookup_keyword("BOOLEAN"), Some(Token::BoolType));
        assert_eq!(lookup_keyword("INT"), Some(Token::Int32Type));
        assert_eq!(lookup_keyword("INTEGER"), Some(Token::Int32Type));
    }

    #[test]
    fn display_tokens() {
        assert_eq!(Token::LeftParen.to_string(), "(");
        assert_eq!(Token::Arrow.to_string(), "->");
        assert_eq!(Token::Match.to_string(), "MATCH");
        assert_eq!(Token::Integer(42).to_string(), "42");
        assert_eq!(Token::StringLiteral(SmolStr::new("hi")).to_string(), "'hi'");
        assert_eq!(Token::Eof.to_string(), "<EOF>");
    }
}
