//! AST node types produced by the Cypher parser.

use crate::span::{Span, Spanned};
use smol_str::SmolStr;

// =============================================================================
// Top-Level Statement
// =============================================================================

/// A complete parsed statement.
#[derive(Clone, Debug)]
pub enum Statement {
    Query(Query),
    CreateNodeTable(CreateNodeTable),
    CreateRelTable(CreateRelTable),
    Drop(DropStatement),
    AlterTable(AlterTable),
    CopyFrom(CopyFrom),
    CopyTo(CopyTo),
    StandaloneCall(StandaloneCall),
    Transaction(TransactionStatement),
    ExportDatabase(ExportDatabase),
    ImportDatabase(ImportDatabase),
    AttachDatabase(AttachDatabase),
    UseDatabase(UseDatabase),
    CreateMacro(CreateMacro),
    InstallExtension(InstallExtension),
    LoadExtension(LoadExtension),
    Explain(Box<Statement>),
    Profile(Box<Statement>),
}

// =============================================================================
// Query
// =============================================================================

#[derive(Clone, Debug)]
pub struct Query {
    pub parts: Vec<QueryPart>,
    /// UNION / UNION ALL chains: (is_all, query).
    pub union_all: Vec<(bool, Query)>,
}

#[derive(Clone, Debug)]
pub struct QueryPart {
    pub reading_clauses: Vec<ReadingClause>,
    pub updating_clauses: Vec<UpdatingClause>,
    pub projection: Option<ProjectionBody>,
    /// `true` = RETURN, `false` = WITH.
    pub is_return: bool,
}

// =============================================================================
// Reading Clauses
// =============================================================================

#[derive(Clone, Debug)]
pub enum ReadingClause {
    Match(MatchClause),
    Unwind(UnwindClause),
    InQueryCall(InQueryCall),
    LoadFrom(LoadFrom),
}

#[derive(Clone, Debug)]
pub struct MatchClause {
    pub is_optional: bool,
    pub patterns: Vec<Pattern>,
    pub where_clause: Option<Spanned<Expression>>,
}

#[derive(Clone, Debug)]
pub struct UnwindClause {
    pub expression: Spanned<Expression>,
    pub alias: Spanned<SmolStr>,
}

#[derive(Clone, Debug)]
pub struct InQueryCall {
    pub procedure: Spanned<SmolStr>,
    pub args: Vec<Spanned<Expression>>,
    pub yield_items: Vec<Spanned<SmolStr>>,
}

#[derive(Clone, Debug)]
pub struct LoadFrom {
    pub source: Spanned<Expression>,
}

// =============================================================================
// Patterns
// =============================================================================

#[derive(Clone, Debug)]
pub struct Pattern {
    pub variable: Option<Spanned<SmolStr>>,
    pub elements: Vec<PatternElement>,
}

#[derive(Clone, Debug)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

#[derive(Clone, Debug)]
pub struct NodePattern {
    pub variable: Option<Spanned<SmolStr>>,
    pub labels: Vec<Spanned<SmolStr>>,
    pub properties: Option<Vec<(Spanned<SmolStr>, Spanned<Expression>)>>,
    pub span: Span,
}

#[derive(Clone, Debug)]
pub struct RelationshipPattern {
    pub variable: Option<Spanned<SmolStr>>,
    pub rel_types: Vec<Spanned<SmolStr>>,
    pub direction: Direction,
    /// Variable-length range: `[*min..max]`.
    pub range: Option<(Option<u32>, Option<u32>)>,
    pub properties: Option<Vec<(Spanned<SmolStr>, Spanned<Expression>)>>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,  // <-[]-
    Right, // -[]->
    Both,  // -[]-
}

// =============================================================================
// Updating Clauses
// =============================================================================

#[derive(Clone, Debug)]
pub enum UpdatingClause {
    Create(Vec<Pattern>),
    Merge(MergeClause),
    Set(Vec<SetItem>),
    Delete(DeleteClause),
    Remove(Vec<RemoveItem>),
}

#[derive(Clone, Debug)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_match: Vec<SetItem>,
    pub on_create: Vec<SetItem>,
}

#[derive(Clone, Debug)]
pub struct DeleteClause {
    pub detach: bool,
    pub expressions: Vec<Spanned<Expression>>,
}

#[derive(Clone, Debug)]
pub enum SetItem {
    Property {
        entity: Spanned<Expression>,
        value: Spanned<Expression>,
    },
    AllProperties {
        entity: Spanned<SmolStr>,
        value: Spanned<Expression>,
    },
    Labels {
        entity: Spanned<SmolStr>,
        labels: Vec<Spanned<SmolStr>>,
    },
}

#[derive(Clone, Debug)]
pub enum RemoveItem {
    Property(Spanned<Expression>),
    Labels {
        entity: Spanned<SmolStr>,
        labels: Vec<Spanned<SmolStr>>,
    },
}

// =============================================================================
// Projection
// =============================================================================

#[derive(Clone, Debug)]
pub struct ProjectionBody {
    pub distinct: bool,
    pub items: ProjectionItems,
    pub order_by: Vec<(Spanned<Expression>, SortOrder)>,
    pub skip: Option<Spanned<Expression>>,
    pub limit: Option<Spanned<Expression>>,
}

#[derive(Clone, Debug)]
pub enum ProjectionItems {
    All,
    Expressions(Vec<(Spanned<Expression>, Option<Spanned<SmolStr>>)>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

// =============================================================================
// Expressions
// =============================================================================

#[derive(Clone, Debug)]
pub enum Expression {
    Literal(Literal),
    Variable(SmolStr),
    Parameter(SmolStr),
    Property {
        object: Box<Spanned<Expression>>,
        key: Spanned<SmolStr>,
    },
    FunctionCall {
        name: Vec<Spanned<SmolStr>>,
        distinct: bool,
        args: Vec<Spanned<Expression>>,
    },
    CountStar,
    UnaryOp {
        op: UnaryOp,
        operand: Box<Spanned<Expression>>,
    },
    BinaryOp {
        left: Box<Spanned<Expression>>,
        op: BinaryOp,
        right: Box<Spanned<Expression>>,
    },
    Comparison {
        left: Box<Spanned<Expression>>,
        ops: Vec<(ComparisonOp, Spanned<Expression>)>,
    },
    IsNull {
        expr: Box<Spanned<Expression>>,
        negated: bool,
    },
    ListLiteral(Vec<Spanned<Expression>>),
    MapLiteral(Vec<(Spanned<SmolStr>, Spanned<Expression>)>),
    Subscript {
        expr: Box<Spanned<Expression>>,
        index: Box<Spanned<Expression>>,
    },
    Slice {
        expr: Box<Spanned<Expression>>,
        from: Option<Box<Spanned<Expression>>>,
        to: Option<Box<Spanned<Expression>>>,
    },
    Case {
        operand: Option<Box<Spanned<Expression>>>,
        whens: Vec<(Spanned<Expression>, Spanned<Expression>)>,
        else_expr: Option<Box<Spanned<Expression>>>,
    },
    ExistsSubquery(Box<Query>),
    CountSubquery(Box<Query>),
    Quantifier {
        kind: QuantifierKind,
        variable: Spanned<SmolStr>,
        list: Box<Spanned<Expression>>,
        predicate: Box<Spanned<Expression>>,
    },
    StringOp {
        left: Box<Spanned<Expression>>,
        op: StringOp,
        right: Box<Spanned<Expression>>,
    },
    InList {
        expr: Box<Spanned<Expression>>,
        list: Box<Spanned<Expression>>,
        negated: bool,
    },
    /// Label predicate: `a:Label` (true if node has given label)
    HasLabel {
        expr: Box<Spanned<Expression>>,
        labels: Vec<Spanned<SmolStr>>,
    },
    /// List comprehension: `[x IN list | expr]` or `[x IN list WHERE pred | expr]`
    ListComprehension {
        variable: Spanned<SmolStr>,
        list: Box<Spanned<Expression>>,
        filter: Option<Box<Spanned<Expression>>>,
        projection: Option<Box<Spanned<Expression>>>,
    },
}

#[derive(Clone, Debug)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(SmolStr),
    Bool(bool),
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Minus,
    BitwiseNot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    And,
    Or,
    Xor,
    BitwiseAnd,
    BitwiseOr,
    ShiftLeft,
    ShiftRight,
    Concat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    RegexMatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringOp {
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantifierKind {
    All,
    Any,
    None,
    Single,
}

// =============================================================================
// DDL Statements
// =============================================================================

#[derive(Clone, Debug)]
pub struct CreateNodeTable {
    pub name: Spanned<SmolStr>,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Spanned<SmolStr>,
}

#[derive(Clone, Debug)]
pub struct CreateRelTable {
    pub name: Spanned<SmolStr>,
    pub if_not_exists: bool,
    pub from_table: Spanned<SmolStr>,
    pub to_table: Spanned<SmolStr>,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: Spanned<SmolStr>,
    pub data_type: Spanned<SmolStr>,
    pub default_value: Option<Spanned<Expression>>,
}

#[derive(Clone, Debug)]
pub struct DropStatement {
    pub object_type: DropObjectType,
    pub name: Spanned<SmolStr>,
    pub if_exists: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropObjectType {
    Table,
    Sequence,
}

#[derive(Clone, Debug)]
pub struct AlterTable {
    pub table_name: Spanned<SmolStr>,
    pub action: AlterAction,
}

#[derive(Clone, Debug)]
pub enum AlterAction {
    AddColumn(ColumnDefinition),
    DropColumn(Spanned<SmolStr>),
    RenameColumn {
        old_name: Spanned<SmolStr>,
        new_name: Spanned<SmolStr>,
    },
    RenameTable(Spanned<SmolStr>),
    Comment(SmolStr),
}

// =============================================================================
// Copy / Import / Export
// =============================================================================

#[derive(Clone, Debug)]
pub struct CopyFrom {
    pub table_name: Spanned<SmolStr>,
    pub source: Spanned<Expression>,
    pub options: Vec<(Spanned<SmolStr>, Spanned<Expression>)>,
}

#[derive(Clone, Debug)]
pub struct CopyTo {
    pub source: CopyToSource,
    pub destination: Spanned<Expression>,
    pub options: Vec<(Spanned<SmolStr>, Spanned<Expression>)>,
}

#[derive(Clone, Debug)]
pub enum CopyToSource {
    Table(Spanned<SmolStr>),
    Query(Box<Query>),
}

#[derive(Clone, Debug)]
pub struct ExportDatabase {
    pub path: Spanned<Expression>,
    pub options: Vec<(Spanned<SmolStr>, Spanned<Expression>)>,
}

#[derive(Clone, Debug)]
pub struct ImportDatabase {
    pub path: Spanned<Expression>,
}

#[derive(Clone, Debug)]
pub struct AttachDatabase {
    pub path: Spanned<Expression>,
    pub alias: Option<Spanned<SmolStr>>,
    pub db_type: Option<Spanned<SmolStr>>,
}

#[derive(Clone, Debug)]
pub struct UseDatabase {
    pub name: Spanned<SmolStr>,
}

// =============================================================================
// Procedures / Extensions / Macros
// =============================================================================

#[derive(Clone, Debug)]
pub struct StandaloneCall {
    pub procedure: Spanned<SmolStr>,
    pub args: Vec<Spanned<Expression>>,
}

#[derive(Clone, Debug)]
pub struct CreateMacro {
    pub name: Spanned<SmolStr>,
    pub params: Vec<Spanned<SmolStr>>,
    pub body: Spanned<Expression>,
}

#[derive(Clone, Debug)]
pub struct InstallExtension {
    pub name: Spanned<SmolStr>,
}

#[derive(Clone, Debug)]
pub struct LoadExtension {
    pub path: Spanned<Expression>,
}

// =============================================================================
// Transaction
// =============================================================================

#[derive(Clone, Debug)]
pub enum TransactionStatement {
    Begin(TransactionMode),
    Commit,
    Rollback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    ReadOnly,
    ReadWrite,
}
