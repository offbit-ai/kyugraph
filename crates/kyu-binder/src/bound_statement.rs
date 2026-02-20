//! Bound statement types — fully resolved, typed statements ready for planning.
//!
//! All names are resolved to IDs, all types are inferred.

use kyu_common::id::{PropertyId, TableId};
use kyu_expression::BoundExpression;
use kyu_parser::ast::{Direction, SortOrder, TransactionStatement};
use kyu_types::LogicalType;
use smol_str::SmolStr;

// ---------------------------------------------------------------------------
// Top-level statement
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum BoundStatement {
    Query(BoundQuery),
    CreateNodeTable(BoundCreateNodeTable),
    CreateRelTable(BoundCreateRelTable),
    Drop(BoundDrop),
    AlterTable(BoundAlterTable),
    CopyFrom(BoundCopyFrom),
    Transaction(TransactionStatement),
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct BoundQuery {
    pub parts: Vec<BoundQueryPart>,
    pub union_all: Vec<(bool, BoundQuery)>,
    pub output_schema: Vec<(SmolStr, LogicalType)>,
}

#[derive(Clone, Debug)]
pub struct BoundQueryPart {
    pub reading_clauses: Vec<BoundReadingClause>,
    pub updating_clauses: Vec<BoundUpdatingClause>,
    pub projection: Option<BoundProjection>,
    pub is_return: bool,
}

// ---------------------------------------------------------------------------
// Reading clauses
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum BoundReadingClause {
    Match(BoundMatchClause),
    Unwind(BoundUnwindClause),
}

#[derive(Clone, Debug)]
pub struct BoundMatchClause {
    pub is_optional: bool,
    pub patterns: Vec<BoundPattern>,
    pub where_clause: Option<BoundExpression>,
}

#[derive(Clone, Debug)]
pub struct BoundPattern {
    pub elements: Vec<BoundPatternElement>,
}

#[derive(Clone, Debug)]
pub enum BoundPatternElement {
    Node(BoundNodePattern),
    Relationship(BoundRelPattern),
}

#[derive(Clone, Debug)]
pub struct BoundNodePattern {
    pub variable_index: Option<u32>,
    pub table_id: TableId,
    pub properties: Vec<(PropertyId, BoundExpression)>,
}

#[derive(Clone, Debug)]
pub struct BoundRelPattern {
    pub variable_index: Option<u32>,
    pub table_id: TableId,
    pub direction: Direction,
    pub range: Option<(Option<u32>, Option<u32>)>,
    pub properties: Vec<(PropertyId, BoundExpression)>,
}

#[derive(Clone, Debug)]
pub struct BoundUnwindClause {
    pub expression: BoundExpression,
    pub variable_index: u32,
    pub element_type: LogicalType,
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct BoundProjection {
    pub distinct: bool,
    pub items: Vec<BoundProjectionItem>,
    pub order_by: Vec<(BoundExpression, SortOrder)>,
    pub skip: Option<BoundExpression>,
    pub limit: Option<BoundExpression>,
}

#[derive(Clone, Debug)]
pub struct BoundProjectionItem {
    pub expression: BoundExpression,
    pub alias: SmolStr,
}

// ---------------------------------------------------------------------------
// Updating clauses
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum BoundUpdatingClause {
    Create(Vec<BoundPattern>),
    Set(Vec<BoundSetItem>),
    Delete(BoundDeleteClause),
}

#[derive(Clone, Debug)]
pub struct BoundSetItem {
    pub object: BoundExpression,
    pub property_id: PropertyId,
    pub value: BoundExpression,
}

#[derive(Clone, Debug)]
pub struct BoundDeleteClause {
    pub detach: bool,
    pub expressions: Vec<BoundExpression>,
}

// ---------------------------------------------------------------------------
// DDL bound types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct BoundCreateNodeTable {
    pub table_id: TableId,
    pub name: SmolStr,
    pub columns: Vec<BoundColumnDef>,
    pub primary_key_idx: usize,
}

#[derive(Clone, Debug)]
pub struct BoundCreateRelTable {
    pub table_id: TableId,
    pub name: SmolStr,
    pub from_table_id: TableId,
    pub to_table_id: TableId,
    pub columns: Vec<BoundColumnDef>,
}

#[derive(Clone, Debug)]
pub struct BoundColumnDef {
    pub property_id: PropertyId,
    pub name: SmolStr,
    pub data_type: LogicalType,
    pub default_value: Option<BoundExpression>,
}

#[derive(Clone, Debug)]
pub struct BoundDrop {
    pub table_id: TableId,
    pub name: SmolStr,
}

#[derive(Clone, Debug)]
pub struct BoundAlterTable {
    pub table_id: TableId,
    pub action: BoundAlterAction,
}

#[derive(Clone, Debug)]
pub enum BoundAlterAction {
    AddColumn(BoundColumnDef),
    DropColumn { property_id: PropertyId },
    RenameColumn {
        property_id: PropertyId,
        new_name: SmolStr,
    },
    RenameTable {
        new_name: SmolStr,
    },
    Comment(SmolStr),
}

#[derive(Clone, Debug)]
pub struct BoundCopyFrom {
    pub table_id: TableId,
    pub source: BoundExpression,
}
