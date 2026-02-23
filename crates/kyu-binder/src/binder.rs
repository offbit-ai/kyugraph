//! Statement binder — the main entry point for semantic analysis.
//!
//! Transforms parser AST + catalog snapshot into fully resolved, typed
//! BoundStatement ready for the planner/executor.

use kyu_catalog::{CatalogContent, CatalogEntry};
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_expression::bound_expr::BoundExpression;
use kyu_expression::{try_coerce, FunctionRegistry};
use kyu_parser::ast;
use kyu_parser::span::Spanned;
use kyu_types::LogicalType;
use smol_str::SmolStr;

use crate::bound_statement::*;
use crate::expression_binder::{bind_expression, BindContext};
use crate::scope::BinderScope;

/// The semantic binder. Transforms parser AST into bound, typed statements.
pub struct Binder {
    catalog: CatalogContent,
    registry: FunctionRegistry,
    scope: BinderScope,
    bind_ctx: BindContext,
}

impl Binder {
    /// Create a new binder with a catalog snapshot and function registry.
    pub fn new(catalog: CatalogContent, registry: FunctionRegistry) -> Self {
        Self {
            catalog,
            registry,
            scope: BinderScope::new(),
            bind_ctx: BindContext::empty(),
        }
    }

    /// Set the bind-time context (parameters and environment).
    pub fn with_context(mut self, ctx: BindContext) -> Self {
        self.bind_ctx = ctx;
        self
    }

    /// Bind a parsed statement.
    pub fn bind(&mut self, stmt: &ast::Statement) -> KyuResult<BoundStatement> {
        match stmt {
            ast::Statement::Query(query) => {
                let bound = self.bind_query(query)?;
                Ok(BoundStatement::Query(bound))
            }
            ast::Statement::CreateNodeTable(s) => self.bind_create_node_table(s),
            ast::Statement::CreateRelTable(s) => self.bind_create_rel_table(s),
            ast::Statement::Drop(s) => self.bind_drop(s),
            ast::Statement::AlterTable(s) => self.bind_alter_table(s),
            ast::Statement::CopyFrom(s) => self.bind_copy_from(s),
            ast::Statement::Transaction(s) => Ok(BoundStatement::Transaction(s.clone())),
            _ => Err(KyuError::NotImplemented(
                "statement type not yet supported in binder".into(),
            )),
        }
    }

    fn bind_query(&mut self, query: &ast::Query) -> KyuResult<BoundQuery> {
        let mut bound_parts = Vec::with_capacity(query.parts.len());
        for part in &query.parts {
            bound_parts.push(self.bind_query_part(part)?);
        }

        let mut bound_unions = Vec::new();
        for (is_all, union_query) in &query.union_all {
            self.scope.push_frame();
            let bound = self.bind_query(union_query)?;
            self.scope.pop_frame();
            bound_unions.push((*is_all, bound));
        }

        let output_schema = self.derive_output_schema(&bound_parts)?;

        Ok(BoundQuery {
            parts: bound_parts,
            union_all: bound_unions,
            output_schema,
        })
    }

    fn bind_query_part(&mut self, part: &ast::QueryPart) -> KyuResult<BoundQueryPart> {
        let mut reading = Vec::new();
        for clause in &part.reading_clauses {
            reading.push(self.bind_reading_clause(clause)?);
        }

        let mut updating = Vec::new();
        for clause in &part.updating_clauses {
            updating.push(self.bind_updating_clause(clause)?);
        }

        let projection = if let Some(ref proj) = part.projection {
            Some(self.bind_projection(proj)?)
        } else {
            None
        };

        // If this is WITH (not RETURN), create new scope from projection.
        if !part.is_return
            && let Some(ref proj) = projection
        {
            let projected: Vec<_> = proj
                .items
                .iter()
                .map(|item| (item.alias.clone(), item.expression.result_type().clone()))
                .collect();
            self.scope.new_from_projection(projected);
        }

        Ok(BoundQueryPart {
            reading_clauses: reading,
            updating_clauses: updating,
            projection,
            is_return: part.is_return,
        })
    }

    fn bind_reading_clause(
        &mut self,
        clause: &ast::ReadingClause,
    ) -> KyuResult<BoundReadingClause> {
        match clause {
            ast::ReadingClause::Match(m) => {
                let bound = self.bind_match(m)?;
                Ok(BoundReadingClause::Match(bound))
            }
            ast::ReadingClause::Unwind(u) => {
                let bound = self.bind_unwind(u)?;
                Ok(BoundReadingClause::Unwind(bound))
            }
            _ => Err(KyuError::NotImplemented(
                "reading clause not yet supported".into(),
            )),
        }
    }

    fn bind_match(&mut self, m: &ast::MatchClause) -> KyuResult<BoundMatchClause> {
        let mut bound_patterns = Vec::with_capacity(m.patterns.len());
        for pattern in &m.patterns {
            bound_patterns.push(self.bind_pattern(pattern)?);
        }

        let where_clause = if let Some(ref where_expr) = m.where_clause {
            let bound = bind_expression(
                where_expr,
                &self.scope,
                &self.catalog,
                &self.registry,
                &self.bind_ctx,
            )?;
            let bound = try_coerce(bound, &LogicalType::Bool)?;
            Some(bound)
        } else {
            None
        };

        Ok(BoundMatchClause {
            is_optional: m.is_optional,
            patterns: bound_patterns,
            where_clause,
        })
    }

    fn bind_pattern(&mut self, pattern: &ast::Pattern) -> KyuResult<BoundPattern> {
        let mut bound_elements = Vec::with_capacity(pattern.elements.len());
        for element in &pattern.elements {
            match element {
                ast::PatternElement::Node(node) => {
                    bound_elements
                        .push(BoundPatternElement::Node(self.bind_node_pattern(node)?));
                }
                ast::PatternElement::Relationship(rel) => {
                    bound_elements.push(BoundPatternElement::Relationship(
                        self.bind_rel_pattern(rel)?,
                    ));
                }
            }
        }
        Ok(BoundPattern {
            elements: bound_elements,
        })
    }

    fn bind_node_pattern(
        &mut self,
        node: &ast::NodePattern,
    ) -> KyuResult<BoundNodePattern> {
        // Resolve label to table ID.
        let table_id = if !node.labels.is_empty() {
            let label = &node.labels[0].0;
            let entry = self.catalog.find_by_name(label).ok_or_else(|| {
                KyuError::Binder(format!("node table '{label}' not found"))
            })?;
            if !entry.is_node_table() {
                return Err(KyuError::Binder(format!(
                    "'{label}' is not a node table"
                )));
            }
            entry.table_id()
        } else {
            return Err(KyuError::Binder(
                "node patterns must specify a label".into(),
            ));
        };

        // Define variable in scope.
        let variable_index = if let Some(ref var) = node.variable {
            let info =
                self.scope
                    .define(&var.0, LogicalType::Node, Some(table_id))?;
            Some(info.index)
        } else {
            None
        };

        // Bind property filter expressions.
        let properties = self.bind_pattern_properties(table_id, &node.properties)?;

        Ok(BoundNodePattern {
            variable_index,
            table_id,
            properties,
        })
    }

    fn bind_rel_pattern(
        &mut self,
        rel: &ast::RelationshipPattern,
    ) -> KyuResult<BoundRelPattern> {
        // Resolve relationship type to table ID.
        let table_id = if !rel.rel_types.is_empty() {
            let rel_type = &rel.rel_types[0].0;
            let entry = self.catalog.find_by_name(rel_type).ok_or_else(|| {
                KyuError::Binder(format!("relationship table '{rel_type}' not found"))
            })?;
            if !entry.is_rel_table() {
                return Err(KyuError::Binder(format!(
                    "'{rel_type}' is not a relationship table"
                )));
            }
            entry.table_id()
        } else {
            return Err(KyuError::Binder(
                "relationship patterns must specify a type".into(),
            ));
        };

        // Define variable in scope.
        let variable_index = if let Some(ref var) = rel.variable {
            let info =
                self.scope
                    .define(&var.0, LogicalType::Rel, Some(table_id))?;
            Some(info.index)
        } else {
            None
        };

        // Bind property filter expressions.
        let properties = self.bind_pattern_properties(table_id, &rel.properties)?;

        Ok(BoundRelPattern {
            variable_index,
            table_id,
            direction: rel.direction,
            range: rel.range,
            properties,
        })
    }

    fn bind_pattern_properties(
        &self,
        table_id: TableId,
        properties: &Option<Vec<(Spanned<SmolStr>, Spanned<ast::Expression>)>>,
    ) -> KyuResult<Vec<(kyu_common::id::PropertyId, BoundExpression)>> {
        let mut bound_props = Vec::new();
        if let Some(props) = properties {
            let entry = self.catalog.find_by_id(table_id).ok_or_else(|| {
                KyuError::Binder(format!("table {table_id:?} not found"))
            })?;
            for (key, value) in props {
                let prop = find_property(entry, &key.0)?;
                let bound_val = bind_expression(
                    value,
                    &self.scope,
                    &self.catalog,
                    &self.registry,
                    &self.bind_ctx,
                )?;
                let bound_val = try_coerce(bound_val, &prop.data_type)?;
                bound_props.push((prop.id, bound_val));
            }
        }
        Ok(bound_props)
    }

    fn bind_unwind(&mut self, u: &ast::UnwindClause) -> KyuResult<BoundUnwindClause> {
        let bound_expr = bind_expression(
            &u.expression,
            &self.scope,
            &self.catalog,
            &self.registry,
            &self.bind_ctx,
        )?;
        let element_type = match bound_expr.result_type() {
            LogicalType::List(elem) => *elem.clone(),
            _ => LogicalType::Any,
        };
        let info = self.scope.define(&u.alias.0, element_type.clone(), None)?;
        Ok(BoundUnwindClause {
            expression: bound_expr,
            variable_index: info.index,
            element_type,
        })
    }

    fn bind_projection(
        &self,
        proj: &ast::ProjectionBody,
    ) -> KyuResult<BoundProjection> {
        let items = match &proj.items {
            ast::ProjectionItems::All => {
                // RETURN * — project all variables in current scope.
                self.scope
                    .current_variables()
                    .iter()
                    .map(|(name, info)| BoundProjectionItem {
                        expression: BoundExpression::Variable {
                            index: info.index,
                            result_type: info.data_type.clone(),
                        },
                        alias: name.clone(),
                    })
                    .collect()
            }
            ast::ProjectionItems::Expressions(exprs) => {
                let mut items = Vec::with_capacity(exprs.len());
                for (expr, alias) in exprs {
                    let bound = bind_expression(
                        expr,
                        &self.scope,
                        &self.catalog,
                        &self.registry,
                        &self.bind_ctx,
                    )?;
                    let alias = alias
                        .as_ref()
                        .map(|a| a.0.clone())
                        .unwrap_or_else(|| infer_alias(&expr.0));
                    items.push(BoundProjectionItem {
                        expression: bound,
                        alias,
                    });
                }
                items
            }
        };

        let order_by = proj
            .order_by
            .iter()
            .map(|(expr, order)| {
                let bound = bind_expression(
                    expr,
                    &self.scope,
                    &self.catalog,
                    &self.registry,
                    &self.bind_ctx,
                )?;
                Ok((bound, *order))
            })
            .collect::<KyuResult<Vec<_>>>()?;

        let skip = proj
            .skip
            .as_ref()
            .map(|e| {
                bind_expression(e, &self.scope, &self.catalog, &self.registry, &self.bind_ctx)
            })
            .transpose()?;

        let limit = proj
            .limit
            .as_ref()
            .map(|e| {
                bind_expression(e, &self.scope, &self.catalog, &self.registry, &self.bind_ctx)
            })
            .transpose()?;

        Ok(BoundProjection {
            distinct: proj.distinct,
            items,
            order_by,
            skip,
            limit,
        })
    }

    fn bind_updating_clause(
        &mut self,
        clause: &ast::UpdatingClause,
    ) -> KyuResult<BoundUpdatingClause> {
        match clause {
            ast::UpdatingClause::Create(patterns) => {
                let mut bound = Vec::with_capacity(patterns.len());
                for p in patterns {
                    bound.push(self.bind_pattern(p)?);
                }
                Ok(BoundUpdatingClause::Create(bound))
            }
            ast::UpdatingClause::Set(items) => {
                let mut bound = Vec::with_capacity(items.len());
                for item in items {
                    if let ast::SetItem::Property { entity, value } = item {
                        let bound_entity = bind_expression(
                            entity,
                            &self.scope,
                            &self.catalog,
                            &self.registry,
                            &self.bind_ctx,
                        )?;
                        let bound_value = bind_expression(
                            value,
                            &self.scope,
                            &self.catalog,
                            &self.registry,
                            &self.bind_ctx,
                        )?;
                        // Extract property_id from the bound entity (should be a Property).
                        let property_id = match &bound_entity {
                            BoundExpression::Property { property_id, .. } => *property_id,
                            _ => {
                                return Err(KyuError::Binder(
                                    "SET target must be a property".into(),
                                ))
                            }
                        };
                        bound.push(BoundSetItem {
                            object: bound_entity,
                            property_id,
                            value: bound_value,
                        });
                    } else {
                        return Err(KyuError::NotImplemented(
                            "SET variant not yet supported".into(),
                        ));
                    }
                }
                Ok(BoundUpdatingClause::Set(bound))
            }
            ast::UpdatingClause::Delete(del) => {
                let exprs = del
                    .expressions
                    .iter()
                    .map(|e| {
                        bind_expression(
                            e,
                            &self.scope,
                            &self.catalog,
                            &self.registry,
                            &self.bind_ctx,
                        )
                    })
                    .collect::<KyuResult<Vec<_>>>()?;
                Ok(BoundUpdatingClause::Delete(BoundDeleteClause {
                    detach: del.detach,
                    expressions: exprs,
                }))
            }
            _ => Err(KyuError::NotImplemented(
                "updating clause not yet supported".into(),
            )),
        }
    }

    fn derive_output_schema(
        &self,
        parts: &[BoundQueryPart],
    ) -> KyuResult<Vec<(SmolStr, LogicalType)>> {
        if let Some(last) = parts.last()
            && let Some(ref proj) = last.projection
        {
            return Ok(proj
                .items
                .iter()
                .map(|item| (item.alias.clone(), item.expression.result_type().clone()))
                .collect());
        }
        Ok(Vec::new())
    }

    // ---- DDL binders ----

    fn bind_create_node_table(
        &self,
        stmt: &ast::CreateNodeTable,
    ) -> KyuResult<BoundStatement> {
        if !stmt.if_not_exists && self.catalog.contains_name(&stmt.name.0) {
            return Err(KyuError::Binder(format!(
                "table '{}' already exists",
                stmt.name.0
            )));
        }

        let mut columns = Vec::with_capacity(stmt.columns.len());
        let mut primary_key_idx = None;

        for (i, col) in stmt.columns.iter().enumerate() {
            let data_type = kyu_catalog::resolve_type(&col.data_type.0)?;
            let default_value = col
                .default_value
                .as_ref()
                .map(|e| {
                    bind_expression(
                        e,
                        &self.scope,
                        &self.catalog,
                        &self.registry,
                        &self.bind_ctx,
                    )
                })
                .transpose()?;

            if col.name.0.to_lowercase() == stmt.primary_key.0.to_lowercase() {
                primary_key_idx = Some(i);
            }

            columns.push(BoundColumnDef {
                property_id: kyu_common::id::PropertyId(i as u32),
                name: col.name.0.clone(),
                data_type,
                default_value,
            });
        }

        let primary_key_idx = primary_key_idx.ok_or_else(|| {
            KyuError::Binder(format!(
                "primary key '{}' not found in columns",
                stmt.primary_key.0
            ))
        })?;

        Ok(BoundStatement::CreateNodeTable(BoundCreateNodeTable {
            table_id: TableId(0), // Placeholder — actual ID assigned at commit.
            name: stmt.name.0.clone(),
            columns,
            primary_key_idx,
        }))
    }

    fn bind_create_rel_table(
        &self,
        stmt: &ast::CreateRelTable,
    ) -> KyuResult<BoundStatement> {
        if !stmt.if_not_exists && self.catalog.contains_name(&stmt.name.0) {
            return Err(KyuError::Binder(format!(
                "table '{}' already exists",
                stmt.name.0
            )));
        }

        let from_entry = self.catalog.find_by_name(&stmt.from_table.0).ok_or_else(|| {
            KyuError::Binder(format!("FROM table '{}' not found", stmt.from_table.0))
        })?;
        let to_entry = self.catalog.find_by_name(&stmt.to_table.0).ok_or_else(|| {
            KyuError::Binder(format!("TO table '{}' not found", stmt.to_table.0))
        })?;

        let mut columns = Vec::with_capacity(stmt.columns.len());
        for (i, col) in stmt.columns.iter().enumerate() {
            let data_type = kyu_catalog::resolve_type(&col.data_type.0)?;
            columns.push(BoundColumnDef {
                property_id: kyu_common::id::PropertyId(i as u32),
                name: col.name.0.clone(),
                data_type,
                default_value: None,
            });
        }

        Ok(BoundStatement::CreateRelTable(BoundCreateRelTable {
            table_id: TableId(0),
            name: stmt.name.0.clone(),
            from_table_id: from_entry.table_id(),
            to_table_id: to_entry.table_id(),
            columns,
        }))
    }

    fn bind_drop(&self, stmt: &ast::DropStatement) -> KyuResult<BoundStatement> {
        let entry = self.catalog.find_by_name(&stmt.name.0).ok_or_else(|| {
            KyuError::Binder(format!("table '{}' not found", stmt.name.0))
        })?;
        Ok(BoundStatement::Drop(BoundDrop {
            table_id: entry.table_id(),
            name: entry.name().clone(),
        }))
    }

    fn bind_alter_table(&self, stmt: &ast::AlterTable) -> KyuResult<BoundStatement> {
        let entry = self.catalog.find_by_name(&stmt.table_name.0).ok_or_else(|| {
            KyuError::Binder(format!("table '{}' not found", stmt.table_name.0))
        })?;
        let table_id = entry.table_id();

        let action = match &stmt.action {
            ast::AlterAction::AddColumn(col) => {
                let data_type = kyu_catalog::resolve_type(&col.data_type.0)?;
                BoundAlterAction::AddColumn(BoundColumnDef {
                    property_id: kyu_common::id::PropertyId(0), // assigned at commit
                    name: col.name.0.clone(),
                    data_type,
                    default_value: None,
                })
            }
            ast::AlterAction::DropColumn(col_name) => {
                let prop = find_property(entry, &col_name.0)?;
                BoundAlterAction::DropColumn {
                    property_id: prop.id,
                }
            }
            ast::AlterAction::RenameColumn { old_name, new_name } => {
                let prop = find_property(entry, &old_name.0)?;
                BoundAlterAction::RenameColumn {
                    property_id: prop.id,
                    new_name: new_name.0.clone(),
                }
            }
            ast::AlterAction::RenameTable(new_name) => {
                BoundAlterAction::RenameTable {
                    new_name: new_name.0.clone(),
                }
            }
            ast::AlterAction::Comment(comment) => {
                BoundAlterAction::Comment(comment.clone())
            }
        };

        Ok(BoundStatement::AlterTable(BoundAlterTable {
            table_id,
            action,
        }))
    }

    fn bind_copy_from(&self, stmt: &ast::CopyFrom) -> KyuResult<BoundStatement> {
        let entry = self.catalog.find_by_name(&stmt.table_name.0).ok_or_else(|| {
            KyuError::Binder(format!("table '{}' not found", stmt.table_name.0))
        })?;
        let bound_source = bind_expression(
            &stmt.source,
            &self.scope,
            &self.catalog,
            &self.registry,
            &self.bind_ctx,
        )?;
        Ok(BoundStatement::CopyFrom(BoundCopyFrom {
            table_id: entry.table_id(),
            source: bound_source,
        }))
    }
}

fn find_property<'a>(
    entry: &'a CatalogEntry,
    name: &str,
) -> KyuResult<&'a kyu_catalog::Property> {
    let lower = name.to_lowercase();
    entry
        .properties()
        .iter()
        .find(|p| p.name.to_lowercase() == lower)
        .ok_or_else(|| {
            KyuError::Binder(format!(
                "property '{}' not found on table '{}'",
                name,
                entry.name()
            ))
        })
}

/// Infer an alias from an expression (for unaliased projections).
fn infer_alias(expr: &ast::Expression) -> SmolStr {
    match expr {
        ast::Expression::Variable(name) => name.clone(),
        ast::Expression::Property { key, .. } => key.0.clone(),
        ast::Expression::FunctionCall { name, .. } => {
            let joined: String = name.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>().join(".");
            SmolStr::new(joined)
        }
        ast::Expression::CountStar => SmolStr::new("count(*)"),
        _ => SmolStr::new("expr"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_catalog::{CatalogContent, NodeTableEntry, Property, RelTableEntry};
    use kyu_common::id::PropertyId;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: TableId(0),
                name: SmolStr::new("Person"),
                properties: vec![
                    Property::new(PropertyId(0), "name", LogicalType::String, true),
                    Property::new(PropertyId(1), "age", LogicalType::Int64, false),
                ],
                primary_key_idx: 0,
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
            .add_rel_table(RelTableEntry {
                table_id: TableId(1),
                name: SmolStr::new("KNOWS"),
                from_table_id: TableId(0),
                to_table_id: TableId(0),
                properties: vec![Property::new(
                    PropertyId(2),
                    "since",
                    LogicalType::Int64,
                    false,
                )],
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
    }

    fn parse_and_bind(cypher: &str, catalog: &CatalogContent) -> KyuResult<BoundStatement> {
        let result = kyu_parser::parse(cypher);
        let stmt = result.ast.ok_or_else(|| {
            KyuError::Binder(format!(
                "parse failed: {:?}",
                result.errors
            ))
        })?;
        let mut binder = Binder::new(catalog.clone(), FunctionRegistry::with_builtins());
        binder.bind(&stmt)
    }

    fn parse_and_bind_with_ctx(
        cypher: &str,
        catalog: &CatalogContent,
        ctx: BindContext,
    ) -> KyuResult<BoundStatement> {
        let result = kyu_parser::parse(cypher);
        let stmt = result.ast.ok_or_else(|| {
            KyuError::Binder(format!(
                "parse failed: {:?}",
                result.errors
            ))
        })?;
        let mut binder = Binder::new(catalog.clone(), FunctionRegistry::with_builtins())
            .with_context(ctx);
        binder.bind(&stmt)
    }

    #[test]
    fn bind_match_return_property() {
        let catalog = make_catalog();
        let bound = parse_and_bind("MATCH (p:Person) RETURN p.name", &catalog).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema.len(), 1);
        assert_eq!(query.output_schema[0].1, LogicalType::String);
    }

    #[test]
    fn bind_match_where_return() {
        let catalog = make_catalog();
        let bound =
            parse_and_bind("MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age", &catalog)
                .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema.len(), 2);
        assert_eq!(query.output_schema[0].1, LogicalType::String);
        assert_eq!(query.output_schema[1].1, LogicalType::Int64);

        // Verify WHERE clause exists.
        let match_clause = match &query.parts[0].reading_clauses[0] {
            BoundReadingClause::Match(m) => m,
            _ => panic!("expected match"),
        };
        assert!(match_clause.where_clause.is_some());
    }

    #[test]
    fn bind_match_relationship() {
        let catalog = make_catalog();
        let bound = parse_and_bind(
            "MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, b.name",
            &catalog,
        )
        .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema.len(), 2);

        let match_clause = match &query.parts[0].reading_clauses[0] {
            BoundReadingClause::Match(m) => m,
            _ => panic!("expected match"),
        };
        // Should have 3 pattern elements: node, rel, node.
        assert_eq!(match_clause.patterns[0].elements.len(), 3);
    }

    #[test]
    fn bind_return_arithmetic_coercion() {
        let catalog = make_catalog();
        let bound = parse_and_bind("RETURN 1 + 2.0 AS result", &catalog).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::Double);
    }

    #[test]
    fn bind_return_function() {
        let catalog = make_catalog();
        let bound = parse_and_bind("RETURN upper('hello') AS up", &catalog).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::String);
    }

    #[test]
    fn bind_return_count_star() {
        let catalog = make_catalog();
        let bound = parse_and_bind("RETURN count(*) AS cnt", &catalog).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::Int64);
    }

    #[test]
    fn bind_return_case() {
        let catalog = make_catalog();
        let bound =
            parse_and_bind("RETURN CASE WHEN true THEN 1 ELSE 2 END AS val", &catalog)
                .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::Int64);
    }

    #[test]
    fn bind_return_star() {
        let catalog = make_catalog();
        let bound =
            parse_and_bind("MATCH (p:Person) RETURN *", &catalog).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        // RETURN * should expand to all variables in scope (just 'p').
        assert_eq!(query.output_schema.len(), 1);
    }

    #[test]
    fn bind_undefined_variable_error() {
        let catalog = make_catalog();
        let result = parse_and_bind("RETURN x.name", &catalog);
        assert!(result.is_err());
    }

    #[test]
    fn bind_unknown_label_error() {
        let catalog = make_catalog();
        let result = parse_and_bind("MATCH (p:UnknownTable) RETURN p", &catalog);
        assert!(result.is_err());
    }

    #[test]
    fn bind_type_mismatch_error() {
        let catalog = make_catalog();
        let result = parse_and_bind("RETURN 'hello' + 42", &catalog);
        assert!(result.is_err());
    }

    #[test]
    fn bind_drop_table() {
        let catalog = make_catalog();
        let bound = parse_and_bind("DROP TABLE Person", &catalog).unwrap();
        match bound {
            BoundStatement::Drop(d) => {
                assert_eq!(d.table_id, TableId(0));
                assert_eq!(d.name.as_str(), "Person");
            }
            _ => panic!("expected Drop"),
        }
    }

    #[test]
    fn bind_drop_nonexistent_error() {
        let catalog = make_catalog();
        let result = parse_and_bind("DROP TABLE Nonexistent", &catalog);
        assert!(result.is_err());
    }

    #[test]
    fn bind_with_scope_chaining() {
        let catalog = make_catalog();
        let bound = parse_and_bind(
            "MATCH (p:Person) WITH p.name AS name RETURN upper(name) AS up",
            &catalog,
        )
        .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        // Final output should be String (upper returns String).
        assert_eq!(query.output_schema[0].1, LogicalType::String);
    }

    #[test]
    fn bind_create_node_table() {
        let catalog = make_catalog();
        let bound = parse_and_bind(
            "CREATE NODE TABLE Movie (id INT64, title STRING, PRIMARY KEY (id))",
            &catalog,
        )
        .unwrap();
        match bound {
            BoundStatement::CreateNodeTable(c) => {
                assert_eq!(c.name.as_str(), "Movie");
                assert_eq!(c.columns.len(), 2);
                assert_eq!(c.primary_key_idx, 0);
                assert_eq!(c.columns[0].data_type, LogicalType::Int64);
                assert_eq!(c.columns[1].data_type, LogicalType::String);
            }
            _ => panic!("expected CreateNodeTable"),
        }
    }

    #[test]
    fn bind_create_rel_table() {
        let catalog = make_catalog();
        let bound = parse_and_bind(
            "CREATE REL TABLE ACTED_IN (FROM Person TO Person, role STRING)",
            &catalog,
        )
        .unwrap();
        match bound {
            BoundStatement::CreateRelTable(c) => {
                assert_eq!(c.name.as_str(), "ACTED_IN");
                assert_eq!(c.from_table_id, TableId(0));
                assert_eq!(c.to_table_id, TableId(0));
                assert_eq!(c.columns.len(), 1);
            }
            _ => panic!("expected CreateRelTable"),
        }
    }

    #[test]
    fn bind_match_with_limit() {
        let catalog = make_catalog();
        let bound = parse_and_bind(
            "MATCH (p:Person) RETURN p.name LIMIT 10",
            &catalog,
        )
        .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        let proj = query.parts[0].projection.as_ref().unwrap();
        assert!(proj.limit.is_some());
    }

    // ---- Parameterized query tests ----

    #[test]
    fn bind_parameterized_where() {
        let catalog = make_catalog();
        let mut ctx = BindContext::empty();
        ctx.params
            .insert(SmolStr::new("min_age"), kyu_types::TypedValue::Int64(25));
        let bound = parse_and_bind_with_ctx(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
            &catalog,
            ctx,
        )
        .unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema.len(), 1);
        assert_eq!(query.output_schema[0].1, LogicalType::String);
    }

    #[test]
    fn bind_parameterized_return() {
        let catalog = make_catalog();
        let mut ctx = BindContext::empty();
        ctx.params
            .insert(SmolStr::new("x"), kyu_types::TypedValue::Int64(42));
        let bound = parse_and_bind_with_ctx("RETURN $x AS val", &catalog, ctx).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::Int64);
    }

    #[test]
    fn bind_missing_param_error() {
        let catalog = make_catalog();
        let result = parse_and_bind("RETURN $missing AS val", &catalog);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unresolved parameter '$missing'"));
    }

    #[test]
    fn bind_env_in_return() {
        let catalog = make_catalog();
        let mut ctx = BindContext::empty();
        ctx.env.insert(
            SmolStr::new("PREFIX"),
            kyu_types::TypedValue::String(SmolStr::new("hello_")),
        );
        let bound =
            parse_and_bind_with_ctx("RETURN env('PREFIX') AS prefix", &catalog, ctx).unwrap();
        let query = match bound {
            BoundStatement::Query(q) => q,
            _ => panic!("expected query"),
        };
        assert_eq!(query.output_schema[0].1, LogicalType::String);
    }
}
