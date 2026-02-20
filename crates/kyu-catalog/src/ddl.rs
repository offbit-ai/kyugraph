//! DDL execution: translates parser AST nodes into catalog mutations.

use kyu_common::{KyuError, KyuResult, TableId};
use kyu_parser::ast;
use smol_str::SmolStr;

use crate::catalog::CatalogContent;
use crate::entry::{CatalogEntry, NodeTableEntry, Property, RelTableEntry};
use crate::type_resolver::resolve_type;

/// Execute a CREATE NODE TABLE statement.
pub fn execute_create_node_table(
    content: &mut CatalogContent,
    stmt: &ast::CreateNodeTable,
) -> KyuResult<TableId> {
    let name = &stmt.name.0;

    if stmt.if_not_exists && content.contains_name(name) {
        // Silently succeed
        return Ok(content.find_by_name(name).unwrap().table_id());
    }

    if content.contains_name(name) {
        return Err(KyuError::Catalog(format!(
            "table '{name}' already exists"
        )));
    }

    // Resolve columns
    let mut properties = Vec::with_capacity(stmt.columns.len());
    let mut primary_key_idx = None;
    let pk_name_lower = stmt.primary_key.0.to_lowercase();

    for col in &stmt.columns {
        let prop_id = content.alloc_property_id();
        let data_type = resolve_type(&col.data_type.0)?;
        let col_name_lower = col.name.0.to_lowercase();
        let is_pk = col_name_lower == pk_name_lower;

        if is_pk {
            primary_key_idx = Some(properties.len());
        }

        let mut prop = Property::new(prop_id, col.name.0.clone(), data_type, is_pk);
        if let Some(ref default_expr) = col.default_value {
            // Store default value as string representation for now
            prop = prop.with_default(format!("{:?}", default_expr.0));
        }
        properties.push(prop);
    }

    let primary_key_idx = primary_key_idx.ok_or_else(|| {
        KyuError::Catalog(format!(
            "primary key '{}' not found in column definitions for table '{name}'",
            stmt.primary_key.0
        ))
    })?;

    let table_id = content.alloc_table_id();
    let entry = NodeTableEntry {
        table_id,
        name: name.clone(),
        properties,
        primary_key_idx,
        num_rows: 0,
        comment: None,
    };

    content.add_node_table(entry)?;
    Ok(table_id)
}

/// Execute a CREATE REL TABLE statement.
pub fn execute_create_rel_table(
    content: &mut CatalogContent,
    stmt: &ast::CreateRelTable,
) -> KyuResult<TableId> {
    let name = &stmt.name.0;

    if stmt.if_not_exists && content.contains_name(name) {
        return Ok(content.find_by_name(name).unwrap().table_id());
    }

    if content.contains_name(name) {
        return Err(KyuError::Catalog(format!(
            "table '{name}' already exists"
        )));
    }

    // Resolve FROM and TO node tables
    let from_entry = content.find_by_name(&stmt.from_table.0).ok_or_else(|| {
        KyuError::Catalog(format!(
            "FROM table '{}' not found",
            stmt.from_table.0
        ))
    })?;
    if !from_entry.is_node_table() {
        return Err(KyuError::Catalog(format!(
            "FROM table '{}' is not a node table",
            stmt.from_table.0
        )));
    }
    let from_table_id = from_entry.table_id();

    let to_entry = content.find_by_name(&stmt.to_table.0).ok_or_else(|| {
        KyuError::Catalog(format!(
            "TO table '{}' not found",
            stmt.to_table.0
        ))
    })?;
    if !to_entry.is_node_table() {
        return Err(KyuError::Catalog(format!(
            "TO table '{}' is not a node table",
            stmt.to_table.0
        )));
    }
    let to_table_id = to_entry.table_id();

    // Resolve columns
    let mut properties = Vec::with_capacity(stmt.columns.len());
    for col in &stmt.columns {
        let prop_id = content.alloc_property_id();
        let data_type = resolve_type(&col.data_type.0)?;
        let mut prop = Property::new(prop_id, col.name.0.clone(), data_type, false);
        if let Some(ref default_expr) = col.default_value {
            prop = prop.with_default(format!("{:?}", default_expr.0));
        }
        properties.push(prop);
    }

    let table_id = content.alloc_table_id();
    let entry = RelTableEntry {
        table_id,
        name: name.clone(),
        from_table_id,
        to_table_id,
        properties,
        num_rows: 0,
        comment: None,
    };

    content.add_rel_table(entry)?;
    Ok(table_id)
}

/// Execute a DROP TABLE statement.
pub fn execute_drop(
    content: &mut CatalogContent,
    stmt: &ast::DropStatement,
) -> KyuResult<()> {
    let name = &stmt.name.0;

    if stmt.if_exists && !content.contains_name(name) {
        return Ok(());
    }

    let entry = content.find_by_name(name).ok_or_else(|| {
        KyuError::Catalog(format!("table '{name}' not found"))
    })?;
    let table_id = entry.table_id();

    // Check if any rel table references this node table
    if entry.is_node_table() {
        for rel in content.rel_tables() {
            if rel.from_table_id == table_id || rel.to_table_id == table_id {
                return Err(KyuError::Catalog(format!(
                    "cannot drop table '{name}': referenced by relationship table '{}'",
                    rel.name
                )));
            }
        }
    }

    content.remove_by_id(table_id);
    Ok(())
}

/// Execute an ALTER TABLE statement.
pub fn execute_alter_table(
    content: &mut CatalogContent,
    stmt: &ast::AlterTable,
) -> KyuResult<()> {
    let table_name = &stmt.table_name.0;

    if !content.contains_name(table_name) {
        return Err(KyuError::Catalog(format!(
            "table '{table_name}' not found"
        )));
    }

    match &stmt.action {
        ast::AlterAction::AddColumn(col) => {
            let prop_id = content.alloc_property_id();
            let data_type = resolve_type(&col.data_type.0)?;
            let mut prop = Property::new(prop_id, col.name.0.clone(), data_type, false);
            if let Some(ref default_expr) = col.default_value {
                prop = prop.with_default(format!("{:?}", default_expr.0));
            }
            content.add_property_to_table(table_name, prop)?;
        }
        ast::AlterAction::DropColumn(col_name) => {
            content.drop_property_from_table(table_name, &col_name.0)?;
        }
        ast::AlterAction::RenameColumn { old_name, new_name } => {
            content.rename_property(table_name, &old_name.0, &new_name.0)?;
        }
        ast::AlterAction::RenameTable(new_name) => {
            // Check new name doesn't conflict
            if content.contains_name(&new_name.0) {
                return Err(KyuError::Catalog(format!(
                    "table '{}' already exists",
                    new_name.0
                )));
            }
            let entry = content.find_entry_mut(table_name).unwrap();
            match entry {
                CatalogEntry::NodeTable(nt) => nt.name = new_name.0.clone(),
                CatalogEntry::RelTable(rt) => rt.name = new_name.0.clone(),
            }
        }
        ast::AlterAction::Comment(comment) => {
            let entry = content.find_entry_mut(table_name).unwrap();
            match entry {
                CatalogEntry::NodeTable(nt) => nt.comment = Some(SmolStr::new(comment.as_str())),
                CatalogEntry::RelTable(rt) => rt.comment = Some(SmolStr::new(comment.as_str())),
            }
        }
    }

    Ok(())
}

/// Helper to create a Spanned value for tests (span = 0..0).
#[cfg(test)]
fn spanned<T>(value: T) -> (T, std::ops::Range<usize>) {
    (value, 0..0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_parser::ast::*;
    use kyu_types::LogicalType;

    fn col(name: &str, ty: &str) -> ColumnDefinition {
        ColumnDefinition {
            name: spanned(SmolStr::new(name)),
            data_type: spanned(SmolStr::new(ty)),
            default_value: None,
        }
    }

    #[test]
    fn create_node_table() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![
                col("id", "SERIAL"),
                col("name", "STRING"),
                col("age", "INT64"),
            ],
            primary_key: spanned(SmolStr::new("id")),
        };

        let tid = execute_create_node_table(&mut content, &stmt).unwrap();
        assert_eq!(tid, TableId(0));
        assert_eq!(content.num_tables(), 1);

        let entry = content.find_by_name("Person").unwrap();
        let nt = entry.as_node_table().unwrap();
        assert_eq!(nt.properties.len(), 3);
        assert_eq!(nt.primary_key_property().name, "id");
        assert_eq!(nt.primary_key_property().data_type, LogicalType::Serial);
    }

    #[test]
    fn create_node_table_duplicate_fails() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();
        assert!(execute_create_node_table(&mut content, &stmt).is_err());
    }

    #[test]
    fn create_node_table_if_not_exists() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: true,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        let t1 = execute_create_node_table(&mut content, &stmt).unwrap();
        let t2 = execute_create_node_table(&mut content, &stmt).unwrap();
        assert_eq!(t1, t2);
        assert_eq!(content.num_tables(), 1);
    }

    #[test]
    fn create_node_table_missing_pk() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("name", "STRING")],
            primary_key: spanned(SmolStr::new("id")),
        };
        assert!(execute_create_node_table(&mut content, &stmt).is_err());
    }

    #[test]
    fn create_node_table_bad_type() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "FOOBAR")],
            primary_key: spanned(SmolStr::new("id")),
        };
        assert!(execute_create_node_table(&mut content, &stmt).is_err());
    }

    #[test]
    fn create_rel_table() {
        let mut content = CatalogContent::new();

        // Create two node tables first
        let person_stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &person_stmt).unwrap();

        let org_stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Organization")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &org_stmt).unwrap();

        let rel_stmt = CreateRelTable {
            name: spanned(SmolStr::new("WorksAt")),
            if_not_exists: false,
            from_table: spanned(SmolStr::new("Person")),
            to_table: spanned(SmolStr::new("Organization")),
            columns: vec![col("since", "DATE"), col("role", "STRING")],
        };

        let tid = execute_create_rel_table(&mut content, &rel_stmt).unwrap();
        assert_eq!(content.num_tables(), 3);

        let entry = content.find_by_id(tid).unwrap();
        let rt = entry.as_rel_table().unwrap();
        assert_eq!(rt.properties.len(), 2);
        assert_eq!(rt.from_table_id, TableId(0));
        assert_eq!(rt.to_table_id, TableId(1));
    }

    #[test]
    fn create_rel_table_missing_from() {
        let mut content = CatalogContent::new();
        let stmt = CreateRelTable {
            name: spanned(SmolStr::new("Knows")),
            if_not_exists: false,
            from_table: spanned(SmolStr::new("Missing")),
            to_table: spanned(SmolStr::new("Missing")),
            columns: vec![],
        };
        assert!(execute_create_rel_table(&mut content, &stmt).is_err());
    }

    #[test]
    fn create_rel_table_from_is_rel() {
        let mut content = CatalogContent::new();

        // Create node table
        let node_stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &node_stmt).unwrap();

        // Create rel table
        let rel_stmt = CreateRelTable {
            name: spanned(SmolStr::new("Knows")),
            if_not_exists: false,
            from_table: spanned(SmolStr::new("Person")),
            to_table: spanned(SmolStr::new("Person")),
            columns: vec![],
        };
        execute_create_rel_table(&mut content, &rel_stmt).unwrap();

        // Try creating rel from rel table
        let bad_stmt = CreateRelTable {
            name: spanned(SmolStr::new("Meta")),
            if_not_exists: false,
            from_table: spanned(SmolStr::new("Knows")),
            to_table: spanned(SmolStr::new("Person")),
            columns: vec![],
        };
        assert!(execute_create_rel_table(&mut content, &bad_stmt).is_err());
    }

    #[test]
    fn drop_table() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let drop_stmt = DropStatement {
            object_type: DropObjectType::Table,
            name: spanned(SmolStr::new("Person")),
            if_exists: false,
        };
        execute_drop(&mut content, &drop_stmt).unwrap();
        assert_eq!(content.num_tables(), 0);
    }

    #[test]
    fn drop_table_not_found() {
        let mut content = CatalogContent::new();
        let stmt = DropStatement {
            object_type: DropObjectType::Table,
            name: spanned(SmolStr::new("Missing")),
            if_exists: false,
        };
        assert!(execute_drop(&mut content, &stmt).is_err());
    }

    #[test]
    fn drop_table_if_exists() {
        let mut content = CatalogContent::new();
        let stmt = DropStatement {
            object_type: DropObjectType::Table,
            name: spanned(SmolStr::new("Missing")),
            if_exists: true,
        };
        execute_drop(&mut content, &stmt).unwrap(); // should not error
    }

    #[test]
    fn drop_referenced_node_table_fails() {
        let mut content = CatalogContent::new();

        let node_stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &node_stmt).unwrap();

        let rel_stmt = CreateRelTable {
            name: spanned(SmolStr::new("Knows")),
            if_not_exists: false,
            from_table: spanned(SmolStr::new("Person")),
            to_table: spanned(SmolStr::new("Person")),
            columns: vec![],
        };
        execute_create_rel_table(&mut content, &rel_stmt).unwrap();

        let drop_stmt = DropStatement {
            object_type: DropObjectType::Table,
            name: spanned(SmolStr::new("Person")),
            if_exists: false,
        };
        assert!(execute_drop(&mut content, &drop_stmt).is_err());
    }

    #[test]
    fn alter_add_column() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Person")),
            action: AlterAction::AddColumn(col("email", "STRING")),
        };
        execute_alter_table(&mut content, &alter).unwrap();

        let entry = content.find_by_name("Person").unwrap();
        assert_eq!(entry.properties().len(), 2);
    }

    #[test]
    fn alter_drop_column() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL"), col("email", "STRING")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Person")),
            action: AlterAction::DropColumn(spanned(SmolStr::new("email"))),
        };
        execute_alter_table(&mut content, &alter).unwrap();

        let entry = content.find_by_name("Person").unwrap();
        assert_eq!(entry.properties().len(), 1);
    }

    #[test]
    fn alter_rename_column() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL"), col("email", "STRING")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Person")),
            action: AlterAction::RenameColumn {
                old_name: spanned(SmolStr::new("email")),
                new_name: spanned(SmolStr::new("mail")),
            },
        };
        execute_alter_table(&mut content, &alter).unwrap();

        let entry = content.find_by_name("Person").unwrap();
        assert!(entry.properties().iter().any(|p| p.name == "mail"));
        assert!(!entry.properties().iter().any(|p| p.name == "email"));
    }

    #[test]
    fn alter_rename_table() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Person")),
            action: AlterAction::RenameTable(spanned(SmolStr::new("People"))),
        };
        execute_alter_table(&mut content, &alter).unwrap();

        assert!(content.find_by_name("People").is_some());
        assert!(content.find_by_name("Person").is_none());
    }

    #[test]
    fn alter_comment() {
        let mut content = CatalogContent::new();
        let stmt = CreateNodeTable {
            name: spanned(SmolStr::new("Person")),
            if_not_exists: false,
            columns: vec![col("id", "SERIAL")],
            primary_key: spanned(SmolStr::new("id")),
        };
        execute_create_node_table(&mut content, &stmt).unwrap();

        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Person")),
            action: AlterAction::Comment(SmolStr::new("People table")),
        };
        execute_alter_table(&mut content, &alter).unwrap();

        let entry = content.find_by_name("Person").unwrap();
        let nt = entry.as_node_table().unwrap();
        assert_eq!(nt.comment.as_deref(), Some("People table"));
    }

    #[test]
    fn alter_missing_table() {
        let mut content = CatalogContent::new();
        let alter = AlterTable {
            table_name: spanned(SmolStr::new("Missing")),
            action: AlterAction::Comment(SmolStr::new("test")),
        };
        assert!(execute_alter_table(&mut content, &alter).is_err());
    }
}
