use kyu_catalog::*;
use kyu_catalog::ddl::*;
use kyu_common::TableId;
use kyu_parser::ast::*;
use kyu_types::LogicalType;
use smol_str::SmolStr;

fn spanned<T>(value: T) -> (T, std::ops::Range<usize>) {
    (value, 0..0)
}

fn col(name: &str, ty: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: spanned(SmolStr::new(name)),
        data_type: spanned(SmolStr::new(ty)),
        default_value: None,
    }
}

fn create_person_table(content: &mut CatalogContent) -> TableId {
    let stmt = CreateNodeTable {
        name: spanned(SmolStr::new("Person")),
        if_not_exists: false,
        columns: vec![
            col("id", "SERIAL"),
            col("name", "STRING"),
            col("age", "INT64"),
            col("email", "STRING"),
        ],
        primary_key: spanned(SmolStr::new("id")),
    };
    execute_create_node_table(content, &stmt).unwrap()
}

fn create_org_table(content: &mut CatalogContent) -> TableId {
    let stmt = CreateNodeTable {
        name: spanned(SmolStr::new("Organization")),
        if_not_exists: false,
        columns: vec![
            col("id", "SERIAL"),
            col("name", "STRING"),
            col("founded", "DATE"),
        ],
        primary_key: spanned(SmolStr::new("id")),
    };
    execute_create_node_table(content, &stmt).unwrap()
}

#[test]
fn full_schema_lifecycle() {
    let catalog = Catalog::new();

    // --- Transaction 1: Create node tables ---
    let mut content = catalog.begin_write();
    let person_id = create_person_table(&mut content);
    let org_id = create_org_table(&mut content);
    catalog.commit_write(content);

    assert_eq!(catalog.version(), 1);
    assert_eq!(catalog.num_tables(), 2);

    // --- Transaction 2: Create relationship table ---
    let mut content = catalog.begin_write();
    let rel_stmt = CreateRelTable {
        name: spanned(SmolStr::new("WorksAt")),
        if_not_exists: false,
        from_table: spanned(SmolStr::new("Person")),
        to_table: spanned(SmolStr::new("Organization")),
        columns: vec![col("since", "DATE"), col("role", "STRING")],
    };
    let works_at_id = execute_create_rel_table(&mut content, &rel_stmt).unwrap();
    catalog.commit_write(content);

    assert_eq!(catalog.version(), 2);
    assert_eq!(catalog.num_tables(), 3);

    // Verify relationship table
    let snapshot = catalog.read();
    let rt = snapshot.find_by_id(works_at_id).unwrap().as_rel_table().unwrap();
    assert_eq!(rt.from_table_id, person_id);
    assert_eq!(rt.to_table_id, org_id);
    assert_eq!(rt.properties.len(), 2);

    // --- Transaction 3: Alter table - add column ---
    let mut content = catalog.begin_write();
    let alter = AlterTable {
        table_name: spanned(SmolStr::new("Person")),
        action: AlterAction::AddColumn(col("phone", "STRING")),
    };
    execute_alter_table(&mut content, &alter).unwrap();
    catalog.commit_write(content);

    let snapshot = catalog.read();
    let person = snapshot.find_by_name("Person").unwrap();
    assert_eq!(person.properties().len(), 5); // id, name, age, email, phone

    // --- Transaction 4: Alter table - rename column ---
    let mut content = catalog.begin_write();
    let alter = AlterTable {
        table_name: spanned(SmolStr::new("Person")),
        action: AlterAction::RenameColumn {
            old_name: spanned(SmolStr::new("phone")),
            new_name: spanned(SmolStr::new("mobile")),
        },
    };
    execute_alter_table(&mut content, &alter).unwrap();
    catalog.commit_write(content);

    let snapshot = catalog.read();
    let person = snapshot.find_by_name("Person").unwrap();
    assert!(person.properties().iter().any(|p| p.name == "mobile"));

    // --- Transaction 5: Alter table - drop column ---
    let mut content = catalog.begin_write();
    let alter = AlterTable {
        table_name: spanned(SmolStr::new("Person")),
        action: AlterAction::DropColumn(spanned(SmolStr::new("mobile"))),
    };
    execute_alter_table(&mut content, &alter).unwrap();
    catalog.commit_write(content);

    let snapshot = catalog.read();
    let person = snapshot.find_by_name("Person").unwrap();
    assert_eq!(person.properties().len(), 4); // back to id, name, age, email

    // --- Transaction 6: Drop rel table, then node table ---
    let mut content = catalog.begin_write();
    let drop_rel = DropStatement {
        object_type: DropObjectType::Table,
        name: spanned(SmolStr::new("WorksAt")),
        if_exists: false,
    };
    execute_drop(&mut content, &drop_rel).unwrap();

    let drop_org = DropStatement {
        object_type: DropObjectType::Table,
        name: spanned(SmolStr::new("Organization")),
        if_exists: false,
    };
    execute_drop(&mut content, &drop_org).unwrap();
    catalog.commit_write(content);

    assert_eq!(catalog.num_tables(), 1); // only Person remains
    assert_eq!(catalog.version(), 6);
}

#[test]
fn type_resolver_in_ddl() {
    let mut content = CatalogContent::new();
    let stmt = CreateNodeTable {
        name: spanned(SmolStr::new("Document")),
        if_not_exists: false,
        columns: vec![
            col("id", "SERIAL"),
            col("title", "STRING"),
            col("embedding", "FLOAT[1536]"),
            col("tags", "STRING[]"),
            col("metadata", "MAP(STRING, STRING)"),
            col("created_at", "TIMESTAMP"),
        ],
        primary_key: spanned(SmolStr::new("id")),
    };

    execute_create_node_table(&mut content, &stmt).unwrap();

    let entry = content.find_by_name("Document").unwrap().as_node_table().unwrap();
    assert_eq!(entry.properties.len(), 6);

    // Verify complex types resolved correctly
    assert_eq!(entry.properties[2].data_type, LogicalType::Array {
        element: Box::new(LogicalType::Float),
        size: 1536,
    });
    assert_eq!(entry.properties[3].data_type, LogicalType::List(Box::new(LogicalType::String)));
    assert_eq!(entry.properties[4].data_type, LogicalType::Map {
        key: Box::new(LogicalType::String),
        value: Box::new(LogicalType::String),
    });
    assert_eq!(entry.properties[5].data_type, LogicalType::Timestamp);
}

#[test]
fn snapshot_isolation() {
    let catalog = Catalog::new();

    // Create initial table
    let mut content = catalog.begin_write();
    create_person_table(&mut content);
    catalog.commit_write(content);

    // Take a snapshot
    let snapshot = catalog.read();
    assert_eq!(snapshot.num_tables(), 1);

    // Modify the catalog (new transaction)
    let mut content = catalog.begin_write();
    create_org_table(&mut content);
    catalog.commit_write(content);

    // The snapshot should NOT see the new table
    assert_eq!(snapshot.num_tables(), 1);

    // But a new read should
    let new_snapshot = catalog.read();
    assert_eq!(new_snapshot.num_tables(), 2);
}

#[test]
fn cannot_drop_node_table_with_rel_dependency() {
    let mut content = CatalogContent::new();
    create_person_table(&mut content);

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
    let result = execute_drop(&mut content, &drop_stmt);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("referenced by"));
}

#[test]
fn rename_table() {
    let mut content = CatalogContent::new();
    create_person_table(&mut content);

    let alter = AlterTable {
        table_name: spanned(SmolStr::new("Person")),
        action: AlterAction::RenameTable(spanned(SmolStr::new("People"))),
    };
    execute_alter_table(&mut content, &alter).unwrap();

    assert!(content.find_by_name("People").is_some());
    assert!(content.find_by_name("Person").is_none());
}

#[test]
fn self_referencing_rel_table() {
    let mut content = CatalogContent::new();
    create_person_table(&mut content);

    let rel_stmt = CreateRelTable {
        name: spanned(SmolStr::new("Knows")),
        if_not_exists: false,
        from_table: spanned(SmolStr::new("Person")),
        to_table: spanned(SmolStr::new("Person")),
        columns: vec![col("since", "DATE")],
    };
    let tid = execute_create_rel_table(&mut content, &rel_stmt).unwrap();

    let rt = content.find_by_id(tid).unwrap().as_rel_table().unwrap();
    assert_eq!(rt.from_table_id, rt.to_table_id);
    assert_eq!(rt.properties.len(), 1);
}
