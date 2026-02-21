//! Connection — executes Cypher queries and DDL against a Database.

use std::sync::{Arc, RwLock};

use kyu_binder::{Binder, BoundStatement};
use kyu_catalog::{Catalog, NodeTableEntry, Property, RelTableEntry};
use kyu_common::{KyuError, KyuResult};
use kyu_executor::{ExecutionContext, QueryResult, execute};
use kyu_expression::FunctionRegistry;
use kyu_planner::build_query_plan;
use kyu_types::LogicalType;

use crate::storage::NodeGroupStorage;

/// A connection to a KyuGraph database.
///
/// Connections share catalog and storage via `Arc`. Each query gets a
/// consistent catalog snapshot for binding and planning; DDL mutates
/// both catalog and storage atomically.
pub struct Connection {
    catalog: Arc<Catalog>,
    storage: Arc<RwLock<NodeGroupStorage>>,
}

impl Connection {
    pub(crate) fn new(catalog: Arc<Catalog>, storage: Arc<RwLock<NodeGroupStorage>>) -> Self {
        Self { catalog, storage }
    }

    /// Execute a Cypher statement, returning a QueryResult.
    pub fn query(&self, cypher: &str) -> KyuResult<QueryResult> {
        // 1. Parse
        let parse_result = kyu_parser::parse(cypher);
        let stmt = parse_result
            .ast
            .ok_or_else(|| KyuError::Parser(format!("{:?}", parse_result.errors)))?;

        // 2. Bind (against a catalog snapshot)
        let catalog_snapshot = self.catalog.read();
        let mut binder = Binder::new(catalog_snapshot, FunctionRegistry::with_builtins());
        let bound = binder.bind(&stmt)?;

        // 3. Route: query vs DDL
        match bound {
            BoundStatement::Query(query) => {
                let catalog_snapshot = self.catalog.read();
                let plan = build_query_plan(&query, &catalog_snapshot)?;
                let storage_guard = self.storage.read().unwrap();
                let ctx = ExecutionContext::new(catalog_snapshot, &*storage_guard);
                execute(&plan, &query.output_schema, &ctx)
            }
            BoundStatement::CreateNodeTable(create) => self.exec_create_node_table(&create),
            BoundStatement::CreateRelTable(create) => self.exec_create_rel_table(&create),
            BoundStatement::Drop(drop) => self.exec_drop(&drop),
            _ => Err(KyuError::NotImplemented(
                "statement type not yet supported".into(),
            )),
        }
    }

    fn exec_create_node_table(
        &self,
        create: &kyu_binder::BoundCreateNodeTable,
    ) -> KyuResult<QueryResult> {
        let mut catalog = self.catalog.begin_write();

        let table_id = catalog.alloc_table_id();
        let properties: Vec<Property> = create
            .columns
            .iter()
            .map(|col| {
                let prop_id = catalog.alloc_property_id();
                Property::new(
                    prop_id,
                    col.name.clone(),
                    col.data_type.clone(),
                    col.property_id.0 as usize == create.primary_key_idx,
                )
            })
            .collect();

        let schema: Vec<LogicalType> = create.columns.iter().map(|c| c.data_type.clone()).collect();

        catalog.add_node_table(NodeTableEntry {
            table_id,
            name: create.name.clone(),
            properties,
            primary_key_idx: create.primary_key_idx,
            num_rows: 0,
            comment: None,
        })?;

        self.catalog.commit_write(catalog);

        // Create corresponding storage table.
        self.storage.write().unwrap().create_table(table_id, schema);

        Ok(QueryResult::new(vec![], vec![]))
    }

    fn exec_create_rel_table(
        &self,
        create: &kyu_binder::BoundCreateRelTable,
    ) -> KyuResult<QueryResult> {
        let mut catalog = self.catalog.begin_write();

        let table_id = catalog.alloc_table_id();
        let properties: Vec<Property> = create
            .columns
            .iter()
            .map(|col| {
                let prop_id = catalog.alloc_property_id();
                Property::new(prop_id, col.name.clone(), col.data_type.clone(), false)
            })
            .collect();

        let schema: Vec<LogicalType> = create.columns.iter().map(|c| c.data_type.clone()).collect();

        catalog.add_rel_table(RelTableEntry {
            table_id,
            name: create.name.clone(),
            from_table_id: create.from_table_id,
            to_table_id: create.to_table_id,
            properties,
            num_rows: 0,
            comment: None,
        })?;

        self.catalog.commit_write(catalog);

        self.storage.write().unwrap().create_table(table_id, schema);

        Ok(QueryResult::new(vec![], vec![]))
    }

    fn exec_drop(&self, drop: &kyu_binder::BoundDrop) -> KyuResult<QueryResult> {
        let mut catalog = self.catalog.begin_write();
        catalog.remove_by_id(drop.table_id).ok_or_else(|| {
            KyuError::Catalog(format!("table '{}' not found", drop.name))
        })?;
        self.catalog.commit_write(catalog);

        self.storage.write().unwrap().drop_table(drop.table_id);

        Ok(QueryResult::new(vec![], vec![]))
    }
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use kyu_types::TypedValue;
    use smol_str::SmolStr;

    #[test]
    fn create_database_and_connect() {
        let db = Database::in_memory();
        let _conn = db.connect();
        assert_eq!(db.catalog().num_tables(), 0);
    }

    #[test]
    fn return_literal() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("RETURN 1 AS x").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0], vec![TypedValue::Int64(1)]);
    }

    #[test]
    fn return_arithmetic() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("RETURN 2 + 3 AS sum").unwrap();
        assert_eq!(result.rows[0], vec![TypedValue::Int64(5)]);
    }

    #[test]
    fn create_node_table() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        assert_eq!(db.catalog().num_tables(), 1);
        let snapshot = db.catalog().read();
        let entry = snapshot.find_by_name("Person").unwrap();
        assert!(entry.is_node_table());
        assert_eq!(entry.properties().len(), 2);

        assert!(db.storage().read().unwrap().has_table(entry.table_id()));
    }

    #[test]
    fn create_and_query_empty_table() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn create_rel_table() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person, since INT64)")
            .unwrap();

        assert_eq!(db.catalog().num_tables(), 2);
        let snapshot = db.catalog().read();
        let entry = snapshot.find_by_name("KNOWS").unwrap();
        assert!(entry.is_rel_table());
    }

    #[test]
    fn drop_table() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
            .unwrap();
        assert_eq!(db.catalog().num_tables(), 1);

        conn.query("DROP TABLE Person").unwrap();
        assert_eq!(db.catalog().num_tables(), 0);
    }

    #[test]
    fn create_duplicate_error() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
            .unwrap();
        let result = conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))");
        assert!(result.is_err());
    }

    #[test]
    fn parse_error_propagated() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("THIS IS NOT VALID CYPHER !!!");
        assert!(result.is_err());
    }

    #[test]
    fn multiple_connections_share_state() {
        let db = Database::in_memory();
        let conn1 = db.connect();
        let conn2 = db.connect();

        conn1
            .query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
            .unwrap();

        // conn2 should see the table created by conn1.
        assert_eq!(db.catalog().num_tables(), 1);
        let result = conn2.query("MATCH (p:Person) RETURN p.id").unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn storage_roundtrip_insert_scan() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        // Get table ID from catalog.
        let snapshot = db.catalog().read();
        let table_id = snapshot.find_by_name("Person").unwrap().table_id();
        drop(snapshot);

        // Insert directly via storage API (DML via Cypher deferred).
        db.storage()
            .write()
            .unwrap()
            .insert_row(
                table_id,
                &[
                    TypedValue::Int64(1),
                    TypedValue::String(SmolStr::new("Alice")),
                ],
            )
            .unwrap();

        // Query reads from real NodeGroup/ColumnChunk storage.
        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.rows[0][0],
            TypedValue::String(SmolStr::new("Alice"))
        );
    }

    #[test]
    fn storage_roundtrip_multiple_rows() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();

        let snapshot = db.catalog().read();
        let table_id = snapshot.find_by_name("Person").unwrap().table_id();
        drop(snapshot);

        let mut storage = db.storage().write().unwrap();
        storage
            .insert_row(
                table_id,
                &[
                    TypedValue::Int64(1),
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::Int64(25),
                ],
            )
            .unwrap();
        storage
            .insert_row(
                table_id,
                &[
                    TypedValue::Int64(2),
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(30),
                ],
            )
            .unwrap();
        drop(storage);

        let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();
        assert_eq!(result.num_rows(), 2);
    }
}
