//! Connection — executes Cypher queries and DDL against a Database.

use std::sync::{Arc, RwLock};

use std::collections::HashMap;

use kyu_binder::{
    BoundMatchClause, BoundNodePattern, BoundPatternElement, BoundQuery,
    BoundReadingClause, BoundUpdatingClause, Binder, BoundStatement,
};
use kyu_catalog::{Catalog, NodeTableEntry, Property, RelTableEntry};
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_executor::{ExecutionContext, QueryResult, Storage, execute};
use kyu_expression::{FunctionRegistry, evaluate, evaluate_constant};
use kyu_planner::{build_query_plan, resolve_properties};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::storage::NodeGroupStorage;

/// A connection to a KyuGraph database.
///
/// Connections share catalog and storage via `Arc`. Each query gets a
/// consistent catalog snapshot for binding and planning; DDL mutates
/// both catalog and storage atomically.
pub struct Connection {
    catalog: Arc<Catalog>,
    storage: Arc<RwLock<NodeGroupStorage>>,
    extensions: Arc<Vec<Box<dyn kyu_extension::Extension>>>,
}

impl Connection {
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        storage: Arc<RwLock<NodeGroupStorage>>,
        extensions: Arc<Vec<Box<dyn kyu_extension::Extension>>>,
    ) -> Self {
        Self { catalog, storage, extensions }
    }

    /// Execute a Cypher statement, returning a QueryResult.
    pub fn query(&self, cypher: &str) -> KyuResult<QueryResult> {
        // Fast path: CALL ext.proc(...) routing to extensions.
        if let Some(result) = self.try_call_extension(cypher)? {
            return Ok(result);
        }

        // 1. Parse
        let parse_result = kyu_parser::parse(cypher);
        let stmt = parse_result
            .ast
            .ok_or_else(|| KyuError::Parser(format!("{:?}", parse_result.errors)))?;

        // 2. Bind (against a catalog snapshot)
        let catalog_snapshot = self.catalog.read();
        let mut binder = Binder::new(catalog_snapshot, FunctionRegistry::with_builtins());
        let bound = binder.bind(&stmt)?;

        // 3. Route: query vs DDL vs DML
        match bound {
            BoundStatement::Query(query) => {
                // Check if this is a standalone DML (CREATE/SET/DELETE without MATCH).
                if self.is_standalone_dml(&query) {
                    return self.exec_dml(&query);
                }
                // Check if this is MATCH + SET/DELETE (read-then-write DML).
                if self.has_match_mutations(&query) {
                    return self.exec_match_dml(&query);
                }
                let catalog_snapshot = self.catalog.read();
                let plan = build_query_plan(&query, &catalog_snapshot)?;
                let storage_guard = self.storage.read().unwrap();
                let ctx = ExecutionContext::new(catalog_snapshot, &*storage_guard);
                execute(&plan, &query.output_schema, &ctx)
            }
            BoundStatement::CreateNodeTable(create) => self.exec_create_node_table(&create),
            BoundStatement::CreateRelTable(create) => self.exec_create_rel_table(&create),
            BoundStatement::Drop(drop) => self.exec_drop(&drop),
            BoundStatement::CopyFrom(copy) => self.exec_copy_from(&copy),
            _ => Err(KyuError::NotImplemented(
                "statement type not yet supported".into(),
            )),
        }
    }

    // ---- DML execution ----

    /// Returns true if the query has no reading clauses (standalone CREATE, CREATE...RETURN).
    fn is_standalone_dml(&self, query: &BoundQuery) -> bool {
        query.parts.iter().all(|part| {
            part.reading_clauses.is_empty() && !part.updating_clauses.is_empty()
        })
    }

    /// Execute a standalone DML statement (CREATE nodes/rels, with optional RETURN).
    fn exec_dml(&self, query: &BoundQuery) -> KyuResult<QueryResult> {
        let catalog_snapshot = self.catalog.read();

        for part in &query.parts {
            // Track created nodes for potential RETURN projection.
            let mut created_nodes: Vec<(Option<u32>, TableId, Vec<TypedValue>)> = Vec::new();

            for clause in &part.updating_clauses {
                match clause {
                    BoundUpdatingClause::Create(patterns) => {
                        for pattern in patterns {
                            for element in &pattern.elements {
                                match element {
                                    BoundPatternElement::Node(node) => {
                                        let values =
                                            self.exec_create_node(node, &catalog_snapshot)?;
                                        created_nodes.push((
                                            node.variable_index,
                                            node.table_id,
                                            values,
                                        ));
                                    }
                                    BoundPatternElement::Relationship(_rel) => {
                                        return Err(KyuError::NotImplemented(
                                            "CREATE relationship not yet supported".into(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    BoundUpdatingClause::Set(_) => {
                        return Err(KyuError::NotImplemented(
                            "standalone SET without MATCH".into(),
                        ));
                    }
                    BoundUpdatingClause::Delete(_) => {
                        return Err(KyuError::NotImplemented(
                            "standalone DELETE without MATCH".into(),
                        ));
                    }
                }
            }

            // Handle RETURN projection if present.
            if let Some(ref proj) = part.projection {
                let mut prop_map: HashMap<(u32, SmolStr), u32> = HashMap::new();
                let mut combined_values: Vec<TypedValue> = Vec::new();
                let mut offset = 0u32;

                for (var_idx, table_id, values) in &created_nodes {
                    if let Some(entry) = catalog_snapshot.find_by_id(*table_id) {
                        if let Some(vi) = var_idx {
                            for (i, prop) in entry.properties().iter().enumerate() {
                                prop_map.insert((*vi, prop.name.clone()), offset + i as u32);
                            }
                        }
                        offset += entry.properties().len() as u32;
                    }
                    combined_values.extend(values.iter().cloned());
                }

                let col_names: Vec<SmolStr> =
                    proj.items.iter().map(|item| item.alias.clone()).collect();
                let col_types: Vec<LogicalType> = proj
                    .items
                    .iter()
                    .map(|item| item.expression.result_type().clone())
                    .collect();

                let mut row: Vec<TypedValue> = Vec::with_capacity(proj.items.len());
                for item in &proj.items {
                    let resolved = resolve_properties(&item.expression, &prop_map);
                    let value = evaluate(&resolved, combined_values.as_slice())?;
                    row.push(value);
                }

                let mut result = QueryResult::new(col_names, col_types);
                result.push_row(row);
                return Ok(result);
            }
        }

        Ok(QueryResult::new(vec![], vec![]))
    }

    /// Insert a single node row into storage based on a CREATE pattern.
    /// Returns the created values in catalog property order.
    fn exec_create_node(
        &self,
        node: &BoundNodePattern,
        catalog: &kyu_catalog::CatalogContent,
    ) -> KyuResult<Vec<TypedValue>> {
        let entry = catalog.find_by_id(node.table_id).ok_or_else(|| {
            KyuError::Catalog(format!("table {:?} not found", node.table_id))
        })?;
        let properties = entry.properties();

        let mut values = Vec::with_capacity(properties.len());
        for prop in properties {
            let value = if let Some((_pid, expr)) =
                node.properties.iter().find(|(pid, _)| *pid == prop.id)
            {
                evaluate_constant(expr)?
            } else {
                TypedValue::Null
            };
            values.push(value);
        }

        self.storage
            .write()
            .unwrap()
            .insert_row(node.table_id, &values)?;

        Ok(values)
    }

    /// Returns true if the query has both MATCH (reading) and SET/DELETE (updating) clauses.
    fn has_match_mutations(&self, query: &BoundQuery) -> bool {
        query.parts.iter().any(|part| {
            !part.reading_clauses.is_empty() && !part.updating_clauses.is_empty()
        })
    }

    /// Execute MATCH...SET/DELETE: scan, filter, then mutate.
    fn exec_match_dml(&self, query: &BoundQuery) -> KyuResult<QueryResult> {
        let catalog_snapshot = self.catalog.read();

        for part in &query.parts {
            let match_clause = part
                .reading_clauses
                .iter()
                .find_map(|c| match c {
                    BoundReadingClause::Match(m) => Some(m),
                    _ => None,
                })
                .ok_or_else(|| {
                    KyuError::NotImplemented("MATCH...SET/DELETE requires a MATCH clause".into())
                })?;

            // Extract the node table from the pattern.
            let (table_id, var_idx) = self.extract_match_node(match_clause)?;

            // Build property map: (variable_index, property_name) → column_index.
            let entry = catalog_snapshot.find_by_id(table_id).ok_or_else(|| {
                KyuError::Catalog(format!("table {:?} not found", table_id))
            })?;
            let properties = entry.properties();
            let prop_map: HashMap<(u32, SmolStr), u32> = properties
                .iter()
                .enumerate()
                .filter_map(|(i, p)| var_idx.map(|vi| ((vi, p.name.clone()), i as u32)))
                .collect();

            // Resolve WHERE predicate.
            let resolved_where = match_clause
                .where_clause
                .as_ref()
                .map(|w| resolve_properties(w, &prop_map));

            // Phase 1: Read — scan rows and collect mutations.
            let rows = self.storage.read().unwrap().scan_rows(table_id)?;

            let mut set_mutations: Vec<(u64, usize, TypedValue)> = Vec::new();
            let mut delete_rows: Vec<u64> = Vec::new();

            for (row_idx, row_values) in &rows {
                // Evaluate WHERE.
                if let Some(ref pred) = resolved_where {
                    let result = evaluate(pred, row_values.as_slice())?;
                    if result != TypedValue::Bool(true) {
                        continue;
                    }
                }

                // Process updating clauses for this matching row.
                for clause in &part.updating_clauses {
                    match clause {
                        BoundUpdatingClause::Set(items) => {
                            for item in items {
                                let resolved_value =
                                    resolve_properties(&item.value, &prop_map);
                                let new_value =
                                    evaluate(&resolved_value, row_values.as_slice())?;
                                let col_idx = properties
                                    .iter()
                                    .position(|p| p.id == item.property_id)
                                    .ok_or_else(|| {
                                        KyuError::Storage(format!(
                                            "property {:?} not found",
                                            item.property_id
                                        ))
                                    })?;
                                set_mutations.push((*row_idx, col_idx, new_value));
                            }
                        }
                        BoundUpdatingClause::Delete(_) => {
                            delete_rows.push(*row_idx);
                        }
                        _ => {}
                    }
                }
            }

            // Phase 2: Write — apply mutations.
            let mut storage = self.storage.write().unwrap();
            for (row_idx, col_idx, value) in &set_mutations {
                storage.update_cell(table_id, *row_idx, *col_idx, value)?;
            }
            for row_idx in &delete_rows {
                storage.delete_row(table_id, *row_idx)?;
            }
        }

        Ok(QueryResult::new(vec![], vec![]))
    }

    /// Extract the single node table_id and variable_index from a MATCH clause.
    fn extract_match_node(
        &self,
        match_clause: &BoundMatchClause,
    ) -> KyuResult<(TableId, Option<u32>)> {
        for pattern in &match_clause.patterns {
            for element in &pattern.elements {
                if let BoundPatternElement::Node(node) = element {
                    return Ok((node.table_id, node.variable_index));
                }
            }
        }
        Err(KyuError::NotImplemented(
            "MATCH clause must contain at least one node pattern".into(),
        ))
    }

    // ---- Extension CALL routing ----

    /// Try to parse and route a `CALL ext.proc(args...)` statement to a registered extension.
    /// Returns `None` if the statement is not a CALL.
    fn try_call_extension(&self, cypher: &str) -> KyuResult<Option<QueryResult>> {
        let trimmed = cypher.trim();
        if !trimmed.to_uppercase().starts_with("CALL ") {
            return Ok(None);
        }

        // Parse: CALL <ext>.<proc>(<arg1>, <arg2>, ...)
        let rest = trimmed[5..].trim();
        let dot_pos = rest.find('.').ok_or_else(|| {
            KyuError::Binder("CALL requires <extension>.<procedure>(...) syntax".into())
        })?;
        let ext_name = &rest[..dot_pos];
        let after_dot = &rest[dot_pos + 1..];

        let paren_pos = after_dot.find('(').ok_or_else(|| {
            KyuError::Binder("CALL requires <extension>.<procedure>(...) syntax".into())
        })?;
        let proc_name = &after_dot[..paren_pos];
        let args_str = after_dot[paren_pos + 1..].trim_end_matches([')', ';']);

        let args: Vec<String> = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            args_str.split(',').map(|s| s.trim().trim_matches('\'').to_string()).collect()
        };

        // Find matching extension.
        let ext = self.extensions.iter().find(|e| e.name() == ext_name).ok_or_else(|| {
            KyuError::Binder(format!("unknown extension '{ext_name}'"))
        })?;

        // Build adjacency from all relationship tables.
        let adjacency = self.build_graph_adjacency();

        // Execute.
        let rows = ext.execute(proc_name, &args, &adjacency).map_err(|e| {
            KyuError::Runtime(format!("extension error: {e}"))
        })?;

        // Get procedure signature to determine column names.
        let proc_sig = ext.procedures().into_iter().find(|p| p.name == proc_name).ok_or_else(|| {
            KyuError::Binder(format!("unknown procedure '{proc_name}' in extension '{ext_name}'"))
        })?;

        let col_names: Vec<SmolStr> = proc_sig.columns.iter().map(|c| SmolStr::new(&c.name)).collect();
        let col_types: Vec<LogicalType> = proc_sig.columns.iter().map(|c| {
            match c.type_desc.as_str() {
                "INT64" => LogicalType::Int64,
                "DOUBLE" => LogicalType::Double,
                "STRING" => LogicalType::String,
                _ => LogicalType::String,
            }
        }).collect();

        let mut result = QueryResult::new(col_names.clone(), col_types.clone());
        for proc_row in &rows {
            let row: Vec<TypedValue> = col_names.iter().zip(col_types.iter()).map(|(name, ty)| {
                let val = proc_row.get(name.as_str()).map(|s| s.as_str()).unwrap_or("");
                match ty {
                    LogicalType::Int64 => val.parse::<i64>().map(TypedValue::Int64).unwrap_or(TypedValue::Null),
                    LogicalType::Double => val.parse::<f64>().map(TypedValue::Double).unwrap_or(TypedValue::Null),
                    _ => TypedValue::String(SmolStr::new(val)),
                }
            }).collect();
            result.push_row(row);
        }

        Ok(Some(result))
    }

    /// Build a complete graph adjacency map from all relationship tables.
    fn build_graph_adjacency(&self) -> std::collections::HashMap<i64, Vec<(i64, f64)>> {
        let mut adjacency: std::collections::HashMap<i64, Vec<(i64, f64)>> = std::collections::HashMap::new();
        let catalog = self.catalog.read();
        let storage = self.storage.read().unwrap();

        for rel in catalog.rel_tables() {
            let table_id = rel.table_id;
            for chunk in storage.scan_table(table_id) {
                for row_idx in 0..chunk.num_rows() {
                    let src = chunk.get_value(row_idx, 0);
                    let dst = chunk.get_value(row_idx, 1);
                    if let (TypedValue::Int64(s), TypedValue::Int64(d)) = (src, dst) {
                        adjacency.entry(s).or_default().push((d, 1.0));
                    }
                }
            }
        }

        adjacency
    }

    // ---- DDL execution ----

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

        // Storage schema: src_key_type, dst_key_type, then user properties.
        let from_key_type = catalog
            .find_by_id(create.from_table_id)
            .and_then(|e| e.as_node_table())
            .map(|n| n.primary_key_property().data_type.clone())
            .unwrap_or(LogicalType::Int64);
        let to_key_type = catalog
            .find_by_id(create.to_table_id)
            .and_then(|e| e.as_node_table())
            .map(|n| n.primary_key_property().data_type.clone())
            .unwrap_or(LogicalType::Int64);
        let mut schema = vec![from_key_type, to_key_type];
        schema.extend(create.columns.iter().map(|c| c.data_type.clone()));

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

    // ---- COPY FROM ----

    fn exec_copy_from(&self, copy: &kyu_binder::BoundCopyFrom) -> KyuResult<QueryResult> {
        // Evaluate source expression to get file path.
        let path_val = evaluate_constant(&copy.source)?;
        let path = match &path_val {
            TypedValue::String(s) => s.as_str().to_string(),
            _ => {
                return Err(KyuError::Storage(
                    "COPY FROM source must be a string path".into(),
                ))
            }
        };

        // Get the table schema from catalog.
        let catalog_snapshot = self.catalog.read();
        let entry = catalog_snapshot.find_by_id(copy.table_id).ok_or_else(|| {
            KyuError::Catalog(format!("table {:?} not found", copy.table_id))
        })?;
        let properties = entry.properties();
        let schema: Vec<LogicalType> = properties.iter().map(|p| p.data_type.clone()).collect();

        // Open CSV reader.
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&path)
            .map_err(|e| KyuError::Storage(format!("cannot open '{}': {}", path, e)))?;

        let mut storage = self.storage.write().unwrap();
        let mut row_count = 0u64;

        for result in reader.records() {
            let record = result
                .map_err(|e| KyuError::Storage(format!("CSV parse error: {}", e)))?;

            let mut values = Vec::with_capacity(schema.len());
            for (i, ty) in schema.iter().enumerate() {
                let field = record.get(i).unwrap_or("");
                let value = if field.is_empty() {
                    TypedValue::Null
                } else {
                    parse_csv_field(field, ty)?
                };
                values.push(value);
            }

            storage.insert_row(copy.table_id, &values)?;
            row_count += 1;
        }

        let _ = row_count;
        Ok(QueryResult::new(vec![], vec![]))
    }
}

/// Parse a CSV field string into a TypedValue based on the column's LogicalType.
fn parse_csv_field(field: &str, ty: &LogicalType) -> KyuResult<TypedValue> {
    match ty {
        LogicalType::Int8 => field
            .parse::<i8>()
            .map(TypedValue::Int8)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as INT8: {}", field, e))),
        LogicalType::Int16 => field
            .parse::<i16>()
            .map(TypedValue::Int16)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as INT16: {}", field, e))),
        LogicalType::Int32 => field
            .parse::<i32>()
            .map(TypedValue::Int32)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as INT32: {}", field, e))),
        LogicalType::Int64 | LogicalType::Serial => field
            .parse::<i64>()
            .map(TypedValue::Int64)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as INT64: {}", field, e))),
        LogicalType::Float => field
            .parse::<f32>()
            .map(TypedValue::Float)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as FLOAT: {}", field, e))),
        LogicalType::Double => field
            .parse::<f64>()
            .map(TypedValue::Double)
            .map_err(|e| KyuError::Storage(format!("cannot parse '{}' as DOUBLE: {}", field, e))),
        LogicalType::Bool => match field.to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" => Ok(TypedValue::Bool(true)),
            "false" | "0" | "f" | "no" => Ok(TypedValue::Bool(false)),
            _ => Err(KyuError::Storage(format!(
                "cannot parse '{}' as BOOL",
                field
            ))),
        },
        LogicalType::String => Ok(TypedValue::String(SmolStr::new(field))),
        _ => Err(KyuError::Storage(format!(
            "unsupported type {} for CSV import",
            ty.type_name()
        ))),
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
    fn create_node_via_cypher() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        conn.query("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.rows[0][0],
            TypedValue::String(SmolStr::new("Alice"))
        );
    }

    #[test]
    fn create_multiple_nodes() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        conn.query("CREATE (a:Person {id: 1, name: 'Alice'}), (b:Person {id: 2, name: 'Bob'})")
            .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn create_node_partial_properties() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        // name not specified — should be NULL
        conn.query("CREATE (n:Person {id: 1})").unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0][0], TypedValue::Int64(1));
        assert_eq!(result.rows[0][1], TypedValue::Null);
    }

    #[test]
    fn create_and_return() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        let result = conn
            .query("CREATE (n:Person {id: 1, name: 'Alice'}) RETURN n.name, n.id")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.rows[0][0],
            TypedValue::String(SmolStr::new("Alice"))
        );
        assert_eq!(result.rows[0][1], TypedValue::Int64(1));
    }

    #[test]
    fn match_set_property() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (n:Person {id: 1, name: 'Alice', age: 25})")
            .unwrap();

        conn.query("MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31")
            .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.age").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0][0], TypedValue::Int64(31));
    }

    #[test]
    fn match_set_with_where() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (a:Person {id: 1, name: 'Alice', age: 25})")
            .unwrap();
        conn.query("CREATE (b:Person {id: 2, name: 'Bob', age: 30})")
            .unwrap();

        // Only update Alice's age.
        conn.query("MATCH (p:Person) WHERE p.id = 1 SET p.age = 26")
            .unwrap();

        let result = conn
            .query("MATCH (p:Person) RETURN p.name, p.age")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        // Find Alice's row and Bob's row.
        let alice_row = result
            .rows
            .iter()
            .find(|r| r[0] == TypedValue::String(SmolStr::new("Alice")))
            .unwrap();
        let bob_row = result
            .rows
            .iter()
            .find(|r| r[0] == TypedValue::String(SmolStr::new("Bob")))
            .unwrap();
        assert_eq!(alice_row[1], TypedValue::Int64(26)); // updated
        assert_eq!(bob_row[1], TypedValue::Int64(30)); // unchanged
    }

    #[test]
    fn match_set_all_rows() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, active INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (a:Person {id: 1, active: 0})")
            .unwrap();
        conn.query("CREATE (b:Person {id: 2, active: 0})")
            .unwrap();

        // SET without WHERE — affects all rows.
        conn.query("MATCH (p:Person) SET p.active = 1").unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.active").unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows[0][0], TypedValue::Int64(1));
        assert_eq!(result.rows[1][0], TypedValue::Int64(1));
    }

    #[test]
    fn match_delete() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (a:Person {id: 1, name: 'Alice'})").unwrap();
        conn.query("CREATE (b:Person {id: 2, name: 'Bob'})").unwrap();

        conn.query("MATCH (p:Person) WHERE p.name = 'Alice' DELETE p")
            .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.rows[0][0],
            TypedValue::String(SmolStr::new("Bob"))
        );
    }

    #[test]
    fn match_delete_all() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (a:Person {id: 1})").unwrap();
        conn.query("CREATE (b:Person {id: 2})").unwrap();

        conn.query("MATCH (p:Person) DELETE p").unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.id").unwrap();
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

    #[test]
    fn copy_from_csv() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("kyu_test_csv");
        let _ = std::fs::create_dir_all(&dir);
        let csv_path = dir.join("persons.csv");
        {
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "id,name").unwrap();
            writeln!(f, "1,Alice").unwrap();
            writeln!(f, "2,Bob").unwrap();
            writeln!(f, "3,Charlie").unwrap();
        }

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        conn.query(&format!(
            "COPY Person FROM '{}'",
            csv_path.display()
        ))
        .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 3);

        // Clean up.
        let _ = std::fs::remove_file(&csv_path);
    }

    #[test]
    fn copy_from_csv_multiple_types() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("kyu_test_csv");
        let _ = std::fs::create_dir_all(&dir);
        let csv_path = dir.join("typed.csv");
        {
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "id,name,score,active").unwrap();
            writeln!(f, "1,Alice,95.5,true").unwrap();
            writeln!(f, "2,Bob,87.3,false").unwrap();
        }

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query(
            "CREATE NODE TABLE Student (id INT64, name STRING, score DOUBLE, active BOOL, PRIMARY KEY (id))",
        )
        .unwrap();
        conn.query(&format!(
            "COPY Student FROM '{}'",
            csv_path.display()
        ))
        .unwrap();

        let result = conn
            .query("MATCH (s:Student) RETURN s.name, s.score, s.active")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(
            result.rows[0][0],
            TypedValue::String(SmolStr::new("Alice"))
        );
        assert_eq!(result.rows[0][1], TypedValue::Double(95.5));
        assert_eq!(result.rows[0][2], TypedValue::Bool(true));

        let _ = std::fs::remove_file(&csv_path);
    }

    #[test]
    fn call_extension_pagerank() {
        let mut db = Database::in_memory();
        db.register_extension(Box::new(ext_algo::AlgoExtension));
        let conn = db.connect();

        // Create graph: 1->2->3->1 (cycle).
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))").unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
        conn.query("CREATE (n:Person {id: 1})").unwrap();
        conn.query("CREATE (n:Person {id: 2})").unwrap();
        conn.query("CREATE (n:Person {id: 3})").unwrap();

        // Insert relationships directly.
        let snapshot = db.catalog().read();
        let rel_table_id = snapshot.find_by_name("KNOWS").unwrap().table_id();
        drop(snapshot);
        {
            let mut storage = db.storage().write().unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(1), TypedValue::Int64(2)]).unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(2), TypedValue::Int64(3)]).unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(3), TypedValue::Int64(1)]).unwrap();
        }

        let result = conn.query("CALL algo.pageRank(0.85, 20, 0.000001)").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.column_names.len(), 2);
        // All ranks should be positive.
        for row in &result.rows {
            if let TypedValue::Double(rank) = &row[1] {
                assert!(*rank > 0.0);
            }
        }
    }

    #[test]
    fn call_extension_wcc() {
        let mut db = Database::in_memory();
        db.register_extension(Box::new(ext_algo::AlgoExtension));
        let conn = db.connect();

        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))").unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
        conn.query("CREATE (n:Person {id: 1})").unwrap();
        conn.query("CREATE (n:Person {id: 2})").unwrap();
        conn.query("CREATE (n:Person {id: 10})").unwrap();
        conn.query("CREATE (n:Person {id: 11})").unwrap();

        let snapshot = db.catalog().read();
        let rel_table_id = snapshot.find_by_name("KNOWS").unwrap().table_id();
        drop(snapshot);
        {
            let mut storage = db.storage().write().unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(1), TypedValue::Int64(2)]).unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(10), TypedValue::Int64(11)]).unwrap();
        }

        let result = conn.query("CALL algo.wcc()").unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn call_extension_betweenness() {
        let mut db = Database::in_memory();
        db.register_extension(Box::new(ext_algo::AlgoExtension));
        let conn = db.connect();

        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))").unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
        conn.query("CREATE (n:Person {id: 1})").unwrap();
        conn.query("CREATE (n:Person {id: 2})").unwrap();
        conn.query("CREATE (n:Person {id: 3})").unwrap();

        let snapshot = db.catalog().read();
        let rel_table_id = snapshot.find_by_name("KNOWS").unwrap().table_id();
        drop(snapshot);
        {
            let mut storage = db.storage().write().unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(1), TypedValue::Int64(2)]).unwrap();
            storage.insert_row(rel_table_id, &[TypedValue::Int64(2), TypedValue::Int64(3)]).unwrap();
        }

        let result = conn.query("CALL algo.betweenness()").unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn call_unknown_extension() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("CALL nonexistent.proc()");
        assert!(result.is_err());
    }
}
