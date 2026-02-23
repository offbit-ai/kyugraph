//! Connection — executes Cypher queries and DDL against a Database.

use std::sync::{Arc, RwLock};

use std::collections::HashMap;

use std::time::Instant;

use kyu_binder::{
    BindContext, BoundMatchClause, BoundNodePattern, BoundPatternElement, BoundQuery,
    BoundReadingClause, BoundUpdatingClause, Binder, BoundStatement,
};
use kyu_catalog::{Catalog, NodeTableEntry, Property, RelTableEntry};
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_delta::{DeltaBatch, DeltaStats, GraphDelta};
use kyu_executor::{ExecutionContext, QueryResult, Storage, execute};
use kyu_expression::{FunctionRegistry, evaluate, evaluate_constant};
use kyu_planner::{build_query_plan, optimize, resolve_properties};
use kyu_transaction::{Checkpointer, TransactionManager, TransactionType, Wal};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::storage::NodeGroupStorage;

/// A connection to a KyuGraph database.
///
/// Connections share catalog and storage via `Arc`. Each query gets a
/// consistent catalog snapshot for binding and planning; DDL mutates
/// both catalog and storage atomically. Every query is wrapped in a
/// transaction for crash safety and isolation.
pub struct Connection {
    catalog: Arc<Catalog>,
    storage: Arc<RwLock<NodeGroupStorage>>,
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    checkpointer: Arc<Checkpointer>,
    extensions: Arc<Vec<Box<dyn kyu_extension::Extension>>>,
}

impl Connection {
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        storage: Arc<RwLock<NodeGroupStorage>>,
        txn_mgr: Arc<TransactionManager>,
        wal: Arc<Wal>,
        checkpointer: Arc<Checkpointer>,
        extensions: Arc<Vec<Box<dyn kyu_extension::Extension>>>,
    ) -> Self {
        Self { catalog, storage, txn_mgr, wal, checkpointer, extensions }
    }

    /// Execute a Cypher statement, returning a QueryResult.
    ///
    /// Each statement is wrapped in a transaction: read-only queries get a
    /// `ReadOnly` transaction, write queries get a `Write` transaction.
    /// The transaction is committed on success and rolled back on error.
    pub fn query(&self, cypher: &str) -> KyuResult<QueryResult> {
        self.query_internal(cypher, BindContext::empty())
    }

    /// Execute a Cypher statement with parameter bindings for `$param` placeholders.
    ///
    /// Parameters are resolved to literal values at bind time, before planning
    /// and execution. This is the preferred way to pass dynamic values safely.
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use kyu_types::TypedValue;
    ///
    /// let mut params = HashMap::new();
    /// params.insert("min_age".to_string(), TypedValue::Int64(25));
    /// let result = conn.query_with_params(
    ///     "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
    ///     params,
    /// ).unwrap();
    /// ```
    pub fn query_with_params(
        &self,
        cypher: &str,
        params: HashMap<String, TypedValue>,
    ) -> KyuResult<QueryResult> {
        let ctx = BindContext {
            params: params
                .into_iter()
                .map(|(k, v)| (SmolStr::new(k), v))
                .collect(),
            env: HashMap::new(),
        };
        self.query_internal(cypher, ctx)
    }

    /// Full VM-style execution with both `$param` bindings and `env()` values.
    ///
    /// The Cypher evaluator is treated like a virtual machine: it accepts a
    /// query string plus two context maps that are resolved before planning:
    ///
    /// - `params`: `$param` placeholders → `TypedValue`
    /// - `env`: `env('KEY')` lookups → `TypedValue`
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use kyu_types::TypedValue;
    ///
    /// let mut params = HashMap::new();
    /// params.insert("name".to_string(), TypedValue::String("Alice".into()));
    /// let mut env = HashMap::new();
    /// env.insert("PREFIX".to_string(), TypedValue::String("graph_".into()));
    /// let result = conn.execute(
    ///     "MATCH (p:Person) WHERE p.name = $name RETURN p.name",
    ///     params,
    ///     env,
    /// ).unwrap();
    /// ```
    pub fn execute(
        &self,
        cypher: &str,
        params: HashMap<String, TypedValue>,
        env: HashMap<String, TypedValue>,
    ) -> KyuResult<QueryResult> {
        let ctx = BindContext {
            params: params
                .into_iter()
                .map(|(k, v)| (SmolStr::new(k), v))
                .collect(),
            env: env
                .into_iter()
                .map(|(k, v)| (SmolStr::new(k), v))
                .collect(),
        };
        self.query_internal(cypher, ctx)
    }

    fn query_internal(&self, cypher: &str, ctx: BindContext) -> KyuResult<QueryResult> {
        // Fast path: CHECKPOINT command.
        if cypher.trim().eq_ignore_ascii_case("CHECKPOINT")
            || cypher.trim().eq_ignore_ascii_case("CHECKPOINT;")
        {
            self.checkpointer.checkpoint().map_err(|e| {
                KyuError::Transaction(format!("checkpoint failed: {e}"))
            })?;
            return Ok(QueryResult::new(vec![], vec![]));
        }

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
        let mut binder = Binder::new(catalog_snapshot, FunctionRegistry::with_builtins())
            .with_context(ctx);
        let bound = binder.bind(&stmt)?;

        // 3. Determine if this is a write operation and/or DDL.
        let is_ddl = matches!(
            &bound,
            BoundStatement::CreateNodeTable(_)
                | BoundStatement::CreateRelTable(_)
                | BoundStatement::Drop(_)
        );
        let is_write = match &bound {
            BoundStatement::Query(q) => self.is_standalone_dml(q) || self.has_match_mutations(q),
            BoundStatement::CopyFrom(_) => true,
            _ => is_ddl,
        };

        // 4. Begin transaction.
        let txn_type = if is_write { TransactionType::Write } else { TransactionType::ReadOnly };
        let mut txn = self.txn_mgr.begin(txn_type).map_err(|e| {
            KyuError::Transaction(e.to_string())
        })?;

        // 5. Execute within the transaction.
        let result = self.execute_bound(bound);

        // 6. Commit on success, rollback on error.
        match &result {
            Ok(_) => {
                // For DDL, snapshot the catalog to WAL for crash recovery.
                if is_ddl {
                    let snapshot = self.catalog.read().serialize_json();
                    txn.log_catalog_snapshot(snapshot.into_bytes());
                }
                self.txn_mgr.commit(&mut txn, &self.wal, |_, _| {}).map_err(|e| {
                    KyuError::Transaction(e.to_string())
                })?;
                // Auto-checkpoint after write commits if WAL exceeds threshold.
                if is_write {
                    let _ = self.checkpointer.try_checkpoint();
                }
            }
            Err(_) => {
                let _ = self.txn_mgr.rollback(&mut txn, |_| {});
            }
        }

        result
    }

    /// Route a bound statement to the appropriate executor.
    fn execute_bound(&self, bound: BoundStatement) -> KyuResult<QueryResult> {
        match bound {
            BoundStatement::Query(query) => {
                if self.is_standalone_dml(&query) {
                    return self.exec_dml(&query);
                }
                if self.has_match_mutations(&query) {
                    return self.exec_match_dml(&query);
                }
                let catalog_snapshot = self.catalog.read();
                let plan = build_query_plan(&query, &catalog_snapshot)?;
                let plan = optimize(plan, &catalog_snapshot);
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

    // ---- Delta Fast Path ----

    /// Apply a batch of conflict-free, idempotent upserts that bypass OCC.
    ///
    /// All deltas in the batch commit atomically via a single WAL append.
    /// Semantics are "last write wins" — safe only when upsert semantics
    /// are acceptable (ingestion pipelines, agentic code graphs, document
    /// processing).
    pub fn apply_delta(&self, batch: DeltaBatch) -> KyuResult<DeltaStats> {
        let start = Instant::now();
        let mut stats = DeltaStats {
            total_deltas: batch.len() as u64,
            ..DeltaStats::default()
        };

        // Begin a write transaction for WAL serialization.
        let mut txn = self.txn_mgr.begin(TransactionType::Write).map_err(|e| {
            KyuError::Transaction(e.to_string())
        })?;

        let catalog = self.catalog.read();
        let mut storage = self.storage.write().unwrap();

        for delta in batch.iter() {
            match delta {
                GraphDelta::UpsertNode { key, labels: _, props } => {
                    let entry = catalog.find_by_name(key.label.as_str()).ok_or_else(|| {
                        KyuError::Catalog(format!("node table '{}' not found", key.label))
                    })?;
                    let node_entry = entry.as_node_table().ok_or_else(|| {
                        KyuError::Catalog(format!("'{}' is not a node table", key.label))
                    })?;
                    let table_id = node_entry.table_id;
                    let pk_col_idx = node_entry.primary_key_idx;
                    let pk_type = &node_entry.properties[pk_col_idx].data_type;
                    let pk_value = parse_primary_key(key.primary_key.as_str(), pk_type)?;

                    let existing = find_row_by_pk(&storage, table_id, pk_col_idx, &pk_value)?;

                    if let Some(row_idx) = existing {
                        // UPDATE: merge properties (unmentioned props unchanged).
                        for (prop_name, value) in props {
                            if let Some(col_idx) = find_property_index(node_entry, prop_name.as_str()) {
                                storage.update_cell(table_id, row_idx, col_idx, value)?;
                            }
                        }
                        stats.nodes_updated += 1;
                    } else {
                        // INSERT: build full row with PK + props, nulls for absent columns.
                        let values = build_node_row(node_entry, &pk_value, props);
                        storage.insert_row(table_id, &values)?;
                        stats.nodes_created += 1;
                    }
                }

                GraphDelta::UpsertEdge { src, rel_type, dst, props } => {
                    let entry = catalog.find_by_name(rel_type.as_str()).ok_or_else(|| {
                        KyuError::Catalog(format!("rel table '{}' not found", rel_type))
                    })?;
                    let rel_entry = entry.as_rel_table().ok_or_else(|| {
                        KyuError::Catalog(format!("'{}' is not a rel table", rel_type))
                    })?;
                    let rel_table_id = rel_entry.table_id;

                    // Resolve src/dst primary key types from their node tables.
                    let src_node = catalog.find_by_name(src.label.as_str())
                        .and_then(|e| e.as_node_table())
                        .ok_or_else(|| KyuError::Catalog(format!("node table '{}' not found", src.label)))?;
                    let dst_node = catalog.find_by_name(dst.label.as_str())
                        .and_then(|e| e.as_node_table())
                        .ok_or_else(|| KyuError::Catalog(format!("node table '{}' not found", dst.label)))?;

                    let src_pk_type = &src_node.properties[src_node.primary_key_idx].data_type;
                    let dst_pk_type = &dst_node.properties[dst_node.primary_key_idx].data_type;
                    let src_pk = parse_primary_key(src.primary_key.as_str(), src_pk_type)?;
                    let dst_pk = parse_primary_key(dst.primary_key.as_str(), dst_pk_type)?;

                    // Rel table storage schema: [src_key, dst_key, ...user_props]
                    let existing = find_edge_row(&storage, rel_table_id, &src_pk, &dst_pk)?;

                    if let Some(row_idx) = existing {
                        // UPDATE: merge edge properties (offset by 2 for src/dst key cols).
                        for (prop_name, value) in props {
                            if let Some(prop_idx) = find_rel_property_index(rel_entry, prop_name.as_str()) {
                                let col_idx = prop_idx + 2; // skip src_key, dst_key columns
                                storage.update_cell(rel_table_id, row_idx, col_idx, value)?;
                            }
                        }
                        stats.edges_updated += 1;
                    } else {
                        // INSERT: [src_pk, dst_pk, ...props]
                        let values = build_edge_row(rel_entry, &src_pk, &dst_pk, props);
                        storage.insert_row(rel_table_id, &values)?;
                        stats.edges_created += 1;
                    }
                }

                GraphDelta::DeleteNode { key } => {
                    let entry = catalog.find_by_name(key.label.as_str())
                        .and_then(|e| e.as_node_table())
                        .ok_or_else(|| KyuError::Catalog(format!("node table '{}' not found", key.label)))?;
                    let table_id = entry.table_id;
                    let pk_col_idx = entry.primary_key_idx;
                    let pk_type = &entry.properties[pk_col_idx].data_type;
                    let pk_value = parse_primary_key(key.primary_key.as_str(), pk_type)?;

                    if let Some(row_idx) = find_row_by_pk(&storage, table_id, pk_col_idx, &pk_value)? {
                        storage.delete_row(table_id, row_idx)?;
                        stats.nodes_deleted += 1;
                    }
                }

                GraphDelta::DeleteEdge { src, rel_type, dst } => {
                    let rel_entry = catalog.find_by_name(rel_type.as_str())
                        .and_then(|e| e.as_rel_table())
                        .ok_or_else(|| KyuError::Catalog(format!("rel table '{}' not found", rel_type)))?;
                    let rel_table_id = rel_entry.table_id;

                    let src_node = catalog.find_by_name(src.label.as_str())
                        .and_then(|e| e.as_node_table())
                        .ok_or_else(|| KyuError::Catalog(format!("node table '{}' not found", src.label)))?;
                    let dst_node = catalog.find_by_name(dst.label.as_str())
                        .and_then(|e| e.as_node_table())
                        .ok_or_else(|| KyuError::Catalog(format!("node table '{}' not found", dst.label)))?;

                    let src_pk = parse_primary_key(src.primary_key.as_str(), &src_node.properties[src_node.primary_key_idx].data_type)?;
                    let dst_pk = parse_primary_key(dst.primary_key.as_str(), &dst_node.properties[dst_node.primary_key_idx].data_type)?;

                    if let Some(row_idx) = find_edge_row(&storage, rel_table_id, &src_pk, &dst_pk)? {
                        storage.delete_row(rel_table_id, row_idx)?;
                        stats.edges_deleted += 1;
                    }
                }
            }
        }

        drop(storage);
        drop(catalog);

        // Commit WAL record.
        self.txn_mgr.commit(&mut txn, &self.wal, |_, _| {}).map_err(|e| {
            KyuError::Transaction(e.to_string())
        })?;
        let _ = self.checkpointer.try_checkpoint();

        stats.elapsed_micros = start.elapsed().as_micros() as u64;
        Ok(stats)
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

        // Build adjacency only if the extension needs it (e.g., graph algorithms).
        let adjacency = if ext.needs_graph() {
            self.build_graph_adjacency()
        } else {
            std::collections::HashMap::new()
        };

        // Execute.
        let rows = ext.execute(proc_name, &args, &adjacency).map_err(|e| {
            KyuError::Runtime(format!("extension error: {e}"))
        })?;

        // Get procedure signature to determine column names and types.
        let proc_sig = ext.procedures().into_iter().find(|p| p.name == proc_name).ok_or_else(|| {
            KyuError::Binder(format!("unknown procedure '{proc_name}' in extension '{ext_name}'"))
        })?;

        let col_names: Vec<SmolStr> = proc_sig.columns.iter().map(|c| SmolStr::new(&c.name)).collect();
        let col_types: Vec<LogicalType> = proc_sig.columns.iter().map(|c| c.data_type.clone()).collect();

        let mut result = QueryResult::new(col_names, col_types);
        for proc_row in rows {
            result.push_row(proc_row);
        }

        Ok(Some(result))
    }

    /// Build a complete graph adjacency map from all relationship tables.
    ///
    /// Uses typed slice accessors on FlatVector columns for direct i64 buffer
    /// access, avoiding per-element get_value() dispatch and TypedValue construction.
    fn build_graph_adjacency(&self) -> std::collections::HashMap<i64, Vec<(i64, f64)>> {
        use kyu_executor::value_vector::ValueVector;

        let mut adjacency: std::collections::HashMap<i64, Vec<(i64, f64)>> = std::collections::HashMap::new();
        let catalog = self.catalog.read();
        let storage = self.storage.read().unwrap();

        for rel in catalog.rel_tables() {
            let table_id = rel.table_id;
            for chunk in storage.scan_table(table_id) {
                let n = chunk.num_rows();
                if n == 0 {
                    continue;
                }

                let src_col = chunk.column(0);
                let dst_col = chunk.column(1);

                // Fast path: both columns are FlatVector Int64 with identity selection.
                if chunk.selection().is_identity()
                    && let (ValueVector::Flat(src_flat), ValueVector::Flat(dst_flat)) =
                        (src_col, dst_col)
                {
                    let src_slice = src_flat.data_as_i64_slice();
                    let dst_slice = dst_flat.data_as_i64_slice();
                    let src_nm = src_flat.null_mask();
                    let dst_nm = dst_flat.null_mask();
                    for i in 0..n {
                        if !src_nm.is_null(i as u64) && !dst_nm.is_null(i as u64) {
                            adjacency
                                .entry(src_slice[i])
                                .or_default()
                                .push((dst_slice[i], 1.0));
                        }
                    }
                    continue;
                }

                // Fallback: per-element extraction.
                for row_idx in 0..n {
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
                return Err(KyuError::Copy(
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
        drop(catalog_snapshot);

        // Open reader (auto-detects format by extension: .csv, .parquet, .arrow, .ipc).
        let reader = kyu_copy::open_reader(&path, &schema)?;

        let mut storage = self.storage.write().unwrap();
        for row_result in reader {
            let values = row_result?;
            storage.insert_row(copy.table_id, &values)?;
        }

        Ok(QueryResult::new(vec![], vec![]))
    }
}

// ---- Delta helpers (module-level, used by Connection::apply_delta) ----

/// Parse a string primary key into the correct TypedValue for the given column type.
fn parse_primary_key(value: &str, ty: &LogicalType) -> KyuResult<TypedValue> {
    match ty {
        LogicalType::Int8 => value.parse::<i8>().map(TypedValue::Int8)
            .map_err(|e| KyuError::Delta(format!("cannot parse PK '{value}' as INT8: {e}"))),
        LogicalType::Int16 => value.parse::<i16>().map(TypedValue::Int16)
            .map_err(|e| KyuError::Delta(format!("cannot parse PK '{value}' as INT16: {e}"))),
        LogicalType::Int32 => value.parse::<i32>().map(TypedValue::Int32)
            .map_err(|e| KyuError::Delta(format!("cannot parse PK '{value}' as INT32: {e}"))),
        LogicalType::Int64 | LogicalType::Serial => value.parse::<i64>().map(TypedValue::Int64)
            .map_err(|e| KyuError::Delta(format!("cannot parse PK '{value}' as INT64: {e}"))),
        LogicalType::String => Ok(TypedValue::String(SmolStr::new(value))),
        _ => Err(KyuError::Delta(format!(
            "unsupported primary key type '{}' for delta upsert",
            ty.type_name()
        ))),
    }
}

/// Find the global row index of the first live row matching a primary key value.
fn find_row_by_pk(
    storage: &crate::storage::NodeGroupStorage,
    table_id: TableId,
    pk_col_idx: usize,
    pk_value: &TypedValue,
) -> KyuResult<Option<u64>> {
    let rows = storage.scan_rows(table_id)?;
    for (row_idx, row_values) in &rows {
        if row_values.get(pk_col_idx) == Some(pk_value) {
            return Ok(Some(*row_idx));
        }
    }
    Ok(None)
}

/// Find the global row index of an edge row matching src and dst primary keys.
/// Rel table storage schema: [src_key, dst_key, ...user_props].
fn find_edge_row(
    storage: &crate::storage::NodeGroupStorage,
    rel_table_id: TableId,
    src_pk: &TypedValue,
    dst_pk: &TypedValue,
) -> KyuResult<Option<u64>> {
    let rows = storage.scan_rows(rel_table_id)?;
    for (row_idx, row_values) in &rows {
        if row_values.first() == Some(src_pk) && row_values.get(1) == Some(dst_pk) {
            return Ok(Some(*row_idx));
        }
    }
    Ok(None)
}

/// Find a property's column index in a node table entry by name.
fn find_property_index(entry: &NodeTableEntry, name: &str) -> Option<usize> {
    let lower = name.to_lowercase();
    entry.properties.iter().position(|p| p.name.to_lowercase() == lower)
}

/// Find a property's index in a rel table entry by name (0-based within user properties).
fn find_rel_property_index(entry: &RelTableEntry, name: &str) -> Option<usize> {
    let lower = name.to_lowercase();
    entry.properties.iter().position(|p| p.name.to_lowercase() == lower)
}

/// Build a full node row from delta properties. Columns not in `props` default to Null.
fn build_node_row(
    entry: &NodeTableEntry,
    pk_value: &TypedValue,
    props: &hashbrown::HashMap<SmolStr, TypedValue>,
) -> Vec<TypedValue> {
    entry.properties.iter().enumerate().map(|(i, prop)| {
        if i == entry.primary_key_idx {
            pk_value.clone()
        } else if let Some(val) = props.get(&prop.name) {
            val.clone()
        } else {
            TypedValue::Null
        }
    }).collect()
}

/// Build a full edge row: [src_pk, dst_pk, ...user_props].
fn build_edge_row(
    entry: &RelTableEntry,
    src_pk: &TypedValue,
    dst_pk: &TypedValue,
    props: &hashbrown::HashMap<SmolStr, TypedValue>,
) -> Vec<TypedValue> {
    let mut row = vec![src_pk.clone(), dst_pk.clone()];
    for prop in &entry.properties {
        if let Some(val) = props.get(&prop.name) {
            row.push(val.clone());
        } else {
            row.push(TypedValue::Null);
        }
    }
    row
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
        assert_eq!(result.row(0), vec![TypedValue::Int64(1)]);
    }

    #[test]
    fn return_arithmetic() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("RETURN 2 + 3 AS sum").unwrap();
        assert_eq!(result.row(0), vec![TypedValue::Int64(5)]);
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
            result.row(0)[0],
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
        assert_eq!(result.row(0)[0], TypedValue::Int64(1));
        assert_eq!(result.row(0)[1], TypedValue::Null);
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
            result.row(0)[0],
            TypedValue::String(SmolStr::new("Alice"))
        );
        assert_eq!(result.row(0)[1], TypedValue::Int64(1));
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
        assert_eq!(result.row(0)[0], TypedValue::Int64(31));
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
            .iter_rows()
            .find(|r| r[0] == TypedValue::String(SmolStr::new("Alice")))
            .unwrap();
        let bob_row = result
            .iter_rows()
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
        assert_eq!(result.row(0)[0], TypedValue::Int64(1));
        assert_eq!(result.row(1)[0], TypedValue::Int64(1));
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
            result.row(0)[0],
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
            result.row(0)[0],
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
            result.row(0)[0],
            TypedValue::String(SmolStr::new("Alice"))
        );
        assert_eq!(result.row(0)[1], TypedValue::Double(95.5));
        assert_eq!(result.row(0)[2], TypedValue::Bool(true));

        let _ = std::fs::remove_file(&csv_path);
    }

    #[test]
    fn copy_from_parquet() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = std::env::temp_dir().join("kyu_test_parquet_copy");
        let _ = std::fs::create_dir_all(&dir);
        let parquet_path = dir.join("persons.parquet");
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            let ids = Int64Array::from(vec![1, 2, 3]);
            let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(ids), Arc::new(names)],
            )
            .unwrap();
            let file = std::fs::File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        conn.query(&format!(
            "COPY Person FROM '{}'",
            parquet_path.display()
        ))
        .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.row(0)[0], TypedValue::Int64(1));
        assert_eq!(
            result.row(0)[1],
            TypedValue::String(SmolStr::new("Alice"))
        );

        let _ = std::fs::remove_dir_all(&dir);
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
        for row in result.iter_rows() {
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

    #[test]
    fn persistence_survives_restart() {
        let dir = std::env::temp_dir().join("kyu_test_persist_e2e");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Create schema and insert data, then drop the database.
        {
            let db = Database::open(&dir).unwrap();
            let conn = db.connect();
            conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
                .unwrap();
            conn.query("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
            conn.query("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();
            // Drop triggers checkpoint (Drop impl).
        }

        // Phase 2: Reopen and verify schema + data survived.
        {
            let db = Database::open(&dir).unwrap();
            let conn = db.connect();

            // Schema should be recovered.
            assert_eq!(db.catalog().num_tables(), 1);
            let snapshot = db.catalog().read();
            assert!(snapshot.find_by_name("Person").is_some());
            drop(snapshot);

            // Data should be recovered.
            let result = conn.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
            assert_eq!(result.num_rows(), 2);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_ddl_recovery_via_wal() {
        let dir = std::env::temp_dir().join("kyu_test_persist_ddl");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Create schema. The checkpoint on Drop will flush everything.
        {
            let db = Database::open(&dir).unwrap();
            let conn = db.connect();
            conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
                .unwrap();
            conn.query("CREATE NODE TABLE Organization (id INT64, name STRING, PRIMARY KEY (id))")
                .unwrap();
        }

        // Phase 2: Verify both tables survived.
        {
            let db = Database::open(&dir).unwrap();
            assert_eq!(db.catalog().num_tables(), 2);
            let snapshot = db.catalog().read();
            assert!(snapshot.find_by_name("Person").is_some());
            assert!(snapshot.find_by_name("Organization").is_some());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_empty_database() {
        let dir = std::env::temp_dir().join("kyu_test_persist_empty_db");
        let _ = std::fs::remove_dir_all(&dir);

        // Open, do nothing, drop.
        { let _db = Database::open(&dir).unwrap(); }

        // Reopen — should be empty.
        {
            let db = Database::open(&dir).unwrap();
            assert_eq!(db.catalog().num_tables(), 0);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Parameterized query tests ----

    #[test]
    fn return_param() {
        let db = Database::in_memory();
        let conn = db.connect();
        let mut params = std::collections::HashMap::new();
        params.insert("x".to_string(), TypedValue::Int64(42));
        let result = conn
            .query_with_params("RETURN $x AS val", params)
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.row(0), vec![TypedValue::Int64(42)]);
    }

    #[test]
    fn parameterized_where() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
            .unwrap();
        conn.query("CREATE (n:Person {id: 2, name: 'Bob', age: 20})")
            .unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("min_age".to_string(), TypedValue::Int64(25));
        let result = conn
            .query_with_params(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
                params,
            )
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.row(0)[0],
            TypedValue::String(SmolStr::new("Alice"))
        );
    }

    #[test]
    fn parameterized_create() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), TypedValue::Int64(1));
        params.insert(
            "name".to_string(),
            TypedValue::String(SmolStr::new("Alice")),
        );
        conn.query_with_params(
            "CREATE (n:Person {id: $id, name: $name})",
            params,
        )
        .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.row(0)[0],
            TypedValue::String(SmolStr::new("Alice"))
        );
    }

    #[test]
    fn parameterized_set() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, age INT64, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (n:Person {id: 1, age: 25})").unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("new_age".to_string(), TypedValue::Int64(31));
        conn.query_with_params(
            "MATCH (p:Person) WHERE p.id = 1 SET p.age = $new_age",
            params,
        )
        .unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.age").unwrap();
        assert_eq!(result.row(0)[0], TypedValue::Int64(31));
    }

    #[test]
    fn unresolved_param_error() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn.query("RETURN $missing AS val");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unresolved parameter"));
    }

    #[test]
    fn env_resolved() {
        let db = Database::in_memory();
        let conn = db.connect();
        let mut env = std::collections::HashMap::new();
        env.insert(
            "GREETING".to_string(),
            TypedValue::String(SmolStr::new("hello")),
        );
        let result = conn
            .execute("RETURN env('GREETING') AS val", std::collections::HashMap::new(), env)
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.row(0)[0],
            TypedValue::String(SmolStr::new("hello"))
        );
    }

    #[test]
    fn env_missing_returns_null() {
        let db = Database::in_memory();
        let conn = db.connect();
        let result = conn
            .execute(
                "RETURN env('MISSING') AS val",
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.row(0)[0], TypedValue::Null);
    }

    // ---- apply_delta tests ----

    #[test]
    fn delta_upsert_new_nodes() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Function (name STRING, lines INT64, PRIMARY KEY (name))")
            .unwrap();

        let batch = DeltaBatchBuilder::new("file:main.rs", 1)
            .upsert_node("Function", "main", vec![], [("lines", TypedValue::Int64(42))])
            .upsert_node("Function", "helper", vec![], [("lines", TypedValue::Int64(10))])
            .build();

        let stats = conn.apply_delta(batch).unwrap();
        assert_eq!(stats.nodes_created, 2);
        assert_eq!(stats.nodes_updated, 0);

        let result = conn.query("MATCH (f:Function) RETURN f.name, f.lines").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn delta_upsert_existing_node_merges() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Function (name STRING, lines INT64, PRIMARY KEY (name))")
            .unwrap();

        // Create initial node.
        let batch1 = DeltaBatchBuilder::new("file:main.rs", 1)
            .upsert_node("Function", "main", vec![], [("lines", TypedValue::Int64(42))])
            .build();
        conn.apply_delta(batch1).unwrap();

        // Upsert same node with updated lines.
        let batch2 = DeltaBatchBuilder::new("file:main.rs", 2)
            .upsert_node("Function", "main", vec![], [("lines", TypedValue::Int64(50))])
            .build();
        let stats = conn.apply_delta(batch2).unwrap();
        assert_eq!(stats.nodes_created, 0);
        assert_eq!(stats.nodes_updated, 1);

        let result = conn.query("MATCH (f:Function) WHERE f.name = 'main' RETURN f.lines").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.row(0)[0], TypedValue::Int64(50));
    }

    #[test]
    fn delta_delete_node() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        conn.query("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let batch = DeltaBatchBuilder::new("cleanup", 1)
            .delete_node("Person", "1")
            .build();
        let stats = conn.apply_delta(batch).unwrap();
        assert_eq!(stats.nodes_deleted, 1);

        let result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.row(0)[0], TypedValue::String(SmolStr::new("Bob")));
    }

    #[test]
    fn delta_upsert_and_delete_edges() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))").unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person, since INT64)").unwrap();
        conn.query("CREATE (n:Person {id: 1})").unwrap();
        conn.query("CREATE (n:Person {id: 2})").unwrap();

        // Create edge via delta.
        let batch = DeltaBatchBuilder::new("social", 1)
            .upsert_edge("Person", "1", "KNOWS", "Person", "2", [("since", TypedValue::Int64(2024))])
            .build();
        let stats = conn.apply_delta(batch).unwrap();
        assert_eq!(stats.edges_created, 1);

        // Verify edge exists.
        let storage = db.storage().read().unwrap();
        let catalog = db.catalog().read();
        let rel_table_id = catalog.find_by_name("KNOWS").unwrap().table_id();
        let rows = storage.scan_rows(rel_table_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], TypedValue::Int64(1)); // src
        assert_eq!(rows[0].1[1], TypedValue::Int64(2)); // dst
        assert_eq!(rows[0].1[2], TypedValue::Int64(2024)); // since
        drop(storage);
        drop(catalog);

        // Update edge property.
        let batch2 = DeltaBatchBuilder::new("social", 2)
            .upsert_edge("Person", "1", "KNOWS", "Person", "2", [("since", TypedValue::Int64(2025))])
            .build();
        let stats2 = conn.apply_delta(batch2).unwrap();
        assert_eq!(stats2.edges_updated, 1);

        let storage = db.storage().read().unwrap();
        let rows = storage.scan_rows(rel_table_id).unwrap();
        assert_eq!(rows[0].1[2], TypedValue::Int64(2025));
        drop(storage);

        // Delete edge.
        let batch3 = DeltaBatchBuilder::new("social", 3)
            .delete_edge("Person", "1", "KNOWS", "Person", "2")
            .build();
        let stats3 = conn.apply_delta(batch3).unwrap();
        assert_eq!(stats3.edges_deleted, 1);

        let storage = db.storage().read().unwrap();
        let rows = storage.scan_rows(rel_table_id).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delta_idempotent_replay() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE File (path STRING, hash STRING, PRIMARY KEY (path))")
            .unwrap();

        let batch = DeltaBatchBuilder::new("watcher", 100)
            .upsert_node("File", "src/main.rs", vec![], [("hash", TypedValue::String(SmolStr::new("abc123")))])
            .build();

        // Apply once.
        let stats1 = conn.apply_delta(batch.clone()).unwrap();
        assert_eq!(stats1.nodes_created, 1);

        // Apply again — same batch is idempotent (update, not create).
        let stats2 = conn.apply_delta(batch).unwrap();
        assert_eq!(stats2.nodes_created, 0);
        assert_eq!(stats2.nodes_updated, 1);

        // Still only one row.
        let result = conn.query("MATCH (f:File) RETURN f.path").unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn delta_stats_correct() {
        use kyu_delta::DeltaBatchBuilder;

        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
        conn.query("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();

        let batch = DeltaBatchBuilder::new("test", 1)
            .upsert_node("Person", "1", vec![], [("name", TypedValue::String(SmolStr::new("Alice")))])
            .upsert_node("Person", "2", vec![], [("name", TypedValue::String(SmolStr::new("Bob")))])
            .upsert_edge("Person", "1", "KNOWS", "Person", "2", Vec::<(&str, TypedValue)>::new())
            .build();

        let stats = conn.apply_delta(batch).unwrap();
        assert_eq!(stats.nodes_created, 2);
        assert_eq!(stats.edges_created, 1);
        assert_eq!(stats.total_deltas, 3);
        assert!(stats.elapsed_micros > 0);
    }
}
