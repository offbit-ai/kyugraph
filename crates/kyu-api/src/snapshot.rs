//! Versioned, filesystem-independent graph checkpoints for embedded hosts.
use crate::storage::NodeGroupStorage;
use kyu_catalog::CatalogContent;
use kyu_common::{KyuError, KyuResult, id::TableId};
use kyu_types::{LogicalType, TypedValue};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    catalog: String,
    tables: Vec<Table>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    id: u64,
    rows: Vec<Vec<TypedValue>>,
}
struct Output {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "graph snapshot exceeds its byte limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn error(message: impl std::fmt::Display) -> KyuError {
    KyuError::Storage(message.to_string())
}
pub(crate) fn encode(
    catalog: &CatalogContent,
    storage: &NodeGroupStorage,
    limit: usize,
) -> KyuResult<Vec<u8>> {
    let catalog_json = catalog.serialize_json();
    if catalog_json.len() > limit {
        return Err(error("graph catalog exceeds snapshot limit"));
    }
    let mut tables = Vec::new();
    for id in catalog
        .node_tables()
        .into_iter()
        .map(|t| t.table_id)
        .chain(catalog.rel_tables().into_iter().map(|t| t.table_id))
    {
        tables.push(Table {
            id: id.0,
            rows: storage
                .scan_rows(id)?
                .into_iter()
                .map(|(_, row)| row)
                .collect(),
        });
    }
    let snapshot = Snapshot {
        version: 1,
        catalog: catalog_json,
        tables,
    };
    let mut output = Output {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, &snapshot).map_err(error)?;
    Ok(output.bytes)
}
pub(crate) fn decode(bytes: &[u8], limit: usize) -> KyuResult<(CatalogContent, NodeGroupStorage)> {
    if bytes.len() > limit {
        return Err(error("graph snapshot exceeds its byte limit"));
    }
    let snapshot: Snapshot = serde_json::from_slice(bytes).map_err(error)?;
    if snapshot.version != 1 {
        return Err(error("unsupported graph snapshot version"));
    }
    let catalog = CatalogContent::deserialize_json(&snapshot.catalog).map_err(error)?;
    let mut storage = NodeGroupStorage::new();
    let mut ids = HashSet::new();
    for table in catalog.node_tables() {
        if table.primary_key_idx >= table.properties.len()
            || !table.properties[table.primary_key_idx].is_primary_key
            || table.properties.iter().filter(|p| p.is_primary_key).count() != 1
            || !ids.insert(table.table_id.0)
        {
            return Err(error("invalid snapshot node schema"));
        }
        storage.create_table(
            table.table_id,
            table
                .properties
                .iter()
                .map(|p| p.data_type.clone())
                .collect(),
        );
    }
    for table in catalog.rel_tables() {
        if !ids.insert(table.table_id.0) {
            return Err(error("duplicate snapshot table identity"));
        }
        let source = catalog
            .find_by_id(table.from_table_id)
            .and_then(|t| t.as_node_table())
            .ok_or_else(|| error("invalid snapshot relation source"))?;
        let target = catalog
            .find_by_id(table.to_table_id)
            .and_then(|t| t.as_node_table())
            .ok_or_else(|| error("invalid snapshot relation target"))?;
        let mut schema = vec![
            source.primary_key_property().data_type.clone(),
            target.primary_key_property().data_type.clone(),
        ];
        schema.extend(table.properties.iter().map(|p| p.data_type.clone()));
        storage.create_table(table.table_id, schema);
    }
    if snapshot.tables.len() != ids.len() {
        return Err(error("snapshot table data does not match catalog"));
    }
    for table in snapshot.tables {
        if !ids.remove(&table.id) {
            return Err(error("duplicate or unknown snapshot table data"));
        }
        let id = TableId(table.id);
        let schema = storage
            .table_schema(id)
            .ok_or_else(|| error("missing snapshot table schema"))?
            .to_vec();
        for row in table.rows {
            if row.len() != schema.len()
                || row.iter().zip(&schema).any(|(value, ty)| {
                    !matches!(value, TypedValue::Null)
                        && value.logical_type() != *ty
                        && !matches!((value, ty), (TypedValue::Int64(_), LogicalType::Serial))
                })
            {
                return Err(error("snapshot row does not match schema"));
            }
            storage.insert_row(id, &row)?;
        }
    }
    Ok((catalog, storage))
}

#[cfg(test)]
mod tests {
    use crate::Database;
    #[test]
    fn memory_checkpoint_preserves_graph_queries_and_exact_values() {
        let database = Database::in_memory();
        let connection = database.connect();
        connection
            .query(
                "CREATE NODE TABLE Person (id INT64, name STRING, score DOUBLE, PRIMARY KEY(id))",
            )
            .unwrap();
        connection
            .query("CREATE REL TABLE KNOWS (FROM Person TO Person)")
            .unwrap();
        connection
            .query("CREATE (:Person {id:9007199254740993,name:'Mira',score:1.5})")
            .unwrap();
        connection
            .query("CREATE (:Person {id:2,name:'Ada',score:2.5})")
            .unwrap();
        connection.query("MATCH (a:Person),(b:Person) WHERE a.id=9007199254740993 AND b.id=2 CREATE (a)-[:KNOWS]->(b)").unwrap();
        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id,a.name,b.name,a.score";
        let expected = connection
            .query(query)
            .unwrap()
            .iter_rows()
            .map(|r| r.to_vec())
            .collect::<Vec<_>>();
        let bytes = database.snapshot(1_048_576).unwrap();
        drop(connection);
        drop(database);
        let restored = Database::from_snapshot(&bytes, 1_048_576).unwrap();
        assert_eq!(
            restored
                .connect()
                .query(query)
                .unwrap()
                .iter_rows()
                .map(|r| r.to_vec())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(restored.snapshot(8).is_err());
        assert!(Database::from_snapshot(&bytes, 8).is_err());
        assert!(Database::from_snapshot(b"{", 1_048_576).is_err());
        let mut corrupt: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        corrupt["version"] = serde_json::json!(2);
        assert!(
            Database::from_snapshot(&serde_json::to_vec(&corrupt).unwrap(), 1_048_576).is_err()
        );
        corrupt["version"] = serde_json::json!(1);
        corrupt["tables"][0]["rows"][0] = serde_json::json!([]);
        assert!(
            Database::from_snapshot(&serde_json::to_vec(&corrupt).unwrap(), 1_048_576).is_err()
        );
    }
}
