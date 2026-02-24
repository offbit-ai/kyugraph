//! Load graph data from KyuGraph catalog and query results.

use kyu_api::{Connection, Database};
use kyu_common::KyuResult;
use kyu_types::TypedValue;

use crate::state::{EdgeVisual, GraphData, NodeVisual, SchemaData, SchemaTable, Vec2};
use crate::theme;

/// Load schema metadata from the catalog for the sidebar browser.
///
/// Queries actual row counts since the catalog `num_rows` may not reflect
/// rows inserted via individual CREATE statements.
pub fn load_schema(db: &Database) -> SchemaData {
    let catalog = db.catalog();
    let cat = catalog.read();
    let conn = db.connect();

    let node_tables = cat
        .node_tables()
        .iter()
        .map(|entry| {
            let count = count_rows(&conn, &entry.name);
            SchemaTable {
                name: entry.name.to_string(),
                num_rows: count,
                properties: entry
                    .properties
                    .iter()
                    .map(|p| (p.name.to_string(), format!("{:?}", p.data_type)))
                    .collect(),
            }
        })
        .collect();

    let node_tables_cat = cat.node_tables();
    let rel_tables = cat
        .rel_tables()
        .iter()
        .map(|entry| {
            let from_name = node_tables_cat
                .iter()
                .find(|n| n.table_id == entry.from_table_id)
                .map(|n| n.name.as_str())
                .unwrap_or("_");
            let to_name = node_tables_cat
                .iter()
                .find(|n| n.table_id == entry.to_table_id)
                .map(|n| n.name.as_str())
                .unwrap_or("_");
            let count = count_rel_rows(&conn, from_name, &entry.name, to_name);
            SchemaTable {
                name: entry.name.to_string(),
                num_rows: count,
                properties: entry
                    .properties
                    .iter()
                    .map(|p| (p.name.to_string(), format!("{:?}", p.data_type)))
                    .collect(),
            }
        })
        .collect();

    SchemaData {
        node_tables,
        rel_tables,
    }
}

/// Query actual row count for a node table.
fn count_rows(conn: &Connection, table_name: &str) -> u64 {
    let query = format!("MATCH (n:{table_name}) RETURN count(*) AS cnt");
    extract_count(conn, &query)
}

/// Query actual row count for a relationship table.
fn count_rel_rows(conn: &Connection, from: &str, rel: &str, to: &str) -> u64 {
    let query = format!("MATCH (:{from})-[:{rel}]->(:{to}) RETURN count(*) AS cnt");
    extract_count(conn, &query)
}

fn extract_count(conn: &Connection, query: &str) -> u64 {
    match conn.query(query) {
        Ok(result) => {
            if let Some(row) = result.iter_rows().next() {
                match &row[0] {
                    TypedValue::Int64(n) => *n as u64,
                    _ => 0,
                }
            } else {
                0
            }
        }
        Err(_) => 0,
    }
}

/// Load all nodes from a specific node table into the graph.
pub fn load_node_table(
    conn: &Connection,
    db: &Database,
    table_name: &str,
    graph: &mut GraphData,
) -> KyuResult<()> {
    let catalog = db.catalog();
    let cat = catalog.read();
    let node_tables = cat.node_tables();
    let entry = match node_tables.iter().find(|e| e.name == table_name) {
        Some(e) => e,
        None => return Ok(()),
    };

    let prop_names: Vec<&str> = entry.properties.iter().map(|p| p.name.as_str()).collect();
    let return_clause = prop_names
        .iter()
        .map(|p| format!("n.{p}"))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!("MATCH (n:{table_name}) RETURN {return_clause} LIMIT 500");
    let result = conn.query(&query)?;

    let num_cols = result.column_names.len();
    for row in result.iter_rows() {
        let pk_val = format_typed_value(&row[entry.primary_key_idx]);
        let id = format!("{table_name}:{pk_val}");

        // Skip if already loaded.
        if graph.node_index.contains_key(&id) {
            continue;
        }

        let properties: Vec<(String, String)> = (0..num_cols)
            .map(|i| (prop_names[i].to_string(), format_typed_value(&row[i])))
            .collect();

        let node = NodeVisual {
            id: id.clone(),
            label: table_name.to_string(),
            properties,
            pos: random_position(),
            vel: Vec2::default(),
            pinned: false,
            color_idx: theme::label_color_idx(table_name),
        };

        let idx = graph.nodes.len();
        graph.node_index.insert(id, idx);
        graph.nodes.push(node);
    }

    Ok(())
}

/// Load all relationships from a specific rel table into the graph.
pub fn load_rel_table(
    conn: &Connection,
    db: &Database,
    rel_name: &str,
    graph: &mut GraphData,
) -> KyuResult<()> {
    let catalog = db.catalog();
    let cat = catalog.read();

    let rel_tables = cat.rel_tables();
    let rel_entry = match rel_tables.iter().find(|e| e.name == rel_name) {
        Some(e) => e,
        None => return Ok(()),
    };

    // Find source and target table names.
    let node_tables = cat.node_tables();
    let from_table = node_tables
        .iter()
        .find(|e| e.table_id == rel_entry.from_table_id);
    let to_table = node_tables
        .iter()
        .find(|e| e.table_id == rel_entry.to_table_id);

    let (from_name, from_pk) = match from_table {
        Some(e) => (
            e.name.as_str(),
            e.properties[e.primary_key_idx].name.as_str(),
        ),
        None => return Ok(()),
    };
    let (to_name, to_pk) = match to_table {
        Some(e) => (
            e.name.as_str(),
            e.properties[e.primary_key_idx].name.as_str(),
        ),
        None => return Ok(()),
    };

    let query = format!(
        "MATCH (a:{from_name})-[:{rel_name}]->(b:{to_name}) RETURN a.{from_pk}, b.{to_pk} LIMIT 1000"
    );
    let result = conn.query(&query)?;

    for row in result.iter_rows() {
        let src_pk = format_typed_value(&row[0]);
        let dst_pk = format_typed_value(&row[1]);
        let src_id = format!("{from_name}:{src_pk}");
        let dst_id = format!("{to_name}:{dst_pk}");

        if let (Some(&src_idx), Some(&dst_idx)) =
            (graph.node_index.get(&src_id), graph.node_index.get(&dst_id))
        {
            graph.edges.push(EdgeVisual {
                src: src_idx,
                dst: dst_idx,
                rel_type: rel_name.to_string(),
                properties: Vec::new(),
            });
        }
    }

    Ok(())
}

/// Load all node tables and rel tables from the database.
pub fn load_full_graph(conn: &Connection, db: &Database) -> KyuResult<GraphData> {
    let mut graph = GraphData::new();
    let schema = load_schema(db);

    // Load all node tables first.
    for table in &schema.node_tables {
        let _ = load_node_table(conn, db, &table.name, &mut graph);
    }

    // Then load all relationship tables.
    for table in &schema.rel_tables {
        let _ = load_rel_table(conn, db, &table.name, &mut graph);
    }

    Ok(graph)
}

/// Format a TypedValue to a display string.
pub fn format_typed_value(val: &TypedValue) -> String {
    match val {
        TypedValue::Null => "null".to_string(),
        TypedValue::Bool(b) => b.to_string(),
        TypedValue::Int64(i) => i.to_string(),
        TypedValue::Double(f) => format!("{f:.4}"),
        TypedValue::String(s) => s.to_string(),
        TypedValue::InternalId(id) => format!("{id}"),
        other => format!("{other:?}"),
    }
}

/// Generate a random initial position spread around the origin.
fn random_position() -> Vec2 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEED: AtomicU32 = AtomicU32::new(42);

    // Advance state with a simple LCG.
    let s = SEED.fetch_add(1, Ordering::Relaxed);
    let hash = s.wrapping_mul(2654435761); // Knuth's multiplicative hash
    let x_bits = (hash & 0xFFFF) as f32 / 65535.0; // [0, 1]
    let y_bits = ((hash >> 16) & 0xFFFF) as f32 / 65535.0;

    // Spread within [-200, 200] — layout will refine positions.
    Vec2::new(x_bits * 400.0 - 200.0, y_bits * 400.0 - 200.0)
}
