//! Persistence — save/load catalog and storage to/from disk.
//!
//! Layout in database directory:
//!   catalog.json   — serialized CatalogContent
//!   data/<tid>.bin — per-table row data in a simple binary format

use std::path::Path;

use kyu_catalog::CatalogContent;
use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

use crate::storage::NodeGroupStorage;

const CATALOG_FILE: &str = "catalog.json";
const DATA_DIR: &str = "data";

// ---------------------------------------------------------------------------
// Catalog persistence
// ---------------------------------------------------------------------------

/// Save catalog content to `dir/catalog.json`.
pub fn save_catalog(dir: &Path, catalog: &CatalogContent) -> KyuResult<()> {
    let json = catalog.serialize_json();
    let path = dir.join(CATALOG_FILE);
    std::fs::write(&path, json.as_bytes()).map_err(|e| {
        KyuError::Storage(format!("cannot write catalog to '{}': {e}", path.display()))
    })
}

/// Load catalog content from `dir/catalog.json`. Returns `None` if not found.
pub fn load_catalog(dir: &Path) -> KyuResult<Option<CatalogContent>> {
    let path = dir.join(CATALOG_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&path).map_err(|e| {
        KyuError::Storage(format!("cannot read catalog from '{}': {e}", path.display()))
    })?;
    let content = CatalogContent::deserialize_json(&json).map_err(|e| {
        KyuError::Storage(format!("cannot parse catalog JSON: {e}"))
    })?;
    Ok(Some(content))
}

// ---------------------------------------------------------------------------
// Storage persistence
// ---------------------------------------------------------------------------

/// Save all table data to `dir/data/<table_id>.bin`.
pub fn save_storage(
    dir: &Path,
    storage: &NodeGroupStorage,
    catalog: &CatalogContent,
) -> KyuResult<()> {
    let data_dir = dir.join(DATA_DIR);
    std::fs::create_dir_all(&data_dir).map_err(|e| {
        KyuError::Storage(format!("cannot create data dir '{}': {e}", data_dir.display()))
    })?;

    // Remove stale files for tables that no longer exist.
    if let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".bin")
                && let Ok(tid) = name_str.trim_end_matches(".bin").parse::<u64>()
                && !storage.has_table(TableId(tid))
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    // Save node tables.
    for nt in catalog.node_tables() {
        save_table(&data_dir, nt.table_id, storage)?;
    }

    // Save rel tables.
    for rt in catalog.rel_tables() {
        save_table(&data_dir, rt.table_id, storage)?;
    }

    Ok(())
}

fn save_table(
    data_dir: &Path,
    table_id: TableId,
    storage: &NodeGroupStorage,
) -> KyuResult<()> {
    let rows = storage.scan_rows(table_id)?;
    let path = data_dir.join(format!("{}.bin", table_id.0));

    let mut buf = Vec::new();

    // Header: num_rows as u64 LE
    buf.extend_from_slice(&(rows.len() as u64).to_le_bytes());

    // Each row: num_cols u32 LE, then each value
    for (_row_idx, values) in &rows {
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for val in values {
            serialize_typed_value(&mut buf, val);
        }
    }

    std::fs::write(&path, &buf).map_err(|e| {
        KyuError::Storage(format!("cannot write table data to '{}': {e}", path.display()))
    })
}

/// Load all table data from `dir/data/`.
pub fn load_storage(dir: &Path, catalog: &CatalogContent) -> KyuResult<NodeGroupStorage> {
    let mut storage = NodeGroupStorage::new();
    let data_dir = dir.join(DATA_DIR);

    // Create tables from catalog schema.
    for nt in catalog.node_tables() {
        let schema: Vec<LogicalType> = nt.properties.iter().map(|p| p.data_type.clone()).collect();
        storage.create_table(nt.table_id, schema);
    }
    for rt in catalog.rel_tables() {
        // Rel tables store: src_key, dst_key, then user properties.
        // We need to reconstruct the storage schema the same way connection.rs does.
        let from_key_type = catalog
            .find_by_id(rt.from_table_id)
            .and_then(|e| e.as_node_table())
            .map(|n| n.primary_key_property().data_type.clone())
            .unwrap_or(LogicalType::Int64);
        let to_key_type = catalog
            .find_by_id(rt.to_table_id)
            .and_then(|e| e.as_node_table())
            .map(|n| n.primary_key_property().data_type.clone())
            .unwrap_or(LogicalType::Int64);
        let mut schema = vec![from_key_type, to_key_type];
        schema.extend(rt.properties.iter().map(|p| p.data_type.clone()));
        storage.create_table(rt.table_id, schema);
    }

    if !data_dir.exists() {
        return Ok(storage);
    }

    // Load row data from .bin files.
    if let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.ends_with(".bin") {
                continue;
            }
            if let Ok(tid) = name_str.trim_end_matches(".bin").parse::<u64>() {
                let table_id = TableId(tid);
                if storage.has_table(table_id) {
                    load_table_rows(&entry.path(), table_id, &mut storage)?;
                }
            }
        }
    }

    Ok(storage)
}

fn load_table_rows(
    path: &Path,
    table_id: TableId,
    storage: &mut NodeGroupStorage,
) -> KyuResult<()> {
    let data = std::fs::read(path).map_err(|e| {
        KyuError::Storage(format!("cannot read table data from '{}': {e}", path.display()))
    })?;

    if data.len() < 8 {
        return Ok(()); // Empty or corrupt file
    }

    let mut offset = 0;
    let num_rows = read_u64_le(&data, &mut offset);

    for _ in 0..num_rows {
        if offset + 4 > data.len() {
            break;
        }
        let num_cols = read_u32_le(&data, &mut offset) as usize;
        let mut values = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let val = deserialize_typed_value(&data, &mut offset)?;
            values.push(val);
        }
        storage.insert_row(table_id, &values)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// TypedValue binary serialization
// ---------------------------------------------------------------------------

// Tags for TypedValue serialization.
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT8: u8 = 2;
const TAG_INT16: u8 = 3;
const TAG_INT32: u8 = 4;
const TAG_INT64: u8 = 5;
const TAG_FLOAT: u8 = 6;
const TAG_DOUBLE: u8 = 7;
const TAG_STRING: u8 = 8;

fn serialize_typed_value(buf: &mut Vec<u8>, val: &TypedValue) {
    match val {
        TypedValue::Null => buf.push(TAG_NULL),
        TypedValue::Bool(v) => {
            buf.push(TAG_BOOL);
            buf.push(if *v { 1 } else { 0 });
        }
        TypedValue::Int8(v) => {
            buf.push(TAG_INT8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::Int16(v) => {
            buf.push(TAG_INT16);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::Int32(v) => {
            buf.push(TAG_INT32);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::Int64(v) => {
            buf.push(TAG_INT64);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::Float(v) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::Double(v) => {
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypedValue::String(s) => {
            buf.push(TAG_STRING);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        // Unsupported types stored as null.
        _ => buf.push(TAG_NULL),
    }
}

fn deserialize_typed_value(data: &[u8], offset: &mut usize) -> KyuResult<TypedValue> {
    if *offset >= data.len() {
        return Err(KyuError::Storage("unexpected end of table data".into()));
    }
    let tag = data[*offset];
    *offset += 1;

    match tag {
        TAG_NULL => Ok(TypedValue::Null),
        TAG_BOOL => {
            ensure_remaining(data, *offset, 1)?;
            let v = data[*offset] != 0;
            *offset += 1;
            Ok(TypedValue::Bool(v))
        }
        TAG_INT8 => {
            ensure_remaining(data, *offset, 1)?;
            let v = data[*offset] as i8;
            *offset += 1;
            Ok(TypedValue::Int8(v))
        }
        TAG_INT16 => {
            ensure_remaining(data, *offset, 2)?;
            let v = i16::from_le_bytes(data[*offset..*offset + 2].try_into().unwrap());
            *offset += 2;
            Ok(TypedValue::Int16(v))
        }
        TAG_INT32 => {
            ensure_remaining(data, *offset, 4)?;
            let v = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            Ok(TypedValue::Int32(v))
        }
        TAG_INT64 => {
            ensure_remaining(data, *offset, 8)?;
            let v = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(TypedValue::Int64(v))
        }
        TAG_FLOAT => {
            ensure_remaining(data, *offset, 4)?;
            let v = f32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            Ok(TypedValue::Float(v))
        }
        TAG_DOUBLE => {
            ensure_remaining(data, *offset, 8)?;
            let v = f64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(TypedValue::Double(v))
        }
        TAG_STRING => {
            ensure_remaining(data, *offset, 4)?;
            let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
            *offset += 4;
            ensure_remaining(data, *offset, len)?;
            let s = std::str::from_utf8(&data[*offset..*offset + len]).map_err(|e| {
                KyuError::Storage(format!("invalid UTF-8 in table data: {e}"))
            })?;
            *offset += len;
            Ok(TypedValue::String(SmolStr::new(s)))
        }
        _ => Err(KyuError::Storage(format!("unknown TypedValue tag: {tag}"))),
    }
}

fn read_u64_le(data: &[u8], offset: &mut usize) -> u64 {
    let v = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    v
}

fn read_u32_le(data: &[u8], offset: &mut usize) -> u32 {
    let v = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    v
}

fn ensure_remaining(data: &[u8], offset: usize, needed: usize) -> KyuResult<()> {
    if offset + needed > data.len() {
        Err(KyuError::Storage("unexpected end of table data".into()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_catalog::{NodeTableEntry, Property};

    fn make_test_catalog() -> CatalogContent {
        let mut c = CatalogContent::new();
        let tid = c.alloc_table_id();
        let pid0 = c.alloc_property_id();
        let pid1 = c.alloc_property_id();
        let pid2 = c.alloc_property_id();
        c.add_node_table(NodeTableEntry {
            table_id: tid,
            name: SmolStr::new("Person"),
            properties: vec![
                Property::new(pid0, "id", LogicalType::Int64, true),
                Property::new(pid1, "name", LogicalType::String, false),
                Property::new(pid2, "score", LogicalType::Double, false),
            ],
            primary_key_idx: 0,
            num_rows: 0,
            comment: None,
        })
        .unwrap();
        c
    }

    #[test]
    fn catalog_save_load_roundtrip() {
        let dir = std::env::temp_dir().join("kyu_test_persist_catalog");
        let _ = std::fs::create_dir_all(&dir);

        let catalog = make_test_catalog();
        save_catalog(&dir, &catalog).unwrap();

        let loaded = load_catalog(&dir).unwrap().unwrap();
        assert_eq!(loaded.num_tables(), 1);
        assert!(loaded.find_by_name("Person").is_some());
        assert_eq!(loaded.next_table_id, catalog.next_table_id);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_catalog_missing_returns_none() {
        let dir = std::env::temp_dir().join("kyu_test_persist_missing");
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::remove_file(dir.join(CATALOG_FILE));

        let result = load_catalog(&dir).unwrap();
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn storage_save_load_roundtrip() {
        let dir = std::env::temp_dir().join("kyu_test_persist_storage");
        let _ = std::fs::create_dir_all(&dir);

        let catalog = make_test_catalog();
        let tid = TableId(0);
        let schema = vec![LogicalType::Int64, LogicalType::String, LogicalType::Double];

        let mut storage = NodeGroupStorage::new();
        storage.create_table(tid, schema);
        storage
            .insert_row(tid, &[
                TypedValue::Int64(1),
                TypedValue::String(SmolStr::new("Alice")),
                TypedValue::Double(95.5),
            ])
            .unwrap();
        storage
            .insert_row(tid, &[
                TypedValue::Int64(2),
                TypedValue::String(SmolStr::new("Bob")),
                TypedValue::Double(87.3),
            ])
            .unwrap();

        save_storage(&dir, &storage, &catalog).unwrap();

        let loaded = load_storage(&dir, &catalog).unwrap();
        assert!(loaded.has_table(tid));
        assert_eq!(loaded.num_rows(tid), 2);

        let rows = loaded.scan_rows(tid).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[0], TypedValue::Int64(1));
        assert_eq!(rows[0].1[1], TypedValue::String(SmolStr::new("Alice")));
        assert_eq!(rows[0].1[2], TypedValue::Double(95.5));
        assert_eq!(rows[1].1[0], TypedValue::Int64(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn storage_empty_table_roundtrip() {
        let dir = std::env::temp_dir().join("kyu_test_persist_empty");
        let _ = std::fs::create_dir_all(&dir);

        let catalog = make_test_catalog();
        let tid = TableId(0);
        let mut storage = NodeGroupStorage::new();
        storage.create_table(tid, vec![LogicalType::Int64, LogicalType::String, LogicalType::Double]);

        save_storage(&dir, &storage, &catalog).unwrap();
        let loaded = load_storage(&dir, &catalog).unwrap();
        assert!(loaded.has_table(tid));
        assert_eq!(loaded.num_rows(tid), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn typed_value_binary_roundtrip() {
        let values = vec![
            TypedValue::Null,
            TypedValue::Bool(true),
            TypedValue::Bool(false),
            TypedValue::Int8(-42),
            TypedValue::Int16(1234),
            TypedValue::Int32(-999999),
            TypedValue::Int64(i64::MAX),
            TypedValue::Float(3.14),
            TypedValue::Double(2.718281828),
            TypedValue::String(SmolStr::new("hello world")),
            TypedValue::String(SmolStr::new("")),
        ];

        let mut buf = Vec::new();
        for v in &values {
            serialize_typed_value(&mut buf, v);
        }

        let mut offset = 0;
        for expected in &values {
            let actual = deserialize_typed_value(&buf, &mut offset).unwrap();
            assert_eq!(&actual, expected);
        }
        assert_eq!(offset, buf.len());
    }
}
