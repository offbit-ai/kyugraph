//! Per-transaction WAL buffer.
//!
//! Accumulates serialized WAL records in a pre-allocated byte buffer.
//! Flushed to the global WAL on commit in a single copy.

use kyu_common::id::TableId;

use crate::wal_record::WalRecord;

/// Initial capacity for the local WAL buffer (4KB).
const LOCAL_WAL_INIT_CAPACITY: usize = 4096;

/// Per-transaction WAL buffer.
///
/// Pre-allocated at 4KB; grows by doubling. Each log method serializes
/// directly into the byte buffer via `WalRecord::serialize()`.
pub struct LocalWal {
    buffer: Vec<u8>,
    record_count: u32,
}

impl Default for LocalWal {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalWal {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(LOCAL_WAL_INIT_CAPACITY),
            record_count: 0,
        }
    }

    fn log_record(&mut self, record: &WalRecord) {
        record.serialize(&mut self.buffer);
        self.record_count += 1;
    }

    pub fn log_begin_transaction(&mut self) {
        self.log_record(&WalRecord::BeginTransaction);
    }

    pub fn log_commit(&mut self) {
        self.log_record(&WalRecord::Commit);
    }

    pub fn log_table_insertion(&mut self, table_id: TableId, num_rows: u64) {
        self.log_record(&WalRecord::TableInsertion { table_id, num_rows });
    }

    pub fn log_node_deletion(&mut self, table_id: TableId, node_offset: u64) {
        self.log_record(&WalRecord::NodeDeletion {
            table_id,
            node_offset,
        });
    }

    pub fn log_node_update(&mut self, table_id: TableId, column_id: u32, node_offset: u64) {
        self.log_record(&WalRecord::NodeUpdate {
            table_id,
            column_id,
            node_offset,
        });
    }

    pub fn log_rel_deletion(
        &mut self,
        table_id: TableId,
        src_offset: u64,
        dst_offset: u64,
        rel_offset: u64,
    ) {
        self.log_record(&WalRecord::RelDeletion {
            table_id,
            src_offset,
            dst_offset,
            rel_offset,
        });
    }

    pub fn log_rel_update(
        &mut self,
        table_id: TableId,
        column_id: u32,
        src_offset: u64,
        dst_offset: u64,
        rel_offset: u64,
    ) {
        self.log_record(&WalRecord::RelUpdate {
            table_id,
            column_id,
            src_offset,
            dst_offset,
            rel_offset,
        });
    }

    pub fn log_create_catalog_entry(&mut self, entry_type: u8, table_id: TableId) {
        self.log_record(&WalRecord::CreateCatalogEntry {
            entry_type,
            table_id,
        });
    }

    pub fn log_drop_catalog_entry(&mut self, entry_type: u8, entry_id: u64) {
        self.log_record(&WalRecord::DropCatalogEntry {
            entry_type,
            entry_id,
        });
    }

    pub fn log_alter_table_entry(&mut self, table_id: TableId) {
        self.log_record(&WalRecord::AlterTableEntry { table_id });
    }

    pub fn log_catalog_snapshot(&mut self, json_bytes: Vec<u8>) {
        self.log_record(&WalRecord::CatalogSnapshot { json_bytes });
    }

    /// Serialized bytes for flushing to the global WAL.
    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    /// Total serialized size in bytes.
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Reset buffer position without deallocating (reuse for next transaction).
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.record_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_record::WalRecord;

    #[test]
    fn new_local_wal() {
        let wal = LocalWal::new();
        assert!(wal.is_empty());
        assert_eq!(wal.record_count(), 0);
        assert_eq!(wal.size(), 0);
    }

    #[test]
    fn log_begin_and_commit() {
        let mut wal = LocalWal::new();
        wal.log_begin_transaction();
        wal.log_commit();

        assert_eq!(wal.record_count(), 2);

        // Decode records.
        let mut offset = 0;
        let (r1, consumed) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();
        assert_eq!(r1, WalRecord::BeginTransaction);
        offset += consumed;
        let (r2, _) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();
        assert_eq!(r2, WalRecord::Commit);
    }

    #[test]
    fn log_table_insertion() {
        let mut wal = LocalWal::new();
        wal.log_table_insertion(TableId(5), 100);

        let (r, _) = WalRecord::deserialize(wal.data()).unwrap();
        assert_eq!(
            r,
            WalRecord::TableInsertion {
                table_id: TableId(5),
                num_rows: 100
            }
        );
    }

    #[test]
    fn log_node_deletion() {
        let mut wal = LocalWal::new();
        wal.log_node_deletion(TableId(1), 42);

        let (r, _) = WalRecord::deserialize(wal.data()).unwrap();
        assert_eq!(
            r,
            WalRecord::NodeDeletion {
                table_id: TableId(1),
                node_offset: 42
            }
        );
    }

    #[test]
    fn log_node_update() {
        let mut wal = LocalWal::new();
        wal.log_node_update(TableId(2), 3, 99);

        let (r, _) = WalRecord::deserialize(wal.data()).unwrap();
        assert_eq!(
            r,
            WalRecord::NodeUpdate {
                table_id: TableId(2),
                column_id: 3,
                node_offset: 99
            }
        );
    }

    #[test]
    fn log_rel_deletion() {
        let mut wal = LocalWal::new();
        wal.log_rel_deletion(TableId(10), 1, 2, 3);

        let (r, _) = WalRecord::deserialize(wal.data()).unwrap();
        assert_eq!(
            r,
            WalRecord::RelDeletion {
                table_id: TableId(10),
                src_offset: 1,
                dst_offset: 2,
                rel_offset: 3,
            }
        );
    }

    #[test]
    fn log_rel_update() {
        let mut wal = LocalWal::new();
        wal.log_rel_update(TableId(7), 1, 10, 20, 30);

        let (r, _) = WalRecord::deserialize(wal.data()).unwrap();
        assert_eq!(
            r,
            WalRecord::RelUpdate {
                table_id: TableId(7),
                column_id: 1,
                src_offset: 10,
                dst_offset: 20,
                rel_offset: 30,
            }
        );
    }

    #[test]
    fn log_catalog_entries() {
        let mut wal = LocalWal::new();
        wal.log_create_catalog_entry(1, TableId(50));
        wal.log_drop_catalog_entry(2, 99);
        wal.log_alter_table_entry(TableId(50));

        assert_eq!(wal.record_count(), 3);

        let mut offset = 0;
        let (r1, c1) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();
        offset += c1;
        let (r2, c2) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();
        offset += c2;
        let (r3, _) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();

        assert_eq!(
            r1,
            WalRecord::CreateCatalogEntry {
                entry_type: 1,
                table_id: TableId(50)
            }
        );
        assert_eq!(
            r2,
            WalRecord::DropCatalogEntry {
                entry_type: 2,
                entry_id: 99
            }
        );
        assert_eq!(
            r3,
            WalRecord::AlterTableEntry {
                table_id: TableId(50)
            }
        );
    }

    #[test]
    fn clear_resets() {
        let mut wal = LocalWal::new();
        wal.log_begin_transaction();
        wal.log_commit();
        assert!(!wal.is_empty());

        wal.clear();
        assert!(wal.is_empty());
        assert_eq!(wal.record_count(), 0);
        assert_eq!(wal.size(), 0);
    }

    #[test]
    fn full_transaction_log() {
        let mut wal = LocalWal::new();
        wal.log_begin_transaction();
        wal.log_table_insertion(TableId(1), 500);
        wal.log_node_deletion(TableId(1), 10);
        wal.log_commit();

        assert_eq!(wal.record_count(), 4);
        assert!(wal.size() > 0);

        // Decode all records.
        let mut offset = 0;
        let mut records = Vec::new();
        while offset < wal.data().len() {
            let (r, consumed) = WalRecord::deserialize(&wal.data()[offset..]).unwrap();
            records.push(r);
            offset += consumed;
        }
        assert_eq!(records.len(), 4);
        assert_eq!(records[0], WalRecord::BeginTransaction);
        assert_eq!(records[3], WalRecord::Commit);
    }

    #[test]
    fn reuse_after_clear() {
        let mut wal = LocalWal::new();
        wal.log_begin_transaction();
        wal.log_commit();
        wal.clear();

        // Reuse for a new transaction.
        wal.log_begin_transaction();
        wal.log_table_insertion(TableId(2), 10);
        wal.log_commit();
        assert_eq!(wal.record_count(), 3);
    }
}
