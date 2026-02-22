//! kyu-transaction: MVCC, undo buffer, WAL, transaction lifecycle.

pub mod checkpointer;
pub mod local_wal;
pub mod manager;
pub mod transaction;
pub mod types;
pub mod undo_buffer;
pub mod version_info;
pub mod wal;
pub mod wal_record;
pub mod wal_replayer;

pub use checkpointer::Checkpointer;
pub use local_wal::LocalWal;
pub use manager::TransactionManager;
pub use transaction::Transaction;
pub use types::{
    TransactionState, TransactionType, INVALID_TRANSACTION, START_TRANSACTION_ID, VECTOR_CAPACITY,
};
pub use undo_buffer::UndoBuffer;
pub use version_info::{VersionInfo, VectorVersionInfo, WriteConflict};
pub use wal::Wal;
pub use wal_record::{WalRecord, WalRecordType};
pub use wal_replayer::{WalReplayResult, WalReplayer};

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// End-to-end: manager → begin → version operations → commit → verify visibility.
    #[test]
    fn full_transaction_lifecycle() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();

        // Begin a write transaction.
        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        let txn_id = txn.id();
        let start_ts = txn.start_ts();

        // Insert rows 0..100 into node group 0.
        let mut vi = VersionInfo::new();
        vi.append(txn_id, 0, 100);
        txn.push_insert_info(0, 0, 100);

        // Own transaction sees the rows.
        assert!(vi.is_selected(start_ts, txn_id, 0));
        assert!(vi.is_selected(start_ts, txn_id, 99));

        // Commit.
        mgr.commit(&mut txn, &wal, |record, commit_ts| {
            vi.commit_insert(txn_id, commit_ts, record.start_row, record.num_rows);
        })
        .unwrap();

        let commit_ts = txn.commit_ts();
        assert!(commit_ts > 0);

        // After commit, a new read-only transaction should see the rows.
        let txn2 = mgr.begin(TransactionType::ReadOnly).unwrap();
        assert!(vi.is_selected(txn2.start_ts(), txn2.id(), 0));
        assert!(vi.is_selected(txn2.start_ts(), txn2.id(), 99));
    }

    /// End-to-end: insert → commit → delete → rollback → verify still visible.
    #[test]
    fn insert_commit_delete_rollback() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();
        let mut vi = VersionInfo::new();

        // Insert + commit.
        let mut txn1 = mgr.begin(TransactionType::Write).unwrap();
        let txn1_id = txn1.id();
        vi.append(txn1_id, 0, 50);
        txn1.push_insert_info(0, 0, 50);
        mgr.commit(&mut txn1, &wal, |record, commit_ts| {
            vi.commit_insert(txn1_id, commit_ts, record.start_row, record.num_rows);
        })
        .unwrap();

        // Delete + rollback.
        let mut txn2 = mgr.begin(TransactionType::Write).unwrap();
        let txn2_id = txn2.id();
        vi.delete(txn2_id, 0, 10).unwrap();
        txn2.push_delete_info(0, 0, 10);

        // During txn2, rows 0..10 are invisible to txn2.
        assert!(!vi.is_selected(txn2.start_ts(), txn2_id, 0));

        // Rollback.
        mgr.rollback(&mut txn2, |record| {
            vi.rollback_delete(txn2_id, record.start_row, record.num_rows);
        })
        .unwrap();

        // After rollback, rows are visible again.
        let txn3 = mgr.begin(TransactionType::ReadOnly).unwrap();
        assert!(vi.is_selected(txn3.start_ts(), txn3.id(), 0));
        assert!(vi.is_selected(txn3.start_ts(), txn3.id(), 49));
    }

    /// WAL replay after commit.
    #[test]
    fn wal_replay_after_commit() {
        let wal = Wal::in_memory();
        let mgr = TransactionManager::new();

        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        txn.log_table_insertion(kyu_common::id::TableId(1), 100);
        mgr.commit(&mut txn, &wal, |_, _| {}).unwrap();

        let data = wal.in_memory_data().unwrap();
        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 1);
        assert!(result.committed_records.len() >= 2); // At least begin + commit
    }

    /// Write-write conflict detection.
    #[test]
    fn write_write_conflict() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();
        let mut vi = VersionInfo::new();

        // Insert + commit.
        let mut txn1 = mgr.begin(TransactionType::Write).unwrap();
        let txn1_id = txn1.id();
        vi.append(txn1_id, 0, 100);
        txn1.push_insert_info(0, 0, 100);
        mgr.commit(&mut txn1, &wal, |record, commit_ts| {
            vi.commit_insert(txn1_id, commit_ts, record.start_row, record.num_rows);
        })
        .unwrap();

        // Delete rows 0..10.
        let mut txn2 = mgr.begin(TransactionType::Write).unwrap();
        let txn2_id = txn2.id();
        vi.delete(txn2_id, 0, 10).unwrap();

        // Another write txn can't start (single-writer).
        let result = mgr.begin(TransactionType::Write);
        assert!(result.is_err());

        // Commit txn2, then txn3 tries to delete overlapping rows → conflict.
        txn2.push_delete_info(0, 0, 10);
        mgr.commit(&mut txn2, &wal, |record, commit_ts| {
            vi.commit_delete(txn2_id, commit_ts, record.start_row, record.num_rows);
        })
        .unwrap();

        let mut txn3 = mgr.begin(TransactionType::Write).unwrap();
        let txn3_id = txn3.id();
        // Rows 0..10 were deleted by committed txn2 — deleting again should detect conflict
        // (row already has a committed deletion version).
        let conflict = vi.delete(txn3_id, 0, 5);
        assert!(conflict.is_err());

        mgr.rollback(&mut txn3, |_| {}).unwrap();
    }
}
