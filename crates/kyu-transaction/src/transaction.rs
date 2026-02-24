//! Transaction struct — the main handle for a database transaction.
//!
//! Hot fields (type, id, timestamps) are placed first for cache locality.
//! Read-only transactions allocate zero auxiliary structures.

use kyu_common::id::{Lsn, TableId, TxnTs};

use crate::local_wal::LocalWal;
use crate::types::{INVALID_TRANSACTION, TransactionState, TransactionType};
use crate::undo_buffer::UndoBuffer;
use crate::wal::Wal;

/// A database transaction.
pub struct Transaction {
    // Hot fields first (accessed on every visibility check).
    txn_type: TransactionType,
    state: TransactionState,
    force_checkpoint: bool,
    id: u64,
    start_ts: TxnTs,
    commit_ts: TxnTs,
    // Cold fields (behind Option — zero alloc for read-only txns).
    undo_buffer: Option<UndoBuffer>,
    local_wal: Option<LocalWal>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("txn_type", &self.txn_type)
            .field("state", &self.state)
            .field("id", &self.id)
            .field("start_ts", &self.start_ts)
            .field("commit_ts", &self.commit_ts)
            .finish()
    }
}

impl Transaction {
    /// Create a new transaction.
    /// Write transactions get pre-allocated undo buffer and local WAL.
    /// Read-only transactions have no auxiliary allocations.
    pub fn new(txn_type: TransactionType, id: u64, start_ts: TxnTs) -> Self {
        let (undo_buffer, local_wal) = if txn_type.should_append_to_undo_buffer() {
            (Some(UndoBuffer::new()), None)
        } else {
            (None, None)
        };

        let local_wal = if txn_type.should_log_to_wal() {
            let mut lw = LocalWal::new();
            lw.log_begin_transaction();
            Some(lw)
        } else {
            local_wal
        };

        Self {
            txn_type,
            state: TransactionState::Active,
            force_checkpoint: false,
            id,
            start_ts,
            commit_ts: INVALID_TRANSACTION,
            undo_buffer,
            local_wal,
        }
    }

    pub fn txn_type(&self) -> TransactionType {
        self.txn_type
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn start_ts(&self) -> TxnTs {
        self.start_ts
    }

    pub fn commit_ts(&self) -> TxnTs {
        self.commit_ts
    }

    pub fn is_read_only(&self) -> bool {
        self.txn_type.is_read_only()
    }

    pub fn is_write(&self) -> bool {
        self.txn_type.is_write()
    }

    pub fn force_checkpoint(&self) -> bool {
        self.force_checkpoint
    }

    pub fn set_force_checkpoint(&mut self, force: bool) {
        self.force_checkpoint = force;
    }

    pub fn set_commit_ts(&mut self, ts: TxnTs) {
        self.commit_ts = ts;
    }

    /// Push an insertion undo record.
    pub fn push_insert_info(&mut self, node_group_idx: u64, start_row: u64, num_rows: u64) {
        if let Some(ref mut undo) = self.undo_buffer {
            undo.create_insert_info(node_group_idx, start_row, num_rows);
        }
    }

    /// Push a deletion undo record.
    pub fn push_delete_info(&mut self, node_group_idx: u64, start_row: u64, num_rows: u64) {
        if let Some(ref mut undo) = self.undo_buffer {
            undo.create_delete_info(node_group_idx, start_row, num_rows);
        }
    }

    /// Log a table insertion to the local WAL.
    pub fn log_table_insertion(&mut self, table_id: TableId, num_rows: u64) {
        if let Some(ref mut wal) = self.local_wal {
            wal.log_table_insertion(table_id, num_rows);
        }
    }

    /// Log a node deletion to the local WAL.
    pub fn log_node_deletion(&mut self, table_id: TableId, node_offset: u64) {
        if let Some(ref mut wal) = self.local_wal {
            wal.log_node_deletion(table_id, node_offset);
        }
    }

    /// Log a catalog snapshot to the local WAL (for DDL recovery).
    pub fn log_catalog_snapshot(&mut self, json_bytes: Vec<u8>) {
        if let Some(ref mut wal) = self.local_wal {
            wal.log_catalog_snapshot(json_bytes);
        }
    }

    /// Access the undo buffer (for commit/rollback callbacks).
    pub fn undo_buffer(&self) -> Option<&UndoBuffer> {
        self.undo_buffer.as_ref()
    }

    /// Access the local WAL (for commit flushing).
    pub fn local_wal(&self) -> Option<&LocalWal> {
        self.local_wal.as_ref()
    }

    /// Mutable access to the local WAL.
    pub fn local_wal_mut(&mut self) -> Option<&mut LocalWal> {
        self.local_wal.as_mut()
    }

    /// Commit this transaction.
    /// Runs the undo buffer forward scan (commit callbacks) and flushes WAL.
    pub fn commit<F>(&mut self, wal: &Wal, commit_callback: F) -> std::io::Result<Lsn>
    where
        F: FnMut(crate::undo_buffer::UndoRecord, u64),
    {
        debug_assert_eq!(self.state, TransactionState::Active);

        // Run undo buffer commit (forward iteration).
        if let Some(ref undo) = self.undo_buffer {
            undo.commit(self.commit_ts, commit_callback);
        }

        // Log commit marker and flush local WAL.
        // BeginTransaction was already logged at creation time.
        let lsn = if let Some(ref mut local) = self.local_wal {
            local.log_commit();
            wal.log_committed_wal(local)?
        } else {
            0
        };

        self.state = TransactionState::Committed;
        Ok(lsn)
    }

    /// Rollback this transaction.
    /// Runs the undo buffer reverse scan (rollback callbacks) and clears local WAL.
    pub fn rollback<F>(&mut self, rollback_callback: F)
    where
        F: FnMut(crate::undo_buffer::UndoRecord),
    {
        debug_assert_eq!(self.state, TransactionState::Active);

        // Run undo buffer rollback (reverse iteration).
        if let Some(ref undo) = self.undo_buffer {
            undo.rollback(rollback_callback);
        }

        // Clear local WAL.
        if let Some(ref mut local) = self.local_wal {
            local.clear();
        }

        self.state = TransactionState::RolledBack;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::START_TRANSACTION_ID;
    use crate::undo_buffer::UndoRecordType;

    const TXN_ID: u64 = START_TRANSACTION_ID + 1;
    const START_TS: u64 = 100;

    #[test]
    fn new_read_only_transaction() {
        let txn = Transaction::new(TransactionType::ReadOnly, TXN_ID, START_TS);
        assert!(txn.is_read_only());
        assert!(!txn.is_write());
        assert_eq!(txn.state(), TransactionState::Active);
        assert_eq!(txn.id(), TXN_ID);
        assert_eq!(txn.start_ts(), START_TS);
        assert_eq!(txn.commit_ts(), INVALID_TRANSACTION);
        assert!(txn.undo_buffer().is_none());
        assert!(txn.local_wal().is_none());
    }

    #[test]
    fn new_write_transaction() {
        let txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        assert!(!txn.is_read_only());
        assert!(txn.is_write());
        assert!(txn.undo_buffer().is_some());
        assert!(txn.local_wal().is_some());
    }

    #[test]
    fn new_recovery_transaction() {
        let txn = Transaction::new(TransactionType::Recovery, TXN_ID, START_TS);
        assert!(txn.undo_buffer().is_some()); // Recovery needs undo
        assert!(txn.local_wal().is_none()); // Recovery doesn't log to WAL
    }

    #[test]
    fn new_checkpoint_transaction() {
        let txn = Transaction::new(TransactionType::Checkpoint, TXN_ID, START_TS);
        assert!(txn.undo_buffer().is_none());
        assert!(txn.local_wal().is_none());
    }

    #[test]
    fn push_insert_info() {
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        txn.push_insert_info(0, 0, 100);
        txn.push_insert_info(1, 0, 50);
        assert_eq!(txn.undo_buffer().unwrap().record_count(), 2);
    }

    #[test]
    fn push_delete_info() {
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        txn.push_delete_info(0, 10, 5);
        assert_eq!(txn.undo_buffer().unwrap().record_count(), 1);
    }

    #[test]
    fn push_on_read_only_is_noop() {
        let mut txn = Transaction::new(TransactionType::ReadOnly, TXN_ID, START_TS);
        txn.push_insert_info(0, 0, 100);
        assert!(txn.undo_buffer().is_none());
    }

    #[test]
    fn commit_write_transaction() {
        let wal = Wal::in_memory();
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        txn.set_commit_ts(200);
        txn.push_insert_info(0, 0, 10);

        let mut committed = Vec::new();
        let lsn = txn
            .commit(&wal, |record, ts| {
                committed.push((record, ts));
            })
            .unwrap();

        assert!(lsn > 0);
        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0.record_type, UndoRecordType::InsertInfo);
        assert_eq!(committed[0].1, 200);
    }

    #[test]
    fn commit_read_only_transaction() {
        let wal = Wal::in_memory();
        let mut txn = Transaction::new(TransactionType::ReadOnly, TXN_ID, START_TS);
        let lsn = txn.commit(&wal, |_, _| {}).unwrap();
        assert_eq!(lsn, 0); // No WAL for read-only.
        assert_eq!(txn.state(), TransactionState::Committed);
    }

    #[test]
    fn rollback_write_transaction() {
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        txn.push_insert_info(0, 0, 10);
        txn.push_delete_info(1, 5, 3);

        let mut rolled_back = Vec::new();
        txn.rollback(|record| {
            rolled_back.push(record);
        });

        assert_eq!(txn.state(), TransactionState::RolledBack);
        // Reverse order.
        assert_eq!(rolled_back.len(), 2);
        assert_eq!(rolled_back[0].record_type, UndoRecordType::DeleteInfo);
        assert_eq!(rolled_back[1].record_type, UndoRecordType::InsertInfo);
    }

    #[test]
    fn rollback_read_only_transaction() {
        let mut txn = Transaction::new(TransactionType::ReadOnly, TXN_ID, START_TS);
        txn.rollback(|_| {});
        assert_eq!(txn.state(), TransactionState::RolledBack);
    }

    #[test]
    fn force_checkpoint_flag() {
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        assert!(!txn.force_checkpoint());
        txn.set_force_checkpoint(true);
        assert!(txn.force_checkpoint());
    }

    #[test]
    fn log_wal_entries() {
        let mut txn = Transaction::new(TransactionType::Write, TXN_ID, START_TS);
        txn.log_table_insertion(TableId(1), 100);
        txn.log_node_deletion(TableId(1), 50);

        let local = txn.local_wal().unwrap();
        // 1 (BeginTransaction at creation) + 2 payload records = 3
        assert_eq!(local.record_count(), 3);
    }

    #[test]
    fn log_wal_entries_read_only_is_noop() {
        let mut txn = Transaction::new(TransactionType::ReadOnly, TXN_ID, START_TS);
        txn.log_table_insertion(TableId(1), 100);
        assert!(txn.local_wal().is_none());
    }
}
