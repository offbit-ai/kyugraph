//! Transaction manager — begin/commit/rollback lifecycle.
//!
//! Dual-mutex design matching Kuzu's `TransactionManager`:
//! - `mtx_public_calls` serializes commit/rollback
//! - `mtx_new_transactions` serializes begin (separate for future checkpoint wait)

use std::sync::Mutex;

use kyu_common::id::TxnTs;

use crate::transaction::Transaction;
use crate::types::{START_TRANSACTION_ID, TransactionState, TransactionType};
use crate::wal::Wal;

/// Transaction manager error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionManagerError {
    /// Attempted to begin a write transaction while one is already active.
    WriteTransactionActive,
    /// Transaction is not in the expected state.
    InvalidState {
        expected: TransactionState,
        actual: TransactionState,
    },
}

impl std::fmt::Display for TransactionManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WriteTransactionActive => {
                write!(
                    f,
                    "a write transaction is already active (single-writer mode)"
                )
            }
            Self::InvalidState { expected, actual } => {
                write!(
                    f,
                    "transaction state: expected {expected:?}, got {actual:?}"
                )
            }
        }
    }
}

impl std::error::Error for TransactionManagerError {}

struct Inner {
    last_transaction_id: u64,
    last_timestamp: TxnTs,
    active_write_txn: Option<u64>,
    active_read_count: u32,
}

/// Manages transaction lifecycle: begin, commit, rollback.
pub struct TransactionManager {
    mtx_public_calls: Mutex<()>,
    mtx_new_transactions: Mutex<()>,
    inner: Mutex<Inner>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            mtx_public_calls: Mutex::new(()),
            mtx_new_transactions: Mutex::new(()),
            inner: Mutex::new(Inner {
                last_transaction_id: START_TRANSACTION_ID,
                last_timestamp: 0,
                active_write_txn: None,
                active_read_count: 0,
            }),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&self, txn_type: TransactionType) -> Result<Transaction, TransactionManagerError> {
        let _new_txn_guard = self.mtx_new_transactions.lock().unwrap();
        let mut inner = self.inner.lock().unwrap();

        // Single-writer enforcement.
        if txn_type.is_write() && inner.active_write_txn.is_some() {
            return Err(TransactionManagerError::WriteTransactionActive);
        }

        inner.last_transaction_id += 1;
        let txn_id = inner.last_transaction_id;
        let start_ts = inner.last_timestamp;

        let txn = Transaction::new(txn_type, txn_id, start_ts);

        match txn_type {
            TransactionType::Write => {
                inner.active_write_txn = Some(txn_id);
            }
            TransactionType::ReadOnly => {
                inner.active_read_count += 1;
            }
            _ => {}
        }

        Ok(txn)
    }

    /// Commit a transaction.
    /// Assigns commit timestamp, runs undo callbacks, flushes WAL, updates state.
    pub fn commit<F>(
        &self,
        txn: &mut Transaction,
        wal: &Wal,
        commit_callback: F,
    ) -> Result<(), CommitError>
    where
        F: FnMut(crate::undo_buffer::UndoRecord, u64),
    {
        let _public_guard = self.mtx_public_calls.lock().unwrap();

        if txn.state() != TransactionState::Active {
            return Err(CommitError::Manager(
                TransactionManagerError::InvalidState {
                    expected: TransactionState::Active,
                    actual: txn.state(),
                },
            ));
        }

        // Assign commit timestamp.
        {
            let mut inner = self.inner.lock().unwrap();
            inner.last_timestamp += 1;
            txn.set_commit_ts(inner.last_timestamp);
        }

        // Run undo buffer commit + flush WAL.
        txn.commit(wal, commit_callback).map_err(CommitError::Io)?;

        // Remove from active transactions.
        {
            let mut inner = self.inner.lock().unwrap();
            match txn.txn_type() {
                TransactionType::Write => {
                    if inner.active_write_txn == Some(txn.id()) {
                        inner.active_write_txn = None;
                    }
                }
                TransactionType::ReadOnly => {
                    inner.active_read_count = inner.active_read_count.saturating_sub(1);
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Rollback a transaction.
    pub fn rollback<F>(
        &self,
        txn: &mut Transaction,
        rollback_callback: F,
    ) -> Result<(), TransactionManagerError>
    where
        F: FnMut(crate::undo_buffer::UndoRecord),
    {
        let _public_guard = self.mtx_public_calls.lock().unwrap();

        if txn.state() != TransactionState::Active {
            return Err(TransactionManagerError::InvalidState {
                expected: TransactionState::Active,
                actual: txn.state(),
            });
        }

        txn.rollback(rollback_callback);

        // Remove from active transactions.
        let mut inner = self.inner.lock().unwrap();
        match txn.txn_type() {
            TransactionType::Write => {
                if inner.active_write_txn == Some(txn.id()) {
                    inner.active_write_txn = None;
                }
            }
            TransactionType::ReadOnly => {
                inner.active_read_count = inner.active_read_count.saturating_sub(1);
            }
            _ => {}
        }

        Ok(())
    }

    /// Whether there's an active write transaction.
    pub fn has_active_write_txn(&self) -> bool {
        self.inner.lock().unwrap().active_write_txn.is_some()
    }

    /// Number of active read-only transactions.
    pub fn active_read_count(&self) -> u32 {
        self.inner.lock().unwrap().active_read_count
    }

    /// Current global timestamp.
    pub fn last_timestamp(&self) -> TxnTs {
        self.inner.lock().unwrap().last_timestamp
    }

    /// Block new transactions (for future checkpoint support).
    /// Returns a guard that, when dropped, re-enables new transactions.
    pub fn stop_new_transactions(&self) -> impl Drop + '_ {
        self.mtx_new_transactions.lock().unwrap()
    }
}

/// Commit error (manager error or I/O error during WAL flush).
#[derive(Debug)]
pub enum CommitError {
    Manager(TransactionManagerError),
    Io(std::io::Error),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manager(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "WAL I/O error: {e}"),
        }
    }
}

impl std::error::Error for CommitError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::undo_buffer::UndoRecordType;

    #[test]
    fn new_manager() {
        let mgr = TransactionManager::new();
        assert!(!mgr.has_active_write_txn());
        assert_eq!(mgr.active_read_count(), 0);
        assert_eq!(mgr.last_timestamp(), 0);
    }

    #[test]
    fn begin_read_only() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(TransactionType::ReadOnly).unwrap();
        assert!(txn.is_read_only());
        assert_eq!(txn.start_ts(), 0);
        assert_eq!(mgr.active_read_count(), 1);
    }

    #[test]
    fn begin_write() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(TransactionType::Write).unwrap();
        assert!(txn.is_write());
        assert!(mgr.has_active_write_txn());
    }

    #[test]
    fn single_writer_enforcement() {
        let mgr = TransactionManager::new();
        let _txn1 = mgr.begin(TransactionType::Write).unwrap();

        let result = mgr.begin(TransactionType::Write);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            TransactionManagerError::WriteTransactionActive
        );
    }

    #[test]
    fn read_while_write_active() {
        let mgr = TransactionManager::new();
        let _txn_write = mgr.begin(TransactionType::Write).unwrap();

        // Read-only transactions can start while write is active.
        let txn_read = mgr.begin(TransactionType::ReadOnly).unwrap();
        assert!(txn_read.is_read_only());
        assert_eq!(mgr.active_read_count(), 1);
    }

    #[test]
    fn commit_write_transaction() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();
        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        txn.push_insert_info(0, 0, 100);

        let mut committed = Vec::new();
        mgr.commit(&mut txn, &wal, |record, ts| {
            committed.push((record, ts));
        })
        .unwrap();

        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(txn.commit_ts(), 1); // First commit gets timestamp 1.
        assert!(!mgr.has_active_write_txn());
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0.record_type, UndoRecordType::InsertInfo);
    }

    #[test]
    fn commit_read_only_transaction() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();
        let mut txn = mgr.begin(TransactionType::ReadOnly).unwrap();

        mgr.commit(&mut txn, &wal, |_, _| {}).unwrap();
        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(mgr.active_read_count(), 0);
    }

    #[test]
    fn rollback_write_transaction() {
        let mgr = TransactionManager::new();
        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        txn.push_insert_info(0, 0, 10);

        let mut rolled_back = Vec::new();
        mgr.rollback(&mut txn, |record| {
            rolled_back.push(record);
        })
        .unwrap();

        assert_eq!(txn.state(), TransactionState::RolledBack);
        assert!(!mgr.has_active_write_txn());
        assert_eq!(rolled_back.len(), 1);
    }

    #[test]
    fn rollback_read_only_transaction() {
        let mgr = TransactionManager::new();
        let mut txn = mgr.begin(TransactionType::ReadOnly).unwrap();

        mgr.rollback(&mut txn, |_| {}).unwrap();
        assert_eq!(txn.state(), TransactionState::RolledBack);
        assert_eq!(mgr.active_read_count(), 0);
    }

    #[test]
    fn commit_increments_timestamp() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();

        let mut txn1 = mgr.begin(TransactionType::Write).unwrap();
        mgr.commit(&mut txn1, &wal, |_, _| {}).unwrap();
        assert_eq!(mgr.last_timestamp(), 1);

        let mut txn2 = mgr.begin(TransactionType::Write).unwrap();
        mgr.commit(&mut txn2, &wal, |_, _| {}).unwrap();
        assert_eq!(mgr.last_timestamp(), 2);
    }

    #[test]
    fn new_write_after_commit() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();

        let mut txn1 = mgr.begin(TransactionType::Write).unwrap();
        mgr.commit(&mut txn1, &wal, |_, _| {}).unwrap();

        // New write transaction should succeed after commit.
        let txn2 = mgr.begin(TransactionType::Write).unwrap();
        assert!(txn2.is_write());
        assert_eq!(txn2.start_ts(), 1); // Sees committed timestamp.
    }

    #[test]
    fn new_write_after_rollback() {
        let mgr = TransactionManager::new();

        let mut txn1 = mgr.begin(TransactionType::Write).unwrap();
        mgr.rollback(&mut txn1, |_| {}).unwrap();

        let txn2 = mgr.begin(TransactionType::Write).unwrap();
        assert!(txn2.is_write());
    }

    #[test]
    fn transaction_ids_are_unique() {
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin(TransactionType::ReadOnly).unwrap();
        let txn2 = mgr.begin(TransactionType::ReadOnly).unwrap();
        assert_ne!(txn1.id(), txn2.id());
        assert!(txn1.id() > START_TRANSACTION_ID);
        assert!(txn2.id() > START_TRANSACTION_ID);
    }

    #[test]
    fn commit_already_committed_fails() {
        let mgr = TransactionManager::new();
        let wal = Wal::in_memory();
        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        mgr.commit(&mut txn, &wal, |_, _| {}).unwrap();

        let result = mgr.commit(&mut txn, &wal, |_, _| {});
        assert!(result.is_err());
    }

    #[test]
    fn rollback_already_rolled_back_fails() {
        let mgr = TransactionManager::new();
        let mut txn = mgr.begin(TransactionType::Write).unwrap();
        mgr.rollback(&mut txn, |_| {}).unwrap();

        let result = mgr.rollback(&mut txn, |_| {});
        assert!(result.is_err());
    }

    #[test]
    fn error_display() {
        let err = TransactionManagerError::WriteTransactionActive;
        assert!(err.to_string().contains("single-writer"));
    }
}
