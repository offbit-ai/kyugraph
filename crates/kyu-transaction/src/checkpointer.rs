//! Checkpointer — bounds WAL growth by flushing state and truncating the log.
//!
//! Quiesces new transactions, waits for active ones to drain, writes a
//! checkpoint WAL record, then clears the log. This bounds crash-recovery
//! replay time.

use std::sync::Arc;
use std::time::Duration;

use crate::manager::TransactionManager;
use crate::wal::Wal;

/// Default WAL size threshold (256 MB) before auto-checkpoint triggers.
pub const DEFAULT_CHECKPOINT_THRESHOLD: u64 = 256 * 1024 * 1024;

/// Maximum time to wait for active transactions to drain during checkpoint.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Drain poll interval.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Checkpointer — writes a checkpoint record and truncates the WAL.
pub struct Checkpointer {
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    threshold: u64,
    /// Optional callback invoked during checkpoint (after draining, before WAL truncation)
    /// to flush application state (catalog + storage) to disk.
    flush_fn: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
}

impl Checkpointer {
    /// Create a new checkpointer.
    pub fn new(txn_mgr: Arc<TransactionManager>, wal: Arc<Wal>) -> Self {
        Self {
            txn_mgr,
            wal,
            threshold: DEFAULT_CHECKPOINT_THRESHOLD,
            flush_fn: None,
        }
    }

    /// Create with a custom WAL size threshold.
    pub fn with_threshold(txn_mgr: Arc<TransactionManager>, wal: Arc<Wal>, threshold: u64) -> Self {
        Self { txn_mgr, wal, threshold, flush_fn: None }
    }

    /// Set a flush callback invoked during checkpoint to persist application state.
    pub fn with_flush(mut self, f: Arc<dyn Fn() -> Result<(), String> + Send + Sync>) -> Self {
        self.flush_fn = Some(f);
        self
    }

    /// Returns `true` if the WAL size exceeds the checkpoint threshold.
    pub fn should_checkpoint(&self) -> bool {
        self.wal.file_size() >= self.threshold
    }

    /// Execute a checkpoint: block new transactions, drain active ones,
    /// write checkpoint record, and truncate the WAL.
    ///
    /// Returns the LSN of the checkpoint record, or an error if draining
    /// times out or WAL I/O fails.
    pub fn checkpoint(&self) -> Result<u64, CheckpointError> {
        // 1. Block new transactions.
        let _guard = self.txn_mgr.stop_new_transactions();

        // 2. Wait for active transactions to drain.
        let deadline = std::time::Instant::now() + DRAIN_TIMEOUT;
        loop {
            if !self.txn_mgr.has_active_write_txn()
                && self.txn_mgr.active_read_count() == 0
            {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(CheckpointError::DrainTimeout);
            }
            std::thread::sleep(DRAIN_POLL_INTERVAL);
        }

        // 3. Flush application state (catalog + storage) to disk.
        if let Some(ref flush) = self.flush_fn {
            flush().map_err(CheckpointError::Flush)?;
        }

        // 4. Write checkpoint record.
        let lsn = self.wal.log_checkpoint().map_err(CheckpointError::Io)?;

        // 5. Truncate WAL.
        self.wal.clear().map_err(CheckpointError::Io)?;

        Ok(lsn)
    }

    /// Try to checkpoint if the WAL exceeds the threshold.
    /// Returns `Ok(Some(lsn))` if a checkpoint was performed, `Ok(None)` if
    /// the threshold wasn't met.
    pub fn try_checkpoint(&self) -> Result<Option<u64>, CheckpointError> {
        if self.should_checkpoint() {
            self.checkpoint().map(Some)
        } else {
            Ok(None)
        }
    }
}

/// Checkpoint error.
#[derive(Debug)]
pub enum CheckpointError {
    /// Active transactions did not drain within the timeout.
    DrainTimeout,
    /// WAL I/O error.
    Io(std::io::Error),
    /// Flush callback failed.
    Flush(String),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DrainTimeout => write!(f, "checkpoint timed out waiting for active transactions to drain"),
            Self::Io(e) => write!(f, "checkpoint WAL I/O error: {e}"),
            Self::Flush(e) => write!(f, "checkpoint flush error: {e}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TransactionType, Wal};

    #[test]
    fn checkpoint_empty_wal() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());
        let cp = Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal));

        let lsn = cp.checkpoint().unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(wal.file_size(), 0); // Cleared after checkpoint
    }

    #[test]
    fn checkpoint_after_commits() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());

        // Perform some write transactions.
        for _ in 0..5 {
            let mut txn = txn_mgr.begin(TransactionType::Write).unwrap();
            txn.log_table_insertion(kyu_common::id::TableId(1), 100);
            txn_mgr.commit(&mut txn, &wal, |_, _| {}).unwrap();
        }

        assert!(wal.file_size() > 0);

        let cp = Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal));
        let lsn = cp.checkpoint().unwrap();
        assert!(lsn > 0);
        assert_eq!(wal.file_size(), 0);
    }

    #[test]
    fn should_checkpoint_threshold() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());

        // With default threshold (256 MB), empty WAL should not trigger.
        let cp = Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal));
        assert!(!cp.should_checkpoint());

        // With threshold = 0, should always trigger.
        let cp_zero = Checkpointer::with_threshold(Arc::clone(&txn_mgr), Arc::clone(&wal), 0);
        assert!(cp_zero.should_checkpoint());
    }

    #[test]
    fn try_checkpoint_below_threshold() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());
        let cp = Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal));

        let result = cp.try_checkpoint().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn try_checkpoint_above_threshold() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());

        // Commit some data.
        let mut txn = txn_mgr.begin(TransactionType::Write).unwrap();
        txn.log_table_insertion(kyu_common::id::TableId(1), 100);
        txn_mgr.commit(&mut txn, &wal, |_, _| {}).unwrap();

        // Use threshold = 1 byte so any data triggers checkpoint.
        let cp = Checkpointer::with_threshold(Arc::clone(&txn_mgr), Arc::clone(&wal), 1);
        let result = cp.try_checkpoint().unwrap();
        assert!(result.is_some());
        assert_eq!(wal.file_size(), 0);
    }

    #[test]
    fn checkpoint_waits_for_read_to_finish() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());

        // Start a read transaction.
        let mut read_txn = txn_mgr.begin(TransactionType::ReadOnly).unwrap();

        // Checkpoint in a background thread.
        let txn_mgr2 = Arc::clone(&txn_mgr);
        let wal2 = Arc::clone(&wal);
        let handle = std::thread::spawn(move || {
            let cp = Checkpointer::new(txn_mgr2, wal2);
            cp.checkpoint()
        });

        // Let the checkpoint start waiting, then finish the read.
        std::thread::sleep(Duration::from_millis(50));
        txn_mgr.commit(&mut read_txn, &wal, |_, _| {}).unwrap();

        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }
}
