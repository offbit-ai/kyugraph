//! WAL recovery — replay committed transactions from the WAL.
//!
//! Parses the WAL file, groups records by transaction (BeginTransaction..Commit),
//! and returns only committed transactions. Incomplete transactions (no Commit
//! record) are skipped, handling crash recovery gracefully.

use std::path::{Path, PathBuf};

use crate::wal_record::{WalDeserializeError, WalRecord};

/// Result of replaying the WAL.
#[derive(Debug)]
pub struct WalReplayResult {
    /// Total number of raw records read.
    pub records_read: u64,
    /// Number of fully committed transactions found.
    pub committed_transactions: u64,
    /// Number of incomplete (not committed) transactions found.
    pub incomplete_transactions: u64,
    /// Whether the WAL ends with a Checkpoint record.
    pub ends_with_checkpoint: bool,
    /// All records from committed transactions, in order.
    pub committed_records: Vec<WalRecord>,
}

/// WAL replayer for crash recovery.
pub struct WalReplayer {
    wal_path: PathBuf,
}

impl WalReplayer {
    pub fn new(wal_path: &Path) -> Self {
        Self {
            wal_path: wal_path.to_path_buf(),
        }
    }

    /// Replay the WAL file and return committed transaction records.
    pub fn replay(&self) -> Result<WalReplayResult, WalReplayError> {
        if !self.wal_path.exists() {
            return Ok(WalReplayResult {
                records_read: 0,
                committed_transactions: 0,
                incomplete_transactions: 0,
                ends_with_checkpoint: false,
                committed_records: Vec::new(),
            });
        }

        let data = std::fs::read(&self.wal_path).map_err(|e| WalReplayError::Io(e.to_string()))?;

        Self::replay_from_bytes(&data)
    }

    /// Replay from a byte slice (also used for in-memory WAL testing).
    pub fn replay_from_bytes(data: &[u8]) -> Result<WalReplayResult, WalReplayError> {
        let mut records_read = 0u64;
        let mut committed_transactions = 0u64;
        let mut incomplete_transactions = 0u64;
        let mut ends_with_checkpoint = false;
        let mut committed_records = Vec::new();

        // Current transaction being accumulated.
        let mut current_txn: Vec<WalRecord> = Vec::new();
        let mut in_transaction = false;

        let mut offset = 0;
        while offset < data.len() {
            let remaining = &data[offset..];
            let (record, consumed) = match WalRecord::deserialize(remaining) {
                Ok((r, c)) => (r, c),
                Err(WalDeserializeError::UnexpectedEof) => {
                    // Truncated record at end of WAL — crash recovery.
                    break;
                }
                Err(e) => {
                    return Err(WalReplayError::Deserialize(e));
                }
            };

            records_read += 1;
            offset += consumed;

            match &record {
                WalRecord::BeginTransaction => {
                    if in_transaction {
                        // Previous transaction was incomplete (no Commit).
                        incomplete_transactions += 1;
                        current_txn.clear();
                    }
                    in_transaction = true;
                    current_txn.push(record);
                    ends_with_checkpoint = false;
                }
                WalRecord::Commit => {
                    if in_transaction {
                        current_txn.push(record);
                        committed_records.append(&mut current_txn);
                        committed_transactions += 1;
                        in_transaction = false;
                    }
                    ends_with_checkpoint = false;
                }
                WalRecord::Checkpoint => {
                    if in_transaction {
                        incomplete_transactions += 1;
                        current_txn.clear();
                        in_transaction = false;
                    }
                    committed_records.push(record);
                    ends_with_checkpoint = true;
                }
                _ => {
                    if in_transaction {
                        current_txn.push(record);
                    }
                    ends_with_checkpoint = false;
                }
            }
        }

        // Any remaining open transaction is incomplete.
        if in_transaction {
            incomplete_transactions += 1;
        }

        Ok(WalReplayResult {
            records_read,
            committed_transactions,
            incomplete_transactions,
            ends_with_checkpoint,
            committed_records,
        })
    }
}

/// WAL replay error.
#[derive(Debug)]
pub enum WalReplayError {
    Io(String),
    Deserialize(WalDeserializeError),
}

impl std::fmt::Display for WalReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL I/O error: {e}"),
            Self::Deserialize(e) => write!(f, "WAL deserialization error: {e}"),
        }
    }
}

impl std::error::Error for WalReplayError {}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_common::id::TableId;

    fn make_committed_txn(table_id: u64, num_rows: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        WalRecord::BeginTransaction.serialize(&mut buf);
        WalRecord::TableInsertion {
            table_id: TableId(table_id),
            num_rows,
        }
        .serialize(&mut buf);
        WalRecord::Commit.serialize(&mut buf);
        buf
    }

    #[test]
    fn empty_wal() {
        let result = WalReplayer::replay_from_bytes(&[]).unwrap();
        assert_eq!(result.records_read, 0);
        assert_eq!(result.committed_transactions, 0);
        assert_eq!(result.incomplete_transactions, 0);
        assert!(!result.ends_with_checkpoint);
        assert!(result.committed_records.is_empty());
    }

    #[test]
    fn single_committed_transaction() {
        let data = make_committed_txn(1, 100);
        let result = WalReplayer::replay_from_bytes(&data).unwrap();

        assert_eq!(result.records_read, 3);
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.incomplete_transactions, 0);
        assert_eq!(result.committed_records.len(), 3);
        assert_eq!(result.committed_records[0], WalRecord::BeginTransaction);
        assert_eq!(result.committed_records[2], WalRecord::Commit);
    }

    #[test]
    fn multiple_committed_transactions() {
        let mut data = make_committed_txn(1, 100);
        data.extend(make_committed_txn(2, 200));
        data.extend(make_committed_txn(3, 300));

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 3);
        assert_eq!(result.committed_records.len(), 9);
    }

    #[test]
    fn incomplete_transaction_skipped() {
        let mut data = make_committed_txn(1, 100);
        // Incomplete: BeginTransaction + insertion, no Commit.
        WalRecord::BeginTransaction.serialize(&mut data);
        WalRecord::TableInsertion {
            table_id: TableId(2),
            num_rows: 50,
        }
        .serialize(&mut data);

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.incomplete_transactions, 1);
        assert_eq!(result.committed_records.len(), 3); // Only first txn.
    }

    #[test]
    fn truncated_wal_recovery() {
        let mut data = make_committed_txn(1, 100);
        // Add partial data.
        data.push(30); // TableInsertion type byte
        data.push(0); // Partial payload_len

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.committed_records.len(), 3);
    }

    #[test]
    fn checkpoint_at_end() {
        let mut data = make_committed_txn(1, 100);
        WalRecord::Checkpoint.serialize(&mut data);

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert!(result.ends_with_checkpoint);
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.committed_records.len(), 4); // 3 + checkpoint
    }

    #[test]
    fn checkpoint_not_at_end() {
        let mut data = Vec::new();
        WalRecord::Checkpoint.serialize(&mut data);
        data.extend(make_committed_txn(1, 50));

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert!(!result.ends_with_checkpoint); // Last record is Commit, not Checkpoint.
        assert_eq!(result.committed_transactions, 1);
    }

    #[test]
    fn incomplete_before_committed() {
        // Incomplete transaction, then a committed one.
        let mut data = Vec::new();
        WalRecord::BeginTransaction.serialize(&mut data);
        WalRecord::TableInsertion {
            table_id: TableId(1),
            num_rows: 10,
        }
        .serialize(&mut data);
        // No Commit — next BeginTransaction starts new txn.
        data.extend(make_committed_txn(2, 50));

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.incomplete_transactions, 1);
    }

    #[test]
    fn file_backed_replay_nonexistent() {
        let replayer = WalReplayer::new(Path::new("/tmp/nonexistent_wal_file_kyu"));
        let result = replayer.replay().unwrap();
        assert_eq!(result.records_read, 0);
    }

    #[test]
    fn checkpoint_interrupts_incomplete_txn() {
        let mut data = Vec::new();
        WalRecord::BeginTransaction.serialize(&mut data);
        WalRecord::TableInsertion {
            table_id: TableId(1),
            num_rows: 10,
        }
        .serialize(&mut data);
        // Checkpoint interrupts the incomplete transaction.
        WalRecord::Checkpoint.serialize(&mut data);

        let result = WalReplayer::replay_from_bytes(&data).unwrap();
        assert_eq!(result.committed_transactions, 0);
        assert_eq!(result.incomplete_transactions, 1);
        assert!(result.ends_with_checkpoint);
        assert_eq!(result.committed_records.len(), 1); // Just the checkpoint.
    }
}
