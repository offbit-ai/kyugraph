//! Transaction types and constants.

/// Invalid transaction marker — means "no transaction" or "not yet committed".
pub const INVALID_TRANSACTION: u64 = u64::MAX;

/// Transaction IDs start at this value. Values below are commit timestamps.
/// This lets visibility checks distinguish "own uncommitted write" from
/// "committed before snapshot" with a single comparison.
pub const START_TRANSACTION_ID: u64 = 1u64 << 63;

/// Number of rows per vector in the storage layer.
/// Version arrays are exactly this size when allocated.
pub const VECTOR_CAPACITY: usize = 2048;

/// The kind of transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionType {
    ReadOnly,
    Write,
    Checkpoint,
    Recovery,
}

impl TransactionType {
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    pub fn is_write(self) -> bool {
        matches!(self, Self::Write)
    }

    /// Whether this transaction type should log to the WAL.
    pub fn should_log_to_wal(self) -> bool {
        matches!(self, Self::Write)
    }

    /// Whether this transaction type should append to the undo buffer.
    pub fn should_append_to_undo_buffer(self) -> bool {
        matches!(self, Self::Write | Self::Recovery)
    }
}

/// The lifecycle state of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants() {
        assert_eq!(INVALID_TRANSACTION, u64::MAX);
        assert_eq!(START_TRANSACTION_ID, 1u64 << 63);
        assert!(START_TRANSACTION_ID > 0);
        assert_eq!(VECTOR_CAPACITY, 2048);
    }

    #[test]
    fn txn_id_space() {
        // Any commit timestamp is below START_TRANSACTION_ID
        let commit_ts = 1000u64;
        assert!(commit_ts < START_TRANSACTION_ID);

        // Any transaction ID is >= START_TRANSACTION_ID
        let txn_id = START_TRANSACTION_ID + 1;
        assert!(txn_id >= START_TRANSACTION_ID);
    }

    #[test]
    fn transaction_type_read_only() {
        assert!(TransactionType::ReadOnly.is_read_only());
        assert!(!TransactionType::ReadOnly.is_write());
        assert!(!TransactionType::ReadOnly.should_log_to_wal());
        assert!(!TransactionType::ReadOnly.should_append_to_undo_buffer());
    }

    #[test]
    fn transaction_type_write() {
        assert!(!TransactionType::Write.is_read_only());
        assert!(TransactionType::Write.is_write());
        assert!(TransactionType::Write.should_log_to_wal());
        assert!(TransactionType::Write.should_append_to_undo_buffer());
    }

    #[test]
    fn transaction_type_checkpoint() {
        assert!(!TransactionType::Checkpoint.is_read_only());
        assert!(!TransactionType::Checkpoint.is_write());
        assert!(!TransactionType::Checkpoint.should_log_to_wal());
        assert!(!TransactionType::Checkpoint.should_append_to_undo_buffer());
    }

    #[test]
    fn transaction_type_recovery() {
        assert!(!TransactionType::Recovery.is_read_only());
        assert!(!TransactionType::Recovery.is_write());
        assert!(!TransactionType::Recovery.should_log_to_wal());
        assert!(TransactionType::Recovery.should_append_to_undo_buffer());
    }

    #[test]
    fn transaction_state_variants() {
        assert_ne!(TransactionState::Active, TransactionState::Committed);
        assert_ne!(TransactionState::Active, TransactionState::RolledBack);
        assert_ne!(TransactionState::Committed, TransactionState::RolledBack);
    }

    #[test]
    fn transaction_type_copy() {
        let t = TransactionType::Write;
        let t2 = t;
        assert_eq!(t, t2);
    }

    #[test]
    fn transaction_state_copy() {
        let s = TransactionState::Active;
        let s2 = s;
        assert_eq!(s, s2);
    }

    #[test]
    fn invalid_transaction_is_above_start_id() {
        assert!(INVALID_TRANSACTION > START_TRANSACTION_ID);
    }
}
