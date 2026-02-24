//! MVCC row visibility based on version info per vector/node-group.
//!
//! Faithful port of Kuzu's `version_info.cpp` with all performance optimizations:
//! - Enum status gates on hot path (avoid array access when possible)
//! - Lazy allocation with same-version scalar optimization
//! - Fixed-size boxed slices instead of Vec for version arrays

use crate::types::{INVALID_TRANSACTION, START_TRANSACTION_ID, VECTOR_CAPACITY};

/// Insertion status for a vector — enum gate for fast-path visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertionStatus {
    /// No rows have been inserted (all invisible).
    NoInserted,
    /// Some rows need per-row version checking.
    CheckVersion,
    /// All rows are always inserted (checkpointed data).
    AlwaysInserted,
}

/// Deletion status for a vector — enum gate for fast-path visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeletionStatus {
    /// No rows have been deleted.
    NoDeleted,
    /// Some rows need per-row version checking.
    CheckVersion,
}

/// Per-vector (2048 rows) MVCC version info.
///
/// Uses lazy allocation: when all rows share one version, stores a single scalar.
/// Only allocates the full 16KB array when multiple versions appear.
pub struct VectorVersionInfo {
    insertion_status: InsertionStatus,
    deletion_status: DeletionStatus,
    /// Scalar optimization: when all inserted rows share one version.
    same_insertion_version: u64,
    /// Scalar optimization: when all deleted rows share one version.
    same_deletion_version: u64,
    /// Per-row insertion versions. `None` = use same_insertion_version.
    inserted_versions: Option<Box<[u64]>>,
    /// Per-row deletion versions. `None` = use same_deletion_version.
    deleted_versions: Option<Box<[u64]>>,
}

impl Default for VectorVersionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorVersionInfo {
    pub fn new() -> Self {
        Self {
            insertion_status: InsertionStatus::NoInserted,
            deletion_status: DeletionStatus::NoDeleted,
            same_insertion_version: INVALID_TRANSACTION,
            same_deletion_version: INVALID_TRANSACTION,
            inserted_versions: None,
            deleted_versions: None,
        }
    }

    pub fn insertion_status(&self) -> InsertionStatus {
        self.insertion_status
    }

    pub fn deletion_status(&self) -> DeletionStatus {
        self.deletion_status
    }

    /// Allocate the full insertion version array, filling with same_insertion_version.
    fn init_insertion_version_array(&mut self) {
        let arr = vec![self.same_insertion_version; VECTOR_CAPACITY].into_boxed_slice();
        self.inserted_versions = Some(arr);
    }

    /// Allocate the full deletion version array, filling with same_deletion_version.
    fn init_deletion_version_array(&mut self) {
        let arr = vec![self.same_deletion_version; VECTOR_CAPACITY].into_boxed_slice();
        self.deleted_versions = Some(arr);
    }

    /// Record that rows [start_row..start_row+num_rows) were inserted by `transaction_id`.
    pub fn append(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        self.insertion_status = InsertionStatus::CheckVersion;

        // Same-version optimization: if this txn matches the current single version, nothing to do.
        if transaction_id == self.same_insertion_version {
            return;
        }

        // First insertion — store as the single version.
        if self.inserted_versions.is_none() && self.same_insertion_version == INVALID_TRANSACTION {
            self.same_insertion_version = transaction_id;
            return;
        }

        // Second distinct version — need the full array.
        if self.inserted_versions.is_none() {
            self.init_insertion_version_array();
        }

        let versions = self.inserted_versions.as_mut().unwrap();
        let end = (start_row + num_rows) as usize;
        for i in start_row as usize..end {
            versions[i] = transaction_id;
        }
    }

    /// Record that rows [start_row..start_row+num_rows) were deleted by `transaction_id`.
    /// Returns an error if any row is already deleted by a different transaction (write-write conflict).
    ///
    /// Always uses the per-row array (no same-version scalar optimization for deletions,
    /// since partial deletions are common and the scalar would incorrectly apply to all rows).
    pub fn delete(
        &mut self,
        transaction_id: u64,
        start_row: u64,
        num_rows: u64,
    ) -> Result<(), WriteConflict> {
        // Check for write-write conflict before modifying state.
        if self.deletion_status == DeletionStatus::CheckVersion
            && let Some(ref versions) = self.deleted_versions
        {
            let end = (start_row + num_rows) as usize;
            for i in start_row as usize..end {
                let v = versions[i];
                if v != INVALID_TRANSACTION && v != transaction_id {
                    return Err(WriteConflict { row: i as u64 });
                }
            }
        }

        self.deletion_status = DeletionStatus::CheckVersion;

        // Always use per-row array for deletions.
        if self.deleted_versions.is_none() {
            self.init_deletion_version_array();
        }

        let versions = self.deleted_versions.as_mut().unwrap();
        let end = (start_row + num_rows) as usize;
        for i in start_row as usize..end {
            versions[i] = transaction_id;
        }
        Ok(())
    }

    /// Check if a row is inserted and visible to (start_ts, txn_id).
    #[inline]
    fn is_inserted(&self, start_ts: u64, txn_id: u64, row: u64) -> bool {
        if self.insertion_status == InsertionStatus::AlwaysInserted {
            return true;
        }
        if let Some(ref versions) = self.inserted_versions {
            let v = versions[row as usize];
            v == txn_id || (v < START_TRANSACTION_ID && v <= start_ts)
        } else {
            let v = self.same_insertion_version;
            v == txn_id || (v < START_TRANSACTION_ID && v <= start_ts)
        }
    }

    /// Check if a row is deleted and visible to (start_ts, txn_id).
    #[inline]
    fn is_deleted(&self, start_ts: u64, txn_id: u64, row: u64) -> bool {
        if self.deletion_status == DeletionStatus::NoDeleted {
            return false;
        }
        // Deletions always use the per-row array for correctness
        // (same-version scalar would incorrectly apply to non-deleted rows).
        if let Some(ref versions) = self.deleted_versions {
            let v = versions[row as usize];
            v != INVALID_TRANSACTION && (v == txn_id || (v < START_TRANSACTION_ID && v <= start_ts))
        } else {
            false
        }
    }

    /// Check if a row is visible (inserted AND NOT deleted).
    /// Hot path: enum gates checked first to avoid array access.
    #[inline]
    pub fn is_selected(&self, start_ts: u64, txn_id: u64, row: u64) -> bool {
        // Fast path: all inserted, none deleted → always visible.
        if self.deletion_status == DeletionStatus::NoDeleted
            && self.insertion_status == InsertionStatus::AlwaysInserted
        {
            return true;
        }
        // Fast path: nothing inserted → invisible.
        if self.insertion_status == InsertionStatus::NoInserted {
            return false;
        }
        // Slow path: check per-row versions.
        self.is_inserted(start_ts, txn_id, row) && !self.is_deleted(start_ts, txn_id, row)
    }

    /// Build a selection vector of visible rows in [start_row..start_row+num_rows).
    /// Returns the selected row indices (relative to start_output_pos).
    pub fn get_selected_rows(
        &self,
        start_ts: u64,
        txn_id: u64,
        start_row: u64,
        num_rows: u64,
        start_output_pos: u64,
    ) -> Vec<u64> {
        // Fast path: all visible.
        if self.deletion_status == DeletionStatus::NoDeleted
            && self.insertion_status == InsertionStatus::AlwaysInserted
        {
            return (0..num_rows).map(|i| start_output_pos + i).collect();
        }
        // Fast path: nothing inserted.
        if self.insertion_status == InsertionStatus::NoInserted {
            return Vec::new();
        }
        let mut selected = Vec::new();
        for i in 0..num_rows {
            let row = start_row + i;
            if self.is_inserted(start_ts, txn_id, row) && !self.is_deleted(start_ts, txn_id, row) {
                selected.push(start_output_pos + i);
            }
        }
        selected
    }

    /// Finalize insertion commit: replace transaction_id with commit_ts for rows [start_row..+num_rows).
    pub fn set_insert_commit_ts(
        &mut self,
        transaction_id: u64,
        commit_ts: u64,
        start_row: u64,
        num_rows: u64,
    ) {
        if self.inserted_versions.is_none() {
            if self.same_insertion_version == transaction_id {
                self.same_insertion_version = commit_ts;
            }
            return;
        }
        let versions = self.inserted_versions.as_mut().unwrap();
        let end = (start_row + num_rows) as usize;
        for i in start_row as usize..end {
            if versions[i] == transaction_id {
                versions[i] = commit_ts;
            }
        }
    }

    /// Finalize deletion commit: replace transaction_id with commit_ts.
    pub fn set_delete_commit_ts(
        &mut self,
        transaction_id: u64,
        commit_ts: u64,
        start_row: u64,
        num_rows: u64,
    ) {
        if let Some(ref mut versions) = self.deleted_versions {
            let end = (start_row + num_rows) as usize;
            for i in start_row as usize..end {
                if versions[i] == transaction_id {
                    versions[i] = commit_ts;
                }
            }
        }
    }

    /// Rollback insertions: reset rows back to INVALID_TRANSACTION.
    pub fn rollback_insertions(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        if self.inserted_versions.is_none() {
            if self.same_insertion_version == transaction_id {
                self.same_insertion_version = INVALID_TRANSACTION;
                self.insertion_status = InsertionStatus::NoInserted;
            }
            return;
        }
        let versions = self.inserted_versions.as_mut().unwrap();
        let end = (start_row + num_rows) as usize;
        let mut any_remaining = false;
        for i in start_row as usize..end {
            if versions[i] == transaction_id {
                versions[i] = INVALID_TRANSACTION;
            }
        }
        // Check if any versions remain.
        for v in versions.iter() {
            if *v != INVALID_TRANSACTION {
                any_remaining = true;
                break;
            }
        }
        if !any_remaining {
            self.insertion_status = InsertionStatus::NoInserted;
        }
    }

    /// Rollback deletions: reset rows back to INVALID_TRANSACTION.
    pub fn rollback_deletions(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        if let Some(ref mut versions) = self.deleted_versions {
            let end = (start_row + num_rows) as usize;
            for i in start_row as usize..end {
                if versions[i] == transaction_id {
                    versions[i] = INVALID_TRANSACTION;
                }
            }
            let any_remaining = versions.iter().any(|v| *v != INVALID_TRANSACTION);
            if !any_remaining {
                self.deletion_status = DeletionStatus::NoDeleted;
            }
        }
    }

    /// Set insertion status to AlwaysInserted (after checkpoint).
    pub fn set_always_inserted(&mut self) {
        self.insertion_status = InsertionStatus::AlwaysInserted;
        self.inserted_versions = None;
        self.same_insertion_version = INVALID_TRANSACTION;
    }

    /// Check if using the same-version scalar (no array allocated).
    pub fn is_same_insertion_version(&self) -> bool {
        self.inserted_versions.is_none() && self.same_insertion_version != INVALID_TRANSACTION
    }

    /// Check if using the same-version scalar for deletions.
    pub fn is_same_deletion_version(&self) -> bool {
        self.deleted_versions.is_none() && self.same_deletion_version != INVALID_TRANSACTION
    }

    /// Whether any row has version info that needs checking.
    pub fn has_any_version_info(&self) -> bool {
        self.insertion_status != InsertionStatus::AlwaysInserted
            || self.deletion_status != DeletionStatus::NoDeleted
    }
}

/// Write-write conflict error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConflict {
    pub row: u64,
}

impl std::fmt::Display for WriteConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write-write conflict at row {}", self.row)
    }
}

impl std::error::Error for WriteConflict {}

/// Per-node-group version info, owning one `VectorVersionInfo` per vector.
///
/// `None` entries mean "always visible" (checkpointed data) — zero cost.
pub struct VersionInfo {
    vectors: Vec<Option<Box<VectorVersionInfo>>>,
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionInfo {
    pub fn new() -> Self {
        Self {
            vectors: Vec::new(),
        }
    }

    /// Ensure we have enough vector slots for the given row.
    fn ensure_vector(&mut self, row: u64) {
        let vec_idx = (row / VECTOR_CAPACITY as u64) as usize;
        if vec_idx >= self.vectors.len() {
            self.vectors.resize_with(vec_idx + 1, || None);
        }
        if self.vectors[vec_idx].is_none() {
            self.vectors[vec_idx] = Some(Box::new(VectorVersionInfo::new()));
        }
    }

    fn get_vector(&self, row: u64) -> Option<&VectorVersionInfo> {
        let vec_idx = (row / VECTOR_CAPACITY as u64) as usize;
        self.vectors.get(vec_idx).and_then(|v| v.as_deref())
    }

    fn get_vector_mut(&mut self, row: u64) -> Option<&mut VectorVersionInfo> {
        let vec_idx = (row / VECTOR_CAPACITY as u64) as usize;
        self.vectors.get_mut(vec_idx).and_then(|v| v.as_deref_mut())
    }

    /// Record insertion of rows [start_row..start_row+num_rows) by transaction_id.
    pub fn append(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            self.ensure_vector(vec_start);
            let vvi = self.get_vector_mut(vec_start).unwrap();
            vvi.append(transaction_id, local_start, local_num);
        }
    }

    /// Record deletion of rows [start_row..start_row+num_rows) by transaction_id.
    pub fn delete(
        &mut self,
        transaction_id: u64,
        start_row: u64,
        num_rows: u64,
    ) -> Result<(), WriteConflict> {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            self.ensure_vector(vec_start);
            let vvi = self.get_vector_mut(vec_start).unwrap();
            vvi.delete(transaction_id, local_start, local_num)?;
        }
        Ok(())
    }

    /// Check visibility of a single row.
    #[inline]
    pub fn is_selected(&self, start_ts: u64, txn_id: u64, row: u64) -> bool {
        match self.get_vector(row) {
            Some(vvi) => {
                let local_row = row % VECTOR_CAPACITY as u64;
                vvi.is_selected(start_ts, txn_id, local_row)
            }
            // No version info → always visible (checkpointed).
            None => true,
        }
    }

    /// Commit insertions for rows [start_row..+num_rows).
    pub fn commit_insert(
        &mut self,
        transaction_id: u64,
        commit_ts: u64,
        start_row: u64,
        num_rows: u64,
    ) {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            if let Some(vvi) = self.get_vector_mut(vec_start) {
                vvi.set_insert_commit_ts(transaction_id, commit_ts, local_start, local_num);
            }
        }
    }

    /// Commit deletions for rows [start_row..+num_rows).
    pub fn commit_delete(
        &mut self,
        transaction_id: u64,
        commit_ts: u64,
        start_row: u64,
        num_rows: u64,
    ) {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            if let Some(vvi) = self.get_vector_mut(vec_start) {
                vvi.set_delete_commit_ts(transaction_id, commit_ts, local_start, local_num);
            }
        }
    }

    /// Rollback insertions for rows [start_row..+num_rows).
    pub fn rollback_insert(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            if let Some(vvi) = self.get_vector_mut(vec_start) {
                vvi.rollback_insertions(transaction_id, local_start, local_num);
            }
        }
    }

    /// Rollback deletions for rows [start_row..+num_rows).
    pub fn rollback_delete(&mut self, transaction_id: u64, start_row: u64, num_rows: u64) {
        let start_vec = start_row / VECTOR_CAPACITY as u64;
        let end_row = start_row + num_rows;
        let end_vec = (end_row.saturating_sub(1)) / VECTOR_CAPACITY as u64;

        for vec_idx in start_vec..=end_vec {
            let vec_start = vec_idx * VECTOR_CAPACITY as u64;
            let local_start = start_row.saturating_sub(vec_start);
            let local_end = if end_row < vec_start + VECTOR_CAPACITY as u64 {
                end_row - vec_start
            } else {
                VECTOR_CAPACITY as u64
            };
            let local_num = local_end - local_start;

            if let Some(vvi) = self.get_vector_mut(vec_start) {
                vvi.rollback_deletions(transaction_id, local_start, local_num);
            }
        }
    }

    pub fn num_vectors(&self) -> usize {
        self.vectors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TXN_A: u64 = START_TRANSACTION_ID + 1;
    const TXN_B: u64 = START_TRANSACTION_ID + 2;
    const TS_10: u64 = 10;
    const TS_20: u64 = 20;

    // --- VectorVersionInfo tests ---

    #[test]
    fn new_vector_version_info() {
        let vvi = VectorVersionInfo::new();
        assert_eq!(vvi.insertion_status(), InsertionStatus::NoInserted);
        assert_eq!(vvi.deletion_status(), DeletionStatus::NoDeleted);
    }

    #[test]
    fn fast_path_no_inserted() {
        let vvi = VectorVersionInfo::new();
        assert!(!vvi.is_selected(TS_10, TXN_A, 0));
    }

    #[test]
    fn fast_path_always_inserted_no_deleted() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        assert!(vvi.is_selected(TS_10, TXN_A, 0));
        assert!(vvi.is_selected(TS_10, TXN_A, 100));
    }

    #[test]
    fn append_single_txn_same_version_optimization() {
        let mut vvi = VectorVersionInfo::new();
        vvi.append(TXN_A, 0, 10);

        assert!(vvi.is_same_insertion_version());
        // Own transaction can see the rows.
        assert!(vvi.is_selected(0, TXN_A, 0));
        assert!(vvi.is_selected(0, TXN_A, 9));
        // Other transaction cannot.
        assert!(!vvi.is_selected(0, TXN_B, 0));
    }

    #[test]
    fn append_same_txn_twice_no_array() {
        let mut vvi = VectorVersionInfo::new();
        vvi.append(TXN_A, 0, 5);
        vvi.append(TXN_A, 5, 5);
        // Still using same-version scalar.
        assert!(vvi.is_same_insertion_version());
    }

    #[test]
    fn append_two_txns_promotes_to_array() {
        let mut vvi = VectorVersionInfo::new();
        vvi.append(TXN_A, 0, 5);
        vvi.append(TXN_B, 5, 5);

        assert!(!vvi.is_same_insertion_version());
        // TXN_A sees its rows.
        assert!(vvi.is_selected(0, TXN_A, 0));
        assert!(!vvi.is_selected(0, TXN_A, 5));
        // TXN_B sees its rows.
        assert!(vvi.is_selected(0, TXN_B, 5));
        assert!(!vvi.is_selected(0, TXN_B, 0));
    }

    #[test]
    fn committed_insert_visible_by_timestamp() {
        let mut vvi = VectorVersionInfo::new();
        vvi.append(TXN_A, 0, 10);
        vvi.set_insert_commit_ts(TXN_A, TS_10, 0, 10);

        // Snapshot at ts=10 should see committed rows.
        assert!(vvi.is_selected(TS_10, TXN_B, 0));
        // Snapshot at ts=9 should NOT see them.
        assert!(!vvi.is_selected(9, TXN_B, 0));
    }

    #[test]
    fn delete_basic() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();

        // TXN_A sees the deletion.
        assert!(!vvi.is_selected(0, TXN_A, 0));
        // Other transactions still see the row.
        assert!(vvi.is_selected(0, TXN_B, 0));
        // Rows outside deletion range still visible.
        assert!(vvi.is_selected(0, TXN_A, 5));
    }

    #[test]
    fn delete_commit_ts() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();
        vvi.set_delete_commit_ts(TXN_A, TS_10, 0, 5);

        // After commit at ts=10, snapshot at ts=10 sees deletion.
        assert!(!vvi.is_selected(TS_10, TXN_B, 0));
        // Snapshot before ts=10 doesn't.
        assert!(vvi.is_selected(9, TXN_B, 0));
    }

    #[test]
    fn write_write_conflict_detection() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();

        // TXN_B tries to delete overlapping rows → conflict.
        let result = vvi.delete(TXN_B, 3, 2);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.row, 3);
    }

    #[test]
    fn same_txn_can_redelete() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();
        // Same txn can delete again.
        assert!(vvi.delete(TXN_A, 0, 5).is_ok());
    }

    #[test]
    fn rollback_insertions() {
        let mut vvi = VectorVersionInfo::new();
        vvi.append(TXN_A, 0, 10);
        assert!(vvi.is_selected(0, TXN_A, 0));

        vvi.rollback_insertions(TXN_A, 0, 10);
        assert_eq!(vvi.insertion_status(), InsertionStatus::NoInserted);
        assert!(!vvi.is_selected(0, TXN_A, 0));
    }

    #[test]
    fn rollback_deletions() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();
        assert!(!vvi.is_selected(0, TXN_A, 0));

        vvi.rollback_deletions(TXN_A, 0, 5);
        assert_eq!(vvi.deletion_status(), DeletionStatus::NoDeleted);
        assert!(vvi.is_selected(0, TXN_A, 0));
    }

    #[test]
    fn get_selected_rows_all_visible() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        let selected = vvi.get_selected_rows(TS_10, TXN_A, 0, 5, 100);
        assert_eq!(selected, vec![100, 101, 102, 103, 104]);
    }

    #[test]
    fn get_selected_rows_none_visible() {
        let vvi = VectorVersionInfo::new();
        let selected = vvi.get_selected_rows(TS_10, TXN_A, 0, 5, 0);
        assert!(selected.is_empty());
    }

    #[test]
    fn get_selected_rows_partial() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 1, 2).unwrap();
        vvi.set_delete_commit_ts(TXN_A, TS_10, 1, 2);

        let selected = vvi.get_selected_rows(TS_20, TXN_B, 0, 5, 0);
        // Rows 1,2 deleted at ts=10, visible at ts=20 means deleted.
        assert_eq!(selected, vec![0, 3, 4]);
    }

    #[test]
    fn has_any_version_info() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        assert!(!vvi.has_any_version_info());

        vvi.delete(TXN_A, 0, 1).unwrap();
        assert!(vvi.has_any_version_info());
    }

    // --- VersionInfo (per-node-group) tests ---

    #[test]
    fn version_info_empty() {
        let vi = VersionInfo::new();
        // No version info → always visible.
        assert!(vi.is_selected(TS_10, TXN_A, 0));
        assert!(vi.is_selected(TS_10, TXN_A, 5000));
    }

    #[test]
    fn version_info_append_and_select() {
        let mut vi = VersionInfo::new();
        vi.append(TXN_A, 0, 100);

        assert!(vi.is_selected(0, TXN_A, 0));
        assert!(vi.is_selected(0, TXN_A, 99));
        assert!(!vi.is_selected(0, TXN_B, 0));
    }

    #[test]
    fn version_info_cross_vector_append() {
        let mut vi = VersionInfo::new();
        // Append across vector boundary: rows 2040..2060.
        vi.append(TXN_A, 2040, 20);

        assert!(vi.is_selected(0, TXN_A, 2040));
        assert!(vi.is_selected(0, TXN_A, 2059));
        assert!(!vi.is_selected(0, TXN_B, 2050));
        assert_eq!(vi.num_vectors(), 2);
    }

    #[test]
    fn version_info_delete_and_conflict() {
        let mut vi = VersionInfo::new();
        vi.append(TXN_A, 0, 10);
        vi.commit_insert(TXN_A, TS_10, 0, 10);

        // Delete with TXN_B.
        vi.delete(TXN_B, 0, 5).unwrap();
        assert!(!vi.is_selected(TS_20, TXN_B, 0));
        assert!(vi.is_selected(TS_20, TXN_B, 5));
    }

    #[test]
    fn version_info_commit_and_rollback() {
        let mut vi = VersionInfo::new();
        vi.append(TXN_A, 0, 10);

        // Commit insert.
        vi.commit_insert(TXN_A, TS_10, 0, 10);
        assert!(vi.is_selected(TS_10, TXN_B, 0));

        // Delete and rollback.
        vi.delete(TXN_B, 0, 5).unwrap();
        assert!(!vi.is_selected(TS_20, TXN_B, 0));

        vi.rollback_delete(TXN_B, 0, 5);
        assert!(vi.is_selected(TS_20, TXN_B, 0));
    }

    #[test]
    fn version_info_rollback_insert() {
        let mut vi = VersionInfo::new();
        vi.append(TXN_A, 0, 10);
        assert!(vi.is_selected(0, TXN_A, 0));

        vi.rollback_insert(TXN_A, 0, 10);
        assert!(!vi.is_selected(0, TXN_A, 0));
    }

    #[test]
    fn version_info_multiple_vectors() {
        let mut vi = VersionInfo::new();
        // Insert in vector 0.
        vi.append(TXN_A, 0, 100);
        // Insert in vector 2 (rows 4096..4196).
        vi.append(TXN_A, 4096, 100);

        assert!(vi.is_selected(0, TXN_A, 50));
        assert!(vi.is_selected(0, TXN_A, 4100));
        // Vector 1 has no version info → visible.
        assert!(vi.is_selected(0, TXN_A, 2048));
        assert_eq!(vi.num_vectors(), 3);
    }

    #[test]
    fn version_info_write_conflict_propagates() {
        let mut vi = VersionInfo::new();
        vi.append(TXN_A, 0, 10);
        vi.commit_insert(TXN_A, TS_10, 0, 10);

        vi.delete(TXN_A, 0, 5).unwrap();
        let result = vi.delete(TXN_B, 3, 2);
        assert!(result.is_err());
    }

    #[test]
    fn delete_always_uses_array() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();
        // Deletions always use per-row array for correctness.
        assert!(!vvi.is_same_deletion_version());
    }

    #[test]
    fn delete_two_txns_no_conflict_after_commit() {
        let mut vvi = VectorVersionInfo::new();
        vvi.set_always_inserted();
        vvi.delete(TXN_A, 0, 5).unwrap();
        // Commit TXN_A's deletion so TXN_B doesn't conflict.
        vvi.set_delete_commit_ts(TXN_A, TS_10, 0, 5);

        // TXN_B deletes different rows — no conflict.
        vvi.delete(TXN_B, 5, 5).unwrap();
        // Both deletions in the array.
        assert!(!vvi.is_same_deletion_version());
    }

    #[test]
    fn write_conflict_display() {
        let err = WriteConflict { row: 42 };
        assert_eq!(err.to_string(), "write-write conflict at row 42");
    }

    #[test]
    fn committed_then_deleted_visibility() {
        let mut vvi = VectorVersionInfo::new();
        // Insert rows 0..10 by TXN_A, commit at ts=10.
        vvi.append(TXN_A, 0, 10);
        vvi.set_insert_commit_ts(TXN_A, TS_10, 0, 10);

        // Delete rows 0..5 by TXN_B, commit at ts=20.
        vvi.delete(TXN_B, 0, 5).unwrap();
        vvi.set_delete_commit_ts(TXN_B, TS_20, 0, 5);

        // Snapshot at ts=15: sees insert, doesn't see delete.
        assert!(vvi.is_selected(15, START_TRANSACTION_ID + 99, 0));
        // Snapshot at ts=25: sees both insert and delete → row invisible.
        assert!(!vvi.is_selected(25, START_TRANSACTION_ID + 99, 0));
        // Row 5 was never deleted → always visible after ts=10.
        assert!(vvi.is_selected(25, START_TRANSACTION_ID + 99, 5));
    }
}
