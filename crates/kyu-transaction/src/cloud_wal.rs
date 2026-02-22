//! Cloud WAL: local WAL + remote append for cross-AZ durability.
//!
//! `CloudWal` wraps a local `Wal` and pushes committed segments to a
//! remote `WalSink`. The local WAL provides low-latency writes; the sink
//! provides durability beyond the local machine (e.g. S3, Kinesis).
//!
//! Each committed transaction's WAL data is assigned a monotonic segment ID
//! and pushed to the sink after the local WAL flush succeeds.

use std::sync::Mutex;

use kyu_common::id::Lsn;

use crate::local_wal::LocalWal;
use crate::wal::Wal;

/// Trait for pushing WAL segments to a remote durable store.
///
/// Implementations might write to S3, Kinesis, Redpanda, or an in-memory
/// buffer (for testing).
pub trait WalSink: Send + Sync {
    /// Push a WAL segment to the remote store.
    ///
    /// - `segment_id`: monotonically increasing segment identifier
    /// - `data`: raw WAL bytes for this segment
    fn push_segment(&self, segment_id: u64, data: &[u8]) -> std::io::Result<()>;
}

/// In-memory WAL sink for testing.
pub struct MemorySink {
    segments: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            segments: Mutex::new(Vec::new()),
        }
    }

    /// Number of segments pushed.
    pub fn segment_count(&self) -> usize {
        self.segments.lock().unwrap().len()
    }

    /// Get all pushed segments as `(segment_id, data)` pairs.
    pub fn segments(&self) -> Vec<(u64, Vec<u8>)> {
        self.segments.lock().unwrap().clone()
    }

    /// Total bytes across all segments.
    pub fn total_bytes(&self) -> usize {
        self.segments
            .lock()
            .unwrap()
            .iter()
            .map(|(_, d)| d.len())
            .sum()
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl WalSink for MemorySink {
    fn push_segment(&self, segment_id: u64, data: &[u8]) -> std::io::Result<()> {
        self.segments
            .lock()
            .unwrap()
            .push((segment_id, data.to_vec()));
        Ok(())
    }
}

/// Cloud WAL combining local durability with remote replication.
///
/// Write path:
/// 1. Flush to local `Wal` (fast, fsync'd)
/// 2. Push segment to remote `WalSink` (durable across AZ)
///
/// If the remote push fails, the local WAL still has the data,
/// so the transaction is locally committed. The caller can retry
/// the remote push or rely on background catch-up.
pub struct CloudWal {
    local: Wal,
    sink: Box<dyn WalSink>,
    segment_counter: Mutex<u64>,
}

impl CloudWal {
    /// Create a new cloud WAL.
    ///
    /// - `local`: the underlying local WAL (file-backed or in-memory)
    /// - `sink`: remote WAL sink for cross-AZ durability
    pub fn new(local: Wal, sink: Box<dyn WalSink>) -> Self {
        Self {
            local,
            sink,
            segment_counter: Mutex::new(0),
        }
    }

    /// Flush a committed transaction's local WAL buffer, then push to remote.
    /// Returns the LSN assigned by the local WAL.
    pub fn log_committed_wal(&self, local_wal: &LocalWal) -> std::io::Result<Lsn> {
        // Step 1: flush to local WAL.
        let lsn = self.local.log_committed_wal(local_wal)?;

        if lsn > 0 && !local_wal.is_empty() {
            // Step 2: push segment to remote sink.
            let segment_id = {
                let mut counter = self.segment_counter.lock().unwrap();
                *counter += 1;
                *counter
            };
            // Best-effort remote push. Log locally regardless.
            if let Err(e) = self.sink.push_segment(segment_id, local_wal.data()) {
                // Log the error but don't fail the transaction — local WAL is durable.
                eprintln!("CloudWal: remote push failed for segment {segment_id}: {e}");
            }
        }

        Ok(lsn)
    }

    /// Log a checkpoint record to both local and remote.
    pub fn log_checkpoint(&self) -> std::io::Result<Lsn> {
        let lsn = self.local.log_checkpoint()?;

        if lsn > 0 {
            let segment_id = {
                let mut counter = self.segment_counter.lock().unwrap();
                *counter += 1;
                *counter
            };
            // Push checkpoint marker to remote.
            let marker = b"CHECKPOINT";
            let _ = self.sink.push_segment(segment_id, marker);
        }

        Ok(lsn)
    }

    /// Clear the local WAL.
    pub fn clear(&self) -> std::io::Result<()> {
        self.local.clear()
    }

    /// Reset local WAL state.
    pub fn reset(&self) -> std::io::Result<()> {
        self.local.reset()
    }

    /// Current LSN from the local WAL.
    pub fn current_lsn(&self) -> Lsn {
        self.local.current_lsn()
    }

    /// Current local WAL file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.local.file_size()
    }

    /// Number of segments pushed to the remote sink.
    pub fn remote_segment_count(&self) -> u64 {
        *self.segment_counter.lock().unwrap()
    }

    /// Access the underlying local WAL.
    pub fn local_wal(&self) -> &Wal {
        &self.local
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use kyu_common::id::TableId;

    #[test]
    fn memory_sink_captures_segments() {
        let sink = MemorySink::new();
        sink.push_segment(1, b"hello").unwrap();
        sink.push_segment(2, b"world").unwrap();
        assert_eq!(sink.segment_count(), 2);
        assert_eq!(sink.total_bytes(), 10);

        let segs = sink.segments();
        assert_eq!(segs[0], (1, b"hello".to_vec()));
        assert_eq!(segs[1], (2, b"world".to_vec()));
    }

    #[test]
    fn cloud_wal_log_committed() {
        let sink = Arc::new(MemorySink::new());
        let cloud = CloudWal::new(Wal::in_memory(), Box::new(ArcSink(Arc::clone(&sink))));

        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_table_insertion(TableId(1), 50);
        local.log_commit();

        let lsn = cloud.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(cloud.current_lsn(), 1);

        // Segment was pushed to sink.
        assert_eq!(sink.segment_count(), 1);
        assert!(sink.total_bytes() > 0);
    }

    #[test]
    fn cloud_wal_empty_local_no_push() {
        let sink = Arc::new(MemorySink::new());
        let cloud = CloudWal::new(Wal::in_memory(), Box::new(ArcSink(Arc::clone(&sink))));

        let local = LocalWal::new(); // empty
        let lsn = cloud.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 0);
        assert_eq!(sink.segment_count(), 0);
    }

    #[test]
    fn cloud_wal_checkpoint() {
        let sink = Arc::new(MemorySink::new());
        let cloud = CloudWal::new(Wal::in_memory(), Box::new(ArcSink(Arc::clone(&sink))));

        let lsn = cloud.log_checkpoint().unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(sink.segment_count(), 1);

        let segs = sink.segments();
        assert_eq!(segs[0].1, b"CHECKPOINT");
    }

    #[test]
    fn cloud_wal_multiple_commits() {
        let sink = Arc::new(MemorySink::new());
        let cloud = CloudWal::new(Wal::in_memory(), Box::new(ArcSink(Arc::clone(&sink))));

        for i in 0..3 {
            let mut local = LocalWal::new();
            local.log_begin_transaction();
            local.log_table_insertion(TableId(i), 10);
            local.log_commit();
            cloud.log_committed_wal(&local).unwrap();
        }

        assert_eq!(cloud.current_lsn(), 3);
        assert_eq!(cloud.remote_segment_count(), 3);
        assert_eq!(sink.segment_count(), 3);

        // Segment IDs are sequential.
        let segs = sink.segments();
        assert_eq!(segs[0].0, 1);
        assert_eq!(segs[1].0, 2);
        assert_eq!(segs[2].0, 3);
    }

    #[test]
    fn cloud_wal_clear_and_reset() {
        let sink = Arc::new(MemorySink::new());
        let cloud = CloudWal::new(Wal::in_memory(), Box::new(ArcSink(Arc::clone(&sink))));

        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_commit();
        cloud.log_committed_wal(&local).unwrap();

        assert!(cloud.file_size() > 0);
        cloud.clear().unwrap();
        assert_eq!(cloud.file_size(), 0);

        cloud.reset().unwrap();
        assert_eq!(cloud.current_lsn(), 0);
    }

    /// Wrapper to use `Arc<MemorySink>` as a `Box<dyn WalSink>`.
    struct ArcSink(Arc<MemorySink>);

    impl WalSink for ArcSink {
        fn push_segment(&self, segment_id: u64, data: &[u8]) -> std::io::Result<()> {
            self.0.push_segment(segment_id, data)
        }
    }
}
