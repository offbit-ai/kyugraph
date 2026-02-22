//! Staging buffer for batched HNSW insertions.
//!
//! Accumulates vectors before bulk-inserting into the live HNSW index.
//! Search merges results from both the live index (O(log n) HNSW) and
//! the pending buffer (O(n) brute-force scan, fits L2 cache for n ≤ 10K).

use crate::distance::DistanceMetric;
use crate::hnsw::HnswIndex;

/// Staging buffer capacity before auto-flush.
const DEFAULT_CAPACITY: usize = 10_000;

/// Staging buffer with pending vectors awaiting HNSW insertion.
pub struct StagingBuffer {
    pending: Vec<(usize, Vec<f32>)>, // (external_id, vector)
    capacity: usize,
}

impl Default for StagingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl StagingBuffer {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            capacity: DEFAULT_CAPACITY,
        }
    }

    /// Add a vector to the staging buffer. Returns true if auto-flush is needed.
    pub fn add(&mut self, external_id: usize, vector: Vec<f32>) -> bool {
        self.pending.push((external_id, vector));
        self.pending.len() >= self.capacity
    }

    /// Number of pending vectors.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Search both the live HNSW index and the pending buffer, merging results.
    pub fn search_merged(
        &self,
        live: &HnswIndex,
        query: &[f32],
        k: usize,
        ef: usize,
        metric: DistanceMetric,
    ) -> Vec<(usize, f32)> {
        // HNSW search on live index.
        let mut results = if !live.is_empty() {
            live.search(query, k, ef)
        } else {
            Vec::new()
        };

        // Brute-force scan of pending buffer.
        for (ext_id, vec) in &self.pending {
            let dist = metric.distance(query, vec);
            results.push((*ext_id, dist));
        }

        // Sort by distance, take top-k.
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Flush all pending vectors into the live HNSW index.
    pub fn flush(&mut self, index: &mut HnswIndex) {
        for (_ext_id, vec) in self.pending.drain(..) {
            index.insert(&vec);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::HnswConfig;

    #[test]
    fn auto_flush_at_capacity() {
        let mut buf = StagingBuffer {
            pending: Vec::new(),
            capacity: 3,
        };
        assert!(!buf.add(0, vec![1.0]));
        assert!(!buf.add(1, vec![2.0]));
        assert!(buf.add(2, vec![3.0])); // triggers at capacity
        assert_eq!(buf.pending_count(), 3);
    }

    #[test]
    fn search_merged_includes_pending() {
        let live = HnswIndex::new(2, HnswConfig::default());
        let mut buf = StagingBuffer::new();

        buf.add(0, vec![1.0, 0.0]);
        buf.add(1, vec![0.0, 1.0]);

        let results = buf.search_merged(&live, &[0.9, 0.0], 2, 50, DistanceMetric::L2);
        assert_eq!(results.len(), 2);
        // Nearest should be vector [1.0, 0.0] (id=0).
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn flush_moves_to_live() {
        let mut index = HnswIndex::new(2, HnswConfig::default());
        let mut buf = StagingBuffer::new();

        buf.add(0, vec![1.0, 0.0]);
        buf.add(1, vec![0.0, 1.0]);
        assert_eq!(buf.pending_count(), 2);

        buf.flush(&mut index);
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(index.len(), 2);

        // Search on live index should find both vectors.
        let results = index.search(&[1.0, 0.0], 2, 50);
        assert_eq!(results.len(), 2);
    }
}
