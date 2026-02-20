use hashbrown::HashMap;
use smol_str::SmolStr;

/// Logical vector clock for causal ordering between workers.
///
/// Each entry maps a worker ID to its logical timestamp.
/// Used only when workers have causal dependencies. For most workloads,
/// this is `None` on `DeltaBatch` — timestamp alone handles
/// last-write-wins convergence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VectorClock {
    entries: HashMap<SmolStr, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the timestamp for a specific worker (0 if absent).
    pub fn get(&self, worker_id: &str) -> u64 {
        self.entries.get(worker_id).copied().unwrap_or(0)
    }

    /// Set the timestamp for a specific worker.
    pub fn set(&mut self, worker_id: impl Into<SmolStr>, ts: u64) {
        self.entries.insert(worker_id.into(), ts);
    }

    /// Increment the timestamp for a worker, returning the new value.
    pub fn tick(&mut self, worker_id: impl Into<SmolStr>) -> u64 {
        let key = worker_id.into();
        let entry = self.entries.entry(key).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Merge another clock (component-wise maximum).
    pub fn merge(&mut self, other: &VectorClock) {
        for (k, &v) in &other.entries {
            let entry = self.entries.entry(k.clone()).or_insert(0);
            *entry = (*entry).max(v);
        }
    }

    /// Returns true if every entry in `self` is <= the corresponding entry in `other`.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        self.entries.iter().all(|(k, &v)| v <= other.get(k))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let vc = VectorClock::new();
        assert!(vc.is_empty());
        assert_eq!(vc.len(), 0);
    }

    #[test]
    fn set_and_get() {
        let mut vc = VectorClock::new();
        vc.set("w1", 5);
        assert_eq!(vc.get("w1"), 5);
    }

    #[test]
    fn get_missing_returns_zero() {
        let vc = VectorClock::new();
        assert_eq!(vc.get("nonexistent"), 0);
    }

    #[test]
    fn tick_increments() {
        let mut vc = VectorClock::new();
        assert_eq!(vc.tick("w1"), 1);
        assert_eq!(vc.tick("w1"), 2);
        assert_eq!(vc.tick("w1"), 3);
        assert_eq!(vc.get("w1"), 3);
    }

    #[test]
    fn merge_takes_max() {
        let mut a = VectorClock::new();
        a.set("w1", 3);
        a.set("w2", 5);
        let mut b = VectorClock::new();
        b.set("w1", 7);
        b.set("w2", 2);
        a.merge(&b);
        assert_eq!(a.get("w1"), 7);
        assert_eq!(a.get("w2"), 5);
    }

    #[test]
    fn merge_adds_new_entries() {
        let mut a = VectorClock::new();
        a.set("w1", 3);
        let mut b = VectorClock::new();
        b.set("w2", 5);
        a.merge(&b);
        assert_eq!(a.get("w2"), 5);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn happens_before_true() {
        let mut a = VectorClock::new();
        a.set("w1", 1);
        a.set("w2", 2);
        let mut b = VectorClock::new();
        b.set("w1", 3);
        b.set("w2", 4);
        assert!(a.happens_before(&b));
    }

    #[test]
    fn happens_before_false_concurrent() {
        let mut a = VectorClock::new();
        a.set("w1", 3);
        a.set("w2", 1);
        let mut b = VectorClock::new();
        b.set("w1", 1);
        b.set("w2", 3);
        assert!(!a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }
}
