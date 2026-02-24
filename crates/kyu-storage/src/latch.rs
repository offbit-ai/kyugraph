use std::sync::atomic::{AtomicU32, Ordering};

/// A lightweight reader-writer spinlock using a single `AtomicU32`.
///
/// Encoding:
/// - `0` = unlocked
/// - `u32::MAX` = write-locked
/// - `1..u32::MAX-1` = number of concurrent readers
///
/// # Safety
///
/// This uses `unsafe` only in the sense of lock correctness. The latch itself
/// contains no raw pointers — it is a synchronization primitive that
/// protects access to frame data via the buffer manager.
pub struct RwLatch {
    state: AtomicU32,
}

const UNLOCKED: u32 = 0;
const WRITE_LOCKED: u32 = u32::MAX;

impl RwLatch {
    pub const fn new() -> Self {
        Self {
            state: AtomicU32::new(UNLOCKED),
        }
    }

    /// Acquire shared (read) access. Spins until successful.
    pub fn lock_shared(&self) {
        loop {
            let current = self.state.load(Ordering::Relaxed);
            if current == WRITE_LOCKED {
                std::hint::spin_loop();
                continue;
            }
            // Try to increment reader count
            if self
                .state
                .compare_exchange_weak(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to acquire shared access. Returns true on success.
    pub fn try_lock_shared(&self) -> bool {
        let current = self.state.load(Ordering::Relaxed);
        if current == WRITE_LOCKED {
            return false;
        }
        self.state
            .compare_exchange_weak(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Release shared access.
    pub fn unlock_shared(&self) {
        let prev = self.state.fetch_sub(1, Ordering::Release);
        debug_assert!(prev != UNLOCKED && prev != WRITE_LOCKED);
    }

    /// Acquire exclusive (write) access. Spins until successful.
    pub fn lock_exclusive(&self) {
        loop {
            if self
                .state
                .compare_exchange_weak(UNLOCKED, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to acquire exclusive access. Returns true on success.
    pub fn try_lock_exclusive(&self) -> bool {
        self.state
            .compare_exchange(UNLOCKED, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Release exclusive access.
    pub fn unlock_exclusive(&self) {
        let prev = self.state.swap(UNLOCKED, Ordering::Release);
        debug_assert_eq!(prev, WRITE_LOCKED);
    }

    /// Check if the latch is currently write-locked.
    pub fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) == WRITE_LOCKED
    }

    /// Check if the latch is unlocked (no readers or writers).
    pub fn is_unlocked(&self) -> bool {
        self.state.load(Ordering::Relaxed) == UNLOCKED
    }

    /// Get the number of current readers (0 if unlocked or write-locked).
    pub fn reader_count(&self) -> u32 {
        let s = self.state.load(Ordering::Relaxed);
        if s == WRITE_LOCKED { 0 } else { s }
    }
}

impl Default for RwLatch {
    fn default() -> Self {
        Self::new()
    }
}

// RwLatch is safe to share between threads (it's a synchronization primitive).
unsafe impl Send for RwLatch {}
unsafe impl Sync for RwLatch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_unlocked() {
        let latch = RwLatch::new();
        assert!(latch.is_unlocked());
        assert!(!latch.is_write_locked());
    }

    #[test]
    fn exclusive_lock_unlock() {
        let latch = RwLatch::new();
        latch.lock_exclusive();
        assert!(latch.is_write_locked());
        assert!(!latch.is_unlocked());
        latch.unlock_exclusive();
        assert!(latch.is_unlocked());
    }

    #[test]
    fn shared_lock_unlock() {
        let latch = RwLatch::new();
        latch.lock_shared();
        assert!(!latch.is_unlocked());
        assert!(!latch.is_write_locked());
        assert_eq!(latch.reader_count(), 1);
        latch.unlock_shared();
        assert!(latch.is_unlocked());
    }

    #[test]
    fn multiple_readers() {
        let latch = RwLatch::new();
        latch.lock_shared();
        latch.lock_shared();
        latch.lock_shared();
        assert_eq!(latch.reader_count(), 3);
        latch.unlock_shared();
        assert_eq!(latch.reader_count(), 2);
        latch.unlock_shared();
        latch.unlock_shared();
        assert!(latch.is_unlocked());
    }

    #[test]
    fn try_lock_shared_succeeds() {
        let latch = RwLatch::new();
        assert!(latch.try_lock_shared());
        assert_eq!(latch.reader_count(), 1);
        latch.unlock_shared();
    }

    #[test]
    fn try_lock_shared_fails_when_write_locked() {
        let latch = RwLatch::new();
        latch.lock_exclusive();
        assert!(!latch.try_lock_shared());
        latch.unlock_exclusive();
    }

    #[test]
    fn try_lock_exclusive_succeeds() {
        let latch = RwLatch::new();
        assert!(latch.try_lock_exclusive());
        assert!(latch.is_write_locked());
        latch.unlock_exclusive();
    }

    #[test]
    fn try_lock_exclusive_fails_when_read_locked() {
        let latch = RwLatch::new();
        latch.lock_shared();
        assert!(!latch.try_lock_exclusive());
        latch.unlock_shared();
    }

    #[test]
    fn try_lock_exclusive_fails_when_write_locked() {
        let latch = RwLatch::new();
        latch.lock_exclusive();
        assert!(!latch.try_lock_exclusive());
        latch.unlock_exclusive();
    }

    #[test]
    fn concurrent_readers() {
        use std::sync::Arc;
        let latch = Arc::new(RwLatch::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let l = Arc::clone(&latch);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    l.lock_shared();
                    // Simulate reading
                    std::hint::black_box(42);
                    l.unlock_shared();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert!(latch.is_unlocked());
    }

    #[test]
    fn concurrent_writers() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let latch = Arc::new(RwLatch::new());
        let counter = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..4 {
            let l = Arc::clone(&latch);
            let c = Arc::clone(&counter);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    l.lock_exclusive();
                    c.fetch_add(1, Ordering::Relaxed);
                    l.unlock_exclusive();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 4000);
        assert!(latch.is_unlocked());
    }
}
