//! Write-through disk cache between a buffer manager and a remote page store.
//!
//! Reads check the local NVMe cache first, falling back to the remote store
//! on a miss. Writes go to both local and remote (write-through) so that the
//! remote store is always up-to-date.
//!
//! Typical deployment: `BufferManager → DiskCache(LocalPageStore, RemotePageStore)`.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use kyu_common::KyuResult;

use crate::local_page_store::LocalPageStore;
use crate::page_id::{FileId, PageId, PAGE_SIZE};
use crate::page_store::PageStore;

/// Write-through disk cache wrapping a remote `PageStore`.
///
/// The local cache is a `LocalPageStore` on fast storage (NVMe).
/// Pages are cached on first read and updated on every write.
pub struct DiskCache {
    local: LocalPageStore,
    remote: Box<dyn PageStore>,
    /// Set of (file_id, page_idx) pairs known to be in the local cache.
    cached_pages: Mutex<HashSet<(u32, u32)>>,
}

impl DiskCache {
    /// Create a new disk cache.
    ///
    /// - `cache_dir`: local directory for cached pages (should be on fast storage)
    /// - `remote`: the underlying remote page store (e.g. S3)
    pub fn new(cache_dir: impl AsRef<Path>, remote: Box<dyn PageStore>) -> KyuResult<Self> {
        let local = LocalPageStore::new(cache_dir)?;
        Ok(Self {
            local,
            remote,
            cached_pages: Mutex::new(HashSet::new()),
        })
    }

    /// Number of pages currently in the local cache.
    pub fn cached_count(&self) -> usize {
        self.cached_pages.lock().unwrap().len()
    }

    /// Evict all cached pages (for testing or memory pressure).
    pub fn evict_all(&self) {
        self.cached_pages.lock().unwrap().clear();
    }
}

impl PageStore for DiskCache {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let key = (page_id.file_id.0, page_id.page_idx);

        // Check local cache first.
        if self.cached_pages.lock().unwrap().contains(&key) {
            return self.local.read_page(page_id, buf);
        }

        // Cache miss: fetch from remote.
        self.remote.read_page(page_id, buf)?;

        // Cache locally if the page has data (skip all-zero pages to save space).
        if buf.iter().any(|&b| b != 0) {
            let _ = self.local.write_page(page_id, buf);
            self.cached_pages.lock().unwrap().insert(key);
        }

        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);

        // Write-through: local first (fast), then remote (durable).
        self.local.write_page(page_id, buf)?;
        self.remote.write_page(page_id, buf)?;

        let key = (page_id.file_id.0, page_id.page_idx);
        self.cached_pages.lock().unwrap().insert(key);
        Ok(())
    }

    fn allocate_page(&self, file_id: FileId) -> KyuResult<u32> {
        // Remote is the source of truth for page allocation.
        self.remote.allocate_page(file_id)
    }

    fn sync_file(&self, file_id: FileId) -> KyuResult<()> {
        // Sync both layers.
        self.local.sync_file(file_id)?;
        self.remote.sync_file(file_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::MockPageStore;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> (std::path::PathBuf, impl Drop) {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kyu_diskcache_test_{}_{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        struct Guard(std::path::PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        let guard = Guard(dir.clone());
        (dir, guard)
    }

    #[test]
    fn cache_miss_fetches_from_remote() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();

        // Pre-populate remote with a page.
        let pid = PageId::new(FileId(0), 0);
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 0xAB;
        remote.write_page(pid, &data).unwrap();

        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();
        assert_eq!(cache.cached_count(), 0);

        // Read should fetch from remote and cache locally.
        let mut buf = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf).unwrap();
        assert_eq!(buf[0], 0xAB);
        assert_eq!(cache.cached_count(), 1);
    }

    #[test]
    fn cache_hit_reads_locally() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();

        let pid = PageId::new(FileId(0), 0);
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 0xCD;
        remote.write_page(pid, &data).unwrap();

        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();

        // First read: cache miss.
        let mut buf = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf).unwrap();
        assert_eq!(buf[0], 0xCD);
        assert_eq!(cache.cached_count(), 1);

        // Second read: cache hit (reads from local).
        let mut buf2 = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf2).unwrap();
        assert_eq!(buf2[0], 0xCD);
    }

    #[test]
    fn write_through_updates_both() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();

        // Write a page that doesn't exist yet.
        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();
        let pid = PageId::new(FileId(0), 0);
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 0xEF;
        cache.write_page(pid, &data).unwrap();

        // Cache should now have it.
        assert_eq!(cache.cached_count(), 1);

        // Read should return the written data.
        let mut buf = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf).unwrap();
        assert_eq!(buf[0], 0xEF);
    }

    #[test]
    fn allocate_delegates_to_remote() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();
        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();

        let p0 = cache.allocate_page(FileId(0)).unwrap();
        let p1 = cache.allocate_page(FileId(0)).unwrap();
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
    }

    #[test]
    fn evict_clears_cache_state() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();

        let pid = PageId::new(FileId(0), 0);
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 1;
        remote.write_page(pid, &data).unwrap();

        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf).unwrap();
        assert_eq!(cache.cached_count(), 1);

        cache.evict_all();
        assert_eq!(cache.cached_count(), 0);
    }

    #[test]
    fn zero_page_not_cached() {
        let (dir, _guard) = temp_dir();
        let remote = MockPageStore::new();
        let cache = DiskCache::new(&dir, Box::new(remote)).unwrap();

        // Read an unwritten page (all zeros) — should NOT be cached.
        let pid = PageId::new(FileId(0), 99);
        let mut buf = vec![0u8; PAGE_SIZE];
        cache.read_page(pid, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        assert_eq!(cache.cached_count(), 0);
    }
}
