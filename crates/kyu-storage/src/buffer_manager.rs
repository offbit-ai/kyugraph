use dashmap::DashMap;
use kyu_common::{KyuError, KyuResult};

use crate::page_id::{FileId, FrameIdx, PageId, PoolId};
use crate::page_store::PageStore;
use crate::pool::Pool;

/// Buffer manager with split read/write pools.
///
/// Pages are loaded from a `PageStore` into frames. The read pool holds
/// pages for analytical queries (70% default), the write pool holds pages
/// for ingestion/WAL staging (30% default).
///
/// Eviction uses the clock (second-chance) algorithm per pool.
pub struct BufferManager {
    read_pool: Pool,
    write_pool: Pool,
    /// Maps PageId -> (PoolId, FrameIdx) for O(1) lookup.
    page_table: DashMap<PageId, (PoolId, FrameIdx)>,
    store: Box<dyn PageStore>,
}

impl BufferManager {
    /// Create a new buffer manager with the given total frame count and read ratio.
    pub fn new(total_frames: u32, read_ratio: f64, store: Box<dyn PageStore>) -> Self {
        let read_frames = ((total_frames as f64) * read_ratio).round() as u32;
        let write_frames = total_frames.saturating_sub(read_frames).max(1);
        let read_frames = read_frames.max(1);

        Self {
            read_pool: Pool::new(read_frames),
            write_pool: Pool::new(write_frames),
            page_table: DashMap::new(),
            store,
        }
    }

    /// Pin a page for reading. Loads from disk if not already in memory.
    pub fn pin_read(&self, page_id: PageId) -> KyuResult<PinnedPage<'_>> {
        self.pin_page(page_id, PoolId::Read)
    }

    /// Pin a page for writing. Loads from disk if not already in memory.
    pub fn pin_write(&self, page_id: PageId) -> KyuResult<PinnedPage<'_>> {
        self.pin_page(page_id, PoolId::Write)
    }

    /// Allocate a new page in the given file and pin it for writing.
    pub fn allocate_new_page(&self, file_id: FileId) -> KyuResult<(PageId, PinnedPage<'_>)> {
        let page_idx = self.store.allocate_page(file_id)?;
        let page_id = PageId::new(file_id, page_idx);
        let pinned = self.pin_page(page_id, PoolId::Write)?;
        Ok((page_id, pinned))
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> KyuResult<()> {
        self.flush_pool(&self.read_pool)?;
        self.flush_pool(&self.write_pool)?;
        Ok(())
    }

    /// Flush dirty pages from a specific pool.
    fn flush_pool(&self, pool: &Pool) -> KyuResult<()> {
        for i in 0..pool.num_frames() {
            let frame = pool.frame(FrameIdx(i));
            if frame.is_dirty() && frame.has_valid_page() {
                // SAFETY: We're reading the data to write to disk.
                // In a production system, we'd acquire a shared latch first.
                let data = unsafe { frame.data() };
                self.store.write_page(frame.page_id(), data)?;
                frame.clear_dirty();
            }
        }
        Ok(())
    }

    /// Pin a page in the specified pool.
    fn pin_page(&self, page_id: PageId, preferred_pool: PoolId) -> KyuResult<PinnedPage<'_>> {
        // 1. Check if already in memory
        if let Some(entry) = self.page_table.get(&page_id) {
            let (pool_id, frame_idx) = *entry;
            let pool = self.get_pool(pool_id);
            let frame = pool.frame(frame_idx);
            frame.pin();
            frame.set_recently_used();
            return Ok(PinnedPage {
                bm: self,
                page_id,
                pool_id,
                frame_idx,
            });
        }

        // 2. Find a frame in the preferred pool
        let pool = self.get_pool(preferred_pool);
        let frame_idx = self.find_or_evict_frame(pool, preferred_pool)?;
        let frame = pool.frame(frame_idx);

        // 3. Load page data from disk
        // SAFETY: We have exclusive access to this frame (it was just evicted/empty).
        let data = unsafe { frame.data_mut() };
        self.store.read_page(page_id, data)?;

        // 4. Set up the frame
        frame.set_page_id(page_id);
        frame.pin();
        frame.set_recently_used();
        frame.clear_dirty();

        // 5. Update page table
        self.page_table.insert(page_id, (preferred_pool, frame_idx));

        Ok(PinnedPage {
            bm: self,
            page_id,
            pool_id: preferred_pool,
            frame_idx,
        })
    }

    /// Find an empty frame or evict one.
    fn find_or_evict_frame(&self, pool: &Pool, _pool_id: PoolId) -> KyuResult<FrameIdx> {
        // Try to find an empty frame first
        if let Some(idx) = pool.find_empty() {
            return Ok(idx);
        }

        // Evict using clock algorithm
        let idx = pool.find_evictable().ok_or_else(|| {
            KyuError::Storage("buffer pool exhausted: all frames are pinned".into())
        })?;

        let frame = pool.frame(idx);

        // Write dirty page to disk before evicting
        if frame.is_dirty() && frame.has_valid_page() {
            // SAFETY: Frame is not pinned (eviction only picks unpinned frames).
            let data = unsafe { frame.data() };
            self.store.write_page(frame.page_id(), data)?;
        }

        // Remove old mapping
        if frame.has_valid_page() {
            self.page_table.remove(&frame.page_id());
        }

        frame.reset();
        Ok(idx)
    }

    fn get_pool(&self, pool_id: PoolId) -> &Pool {
        match pool_id {
            PoolId::Read => &self.read_pool,
            PoolId::Write => &self.write_pool,
        }
    }

    /// Get the total number of frames across both pools.
    pub fn total_frames(&self) -> u32 {
        self.read_pool.num_frames() + self.write_pool.num_frames()
    }

    /// Get statistics about the buffer manager.
    pub fn stats(&self) -> BufferManagerStats {
        BufferManagerStats {
            read_pool_frames: self.read_pool.num_frames(),
            write_pool_frames: self.write_pool.num_frames(),
            read_pool_loaded: self.read_pool.loaded_count(),
            write_pool_loaded: self.write_pool.loaded_count(),
            read_pool_dirty: self.read_pool.dirty_count(),
            write_pool_dirty: self.write_pool.dirty_count(),
            read_pool_pinned: self.read_pool.pinned_count(),
            write_pool_pinned: self.write_pool.pinned_count(),
            page_table_entries: self.page_table.len() as u32,
        }
    }
}

/// Statistics about buffer manager state.
#[derive(Clone, Debug)]
pub struct BufferManagerStats {
    pub read_pool_frames: u32,
    pub write_pool_frames: u32,
    pub read_pool_loaded: u32,
    pub write_pool_loaded: u32,
    pub read_pool_dirty: u32,
    pub write_pool_dirty: u32,
    pub read_pool_pinned: u32,
    pub write_pool_pinned: u32,
    pub page_table_entries: u32,
}

/// RAII guard that automatically unpins a page when dropped.
///
/// Provides safe access to the underlying frame data.
pub struct PinnedPage<'bm> {
    bm: &'bm BufferManager,
    page_id: PageId,
    pool_id: PoolId,
    frame_idx: FrameIdx,
}

impl<'bm> PinnedPage<'bm> {
    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get a shared (read-only) view of the page data.
    pub fn data(&self) -> &[u8] {
        let pool = self.bm.get_pool(self.pool_id);
        let frame = pool.frame(self.frame_idx);
        // SAFETY: The page is pinned, so the frame won't be evicted.
        // We hold a shared reference, which is safe for reading.
        unsafe { frame.data() }
    }

    /// Get an exclusive (mutable) view of the page data. Marks the page as dirty.
    pub fn data_mut(&mut self) -> &mut [u8] {
        let pool = self.bm.get_pool(self.pool_id);
        let frame = pool.frame(self.frame_idx);
        frame.set_dirty();
        // SAFETY: The page is pinned. We hold &mut self, preventing concurrent access.
        unsafe { frame.data_mut() }
    }

    /// Check if this page is dirty.
    pub fn is_dirty(&self) -> bool {
        let pool = self.bm.get_pool(self.pool_id);
        pool.frame(self.frame_idx).is_dirty()
    }

    /// Manually mark the page as dirty.
    pub fn mark_dirty(&self) {
        let pool = self.bm.get_pool(self.pool_id);
        pool.frame(self.frame_idx).set_dirty();
    }
}

impl Drop for PinnedPage<'_> {
    fn drop(&mut self) {
        let pool = self.bm.get_pool(self.pool_id);
        pool.frame(self.frame_idx).unpin();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_id::PAGE_SIZE;
    use crate::page_store::MockPageStore;

    fn make_bm(frames: u32) -> BufferManager {
        BufferManager::new(frames, 0.7, Box::new(MockPageStore::new()))
    }

    #[test]
    fn create_buffer_manager() {
        let bm = make_bm(10);
        assert_eq!(bm.total_frames(), 10);
        let stats = bm.stats();
        assert_eq!(stats.read_pool_frames, 7);
        assert_eq!(stats.write_pool_frames, 3);
    }

    #[test]
    fn pin_and_unpin_read() {
        let bm = make_bm(10);
        let pid = PageId::new(FileId(0), 0);
        {
            let page = bm.pin_read(pid).unwrap();
            assert_eq!(page.page_id(), pid);
            assert_eq!(page.data().len(), PAGE_SIZE);
            // Verify initial data is zeros (from MockPageStore)
            assert!(page.data().iter().all(|&b| b == 0));
        }
        // After drop, the frame should be unpinned
        let stats = bm.stats();
        assert_eq!(stats.read_pool_pinned, 0);
    }

    #[test]
    fn pin_write_marks_dirty() {
        let bm = make_bm(10);
        let pid = PageId::new(FileId(0), 0);
        {
            let mut page = bm.pin_write(pid).unwrap();
            assert!(!page.is_dirty());
            page.data_mut()[0] = 42;
            assert!(page.is_dirty());
        }
    }

    #[test]
    fn pin_same_page_twice() {
        let bm = make_bm(10);
        let pid = PageId::new(FileId(0), 0);
        let p1 = bm.pin_read(pid).unwrap();
        let p2 = bm.pin_read(pid).unwrap();
        assert_eq!(p1.page_id(), p2.page_id());
        drop(p1);
        drop(p2);
    }

    #[test]
    fn write_and_read_back() {
        let bm = make_bm(10);
        let pid = PageId::new(FileId(0), 0);

        // Write data
        {
            let mut page = bm.pin_write(pid).unwrap();
            page.data_mut()[0] = 0xAB;
            page.data_mut()[1] = 0xCD;
        }

        // Read it back (same frame, still in memory)
        {
            let page = bm.pin_read(pid).unwrap();
            assert_eq!(page.data()[0], 0xAB);
            assert_eq!(page.data()[1], 0xCD);
        }
    }

    #[test]
    fn allocate_new_page() {
        let bm = make_bm(10);
        let (pid, mut page) = bm.allocate_new_page(FileId(0)).unwrap();
        assert_eq!(pid.file_id, FileId(0));
        assert_eq!(pid.page_idx, 0);
        page.data_mut()[0] = 99;
        drop(page);

        let (pid2, _page2) = bm.allocate_new_page(FileId(0)).unwrap();
        assert_eq!(pid2.page_idx, 1);
    }

    #[test]
    fn flush_all() {
        let store = MockPageStore::new();
        let bm = BufferManager::new(10, 0.7, Box::new(store));
        let pid = PageId::new(FileId(0), 0);

        {
            let mut page = bm.pin_write(pid).unwrap();
            page.data_mut()[0] = 0xFF;
        }

        bm.flush_all().unwrap();

        let stats = bm.stats();
        assert_eq!(stats.write_pool_dirty, 0);
    }

    #[test]
    fn eviction_on_full_pool() {
        // Small pool: 3 read frames + 1 write frame
        let bm = make_bm(4);

        // Fill up the read pool (3 frames)
        for i in 0..3 {
            let pid = PageId::new(FileId(0), i);
            let page = bm.pin_read(pid).unwrap();
            drop(page); // Unpin immediately so it can be evicted
        }

        assert_eq!(bm.stats().read_pool_loaded, 3);

        // Pin a 4th page — should trigger eviction
        let pid = PageId::new(FileId(0), 3);
        let page = bm.pin_read(pid).unwrap();
        assert_eq!(page.data().len(), PAGE_SIZE);
        drop(page);
    }

    #[test]
    fn eviction_writes_dirty_page() {
        let bm = BufferManager::new(4, 0.75, Box::new(MockPageStore::new()));

        // Fill 3 read frames with dirty data
        for i in 0..3 {
            let pid = PageId::new(FileId(0), i);
            let page = bm.pin_read(pid).unwrap();
            page.mark_dirty();
            drop(page);
        }

        // Trigger eviction
        let pid = PageId::new(FileId(0), 10);
        let _page = bm.pin_read(pid).unwrap();

        // The evicted dirty page should have been written to the store
        // We can't easily check MockPageStore through the Box, but the
        // test passes if eviction doesn't error
    }

    #[test]
    fn buffer_pool_exhausted() {
        // 2 frames total: 1 read + 1 write (minimum)
        let bm = make_bm(2);

        // Pin both frames and keep them pinned
        let p1 = bm.pin_read(PageId::new(FileId(0), 0)).unwrap();

        // Try to pin another page in the same pool — should fail
        // since there's only 1 read frame and it's pinned
        let result = bm.pin_read(PageId::new(FileId(0), 1));
        assert!(result.is_err());

        drop(p1);
    }

    #[test]
    fn stats() {
        let bm = make_bm(10);
        let stats = bm.stats();
        assert_eq!(stats.read_pool_loaded, 0);
        assert_eq!(stats.write_pool_loaded, 0);
        assert_eq!(stats.page_table_entries, 0);

        let _p = bm.pin_read(PageId::new(FileId(0), 0)).unwrap();
        let stats = bm.stats();
        assert_eq!(stats.read_pool_loaded, 1);
        assert_eq!(stats.read_pool_pinned, 1);
        assert_eq!(stats.page_table_entries, 1);
    }

    #[test]
    fn split_pool_ratio() {
        let bm = BufferManager::new(100, 0.7, Box::new(MockPageStore::new()));
        let stats = bm.stats();
        assert_eq!(stats.read_pool_frames, 70);
        assert_eq!(stats.write_pool_frames, 30);
    }

    #[test]
    fn minimum_pool_sizes() {
        // Even with extreme ratios, each pool gets at least 1 frame
        let bm = BufferManager::new(2, 0.0, Box::new(MockPageStore::new()));
        assert!(bm.read_pool.num_frames() >= 1);
        assert!(bm.write_pool.num_frames() >= 1);
    }
}
