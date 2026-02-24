use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::latch::RwLatch;
use crate::page_id::{PAGE_SIZE, PageId};

/// A buffer frame: a PAGE_SIZE block of memory with metadata for buffer management.
///
/// # Safety
///
/// The `data` pointer is allocated via `std::alloc` and must be freed via `Frame::drop`.
/// Access to the data must be coordinated through:
/// - `pin_count > 0` (frame is in use, cannot be evicted)
/// - `latch` (read/write access to the page data)
pub struct Frame {
    /// Raw pointer to PAGE_SIZE bytes of aligned memory.
    data: *mut u8,
    /// Which page is currently loaded in this frame (encoded as u64).
    page_id: AtomicU64,
    /// Number of threads currently using this frame. >0 prevents eviction.
    pin_count: AtomicU32,
    /// Whether the frame has been modified since loading.
    dirty: AtomicBool,
    /// Whether the frame was recently accessed (for clock eviction).
    recently_used: AtomicBool,
    /// Reader-writer latch protecting the page data.
    latch: RwLatch,
}

// Frame is safe to share across threads: all mutable state is atomic or latch-protected.
unsafe impl Send for Frame {}
unsafe impl Sync for Frame {}

impl Frame {
    /// Allocate a new frame with zeroed PAGE_SIZE memory.
    pub fn new() -> Self {
        let layout =
            std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).expect("invalid page layout");
        // SAFETY: Layout is valid (non-zero, power-of-two alignment).
        let data = unsafe { std::alloc::alloc_zeroed(layout) };
        if data.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self {
            data,
            page_id: AtomicU64::new(PageId::INVALID.to_u64()),
            pin_count: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
            recently_used: AtomicBool::new(false),
            latch: RwLatch::new(),
        }
    }

    /// Get a shared (read-only) reference to the page data.
    ///
    /// # Safety
    ///
    /// Caller must hold a shared latch or pin and ensure no writer exists.
    pub unsafe fn data(&self) -> &[u8] {
        // SAFETY: data was allocated with PAGE_SIZE bytes and is valid for the lifetime of Frame.
        unsafe { std::slice::from_raw_parts(self.data, PAGE_SIZE) }
    }

    /// Get an exclusive (mutable) reference to the page data.
    ///
    /// # Safety
    ///
    /// Caller must hold an exclusive latch. The `&self` signature is
    /// intentional: `Frame` uses interior mutability via a raw pointer,
    /// coordinated by pin_count and latch.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn data_mut(&self) -> &mut [u8] {
        // SAFETY: data was allocated with PAGE_SIZE bytes. Exclusive access is ensured by the caller.
        unsafe { std::slice::from_raw_parts_mut(self.data, PAGE_SIZE) }
    }

    /// Get the page ID currently loaded in this frame.
    pub fn page_id(&self) -> PageId {
        PageId::from_u64(self.page_id.load(Ordering::Acquire))
    }

    /// Set the page ID for this frame.
    pub fn set_page_id(&self, pid: PageId) {
        self.page_id.store(pid.to_u64(), Ordering::Release);
    }

    /// Get the current pin count.
    pub fn pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Increment the pin count.
    pub fn pin(&self) -> u32 {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrement the pin count.
    pub fn unpin(&self) -> u32 {
        let prev = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "unpin called on unpinned frame");
        prev - 1
    }

    /// Check if the frame is pinned.
    pub fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    /// Check if the frame is dirty.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Mark the frame as dirty.
    pub fn set_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Clear the dirty flag.
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    /// Check if the frame was recently used.
    pub fn is_recently_used(&self) -> bool {
        self.recently_used.load(Ordering::Relaxed)
    }

    /// Mark the frame as recently used.
    pub fn set_recently_used(&self) {
        self.recently_used.store(true, Ordering::Relaxed);
    }

    /// Clear the recently-used flag (second-chance eviction).
    pub fn clear_recently_used(&self) {
        self.recently_used.store(false, Ordering::Relaxed);
    }

    /// Get a reference to the latch.
    pub fn latch(&self) -> &RwLatch {
        &self.latch
    }

    /// Reset the frame to an empty state (for after eviction).
    pub fn reset(&self) {
        self.page_id
            .store(PageId::INVALID.to_u64(), Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        self.recently_used.store(false, Ordering::Relaxed);
        // pin_count should already be 0 when resetting
        debug_assert_eq!(self.pin_count(), 0);
    }

    /// Check if this frame holds a valid page.
    pub fn has_valid_page(&self) -> bool {
        self.page_id().is_valid()
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        let layout =
            std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).expect("invalid page layout");
        // SAFETY: data was allocated with this exact layout in Frame::new().
        unsafe {
            std::alloc::dealloc(self.data, layout);
        }
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_id::FileId;

    #[test]
    fn new_frame() {
        let frame = Frame::new();
        assert!(!frame.has_valid_page());
        assert_eq!(frame.pin_count(), 0);
        assert!(!frame.is_dirty());
        assert!(!frame.is_recently_used());
        assert!(!frame.is_pinned());
    }

    #[test]
    fn data_is_zeroed() {
        let frame = Frame::new();
        // SAFETY: No concurrent access in this test.
        let data = unsafe { frame.data() };
        assert!(data.iter().all(|&b| b == 0));
        assert_eq!(data.len(), PAGE_SIZE);
    }

    #[test]
    fn write_and_read_data() {
        let frame = Frame::new();
        // SAFETY: No concurrent access in this test.
        unsafe {
            let data = frame.data_mut();
            data[0] = 0xDE;
            data[1] = 0xAD;
            data[PAGE_SIZE - 1] = 0xFF;
        }
        let data = unsafe { frame.data() };
        assert_eq!(data[0], 0xDE);
        assert_eq!(data[1], 0xAD);
        assert_eq!(data[PAGE_SIZE - 1], 0xFF);
    }

    #[test]
    fn set_page_id() {
        let frame = Frame::new();
        let pid = PageId::new(FileId(1), 42);
        frame.set_page_id(pid);
        assert_eq!(frame.page_id(), pid);
        assert!(frame.has_valid_page());
    }

    #[test]
    fn pin_and_unpin() {
        let frame = Frame::new();
        assert_eq!(frame.pin(), 1);
        assert_eq!(frame.pin(), 2);
        assert!(frame.is_pinned());
        assert_eq!(frame.unpin(), 1);
        assert_eq!(frame.unpin(), 0);
        assert!(!frame.is_pinned());
    }

    #[test]
    fn dirty_flag() {
        let frame = Frame::new();
        assert!(!frame.is_dirty());
        frame.set_dirty();
        assert!(frame.is_dirty());
        frame.clear_dirty();
        assert!(!frame.is_dirty());
    }

    #[test]
    fn recently_used_flag() {
        let frame = Frame::new();
        assert!(!frame.is_recently_used());
        frame.set_recently_used();
        assert!(frame.is_recently_used());
        frame.clear_recently_used();
        assert!(!frame.is_recently_used());
    }

    #[test]
    fn reset() {
        let frame = Frame::new();
        frame.set_page_id(PageId::new(FileId(1), 0));
        frame.set_dirty();
        frame.set_recently_used();
        frame.reset();
        assert!(!frame.has_valid_page());
        assert!(!frame.is_dirty());
        assert!(!frame.is_recently_used());
    }

    #[test]
    fn latch_access() {
        let frame = Frame::new();
        frame.latch().lock_shared();
        assert!(!frame.latch().is_unlocked());
        frame.latch().unlock_shared();
        assert!(frame.latch().is_unlocked());
    }
}
