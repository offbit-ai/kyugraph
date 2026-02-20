use std::sync::atomic::{AtomicU32, Ordering};

use crate::frame::Frame;
use crate::page_id::FrameIdx;

/// A pool of buffer frames with clock (second-chance FIFO) eviction.
///
/// Each pool manages a fixed set of frames. The buffer manager uses
/// two pools: one for reads (70%) and one for writes (30%).
pub struct Pool {
    frames: Vec<Frame>,
    clock_hand: AtomicU32,
}

impl Pool {
    /// Create a new pool with the given number of frames.
    pub fn new(num_frames: u32) -> Self {
        let mut frames = Vec::with_capacity(num_frames as usize);
        for _ in 0..num_frames {
            frames.push(Frame::new());
        }
        Self {
            frames,
            clock_hand: AtomicU32::new(0),
        }
    }

    /// Get a reference to a frame by index.
    pub fn frame(&self, idx: FrameIdx) -> &Frame {
        &self.frames[idx.0 as usize]
    }

    /// Get the number of frames in this pool.
    pub fn num_frames(&self) -> u32 {
        self.frames.len() as u32
    }

    /// Find an evictable frame using the clock (second-chance) algorithm.
    ///
    /// Returns the index of an evictable frame, or `None` if all frames are pinned.
    /// A frame is evictable if it is not pinned. If it has the recently_used flag,
    /// we clear it and skip to the next frame (second chance).
    ///
    /// We scan at most `2 * num_frames` to ensure we complete a full cycle
    /// with cleared flags before giving up.
    pub fn find_evictable(&self) -> Option<FrameIdx> {
        let n = self.num_frames();
        if n == 0 {
            return None;
        }
        let max_scans = 2 * n;

        for _ in 0..max_scans {
            let idx = self.clock_hand.fetch_add(1, Ordering::Relaxed) % n;
            let frame = &self.frames[idx as usize];

            // Skip pinned frames
            if frame.is_pinned() {
                continue;
            }

            // Second-chance: if recently used, clear and move on
            if frame.is_recently_used() {
                frame.clear_recently_used();
                continue;
            }

            // Found an evictable frame
            return Some(FrameIdx(idx));
        }

        None // All frames are pinned
    }

    /// Find the first empty (invalid page) frame.
    pub fn find_empty(&self) -> Option<FrameIdx> {
        for (i, frame) in self.frames.iter().enumerate() {
            if !frame.has_valid_page() && !frame.is_pinned() {
                return Some(FrameIdx(i as u32));
            }
        }
        None
    }

    /// Count the number of dirty frames.
    pub fn dirty_count(&self) -> u32 {
        self.frames.iter().filter(|f| f.is_dirty()).count() as u32
    }

    /// Count the number of pinned frames.
    pub fn pinned_count(&self) -> u32 {
        self.frames.iter().filter(|f| f.is_pinned()).count() as u32
    }

    /// Count the number of frames with valid pages loaded.
    pub fn loaded_count(&self) -> u32 {
        self.frames
            .iter()
            .filter(|f| f.has_valid_page())
            .count() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_id::{FileId, PageId};

    #[test]
    fn new_pool() {
        let pool = Pool::new(10);
        assert_eq!(pool.num_frames(), 10);
        assert_eq!(pool.dirty_count(), 0);
        assert_eq!(pool.pinned_count(), 0);
        assert_eq!(pool.loaded_count(), 0);
    }

    #[test]
    fn find_empty_in_fresh_pool() {
        let pool = Pool::new(4);
        let idx = pool.find_empty().unwrap();
        assert_eq!(idx, FrameIdx(0));
    }

    #[test]
    fn find_empty_skips_loaded() {
        let pool = Pool::new(4);
        pool.frame(FrameIdx(0)).set_page_id(PageId::new(FileId(0), 0));
        pool.frame(FrameIdx(1)).set_page_id(PageId::new(FileId(0), 1));

        let idx = pool.find_empty().unwrap();
        assert_eq!(idx, FrameIdx(2));
    }

    #[test]
    fn find_empty_none_when_full() {
        let pool = Pool::new(2);
        pool.frame(FrameIdx(0)).set_page_id(PageId::new(FileId(0), 0));
        pool.frame(FrameIdx(1)).set_page_id(PageId::new(FileId(0), 1));

        assert!(pool.find_empty().is_none());
    }

    #[test]
    fn find_evictable_in_loaded_pool() {
        let pool = Pool::new(4);
        for i in 0..4 {
            let f = pool.frame(FrameIdx(i));
            f.set_page_id(PageId::new(FileId(0), i as u32));
        }

        let idx = pool.find_evictable().unwrap();
        assert!(idx.0 < 4);
    }

    #[test]
    fn find_evictable_skips_pinned() {
        let pool = Pool::new(3);
        for i in 0..3 {
            let f = pool.frame(FrameIdx(i));
            f.set_page_id(PageId::new(FileId(0), i as u32));
        }

        // Pin frames 0 and 1
        pool.frame(FrameIdx(0)).pin();
        pool.frame(FrameIdx(1)).pin();

        let idx = pool.find_evictable().unwrap();
        assert_eq!(idx, FrameIdx(2));
    }

    #[test]
    fn find_evictable_none_when_all_pinned() {
        let pool = Pool::new(2);
        for i in 0..2 {
            let f = pool.frame(FrameIdx(i));
            f.set_page_id(PageId::new(FileId(0), i as u32));
            f.pin();
        }

        assert!(pool.find_evictable().is_none());
    }

    #[test]
    fn find_evictable_second_chance() {
        let pool = Pool::new(3);
        for i in 0..3 {
            let f = pool.frame(FrameIdx(i));
            f.set_page_id(PageId::new(FileId(0), i as u32));
        }

        // Mark all as recently used
        pool.frame(FrameIdx(0)).set_recently_used();
        pool.frame(FrameIdx(1)).set_recently_used();
        pool.frame(FrameIdx(2)).set_recently_used();

        // Should still find one after clearing recently_used flags
        let idx = pool.find_evictable().unwrap();
        assert!(idx.0 < 3);
    }

    #[test]
    fn dirty_count() {
        let pool = Pool::new(4);
        pool.frame(FrameIdx(0)).set_dirty();
        pool.frame(FrameIdx(2)).set_dirty();
        assert_eq!(pool.dirty_count(), 2);
    }

    #[test]
    fn pinned_count() {
        let pool = Pool::new(4);
        pool.frame(FrameIdx(0)).pin();
        pool.frame(FrameIdx(1)).pin();
        pool.frame(FrameIdx(3)).pin();
        assert_eq!(pool.pinned_count(), 3);
    }

    #[test]
    fn loaded_count() {
        let pool = Pool::new(4);
        pool.frame(FrameIdx(0)).set_page_id(PageId::new(FileId(0), 0));
        pool.frame(FrameIdx(2)).set_page_id(PageId::new(FileId(0), 1));
        assert_eq!(pool.loaded_count(), 2);
    }
}
