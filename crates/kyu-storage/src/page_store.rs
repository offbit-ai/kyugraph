use kyu_common::KyuResult;

use crate::page_id::{FileId, PageId, PAGE_SIZE};

/// Trait for reading and writing pages to persistent storage.
///
/// Implementations include:
/// - `LocalPageStore`: filesystem-backed storage
/// - `MockPageStore`: in-memory store for testing
pub trait PageStore: Send + Sync {
    /// Read a page from storage into the given buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> KyuResult<()>;

    /// Write a page from the given buffer to storage.
    /// The buffer must be exactly PAGE_SIZE bytes.
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> KyuResult<()>;

    /// Allocate a new page in the given file, returning its page index.
    fn allocate_page(&self, file_id: FileId) -> KyuResult<u32>;

    /// Sync a file's data to durable storage.
    fn sync_file(&self, file_id: FileId) -> KyuResult<()>;
}

/// In-memory page store for testing. No disk I/O.
pub struct MockPageStore {
    pages: std::sync::Mutex<std::collections::HashMap<(u32, u32), Vec<u8>>>,
    next_page: std::sync::Mutex<std::collections::HashMap<u32, u32>>,
}

impl MockPageStore {
    pub fn new() -> Self {
        Self {
            pages: std::sync::Mutex::new(std::collections::HashMap::new()),
            next_page: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get the number of pages stored.
    pub fn page_count(&self) -> usize {
        self.pages.lock().unwrap().len()
    }
}

impl Default for MockPageStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PageStore for MockPageStore {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let pages = self.pages.lock().unwrap();
        let key = (page_id.file_id.0, page_id.page_idx);
        if let Some(data) = pages.get(&key) {
            buf.copy_from_slice(data);
        } else {
            // Unwritten pages return zeros
            buf.fill(0);
        }
        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let mut pages = self.pages.lock().unwrap();
        let key = (page_id.file_id.0, page_id.page_idx);
        pages.insert(key, buf.to_vec());
        Ok(())
    }

    fn allocate_page(&self, file_id: FileId) -> KyuResult<u32> {
        let mut next = self.next_page.lock().unwrap();
        let idx = next.entry(file_id.0).or_insert(0);
        let page_idx = *idx;
        *idx += 1;
        Ok(page_idx)
    }

    fn sync_file(&self, _file_id: FileId) -> KyuResult<()> {
        // No-op for in-memory store
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_store_read_unwritten() {
        let store = MockPageStore::new();
        let mut buf = vec![0xFFu8; PAGE_SIZE];
        store
            .read_page(PageId::new(FileId(0), 0), &mut buf)
            .unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn mock_store_write_and_read() {
        let store = MockPageStore::new();
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 0xDE;
        data[1] = 0xAD;

        let pid = PageId::new(FileId(0), 0);
        store.write_page(pid, &data).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        store.read_page(pid, &mut buf).unwrap();
        assert_eq!(buf[0], 0xDE);
        assert_eq!(buf[1], 0xAD);
    }

    #[test]
    fn mock_store_allocate() {
        let store = MockPageStore::new();
        let p0 = store.allocate_page(FileId(0)).unwrap();
        let p1 = store.allocate_page(FileId(0)).unwrap();
        let p2 = store.allocate_page(FileId(1)).unwrap();
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
        assert_eq!(p2, 0); // Different file, starts at 0
    }

    #[test]
    fn mock_store_sync() {
        let store = MockPageStore::new();
        store.sync_file(FileId(0)).unwrap(); // Should not error
    }

    #[test]
    fn mock_store_page_count() {
        let store = MockPageStore::new();
        assert_eq!(store.page_count(), 0);

        let data = vec![0u8; PAGE_SIZE];
        store
            .write_page(PageId::new(FileId(0), 0), &data)
            .unwrap();
        store
            .write_page(PageId::new(FileId(0), 1), &data)
            .unwrap();
        assert_eq!(store.page_count(), 2);
    }

    #[test]
    fn mock_store_overwrite() {
        let store = MockPageStore::new();
        let pid = PageId::new(FileId(0), 0);

        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 1;
        store.write_page(pid, &data).unwrap();

        data[0] = 2;
        store.write_page(pid, &data).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        store.read_page(pid, &mut buf).unwrap();
        assert_eq!(buf[0], 2);
    }
}
