use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use kyu_common::{KyuError, KyuResult};

use crate::page_id::{FileId, PageId, PAGE_SIZE};
use crate::page_store::PageStore;

/// Filesystem-backed page store.
///
/// Each `FileId` maps to a file on disk under the root directory.
/// Pages are stored at fixed offsets: `page_idx * PAGE_SIZE`.
pub struct LocalPageStore {
    root: PathBuf,
    files: Mutex<HashMap<u32, File>>,
}

impl LocalPageStore {
    /// Create a new local page store rooted at the given directory.
    /// The directory must already exist.
    pub fn new(root: impl AsRef<Path>) -> KyuResult<Self> {
        let root = root.as_ref().to_path_buf();
        if !root.exists() {
            std::fs::create_dir_all(&root).map_err(|e| {
                KyuError::Storage(format!("failed to create storage directory: {e}"))
            })?;
        }
        Ok(Self {
            root,
            files: Mutex::new(HashMap::new()),
        })
    }

    /// Get or open the file for a given FileId.
    fn get_or_open_file(&self, file_id: FileId) -> KyuResult<std::sync::MutexGuard<'_, HashMap<u32, File>>> {
        let mut files = self.files.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(e) = files.entry(file_id.0) {
            let path = self.root.join(format!("file_{}.db", file_id.0));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|err| {
                    KyuError::Storage(format!("failed to open {}: {err}", path.display()))
                })?;
            e.insert(file);
        }
        Ok(files)
    }

    /// Get the file path for a given FileId.
    pub fn file_path(&self, file_id: FileId) -> PathBuf {
        self.root.join(format!("file_{}.db", file_id.0))
    }
}

impl PageStore for LocalPageStore {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let mut files = self.get_or_open_file(page_id.file_id)?;
        let file = files.get_mut(&page_id.file_id.0).unwrap();
        let offset = page_id.page_idx as u64 * PAGE_SIZE as u64;

        // If the file is shorter than the offset, return zeros
        let file_len = file
            .seek(SeekFrom::End(0))
            .map_err(|e| KyuError::Storage(format!("seek error: {e}")))?;

        if offset >= file_len {
            buf.fill(0);
            return Ok(());
        }

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| KyuError::Storage(format!("seek error: {e}")))?;

        let bytes_available = (file_len - offset) as usize;
        if bytes_available < PAGE_SIZE {
            // Partial page: read what we can, zero the rest
            buf.fill(0);
            file.read_exact(&mut buf[..bytes_available])
                .map_err(|e| KyuError::Storage(format!("read error: {e}")))?;
        } else {
            file.read_exact(buf)
                .map_err(|e| KyuError::Storage(format!("read error: {e}")))?;
        }

        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let mut files = self.get_or_open_file(page_id.file_id)?;
        let file = files.get_mut(&page_id.file_id.0).unwrap();
        let offset = page_id.page_idx as u64 * PAGE_SIZE as u64;

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| KyuError::Storage(format!("seek error: {e}")))?;
        file.write_all(buf)
            .map_err(|e| KyuError::Storage(format!("write error: {e}")))?;

        Ok(())
    }

    fn allocate_page(&self, file_id: FileId) -> KyuResult<u32> {
        let mut files = self.get_or_open_file(file_id)?;
        let file = files.get_mut(&file_id.0).unwrap();
        let file_len = file
            .seek(SeekFrom::End(0))
            .map_err(|e| KyuError::Storage(format!("seek error: {e}")))?;

        // Next page index is file_len / PAGE_SIZE (pages are appended)
        let page_idx = (file_len / PAGE_SIZE as u64) as u32;

        // Write a zero page to extend the file
        let zeros = vec![0u8; PAGE_SIZE];
        file.write_all(&zeros)
            .map_err(|e| KyuError::Storage(format!("write error: {e}")))?;

        Ok(page_idx)
    }

    fn sync_file(&self, file_id: FileId) -> KyuResult<()> {
        let mut files = self.get_or_open_file(file_id)?;
        if let Some(file) = files.get_mut(&file_id.0) {
            file.sync_all()
                .map_err(|e| KyuError::Storage(format!("sync error: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn with_temp_store<F: FnOnce(LocalPageStore)>(f: F) {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kyu_test_{}_{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let store = LocalPageStore::new(&dir).unwrap();
        f(store);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_and_read_page() {
        with_temp_store(|store| {
            let pid = PageId::new(FileId(0), 0);
            let mut data = vec![0u8; PAGE_SIZE];
            data[0] = 0xDE;
            data[1] = 0xAD;
            data[PAGE_SIZE - 1] = 0xFF;

            store.write_page(pid, &data).unwrap();

            let mut buf = vec![0u8; PAGE_SIZE];
            store.read_page(pid, &mut buf).unwrap();
            assert_eq!(buf[0], 0xDE);
            assert_eq!(buf[1], 0xAD);
            assert_eq!(buf[PAGE_SIZE - 1], 0xFF);
        });
    }

    #[test]
    fn read_unwritten_returns_zeros() {
        with_temp_store(|store| {
            let mut buf = vec![0xFFu8; PAGE_SIZE];
            store
                .read_page(PageId::new(FileId(0), 0), &mut buf)
                .unwrap();
            assert!(buf.iter().all(|&b| b == 0));
        });
    }

    #[test]
    fn allocate_pages() {
        with_temp_store(|store| {
            let p0 = store.allocate_page(FileId(0)).unwrap();
            let p1 = store.allocate_page(FileId(0)).unwrap();
            assert_eq!(p0, 0);
            assert_eq!(p1, 1);

            // File should be 2 * PAGE_SIZE bytes
            let path = store.file_path(FileId(0));
            let len = std::fs::metadata(&path).unwrap().len();
            assert_eq!(len, 2 * PAGE_SIZE as u64);
        });
    }

    #[test]
    fn multiple_files() {
        with_temp_store(|store| {
            let mut data = vec![0u8; PAGE_SIZE];
            data[0] = 1;
            store.write_page(PageId::new(FileId(0), 0), &data).unwrap();

            data[0] = 2;
            store.write_page(PageId::new(FileId(1), 0), &data).unwrap();

            let mut buf = vec![0u8; PAGE_SIZE];
            store.read_page(PageId::new(FileId(0), 0), &mut buf).unwrap();
            assert_eq!(buf[0], 1);

            store.read_page(PageId::new(FileId(1), 0), &mut buf).unwrap();
            assert_eq!(buf[0], 2);
        });
    }

    #[test]
    fn sync_file() {
        with_temp_store(|store| {
            let mut data = vec![0u8; PAGE_SIZE];
            data[0] = 42;
            store.write_page(PageId::new(FileId(0), 0), &data).unwrap();
            store.sync_file(FileId(0)).unwrap();
        });
    }

    #[test]
    fn overwrite_page() {
        with_temp_store(|store| {
            let pid = PageId::new(FileId(0), 0);

            let mut data = vec![0u8; PAGE_SIZE];
            data[0] = 1;
            store.write_page(pid, &data).unwrap();

            data[0] = 2;
            store.write_page(pid, &data).unwrap();

            let mut buf = vec![0u8; PAGE_SIZE];
            store.read_page(pid, &mut buf).unwrap();
            assert_eq!(buf[0], 2);
        });
    }
}
