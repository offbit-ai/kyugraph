//! Global write-ahead log (WAL).
//!
//! Supports file-backed WAL for durability and in-memory WAL for testing.
//! Single mutex acquired once per commit to flush the entire local buffer + fsync.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use kyu_common::id::Lsn;

use crate::local_wal::LocalWal;
use crate::wal_record::WalRecord;

/// WAL file name within the database directory.
const WAL_FILE_NAME: &str = "wal";

/// Global write-ahead log.
pub struct Wal {
    inner: Mutex<WalInner>,
}

enum WalStorage {
    /// File-backed WAL for durability.
    File {
        wal_path: PathBuf,
        writer: Option<BufWriter<File>>,
    },
    /// In-memory WAL for testing.
    InMemory {
        buffer: Vec<u8>,
    },
}

struct WalInner {
    storage: WalStorage,
    current_lsn: Lsn,
    file_size: u64,
    read_only: bool,
}

impl Wal {
    /// Create a file-backed WAL in the given database directory.
    pub fn new(db_path: &Path, read_only: bool) -> std::io::Result<Self> {
        let wal_path = db_path.join(WAL_FILE_NAME);
        let file_size = if wal_path.exists() {
            std::fs::metadata(&wal_path)?.len()
        } else {
            0
        };
        Ok(Self {
            inner: Mutex::new(WalInner {
                storage: WalStorage::File {
                    wal_path,
                    writer: None,
                },
                current_lsn: 0,
                file_size,
                read_only,
            }),
        })
    }

    /// Create an in-memory WAL (for tests).
    pub fn in_memory() -> Self {
        Self {
            inner: Mutex::new(WalInner {
                storage: WalStorage::InMemory { buffer: Vec::new() },
                current_lsn: 0,
                file_size: 0,
                read_only: false,
            }),
        }
    }

    /// Flush a committed transaction's local WAL to the global WAL.
    /// Returns the LSN assigned to this commit.
    pub fn log_committed_wal(&self, local_wal: &LocalWal) -> std::io::Result<Lsn> {
        if local_wal.is_empty() {
            return Ok(0);
        }

        let mut inner = self.inner.lock().unwrap();
        if inner.read_only {
            return Ok(0);
        }

        let data = local_wal.data();
        match &mut inner.storage {
            WalStorage::File { wal_path, writer } => {
                if writer.is_none() {
                    let file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(wal_path)?;
                    *writer = Some(BufWriter::new(file));
                }
                let w = writer.as_mut().unwrap();
                w.write_all(data)?;
                w.flush()?;
                w.get_ref().sync_all()?;
                inner.file_size += data.len() as u64;
            }
            WalStorage::InMemory { buffer } => {
                buffer.extend_from_slice(data);
                inner.file_size += data.len() as u64;
            }
        }

        inner.current_lsn += 1;
        Ok(inner.current_lsn)
    }

    /// Log a checkpoint record.
    pub fn log_checkpoint(&self) -> std::io::Result<Lsn> {
        let mut buf = Vec::new();
        WalRecord::Checkpoint.serialize(&mut buf);

        let mut inner = self.inner.lock().unwrap();
        if inner.read_only {
            return Ok(0);
        }

        match &mut inner.storage {
            WalStorage::File { wal_path, writer } => {
                if writer.is_none() {
                    let file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(wal_path)?;
                    *writer = Some(BufWriter::new(file));
                }
                let w = writer.as_mut().unwrap();
                w.write_all(&buf)?;
                w.flush()?;
                w.get_ref().sync_all()?;
                inner.file_size += buf.len() as u64;
            }
            WalStorage::InMemory { buffer } => {
                buffer.extend_from_slice(&buf);
                inner.file_size += buf.len() as u64;
            }
        }

        inner.current_lsn += 1;
        Ok(inner.current_lsn)
    }

    /// Clear the WAL (truncate file or clear buffer).
    pub fn clear(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match &mut inner.storage {
            WalStorage::File { wal_path, writer } => {
                *writer = None;
                if wal_path.exists() {
                    File::create(wal_path)?; // truncate
                }
                inner.file_size = 0;
            }
            WalStorage::InMemory { buffer } => {
                buffer.clear();
                inner.file_size = 0;
            }
        }
        Ok(())
    }

    /// Reset the WAL state (LSN and file size).
    pub fn reset(&self) -> std::io::Result<()> {
        self.clear()?;
        let mut inner = self.inner.lock().unwrap();
        inner.current_lsn = 0;
        Ok(())
    }

    /// Current LSN.
    pub fn current_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().current_lsn
    }

    /// Current WAL file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.inner.lock().unwrap().file_size
    }

    /// Read the raw WAL data (only for in-memory WAL, used in tests).
    pub fn in_memory_data(&self) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        match &inner.storage {
            WalStorage::InMemory { buffer } => Some(buffer.clone()),
            WalStorage::File { .. } => None,
        }
    }

    /// Get the WAL file path (only for file-backed WAL).
    pub fn wal_path(&self) -> Option<PathBuf> {
        let inner = self.inner.lock().unwrap();
        match &inner.storage {
            WalStorage::File { wal_path, .. } => Some(wal_path.clone()),
            WalStorage::InMemory { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_common::id::TableId;

    #[test]
    fn in_memory_wal_new() {
        let wal = Wal::in_memory();
        assert_eq!(wal.current_lsn(), 0);
        assert_eq!(wal.file_size(), 0);
    }

    #[test]
    fn in_memory_log_committed() {
        let wal = Wal::in_memory();
        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_table_insertion(TableId(1), 100);
        local.log_commit();

        let lsn = wal.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(wal.current_lsn(), 1);
        assert!(wal.file_size() > 0);

        // Verify data is in the buffer.
        let data = wal.in_memory_data().unwrap();
        assert_eq!(data.len(), local.data().len());
    }

    #[test]
    fn in_memory_multiple_commits() {
        let wal = Wal::in_memory();

        for i in 0..3 {
            let mut local = LocalWal::new();
            local.log_begin_transaction();
            local.log_table_insertion(TableId(i), 10);
            local.log_commit();
            wal.log_committed_wal(&local).unwrap();
        }

        assert_eq!(wal.current_lsn(), 3);
    }

    #[test]
    fn in_memory_empty_local_wal() {
        let wal = Wal::in_memory();
        let local = LocalWal::new();
        let lsn = wal.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 0); // No-op for empty local WAL.
        assert_eq!(wal.current_lsn(), 0);
    }

    #[test]
    fn in_memory_clear() {
        let wal = Wal::in_memory();
        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_commit();
        wal.log_committed_wal(&local).unwrap();

        wal.clear().unwrap();
        assert_eq!(wal.file_size(), 0);
        let data = wal.in_memory_data().unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn in_memory_reset() {
        let wal = Wal::in_memory();
        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_commit();
        wal.log_committed_wal(&local).unwrap();

        wal.reset().unwrap();
        assert_eq!(wal.current_lsn(), 0);
        assert_eq!(wal.file_size(), 0);
    }

    #[test]
    fn in_memory_checkpoint() {
        let wal = Wal::in_memory();
        let lsn = wal.log_checkpoint().unwrap();
        assert_eq!(lsn, 1);
        assert!(wal.file_size() > 0);

        let data = wal.in_memory_data().unwrap();
        let (record, _) = WalRecord::deserialize(&data).unwrap();
        assert_eq!(record, WalRecord::Checkpoint);
    }

    #[test]
    fn file_backed_wal() {
        let dir = tempdir();
        let wal = Wal::new(dir.path(), false).unwrap();

        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_table_insertion(TableId(1), 50);
        local.log_commit();

        let lsn = wal.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 1);
        assert!(wal.file_size() > 0);

        // Verify file exists.
        let wal_path = dir.path().join("wal");
        assert!(wal_path.exists());

        // Verify file contents match.
        let file_data = std::fs::read(&wal_path).unwrap();
        assert_eq!(file_data.len(), local.data().len());
    }

    #[test]
    fn file_backed_wal_read_only() {
        let dir = tempdir();
        let wal = Wal::new(dir.path(), true).unwrap();

        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_commit();

        let lsn = wal.log_committed_wal(&local).unwrap();
        assert_eq!(lsn, 0); // No-op for read-only.
    }

    #[test]
    fn file_backed_wal_clear() {
        let dir = tempdir();
        let wal = Wal::new(dir.path(), false).unwrap();

        let mut local = LocalWal::new();
        local.log_begin_transaction();
        local.log_commit();
        wal.log_committed_wal(&local).unwrap();

        wal.clear().unwrap();
        assert_eq!(wal.file_size(), 0);
    }

    fn tempdir() -> TempDir {
        TempDir::new()
    }

    struct TempDir(PathBuf);

    static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    impl TempDir {
        fn new() -> Self {
            let id = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "kyu_wal_test_{}_{id}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
