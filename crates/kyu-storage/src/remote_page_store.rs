//! S3-backed page store for cloud deployment.
//!
//! Pages are stored as individual S3 objects under a configurable prefix:
//! `{prefix}/file_{file_id}/page_{page_idx}`
//!
//! The `PageStore` trait is synchronous, so this implementation blocks on
//! async S3 operations using a dedicated Tokio runtime. Do **not** call
//! `PageStore` methods from within an existing Tokio async context — use
//! `tokio::task::spawn_blocking` to move to a blocking thread first.

use std::collections::HashMap;
use std::sync::Mutex;

use aws_sdk_s3::primitives::ByteStream;
use kyu_common::{KyuError, KyuResult};

use crate::page_id::{FileId, PageId, PAGE_SIZE};
use crate::page_store::PageStore;

/// S3-backed page store.
///
/// Each page is stored as a separate S3 object. Object key layout:
/// `{prefix}/file_{file_id}/page_{page_idx}`.
///
/// Writes are durable once `put_object` succeeds (S3 strong consistency).
/// `sync_file` is a no-op since S3 guarantees read-after-write consistency.
pub struct RemotePageStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    runtime: tokio::runtime::Runtime,
    /// Tracks next page index per file for `allocate_page`.
    next_page: Mutex<HashMap<u32, u32>>,
}

impl RemotePageStore {
    /// Create a new S3 page store from an AWS SDK config.
    ///
    /// - `bucket`: S3 bucket name
    /// - `prefix`: key prefix for all pages (e.g. `"tenant-42/db"`)
    /// - `config`: AWS SDK config, typically from `aws_config::load_defaults()`
    pub fn new(bucket: String, prefix: String, config: &aws_config::SdkConfig) -> Self {
        let client = aws_sdk_s3::Client::new(config);
        Self::with_client(client, bucket, prefix)
    }

    /// Create from an existing S3 client (useful for testing with custom endpoints).
    pub fn with_client(client: aws_sdk_s3::Client, bucket: String, prefix: String) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for S3 page store");
        Self {
            client,
            bucket,
            prefix,
            runtime,
            next_page: Mutex::new(HashMap::new()),
        }
    }

    /// S3 object key for a given page.
    fn object_key(&self, page_id: PageId) -> String {
        format!(
            "{}/file_{}/page_{}",
            self.prefix, page_id.file_id.0, page_id.page_idx
        )
    }
}

impl PageStore for RemotePageStore {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let key = self.object_key(page_id);

        let result = self.runtime.block_on(async {
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
        });

        match result {
            Ok(output) => {
                let body = self
                    .runtime
                    .block_on(output.body.collect())
                    .map_err(|e| KyuError::Storage(format!("S3 read body error: {e}")))?;
                let data = body.into_bytes();
                if data.len() >= PAGE_SIZE {
                    buf.copy_from_slice(&data[..PAGE_SIZE]);
                } else {
                    // Partial object: fill remainder with zeros.
                    buf.fill(0);
                    buf[..data.len()].copy_from_slice(&data);
                }
                Ok(())
            }
            Err(err) => {
                // NoSuchKey → unwritten page → return zeros.
                let service_err = err.into_service_error();
                if service_err.is_no_such_key() {
                    buf.fill(0);
                    Ok(())
                } else {
                    Err(KyuError::Storage(format!("S3 read error: {service_err}")))
                }
            }
        }
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> KyuResult<()> {
        assert_eq!(buf.len(), PAGE_SIZE);
        let key = self.object_key(page_id);

        self.runtime
            .block_on(async {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .body(ByteStream::from(buf.to_vec()))
                    .send()
                    .await
            })
            .map_err(|e| KyuError::Storage(format!("S3 write error: {e}")))?;

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
        // S3 writes are immediately durable after PutObject succeeds.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_format() {
        // We can't construct a full RemotePageStore without AWS config,
        // so test the key format logic directly.
        let prefix = "tenant-1/db";
        let page_id = PageId::new(FileId(3), 42);
        let key = format!(
            "{}/file_{}/page_{}",
            prefix, page_id.file_id.0, page_id.page_idx
        );
        assert_eq!(key, "tenant-1/db/file_3/page_42");
    }

    #[test]
    fn object_key_root_prefix() {
        let prefix = "data";
        let page_id = PageId::new(FileId(0), 0);
        let key = format!(
            "{}/file_{}/page_{}",
            prefix, page_id.file_id.0, page_id.page_idx
        );
        assert_eq!(key, "data/file_0/page_0");
    }

    #[test]
    fn allocate_page_counter() {
        // Test the allocation counter without S3.
        let next_page: Mutex<HashMap<u32, u32>> = Mutex::new(HashMap::new());

        let alloc = |file_id: u32| -> u32 {
            let mut next = next_page.lock().unwrap();
            let idx = next.entry(file_id).or_insert(0);
            let page_idx = *idx;
            *idx += 1;
            page_idx
        };

        assert_eq!(alloc(0), 0);
        assert_eq!(alloc(0), 1);
        assert_eq!(alloc(0), 2);
        assert_eq!(alloc(1), 0); // Different file starts at 0.
        assert_eq!(alloc(1), 1);
        assert_eq!(alloc(0), 3);
    }
}
