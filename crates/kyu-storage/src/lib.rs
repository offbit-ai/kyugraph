//! kyu-storage: buffer manager, page store, frame management, columnar storage.
//!
//! Unsafe boundary: `frame.rs` and `latch.rs` contain the only `unsafe` code.
//! All other modules are safe Rust.

pub mod buffer_manager;
pub mod chunked_node_group;
pub mod column_chunk;
pub mod constants;
pub mod csr;
pub mod disk_cache;
pub mod frame;
pub mod latch;
pub mod local_page_store;
pub mod node_group;
pub mod null_mask;
pub mod page_id;
pub mod page_store;
pub mod pool;
pub mod remote_page_store;
pub mod storage_types;

pub use buffer_manager::{BufferManager, BufferManagerStats, PinnedPage};
pub use chunked_node_group::ChunkedNodeGroup;
pub use column_chunk::{
    BoolChunkData, ColumnChunk, ColumnChunkData, FixedSizeValue, StringChunkData,
};
pub use constants::*;
pub use csr::{CsrDirection, CsrHeader, CsrIndex, CsrList, CsrNodeGroup, NodeCsrIndex};
pub use disk_cache::DiskCache;
pub use frame::Frame;
pub use latch::RwLatch;
pub use local_page_store::LocalPageStore;
pub use node_group::{NodeGroup, NodeGroupIdx};
pub use null_mask::NullMask;
pub use page_id::{FileId, FrameIdx, PAGE_SIZE, PageId, PoolId};
pub use page_store::{MockPageStore, PageStore};
pub use pool::Pool;
pub use remote_page_store::RemotePageStore;
pub use storage_types::*;
