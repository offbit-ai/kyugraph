//! kyu-storage: buffer manager, page store, frame management.
//!
//! Unsafe boundary: `frame.rs` and `latch.rs` contain the only `unsafe` code.
//! All other modules are safe Rust.

pub mod buffer_manager;
pub mod frame;
pub mod latch;
pub mod local_page_store;
pub mod page_id;
pub mod page_store;
pub mod pool;

pub use buffer_manager::{BufferManager, BufferManagerStats, PinnedPage};
pub use frame::Frame;
pub use latch::RwLatch;
pub use local_page_store::LocalPageStore;
pub use page_id::{FileId, FrameIdx, PageId, PoolId, PAGE_SIZE};
pub use page_store::{MockPageStore, PageStore};
pub use pool::Pool;
