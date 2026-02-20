//! kyu-delta: GraphDelta, DeltaBatch, upsert fast path.
//!
//! This crate defines the data model for conflict-free idempotent upserts
//! that bypass OCC for "last write wins" semantics. Designed for
//! KyuGraph's target workloads: agentic code graphs and high-throughput
//! document ingestion.
//!
//! kyu-delta is a data-model-only crate. The actual application of deltas
//! to storage is done by the executor (kyu-executor) in later phases.

pub mod batch;
pub mod delta;
pub mod node_key;
pub mod stats;
pub mod value;
pub mod vector_clock;

pub use batch::{DeltaBatch, DeltaBatchBuilder};
pub use delta::GraphDelta;
pub use node_key::NodeKey;
pub use stats::DeltaStats;
pub use value::DeltaValue;
pub use vector_clock::VectorClock;
