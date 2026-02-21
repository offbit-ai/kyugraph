//! Physical operator implementations.

pub mod aggregate;
pub mod bfs;
pub mod cross_product;
pub mod distinct;
pub mod empty;
pub mod filter;
pub mod hash_join;
pub mod limit;
pub mod projection;
pub mod recursive_join;
pub mod scan;
pub mod sort;
pub mod unwind;

pub use aggregate::AggregateOp;
pub use bfs::ShortestPathOp;
pub use cross_product::CrossProductOp;
pub use distinct::DistinctOp;
pub use empty::EmptyOp;
pub use filter::FilterOp;
pub use hash_join::HashJoinOp;
pub use limit::LimitOp;
pub use projection::ProjectionOp;
pub use recursive_join::RecursiveJoinOp;
pub use scan::ScanNodeOp;
pub use sort::OrderByOp;
pub use unwind::UnwindOp;
