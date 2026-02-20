//! kyu-executor: physical operators, morsel scheduler.

pub mod context;
pub mod data_chunk;
pub mod execute;
pub mod mapper;
pub mod operators;
pub mod physical_plan;
pub mod result;

pub use context::{ExecutionContext, MockStorage};
pub use data_chunk::DataChunk;
pub use execute::{execute, execute_statement};
pub use physical_plan::PhysicalOperator;
pub use result::QueryResult;
