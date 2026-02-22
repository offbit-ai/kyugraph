//! kyu-coord: coordinator, worker pool, task queue, tenant registry.
//!
//! Manages multi-tenant database workloads by routing queries to workers
//! and tracking per-tenant configuration and resource limits.

mod task;
mod tenant;
mod worker;

pub use task::{Task, TaskId, TaskPriority, TaskQueue, TaskResult};
pub use tenant::{TenantConfig, TenantId, TenantRegistry};
pub use worker::WorkerPool;
