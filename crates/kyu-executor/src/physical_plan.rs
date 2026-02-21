//! Physical operator enum — pull-based execution via `next()`.

use kyu_common::KyuResult;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::operators::*;

/// A physical operator. Enum dispatch for branch-predictable execution.
pub enum PhysicalOperator {
    ScanNode(ScanNodeOp),
    Filter(FilterOp),
    Projection(ProjectionOp),
    HashJoin(HashJoinOp),
    CrossProduct(CrossProductOp),
    Aggregate(AggregateOp),
    OrderBy(OrderByOp),
    Limit(LimitOp),
    Distinct(DistinctOp),
    Unwind(UnwindOp),
    Empty(EmptyOp),
}

impl PhysicalOperator {
    /// Pull the next batch of rows. Returns `None` when exhausted.
    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        match self {
            Self::ScanNode(op) => op.next(ctx),
            Self::Filter(op) => op.next(ctx),
            Self::Projection(op) => op.next(ctx),
            Self::HashJoin(op) => op.next(ctx),
            Self::CrossProduct(op) => op.next(ctx),
            Self::Aggregate(op) => op.next(ctx),
            Self::OrderBy(op) => op.next(ctx),
            Self::Limit(op) => op.next(ctx),
            Self::Distinct(op) => op.next(ctx),
            Self::Unwind(op) => op.next(ctx),
            Self::Empty(op) => op.next(ctx),
        }
    }
}
