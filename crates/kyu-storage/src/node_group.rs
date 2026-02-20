//! NodeGroup: manages NODE_GROUP_SIZE (131,072) rows as a collection of ChunkedNodeGroups.

use kyu_types::LogicalType;

use crate::chunked_node_group::ChunkedNodeGroup;
use crate::constants::{CHUNKED_NODE_GROUP_CAPACITY, NODE_GROUP_SIZE};
use crate::storage_types::NodeGroupFormat;

/// Typed node group index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeGroupIdx(pub u64);

/// A node group holds up to `NODE_GROUP_SIZE` (131,072) rows,
/// organized as a collection of `ChunkedNodeGroup`s (each 2,048 rows).
pub struct NodeGroup {
    node_group_idx: NodeGroupIdx,
    format: NodeGroupFormat,
    data_types: Vec<LogicalType>,
    num_rows: u64,
    capacity: u64,
    chunked_groups: Vec<ChunkedNodeGroup>,
}

impl NodeGroup {
    /// Create a new node group with default capacity (NODE_GROUP_SIZE).
    pub fn new(node_group_idx: NodeGroupIdx, data_types: Vec<LogicalType>) -> Self {
        Self::with_capacity(node_group_idx, data_types, NODE_GROUP_SIZE)
    }

    /// Create a node group with a custom capacity.
    pub fn with_capacity(
        node_group_idx: NodeGroupIdx,
        data_types: Vec<LogicalType>,
        capacity: u64,
    ) -> Self {
        Self {
            node_group_idx,
            format: NodeGroupFormat::Regular,
            data_types,
            num_rows: 0,
            capacity,
            chunked_groups: Vec::new(),
        }
    }

    pub fn node_group_idx(&self) -> NodeGroupIdx {
        self.node_group_idx
    }

    pub fn format(&self) -> NodeGroupFormat {
        self.format
    }

    pub fn set_format(&mut self, format: NodeGroupFormat) {
        self.format = format;
    }

    pub fn data_types(&self) -> &[LogicalType] {
        &self.data_types
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn is_full(&self) -> bool {
        self.num_rows >= self.capacity
    }

    pub fn num_chunked_groups(&self) -> usize {
        self.chunked_groups.len()
    }

    pub fn chunked_group(&self, idx: usize) -> &ChunkedNodeGroup {
        &self.chunked_groups[idx]
    }

    pub fn chunked_group_mut(&mut self, idx: usize) -> &mut ChunkedNodeGroup {
        &mut self.chunked_groups[idx]
    }

    /// Map a global row index to (chunked_group_idx, local_row_within_group).
    pub fn global_row_to_chunked_group(&self, row: u64) -> (usize, u64) {
        let chunk_capacity = self
            .chunked_groups
            .first()
            .map(|g| g.capacity())
            .unwrap_or(CHUNKED_NODE_GROUP_CAPACITY);
        let group_idx = (row / chunk_capacity) as usize;
        let local_row = row % chunk_capacity;
        (group_idx, local_row)
    }

    /// Append a row of raw byte values across all columns.
    /// Automatically creates new ChunkedNodeGroups as needed.
    /// Returns the global row index within the node group.
    pub fn append_row(&mut self, values: &[Option<&[u8]>]) -> u64 {
        debug_assert!(!self.is_full());

        // Create first chunked group or a new one if current is full.
        let needs_new = self
            .chunked_groups
            .last()
            .is_none_or(|g| g.is_full());

        if needs_new {
            let start = self.num_rows;
            let remaining = self.capacity - self.num_rows;
            let cap = remaining.min(CHUNKED_NODE_GROUP_CAPACITY);
            self.chunked_groups
                .push(ChunkedNodeGroup::with_capacity(&self.data_types, start, cap));
        }

        let last = self.chunked_groups.last_mut().unwrap();
        last.append_row(values);
        let global_row = self.num_rows;
        self.num_rows += 1;
        global_row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_node_group() {
        let ng = NodeGroup::new(NodeGroupIdx(0), vec![LogicalType::Int64]);
        assert_eq!(ng.node_group_idx(), NodeGroupIdx(0));
        assert_eq!(ng.capacity(), NODE_GROUP_SIZE);
        assert_eq!(ng.num_rows(), 0);
        assert!(!ng.is_full());
        assert_eq!(ng.num_chunked_groups(), 0);
    }

    #[test]
    fn append_creates_chunked_group() {
        let mut ng = NodeGroup::new(NodeGroupIdx(0), vec![LogicalType::Int32]);
        let val: i32 = 42;
        ng.append_row(&[Some(&val.to_ne_bytes())]);
        assert_eq!(ng.num_rows(), 1);
        assert_eq!(ng.num_chunked_groups(), 1);
    }

    #[test]
    fn append_multiple_rows() {
        let mut ng = NodeGroup::new(NodeGroupIdx(0), vec![LogicalType::Int64]);
        for i in 0..100u64 {
            let val = i as i64;
            ng.append_row(&[Some(&val.to_ne_bytes())]);
        }
        assert_eq!(ng.num_rows(), 100);
        assert_eq!(ng.num_chunked_groups(), 1);
    }

    #[test]
    fn spill_to_second_chunked_group() {
        let mut ng = NodeGroup::with_capacity(
            NodeGroupIdx(0),
            vec![LogicalType::Int32],
            CHUNKED_NODE_GROUP_CAPACITY + 10,
        );

        for i in 0..CHUNKED_NODE_GROUP_CAPACITY + 5 {
            let val = i as i32;
            ng.append_row(&[Some(&val.to_ne_bytes())]);
        }

        assert_eq!(ng.num_rows(), CHUNKED_NODE_GROUP_CAPACITY + 5);
        assert_eq!(ng.num_chunked_groups(), 2);
        assert_eq!(
            ng.chunked_group(0).num_rows(),
            CHUNKED_NODE_GROUP_CAPACITY
        );
        assert_eq!(ng.chunked_group(1).num_rows(), 5);
    }

    #[test]
    fn global_row_to_chunked_group_mapping() {
        let mut ng = NodeGroup::new(NodeGroupIdx(0), vec![LogicalType::Int32]);
        for i in 0..CHUNKED_NODE_GROUP_CAPACITY + 5 {
            let val = i as i32;
            ng.append_row(&[Some(&val.to_ne_bytes())]);
        }

        let (g0, l0) = ng.global_row_to_chunked_group(0);
        assert_eq!(g0, 0);
        assert_eq!(l0, 0);

        let (g1, l1) = ng.global_row_to_chunked_group(CHUNKED_NODE_GROUP_CAPACITY);
        assert_eq!(g1, 1);
        assert_eq!(l1, 0);

        let (g2, l2) = ng.global_row_to_chunked_group(CHUNKED_NODE_GROUP_CAPACITY + 4);
        assert_eq!(g2, 1);
        assert_eq!(l2, 4);
    }

    #[test]
    fn with_nulls() {
        let mut ng = NodeGroup::new(
            NodeGroupIdx(0),
            vec![LogicalType::Int32, LogicalType::Int64],
        );
        let v: i32 = 1;
        ng.append_row(&[Some(&v.to_ne_bytes()), None]);
        assert_eq!(ng.num_rows(), 1);

        let group = ng.chunked_group(0);
        assert!(!group.column(0).is_null(0));
        assert!(group.column(1).is_null(0));
    }

    #[test]
    fn custom_capacity() {
        let ng = NodeGroup::with_capacity(NodeGroupIdx(5), vec![LogicalType::Int32], 100);
        assert_eq!(ng.capacity(), 100);
        assert_eq!(ng.node_group_idx(), NodeGroupIdx(5));
    }

    #[test]
    fn is_full_at_capacity() {
        let mut ng = NodeGroup::with_capacity(NodeGroupIdx(0), vec![LogicalType::Int32], 3);
        let v: i32 = 0;
        ng.append_row(&[Some(&v.to_ne_bytes())]);
        ng.append_row(&[Some(&v.to_ne_bytes())]);
        assert!(!ng.is_full());
        ng.append_row(&[Some(&v.to_ne_bytes())]);
        assert!(ng.is_full());
    }

    #[test]
    fn format_default() {
        let ng = NodeGroup::new(NodeGroupIdx(0), vec![LogicalType::Int32]);
        assert_eq!(ng.format(), NodeGroupFormat::Regular);
    }

    #[test]
    fn data_types() {
        let types = vec![LogicalType::Int32, LogicalType::String, LogicalType::Bool];
        let ng = NodeGroup::new(NodeGroupIdx(0), types.clone());
        assert_eq!(ng.data_types(), &types);
    }
}
