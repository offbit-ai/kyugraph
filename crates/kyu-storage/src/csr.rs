//! Compressed Sparse Row (CSR) structures for relationship storage.
//!
//! CSR stores adjacency lists compactly: a header with (offset, length) per bound node,
//! and a body with neighbor IDs, relationship IDs, and properties.
//! Phase 3 is bulk-load only (no incremental insert).

use kyu_types::LogicalType;

use crate::column_chunk::ColumnChunkData;
use crate::constants::{NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
use crate::node_group::{NodeGroup, NodeGroupIdx};
use crate::storage_types::NodeGroupFormat;

/// A single CSR adjacency list: where it starts and how many entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CsrList {
    pub start_row: u64,
    pub length: u64,
}

impl CsrList {
    pub const EMPTY: Self = Self {
        start_row: 0,
        length: 0,
    };

    pub const fn new(start_row: u64, length: u64) -> Self {
        Self { start_row, length }
    }

    pub fn end_row(&self) -> u64 {
        self.start_row + self.length
    }
}

/// Direction of CSR traversal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CsrDirection {
    Forward,
    Backward,
}

/// Per-node CSR index tracking which rows belong to a given bound node.
#[derive(Clone, Debug)]
pub struct NodeCsrIndex {
    pub is_sequential: bool,
    pub row_indices: Vec<u64>,
}

impl NodeCsrIndex {
    pub fn sequential() -> Self {
        Self {
            is_sequential: true,
            row_indices: Vec::new(),
        }
    }

    pub fn with_indices(indices: Vec<u64>) -> Self {
        Self {
            is_sequential: false,
            row_indices: indices,
        }
    }

    pub fn len(&self) -> usize {
        self.row_indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_indices.is_empty()
    }
}

/// CSR index: one `NodeCsrIndex` per bound node in the group.
pub struct CsrIndex {
    indices: Vec<NodeCsrIndex>,
}

impl CsrIndex {
    pub fn new(num_nodes: u64) -> Self {
        Self {
            indices: (0..num_nodes).map(|_| NodeCsrIndex::sequential()).collect(),
        }
    }

    pub fn get(&self, offset: u64) -> &NodeCsrIndex {
        &self.indices[offset as usize]
    }

    pub fn set(&mut self, offset: u64, index: NodeCsrIndex) {
        self.indices[offset as usize] = index;
    }

    pub fn num_nodes(&self) -> u64 {
        self.indices.len() as u64
    }
}

/// CSR header: offset and length columns for each bound node.
/// Each entry stores a `CsrList` indicating where adjacency data starts and how long it is.
pub struct CsrHeader {
    offsets: ColumnChunkData,
    lengths: ColumnChunkData,
    num_nodes: u64,
}

impl CsrHeader {
    pub fn new(num_nodes: u64) -> Self {
        let mut offsets = ColumnChunkData::new(LogicalType::UInt64, num_nodes);
        let mut lengths = ColumnChunkData::new(LogicalType::UInt64, num_nodes);
        offsets.set_num_values(num_nodes);
        lengths.set_num_values(num_nodes);
        Self {
            offsets,
            lengths,
            num_nodes,
        }
    }

    pub fn num_nodes(&self) -> u64 {
        self.num_nodes
    }

    pub fn get_csr_list(&self, offset: u64) -> CsrList {
        CsrList {
            start_row: self.offsets.get_value::<u64>(offset),
            length: self.lengths.get_value::<u64>(offset),
        }
    }

    pub fn set_csr_list(&mut self, offset: u64, list: CsrList) {
        self.offsets.set_value(offset, list.start_row);
        self.lengths.set_value(offset, list.length);
    }

    /// Total number of relationship entries across all nodes.
    pub fn total_length(&self) -> u64 {
        let mut total = 0u64;
        for i in 0..self.num_nodes {
            total += self.lengths.get_value::<u64>(i);
        }
        total
    }

    pub fn offsets(&self) -> &ColumnChunkData {
        &self.offsets
    }

    pub fn lengths(&self) -> &ColumnChunkData {
        &self.lengths
    }
}

/// CSR node group: wraps a NodeGroup with a CSR header and index.
///
/// Body columns: `[InternalId (NBR_ID), InternalId (REL_ID), ...properties]`
pub struct CsrNodeGroup {
    header: CsrHeader,
    index: CsrIndex,
    body: NodeGroup,
}

impl CsrNodeGroup {
    /// Create a new CSR node group.
    /// `num_bound_nodes` is the number of source nodes in this group.
    /// `property_types` are the types of relationship properties (excluding NBR_ID and REL_ID).
    /// `body_capacity` is the total number of relationship entries.
    pub fn new(
        node_group_idx: NodeGroupIdx,
        num_bound_nodes: u64,
        property_types: &[LogicalType],
        body_capacity: u64,
    ) -> Self {
        let header = CsrHeader::new(num_bound_nodes);
        let index = CsrIndex::new(num_bound_nodes);

        // Body columns: NBR_ID (InternalId), REL_ID (InternalId), then properties
        let mut body_types = vec![LogicalType::InternalId, LogicalType::InternalId];
        body_types.extend_from_slice(property_types);

        let mut body = NodeGroup::with_capacity(node_group_idx, body_types, body_capacity);
        body.set_format(NodeGroupFormat::Csr);

        Self {
            header,
            index,
            body,
        }
    }

    pub fn header(&self) -> &CsrHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut CsrHeader {
        &mut self.header
    }

    pub fn index(&self) -> &CsrIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut CsrIndex {
        &mut self.index
    }

    pub fn body(&self) -> &NodeGroup {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut NodeGroup {
        &mut self.body
    }

    /// Get the CSR list (start_row, length) for a bound node.
    pub fn get_neighbors(&self, bound_node_offset: u64) -> CsrList {
        self.header.get_csr_list(bound_node_offset)
    }

    /// Number of body columns (including NBR_ID and REL_ID).
    pub fn num_body_columns(&self) -> usize {
        self.body.data_types().len()
    }

    /// Column ID for neighbor ID.
    pub fn nbr_id_column_id() -> u32 {
        NBR_ID_COLUMN_ID
    }

    /// Column ID for relationship ID.
    pub fn rel_id_column_id() -> u32 {
        REL_ID_COLUMN_ID
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_chunk::FixedSizeValue;
    use kyu_common::InternalId;

    #[test]
    fn csr_list_new() {
        let list = CsrList::new(10, 5);
        assert_eq!(list.start_row, 10);
        assert_eq!(list.length, 5);
        assert_eq!(list.end_row(), 15);
    }

    #[test]
    fn csr_list_empty() {
        let list = CsrList::EMPTY;
        assert_eq!(list.start_row, 0);
        assert_eq!(list.length, 0);
    }

    #[test]
    fn node_csr_index_sequential() {
        let idx = NodeCsrIndex::sequential();
        assert!(idx.is_sequential);
        assert!(idx.is_empty());
    }

    #[test]
    fn node_csr_index_with_indices() {
        let idx = NodeCsrIndex::with_indices(vec![0, 1, 2]);
        assert!(!idx.is_sequential);
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn csr_index_new() {
        let idx = CsrIndex::new(10);
        assert_eq!(idx.num_nodes(), 10);
        assert!(idx.get(0).is_sequential);
    }

    #[test]
    fn csr_index_set() {
        let mut idx = CsrIndex::new(5);
        idx.set(2, NodeCsrIndex::with_indices(vec![10, 20]));
        assert!(!idx.get(2).is_sequential);
        assert_eq!(idx.get(2).len(), 2);
    }

    #[test]
    fn csr_header_new() {
        let header = CsrHeader::new(10);
        assert_eq!(header.num_nodes(), 10);
        assert_eq!(header.total_length(), 0);
    }

    #[test]
    fn csr_header_set_and_get() {
        let mut header = CsrHeader::new(10);
        header.set_csr_list(0, CsrList::new(0, 5));
        header.set_csr_list(1, CsrList::new(5, 3));
        header.set_csr_list(2, CsrList::new(8, 7));

        assert_eq!(header.get_csr_list(0), CsrList::new(0, 5));
        assert_eq!(header.get_csr_list(1), CsrList::new(5, 3));
        assert_eq!(header.get_csr_list(2), CsrList::new(8, 7));
        assert_eq!(header.total_length(), 15);
    }

    #[test]
    fn csr_node_group_new() {
        let cng = CsrNodeGroup::new(NodeGroupIdx(0), 10, &[LogicalType::Int32], 100);
        assert_eq!(cng.header().num_nodes(), 10);
        assert_eq!(cng.index().num_nodes(), 10);
        assert_eq!(cng.num_body_columns(), 3); // NBR_ID + REL_ID + 1 property
        assert_eq!(cng.body().capacity(), 100);
    }

    #[test]
    fn csr_node_group_bulk_load() {
        // Simulate a simple CSR bulk load:
        // Node 0 -> neighbors [1, 2]
        // Node 1 -> neighbor [3]
        let mut cng = CsrNodeGroup::new(NodeGroupIdx(0), 3, &[], 10);

        // Set header
        cng.header_mut().set_csr_list(0, CsrList::new(0, 2));
        cng.header_mut().set_csr_list(1, CsrList::new(2, 1));
        cng.header_mut().set_csr_list(2, CsrList::EMPTY);

        // Add body rows: NBR_ID + REL_ID (no extra properties)
        let nbr1 = InternalId::new(0, 1);
        let rel1 = InternalId::new(0, 100);
        cng.body_mut()
            .append_row(&[Some(&nbr1.to_bytes()), Some(&rel1.to_bytes())]);

        let nbr2 = InternalId::new(0, 2);
        let rel2 = InternalId::new(0, 101);
        cng.body_mut()
            .append_row(&[Some(&nbr2.to_bytes()), Some(&rel2.to_bytes())]);

        let nbr3 = InternalId::new(0, 3);
        let rel3 = InternalId::new(0, 102);
        cng.body_mut()
            .append_row(&[Some(&nbr3.to_bytes()), Some(&rel3.to_bytes())]);

        // Verify
        assert_eq!(cng.get_neighbors(0), CsrList::new(0, 2));
        assert_eq!(cng.get_neighbors(1), CsrList::new(2, 1));
        assert_eq!(cng.get_neighbors(2), CsrList::EMPTY);
        assert_eq!(cng.body().num_rows(), 3);
        assert_eq!(cng.header().total_length(), 3);
    }

    #[test]
    fn csr_node_group_with_properties() {
        let props = vec![LogicalType::Double, LogicalType::Int32];
        let cng = CsrNodeGroup::new(NodeGroupIdx(0), 5, &props, 50);
        // NBR_ID + REL_ID + 2 properties
        assert_eq!(cng.num_body_columns(), 4);
    }

    #[test]
    fn csr_node_group_column_ids() {
        assert_eq!(CsrNodeGroup::nbr_id_column_id(), 0);
        assert_eq!(CsrNodeGroup::rel_id_column_id(), 1);
    }

    #[test]
    fn csr_direction() {
        assert_ne!(CsrDirection::Forward, CsrDirection::Backward);
    }

    #[test]
    fn csr_header_total_length_empty() {
        let header = CsrHeader::new(5);
        assert_eq!(header.total_length(), 0);
    }
}
