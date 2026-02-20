//! Centralized storage constants matching Kuzu's system configuration.

/// Number of rows per node group (2^17).
pub const NODE_GROUP_SIZE: u64 = 131_072;

/// Number of rows per chunked node group (2^11).
pub const CHUNKED_NODE_GROUP_CAPACITY: u64 = 2_048;

/// CSR leaf region size (2^10).
pub const CSR_LEAF_REGION_SIZE: u64 = 1_024;

/// Target density for packed CSR.
pub const PACKED_CSR_DENSITY: f64 = 0.8;

/// High density threshold for CSR leaf regions.
pub const LEAF_HIGH_CSR_DENSITY: f64 = 0.8;

/// Column index for neighbor ID in CSR node groups.
pub const NBR_ID_COLUMN_ID: u32 = 0;

/// Column index for relationship ID in CSR node groups.
pub const REL_ID_COLUMN_ID: u32 = 1;

/// Size of a hash index slot in bytes.
pub const SLOT_CAPACITY_BYTES: usize = 256;

/// Number of sub-indices in a hash index.
pub const NUM_HASH_INDEXES: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_group_size_is_power_of_two() {
        assert!(NODE_GROUP_SIZE.is_power_of_two());
        assert_eq!(NODE_GROUP_SIZE, 1 << 17);
    }

    #[test]
    fn chunked_capacity_is_power_of_two() {
        assert!(CHUNKED_NODE_GROUP_CAPACITY.is_power_of_two());
        assert_eq!(CHUNKED_NODE_GROUP_CAPACITY, 1 << 11);
    }

    #[test]
    fn csr_leaf_region_is_power_of_two() {
        assert!(CSR_LEAF_REGION_SIZE.is_power_of_two());
        assert_eq!(CSR_LEAF_REGION_SIZE, 1 << 10);
    }

    #[test]
    fn chunked_groups_per_node_group() {
        assert_eq!(NODE_GROUP_SIZE / CHUNKED_NODE_GROUP_CAPACITY, 64);
    }

    #[test]
    fn column_ids() {
        assert_eq!(NBR_ID_COLUMN_ID, 0);
        assert_eq!(REL_ID_COLUMN_ID, 1);
    }
}
