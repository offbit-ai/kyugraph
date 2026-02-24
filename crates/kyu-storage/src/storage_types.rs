//! Storage type primitives: PageRange, StorageValue, column chunk stats and metadata.

/// Contiguous range of pages on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PageRange {
    pub start_page_idx: u32,
    pub num_pages: u32,
}

impl PageRange {
    pub const EMPTY: Self = Self {
        start_page_idx: 0,
        num_pages: 0,
    };

    pub const fn new(start_page_idx: u32, num_pages: u32) -> Self {
        Self {
            start_page_idx,
            num_pages,
        }
    }

    pub const fn end_page_idx(&self) -> u32 {
        self.start_page_idx + self.num_pages
    }
}

/// A typed storage value used for zone-map statistics (min/max).
/// Matches Kuzu's `StorageValue` union.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StorageValue {
    SignedInt(i64),
    UnsignedInt(u64),
    Float(f64),
    SignedInt128(i128),
}

impl StorageValue {
    pub fn min_of(a: &Self, b: &Self) -> Self {
        match (a, b) {
            (Self::SignedInt(x), Self::SignedInt(y)) => Self::SignedInt(*x.min(y)),
            (Self::UnsignedInt(x), Self::UnsignedInt(y)) => Self::UnsignedInt(*x.min(y)),
            (Self::Float(x), Self::Float(y)) => Self::Float(x.min(*y)),
            (Self::SignedInt128(x), Self::SignedInt128(y)) => Self::SignedInt128(*x.min(y)),
            _ => *a,
        }
    }

    pub fn max_of(a: &Self, b: &Self) -> Self {
        match (a, b) {
            (Self::SignedInt(x), Self::SignedInt(y)) => Self::SignedInt(*x.max(y)),
            (Self::UnsignedInt(x), Self::UnsignedInt(y)) => Self::UnsignedInt(*x.max(y)),
            (Self::Float(x), Self::Float(y)) => Self::Float(x.max(*y)),
            (Self::SignedInt128(x), Self::SignedInt128(y)) => Self::SignedInt128(*x.max(y)),
            _ => *a,
        }
    }
}

/// Zone-map statistics for a column chunk (min and max values).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnChunkStats {
    pub min: Option<StorageValue>,
    pub max: Option<StorageValue>,
}

impl ColumnChunkStats {
    pub fn update(&mut self, val: StorageValue) {
        self.min = Some(match &self.min {
            Some(cur) => StorageValue::min_of(cur, &val),
            None => val,
        });
        self.max = Some(match &self.max {
            Some(cur) => StorageValue::max_of(cur, &val),
            None => val,
        });
    }

    pub fn merge(&mut self, other: &ColumnChunkStats) {
        if let Some(ref other_min) = other.min {
            self.min = Some(match &self.min {
                Some(cur) => StorageValue::min_of(cur, other_min),
                None => *other_min,
            });
        }
        if let Some(ref other_max) = other.max {
            self.max = Some(match &self.max {
                Some(cur) => StorageValue::max_of(cur, other_max),
                None => *other_max,
            });
        }
    }
}

/// Merged statistics including null information.
#[derive(Clone, Debug, Default)]
pub struct MergedColumnChunkStats {
    pub stats: ColumnChunkStats,
    pub guaranteed_no_nulls: bool,
    pub guaranteed_all_nulls: bool,
}

/// Compression type used for column chunk data.
/// Phase 3 only supports uncompressed data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CompressionType {
    #[default]
    Uncompressed,
}

/// Metadata for a column chunk stored on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ColumnChunkMetadata {
    pub page_range: PageRange,
    pub num_values: u64,
    pub compression: CompressionType,
}

impl ColumnChunkMetadata {
    pub fn new(page_range: PageRange, num_values: u64, compression: CompressionType) -> Self {
        Self {
            page_range,
            num_values,
            compression,
        }
    }
}

/// Whether data is currently in memory or on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ResidencyState {
    #[default]
    InMemory,
    OnDisk,
}

/// Format of a node group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NodeGroupFormat {
    #[default]
    Regular,
    Csr,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_range_empty() {
        let r = PageRange::EMPTY;
        assert_eq!(r.start_page_idx, 0);
        assert_eq!(r.num_pages, 0);
        assert_eq!(r.end_page_idx(), 0);
    }

    #[test]
    fn page_range_end() {
        let r = PageRange::new(10, 5);
        assert_eq!(r.end_page_idx(), 15);
    }

    #[test]
    fn storage_value_min_max_signed() {
        let a = StorageValue::SignedInt(-10);
        let b = StorageValue::SignedInt(20);
        assert_eq!(StorageValue::min_of(&a, &b), StorageValue::SignedInt(-10));
        assert_eq!(StorageValue::max_of(&a, &b), StorageValue::SignedInt(20));
    }

    #[test]
    fn storage_value_min_max_unsigned() {
        let a = StorageValue::UnsignedInt(5);
        let b = StorageValue::UnsignedInt(100);
        assert_eq!(StorageValue::min_of(&a, &b), StorageValue::UnsignedInt(5));
        assert_eq!(StorageValue::max_of(&a, &b), StorageValue::UnsignedInt(100));
    }

    #[test]
    fn storage_value_min_max_float() {
        let a = StorageValue::Float(1.5);
        let b = StorageValue::Float(3.7);
        assert_eq!(StorageValue::min_of(&a, &b), StorageValue::Float(1.5));
        assert_eq!(StorageValue::max_of(&a, &b), StorageValue::Float(3.7));
    }

    #[test]
    fn storage_value_min_max_i128() {
        let a = StorageValue::SignedInt128(-1);
        let b = StorageValue::SignedInt128(1);
        assert_eq!(StorageValue::min_of(&a, &b), StorageValue::SignedInt128(-1));
        assert_eq!(StorageValue::max_of(&a, &b), StorageValue::SignedInt128(1));
    }

    #[test]
    fn column_chunk_stats_update() {
        let mut stats = ColumnChunkStats::default();
        stats.update(StorageValue::SignedInt(10));
        stats.update(StorageValue::SignedInt(-5));
        stats.update(StorageValue::SignedInt(20));
        assert_eq!(stats.min, Some(StorageValue::SignedInt(-5)));
        assert_eq!(stats.max, Some(StorageValue::SignedInt(20)));
    }

    #[test]
    fn column_chunk_stats_merge() {
        let mut a = ColumnChunkStats::default();
        a.update(StorageValue::SignedInt(10));
        a.update(StorageValue::SignedInt(20));

        let mut b = ColumnChunkStats::default();
        b.update(StorageValue::SignedInt(-5));
        b.update(StorageValue::SignedInt(15));

        a.merge(&b);
        assert_eq!(a.min, Some(StorageValue::SignedInt(-5)));
        assert_eq!(a.max, Some(StorageValue::SignedInt(20)));
    }

    #[test]
    fn metadata_defaults() {
        let meta = ColumnChunkMetadata::default();
        assert_eq!(meta.num_values, 0);
        assert_eq!(meta.compression, CompressionType::Uncompressed);
        assert_eq!(meta.page_range, PageRange::EMPTY);
    }

    #[test]
    fn residency_state_default() {
        assert_eq!(ResidencyState::default(), ResidencyState::InMemory);
    }

    #[test]
    fn node_group_format_default() {
        assert_eq!(NodeGroupFormat::default(), NodeGroupFormat::Regular);
    }
}
