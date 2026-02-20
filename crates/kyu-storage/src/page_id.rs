/// Size of a single database page in bytes (4 KiB).
pub const PAGE_SIZE: usize = 4096;

/// File identifier within the storage layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId(pub u32);

/// A page identifier: (file_id, page_idx) uniquely identifies a page on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageId {
    pub file_id: FileId,
    pub page_idx: u32,
}

impl PageId {
    pub const fn new(file_id: FileId, page_idx: u32) -> Self {
        Self { file_id, page_idx }
    }

    /// Encode as a u64 for atomic storage: high 32 bits = file_id, low 32 bits = page_idx.
    pub const fn to_u64(self) -> u64 {
        ((self.file_id.0 as u64) << 32) | (self.page_idx as u64)
    }

    /// Decode from a u64.
    pub const fn from_u64(val: u64) -> Self {
        Self {
            file_id: FileId((val >> 32) as u32),
            page_idx: val as u32,
        }
    }

    /// Sentinel value for "no page loaded".
    pub const INVALID: Self = Self {
        file_id: FileId(u32::MAX),
        page_idx: u32::MAX,
    };

    pub const fn is_valid(self) -> bool {
        self.file_id.0 != u32::MAX
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file_id.0, self.page_idx)
    }
}

/// Index of a frame within a pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FrameIdx(pub u32);

/// Identifies which pool a frame belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PoolId {
    Read,
    Write,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_roundtrip() {
        let pid = PageId::new(FileId(42), 1000);
        let encoded = pid.to_u64();
        let decoded = PageId::from_u64(encoded);
        assert_eq!(pid, decoded);
    }

    #[test]
    fn page_id_invalid() {
        assert!(!PageId::INVALID.is_valid());
        assert!(PageId::new(FileId(0), 0).is_valid());
    }

    #[test]
    fn page_id_display() {
        let pid = PageId::new(FileId(3), 42);
        assert_eq!(pid.to_string(), "3:42");
    }

    #[test]
    fn page_id_encoding() {
        let pid = PageId::new(FileId(1), 2);
        let val = pid.to_u64();
        assert_eq!(val, (1u64 << 32) | 2);
    }

    #[test]
    fn page_size() {
        assert_eq!(PAGE_SIZE, 4096);
    }

    #[test]
    fn pool_id_eq() {
        assert_eq!(PoolId::Read, PoolId::Read);
        assert_ne!(PoolId::Read, PoolId::Write);
    }

    #[test]
    fn frame_idx_eq() {
        assert_eq!(FrameIdx(0), FrameIdx(0));
        assert_ne!(FrameIdx(0), FrameIdx(1));
    }
}
