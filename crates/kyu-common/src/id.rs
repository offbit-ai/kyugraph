/// Internal row identifier within the engine.
/// Matches the C++ `internalID_t` layout: (table_id, offset).
/// Total size: 16 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(C)]
pub struct InternalId {
    pub table_id: u64,
    pub offset: u64,
}

impl InternalId {
    pub const INVALID: Self = Self {
        table_id: u64::MAX,
        offset: u64::MAX,
    };

    pub const fn new(table_id: u64, offset: u64) -> Self {
        Self { table_id, offset }
    }

    pub const fn is_valid(&self) -> bool {
        self.table_id != u64::MAX
    }
}

impl std::fmt::Display for InternalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_id, self.offset)
    }
}

/// Table identifier in the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(pub u64);

/// Property identifier within a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PropertyId(pub u32);

/// Column identifier within a node group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

/// Transaction timestamp / sequence number.
pub type TxnTs = u64;

/// Write-ahead log sequence number.
pub type Lsn = u64;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn internal_id_invalid() {
        assert!(!InternalId::INVALID.is_valid());
    }

    #[test]
    fn internal_id_valid() {
        let id = InternalId::new(0, 0);
        assert!(id.is_valid());
    }

    #[test]
    fn internal_id_display() {
        let id = InternalId::new(3, 42);
        assert_eq!(id.to_string(), "3:42");
    }

    #[test]
    fn internal_id_ordering() {
        let a = InternalId::new(1, 10);
        let b = InternalId::new(1, 20);
        let c = InternalId::new(2, 5);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn internal_id_hash() {
        let mut set = HashSet::new();
        set.insert(InternalId::new(1, 1));
        set.insert(InternalId::new(1, 1));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn internal_id_size() {
        assert_eq!(std::mem::size_of::<InternalId>(), 16);
    }

    #[test]
    fn newtype_ids() {
        let t1 = TableId(1);
        let t2 = TableId(2);
        assert_ne!(t1, t2);
        assert!(t1 < t2);

        let p = PropertyId(0);
        assert_eq!(p.0, 0);

        let c = ColumnId(5);
        assert_eq!(c.0, 5);
    }
}
