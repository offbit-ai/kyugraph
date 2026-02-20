use smol_str::SmolStr;

/// A stable, user-defined identifier for a node.
///
/// Unlike `InternalId` (storage-internal, may change across compaction),
/// a `NodeKey` is the external handle that ingestion pipelines and agents
/// use to refer to entities.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeKey {
    /// The node table (label) this key belongs to.
    pub label: SmolStr,
    /// The primary key value as a string.
    pub primary_key: SmolStr,
}

impl NodeKey {
    pub fn new(label: impl Into<SmolStr>, primary_key: impl Into<SmolStr>) -> Self {
        Self {
            label: label.into(),
            primary_key: primary_key.into(),
        }
    }
}

impl std::fmt::Display for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.label, self.primary_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_basic() {
        let key = NodeKey::new("Function", "main");
        assert_eq!(key.label, "Function");
        assert_eq!(key.primary_key, "main");
    }

    #[test]
    fn display_format() {
        let key = NodeKey::new("Document", "invoice_123");
        assert_eq!(format!("{key}"), "Document:invoice_123");
    }

    #[test]
    fn equality() {
        let a = NodeKey::new("Function", "main");
        let b = NodeKey::new("Function", "main");
        assert_eq!(a, b);
    }

    #[test]
    fn different_labels_not_equal() {
        let a = NodeKey::new("Function", "main");
        let b = NodeKey::new("File", "main");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = NodeKey::new("X", "y");
        let b = NodeKey::new("X", "y");
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn clone() {
        let a = NodeKey::new("A", "b");
        let b = a.clone();
        assert_eq!(a, b);
    }
}
