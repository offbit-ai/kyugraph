use hashbrown::HashMap;
use smol_str::SmolStr;

use crate::node_key::NodeKey;
use crate::value::DeltaValue;

/// A single graph mutation in the delta fast path.
///
/// Each variant is idempotent and conflict-free. "Last write wins"
/// semantics: if two deltas target the same entity, the one with the
/// higher batch timestamp takes precedence.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphDelta {
    /// Insert or update a node. Properties are merged (new overwrite existing;
    /// unmentioned properties are kept).
    UpsertNode {
        key: NodeKey,
        labels: Vec<SmolStr>,
        props: HashMap<SmolStr, DeltaValue>,
    },

    /// Insert or update an edge. Properties are merged.
    UpsertEdge {
        src: NodeKey,
        rel_type: SmolStr,
        dst: NodeKey,
        props: HashMap<SmolStr, DeltaValue>,
    },

    /// Delete a node and all its incident edges.
    DeleteNode { key: NodeKey },

    /// Delete a specific edge.
    DeleteEdge {
        src: NodeKey,
        rel_type: SmolStr,
        dst: NodeKey,
    },
}

impl GraphDelta {
    pub fn is_node_op(&self) -> bool {
        matches!(self, Self::UpsertNode { .. } | Self::DeleteNode { .. })
    }

    pub fn is_edge_op(&self) -> bool {
        matches!(self, Self::UpsertEdge { .. } | Self::DeleteEdge { .. })
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::DeleteNode { .. } | Self::DeleteEdge { .. })
    }

    pub fn is_upsert(&self) -> bool {
        matches!(self, Self::UpsertNode { .. } | Self::UpsertEdge { .. })
    }

    /// Returns the primary `NodeKey` involved in this delta.
    /// For edges, returns the source node key.
    pub fn primary_key(&self) -> &NodeKey {
        match self {
            Self::UpsertNode { key, .. } | Self::DeleteNode { key } => key,
            Self::UpsertEdge { src, .. } | Self::DeleteEdge { src, .. } => src,
        }
    }

    /// Returns all `NodeKey`s referenced by this delta.
    pub fn referenced_keys(&self) -> Vec<&NodeKey> {
        match self {
            Self::UpsertNode { key, .. } | Self::DeleteNode { key } => vec![key],
            Self::UpsertEdge { src, dst, .. } | Self::DeleteEdge { src, dst, .. } => {
                vec![src, dst]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_upsert_node() -> GraphDelta {
        GraphDelta::UpsertNode {
            key: NodeKey::new("Function", "main"),
            labels: vec![],
            props: HashMap::new(),
        }
    }

    fn sample_upsert_edge() -> GraphDelta {
        GraphDelta::UpsertEdge {
            src: NodeKey::new("Function", "main"),
            rel_type: SmolStr::new("calls"),
            dst: NodeKey::new("Function", "helper"),
            props: HashMap::new(),
        }
    }

    fn sample_delete_node() -> GraphDelta {
        GraphDelta::DeleteNode {
            key: NodeKey::new("Function", "old"),
        }
    }

    fn sample_delete_edge() -> GraphDelta {
        GraphDelta::DeleteEdge {
            src: NodeKey::new("Function", "main"),
            rel_type: SmolStr::new("calls"),
            dst: NodeKey::new("Function", "removed"),
        }
    }

    #[test]
    fn upsert_node_is_node_op() {
        assert!(sample_upsert_node().is_node_op());
        assert!(!sample_upsert_node().is_edge_op());
    }

    #[test]
    fn upsert_edge_is_edge_op() {
        assert!(sample_upsert_edge().is_edge_op());
        assert!(!sample_upsert_edge().is_node_op());
    }

    #[test]
    fn delete_node_is_delete() {
        assert!(sample_delete_node().is_delete());
        assert!(!sample_delete_node().is_upsert());
    }

    #[test]
    fn delete_edge_is_delete() {
        assert!(sample_delete_edge().is_delete());
        assert!(!sample_delete_edge().is_upsert());
    }

    #[test]
    fn upsert_is_not_delete() {
        assert!(sample_upsert_node().is_upsert());
        assert!(!sample_upsert_node().is_delete());
    }

    #[test]
    fn primary_key_for_node() {
        let d = sample_upsert_node();
        assert_eq!(d.primary_key().label, "Function");
        assert_eq!(d.primary_key().primary_key, "main");
    }

    #[test]
    fn primary_key_for_edge_is_src() {
        let d = sample_upsert_edge();
        assert_eq!(d.primary_key().label, "Function");
        assert_eq!(d.primary_key().primary_key, "main");
    }

    #[test]
    fn referenced_keys_node() {
        let d = sample_upsert_node();
        assert_eq!(d.referenced_keys().len(), 1);
    }

    #[test]
    fn referenced_keys_edge() {
        let d = sample_upsert_edge();
        let keys = d.referenced_keys();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].primary_key, "main");
        assert_eq!(keys[1].primary_key, "helper");
    }

    #[test]
    fn clone_and_eq() {
        let a = sample_upsert_node();
        let b = a.clone();
        assert_eq!(a, b);
    }
}
