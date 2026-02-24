use std::collections::HashSet;

use hashbrown::HashMap;
use smol_str::SmolStr;

use crate::delta::GraphDelta;
use crate::node_key::NodeKey;
use crate::value::DeltaValue;
use crate::vector_clock::VectorClock;

/// A batch of graph deltas from a single source, committed atomically.
///
/// All deltas in a batch are applied atomically (all or nothing).
/// The `timestamp` is used for last-write-wins conflict resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaBatch {
    /// Source identifier: `"file:src/main.rs"`, `"doc:invoice_12345"`, etc.
    pub source: SmolStr,
    /// Timestamp for last-write-wins ordering. Higher wins.
    pub timestamp: u64,
    /// The mutations in this batch.
    pub deltas: Vec<GraphDelta>,
    /// Optional causal ordering clock. `None` for most workloads.
    pub vector_clock: Option<VectorClock>,
}

impl DeltaBatch {
    pub fn new(source: impl Into<SmolStr>, timestamp: u64) -> Self {
        Self {
            source: source.into(),
            timestamp,
            deltas: Vec::new(),
            vector_clock: None,
        }
    }

    pub fn with_vector_clock(
        source: impl Into<SmolStr>,
        timestamp: u64,
        clock: VectorClock,
    ) -> Self {
        Self {
            source: source.into(),
            timestamp,
            deltas: Vec::new(),
            vector_clock: Some(clock),
        }
    }

    pub fn push(&mut self, delta: GraphDelta) {
        self.deltas.push(delta);
    }

    pub fn extend(&mut self, deltas: impl IntoIterator<Item = GraphDelta>) {
        self.deltas.extend(deltas);
    }

    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &GraphDelta> {
        self.deltas.iter()
    }

    pub fn node_upsert_count(&self) -> usize {
        self.deltas
            .iter()
            .filter(|d| matches!(d, GraphDelta::UpsertNode { .. }))
            .count()
    }

    pub fn edge_upsert_count(&self) -> usize {
        self.deltas
            .iter()
            .filter(|d| matches!(d, GraphDelta::UpsertEdge { .. }))
            .count()
    }

    pub fn delete_count(&self) -> usize {
        self.deltas.iter().filter(|d| d.is_delete()).count()
    }

    /// Collect all unique node table labels referenced in this batch.
    pub fn referenced_labels(&self) -> Vec<SmolStr> {
        let mut labels = HashSet::new();
        for delta in &self.deltas {
            for key in delta.referenced_keys() {
                labels.insert(key.label.clone());
            }
        }
        labels.into_iter().collect()
    }

    /// Collect all unique relationship types referenced in this batch.
    pub fn referenced_rel_types(&self) -> Vec<SmolStr> {
        let mut types = HashSet::new();
        for delta in &self.deltas {
            match delta {
                GraphDelta::UpsertEdge { rel_type, .. }
                | GraphDelta::DeleteEdge { rel_type, .. } => {
                    types.insert(rel_type.clone());
                }
                _ => {}
            }
        }
        types.into_iter().collect()
    }
}

/// Builder for ergonomic `DeltaBatch` construction.
pub struct DeltaBatchBuilder {
    batch: DeltaBatch,
}

impl DeltaBatchBuilder {
    pub fn new(source: impl Into<SmolStr>, timestamp: u64) -> Self {
        Self {
            batch: DeltaBatch::new(source, timestamp),
        }
    }

    pub fn upsert_node(
        mut self,
        label: impl Into<SmolStr>,
        primary_key: impl Into<SmolStr>,
        labels: Vec<SmolStr>,
        props: impl IntoIterator<Item = (impl Into<SmolStr>, DeltaValue)>,
    ) -> Self {
        let props_map: HashMap<SmolStr, DeltaValue> =
            props.into_iter().map(|(k, v)| (k.into(), v)).collect();
        self.batch.push(GraphDelta::UpsertNode {
            key: NodeKey::new(label, primary_key),
            labels,
            props: props_map,
        });
        self
    }

    pub fn upsert_edge(
        mut self,
        src_label: impl Into<SmolStr>,
        src_key: impl Into<SmolStr>,
        rel_type: impl Into<SmolStr>,
        dst_label: impl Into<SmolStr>,
        dst_key: impl Into<SmolStr>,
        props: impl IntoIterator<Item = (impl Into<SmolStr>, DeltaValue)>,
    ) -> Self {
        let props_map: HashMap<SmolStr, DeltaValue> =
            props.into_iter().map(|(k, v)| (k.into(), v)).collect();
        self.batch.push(GraphDelta::UpsertEdge {
            src: NodeKey::new(src_label, src_key),
            rel_type: rel_type.into(),
            dst: NodeKey::new(dst_label, dst_key),
            props: props_map,
        });
        self
    }

    pub fn delete_node(
        mut self,
        label: impl Into<SmolStr>,
        primary_key: impl Into<SmolStr>,
    ) -> Self {
        self.batch.push(GraphDelta::DeleteNode {
            key: NodeKey::new(label, primary_key),
        });
        self
    }

    pub fn delete_edge(
        mut self,
        src_label: impl Into<SmolStr>,
        src_key: impl Into<SmolStr>,
        rel_type: impl Into<SmolStr>,
        dst_label: impl Into<SmolStr>,
        dst_key: impl Into<SmolStr>,
    ) -> Self {
        self.batch.push(GraphDelta::DeleteEdge {
            src: NodeKey::new(src_label, src_key),
            rel_type: rel_type.into(),
            dst: NodeKey::new(dst_label, dst_key),
        });
        self
    }

    pub fn build(self) -> DeltaBatch {
        self.batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_batch_is_empty() {
        let b = DeltaBatch::new("test", 1);
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert!(b.vector_clock.is_none());
    }

    #[test]
    fn push_delta() {
        let mut b = DeltaBatch::new("test", 1);
        b.push(GraphDelta::DeleteNode {
            key: NodeKey::new("A", "1"),
        });
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn extend_deltas() {
        let mut b = DeltaBatch::new("test", 1);
        let deltas = vec![
            GraphDelta::DeleteNode {
                key: NodeKey::new("A", "1"),
            },
            GraphDelta::DeleteNode {
                key: NodeKey::new("A", "2"),
            },
        ];
        b.extend(deltas);
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn len_and_is_empty() {
        let mut b = DeltaBatch::new("test", 1);
        assert!(b.is_empty());
        b.push(GraphDelta::DeleteNode {
            key: NodeKey::new("A", "1"),
        });
        assert!(!b.is_empty());
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn node_upsert_count() {
        let batch = DeltaBatchBuilder::new("test", 1)
            .upsert_node(
                "F",
                "a",
                vec![],
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_node(
                "F",
                "b",
                vec![],
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_edge(
                "F",
                "a",
                "calls",
                "F",
                "b",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        assert_eq!(batch.node_upsert_count(), 2);
    }

    #[test]
    fn edge_upsert_count() {
        let batch = DeltaBatchBuilder::new("test", 1)
            .upsert_node(
                "F",
                "a",
                vec![],
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_edge(
                "F",
                "a",
                "calls",
                "F",
                "b",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_edge(
                "F",
                "b",
                "calls",
                "F",
                "c",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        assert_eq!(batch.edge_upsert_count(), 2);
    }

    #[test]
    fn delete_count() {
        let batch = DeltaBatchBuilder::new("test", 1)
            .delete_node("F", "old")
            .delete_edge("F", "a", "calls", "F", "old")
            .upsert_node(
                "F",
                "new",
                vec![],
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        assert_eq!(batch.delete_count(), 2);
    }

    #[test]
    fn referenced_labels() {
        let batch = DeltaBatchBuilder::new("test", 1)
            .upsert_node(
                "Function",
                "main",
                vec![],
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_edge(
                "Function",
                "main",
                "calls",
                "File",
                "lib.rs",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        let mut labels = batch.referenced_labels();
        labels.sort();
        assert_eq!(labels, vec![SmolStr::new("File"), SmolStr::new("Function")]);
    }

    #[test]
    fn referenced_rel_types() {
        let batch = DeltaBatchBuilder::new("test", 1)
            .upsert_edge(
                "F",
                "a",
                "calls",
                "F",
                "b",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .upsert_edge(
                "F",
                "a",
                "imports",
                "M",
                "x",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        let mut types = batch.referenced_rel_types();
        types.sort();
        assert_eq!(types, vec![SmolStr::new("calls"), SmolStr::new("imports")]);
    }

    #[test]
    fn builder_upsert_node() {
        let batch = DeltaBatchBuilder::new("src", 100)
            .upsert_node(
                "Function",
                "main",
                vec![],
                [("lines", DeltaValue::Int64(42))],
            )
            .build();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.source, "src");
        assert_eq!(batch.timestamp, 100);
        match &batch.deltas[0] {
            GraphDelta::UpsertNode { key, props, .. } => {
                assert_eq!(key.label, "Function");
                assert_eq!(props.get("lines"), Some(&DeltaValue::Int64(42)));
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn builder_upsert_edge() {
        let batch = DeltaBatchBuilder::new("src", 100)
            .upsert_edge(
                "F",
                "a",
                "calls",
                "F",
                "b",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .build();
        assert_eq!(batch.len(), 1);
        match &batch.deltas[0] {
            GraphDelta::UpsertEdge {
                src, rel_type, dst, ..
            } => {
                assert_eq!(src.primary_key, "a");
                assert_eq!(*rel_type, "calls");
                assert_eq!(dst.primary_key, "b");
            }
            _ => panic!("expected UpsertEdge"),
        }
    }

    #[test]
    fn builder_delete_node() {
        let batch = DeltaBatchBuilder::new("src", 1)
            .delete_node("Function", "old")
            .build();
        assert_eq!(batch.len(), 1);
        assert!(batch.deltas[0].is_delete());
    }

    #[test]
    fn builder_delete_edge() {
        let batch = DeltaBatchBuilder::new("src", 1)
            .delete_edge("F", "a", "calls", "F", "b")
            .build();
        assert_eq!(batch.len(), 1);
        assert!(batch.deltas[0].is_delete());
        assert!(batch.deltas[0].is_edge_op());
    }

    #[test]
    fn builder_chained_operations() {
        let batch = DeltaBatchBuilder::new("file:src/main.rs", 1000)
            .upsert_node(
                "Function",
                "main",
                vec![SmolStr::new("Public")],
                [("lines", DeltaValue::Int64(42))],
            )
            .upsert_node(
                "Function",
                "helper",
                vec![],
                [("lines", DeltaValue::Int64(10))],
            )
            .upsert_edge(
                "Function",
                "main",
                "calls",
                "Function",
                "helper",
                std::iter::empty::<(SmolStr, DeltaValue)>(),
            )
            .delete_node("Function", "old_func")
            .build();

        assert_eq!(batch.len(), 4);
        assert_eq!(batch.node_upsert_count(), 2);
        assert_eq!(batch.edge_upsert_count(), 1);
        assert_eq!(batch.delete_count(), 1);
    }

    #[test]
    fn with_vector_clock() {
        let mut vc = VectorClock::new();
        vc.set("w1", 5);
        let batch = DeltaBatch::with_vector_clock("src", 100, vc.clone());
        assert!(batch.vector_clock.is_some());
        assert_eq!(batch.vector_clock.unwrap().get("w1"), 5);
    }
}
