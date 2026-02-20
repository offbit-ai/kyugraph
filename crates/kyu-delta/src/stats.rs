/// Statistics returned after processing a `DeltaBatch`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaStats {
    pub nodes_created: u64,
    pub nodes_updated: u64,
    pub nodes_deleted: u64,
    pub edges_created: u64,
    pub edges_updated: u64,
    pub edges_deleted: u64,
    pub total_deltas: u64,
    pub elapsed_micros: u64,
}

impl DeltaStats {
    pub fn total_mutations(&self) -> u64 {
        self.nodes_created
            + self.nodes_updated
            + self.nodes_deleted
            + self.edges_created
            + self.edges_updated
            + self.edges_deleted
    }

    /// Merge stats from another batch (additive).
    pub fn merge(&mut self, other: &DeltaStats) {
        self.nodes_created += other.nodes_created;
        self.nodes_updated += other.nodes_updated;
        self.nodes_deleted += other.nodes_deleted;
        self.edges_created += other.edges_created;
        self.edges_updated += other.edges_updated;
        self.edges_deleted += other.edges_deleted;
        self.total_deltas += other.total_deltas;
        self.elapsed_micros += other.elapsed_micros;
    }
}

impl std::fmt::Display for DeltaStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "nodes: +{}/~{}/{}, edges: +{}/~{}/{}, total: {}, {}us",
            self.nodes_created,
            self.nodes_updated,
            self.nodes_deleted,
            self.edges_created,
            self.edges_updated,
            self.edges_deleted,
            self.total_deltas,
            self.elapsed_micros,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero() {
        let s = DeltaStats::default();
        assert_eq!(s.total_mutations(), 0);
        assert_eq!(s.total_deltas, 0);
    }

    #[test]
    fn total_mutations() {
        let s = DeltaStats {
            nodes_created: 3,
            nodes_updated: 2,
            nodes_deleted: 1,
            edges_created: 5,
            edges_updated: 0,
            edges_deleted: 1,
            total_deltas: 12,
            elapsed_micros: 100,
        };
        assert_eq!(s.total_mutations(), 12);
    }

    #[test]
    fn merge_additive() {
        let mut a = DeltaStats {
            nodes_created: 3,
            ..Default::default()
        };
        let b = DeltaStats {
            nodes_created: 2,
            edges_created: 5,
            ..Default::default()
        };
        a.merge(&b);
        assert_eq!(a.nodes_created, 5);
        assert_eq!(a.edges_created, 5);
    }

    #[test]
    fn display_format() {
        let s = DeltaStats {
            nodes_created: 1,
            edges_created: 2,
            total_deltas: 3,
            elapsed_micros: 50,
            ..Default::default()
        };
        let d = format!("{s}");
        assert!(d.contains("nodes: +1"));
        assert!(d.contains("edges: +2"));
        assert!(d.contains("50us"));
    }

    #[test]
    fn total_deltas_counted() {
        let s = DeltaStats {
            total_deltas: 42,
            ..Default::default()
        };
        assert_eq!(s.total_deltas, 42);
    }
}
