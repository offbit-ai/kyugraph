//! Weakly Connected Components — Union-Find with path compression and rank.

use std::collections::HashMap;

/// Union-Find data structure for WCC.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // path halving
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        // Union by rank.
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

/// Compute weakly connected components of an undirected graph.
///
/// - `adjacency`: node_id -> [(neighbor_id, weight)] — edges (treated as undirected)
///
/// Returns: node_id -> component_id (the component id is the smallest node_id in the component).
pub fn wcc(adjacency: &HashMap<i64, Vec<(i64, f64)>>) -> HashMap<i64, i64> {
    // Collect all node IDs.
    let mut all_nodes: Vec<i64> = Vec::new();
    for (&src, neighbors) in adjacency {
        all_nodes.push(src);
        for &(dst, _) in neighbors {
            all_nodes.push(dst);
        }
    }
    all_nodes.sort_unstable();
    all_nodes.dedup();

    let n = all_nodes.len();
    if n == 0 {
        return HashMap::new();
    }

    let id_to_idx: HashMap<i64, usize> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();

    let mut uf = UnionFind::new(n);

    // Union all edges.
    for (&src, neighbors) in adjacency {
        let src_idx = id_to_idx[&src];
        for &(dst, _) in neighbors {
            let dst_idx = id_to_idx[&dst];
            uf.union(src_idx, dst_idx);
        }
    }

    // Map each node to its component's representative node_id.
    // Collect all roots, then map each root index to the minimum node_id in that component.
    let mut root_to_min: HashMap<usize, i64> = HashMap::new();
    for (idx, &node_id) in all_nodes.iter().enumerate() {
        let root = uf.find(idx);
        let entry = root_to_min.entry(root).or_insert(node_id);
        if node_id < *entry {
            *entry = node_id;
        }
    }

    all_nodes
        .iter()
        .enumerate()
        .map(|(idx, &node_id)| {
            let root = uf.find(idx);
            (node_id, root_to_min[&root])
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_graph() {
        let adj = HashMap::new();
        let result = wcc(&adj);
        assert!(result.is_empty());
    }

    #[test]
    fn single_node() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![]);
        let result = wcc(&adj);
        assert_eq!(result.len(), 1);
        assert_eq!(result[&1], 1);
    }

    #[test]
    fn two_components() {
        let mut adj = HashMap::new();
        // Component 1: 1-2-3
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(1, 1.0), (3, 1.0)]);
        adj.insert(3, vec![(2, 1.0)]);
        // Component 2: 10-11
        adj.insert(10, vec![(11, 1.0)]);
        adj.insert(11, vec![(10, 1.0)]);
        let result = wcc(&adj);
        assert_eq!(result.len(), 5);
        // Nodes 1,2,3 should share a component.
        assert_eq!(result[&1], result[&2]);
        assert_eq!(result[&2], result[&3]);
        // Nodes 10,11 should share a different component.
        assert_eq!(result[&10], result[&11]);
        // The two components should be different.
        assert_ne!(result[&1], result[&10]);
    }

    #[test]
    fn all_connected() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![(4, 1.0)]);
        adj.insert(4, vec![]);
        let result = wcc(&adj);
        // All in one component.
        let comp = result[&1];
        assert!(result.values().all(|&c| c == comp));
    }

    #[test]
    fn isolated_nodes() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![]);
        adj.insert(2, vec![]);
        adj.insert(3, vec![]);
        let result = wcc(&adj);
        // Each node is its own component.
        assert_eq!(result[&1], 1);
        assert_eq!(result[&2], 2);
        assert_eq!(result[&3], 3);
    }

    #[test]
    fn component_id_is_minimum() {
        let mut adj = HashMap::new();
        adj.insert(5, vec![(10, 1.0)]);
        adj.insert(10, vec![(5, 1.0), (3, 1.0)]);
        adj.insert(3, vec![(10, 1.0)]);
        let result = wcc(&adj);
        // Minimum node_id in {3,5,10} is 3.
        assert_eq!(result[&3], 3);
        assert_eq!(result[&5], 3);
        assert_eq!(result[&10], 3);
    }
}
