//! Betweenness Centrality — Brandes algorithm.
//!
//! Computes the betweenness centrality of each node: the fraction of
//! shortest paths between all pairs that pass through the node.

use std::collections::{HashMap, VecDeque};

/// Compute betweenness centrality for all nodes.
///
/// Uses Brandes' algorithm: BFS from each source, then back-propagate
/// dependency scores along shortest paths.
///
/// - `adjacency`: node_id -> [(neighbor_id, weight)] — edges (weights ignored for unweighted BFS)
///
/// Returns: node_id -> betweenness score (unnormalized).
pub fn betweenness_centrality(adjacency: &HashMap<i64, Vec<(i64, f64)>>) -> HashMap<i64, f64> {
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
    let id_to_idx: HashMap<i64, usize> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();

    // Build index-based adjacency list for fast access.
    let mut adj_idx: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (&src, neighbors) in adjacency {
        let si = id_to_idx[&src];
        for &(dst, _) in neighbors {
            let di = id_to_idx[&dst];
            adj_idx[si].push(di);
        }
    }

    let mut centrality = vec![0.0_f64; n];

    // Brandes: BFS from each source.
    for s in 0..n {
        let mut stack: Vec<usize> = Vec::new();
        let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut sigma = vec![0.0_f64; n]; // number of shortest paths
        sigma[s] = 1.0;
        let mut dist = vec![-1_i64; n];
        dist[s] = 0;
        let mut queue = VecDeque::new();
        queue.push_back(s);

        // BFS phase.
        while let Some(v) = queue.pop_front() {
            stack.push(v);
            for &w in &adj_idx[v] {
                // First visit?
                if dist[w] < 0 {
                    dist[w] = dist[v] + 1;
                    queue.push_back(w);
                }
                // Is this a shortest path to w?
                if dist[w] == dist[v] + 1 {
                    sigma[w] += sigma[v];
                    predecessors[w].push(v);
                }
            }
        }

        // Back-propagation phase.
        let mut delta = vec![0.0_f64; n];
        while let Some(w) = stack.pop() {
            for &v in &predecessors[w] {
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            }
            if w != s {
                centrality[w] += delta[w];
            }
        }
    }

    all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, centrality[i]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_graph() {
        let adj = HashMap::new();
        let result = betweenness_centrality(&adj);
        assert!(result.is_empty());
    }

    #[test]
    fn single_node() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![]);
        let result = betweenness_centrality(&adj);
        assert_eq!(result.len(), 1);
        assert!((result[&1] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn linear_chain() {
        // 1 -> 2 -> 3 -> 4 (directed)
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![(4, 1.0)]);
        adj.insert(4, vec![]);
        let result = betweenness_centrality(&adj);
        // Node 2 is on shortest paths from 1->3 and 1->4.
        // Node 3 is on shortest path from 1->4 and 2->4.
        assert!(result[&2] > 0.0);
        assert!(result[&3] > 0.0);
        // Endpoints have 0 betweenness.
        assert!((result[&1] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn star_graph_undirected() {
        // Center node 1, spokes to 2,3,4,5.
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0), (3, 1.0), (4, 1.0), (5, 1.0)]);
        adj.insert(2, vec![(1, 1.0)]);
        adj.insert(3, vec![(1, 1.0)]);
        adj.insert(4, vec![(1, 1.0)]);
        adj.insert(5, vec![(1, 1.0)]);
        let result = betweenness_centrality(&adj);
        // Center node should have highest betweenness.
        assert!(result[&1] > result[&2]);
        assert!(result[&1] > result[&3]);
    }

    #[test]
    fn triangle_undirected() {
        // Fully connected triangle: 1-2-3.
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0), (3, 1.0)]);
        adj.insert(2, vec![(1, 1.0), (3, 1.0)]);
        adj.insert(3, vec![(1, 1.0), (2, 1.0)]);
        let result = betweenness_centrality(&adj);
        // All nodes should have equal betweenness (0 in a complete graph).
        assert!((result[&1] - result[&2]).abs() < 1e-10);
        assert!((result[&2] - result[&3]).abs() < 1e-10);
    }
}
