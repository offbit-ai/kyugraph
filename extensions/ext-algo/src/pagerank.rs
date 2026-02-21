//! PageRank — iterative power iteration on a weighted adjacency graph.

use std::collections::HashMap;

/// Compute PageRank scores for all nodes in the graph.
///
/// - `adjacency`: node_id -> [(neighbor_id, weight)] — outgoing edges
/// - `damping`: damping factor (typically 0.85)
/// - `max_iterations`: maximum number of iterations
/// - `tolerance`: convergence threshold (L1 norm of rank change)
///
/// Returns: node_id -> rank.
pub fn pagerank(
    adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    damping: f64,
    max_iterations: u32,
    tolerance: f64,
) -> HashMap<i64, f64> {
    // Collect all node IDs (both sources and destinations).
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

    // Map node_id -> index for O(1) lookup.
    let id_to_idx: HashMap<i64, usize> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();

    // Initialize ranks uniformly.
    let init_rank = 1.0 / n as f64;
    let mut ranks = vec![init_rank; n];
    let mut new_ranks = vec![0.0; n];

    // Precompute out-degree for each node.
    let mut out_degree = vec![0.0_f64; n];
    for (&src, neighbors) in adjacency {
        let src_idx = id_to_idx[&src];
        out_degree[src_idx] = neighbors.len() as f64;
    }

    let base = (1.0 - damping) / n as f64;

    for _ in 0..max_iterations {
        // Reset new ranks with base teleportation score.
        new_ranks.fill(base);

        // Distribute rank from each node to its neighbors.
        for (&src, neighbors) in adjacency {
            let src_idx = id_to_idx[&src];
            let degree = out_degree[src_idx];
            if degree == 0.0 {
                continue;
            }
            let share = damping * ranks[src_idx] / degree;
            for &(dst, _) in neighbors {
                let dst_idx = id_to_idx[&dst];
                new_ranks[dst_idx] += share;
            }
        }

        // Handle dangling nodes (no outgoing edges): distribute their rank
        // uniformly across all nodes.
        let mut dangling_sum = 0.0;
        for (idx, &deg) in out_degree.iter().enumerate() {
            if deg == 0.0 {
                dangling_sum += ranks[idx];
            }
        }
        if dangling_sum > 0.0 {
            let dangling_share = damping * dangling_sum / n as f64;
            for r in new_ranks.iter_mut() {
                *r += dangling_share;
            }
        }

        // Check convergence.
        let diff: f64 = ranks
            .iter()
            .zip(new_ranks.iter())
            .map(|(old, new)| (old - new).abs())
            .sum();

        std::mem::swap(&mut ranks, &mut new_ranks);

        if diff < tolerance {
            break;
        }
    }

    // Map back to node_id -> rank.
    all_nodes
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, ranks[i]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_graph() {
        let adj = HashMap::new();
        let result = pagerank(&adj, 0.85, 20, 1e-6);
        assert!(result.is_empty());
    }

    #[test]
    fn single_node() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![]);
        let result = pagerank(&adj, 0.85, 20, 1e-6);
        assert_eq!(result.len(), 1);
        assert!((result[&1] - 1.0).abs() < 1e-4);
    }

    #[test]
    fn two_node_cycle() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(1, 1.0)]);
        let result = pagerank(&adj, 0.85, 100, 1e-8);
        assert_eq!(result.len(), 2);
        // Symmetric graph: both nodes should have equal rank.
        assert!((result[&1] - result[&2]).abs() < 1e-4);
        assert!((result[&1] - 0.5).abs() < 1e-4);
    }

    #[test]
    fn star_graph() {
        // Node 1 is central, nodes 2-5 point to 1.
        let mut adj = HashMap::new();
        adj.insert(2, vec![(1, 1.0)]);
        adj.insert(3, vec![(1, 1.0)]);
        adj.insert(4, vec![(1, 1.0)]);
        adj.insert(5, vec![(1, 1.0)]);
        adj.insert(1, vec![]); // dangling
        let result = pagerank(&adj, 0.85, 100, 1e-8);
        // Central node should have highest rank.
        assert!(result[&1] > result[&2]);
        assert!(result[&1] > result[&3]);
    }

    #[test]
    fn linear_chain() {
        // 1 -> 2 -> 3 -> 4
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![(4, 1.0)]);
        adj.insert(4, vec![]);
        let result = pagerank(&adj, 0.85, 100, 1e-8);
        assert_eq!(result.len(), 4);
        // Node 4 receives rank from the chain; should have decent rank.
        assert!(result.values().all(|&v| v > 0.0));
    }

    #[test]
    fn ranks_sum_to_one() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0), (3, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![(1, 1.0)]);
        let result = pagerank(&adj, 0.85, 100, 1e-8);
        let total: f64 = result.values().sum();
        assert!((total - 1.0).abs() < 1e-4);
    }
}
