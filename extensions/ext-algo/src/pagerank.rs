//! PageRank — iterative power iteration on a graph.
//!
//! Converts the input adjacency HashMap to a CSR (Compressed Sparse Row)
//! layout for cache-friendly iteration. Dense `Vec<f64>` arrays enable
//! LLVM autovectorization of the convergence check and dangling-node
//! aggregation on aarch64 NEON / x86 AVX.

use std::collections::HashMap;

/// Compressed Sparse Row graph — cache-friendly adjacency layout.
///
/// All node IDs are remapped to dense `[0..n)` indices. Neighbors of node `i`
/// are `targets[offsets[i]..offsets[i+1]]`. Uses `u32` indices to halve cache
/// footprint vs `usize` (sufficient for graphs up to ~4B nodes).
struct CsrGraph {
    offsets: Vec<u32>,
    targets: Vec<u32>,
    node_ids: Vec<i64>,
    num_nodes: usize,
}

impl CsrGraph {
    fn from_adjacency(adjacency: &HashMap<i64, Vec<(i64, f64)>>) -> Self {
        // Collect all unique node IDs.
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
            return Self {
                offsets: vec![0],
                targets: Vec::new(),
                node_ids: Vec::new(),
                num_nodes: 0,
            };
        }

        let id_to_idx: HashMap<i64, u32> = all_nodes
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();

        // Count out-degrees → prefix sum → offsets.
        let mut offsets = vec![0u32; n + 1];
        for (&src, neighbors) in adjacency {
            let si = id_to_idx[&src] as usize;
            offsets[si + 1] = neighbors.len() as u32;
        }
        for i in 1..=n {
            offsets[i] += offsets[i - 1];
        }

        // Fill targets.
        let total_edges = offsets[n] as usize;
        let mut targets = vec![0u32; total_edges];
        let mut cursors: Vec<u32> = offsets[..n].to_vec();
        for (&src, neighbors) in adjacency {
            let si = id_to_idx[&src] as usize;
            for &(dst, _) in neighbors {
                let pos = cursors[si] as usize;
                targets[pos] = id_to_idx[&dst];
                cursors[si] += 1;
            }
        }

        Self {
            offsets,
            targets,
            node_ids: all_nodes,
            num_nodes: n,
        }
    }
}

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
    let csr = CsrGraph::from_adjacency(adjacency);
    let n = csr.num_nodes;
    if n == 0 {
        return HashMap::new();
    }

    let n_f64 = n as f64;
    let base = (1.0 - damping) / n_f64;
    let init_rank = 1.0 / n_f64;

    // Precompute damping/degree (avoids f64 division in hot loop)
    // and dangling mask (enables branchless autovectorized reduction).
    let mut inv_degree = vec![0.0_f64; n];
    let mut dangling_mask = vec![0.0_f64; n];
    for i in 0..n {
        let deg = (csr.offsets[i + 1] - csr.offsets[i]) as f64;
        if deg > 0.0 {
            inv_degree[i] = damping / deg;
        } else {
            dangling_mask[i] = 1.0;
        }
    }

    let mut ranks = vec![init_rank; n];
    let mut new_ranks = vec![0.0_f64; n];

    for _ in 0..max_iterations {
        // Dangling sum: branchless multiply-accumulate (autovectorized).
        let dangling_sum: f64 = dangling_mask
            .iter()
            .zip(ranks.iter())
            .map(|(&m, &r)| m * r)
            .sum();
        let teleport = base + damping * dangling_sum / n_f64;

        // Fill with combined teleport + dangling share (vectorized memset).
        new_ranks.fill(teleport);

        // Rank distribution via CSR — no HashMap lookups.
        for src in 0..n {
            let start = csr.offsets[src] as usize;
            let end = csr.offsets[src + 1] as usize;
            if start == end {
                continue;
            }
            let share = ranks[src] * inv_degree[src];
            let targets_slice = &csr.targets[start..end];
            for &dst in targets_slice {
                new_ranks[dst as usize] += share;
            }
        }

        // Convergence check: L1 norm (autovectorized).
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
    csr.node_ids
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

    #[test]
    fn csr_construction() {
        let mut adj = HashMap::new();
        adj.insert(1, vec![(2, 1.0), (3, 1.0)]);
        adj.insert(2, vec![(3, 1.0)]);
        adj.insert(3, vec![]);
        let csr = CsrGraph::from_adjacency(&adj);
        assert_eq!(csr.num_nodes, 3);
        assert_eq!(csr.node_ids, vec![1, 2, 3]);
        // Node 0 (id=1) has 2 neighbors, node 1 (id=2) has 1, node 2 (id=3) has 0.
        assert_eq!(csr.offsets[1] - csr.offsets[0], 2);
        assert_eq!(csr.offsets[2] - csr.offsets[1], 1);
        assert_eq!(csr.offsets[3] - csr.offsets[2], 0);
        assert_eq!(csr.targets.len(), 3);
    }
}
