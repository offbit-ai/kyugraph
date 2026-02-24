//! HNSW (Hierarchical Navigable Small World) graph index.
//!
//! Implements approximate nearest neighbor search with:
//! - Multi-layer navigable small world graph
//! - Greedy search from entry point through layers
//! - Beam search at target layer for recall
//! - Simple heuristic neighbor selection

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::distance::DistanceMetric;

/// Neighbor entry: (distance, vector_id).
#[derive(Clone, Copy)]
struct Neighbor {
    dist: f32,
    id: usize,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.dist.to_bits() == other.dist.to_bits() && self.id == other.id
    }
}

impl Eq for Neighbor {}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.id.cmp(&other.id))
    }
}

/// HNSW index configuration.
pub struct HnswConfig {
    /// Max connections per layer (default 16).
    pub m: usize,
    /// Max connections at layer 0 (default 2*M = 32).
    pub m_max0: usize,
    /// Beam width during construction (default 200).
    pub ef_construction: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            metric: DistanceMetric::L2,
        }
    }
}

/// HNSW index for approximate nearest neighbor search.
pub struct HnswIndex {
    vectors: Vec<Vec<f32>>,
    /// neighbors[vector_id][layer] = vec of (neighbor_id, distance)
    neighbors: Vec<Vec<Vec<(usize, f32)>>>,
    entry_point: Option<usize>,
    max_layer: usize,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    dim: usize,
    metric: DistanceMetric,
    ml: f64, // 1.0 / ln(M)
    rng_state: u64,
}

impl HnswIndex {
    /// Create a new HNSW index.
    pub fn new(dim: usize, config: HnswConfig) -> Self {
        let ml = 1.0 / (config.m as f64).ln();
        Self {
            vectors: Vec::new(),
            neighbors: Vec::new(),
            entry_point: None,
            max_layer: 0,
            m: config.m,
            m_max0: config.m_max0,
            ef_construction: config.ef_construction,
            dim,
            metric: config.metric,
            ml,
            rng_state: 0x5DEECE66D, // seed
        }
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Insert a vector. Returns its assigned id.
    pub fn insert(&mut self, vector: &[f32]) -> usize {
        assert_eq!(vector.len(), self.dim, "dimension mismatch");

        let id = self.vectors.len();
        self.vectors.push(vector.to_vec());

        let level = self.random_level();

        // Initialize neighbor lists for all layers up to `level`.
        let mut layers = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            layers.push(Vec::new());
        }
        self.neighbors.push(layers);

        if self.entry_point.is_none() {
            // First vector — just set it as entry point.
            self.entry_point = Some(id);
            self.max_layer = level;
            return id;
        }

        let ep = self.entry_point.unwrap();
        let mut current_ep = ep;

        // Phase 1: Greedy descent from top layer to insertion layer + 1.
        for layer in (level + 1..=self.max_layer).rev() {
            current_ep = self.greedy_closest(vector, current_ep, layer);
        }

        // Phase 2: At each layer from min(level, max_layer) down to 0,
        // find neighbors and connect.
        let start_layer = level.min(self.max_layer);
        for layer in (0..=start_layer).rev() {
            let m_for_layer = if layer == 0 { self.m_max0 } else { self.m };

            // Search for ef_construction nearest at this layer.
            let candidates = self.search_layer(vector, current_ep, self.ef_construction, layer);

            // Select M best neighbors.
            let selected: Vec<(usize, f32)> = candidates
                .into_iter()
                .take(m_for_layer)
                .map(|n| (n.id, n.dist))
                .collect();

            // Connect: id -> selected neighbors.
            self.neighbors[id][layer] = selected.clone();

            // Reverse connections: selected neighbors -> id.
            for &(neighbor_id, dist) in &selected {
                if neighbor_id < self.neighbors.len() && layer < self.neighbors[neighbor_id].len() {
                    self.neighbors[neighbor_id][layer].push((id, dist));
                    // Prune if too many connections.
                    if self.neighbors[neighbor_id][layer].len() > m_for_layer {
                        self.neighbors[neighbor_id][layer].sort_by(|a, b| {
                            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        self.neighbors[neighbor_id][layer].truncate(m_for_layer);
                    }
                }
            }

            // Update entry point for next layer.
            if !selected.is_empty() {
                current_ep = selected[0].0;
            }
        }

        // Update entry point if new vector has higher level.
        if level > self.max_layer {
            self.entry_point = Some(id);
            self.max_layer = level;
        }

        id
    }

    /// Search for k approximate nearest neighbors.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(usize, f32)> {
        if self.entry_point.is_none() {
            return Vec::new();
        }

        let mut ep = self.entry_point.unwrap();

        // Greedy descent through upper layers.
        for layer in (1..=self.max_layer).rev() {
            ep = self.greedy_closest(query, ep, layer);
        }

        // Beam search at layer 0.
        let ef = ef.max(k);
        let candidates = self.search_layer(query, ep, ef, 0);

        candidates
            .into_iter()
            .take(k)
            .map(|n| (n.id, n.dist))
            .collect()
    }

    /// Greedy search: find the single closest node to `query` at `layer`.
    fn greedy_closest(&self, query: &[f32], start: usize, layer: usize) -> usize {
        let mut best = start;
        let mut best_dist = self.distance(query, best);

        loop {
            let mut changed = false;
            if layer < self.neighbors[best].len() {
                for &(neighbor, _) in &self.neighbors[best][layer] {
                    let d = self.distance(query, neighbor);
                    if d < best_dist {
                        best_dist = d;
                        best = neighbor;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        best
    }

    /// Beam search at a specific layer. Returns neighbors sorted by distance (nearest first).
    fn search_layer(&self, query: &[f32], ep: usize, ef: usize, layer: usize) -> Vec<Neighbor> {
        let ep_dist = self.distance(query, ep);

        // Candidates: min-heap (nearest first for expansion).
        let mut candidates: BinaryHeap<Reverse<Neighbor>> = BinaryHeap::new();
        // Result set: max-heap (furthest first for pruning).
        let mut result: BinaryHeap<Neighbor> = BinaryHeap::new();
        let mut visited = vec![false; self.vectors.len()];

        let ep_neighbor = Neighbor {
            dist: ep_dist,
            id: ep,
        };
        candidates.push(Reverse(ep_neighbor));
        result.push(ep_neighbor);
        visited[ep] = true;

        while let Some(Reverse(current)) = candidates.pop() {
            // If nearest candidate is further than the furthest result, stop.
            if result.peek().is_some_and(|f| current.dist > f.dist) {
                break;
            }

            // Expand neighbors at this layer.
            if layer < self.neighbors[current.id].len() {
                for &(neighbor_id, _) in &self.neighbors[current.id][layer] {
                    if visited[neighbor_id] {
                        continue;
                    }
                    visited[neighbor_id] = true;

                    let d = self.distance(query, neighbor_id);
                    let n = Neighbor {
                        dist: d,
                        id: neighbor_id,
                    };

                    let should_add = result.len() < ef || result.peek().is_some_and(|f| d < f.dist);

                    if should_add {
                        candidates.push(Reverse(n));
                        result.push(n);
                        if result.len() > ef {
                            result.pop(); // Remove furthest.
                        }
                    }
                }
            }
        }

        // Drain result heap into sorted vec (nearest first).
        let mut sorted: Vec<Neighbor> = result.into_vec();
        sorted.sort();
        sorted
    }

    /// Compute distance between query and indexed vector.
    #[inline]
    fn distance(&self, query: &[f32], id: usize) -> f32 {
        self.metric.distance(query, &self.vectors[id])
    }

    /// Assign a random level for a new vector.
    fn random_level(&mut self) -> usize {
        // xorshift64
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;

        let r = (x as f64) / (u64::MAX as f64);
        let level = (-r.ln() * self.ml) as usize;
        level.min(16) // Cap at 16 layers.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(dim: usize) -> HnswIndex {
        HnswIndex::new(dim, HnswConfig::default())
    }

    #[test]
    fn empty_search() {
        let idx = make_index(3);
        let results = idx.search(&[1.0, 0.0, 0.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn single_vector() {
        let mut idx = make_index(3);
        let id = idx.insert(&[1.0, 2.0, 3.0]);
        assert_eq!(id, 0);

        let results = idx.search(&[1.0, 2.0, 3.0], 1, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        assert!(results[0].1 < 1e-6); // distance ~0
    }

    #[test]
    fn exact_knn_small() {
        let mut idx = make_index(2);

        // Insert 10 known vectors.
        let points: Vec<[f32; 2]> = (0..10).map(|i| [i as f32, 0.0]).collect();

        for p in &points {
            idx.insert(p);
        }

        // Query at [5.0, 0.0] — nearest should be point 5.
        let results = idx.search(&[5.0, 0.0], 3, 50);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 5); // exact nearest
    }

    #[test]
    fn recall_100_vectors() {
        let dim = 16;
        let n = 100;
        let mut idx = HnswIndex::new(
            dim,
            HnswConfig {
                m: 16,
                m_max0: 32,
                ef_construction: 100,
                metric: DistanceMetric::L2,
            },
        );

        // Generate deterministic vectors.
        let vectors: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                (0..dim)
                    .map(|d| ((i * 7 + d * 13) % 100) as f32 / 100.0)
                    .collect()
            })
            .collect();

        for v in &vectors {
            idx.insert(v);
        }

        // Query with first vector, k=10.
        let results = idx.search(&vectors[0], 10, 100);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // should find itself

        // Compute brute-force top-10 for recall check.
        let mut brute: Vec<(usize, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i, DistanceMetric::L2.distance(&vectors[0], v)))
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let brute_top10: Vec<usize> = brute.iter().take(10).map(|r| r.0).collect();

        // Check recall@10.
        let hnsw_top10: Vec<usize> = results.iter().take(10).map(|r| r.0).collect();
        let hits: usize = hnsw_top10
            .iter()
            .filter(|id| brute_top10.contains(id))
            .count();
        let recall = hits as f64 / 10.0;
        assert!(recall >= 0.7, "recall@10 = {recall}, expected >= 0.7");
    }

    #[test]
    fn cosine_metric() {
        let mut idx = HnswIndex::new(
            3,
            HnswConfig {
                metric: DistanceMetric::Cosine,
                ..HnswConfig::default()
            },
        );

        idx.insert(&[1.0, 0.0, 0.0]);
        idx.insert(&[0.0, 1.0, 0.0]);
        idx.insert(&[0.9, 0.1, 0.0]); // close to first

        let results = idx.search(&[1.0, 0.0, 0.0], 3, 50);
        assert_eq!(results.len(), 3);
        // First result should be vector 0 (identical).
        assert_eq!(results[0].0, 0);
        assert!(results[0].1 < 1e-5);
    }

    #[test]
    fn insert_respects_dimension() {
        let mut idx = make_index(4);
        idx.insert(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    #[should_panic(expected = "dimension mismatch")]
    fn dimension_mismatch_panics() {
        let mut idx = make_index(4);
        idx.insert(&[1.0, 2.0]); // wrong dimension
    }

    #[test]
    fn level_distribution() {
        let mut idx = make_index(2);
        let mut max_level = 0;
        for i in 0..1000 {
            idx.insert(&[i as f32, 0.0]);
            max_level = max_level.max(idx.max_layer);
        }
        // With M=16, ml=1/ln(16)≈0.36, expected max level for 1000 vectors ≈ 2-3.
        assert!(max_level <= 8, "max_level = {max_level}, unexpectedly high");
    }
}
