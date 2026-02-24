//! Fruchterman-Reingold force-directed graph layout.
//!
//! O(n²) per iteration — acceptable for MVP with ≤2000 nodes.
//! Runs a few steps per frame; pinned nodes are excluded from displacement.

use crate::state::{GraphData, Vec2};

/// Configuration for the layout algorithm.
pub struct LayoutConfig {
    /// Width × Height of the layout area.
    pub area: f32,
    /// Cooling factor per step (0.95 typical).
    pub cooling: f32,
}

impl Default for LayoutConfig {
    fn default() -> Self {
        Self {
            area: 200_000.0, // ~450 × 450, fits within typical canvas bounds
            cooling: 0.95,
        }
    }
}

/// Run one iteration of Fruchterman-Reingold layout.
///
/// Returns `true` if the temperature is still above threshold (keep running).
pub fn layout_step(graph: &mut GraphData, temperature: &mut f32, config: &LayoutConfig) -> bool {
    let n = graph.nodes.len();
    if n == 0 {
        return false;
    }

    let k = (config.area / n as f32).sqrt();
    let k_sq = k * k;

    // Accumulate displacements.
    let mut disp: Vec<Vec2> = vec![Vec2::default(); n];

    // 1. Repulsive forces: all pairs push apart.
    for i in 0..n {
        for j in (i + 1)..n {
            let delta = graph.nodes[i].pos - graph.nodes[j].pos;
            let dist_sq = delta.x * delta.x + delta.y * delta.y;
            let dist = dist_sq.sqrt().max(0.01);
            // Repulsive force: k² / dist
            let force = k_sq / dist;
            let dir = delta * (force / dist);
            disp[i] += dir;
            disp[j] = disp[j] - dir;
        }
    }

    // 2. Attractive forces: edges pull connected nodes together.
    for edge in &graph.edges {
        let delta = graph.nodes[edge.src].pos - graph.nodes[edge.dst].pos;
        let dist = delta.length().max(0.01);
        // Attractive force: dist² / k
        let force = dist * dist / k;
        let dir = delta * (force / dist);
        disp[edge.src] = disp[edge.src] - dir;
        disp[edge.dst] += dir;
    }

    // 3. Apply displacement capped by temperature.
    for (i, d) in disp.iter().enumerate() {
        if graph.nodes[i].pinned {
            continue;
        }
        let mag = d.length().max(0.01);
        let capped = mag.min(*temperature);
        graph.nodes[i].pos += *d * (capped / mag);
    }

    // 4. Cool temperature.
    *temperature *= config.cooling;

    *temperature > 0.1
}

/// Reset layout: randomize positions and set temperature.
pub fn reset_temperature(node_count: usize) -> f32 {
    // Initial temperature proportional to sqrt of area.
    (200_000.0_f32 / node_count.max(1) as f32).sqrt() * 0.5
}

/// Run multiple layout steps (for per-frame batch).
pub fn layout_batch(graph: &mut GraphData, temperature: &mut f32, steps: usize) -> bool {
    let config = LayoutConfig::default();
    let mut running = true;
    for _ in 0..steps {
        running = layout_step(graph, temperature, &config);
        if !running {
            break;
        }
    }
    running
}

/// Center the graph at the origin after layout has converged.
pub fn center_graph(graph: &mut GraphData) {
    if graph.nodes.is_empty() {
        return;
    }
    let n = graph.nodes.len() as f32;
    let cx: f32 = graph.nodes.iter().map(|n| n.pos.x).sum::<f32>() / n;
    let cy: f32 = graph.nodes.iter().map(|n| n.pos.y).sum::<f32>() / n;
    for node in &mut graph.nodes {
        node.pos.x -= cx;
        node.pos.y -= cy;
    }
}
