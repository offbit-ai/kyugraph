//! Hit testing and coordinate transforms for canvas interaction.

use crate::state::{CameraState, GraphData, Selection, Vec2};
use crate::theme::NODE_RADIUS;

/// Convert screen coordinates to world coordinates.
/// Transform is: translate(center + offset) → scale(zoom) → world
/// So inverse: world = (screen - center - offset) / zoom
pub fn screen_to_world(screen: Vec2, camera: &CameraState, canvas_w: f32, canvas_h: f32) -> Vec2 {
    Vec2 {
        x: (screen.x - canvas_w / 2.0 - camera.offset_x) / camera.zoom,
        y: (screen.y - canvas_h / 2.0 - camera.offset_y) / camera.zoom,
    }
}

/// Convert world coordinates to screen coordinates.
pub fn world_to_screen(world: Vec2, camera: &CameraState, canvas_w: f32, canvas_h: f32) -> Vec2 {
    Vec2 {
        x: world.x * camera.zoom + canvas_w / 2.0 + camera.offset_x,
        y: world.y * camera.zoom + canvas_h / 2.0 + camera.offset_y,
    }
}

/// Hit test: find the nearest node within click radius.
/// Returns the node index if hit.
pub fn hit_test_node(world_pos: Vec2, graph: &GraphData) -> Option<usize> {
    let radius = NODE_RADIUS;
    let mut closest: Option<(usize, f32)> = None;

    for (i, node) in graph.nodes.iter().enumerate() {
        let dx = world_pos.x - node.pos.x;
        let dy = world_pos.y - node.pos.y;
        let dist = (dx * dx + dy * dy).sqrt();
        if dist <= radius {
            match closest {
                None => closest = Some((i, dist)),
                Some((_, best)) if dist < best => closest = Some((i, dist)),
                _ => {}
            }
        }
    }

    closest.map(|(i, _)| i)
}

/// Hit test: find the nearest edge within click tolerance.
/// Returns the edge index if hit.
pub fn hit_test_edge(world_pos: Vec2, graph: &GraphData, tolerance: f32) -> Option<usize> {
    let mut closest: Option<(usize, f32)> = None;

    for (i, edge) in graph.edges.iter().enumerate() {
        let src = graph.nodes[edge.src].pos;
        let dst = graph.nodes[edge.dst].pos;
        let dist = point_to_segment_distance(world_pos, src, dst);
        if dist <= tolerance {
            match closest {
                None => closest = Some((i, dist)),
                Some((_, best)) if dist < best => closest = Some((i, dist)),
                _ => {}
            }
        }
    }

    closest.map(|(i, _)| i)
}

/// Combined hit test: try node first, then edge.
pub fn hit_test(world_pos: Vec2, graph: &GraphData) -> Selection {
    if let Some(idx) = hit_test_node(world_pos, graph) {
        return Selection::Node(idx);
    }
    if let Some(idx) = hit_test_edge(world_pos, graph, 5.0) {
        return Selection::Edge(idx);
    }
    Selection::None
}

/// Distance from a point to a line segment.
fn point_to_segment_distance(p: Vec2, a: Vec2, b: Vec2) -> f32 {
    let ab = b - a;
    let ap = p - a;
    let ab_len_sq = ab.x * ab.x + ab.y * ab.y;

    if ab_len_sq < 1e-6 {
        return ap.length();
    }

    let t = ((ap.x * ab.x + ap.y * ab.y) / ab_len_sq).clamp(0.0, 1.0);
    let proj = Vec2::new(a.x + t * ab.x, a.y + t * ab.y);
    (p - proj).length()
}
