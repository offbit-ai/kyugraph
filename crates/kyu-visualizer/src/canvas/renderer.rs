//! Canvas rendering: draws graph edges, nodes, labels, and selection highlights.

use blinc_core::{
    Brush, Color, CornerRadius, DrawContext, Point, Rect, Stroke, TextStyle, Transform,
};

use crate::state::{CameraState, GraphData, Selection, Vec2};
use crate::theme::{ACCENT, EDGE_COLOR, NODE_PALETTE, NODE_RADIUS, TEXT_COLOR, TEXT_DIM};

/// Render the entire graph onto the canvas.
pub fn render_graph(
    ctx: &mut dyn DrawContext,
    width: f32,
    height: f32,
    graph: &GraphData,
    camera: &CameraState,
    selection: &Selection,
) {
    // Apply camera transform: center origin, then pan (screen space), then zoom.
    ctx.push_transform(Transform::translate(
        width / 2.0 + camera.offset_x,
        height / 2.0 + camera.offset_y,
    ));
    ctx.push_transform(Transform::scale(camera.zoom, camera.zoom));

    let label_style = TextStyle {
        size: 11.0,
        color: TEXT_COLOR,
        ..Default::default()
    };
    let edge_style = TextStyle {
        size: 10.0,
        color: TEXT_DIM,
        ..Default::default()
    };

    // 1. Draw edges.
    for (i, edge) in graph.edges.iter().enumerate() {
        let src = graph.nodes[edge.src].pos;
        let dst = graph.nodes[edge.dst].pos;
        let is_selected = *selection == Selection::Edge(i);

        draw_edge(ctx, src, dst, is_selected);

        // Edge label at midpoint.
        if !edge.rel_type.is_empty() {
            let mid_x = (src.x + dst.x) / 2.0;
            let mid_y = (src.y + dst.y) / 2.0;
            ctx.draw_text(&edge.rel_type, Point::new(mid_x, mid_y - 6.0), &edge_style);
        }
    }

    // 2. Draw nodes.
    for (i, node) in graph.nodes.iter().enumerate() {
        let center = Point::new(node.pos.x, node.pos.y);
        let is_selected = *selection == Selection::Node(i);
        let color = NODE_PALETTE[node.color_idx as usize % NODE_PALETTE.len()];

        // Node circle.
        ctx.fill_circle(center, NODE_RADIUS, Brush::Solid(color));

        // Selection ring.
        if is_selected {
            ctx.stroke_circle(
                center,
                NODE_RADIUS + 3.0,
                &Stroke {
                    width: 2.0,
                    ..Default::default()
                },
                Brush::Solid(ACCENT),
            );
        }

        // Node label below circle.
        let display_label = if node.id.len() > 20 {
            &node.id[..20]
        } else {
            &node.id
        };
        ctx.draw_text(
            display_label,
            Point::new(center.x - 30.0, center.y + NODE_RADIUS + 4.0),
            &label_style,
        );
    }

    ctx.pop_transform();
    ctx.pop_transform();
}

/// Draw a single edge line with an arrowhead.
fn draw_edge(ctx: &mut dyn DrawContext, src: Vec2, dst: Vec2, is_selected: bool) {
    let color = if is_selected { ACCENT } else { EDGE_COLOR };
    let width = if is_selected { 2.5 } else { 1.0 };

    let dx = dst.x - src.x;
    let dy = dst.y - src.y;
    let dist = (dx * dx + dy * dy).sqrt();
    if dist < 0.01 {
        return;
    }

    let nx = dx / dist;
    let ny = dy / dist;

    // Offset endpoints to sit on the circle edge.
    let src_x = src.x + nx * NODE_RADIUS;
    let src_y = src.y + ny * NODE_RADIUS;
    let dst_x = dst.x - nx * NODE_RADIUS;
    let dst_y = dst.y - ny * NODE_RADIUS;

    let line_len = ((dst_x - src_x).powi(2) + (dst_y - src_y).powi(2)).sqrt();
    if line_len < 1.0 {
        return;
    }

    // Draw line as a rotated thin rect.
    let angle = dy.atan2(dx);
    let mid_x = (src_x + dst_x) / 2.0;
    let mid_y = (src_y + dst_y) / 2.0;

    ctx.push_transform(Transform::translate(mid_x, mid_y));
    ctx.push_transform(Transform::rotate(angle));
    ctx.fill_rect(
        Rect::new(-line_len / 2.0, -width / 2.0, line_len, width),
        CornerRadius::uniform(0.0),
        Brush::Solid(color),
    );
    ctx.pop_transform();
    ctx.pop_transform();

    // Arrowhead at destination.
    let arrow_size = 8.0;
    let ax = dst_x - nx * arrow_size;
    let ay = dst_y - ny * arrow_size;
    let perp_x = -ny * arrow_size * 0.5;
    let perp_y = nx * arrow_size * 0.5;

    draw_arrow_wing(ctx, dst_x, dst_y, ax + perp_x, ay + perp_y, color, 1.5);
    draw_arrow_wing(ctx, dst_x, dst_y, ax - perp_x, ay - perp_y, color, 1.5);
}

fn draw_arrow_wing(
    ctx: &mut dyn DrawContext,
    x1: f32,
    y1: f32,
    x2: f32,
    y2: f32,
    color: Color,
    width: f32,
) {
    let dx = x2 - x1;
    let dy = y2 - y1;
    let len = (dx * dx + dy * dy).sqrt();
    if len < 0.5 {
        return;
    }
    let angle = dy.atan2(dx);
    let mid_x = (x1 + x2) / 2.0;
    let mid_y = (y1 + y2) / 2.0;

    ctx.push_transform(Transform::translate(mid_x, mid_y));
    ctx.push_transform(Transform::rotate(angle));
    ctx.fill_rect(
        Rect::new(-len / 2.0, -width / 2.0, len, width),
        CornerRadius::uniform(0.0),
        Brush::Solid(color),
    );
    ctx.pop_transform();
    ctx.pop_transform();
}
