//! Top-level app view: creates state signals, wires deps, composes UI.

use std::sync::Arc;

use blinc_app::prelude::*;
use blinc_app::windowed::WindowedContext;
use blinc_core::State;
use kyu_api::Database;

use crate::canvas::{interaction, renderer};
use crate::graph::{layout, loader};
use crate::state::{CameraState, GraphData, SchemaData, Selection, Vec2};
use crate::theme;
use crate::transitions::CanvasMode;
use crate::ui::{inspector, query_bar, sidebar};

/// Data loaded once before the render loop starts.
pub struct InitialData {
    pub schema: SchemaData,
    pub graph: GraphData,
    pub layout_temp: f32,
}

/// Load schema + initial graph synchronously before entering the render loop.
pub fn load_initial_data(db: &Database) -> InitialData {
    let schema = loader::load_schema(db);

    let conn = db.connect();
    let mut graph = match loader::load_full_graph(&conn, db) {
        Ok(g) => g,
        Err(_) => GraphData::new(),
    };

    // Run layout to completion synchronously before the render loop.
    if !graph.nodes.is_empty() {
        let mut temp = layout::reset_temperature(graph.nodes.len());
        while layout::layout_batch(&mut graph, &mut temp, 10) {}
        layout::center_graph(&mut graph);
    }

    InitialData {
        schema,
        graph,
        layout_temp: 0.0,
    }
}

/// Build the entire UI. Called from `WindowedApp::run`.
///
/// All state is initialized from `initial` (loaded once at startup).
/// `build_ui` is purely declarative — it wires signals to UI elements.
pub fn build_ui(
    ctx: &WindowedContext,
    db: Arc<Database>,
    initial: &InitialData,
) -> impl ElementBuilder + use<> {
    // ── Create state signals (seeded from pre-loaded data) ──
    let graph_data = ctx.use_state_keyed("graph_data", || initial.graph.clone());
    let camera = ctx.use_state_keyed("camera", CameraState::default);
    let selection = ctx.use_state_keyed("selection", || Selection::None);
    let query_input = text_input_state_with_placeholder("MATCH (n) RETURN n LIMIT 100");
    let query_error = ctx.use_state_keyed("query_error", || Option::<String>::None);
    let schema_data = ctx.use_state_keyed("schema_data", || initial.schema.clone());
    let layout_temp = ctx.use_state_keyed("layout_temp", || initial.layout_temp);
    let dragged_node = ctx.use_state_keyed("dragged_node", || Option::<usize>::None);
    let prev_drag = ctx.use_state_keyed("prev_drag", || (0.0_f32, 0.0_f32));

    // ── Sidebar: on table click → center camera on that table's nodes ──
    let sidebar_on_click = {
        let gd = graph_data.clone();
        let cam = camera.clone();
        let sel = selection.clone();
        move |table_name: &str| {
            let graph = gd.get();
            // Collect positions of nodes matching this label (node table)
            // or connected by this rel type (rel table).
            let positions: Vec<Vec2> = {
                let node_matches: Vec<Vec2> = graph
                    .nodes
                    .iter()
                    .filter(|n| n.label == table_name)
                    .map(|n| n.pos)
                    .collect();
                if !node_matches.is_empty() {
                    node_matches
                } else {
                    // Rel table: collect src + dst nodes of matching edges.
                    graph
                        .edges
                        .iter()
                        .filter(|e| e.rel_type == table_name)
                        .flat_map(|e| [graph.nodes[e.src].pos, graph.nodes[e.dst].pos])
                        .collect()
                }
            };
            if positions.is_empty() {
                return;
            }
            let n = positions.len() as f32;
            let cx = positions.iter().map(|p| p.x).sum::<f32>() / n;
            let cy = positions.iter().map(|p| p.y).sum::<f32>() / n;
            // Offset is screen-space, applied before zoom.
            // To center world point (cx, cy): offset = -cx * zoom, -cy * zoom.
            let mut camera = cam.get();
            camera.offset_x = -cx * camera.zoom;
            camera.offset_y = -cy * camera.zoom;
            cam.set(camera);
            sel.set(Selection::None);
        }
    };

    // ── Query bar: on execute → run query ──
    let conn_query = Arc::new(db.connect());
    let on_execute = {
        let conn = conn_query.clone();
        let qe = query_error.clone();
        let qi = query_input.clone();
        move |_: ()| {
            let cypher = qi.lock().unwrap().value.clone();
            if cypher.trim().is_empty() {
                return;
            }
            match conn.query(&cypher) {
                Ok(_result) => {
                    qe.set(None);
                    // TODO: parse query result into graph nodes/edges
                }
                Err(e) => {
                    qe.set(Some(format!("{e}")));
                }
            }
        }
    };

    // ── Canvas area with CanvasMode state chart ──
    let canvas_view = build_canvas_view(
        graph_data.clone(),
        camera.clone(),
        selection.clone(),
        layout_temp.clone(),
        dragged_node.clone(),
        prev_drag.clone(),
        ctx.width,
        ctx.height,
    );

    // ── Compose layout ──
    //
    // #app (column)
    //   #header (40px)
    //   #main (row, flex:1)
    //     #sidebar (280px, column)
    //       #schema-browser
    //       #inspector
    //     #canvas-area (flex:1)
    //   #query-bar (50px)
    //
    div()
        .id("app")
        .flex_col()
        .w(ctx.width)
        .h(ctx.height)
        .child(
            div()
                .id("header")
                .h(40.0)
                .flex_row()
                .items_center()
                .p(4.0)
                .bg(theme::BG)
                .border_bottom(1.0, theme::BORDER)
                .child(h2("KyuGraph Visualizer").color(theme::ACCENT)),
        )
        .child(
            div()
                .id("main")
                .flex_grow()
                .child(
                    div()
                        .id("sidebar")
                        .flex_col()
                        .w(280.0)
                        .flex_shrink()
                        .border_right(1.0, theme::BORDER)
                        .child(sidebar::sidebar_view(schema_data.clone(), sidebar_on_click))
                        .child(inspector::inspector_view(
                            selection.clone(),
                            graph_data.clone(),
                        )),
                )
                .child(div().id("canvas-wrap").flex_grow().child(canvas_view)),
        )
        .child(query_bar::query_bar_view(
            query_error,
            query_input.clone(),
            on_execute,
        ))
}

/// Build the canvas area with CanvasMode state chart for interaction.
#[allow(clippy::too_many_arguments)]
fn build_canvas_view(
    graph_data: State<GraphData>,
    camera: State<CameraState>,
    selection: State<Selection>,
    _layout_temp: State<f32>,
    dragged_node: State<Option<usize>>,
    prev_drag: State<(f32, f32)>,
    window_w: f32,
    window_h: f32,
) -> impl ElementBuilder {
    let gd = graph_data.clone();
    let cam = camera.clone();
    let sel = selection.clone();

    stateful::<CanvasMode>()
        .deps([
            graph_data.signal_id(),
            camera.signal_id(),
            selection.signal_id(),
        ])
        .on_state(move |_ctx| {
            let gd = gd.clone();
            let cam = cam.clone();
            let sel = sel.clone();

            div().id("canvas-area").w_full().h_full().child(
                canvas(move |draw_ctx, bounds| {
                    let graph = gd.get();
                    let camera = cam.get();
                    let sel = sel.get();
                    renderer::render_graph(
                        draw_ctx,
                        bounds.width,
                        bounds.height,
                        &graph,
                        &camera,
                        &sel,
                    );
                })
                .w_full()
                .h_full(),
            )
        })
        .on_click({
            let gd = graph_data.clone();
            let cam = camera.clone();
            let sel = selection.clone();
            move |evt| {
                let graph = gd.get();
                let camera = cam.get();
                let world = interaction::screen_to_world(
                    Vec2::new(evt.local_x, evt.local_y),
                    &camera,
                    window_w - 280.0,
                    window_h - 50.0,
                );
                let hit = interaction::hit_test(world, &graph);
                sel.set(hit);
            }
        })
        .on_mouse_down({
            let gd = graph_data.clone();
            let cam = camera.clone();
            let dn = dragged_node.clone();
            let pd = prev_drag.clone();
            move |evt| {
                pd.set((0.0, 0.0));
                let graph = gd.get();
                let camera = cam.get();
                let world = interaction::screen_to_world(
                    Vec2::new(evt.local_x, evt.local_y),
                    &camera,
                    window_w - 280.0,
                    window_h - 50.0,
                );
                if let Some(idx) = interaction::hit_test_node(world, &graph) {
                    dn.set(Some(idx));
                } else {
                    dn.set(None);
                }
            }
        })
        .on_drag({
            let gd = graph_data.clone();
            let cam = camera.clone();
            let dn = dragged_node.clone();
            let pd = prev_drag.clone();
            move |evt| {
                // drag_delta is cumulative from drag start; compute per-frame delta.
                let (prev_x, prev_y) = pd.get();
                let dx = evt.drag_delta_x - prev_x;
                let dy = evt.drag_delta_y - prev_y;
                pd.set((evt.drag_delta_x, evt.drag_delta_y));

                if let Some(idx) = dn.get() {
                    let mut graph = gd.get();
                    let camera = cam.get();
                    if let Some(node) = graph.nodes.get_mut(idx) {
                        node.pos.x += dx / camera.zoom;
                        node.pos.y += dy / camera.zoom;
                        node.pinned = true;
                    }
                    gd.set(graph);
                } else {
                    let mut camera = cam.get();
                    camera.offset_x += dx;
                    camera.offset_y += dy;
                    cam.set(camera);
                }
            }
        })
        .on_drag_end({
            let dn = dragged_node.clone();
            move |_| {
                dn.set(None);
            }
        })
        .on_scroll({
            let cam = camera.clone();
            move |evt| {
                let mut camera = cam.get();
                let zoom_delta = evt.scroll_delta_y * 0.001;
                camera.zoom *= 1.0 + zoom_delta;
                camera.clamp_zoom();
                cam.set(camera);
            }
        })
        .w_full()
        .h_full()
}
