//! Property inspector panel.

use blinc_app::prelude::*;
use blinc_core::State;

use crate::state::{GraphData, Selection};
use crate::theme;

/// Build the inspector panel.
pub fn inspector_view(
    selection: State<Selection>,
    graph_data: State<GraphData>,
) -> impl ElementBuilder {
    stateful::<NoState>()
        .deps([selection.signal_id(), graph_data.signal_id()])
        .on_state(move |_ctx| {
            let sel = selection.get();
            let graph = graph_data.get();

            let mut panel = div()
                .id("inspector")
                .flex_col()
                .p(3.0)
                .gap(1.0)
                .border_top(1.0, theme::BORDER);
            panel = panel.child(
                div()
                    .class("section-title")
                    .child(text("Inspector").color(theme::TEXT_DIM)),
            );

            match sel {
                Selection::None => {
                    panel = panel.child(
                        div()
                            .class("inspector-hint")
                            .child(text("Click a node or edge to inspect").color(theme::TEXT_DIM)),
                    );
                }
                Selection::Node(idx) => {
                    if let Some(node) = graph.nodes.get(idx) {
                        panel = panel.child(div().class("inspector-title").child(
                            text(format!("{}: {}", node.label, node.id)).color(theme::ACCENT),
                        ));
                        for (key, val) in &node.properties {
                            panel = panel.child(
                                div()
                                    .class("prop-row")
                                    .child(
                                        div()
                                            .class("prop-key")
                                            .child(text(key).color(theme::TEXT_DIM)),
                                    )
                                    .child(
                                        div()
                                            .class("prop-val")
                                            .child(text(val).color(theme::TEXT_COLOR)),
                                    ),
                            );
                        }
                    }
                }
                Selection::Edge(idx) => {
                    if let Some(edge) = graph.edges.get(idx) {
                        let src_label = graph
                            .nodes
                            .get(edge.src)
                            .map(|n| n.id.as_str())
                            .unwrap_or("?");
                        let dst_label = graph
                            .nodes
                            .get(edge.dst)
                            .map(|n| n.id.as_str())
                            .unwrap_or("?");
                        panel = panel.child(
                            div().class("inspector-title").child(
                                text(format!("{src_label} -[{}]-> {dst_label}", edge.rel_type))
                                    .color(theme::ACCENT),
                            ),
                        );
                        for (key, val) in &edge.properties {
                            panel = panel.child(
                                div()
                                    .class("prop-row")
                                    .child(
                                        div()
                                            .class("prop-key")
                                            .child(text(key).color(theme::TEXT_DIM)),
                                    )
                                    .child(
                                        div()
                                            .class("prop-val")
                                            .child(text(val).color(theme::TEXT_COLOR)),
                                    ),
                            );
                        }
                    }
                }
            }

            panel
        })
}
