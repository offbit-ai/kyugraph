//! Schema browser sidebar.

use blinc_app::prelude::*;
use blinc_core::State;

use crate::state::SchemaData;
use crate::theme;

/// Build the schema browser (lives inside #sidebar).
pub fn sidebar_view(
    schema: State<SchemaData>,
    on_table_click: impl Fn(&str) + Clone + Send + Sync + 'static,
) -> impl ElementBuilder {
    let on_click = on_table_click.clone();
    stateful::<NoState>()
        .deps([schema.signal_id()])
        .on_state(move |_ctx| {
            let data = schema.get();
            let on_click = on_click.clone();

            let mut browser = div().id("schema-browser").flex_col().p(3.0).gap(1.0);

            // Node tables.
            browser = browser.child(
                div()
                    .class("section-title")
                    .child(text("Node Tables").color(theme::TEXT_DIM)),
            );
            for table in &data.node_tables {
                let name = table.name.clone();
                let count = table.num_rows;
                let cb = on_click.clone();
                browser = browser.child(
                    div()
                        .class("table-entry")
                        .child(
                            div()
                                .class("table-name")
                                .child(text(&name).color(theme::TEXT_COLOR)),
                        )
                        .child(
                            div()
                                .class("table-count")
                                .child(text(format!("({count})")).color(theme::TEXT_DIM)),
                        )
                        .on_click(move |_| cb(&name)),
                );
            }

            // Rel tables.
            browser = browser.child(
                div()
                    .class("section-title")
                    .child(text("Relationships").color(theme::TEXT_DIM)),
            );
            for table in &data.rel_tables {
                let name = table.name.clone();
                let count = table.num_rows;
                let cb = on_click.clone();
                browser = browser.child(
                    div()
                        .class("table-entry")
                        .child(
                            div()
                                .class("table-name")
                                .child(text(&name).color(theme::TEXT_COLOR)),
                        )
                        .child(
                            div()
                                .class("table-count")
                                .child(text(format!("({count})")).color(theme::TEXT_DIM)),
                        )
                        .on_click(move |_| cb(&name)),
                );
            }

            browser
        })
}
