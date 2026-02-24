//! Query bar: Cypher input with execute button.

use blinc_app::prelude::*;
use blinc_core::{Color, State};

use crate::theme;

/// Build the query bar.
pub fn query_bar_view(
    query_error: State<Option<String>>,
    query_input: SharedTextInputState,
    on_execute: impl Fn(()) + Clone + Send + Sync + 'static,
) -> impl ElementBuilder {
    let on_exec = on_execute.clone();

    stateful::<NoState>()
        .deps([query_error.signal_id()])
        .on_state(move |_ctx| {
            let err = query_error.get();
            let on_exec = on_exec.clone();
            let qi = query_input.clone();

            let mut bar = div()
                .id("query-bar")
                .h(50.0)
                .px(3.0)
                .gap(2.0)
                .items_center()
                .border_top(1.0, theme::BORDER);

            bar = bar
                .child(
                    div()
                        .class("query-prompt")
                        .child(text("kyu>").color(theme::ACCENT)),
                )
                .child(text_input(&qi).id("query-input"))
                .child(
                    div()
                        .class("execute-btn")
                        .child(text("Execute").color(Color::rgba(0.04, 0.04, 0.05, 1.0)))
                        .on_click(move |_| {
                            on_exec(());
                        }),
                );

            if let Some(err_msg) = err {
                bar = bar.child(
                    div()
                        .class("error-text")
                        .child(text(err_msg).color(Color::rgba(0.91, 0.36, 0.36, 1.0))),
                );
            }

            bar
        })
}
