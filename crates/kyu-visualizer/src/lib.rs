//! kyu-viz library: shared modules + `launch` entry point.
//!
//! Exposes a single `launch(db)` function so that examples and the default
//! binary can each seed the database differently before opening the window.

#![allow(dead_code)]

pub mod app;
pub mod canvas;
pub mod graph;
pub mod state;
pub mod theme;
pub mod transitions;
pub mod ui;

use std::sync::Arc;

use blinc_app::WindowConfig;
use blinc_app::windowed::WindowedApp;
use kyu_api::Database;

/// Open the KyuGraph visualizer window with a pre-configured database.
///
/// Any tables already in `db` will be loaded into the canvas on startup.
/// Extensions must be registered on `db` before calling this function.
pub fn launch(db: Database) -> Result<(), Box<dyn std::error::Error>> {
    let config = WindowConfig {
        title: "KyuGraph Visualizer".to_string(),
        width: 1280,
        height: 800,
        ..Default::default()
    };

    let db = Arc::new(db);
    let initial = app::load_initial_data(&db);

    let mut css_loaded = false;
    WindowedApp::run(config, move |ctx| {
        if !css_loaded {
            ctx.add_css(include_str!("../style.css"));
            css_loaded = true;
        }
        let db = db.clone();
        app::build_ui(ctx, db, &initial)
    })?;
    Ok(())
}
