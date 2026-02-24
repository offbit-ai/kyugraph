//! Canvas interaction state chart via `StateTransitions`.
//!
//! ```text
//!                 EVT_MOUSE_DOWN_NODE          EVT_MOUSE_UP
//!     Idle  ─────────────────────────>  NodeDragging  ──────>  Idle
//!       │                                                        ^
//!       │   EVT_MOUSE_DOWN_BG             EVT_MOUSE_UP          │
//!       └──────────────────────>  Panning  ──────────────────────┘
//! ```

use blinc_app::prelude::*;

/// Events dispatched by mouse handlers.
pub const EVT_MOUSE_DOWN_NODE: u32 = 1;
pub const EVT_MOUSE_DOWN_BG: u32 = 2;
pub const EVT_MOUSE_UP: u32 = 3;

/// Interaction mode for the graph canvas.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum CanvasMode {
    /// No active interaction.
    #[default]
    Idle,
    /// Dragging a specific node (index stored separately in a signal).
    NodeDragging,
    /// Panning the camera by dragging empty space.
    Panning,
}

impl StateTransitions for CanvasMode {
    fn on_event(&self, event: u32) -> Option<Self> {
        match (self, event) {
            // From Idle
            (CanvasMode::Idle, EVT_MOUSE_DOWN_NODE) => Some(CanvasMode::NodeDragging),
            (CanvasMode::Idle, EVT_MOUSE_DOWN_BG) => Some(CanvasMode::Panning),
            // From NodeDragging
            (CanvasMode::NodeDragging, EVT_MOUSE_UP) => Some(CanvasMode::Idle),
            // From Panning
            (CanvasMode::Panning, EVT_MOUSE_UP) => Some(CanvasMode::Idle),
            // No transition for unrecognized events
            _ => None,
        }
    }
}
