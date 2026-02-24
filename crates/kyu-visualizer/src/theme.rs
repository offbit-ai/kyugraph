//! Color constants for canvas rendering and builder-only UI properties.
//!
//! Visual styling lives in `style.css` where possible. Colors here are used
//! inside `canvas()` draw callbacks and for builder-only properties like
//! directional borders and text colors (which don't inherit in Blinc CSS).

use blinc_core::Color;

// ── Background & accent (used in canvas for selection ring, etc.) ──

pub const BG: Color = Color::rgba(0.04, 0.04, 0.05, 1.0); // #0a0a0c
pub const ACCENT: Color = Color::rgba(0.77, 0.58, 0.42, 1.0); // #c4956a
pub const EDGE_COLOR: Color = Color::rgba(0.29, 0.29, 0.33, 1.0); // #4a4a55
pub const TEXT_COLOR: Color = Color::rgba(0.91, 0.90, 0.89, 1.0); // #e8e6e3
pub const TEXT_DIM: Color = Color::rgba(0.53, 0.53, 0.53, 0.8); // #888 semi
pub const BORDER: Color = Color::rgba(0.16, 0.16, 0.21, 1.0); // #2a2a35

// ── Node radius (world-space, before camera zoom) ──

pub const NODE_RADIUS: f32 = 16.0;

// ── Node color palette — 8 distinct hues that work on dark bg ──

pub const NODE_PALETTE: [Color; 8] = [
    Color::rgba(0.77, 0.58, 0.42, 1.0), // warm sand   #c4956a
    Color::rgba(0.40, 0.70, 0.90, 1.0), // sky blue    #66b3e6
    Color::rgba(0.60, 0.85, 0.55, 1.0), // soft green  #99d98c
    Color::rgba(0.85, 0.50, 0.55, 1.0), // rose        #d9808c
    Color::rgba(0.70, 0.55, 0.85, 1.0), // lavender    #b38cd9
    Color::rgba(0.90, 0.75, 0.40, 1.0), // gold        #e6bf66
    Color::rgba(0.45, 0.80, 0.75, 1.0), // teal        #73ccbf
    Color::rgba(0.85, 0.60, 0.75, 1.0), // pink        #d999bf
];

/// Deterministic color for a node table name.
pub fn label_color(label: &str) -> Color {
    let hash = label
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    NODE_PALETTE[(hash as usize) % NODE_PALETTE.len()]
}

/// Color index for a node table name (stored in NodeVisual).
pub fn label_color_idx(label: &str) -> u8 {
    let hash = label
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    (hash as usize % NODE_PALETTE.len()) as u8
}
