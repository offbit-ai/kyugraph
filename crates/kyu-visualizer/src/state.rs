//! Application state types.
//!
//! These are stored as `State<T>` via `ctx.use_state_keyed()` and
//! their signal IDs are wired to stateful container `.deps()`.

use hashbrown::HashMap;

/// 2D vector for positions and velocities.
#[derive(Clone, Copy, Default, Debug)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

impl Vec2 {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    pub fn length(self) -> f32 {
        (self.x * self.x + self.y * self.y).sqrt()
    }

    pub fn normalized(self) -> Self {
        let len = self.length();
        if len < 1e-6 {
            Self::default()
        } else {
            Self {
                x: self.x / len,
                y: self.y / len,
            }
        }
    }
}

impl std::ops::Add for Vec2 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            x: self.x + rhs.x,
            y: self.y + rhs.y,
        }
    }
}

impl std::ops::AddAssign for Vec2 {
    fn add_assign(&mut self, rhs: Self) {
        self.x += rhs.x;
        self.y += rhs.y;
    }
}

impl std::ops::Sub for Vec2 {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
        }
    }
}

impl std::ops::Mul<f32> for Vec2 {
    type Output = Self;
    fn mul(self, s: f32) -> Self {
        Self {
            x: self.x * s,
            y: self.y * s,
        }
    }
}

impl std::ops::Div<f32> for Vec2 {
    type Output = Self;
    fn div(self, s: f32) -> Self {
        Self {
            x: self.x / s,
            y: self.y / s,
        }
    }
}

/// The full graph data loaded into the visualizer.
#[derive(Clone, Default)]
pub struct GraphData {
    pub nodes: Vec<NodeVisual>,
    pub edges: Vec<EdgeVisual>,
    /// Maps node id ("{table}:{pk}") → index in `nodes` for O(1) dedup.
    pub node_index: HashMap<String, usize>,
}

impl GraphData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
        self.node_index.clear();
    }
}

/// A single node in the visual graph.
#[derive(Clone, Debug)]
pub struct NodeVisual {
    /// Unique id: "{TableName}:{pk_value}"
    pub id: String,
    /// Node table name (e.g. "Person").
    pub label: String,
    /// Pre-formatted (key, value) pairs for inspector display.
    pub properties: Vec<(String, String)>,
    /// Position in world coordinates.
    pub pos: Vec2,
    /// Velocity for force-directed layout.
    pub vel: Vec2,
    /// If true, FR layout skips displacement for this node.
    pub pinned: bool,
    /// Index into `theme::NODE_PALETTE`.
    pub color_idx: u8,
}

/// A single edge in the visual graph.
#[derive(Clone, Debug)]
pub struct EdgeVisual {
    /// Index into `GraphData.nodes` for the source node.
    pub src: usize,
    /// Index into `GraphData.nodes` for the target node.
    pub dst: usize,
    /// Relationship table name (e.g. "KNOWS").
    pub rel_type: String,
    /// Pre-formatted (key, value) pairs for inspector display.
    pub properties: Vec<(String, String)>,
}

/// Camera state for pan and zoom.
#[derive(Clone, Copy, Debug)]
pub struct CameraState {
    pub offset_x: f32,
    pub offset_y: f32,
    pub zoom: f32,
}

impl Default for CameraState {
    fn default() -> Self {
        Self {
            offset_x: 0.0,
            offset_y: 0.0,
            zoom: 1.0,
        }
    }
}

impl CameraState {
    /// Clamp zoom to [0.1, 5.0].
    pub fn clamp_zoom(&mut self) {
        self.zoom = self.zoom.clamp(0.1, 5.0);
    }
}

/// Currently selected element in the graph.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum Selection {
    #[default]
    None,
    Node(usize),
    Edge(usize),
}

/// Schema data loaded from the catalog.
#[derive(Clone, Default, Debug)]
pub struct SchemaData {
    pub node_tables: Vec<SchemaTable>,
    pub rel_tables: Vec<SchemaTable>,
}

/// A table entry for the schema browser.
#[derive(Clone, Debug)]
pub struct SchemaTable {
    pub name: String,
    pub num_rows: u64,
    pub properties: Vec<(String, String)>, // (name, type)
}
