//! BFS shortest path — find shortest path between two nodes.
//!
//! Builds adjacency map from relationship table, then runs BFS from source
//! to target, reconstructing the path via parent pointers.

use std::collections::{HashMap, VecDeque};

use kyu_common::id::TableId;
use kyu_common::KyuResult;
use kyu_parser::ast::Direction;
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::operators::recursive_join::build_adjacency_map;
use crate::physical_plan::PhysicalOperator;

/// Configuration for a shortest path operation.
pub struct ShortestPathConfig {
    pub rel_table_id: TableId,
    pub direction: Direction,
    /// Column in child rows holding source node primary key.
    pub src_key_col: usize,
    /// Column in child rows holding destination node primary key.
    pub dst_key_col: usize,
}

pub struct ShortestPathOp {
    pub child: Box<PhysicalOperator>,
    pub cfg: ShortestPathConfig,
    results: Option<VecDeque<DataChunk>>,
}

impl ShortestPathOp {
    pub fn new(child: PhysicalOperator, cfg: ShortestPathConfig) -> Self {
        Self {
            child: Box::new(child),
            cfg,
            results: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.results.is_none() {
            self.results = Some(self.execute(ctx)?);
        }
        Ok(self.results.as_mut().unwrap().pop_front())
    }

    fn execute(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<VecDeque<DataChunk>> {
        // 1. Drain child to collect source rows (pairs of src, dst nodes).
        let mut source_rows: Vec<Vec<TypedValue>> = Vec::new();
        while let Some(chunk) = self.child.next(ctx)? {
            for row_idx in 0..chunk.num_rows() {
                source_rows.push(chunk.get_row(row_idx));
            }
        }

        // 2. Build adjacency map.
        let adj = build_adjacency_map(ctx, self.cfg.rel_table_id, self.cfg.direction);

        // 3. For each (src, dst) pair, find shortest path.
        let mut result_rows: Vec<Vec<TypedValue>> = Vec::new();

        for row in &source_rows {
            let src = &row[self.cfg.src_key_col];
            let dst = &row[self.cfg.dst_key_col];
            let path = bfs_shortest_path(src, dst, &adj);

            // Output: src columns + path as list.
            let mut out = row.clone();
            out.push(TypedValue::List(path));
            result_rows.push(out);
        }

        // 4. Convert to DataChunks.
        let ncols = source_rows.first().map_or(1, |r| r.len()) + 1;
        let mut chunks = VecDeque::new();
        for batch in result_rows.chunks(2048) {
            chunks.push_back(DataChunk::from_rows(batch, ncols));
        }
        Ok(chunks)
    }
}

/// BFS from `src` to `dst` through the adjacency map.
/// Returns the shortest path as a list of node keys (including src and dst).
/// Returns empty list if no path exists.
pub fn bfs_shortest_path(
    src: &TypedValue,
    dst: &TypedValue,
    adj: &HashMap<TypedValue, Vec<TypedValue>>,
) -> Vec<TypedValue> {
    if src == dst {
        return vec![src.clone()];
    }

    // BFS with parent tracking.
    let mut visited: HashMap<TypedValue, TypedValue> = HashMap::new(); // child -> parent
    visited.insert(src.clone(), src.clone()); // sentinel: src's parent is itself
    let mut queue = VecDeque::new();
    queue.push_back(src.clone());

    while let Some(node) = queue.pop_front() {
        if let Some(neighbors) = adj.get(&node) {
            for neighbor in neighbors {
                if !visited.contains_key(neighbor) {
                    visited.insert(neighbor.clone(), node.clone());
                    if neighbor == dst {
                        // Reconstruct path.
                        return reconstruct_path(&visited, src, dst);
                    }
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    // No path found.
    Vec::new()
}

/// Reconstruct path from parent map.
fn reconstruct_path(
    parents: &HashMap<TypedValue, TypedValue>,
    src: &TypedValue,
    dst: &TypedValue,
) -> Vec<TypedValue> {
    let mut path = Vec::new();
    let mut current = dst.clone();
    loop {
        path.push(current.clone());
        if &current == src {
            break;
        }
        match parents.get(&current) {
            Some(parent) => current = parent.clone(),
            None => break, // shouldn't happen if called correctly
        }
    }
    path.reverse();
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    fn tv(s: &str) -> TypedValue {
        TypedValue::String(SmolStr::new(s))
    }

    #[test]
    fn shortest_path_direct() {
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B"), tv("C")]);
        adj.insert(tv("B"), vec![tv("D")]);
        adj.insert(tv("C"), vec![tv("D")]);

        let path = bfs_shortest_path(&tv("A"), &tv("B"), &adj);
        assert_eq!(path, vec![tv("A"), tv("B")]);
    }

    #[test]
    fn shortest_path_two_hops() {
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B")]);
        adj.insert(tv("B"), vec![tv("C")]);
        adj.insert(tv("C"), vec![tv("D")]);

        let path = bfs_shortest_path(&tv("A"), &tv("C"), &adj);
        assert_eq!(path, vec![tv("A"), tv("B"), tv("C")]);
    }

    #[test]
    fn shortest_path_prefers_direct() {
        // A -> B -> C, A -> C (direct)
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B"), tv("C")]);
        adj.insert(tv("B"), vec![tv("C")]);

        let path = bfs_shortest_path(&tv("A"), &tv("C"), &adj);
        // BFS finds direct A->C first.
        assert_eq!(path, vec![tv("A"), tv("C")]);
    }

    #[test]
    fn shortest_path_no_path() {
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B")]);
        adj.insert(tv("C"), vec![tv("D")]);

        let path = bfs_shortest_path(&tv("A"), &tv("D"), &adj);
        assert!(path.is_empty());
    }

    #[test]
    fn shortest_path_same_node() {
        let adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        let path = bfs_shortest_path(&tv("A"), &tv("A"), &adj);
        assert_eq!(path, vec![tv("A")]);
    }

    #[test]
    fn shortest_path_cycle() {
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B")]);
        adj.insert(tv("B"), vec![tv("C")]);
        adj.insert(tv("C"), vec![tv("A")]); // cycle back

        let path = bfs_shortest_path(&tv("A"), &tv("C"), &adj);
        assert_eq!(path, vec![tv("A"), tv("B"), tv("C")]);
    }

    #[test]
    fn shortest_path_diamond() {
        // A -> B -> D, A -> C -> D
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B"), tv("C")]);
        adj.insert(tv("B"), vec![tv("D")]);
        adj.insert(tv("C"), vec![tv("D")]);

        let path = bfs_shortest_path(&tv("A"), &tv("D"), &adj);
        // Both paths A->B->D and A->C->D are length 2; BFS finds one.
        assert_eq!(path.len(), 3);
        assert_eq!(path[0], tv("A"));
        assert_eq!(path[2], tv("D"));
    }

    #[test]
    fn shortest_path_long_chain() {
        let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();
        adj.insert(tv("A"), vec![tv("B")]);
        adj.insert(tv("B"), vec![tv("C")]);
        adj.insert(tv("C"), vec![tv("D")]);
        adj.insert(tv("D"), vec![tv("E")]);
        adj.insert(tv("E"), vec![tv("F")]);

        let path = bfs_shortest_path(&tv("A"), &tv("F"), &adj);
        assert_eq!(path.len(), 6);
        assert_eq!(path, vec![tv("A"), tv("B"), tv("C"), tv("D"), tv("E"), tv("F")]);
    }
}
