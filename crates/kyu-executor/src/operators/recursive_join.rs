//! Recursive join operator — variable-length path traversal via BFS.
//!
//! Scans relationship table to build an adjacency map, then BFS-expands from
//! each source node for `min_hops..=max_hops` levels. Joins reachable
//! destination nodes with the dest node table to produce combined rows.

use std::collections::{HashMap, HashSet, VecDeque};

use kyu_common::KyuResult;
use kyu_common::id::TableId;
use kyu_parser::ast::Direction;
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

/// Configuration for a recursive join (avoids too-many-arguments).
pub struct RecursiveJoinConfig {
    pub rel_table_id: TableId,
    pub dest_table_id: TableId,
    pub direction: Direction,
    pub min_hops: u32,
    pub max_hops: u32,
    /// Column in child rows that holds the source node's primary key.
    pub src_key_col: usize,
    /// Column in dest node table that holds the primary key.
    pub dest_key_col: usize,
    /// Number of columns in destination node table.
    pub dest_ncols: usize,
}

pub struct RecursiveJoinOp {
    pub child: Box<PhysicalOperator>,
    pub cfg: RecursiveJoinConfig,
    /// Buffered result chunks.
    results: Option<VecDeque<DataChunk>>,
}

impl RecursiveJoinOp {
    pub fn new(child: PhysicalOperator, cfg: RecursiveJoinConfig) -> Self {
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
        // 1. Drain child to collect source rows.
        let mut source_rows: Vec<Vec<TypedValue>> = Vec::new();
        while let Some(chunk) = self.child.next(ctx)? {
            for row_idx in 0..chunk.num_rows() {
                source_rows.push(chunk.get_row(row_idx));
            }
        }

        // 2. Build adjacency map from relationship table.
        let adj = build_adjacency_map(ctx, self.cfg.rel_table_id, self.cfg.direction);

        // 3. Build dest node lookup: primary_key -> full row.
        let dest_lookup = build_node_lookup(ctx, self.cfg.dest_table_id, self.cfg.dest_key_col);

        // 4. BFS from each source, collect result rows.
        let src_ncols = source_rows.first().map_or(0, |r| r.len());
        let total_cols = src_ncols + self.cfg.dest_ncols;
        let mut result_rows: Vec<Vec<TypedValue>> = Vec::new();

        for src_row in &source_rows {
            let src_key = &src_row[self.cfg.src_key_col];
            let reachable = bfs_expand(src_key, &adj, self.cfg.min_hops, self.cfg.max_hops);

            for dest_key in reachable {
                let mut combined = src_row.clone();
                if let Some(dest_row) = dest_lookup.get(&dest_key) {
                    combined.extend_from_slice(dest_row);
                } else {
                    combined.extend(std::iter::repeat_n(TypedValue::Null, self.cfg.dest_ncols));
                }
                result_rows.push(combined);
            }
        }

        // 5. Convert to DataChunks (batch of up to 2048 rows).
        let mut chunks = VecDeque::new();
        let chunk_size = 2048;
        for batch in result_rows.chunks(chunk_size) {
            chunks.push_back(DataChunk::from_rows(batch, total_cols));
        }

        Ok(chunks)
    }
}

/// Build adjacency map: src -> [dst] from a relationship table.
///
/// For Right direction: column 0 = src, column 1 = dst.
/// For Left direction: column 1 = src, column 0 = dst (reversed).
/// For Both: both directions.
pub fn build_adjacency_map(
    ctx: &ExecutionContext<'_>,
    rel_table_id: TableId,
    direction: Direction,
) -> HashMap<TypedValue, Vec<TypedValue>> {
    let mut adj: HashMap<TypedValue, Vec<TypedValue>> = HashMap::new();

    for chunk in ctx.storage.scan_table(rel_table_id) {
        for row_idx in 0..chunk.num_rows() {
            let col0 = chunk.get_value(row_idx, 0);
            let col1 = chunk.get_value(row_idx, 1);

            match direction {
                Direction::Right => {
                    adj.entry(col0).or_default().push(col1);
                }
                Direction::Left => {
                    adj.entry(col1).or_default().push(col0);
                }
                Direction::Both => {
                    adj.entry(col0.clone()).or_default().push(col1.clone());
                    adj.entry(col1).or_default().push(col0);
                }
            }
        }
    }

    adj
}

/// Build a lookup table: node primary key -> full row.
fn build_node_lookup(
    ctx: &ExecutionContext<'_>,
    table_id: TableId,
    key_col: usize,
) -> HashMap<TypedValue, Vec<TypedValue>> {
    let mut lookup = HashMap::new();

    for chunk in ctx.storage.scan_table(table_id) {
        for row_idx in 0..chunk.num_rows() {
            let key = chunk.get_value(row_idx, key_col);
            let row = chunk.get_row(row_idx);
            lookup.insert(key, row);
        }
    }

    lookup
}

/// BFS expansion from a source node through the adjacency map.
/// Returns all distinct nodes reachable in min_hops..=max_hops steps.
fn bfs_expand(
    src: &TypedValue,
    adj: &HashMap<TypedValue, Vec<TypedValue>>,
    min_hops: u32,
    max_hops: u32,
) -> Vec<TypedValue> {
    let mut visited: HashSet<TypedValue> = HashSet::new();
    visited.insert(src.clone());

    // BFS frontier: (node, depth)
    let mut queue: VecDeque<(TypedValue, u32)> = VecDeque::new();
    queue.push_back((src.clone(), 0));

    let mut results = Vec::new();

    while let Some((node, depth)) = queue.pop_front() {
        if depth >= max_hops {
            continue;
        }
        if let Some(neighbors) = adj.get(&node) {
            for neighbor in neighbors {
                if visited.insert(neighbor.clone()) {
                    let next_depth = depth + 1;
                    if next_depth >= min_hops {
                        results.push(neighbor.clone());
                    }
                    queue.push_back((neighbor.clone(), next_depth));
                }
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use crate::operators::scan::ScanNodeOp;
    use kyu_catalog::{CatalogContent, NodeTableEntry, Property, RelTableEntry};
    use kyu_common::id::PropertyId;
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: TableId(0),
                name: SmolStr::new("Person"),
                properties: vec![
                    Property::new(PropertyId(0), "name", LogicalType::String, true),
                    Property::new(PropertyId(1), "age", LogicalType::Int64, false),
                ],
                primary_key_idx: 0,
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
            .add_rel_table(RelTableEntry {
                table_id: TableId(1),
                name: SmolStr::new("KNOWS"),
                from_table_id: TableId(0),
                to_table_id: TableId(0),
                properties: vec![Property::new(
                    PropertyId(2),
                    "since",
                    LogicalType::Int64,
                    false,
                )],
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
    }

    fn make_storage() -> MockStorage {
        let mut storage = MockStorage::new();
        // Person: name, age
        storage.insert_table(
            TableId(0),
            vec![
                vec![
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::Int64(25),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(30),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Charlie")),
                    TypedValue::Int64(35),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Diana")),
                    TypedValue::Int64(28),
                ],
            ],
        );
        // KNOWS: src, dst, since
        // Alice -> Bob, Bob -> Charlie, Charlie -> Diana
        storage.insert_table(
            TableId(1),
            vec![
                vec![
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(2020),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::String(SmolStr::new("Charlie")),
                    TypedValue::Int64(2021),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Charlie")),
                    TypedValue::String(SmolStr::new("Diana")),
                    TypedValue::Int64(2022),
                ],
            ],
        );
        storage
    }

    #[test]
    fn recursive_join_1_hop() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        // Build adjacency from real storage.
        let adj = build_adjacency_map(&ctx, TableId(1), Direction::Right);
        assert!(adj.contains_key(&TypedValue::String(SmolStr::new("Alice"))));

        // BFS from Alice, 1..1 hop.
        let reachable = bfs_expand(&TypedValue::String(SmolStr::new("Alice")), &adj, 1, 1);
        assert_eq!(reachable.len(), 1);
        assert_eq!(reachable[0], TypedValue::String(SmolStr::new("Bob")));
    }

    #[test]
    fn recursive_join_2_hops() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        let adj = build_adjacency_map(&ctx, TableId(1), Direction::Right);
        let reachable = bfs_expand(&TypedValue::String(SmolStr::new("Alice")), &adj, 1, 2);
        // 1 hop: Bob, 2 hops: Charlie → 2 results.
        assert_eq!(reachable.len(), 2);
    }

    #[test]
    fn recursive_join_3_hops() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        let adj = build_adjacency_map(&ctx, TableId(1), Direction::Right);
        let reachable = bfs_expand(&TypedValue::String(SmolStr::new("Alice")), &adj, 1, 3);
        // 1: Bob, 2: Charlie, 3: Diana → 3 results.
        assert_eq!(reachable.len(), 3);
    }

    #[test]
    fn recursive_join_min_2() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        let adj = build_adjacency_map(&ctx, TableId(1), Direction::Right);
        let reachable = bfs_expand(&TypedValue::String(SmolStr::new("Alice")), &adj, 2, 3);
        // min=2: skip Bob, get Charlie (2 hops) and Diana (3 hops).
        assert_eq!(reachable.len(), 2);
    }

    #[test]
    fn recursive_join_operator() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        // Scan all persons as source.
        let scan = PhysicalOperator::ScanNode(ScanNodeOp::new(TableId(0)));
        let mut rj = RecursiveJoinOp::new(
            scan,
            RecursiveJoinConfig {
                rel_table_id: TableId(1),
                dest_table_id: TableId(0),
                direction: Direction::Right,
                min_hops: 1,
                max_hops: 1,
                src_key_col: 0,
                dest_key_col: 0,
                dest_ncols: 2,
            },
        );

        let chunk = rj.next(&ctx).unwrap().unwrap();
        // Alice->Bob, Bob->Charlie, Charlie->Diana = 3 result rows.
        // Diana has no outgoing edges, so no results.
        assert_eq!(chunk.num_rows(), 3);
        // 4 columns: src.name, src.age, dest.name, dest.age
        assert_eq!(chunk.num_columns(), 4);

        // Verify Alice -> Bob
        assert_eq!(
            chunk.get_value(0, 0),
            TypedValue::String(SmolStr::new("Alice"))
        );
        assert_eq!(
            chunk.get_value(0, 2),
            TypedValue::String(SmolStr::new("Bob"))
        );

        // No more chunks.
        assert!(rj.next(&ctx).unwrap().is_none());
    }

    #[test]
    fn recursive_join_both_direction() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(make_catalog(), &storage);

        let adj = build_adjacency_map(&ctx, TableId(1), Direction::Both);
        // Bob with Both direction, 1 hop: Alice + Charlie.
        let reachable = bfs_expand(&TypedValue::String(SmolStr::new("Bob")), &adj, 1, 1);
        assert_eq!(reachable.len(), 2);
    }
}
