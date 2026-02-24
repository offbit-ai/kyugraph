//! Hash join operator — build hash table from left side, probe from right.

use hashbrown::HashMap;
use kyu_common::KyuResult;
use kyu_expression::{BoundExpression, evaluate};
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct HashJoinOp {
    pub build: Box<PhysicalOperator>,
    pub probe: Box<PhysicalOperator>,
    pub build_keys: Vec<BoundExpression>,
    pub probe_keys: Vec<BoundExpression>,
    /// Build-side data: stored chunks + index into them.
    build_data: Option<BuildData>,
}

struct BuildData {
    chunks: Vec<DataChunk>,
    /// key values → list of (chunk_idx, row_idx) pairs.
    ht: HashMap<Vec<TypedValue>, Vec<(u32, u32)>>,
    num_cols: usize,
}

impl HashJoinOp {
    pub fn new(
        build: PhysicalOperator,
        probe: PhysicalOperator,
        build_keys: Vec<BoundExpression>,
        probe_keys: Vec<BoundExpression>,
    ) -> Self {
        Self {
            build: Box::new(build),
            probe: Box::new(probe),
            build_keys,
            probe_keys,
            build_data: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        // Build phase: drain build side on first call, store chunks + index.
        if self.build_data.is_none() {
            let mut chunks = Vec::new();
            let mut ht: HashMap<Vec<TypedValue>, Vec<(u32, u32)>> = HashMap::new();
            while let Some(chunk) = self.build.next(ctx)? {
                let ci = chunks.len() as u32;
                for row_idx in 0..chunk.num_rows() {
                    let row_ref = chunk.row_ref(row_idx);
                    let key = eval_keys(&self.build_keys, &row_ref)?;
                    ht.entry(key).or_default().push((ci, row_idx as u32));
                }
                chunks.push(chunk);
            }
            let num_cols = chunks.first().map_or(0, |c| c.num_columns());
            self.build_data = Some(BuildData {
                chunks,
                ht,
                num_cols,
            });
        }

        let bd = self.build_data.as_ref().unwrap();

        // Probe phase: pull from probe side.
        loop {
            let chunk = match self.probe.next(ctx)? {
                Some(c) => c,
                None => return Ok(None),
            };

            let probe_ncols = chunk.num_columns();
            let total_cols = bd.num_cols + probe_ncols;
            let mut result = DataChunk::with_capacity(total_cols, chunk.num_rows());

            for row_idx in 0..chunk.num_rows() {
                let row_ref = chunk.row_ref(row_idx);
                let key = eval_keys(&self.probe_keys, &row_ref)?;
                if let Some(build_locs) = bd.ht.get(&key) {
                    for &(ci, ri) in build_locs {
                        let build_chunk = &bd.chunks[ci as usize];
                        let mut combined = build_chunk.get_row(ri as usize);
                        for col_idx in 0..probe_ncols {
                            combined.push(chunk.get_value(row_idx, col_idx));
                        }
                        result.append_row(&combined);
                    }
                }
            }

            if !result.is_empty() {
                return Ok(Some(result));
            }
            // No matches for this probe chunk, try next.
        }
    }
}

fn eval_keys<T: kyu_expression::Tuple + ?Sized>(
    keys: &[BoundExpression],
    tuple: &T,
) -> KyuResult<Vec<TypedValue>> {
    keys.iter().map(|k| evaluate(k, tuple)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    #[test]
    fn hash_join_basic() {
        let mut storage = MockStorage::new();
        // Left: id, name
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![
                    TypedValue::Int64(1),
                    TypedValue::String(SmolStr::new("Alice")),
                ],
                vec![
                    TypedValue::Int64(2),
                    TypedValue::String(SmolStr::new("Bob")),
                ],
            ],
        );
        // Right: id, score
        storage.insert_table(
            kyu_common::id::TableId(1),
            vec![
                vec![TypedValue::Int64(1), TypedValue::Int64(100)],
                vec![TypedValue::Int64(2), TypedValue::Int64(200)],
                vec![TypedValue::Int64(3), TypedValue::Int64(300)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);

        let build = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let probe = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(1),
        ));

        // Join on column 0 = column 0.
        let build_key = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        };
        let probe_key = BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        };

        let mut join = HashJoinOp::new(build, probe, vec![build_key], vec![probe_key]);
        let chunk = join.next(&ctx).unwrap().unwrap();
        // id=1 and id=2 match → 2 result rows, each with 4 columns.
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.num_columns(), 4);
        // After join: no more results.
        assert!(join.next(&ctx).unwrap().is_none());
    }
}
