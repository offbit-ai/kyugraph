//! OrderBy operator — materializes all rows, sorts, then emits.

use kyu_common::KyuResult;
use kyu_expression::{BoundExpression, evaluate};
use kyu_parser::ast::SortOrder;
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct OrderByOp {
    pub child: Box<PhysicalOperator>,
    pub order_by: Vec<(BoundExpression, SortOrder)>,
    result: Option<DataChunk>,
}

impl OrderByOp {
    pub fn new(child: PhysicalOperator, order_by: Vec<(BoundExpression, SortOrder)>) -> Self {
        Self {
            child: Box::new(child),
            order_by,
            result: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.result.is_some() {
            return Ok(None);
        }

        // Collect all child chunks without merging (avoids ensure_owned).
        let mut all_chunks: Vec<DataChunk> = Vec::new();
        while let Some(chunk) = self.child.next(ctx)? {
            all_chunks.push(chunk);
        }

        let total_rows: usize = all_chunks.iter().map(|c| c.num_rows()).sum();
        if total_rows == 0 {
            self.result = Some(DataChunk::empty(0));
            return Ok(None);
        }

        let num_cols = all_chunks[0].num_columns();

        // Build a (chunk_idx, local_row) index + evaluate sort keys.
        let num_keys = self.order_by.len();
        let mut row_locs: Vec<(usize, usize)> = Vec::with_capacity(total_rows);
        let mut sort_keys: Vec<Vec<TypedValue>> = Vec::with_capacity(total_rows);
        for (ci, chunk) in all_chunks.iter().enumerate() {
            for row_idx in 0..chunk.num_rows() {
                row_locs.push((ci, row_idx));
                let row_ref = chunk.row_ref(row_idx);
                let keys: Vec<TypedValue> = self
                    .order_by
                    .iter()
                    .map(|(expr, _)| evaluate(expr, &row_ref))
                    .collect::<KyuResult<_>>()?;
                sort_keys.push(keys);
            }
        }

        // Sort a permutation array — swaps move 8-byte indices, not full rows.
        let mut indices: Vec<usize> = (0..total_rows).collect();
        let order_specs: Vec<SortOrder> = self.order_by.iter().map(|(_, order)| *order).collect();
        indices.sort_by(|&a, &b| {
            #[allow(clippy::needless_range_loop)]
            for i in 0..num_keys {
                let cmp = compare_values(&sort_keys[a][i], &sort_keys[b][i]);
                let cmp = match order_specs.get(i) {
                    Some(SortOrder::Descending) => cmp.reverse(),
                    _ => cmp,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Build output by gathering from source chunks via (chunk_idx, local_row).
        let mut result_chunk = DataChunk::with_capacity(num_cols, total_rows);
        for &idx in &indices {
            let (ci, ri) = row_locs[idx];
            result_chunk.append_row_from_chunk(&all_chunks[ci], ri);
        }

        self.result = Some(DataChunk::empty(0));
        Ok(Some(result_chunk))
    }
}

fn compare_values(a: &TypedValue, b: &TypedValue) -> std::cmp::Ordering {
    match (a, b) {
        (TypedValue::Null, TypedValue::Null) => std::cmp::Ordering::Equal,
        (TypedValue::Null, _) => std::cmp::Ordering::Greater, // NULLs sort last
        (_, TypedValue::Null) => std::cmp::Ordering::Less,
        (TypedValue::Int64(a), TypedValue::Int64(b)) => a.cmp(b),
        (TypedValue::Int32(a), TypedValue::Int32(b)) => a.cmp(b),
        (TypedValue::Double(a), TypedValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (TypedValue::Float(a), TypedValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (TypedValue::String(a), TypedValue::String(b)) => a.cmp(b),
        (TypedValue::Bool(a), TypedValue::Bool(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::LogicalType;

    #[test]
    fn sort_ascending() {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(30)],
                vec![TypedValue::Int64(10)],
                vec![TypedValue::Int64(20)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);

        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut sort = OrderByOp::new(
            scan,
            vec![(
                BoundExpression::Variable {
                    index: 0,
                    result_type: LogicalType::Int64,
                },
                SortOrder::Ascending,
            )],
        );
        let chunk = sort.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(10));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(20));
        assert_eq!(chunk.get_value(2, 0), TypedValue::Int64(30));
    }

    #[test]
    fn sort_descending() {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::Int64(10)],
                vec![TypedValue::Int64(30)],
                vec![TypedValue::Int64(20)],
            ],
        );
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);

        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut sort = OrderByOp::new(
            scan,
            vec![(
                BoundExpression::Variable {
                    index: 0,
                    result_type: LogicalType::Int64,
                },
                SortOrder::Descending,
            )],
        );
        let chunk = sort.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.get_value(0, 0), TypedValue::Int64(30));
        assert_eq!(chunk.get_value(1, 0), TypedValue::Int64(20));
        assert_eq!(chunk.get_value(2, 0), TypedValue::Int64(10));
    }
}
