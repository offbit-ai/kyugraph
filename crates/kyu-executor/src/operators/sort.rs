//! OrderBy operator — materializes all rows, sorts, then emits.

use kyu_common::KyuResult;
use kyu_expression::{evaluate, BoundExpression};
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

        // Merge all child chunks into a single column-major DataChunk.
        let mut merged: Option<DataChunk> = None;
        while let Some(chunk) = self.child.next(ctx)? {
            match merged {
                None => merged = Some(chunk),
                Some(ref mut m) => m.append(&chunk),
            }
        }

        let merged = match merged {
            Some(m) if !m.is_empty() => m,
            _ => {
                self.result = Some(DataChunk::empty(0));
                return Ok(None);
            }
        };

        let n = merged.num_rows();
        let num_cols = merged.num_columns();

        // Evaluate sort keys per row using RowRef (only key columns, not full rows).
        let num_keys = self.order_by.len();
        let mut sort_keys: Vec<Vec<TypedValue>> = Vec::with_capacity(n);
        for row_idx in 0..n {
            let row_ref = merged.row_ref(row_idx);
            let keys: Vec<TypedValue> = self
                .order_by
                .iter()
                .map(|(expr, _)| evaluate(expr, &row_ref))
                .collect::<KyuResult<_>>()?;
            sort_keys.push(keys);
        }

        // Sort an index permutation array — swaps move 8-byte indices, not full rows.
        let mut indices: Vec<usize> = (0..n).collect();
        let order_specs: Vec<SortOrder> =
            self.order_by.iter().map(|(_, order)| *order).collect();
        indices.sort_by(|&a, &b| {
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

        // Build output columns by reading source in permuted order.
        let mut result_chunk = DataChunk::with_capacity(num_cols, n);
        for &idx in &indices {
            result_chunk.append_row_from_chunk(&merged, idx);
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
        (TypedValue::Double(a), TypedValue::Double(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (TypedValue::Float(a), TypedValue::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
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
        assert_eq!(chunk.column(0)[0], TypedValue::Int64(10));
        assert_eq!(chunk.column(0)[1], TypedValue::Int64(20));
        assert_eq!(chunk.column(0)[2], TypedValue::Int64(30));
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
        assert_eq!(chunk.column(0)[0], TypedValue::Int64(30));
        assert_eq!(chunk.column(0)[1], TypedValue::Int64(20));
        assert_eq!(chunk.column(0)[2], TypedValue::Int64(10));
    }
}
