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

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
        if self.result.is_some() {
            return Ok(None);
        }

        // Materialize all rows.
        let mut all_rows: Vec<Vec<TypedValue>> = Vec::new();
        while let Some(chunk) = self.child.next(ctx)? {
            for row_idx in 0..chunk.num_rows() {
                all_rows.push(chunk.get_row(row_idx));
            }
        }

        if all_rows.is_empty() {
            self.result = Some(DataChunk::empty(0));
            return Ok(None);
        }

        // Compute sort keys for each row.
        let mut keyed_rows: Vec<(Vec<TypedValue>, Vec<TypedValue>)> = Vec::new();
        for row in &all_rows {
            let keys: Vec<TypedValue> = self
                .order_by
                .iter()
                .map(|(expr, _)| evaluate(expr, row))
                .collect::<KyuResult<_>>()?;
            keyed_rows.push((keys, row.clone()));
        }

        // Sort by keys.
        let order_specs: Vec<SortOrder> =
            self.order_by.iter().map(|(_, order)| *order).collect();
        keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| {
            for (i, (a, b)) in keys_a.iter().zip(keys_b.iter()).enumerate() {
                let cmp = compare_values(a, b);
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

        // Build result.
        let num_cols = all_rows[0].len();
        let sorted_rows: Vec<Vec<TypedValue>> =
            keyed_rows.into_iter().map(|(_, row)| row).collect();
        let chunk = DataChunk::from_rows(&sorted_rows, num_cols);

        self.result = Some(DataChunk::empty(0));
        Ok(Some(chunk))
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
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage);

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
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage);

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
