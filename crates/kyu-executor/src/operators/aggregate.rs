//! Aggregate operator — hash-based GROUP BY + aggregation.

use hashbrown::HashMap;
use kyu_common::KyuResult;
use kyu_expression::{BoundExpression, evaluate};
use kyu_planner::{AggFunc, AggregateSpec};
use kyu_types::TypedValue;

use crate::context::ExecutionContext;
use crate::data_chunk::DataChunk;
use crate::physical_plan::PhysicalOperator;

pub struct AggregateOp {
    pub child: Box<PhysicalOperator>,
    pub group_by: Vec<BoundExpression>,
    pub aggregates: Vec<AggregateSpec>,
    result: Option<DataChunk>,
}

impl AggregateOp {
    pub fn new(
        child: PhysicalOperator,
        group_by: Vec<BoundExpression>,
        aggregates: Vec<AggregateSpec>,
    ) -> Self {
        Self {
            child: Box::new(child),
            group_by,
            aggregates,
            result: None,
        }
    }

    pub fn next(&mut self, ctx: &ExecutionContext<'_>) -> KyuResult<Option<DataChunk>> {
        if self.result.is_some() {
            // Already consumed.
            return Ok(None);
        }

        // Drain child and accumulate.
        let num_aggs = self.aggregates.len();
        let num_groups = self.group_by.len();

        // Map from group-by key → accumulator states.
        let mut groups: HashMap<Vec<TypedValue>, Vec<AccState>> = HashMap::new();
        let mut insertion_order: Vec<Vec<TypedValue>> = Vec::new();

        while let Some(chunk) = self.child.next(ctx)? {
            for row_idx in 0..chunk.num_rows() {
                let row_ref = chunk.row_ref(row_idx);

                let key: Vec<TypedValue> = self
                    .group_by
                    .iter()
                    .map(|expr| evaluate(expr, &row_ref))
                    .collect::<KyuResult<_>>()?;

                let accs = groups.entry(key).or_insert_with_key(|k| {
                    insertion_order.push(k.clone());
                    (0..num_aggs).map(|_| AccState::new()).collect()
                });

                for (i, agg) in self.aggregates.iter().enumerate() {
                    let val = if let Some(ref arg) = agg.arg {
                        evaluate(arg, &row_ref)?
                    } else {
                        TypedValue::Null
                    };
                    accs[i].accumulate(agg.resolved_func, &val);
                }
            }
        }

        // If no groups and no group-by keys (e.g., COUNT(*) with no rows),
        // produce one row with identity values.
        if groups.is_empty() && num_groups == 0 {
            let key = Vec::new();
            let accs: Vec<AccState> = (0..num_aggs).map(|_| AccState::new()).collect();
            groups.insert(key.clone(), accs);
            insertion_order.push(key);
        }

        // Build result DataChunk.
        let total_cols = num_groups + num_aggs;
        let mut result_chunk = DataChunk::with_capacity(total_cols, insertion_order.len());

        for key in &insertion_order {
            let accs = groups.get(key).unwrap();
            let mut row = key.clone();
            for (i, agg) in self.aggregates.iter().enumerate() {
                row.push(accs[i].finalize(agg.resolved_func));
            }
            result_chunk.append_row(&row);
        }

        self.result = Some(DataChunk::empty(0)); // Mark as consumed.

        if result_chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result_chunk))
        }
    }
}

/// Per-group accumulator state.
struct AccState {
    count: i64,
    sum_i64: i64,
    sum_f64: f64,
    min: Option<TypedValue>,
    max: Option<TypedValue>,
    collected: Vec<TypedValue>,
    is_float: bool,
}

impl AccState {
    fn new() -> Self {
        Self {
            count: 0,
            sum_i64: 0,
            sum_f64: 0.0,
            min: None,
            max: None,
            collected: Vec::new(),
            is_float: false,
        }
    }

    fn accumulate(&mut self, func: AggFunc, val: &TypedValue) {
        match func {
            AggFunc::Count => {
                self.count += 1;
            }
            AggFunc::Sum => {
                match val {
                    TypedValue::Int64(v) => self.sum_i64 += v,
                    TypedValue::Int32(v) => self.sum_i64 += *v as i64,
                    TypedValue::Double(v) => {
                        self.sum_f64 += v;
                        self.is_float = true;
                    }
                    TypedValue::Float(v) => {
                        self.sum_f64 += *v as f64;
                        self.is_float = true;
                    }
                    _ => {}
                }
                self.count += 1;
            }
            AggFunc::Avg => {
                match val {
                    TypedValue::Int64(v) => self.sum_f64 += *v as f64,
                    TypedValue::Int32(v) => self.sum_f64 += *v as f64,
                    TypedValue::Double(v) => self.sum_f64 += v,
                    TypedValue::Float(v) => self.sum_f64 += *v as f64,
                    _ => {}
                }
                if *val != TypedValue::Null {
                    self.count += 1;
                }
            }
            AggFunc::Min => {
                if *val != TypedValue::Null {
                    self.min = Some(match &self.min {
                        None => val.clone(),
                        Some(current) => {
                            if typed_value_lt(val, current) {
                                val.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
            AggFunc::Max => {
                if *val != TypedValue::Null {
                    self.max = Some(match &self.max {
                        None => val.clone(),
                        Some(current) => {
                            if typed_value_lt(current, val) {
                                val.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
            AggFunc::Collect => {
                self.collected.push(val.clone());
            }
        }
    }

    fn finalize(&self, func: AggFunc) -> TypedValue {
        match func {
            AggFunc::Count => TypedValue::Int64(self.count),
            AggFunc::Sum => {
                if self.is_float {
                    TypedValue::Double(self.sum_f64 + self.sum_i64 as f64)
                } else {
                    TypedValue::Int64(self.sum_i64)
                }
            }
            AggFunc::Avg => {
                if self.count == 0 {
                    TypedValue::Null
                } else {
                    TypedValue::Double(self.sum_f64 / self.count as f64)
                }
            }
            AggFunc::Min => self.min.clone().unwrap_or(TypedValue::Null),
            AggFunc::Max => self.max.clone().unwrap_or(TypedValue::Null),
            AggFunc::Collect => TypedValue::List(self.collected.clone()),
        }
    }
}

fn typed_value_lt(a: &TypedValue, b: &TypedValue) -> bool {
    match (a, b) {
        (TypedValue::Int64(a), TypedValue::Int64(b)) => a < b,
        (TypedValue::Int32(a), TypedValue::Int32(b)) => a < b,
        (TypedValue::Double(a), TypedValue::Double(b)) => a < b,
        (TypedValue::Float(a), TypedValue::Float(b)) => a < b,
        (TypedValue::String(a), TypedValue::String(b)) => a < b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    fn make_storage() -> MockStorage {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::String(SmolStr::new("A")), TypedValue::Int64(10)],
                vec![TypedValue::String(SmolStr::new("B")), TypedValue::Int64(20)],
                vec![TypedValue::String(SmolStr::new("A")), TypedValue::Int64(30)],
            ],
        );
        storage
    }

    #[test]
    fn count_star_no_group_by() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut agg = AggregateOp::new(
            scan,
            vec![],
            vec![AggregateSpec {
                function_name: SmolStr::new("count"),
                resolved_func: AggFunc::Count,
                arg: None,
                distinct: false,
                result_type: LogicalType::Int64,
                alias: SmolStr::new("cnt"),
            }],
        );
        let chunk = agg.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 1);
        assert_eq!(chunk.get_row(0), vec![TypedValue::Int64(3)]);
    }

    #[test]
    fn sum_with_group_by() {
        let storage = make_storage();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut agg = AggregateOp::new(
            scan,
            vec![BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::String,
            }],
            vec![AggregateSpec {
                function_name: SmolStr::new("sum"),
                resolved_func: AggFunc::Sum,
                arg: Some(BoundExpression::Variable {
                    index: 1,
                    result_type: LogicalType::Int64,
                }),
                distinct: false,
                result_type: LogicalType::Int64,
                alias: SmolStr::new("total"),
            }],
        );
        let chunk = agg.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 2); // Group A and B
        let row0 = chunk.get_row(0);
        let row1 = chunk.get_row(1);
        assert_eq!(row0[0], TypedValue::String(SmolStr::new("A")));
        assert_eq!(row0[1], TypedValue::Int64(40));
        assert_eq!(row1[0], TypedValue::String(SmolStr::new("B")));
        assert_eq!(row1[1], TypedValue::Int64(20));
    }

    #[test]
    fn count_star_empty_input() {
        let storage = MockStorage::new();
        let ctx = ExecutionContext::new(kyu_catalog::CatalogContent::new(), &storage);
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(99),
        ));
        let mut agg = AggregateOp::new(
            scan,
            vec![],
            vec![AggregateSpec {
                function_name: SmolStr::new("count"),
                resolved_func: AggFunc::Count,
                arg: None,
                distinct: false,
                result_type: LogicalType::Int64,
                alias: SmolStr::new("cnt"),
            }],
        );
        let chunk = agg.next(&ctx).unwrap().unwrap();
        assert_eq!(chunk.num_rows(), 1);
        assert_eq!(chunk.get_row(0), vec![TypedValue::Int64(0)]);
    }
}
