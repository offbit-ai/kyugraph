//! Aggregate operator — hash-based GROUP BY + aggregation.

use hashbrown::HashMap;
use kyu_common::KyuResult;
use kyu_expression::{evaluate, BoundExpression};
use kyu_planner::AggregateSpec;
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

    pub fn next(&mut self, ctx: &ExecutionContext) -> KyuResult<Option<DataChunk>> {
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
                let row = chunk.get_row(row_idx);

                let key: Vec<TypedValue> = self
                    .group_by
                    .iter()
                    .map(|expr| evaluate(expr, &row))
                    .collect::<KyuResult<_>>()?;

                let accs = groups.entry(key.clone()).or_insert_with(|| {
                    insertion_order.push(key.clone());
                    (0..num_aggs).map(|_| AccState::new()).collect()
                });

                for (i, agg) in self.aggregates.iter().enumerate() {
                    let val = if let Some(ref arg) = agg.arg {
                        evaluate(arg, &row)?
                    } else {
                        TypedValue::Null
                    };
                    accs[i].accumulate(&agg.function_name, &val);
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
        let mut result_chunk = DataChunk::empty(total_cols);

        for key in &insertion_order {
            let accs = groups.get(key).unwrap();
            let mut row = key.clone();
            for (i, agg) in self.aggregates.iter().enumerate() {
                row.push(accs[i].finalize(&agg.function_name));
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

    fn accumulate(&mut self, func: &str, val: &TypedValue) {
        match func.to_lowercase().as_str() {
            "count" => {
                if *val != TypedValue::Null {
                    self.count += 1;
                } else {
                    // COUNT(*) counts all rows including NULL.
                    self.count += 1;
                }
            }
            "sum" => {
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
            "avg" => {
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
            "min" => {
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
            "max" => {
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
            "collect" => {
                self.collected.push(val.clone());
            }
            _ => {}
        }
    }

    fn finalize(&self, func: &str) -> TypedValue {
        match func.to_lowercase().as_str() {
            "count" => TypedValue::Int64(self.count),
            "sum" => {
                if self.is_float {
                    TypedValue::Double(self.sum_f64 + self.sum_i64 as f64)
                } else {
                    TypedValue::Int64(self.sum_i64)
                }
            }
            "avg" => {
                if self.count == 0 {
                    TypedValue::Null
                } else {
                    TypedValue::Double(self.sum_f64 / self.count as f64)
                }
            }
            "min" => self.min.clone().unwrap_or(TypedValue::Null),
            "max" => self.max.clone().unwrap_or(TypedValue::Null),
            "collect" => {
                // Return as a list — but TypedValue doesn't have List yet, use Null as placeholder.
                TypedValue::Null
            }
            _ => TypedValue::Null,
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

    fn make_ctx() -> ExecutionContext {
        let mut storage = MockStorage::new();
        storage.insert_table(
            kyu_common::id::TableId(0),
            vec![
                vec![TypedValue::String(SmolStr::new("A")), TypedValue::Int64(10)],
                vec![TypedValue::String(SmolStr::new("B")), TypedValue::Int64(20)],
                vec![TypedValue::String(SmolStr::new("A")), TypedValue::Int64(30)],
            ],
        );
        ExecutionContext::new(kyu_catalog::CatalogContent::new(), storage)
    }

    #[test]
    fn count_star_no_group_by() {
        let ctx = make_ctx();
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(0),
        ));
        let mut agg = AggregateOp::new(
            scan,
            vec![],
            vec![AggregateSpec {
                function_name: SmolStr::new("count"),
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
        let ctx = make_ctx();
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
        // Group A: sum=40, Group B: sum=20
        let row0 = chunk.get_row(0);
        let row1 = chunk.get_row(1);
        assert_eq!(row0[0], TypedValue::String(SmolStr::new("A")));
        assert_eq!(row0[1], TypedValue::Int64(40));
        assert_eq!(row1[0], TypedValue::String(SmolStr::new("B")));
        assert_eq!(row1[1], TypedValue::Int64(20));
    }

    #[test]
    fn count_star_empty_input() {
        let ctx = ExecutionContext::new(
            kyu_catalog::CatalogContent::new(),
            MockStorage::new(),
        );
        let scan = PhysicalOperator::ScanNode(crate::operators::scan::ScanNodeOp::new(
            kyu_common::id::TableId(99),
        ));
        let mut agg = AggregateOp::new(
            scan,
            vec![],
            vec![AggregateSpec {
                function_name: SmolStr::new("count"),
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
