//! JIT expression evaluation benchmarks.
//!
//! Compares three evaluation tiers on identical workloads:
//! - **Scalar**: tree-walking `evaluate()` per row
//! - **Batch**: pattern-matched `evaluate_filter_batch` / `evaluate_column`
//! - **JIT**: Cranelift-compiled native code on flat byte buffers

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kyu_executor::batch_eval::{evaluate_column, evaluate_filter_batch};
use kyu_executor::data_chunk::DataChunk;
use kyu_executor::value_vector::{FlatVector, SelectionVector, ValueVector};
use kyu_expression::{BoundExpression, evaluate};
use kyu_parser::ast::{BinaryOp, ComparisonOp};
use kyu_storage::ColumnChunkData;
use kyu_types::{LogicalType, TypedValue};

#[cfg(feature = "jit")]
use kyu_executor::jit::{compile_filter, compile_projection};

const SCALES: &[usize] = &[1_000, 10_000, 100_000];

/// Build a single i64 FlatVector DataChunk.
fn make_i64_flat_chunk(n: usize) -> DataChunk {
    let mut col = ColumnChunkData::new(LogicalType::Int64, n as u64);
    for i in 0..n {
        col.append_value::<i64>(i as i64);
    }
    let flat = FlatVector::from_column_chunk(&col, n);
    DataChunk::from_vectors(vec![ValueVector::Flat(flat)], SelectionVector::identity(n))
}

/// Build a two-column i64 FlatVector DataChunk.
fn make_two_i64_flat_chunk(n: usize) -> DataChunk {
    let mut col_a = ColumnChunkData::new(LogicalType::Int64, n as u64);
    let mut col_b = ColumnChunkData::new(LogicalType::Int64, n as u64);
    for i in 0..n {
        col_a.append_value::<i64>(i as i64);
        col_b.append_value::<i64>((i * 2) as i64);
    }
    DataChunk::from_vectors(
        vec![
            ValueVector::Flat(FlatVector::from_column_chunk(&col_a, n)),
            ValueVector::Flat(FlatVector::from_column_chunk(&col_b, n)),
        ],
        SelectionVector::identity(n),
    )
}

/// `col[0] > N/2` — selects ~50% of rows.
fn filter_gt_half(n: usize) -> BoundExpression {
    BoundExpression::Comparison {
        op: ComparisonOp::Gt,
        left: Box::new(BoundExpression::Variable {
            index: 0,
            result_type: LogicalType::Int64,
        }),
        right: Box::new(BoundExpression::Literal {
            value: TypedValue::Int64((n / 2) as i64),
            result_type: LogicalType::Int64,
        }),
    }
}

/// `col[0] * 2 + col[1]`
fn projection_arithmetic() -> BoundExpression {
    BoundExpression::BinaryOp {
        op: BinaryOp::Add,
        left: Box::new(BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(2),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        }),
        right: Box::new(BoundExpression::Variable {
            index: 1,
            result_type: LogicalType::Int64,
        }),
        result_type: LogicalType::Int64,
    }
}

/// `(col[0] > N/4) AND (col[0] < 3*N/4)` — compound predicate.
fn filter_compound(n: usize) -> BoundExpression {
    BoundExpression::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64((n / 4) as i64),
                result_type: LogicalType::Int64,
            }),
        }),
        right: Box::new(BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64((3 * n / 4) as i64),
                result_type: LogicalType::Int64,
            }),
        }),
        result_type: LogicalType::Bool,
    }
}

// ---------------------------------------------------------------------------
// Filter benchmarks
// ---------------------------------------------------------------------------

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_gt_half");

    for &n in SCALES {
        group.throughput(Throughput::Elements(n as u64));
        let chunk = make_i64_flat_chunk(n);
        let expr = filter_gt_half(n);

        // Scalar: evaluate() per row
        group.bench_with_input(BenchmarkId::new("scalar", n), &n, |b, &n| {
            b.iter(|| {
                let mut count = 0u32;
                for row_idx in 0..n {
                    let val = evaluate(&expr, &chunk.row_ref(row_idx)).unwrap();
                    if val == TypedValue::Bool(true) {
                        count += 1;
                    }
                }
                count
            });
        });

        // Batch: evaluate_filter_batch
        group.bench_with_input(BenchmarkId::new("batch", n), &n, |b, _| {
            b.iter(|| evaluate_filter_batch(&expr, &chunk).unwrap().unwrap());
        });

        // JIT: compiled filter
        #[cfg(feature = "jit")]
        {
            let compiled = compile_filter(&expr).expect("JIT compilation failed");
            let flat = match chunk.column(0) {
                ValueVector::Flat(f) => f,
                _ => unreachable!(),
            };
            let col_ptrs = vec![flat.data_ptr()];
            let null_ptrs = vec![flat.null_mask().data().as_ptr()];
            let sel_ptr = chunk.selection().indices_ptr();

            group.bench_with_input(BenchmarkId::new("jit", n), &n, |b, &n| {
                let mut out = vec![0u32; n];
                b.iter(|| unsafe {
                    compiled.execute(&col_ptrs, &null_ptrs, sel_ptr, n as u32, &mut out)
                });
            });
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Compound filter benchmarks
// ---------------------------------------------------------------------------

fn bench_filter_compound(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_compound");

    for &n in SCALES {
        group.throughput(Throughput::Elements(n as u64));
        let chunk = make_i64_flat_chunk(n);
        let expr = filter_compound(n);

        // Scalar
        group.bench_with_input(BenchmarkId::new("scalar", n), &n, |b, &n| {
            b.iter(|| {
                let mut count = 0u32;
                for row_idx in 0..n {
                    let val = evaluate(&expr, &chunk.row_ref(row_idx)).unwrap();
                    if val == TypedValue::Bool(true) {
                        count += 1;
                    }
                }
                count
            });
        });

        // Batch
        group.bench_with_input(BenchmarkId::new("batch", n), &n, |b, _| {
            b.iter(|| evaluate_filter_batch(&expr, &chunk).unwrap().unwrap());
        });

        // JIT
        #[cfg(feature = "jit")]
        {
            let compiled = compile_filter(&expr).expect("JIT compilation failed");
            let flat = match chunk.column(0) {
                ValueVector::Flat(f) => f,
                _ => unreachable!(),
            };
            let col_ptrs = vec![flat.data_ptr()];
            let null_ptrs = vec![flat.null_mask().data().as_ptr()];
            let sel_ptr = chunk.selection().indices_ptr();

            group.bench_with_input(BenchmarkId::new("jit", n), &n, |b, &n| {
                let mut out = vec![0u32; n];
                b.iter(|| unsafe {
                    compiled.execute(&col_ptrs, &null_ptrs, sel_ptr, n as u32, &mut out)
                });
            });
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Projection benchmarks
// ---------------------------------------------------------------------------

fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection_arithmetic");

    for &n in SCALES {
        group.throughput(Throughput::Elements(n as u64));
        let chunk = make_two_i64_flat_chunk(n);
        let expr = projection_arithmetic();

        // Scalar
        group.bench_with_input(BenchmarkId::new("scalar", n), &n, |b, &n| {
            b.iter(|| {
                let mut results = Vec::with_capacity(n);
                for row_idx in 0..n {
                    results.push(evaluate(&expr, &chunk.row_ref(row_idx)).unwrap());
                }
                results
            });
        });

        // Batch
        group.bench_with_input(BenchmarkId::new("batch", n), &n, |b, _| {
            b.iter(|| evaluate_column(&expr, &chunk).unwrap().unwrap());
        });

        // JIT
        #[cfg(feature = "jit")]
        {
            let compiled = compile_projection(&expr).expect("JIT compilation failed");
            let stride = 8usize; // i64

            let mut col_ptrs = Vec::new();
            let mut null_ptrs = Vec::new();
            for &col_idx in &compiled.col_indices {
                let flat = match chunk.column(col_idx as usize) {
                    ValueVector::Flat(f) => f,
                    _ => unreachable!(),
                };
                col_ptrs.push(flat.data_ptr());
                null_ptrs.push(flat.null_mask().data().as_ptr());
            }
            let sel_ptr = chunk.selection().indices_ptr();

            group.bench_with_input(BenchmarkId::new("jit", n), &n, |b, &n| {
                let mut out_data = vec![0u8; n * stride];
                let null_entries = n.div_ceil(64);
                let mut out_nulls = vec![0u64; null_entries];
                b.iter(|| unsafe {
                    compiled.execute(
                        &col_ptrs,
                        &null_ptrs,
                        sel_ptr,
                        n as u32,
                        &mut out_data,
                        &mut out_nulls,
                    )
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_filter,
    bench_filter_compound,
    bench_projection
);
criterion_main!(benches);
