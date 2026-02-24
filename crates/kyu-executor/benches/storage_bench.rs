//! Storage-level micro-benchmarks for ValueVector, DataChunk, and SelectionVector.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kyu_executor::data_chunk::DataChunk;
use kyu_executor::value_vector::{FlatVector, SelectionVector, ValueVector};
use kyu_storage::ColumnChunkData;
use kyu_types::{LogicalType, TypedValue};

const MICRO_SCALES: &[usize] = &[10_000, 100_000];

/// Build a FlatVector of i64 values.
fn make_flat_i64(n: usize) -> FlatVector {
    let mut chunk = ColumnChunkData::new(LogicalType::Int64, n as u64);
    for i in 0..n {
        chunk.append_value::<i64>(i as i64);
    }
    FlatVector::from_column_chunk(&chunk, n)
}

/// Build an Owned ValueVector of i64 values.
fn make_owned_i64(n: usize) -> ValueVector {
    let data: Vec<TypedValue> = (0..n).map(|i| TypedValue::Int64(i as i64)).collect();
    ValueVector::Owned(data)
}

// ---------------------------------------------------------------------------
// Group 1: ValueVector get_value throughput
// ---------------------------------------------------------------------------

fn bench_value_vector_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_vector_get");

    for &scale in MICRO_SCALES {
        group.throughput(Throughput::Elements(scale as u64));

        // FlatVector (optimized path)
        let flat = make_flat_i64(scale);
        group.bench_with_input(BenchmarkId::new("flat_i64", scale), &scale, |b, &n| {
            b.iter(|| {
                let mut sum = 0i64;
                for i in 0..n {
                    if let TypedValue::Int64(v) = flat.get_value(i) {
                        sum = sum.wrapping_add(v);
                    }
                }
                sum
            });
        });

        // Owned Vec<TypedValue> (baseline)
        let owned = make_owned_i64(scale);
        group.bench_with_input(BenchmarkId::new("owned_i64", scale), &scale, |b, &n| {
            b.iter(|| {
                let mut sum = 0i64;
                for i in 0..n {
                    if let TypedValue::Int64(v) = owned.get_value(i) {
                        sum = sum.wrapping_add(v);
                    }
                }
                sum
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: DataChunk row iteration
// ---------------------------------------------------------------------------

fn bench_data_chunk_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_chunk_iteration");

    let chunk_scales: &[usize] = &[2_048, 10_000];

    for &scale in chunk_scales {
        group.throughput(Throughput::Elements(scale as u64));

        // Build a 5-column DataChunk: 3 i64 + 1 String + 1 Bool (via Owned).
        let rows: Vec<Vec<TypedValue>> = (0..scale)
            .map(|i| {
                vec![
                    TypedValue::Int64(i as i64),
                    TypedValue::Int64((i * 2) as i64),
                    TypedValue::Int64((i * 3) as i64),
                    TypedValue::String(smol_str::SmolStr::new(format!("row_{}", i))),
                    TypedValue::Bool(i % 2 == 0),
                ]
            })
            .collect();
        let chunk = DataChunk::from_rows(&rows, 5);

        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &n| {
            b.iter(|| {
                let mut sum = 0i64;
                for row in 0..n {
                    for col in 0..5 {
                        if let TypedValue::Int64(v) = chunk.get_value(row, col) {
                            sum = sum.wrapping_add(v);
                        }
                    }
                }
                sum
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: SelectionVector filter throughput
// ---------------------------------------------------------------------------

fn bench_selection_vector_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("selection_vector_filter");

    for &scale in MICRO_SCALES {
        // Build a single-column DataChunk.
        let rows: Vec<Vec<TypedValue>> = (0..scale)
            .map(|i| vec![TypedValue::Int64(i as i64)])
            .collect();
        let chunk = DataChunk::from_rows(&rows, 1);

        for &(name, pct) in &[("10pct", 10), ("50pct", 50), ("90pct", 90)] {
            let indices: Vec<u32> = (0..scale as u32)
                .filter(|i| (*i as usize * 100 / scale) < pct)
                .collect();
            let selected_count = indices.len();
            let filtered = chunk
                .clone()
                .with_selection(SelectionVector::from_indices(indices));

            group.throughput(Throughput::Elements(selected_count as u64));
            group.bench_with_input(BenchmarkId::new(name, scale), &selected_count, |b, &n| {
                b.iter(|| {
                    let mut sum = 0i64;
                    for row in 0..n {
                        if let TypedValue::Int64(v) = filtered.get_value(row, 0) {
                            sum = sum.wrapping_add(v);
                        }
                    }
                    sum
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_value_vector_get,
    bench_data_chunk_iteration,
    bench_selection_vector_filter,
);
criterion_main!(benches);
