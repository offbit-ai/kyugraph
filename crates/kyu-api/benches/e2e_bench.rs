//! End-to-end Cypher benchmarks modeled on Kuzu/RyuGraph benchmark queries.
//!
//! Uses the real LDBC SNB Comment/Person schema. Data is generated
//! programmatically via `insert_row()` to avoid external file dependencies.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kyu_api::Database;
use kyu_types::TypedValue;
use smol_str::SmolStr;

const SCALES: &[usize] = &[1_000, 10_000, 100_000];

const BROWSERS: &[&str] = &["Chrome", "Firefox", "Safari", "Edge"];

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
];

/// Create a database with LDBC SNB Comment + Person tables and bulk-loaded data.
fn setup_db(num_comments: usize, num_persons: usize) -> Database {
    let db = Database::in_memory();
    let conn = db.connect();

    conn.query(
        "CREATE NODE TABLE Comment (id INT64, creationDate INT64, locationIP STRING, browserUsed STRING, content STRING, length INT64, PRIMARY KEY (id))",
    ).unwrap();

    conn.query(
        "CREATE NODE TABLE Person (id INT64, firstName STRING, lastName STRING, gender STRING, birthday INT64, creationDate INT64, locationIP STRING, browserUsed STRING, PRIMARY KEY (id))",
    ).unwrap();

    // Get table IDs from catalog.
    let snapshot = db.catalog().read();
    let comment_tid = snapshot.find_by_name("Comment").unwrap().table_id();
    let person_tid = snapshot.find_by_name("Person").unwrap().table_id();
    drop(snapshot);

    // Bulk-insert Comments.
    {
        let mut storage = db.storage().write().unwrap();
        for i in 0..num_comments {
            storage
                .insert_row(
                    comment_tid,
                    &[
                        TypedValue::Int64(i as i64),
                        TypedValue::Int64(1_300_000_000 + i as i64 * 1000),
                        TypedValue::String(SmolStr::new(format!("1.2.3.{}", i % 256))),
                        TypedValue::String(SmolStr::new(BROWSERS[i % BROWSERS.len()])),
                        TypedValue::String(SmolStr::new(format!("comment text {}", i))),
                        TypedValue::Int64((i % 2000) as i64),
                    ],
                )
                .unwrap();
        }
    }

    // Bulk-insert Persons.
    {
        let mut storage = db.storage().write().unwrap();
        for i in 0..num_persons {
            storage
                .insert_row(
                    person_tid,
                    &[
                        TypedValue::Int64(i as i64),
                        TypedValue::String(SmolStr::new(FIRST_NAMES[i % FIRST_NAMES.len()])),
                        TypedValue::String(SmolStr::new(LAST_NAMES[i % LAST_NAMES.len()])),
                        TypedValue::String(SmolStr::new(if i % 2 == 0 { "male" } else { "female" })),
                        TypedValue::Int64(19_700_101 + (i % 20_000) as i64),
                        TypedValue::Int64(1_300_000_000 + i as i64 * 500),
                        TypedValue::String(SmolStr::new(format!("10.0.{}.{}", i % 256, (i / 256) % 256))),
                        TypedValue::String(SmolStr::new(BROWSERS[i % BROWSERS.len()])),
                    ],
                )
                .unwrap();
        }
    }

    db
}

// ---------------------------------------------------------------------------
// Group 1: Fixed-Size Sequential Scan
// Mirrors Kuzu benchmark/queries/ldbc-sf100/fixed_size_seq_scan/
// ---------------------------------------------------------------------------

fn bench_fixed_size_seq_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_size_seq_scan");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) RETURN c.id, c.length, c.creationDate")
                    .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: Variable-Size Sequential Scan
// Mirrors Kuzu benchmark/queries/ldbc-sf100/var_size_seq_scan/
// ---------------------------------------------------------------------------

fn bench_var_size_seq_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_size_seq_scan");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) RETURN c.browserUsed").unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: Scan After Filter
// Mirrors Kuzu benchmark/queries/ldbc-sf100/scan_after_filter/q01
// ---------------------------------------------------------------------------

fn bench_scan_after_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_after_filter");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) WHERE c.length < 10 RETURN c.id")
                    .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 4: Filter — varying selectivity
// Mirrors Kuzu benchmark/queries/ldbc-sf100/filter/q14-q18
// ---------------------------------------------------------------------------

fn bench_filter_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_selectivity");

    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));

        // q14: ~0.15% selectivity (length < 3 out of 0..1999)
        group.bench_with_input(
            BenchmarkId::new("low_sel", scale),
            &scale,
            |b, _| {
                b.iter(|| {
                    conn.query("MATCH (c:Comment) WHERE c.length < 3 RETURN count(*)")
                        .unwrap();
                });
            },
        );

        // q15: ~7.5% selectivity
        group.bench_with_input(
            BenchmarkId::new("med_sel", scale),
            &scale,
            |b, _| {
                b.iter(|| {
                    conn.query("MATCH (c:Comment) WHERE c.length < 150 RETURN count(*)")
                        .unwrap();
                });
            },
        );

        // q16: disjunction
        group.bench_with_input(
            BenchmarkId::new("disjunction", scale),
            &scale,
            |b, _| {
                b.iter(|| {
                    conn.query(
                        "MATCH (c:Comment) WHERE c.length < 5 OR c.length > 1900 RETURN count(*)",
                    )
                    .unwrap();
                });
            },
        );

        // q18: string prefix
        group.bench_with_input(
            BenchmarkId::new("string_prefix", scale),
            &scale,
            |b, _| {
                b.iter(|| {
                    conn.query(
                        "MATCH (c:Comment) WHERE c.browserUsed STARTS WITH 'Ch' RETURN count(*)",
                    )
                    .unwrap();
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 5: Fixed-Size Expression Evaluator
// Mirrors Kuzu benchmark/queries/ldbc-sf100/fixed_size_expr_evaluator/q07
// ---------------------------------------------------------------------------

fn bench_expr_evaluator(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_size_expr_evaluator");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) RETURN c.length * 2 * 2 * 2 * 2")
                    .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 6: Aggregation
// Mirrors Kuzu benchmark/queries/ldbc-sf100/aggregation/q24
// ---------------------------------------------------------------------------

fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) RETURN c.length % 10, count(c.id)")
                    .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 7: Order By
// Mirrors Kuzu benchmark/queries/ldbc-sf100/order_by/q25
// ---------------------------------------------------------------------------

fn bench_order_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("order_by");
    for &scale in SCALES {
        let db = setup_db(scale, 0);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query("MATCH (c:Comment) RETURN c.id ORDER BY c.id LIMIT 5")
                    .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 8: Multi-operator Pipeline
// Composite: filter + aggregate + order + limit
// ---------------------------------------------------------------------------

fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_operator_pipeline");
    for &scale in SCALES {
        let db = setup_db(0, scale);
        let conn = db.connect();
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, _| {
            b.iter(|| {
                conn.query(
                    "MATCH (p:Person) WHERE p.gender = 'male' RETURN p.browserUsed, count(p.id)",
                )
                .unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 9: DML — insert throughput
// ---------------------------------------------------------------------------

fn bench_dml(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml");

    // Single insert latency.
    {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query(
            "CREATE NODE TABLE Comment (id INT64, creationDate INT64, locationIP STRING, browserUsed STRING, content STRING, length INT64, PRIMARY KEY (id))",
        ).unwrap();

        let mut counter = 0i64;
        group.bench_function("single_insert", |b| {
            b.iter(|| {
                let q = format!(
                    "CREATE (c:Comment {{id: {}, creationDate: 1300000000, locationIP: '1.2.3.4', browserUsed: 'Chrome', content: 'bench', length: 5}})",
                    counter
                );
                conn.query(&q).unwrap();
                counter += 1;
            });
        });
    }

    // Bulk load via insert_row().
    for &scale in SCALES {
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(
            BenchmarkId::new("bulk_load", scale),
            &scale,
            |b, &n| {
                b.iter(|| {
                    let db = Database::in_memory();
                    let conn = db.connect();
                    conn.query(
                        "CREATE NODE TABLE Comment (id INT64, creationDate INT64, locationIP STRING, browserUsed STRING, content STRING, length INT64, PRIMARY KEY (id))",
                    ).unwrap();

                    let snapshot = db.catalog().read();
                    let tid = snapshot.find_by_name("Comment").unwrap().table_id();
                    drop(snapshot);

                    let mut storage = db.storage().write().unwrap();
                    for i in 0..n {
                        storage
                            .insert_row(
                                tid,
                                &[
                                    TypedValue::Int64(i as i64),
                                    TypedValue::Int64(1_300_000_000 + i as i64 * 1000),
                                    TypedValue::String(SmolStr::new("1.2.3.4")),
                                    TypedValue::String(SmolStr::new("Chrome")),
                                    TypedValue::String(SmolStr::new("bench")),
                                    TypedValue::Int64(5),
                                ],
                            )
                            .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fixed_size_seq_scan,
    bench_var_size_seq_scan,
    bench_scan_after_filter,
    bench_filter_selectivity,
    bench_expr_evaluator,
    bench_aggregation,
    bench_order_by,
    bench_pipeline,
    bench_dml,
);
criterion_main!(benches);
