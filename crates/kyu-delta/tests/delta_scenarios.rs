use kyu_delta::*;
use smol_str::SmolStr;

#[test]
fn code_graph_file_edit() {
    let batch = DeltaBatchBuilder::new("file:src/main.rs", 1000)
        .upsert_node(
            "Function",
            "main",
            vec![SmolStr::new("Public")],
            [
                ("lines", DeltaValue::Int64(55)),
                ("file", DeltaValue::String(SmolStr::new("src/main.rs"))),
            ],
        )
        .upsert_node(
            "Function",
            "run_server",
            vec![],
            [("lines", DeltaValue::Int64(30))],
        )
        .upsert_edge(
            "Function",
            "main",
            "calls",
            "Function",
            "run_server",
            std::iter::empty::<(SmolStr, DeltaValue)>(),
        )
        .delete_node("Function", "old_handler")
        .build();

    assert_eq!(batch.len(), 4);
    assert_eq!(batch.node_upsert_count(), 2);
    assert_eq!(batch.edge_upsert_count(), 1);
    assert_eq!(batch.delete_count(), 1);
    assert_eq!(batch.source, "file:src/main.rs");

    let mut labels = batch.referenced_labels();
    labels.sort();
    assert_eq!(labels, vec![SmolStr::new("Function")]);
}

#[test]
fn document_ingestion_batch() {
    let batch = DeltaBatchBuilder::new("doc:batch_42", 2000)
        .upsert_node(
            "Document",
            "invoice_001",
            vec![],
            [("title", DeltaValue::String(SmolStr::new("Invoice #001")))],
        )
        .upsert_node("Chunk", "invoice_001_c0", vec![], [("idx", DeltaValue::Int64(0))])
        .upsert_node("Chunk", "invoice_001_c1", vec![], [("idx", DeltaValue::Int64(1))])
        .upsert_edge(
            "Document",
            "invoice_001",
            "contains",
            "Chunk",
            "invoice_001_c0",
            std::iter::empty::<(SmolStr, DeltaValue)>(),
        )
        .upsert_edge(
            "Document",
            "invoice_001",
            "contains",
            "Chunk",
            "invoice_001_c1",
            std::iter::empty::<(SmolStr, DeltaValue)>(),
        )
        .build();

    assert_eq!(batch.len(), 5);
    assert_eq!(batch.node_upsert_count(), 3);
    assert_eq!(batch.edge_upsert_count(), 2);

    let mut labels = batch.referenced_labels();
    labels.sort();
    assert_eq!(labels, vec![SmolStr::new("Chunk"), SmolStr::new("Document")]);

    let types = batch.referenced_rel_types();
    assert_eq!(types, vec![SmolStr::new("contains")]);
}

#[test]
fn delete_and_recreate() {
    let mut batch = DeltaBatch::new("test", 1);
    batch.push(GraphDelta::DeleteNode {
        key: NodeKey::new("X", "1"),
    });
    batch.push(GraphDelta::UpsertNode {
        key: NodeKey::new("X", "1"),
        labels: vec![],
        props: hashbrown::HashMap::new(),
    });
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.delete_count(), 1);
    assert_eq!(batch.node_upsert_count(), 1);
}

#[test]
fn large_batch() {
    let mut batch = DeltaBatch::new("bulk", 9999);
    for i in 0..1000 {
        batch.push(GraphDelta::UpsertNode {
            key: NodeKey::new("N", format!("{i}")),
            labels: vec![],
            props: hashbrown::HashMap::new(),
        });
    }
    assert_eq!(batch.len(), 1000);
    assert_eq!(batch.node_upsert_count(), 1000);
    assert_eq!(batch.edge_upsert_count(), 0);
}

#[test]
fn vector_clock_causal_ordering() {
    let mut clock_a = VectorClock::new();
    clock_a.tick("worker_a");
    clock_a.tick("worker_a");

    let mut clock_b = VectorClock::new();
    clock_b.set("worker_a", 1);
    clock_b.tick("worker_b");

    // clock_b saw worker_a at 1, but worker_a is now at 2
    assert!(!clock_a.happens_before(&clock_b));

    // After merging, clock_b sees both
    clock_b.merge(&clock_a);
    assert_eq!(clock_b.get("worker_a"), 2);
    assert_eq!(clock_b.get("worker_b"), 1);

    // Now clock_a happens-before clock_b
    assert!(clock_a.happens_before(&clock_b));
}
