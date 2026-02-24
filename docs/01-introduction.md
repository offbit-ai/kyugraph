# Welcome to KyuGraph

## What you'll learn

In these tutorials you'll build real graph applications with KyuGraph — from your first query to production-ready pipelines. Each tutorial is hands-on with runnable code.

## What is KyuGraph?

KyuGraph is a high-performance embedded property graph database written in pure Rust. It runs inside your application — no separate server process needed. You write Cypher queries against a columnar storage engine backed by MVCC transactions.

## Who is this for?

- **Rust developers** building applications that need graph queries
- **Data engineers** ingesting structured data into knowledge graphs
- **AI/ML engineers** building agentic systems with code graphs or RAG pipelines

## Prerequisites

You'll need:
- Rust 1.85+ (edition 2024)
- `cargo` installed

## Install

Add KyuGraph to your project:

```toml
[dependencies]
kyu-graph = "0.1"
```

Or install the interactive shell:

```bash
# Homebrew (macOS / Linux)
brew install offbit-ai/kyugraph/kyu-graph-cli

# Cargo
cargo install kyu-graph-cli
```

## What's in these tutorials?

| Tutorial | What you'll build |
|----------|-------------------|
| Your First Graph | Create a social network, insert data, run queries |
| Build a Knowledge Graph | Research paper graph with CSV ingestion, FTS, vector search, PageRank |
| Explore a Codebase | Ingest a Rust workspace into a graph, analyze dependencies |
| Agentic Discovery | Simulate an AI agent that incrementally discovers code relationships |
| Data Ingestion | Bulk loading from CSV/Parquet/Arrow and streaming via the delta fast path |
| Extensions | Graph algorithms, full-text search, vector similarity, JSON functions |
| Arrow Flight | Expose your graph as a gRPC endpoint for remote clients |
| Architecture | Understand the query pipeline and crate structure |
