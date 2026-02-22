//! FTS index backed by tantivy — RAM directory, BM25 ranking, batched commits.

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, TantivyDocument};

/// In-memory full-text search index.
pub struct FtsIndex {
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    doc_id_field: Field,
    content_field: Field,
    next_doc_id: u64,
    dirty: bool,
}

impl Default for FtsIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FtsIndex {
    /// Create a new empty in-memory FTS index.
    pub fn new() -> Self {
        let mut schema_builder = Schema::builder();
        let doc_id_field = schema_builder.add_u64_field("doc_id", STORED | FAST);
        let content_field = schema_builder.add_text_field("content", TEXT | STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let writer = index
            .writer(15_000_000) // 15MB heap — reasonable for in-memory MVP
            .expect("failed to create tantivy IndexWriter");
        let reader = index
            .reader()
            .expect("failed to create tantivy IndexReader");

        Self {
            index,
            writer,
            reader,
            doc_id_field,
            content_field,
            next_doc_id: 0,
            dirty: false,
        }
    }

    /// Index a document. Returns the assigned doc_id.
    pub fn add_document(&mut self, content: &str) -> Result<u64, tantivy::TantivyError> {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        self.writer.add_document(doc!(
            self.doc_id_field => doc_id,
            self.content_field => content,
        ))?;
        self.dirty = true;

        // Batch commit every 1000 documents.
        if self.next_doc_id.is_multiple_of(1000) {
            self.commit()?;
        }

        Ok(doc_id)
    }

    /// BM25-ranked search. Returns `(doc_id, score, content_snippet)`.
    pub fn search(
        &mut self,
        query_str: &str,
        limit: usize,
    ) -> Result<Vec<(u64, f32, String)>, tantivy::TantivyError> {
        // Commit pending writes before searching.
        if self.dirty {
            self.commit()?;
        }

        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.content_field]);
        let query = query_parser
            .parse_query(query_str)
            .map_err(|e| tantivy::TantivyError::InvalidArgument(format!("{e}")))?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_addr) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_addr)?;
            let doc_id = doc
                .get_first(self.doc_id_field)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let snippet = doc
                .get_first(self.content_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            results.push((doc_id, score, snippet));
        }

        Ok(results)
    }

    /// Delete all documents and reset the index.
    ///
    /// Rebuilds the index from scratch (cheap for RAM-backed indices) to avoid
    /// tantivy's async delete semantics which can leave stale segments.
    pub fn clear(&mut self) -> Result<(), tantivy::TantivyError> {
        *self = Self::new();
        Ok(())
    }

    /// Commit pending writes and reload the reader.
    fn commit(&mut self) -> Result<(), tantivy::TantivyError> {
        self.writer.commit()?;
        self.reader.reload()?;
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_search_single() {
        let mut idx = FtsIndex::new();
        let id = idx.add_document("the quick brown fox jumps over the lazy dog").unwrap();
        assert_eq!(id, 0);

        let results = idx.search("fox", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        assert!(results[0].1 > 0.0);
        assert!(results[0].2.contains("fox"));
    }

    #[test]
    fn add_multiple_and_rank() {
        let mut idx = FtsIndex::new();
        idx.add_document("introduction to rust programming").unwrap();
        idx.add_document("rust is a systems programming language").unwrap();
        idx.add_document("python is popular for data science").unwrap();

        let results = idx.search("rust programming", 10).unwrap();
        // Both rust documents should match; python should not.
        assert!(results.len() >= 2);
        let doc_ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        assert!(doc_ids.contains(&0));
        assert!(doc_ids.contains(&1));
    }

    #[test]
    fn search_no_match() {
        let mut idx = FtsIndex::new();
        idx.add_document("hello world").unwrap();
        let results = idx.search("quantum", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_with_limit() {
        let mut idx = FtsIndex::new();
        for i in 0..20 {
            idx.add_document(&format!("document number {i} about testing")).unwrap();
        }
        let results = idx.search("testing", 5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn clear_resets_index() {
        let mut idx = FtsIndex::new();
        idx.add_document("hello world").unwrap();
        idx.clear().unwrap();
        let results = idx.search("hello", 10).unwrap();
        assert!(results.is_empty());

        // New documents start at doc_id 0 again.
        let id = idx.add_document("new document").unwrap();
        assert_eq!(id, 0);
    }

    #[test]
    fn sequential_doc_ids() {
        let mut idx = FtsIndex::new();
        assert_eq!(idx.add_document("first").unwrap(), 0);
        assert_eq!(idx.add_document("second").unwrap(), 1);
        assert_eq!(idx.add_document("third").unwrap(), 2);
    }

    #[test]
    fn bm25_relevance_ordering() {
        let mut idx = FtsIndex::new();
        // Doc 0: mentions "rust" once
        idx.add_document("rust is nice").unwrap();
        // Doc 1: mentions "rust" multiple times
        idx.add_document("rust rust rust is a rust language for rust developers").unwrap();

        let results = idx.search("rust", 10).unwrap();
        assert_eq!(results.len(), 2);
        // Doc 1 should rank higher (more "rust" mentions → higher BM25).
        assert_eq!(results[0].0, 1);
    }
}
