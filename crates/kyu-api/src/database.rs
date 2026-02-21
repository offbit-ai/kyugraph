//! Database — top-level entry point owning catalog + storage.

use std::sync::{Arc, RwLock};

use kyu_catalog::Catalog;
use kyu_extension::Extension;

use crate::connection::Connection;
use crate::storage::NodeGroupStorage;

/// An in-memory graph database instance.
///
/// Owns the catalog (schema) and columnar storage. Create connections
/// via [`Database::connect`] to execute Cypher queries and DDL.
pub struct Database {
    catalog: Arc<Catalog>,
    storage: Arc<RwLock<NodeGroupStorage>>,
    extensions: Arc<Vec<Box<dyn Extension>>>,
}

impl Database {
    /// Create a new in-memory database with empty catalog and storage.
    pub fn in_memory() -> Self {
        Self {
            catalog: Arc::new(Catalog::new()),
            storage: Arc::new(RwLock::new(NodeGroupStorage::new())),
            extensions: Arc::new(Vec::new()),
        }
    }

    /// Register an extension. Must be called before creating connections.
    pub fn register_extension(&mut self, ext: Box<dyn Extension>) {
        Arc::get_mut(&mut self.extensions)
            .expect("cannot register extension while connections exist")
            .push(ext);
    }

    /// Create a connection to this database.
    pub fn connect(&self) -> Connection {
        Connection::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.storage),
            Arc::clone(&self.extensions),
        )
    }

    /// Get a reference to the underlying storage (for direct insertion).
    pub fn storage(&self) -> &Arc<RwLock<NodeGroupStorage>> {
        &self.storage
    }

    /// Get a reference to the underlying catalog.
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }
}
