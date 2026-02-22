//! Database — top-level entry point owning catalog + storage + transactions.

use std::path::Path;
use std::sync::{Arc, RwLock};

use kyu_catalog::Catalog;
use kyu_common::{KyuError, KyuResult};
use kyu_extension::Extension;
use kyu_transaction::{Checkpointer, TransactionManager, Wal};

use crate::connection::Connection;
use crate::storage::NodeGroupStorage;

/// A graph database instance.
///
/// Owns the catalog (schema), columnar storage, transaction manager, and WAL.
/// Create connections via [`Database::connect`] to execute Cypher queries and DDL.
pub struct Database {
    catalog: Arc<Catalog>,
    storage: Arc<RwLock<NodeGroupStorage>>,
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    checkpointer: Arc<Checkpointer>,
    extensions: Arc<Vec<Box<dyn Extension>>>,
}

impl Database {
    /// Create a new in-memory database with empty catalog and storage.
    pub fn in_memory() -> Self {
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(Wal::in_memory());
        let checkpointer = Arc::new(Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal)));
        Self {
            catalog: Arc::new(Catalog::new()),
            storage: Arc::new(RwLock::new(NodeGroupStorage::new())),
            txn_mgr,
            wal,
            checkpointer,
            extensions: Arc::new(Vec::new()),
        }
    }

    /// Open a persistent database at the given directory path.
    /// Creates the directory if it doesn't exist.
    pub fn open(path: &Path) -> KyuResult<Self> {
        std::fs::create_dir_all(path).map_err(|e| {
            KyuError::Storage(format!("cannot create database directory '{}': {e}", path.display()))
        })?;
        let wal = Wal::new(path, false).map_err(|e| {
            KyuError::Storage(format!("cannot open WAL at '{}': {e}", path.display()))
        })?;
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(wal);
        let checkpointer = Arc::new(Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal)));
        Ok(Self {
            catalog: Arc::new(Catalog::new()),
            storage: Arc::new(RwLock::new(NodeGroupStorage::new())),
            txn_mgr,
            wal,
            checkpointer,
            extensions: Arc::new(Vec::new()),
        })
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
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.wal),
            Arc::clone(&self.checkpointer),
            Arc::clone(&self.extensions),
        )
    }

    /// Manually trigger a checkpoint.
    pub fn checkpoint(&self) -> KyuResult<u64> {
        self.checkpointer.checkpoint().map_err(|e| {
            KyuError::Transaction(format!("checkpoint failed: {e}"))
        })
    }

    /// Get a reference to the underlying storage (for direct insertion).
    pub fn storage(&self) -> &Arc<RwLock<NodeGroupStorage>> {
        &self.storage
    }

    /// Get a reference to the underlying catalog.
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Get a reference to the transaction manager.
    pub fn txn_manager(&self) -> &Arc<TransactionManager> {
        &self.txn_mgr
    }

    /// Get a reference to the WAL.
    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }
}
