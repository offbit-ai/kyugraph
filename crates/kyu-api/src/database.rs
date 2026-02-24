//! Database — top-level entry point owning catalog + storage + transactions.

use std::path::{Path, PathBuf};
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
    /// Database directory path for persistent databases, `None` for in-memory.
    db_path: Option<PathBuf>,
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
            db_path: None,
        }
    }

    /// Open a persistent database at the given directory path.
    /// Creates the directory if it doesn't exist.
    /// Loads catalog and storage from checkpoint, then replays WAL for DDL recovery.
    pub fn open(path: &Path) -> KyuResult<Self> {
        use crate::persistence;
        use kyu_transaction::{WalRecord, WalReplayer};

        std::fs::create_dir_all(path).map_err(|e| {
            KyuError::Storage(format!(
                "cannot create database directory '{}': {e}",
                path.display()
            ))
        })?;

        // Load catalog from checkpoint, or start fresh.
        let mut catalog_content = match persistence::load_catalog(path)? {
            Some(mut c) => {
                c.rebuild_indexes();
                c
            }
            None => kyu_catalog::CatalogContent::new(),
        };

        // Replay WAL for DDL recovery: apply the last CatalogSnapshot from
        // committed transactions to recover any DDL after the last checkpoint.
        let wal_path = path.join("wal.bin");
        if wal_path.exists() {
            let replayer = WalReplayer::new(&wal_path);
            if let Ok(result) = replayer.replay() {
                // Find the last CatalogSnapshot in committed records.
                for record in result.committed_records.iter().rev() {
                    if let WalRecord::CatalogSnapshot { json_bytes } = record
                        && let Ok(json) = std::str::from_utf8(json_bytes)
                        && let Ok(recovered) = kyu_catalog::CatalogContent::deserialize_json(json)
                    {
                        catalog_content = recovered;
                        break;
                    }
                }
            }
        }

        // Load storage from checkpoint (uses recovered catalog for schema).
        let storage = persistence::load_storage(path, &catalog_content)?;

        let catalog = Arc::new(Catalog::from_content(catalog_content));
        let storage = Arc::new(RwLock::new(storage));

        let wal = Wal::new(path, false).map_err(|e| {
            KyuError::Storage(format!("cannot open WAL at '{}': {e}", path.display()))
        })?;
        let txn_mgr = Arc::new(TransactionManager::new());
        let wal = Arc::new(wal);

        // Build the flush callback for the checkpointer.
        let flush_catalog = Arc::clone(&catalog);
        let flush_storage = Arc::clone(&storage);
        let flush_path = path.to_path_buf();
        let flush_fn = Arc::new(move || {
            let cat = flush_catalog.read();
            let stor = flush_storage
                .read()
                .map_err(|e| format!("storage lock: {e}"))?;
            persistence::save_catalog(&flush_path, &cat).map_err(|e| format!("{e}"))?;
            persistence::save_storage(&flush_path, &stor, &cat).map_err(|e| format!("{e}"))?;
            Ok(())
        });

        let checkpointer = Arc::new(
            Checkpointer::new(Arc::clone(&txn_mgr), Arc::clone(&wal)).with_flush(flush_fn),
        );

        Ok(Self {
            catalog,
            storage,
            txn_mgr,
            wal,
            checkpointer,
            extensions: Arc::new(Vec::new()),
            db_path: Some(path.to_path_buf()),
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
        self.checkpointer
            .checkpoint()
            .map_err(|e| KyuError::Transaction(format!("checkpoint failed: {e}")))
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

impl Drop for Database {
    fn drop(&mut self) {
        // For persistent databases, flush state to disk on orderly shutdown.
        if self.db_path.is_some() {
            let _ = self.checkpointer.checkpoint();
        }
    }
}
