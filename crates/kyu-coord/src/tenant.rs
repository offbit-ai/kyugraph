//! Tenant registry: maps tenant IDs to their configuration.

use std::path::PathBuf;

use dashmap::DashMap;

/// Unique identifier for a tenant in a multi-tenant deployment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Per-tenant configuration for cloud deployment.
#[derive(Clone, Debug)]
pub struct TenantConfig {
    /// S3 bucket for this tenant's data.
    pub s3_bucket: String,
    /// S3 key prefix for page storage.
    pub s3_prefix: String,
    /// Local directory for NVMe disk cache.
    pub cache_dir: PathBuf,
    /// Maximum memory budget in bytes.
    pub max_memory_bytes: usize,
    /// Maximum concurrent connections.
    pub max_connections: usize,
}

impl TenantConfig {
    /// Create a new tenant config with sensible defaults.
    pub fn new(s3_bucket: String, s3_prefix: String, cache_dir: PathBuf) -> Self {
        Self {
            s3_bucket,
            s3_prefix,
            cache_dir,
            max_memory_bytes: 512 * 1024 * 1024, // 512 MB
            max_connections: 64,
        }
    }
}

/// Thread-safe registry mapping tenant IDs to their configurations.
///
/// Uses `DashMap` for lock-free concurrent reads and fine-grained write locks.
pub struct TenantRegistry {
    tenants: DashMap<TenantId, TenantConfig>,
}

impl TenantRegistry {
    pub fn new() -> Self {
        Self {
            tenants: DashMap::new(),
        }
    }

    /// Register a new tenant. Returns `false` if the tenant already exists.
    pub fn register(&self, id: TenantId, config: TenantConfig) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.tenants.entry(id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(config);
                true
            }
        }
    }

    /// Update an existing tenant's configuration. Returns `false` if not found.
    pub fn update(&self, id: &TenantId, config: TenantConfig) -> bool {
        if let Some(mut entry) = self.tenants.get_mut(id) {
            *entry = config;
            true
        } else {
            false
        }
    }

    /// Remove a tenant. Returns the config if found.
    pub fn remove(&self, id: &TenantId) -> Option<TenantConfig> {
        self.tenants.remove(id).map(|(_, config)| config)
    }

    /// Get a tenant's configuration.
    pub fn get(&self, id: &TenantId) -> Option<TenantConfig> {
        self.tenants.get(id).map(|entry| entry.value().clone())
    }

    /// Check if a tenant exists.
    pub fn contains(&self, id: &TenantId) -> bool {
        self.tenants.contains_key(id)
    }

    /// Number of registered tenants.
    pub fn count(&self) -> usize {
        self.tenants.len()
    }

    /// List all tenant IDs.
    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.iter().map(|e| e.key().clone()).collect()
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TenantConfig {
        TenantConfig::new(
            "test-bucket".into(),
            "tenant-1/db".into(),
            PathBuf::from("/tmp/cache"),
        )
    }

    #[test]
    fn register_and_get() {
        let reg = TenantRegistry::new();
        let id = TenantId::new("t1");
        assert!(reg.register(id.clone(), test_config()));
        assert!(reg.contains(&id));

        let config = reg.get(&id).unwrap();
        assert_eq!(config.s3_bucket, "test-bucket");
        assert_eq!(config.s3_prefix, "tenant-1/db");
    }

    #[test]
    fn register_duplicate_returns_false() {
        let reg = TenantRegistry::new();
        let id = TenantId::new("t1");
        assert!(reg.register(id.clone(), test_config()));
        assert!(!reg.register(id, test_config()));
    }

    #[test]
    fn remove_tenant() {
        let reg = TenantRegistry::new();
        let id = TenantId::new("t1");
        reg.register(id.clone(), test_config());

        let removed = reg.remove(&id);
        assert!(removed.is_some());
        assert!(!reg.contains(&id));
        assert_eq!(reg.count(), 0);
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let reg = TenantRegistry::new();
        assert!(reg.remove(&TenantId::new("ghost")).is_none());
    }

    #[test]
    fn update_config() {
        let reg = TenantRegistry::new();
        let id = TenantId::new("t1");
        reg.register(id.clone(), test_config());

        let mut new_config = test_config();
        new_config.max_memory_bytes = 1024 * 1024 * 1024;
        assert!(reg.update(&id, new_config));

        let config = reg.get(&id).unwrap();
        assert_eq!(config.max_memory_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn update_nonexistent_returns_false() {
        let reg = TenantRegistry::new();
        assert!(!reg.update(&TenantId::new("ghost"), test_config()));
    }

    #[test]
    fn count_and_list() {
        let reg = TenantRegistry::new();
        reg.register(TenantId::new("a"), test_config());
        reg.register(TenantId::new("b"), test_config());
        reg.register(TenantId::new("c"), test_config());
        assert_eq!(reg.count(), 3);

        let mut ids: Vec<String> = reg.tenant_ids().into_iter().map(|t| t.0).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn tenant_id_display() {
        let id = TenantId::new("my-tenant");
        assert_eq!(id.to_string(), "my-tenant");
        assert_eq!(id.as_str(), "my-tenant");
    }

    #[test]
    fn default_config_values() {
        let config = test_config();
        assert_eq!(config.max_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(config.max_connections, 64);
    }
}
