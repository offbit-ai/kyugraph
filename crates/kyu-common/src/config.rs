/// Configuration for a KyuGraph database instance.
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Total buffer pool size in bytes. Default: 256 MB.
    pub buffer_pool_size: usize,
    /// Fraction of buffer pool allocated to the read pool. Default: 0.7 (70%).
    pub read_pool_ratio: f64,
    /// Maximum number of worker threads. Default: number of CPUs.
    pub max_threads: usize,
    /// Enable column compression. Default: true.
    pub enable_compression: bool,
    /// WAL size threshold in bytes before triggering checkpoint. Default: 256 MB.
    pub checkpoint_threshold: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 256 * 1024 * 1024, // 256 MB
            read_pool_ratio: 0.7,
            max_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            enable_compression: true,
            checkpoint_threshold: 256 * 1024 * 1024, // 256 MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let config = DatabaseConfig::default();
        assert_eq!(config.buffer_pool_size, 256 * 1024 * 1024);
        assert!((config.read_pool_ratio - 0.7).abs() < f64::EPSILON);
        assert!(config.max_threads >= 1);
        assert!(config.enable_compression);
        assert_eq!(config.checkpoint_threshold, 256 * 1024 * 1024);
    }

    #[test]
    fn custom_config() {
        let config = DatabaseConfig {
            buffer_pool_size: 1024 * 1024 * 1024,
            read_pool_ratio: 0.8,
            max_threads: 16,
            enable_compression: false,
            checkpoint_threshold: 512 * 1024 * 1024,
        };
        assert_eq!(config.buffer_pool_size, 1024 * 1024 * 1024);
        assert!(!config.enable_compression);
    }
}
