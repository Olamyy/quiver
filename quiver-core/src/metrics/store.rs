use crate::fanout::metrics::FanoutLatencies;
use std::time::SystemTime;

/// Stored metrics for a single feature request.
///
/// Contains latency measurements and contextual metadata from feature resolution.
#[derive(Debug, Clone)]
pub struct StoredMetrics {
    /// Fanout latency measurements at each pipeline phase.
    pub latencies: FanoutLatencies,
    /// Feature view name that was requested.
    pub feature_view: String,
    /// Number of entities in the request.
    pub entity_count: i32,
    /// Timestamp when metrics were stored.
    pub stored_at: SystemTime,
}

/// In-memory metrics store with TTL support.
///
/// Stores request metrics keyed by request_id for retrieval via observability service.
/// Automatically expires entries after configured TTL using moka's background expiration.
#[derive(Clone)]
pub struct MetricsStore {
    cache: moka::future::Cache<String, StoredMetrics>,
}

impl MetricsStore {
    /// Create a new metrics store with default configuration.
    ///
    /// Default: 1-hour TTL, 10,000 max entries, LRU eviction.
    pub fn new() -> Self {
        let cache = moka::future::CacheBuilder::new(10_000)
            .time_to_live(std::time::Duration::from_secs(3600))
            .build();

        Self { cache }
    }

    /// Create a new metrics store with custom TTL.
    pub fn with_ttl(ttl_seconds: u64, max_entries: u32) -> Self {
        let cache = moka::future::CacheBuilder::new(max_entries as u64)
            .time_to_live(std::time::Duration::from_secs(ttl_seconds))
            .build();

        Self { cache }
    }

    /// Store metrics for a request.
    pub async fn store(
        &self,
        request_id: String,
        latencies: FanoutLatencies,
        feature_view: String,
        entity_count: i32,
    ) {
        let metrics = StoredMetrics {
            latencies,
            feature_view,
            entity_count,
            stored_at: SystemTime::now(),
        };
        self.cache.insert(request_id, metrics).await;
    }

    /// Retrieve metrics for a request by ID.
    ///
    /// Returns None if metrics not found or have expired.
    pub async fn get(&self, request_id: &str) -> Option<StoredMetrics> {
        self.cache.get(request_id).await
    }

    /// Flush all metrics from the store.
    ///
    /// Used for cold cache benchmarking (iter_batched pattern).
    /// Returns the number of entries cleared.
    pub async fn flush(&self) -> i32 {
        let count = self.cache.iter().count() as i32;
        self.cache.invalidate_all();
        count
    }
}

impl Default for MetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let store = MetricsStore::new();
        let mut latencies = FanoutLatencies::new();
        latencies.record_phase(crate::fanout::metrics::Phase::Merge, 5.0);

        store
            .store(
                "req-123".to_string(),
                latencies.clone(),
                "user_features".to_string(),
                10,
            )
            .await;

        let retrieved = store.get("req-123").await;
        assert!(retrieved.is_some());
        let stored = retrieved.unwrap();
        assert_eq!(stored.feature_view, "user_features");
        assert_eq!(stored.entity_count, 10);
        assert_eq!(stored.latencies.merge_ms, 5.0);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let store = MetricsStore::new();
        let retrieved = store.get("nonexistent").await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_flush_clears_all_entries() {
        let store = MetricsStore::new();
        let mut latencies = FanoutLatencies::new();
        latencies.record_phase(crate::fanout::metrics::Phase::Merge, 5.0);

        store
            .store(
                "req-1".to_string(),
                latencies.clone(),
                "view1".to_string(),
                10,
            )
            .await;
        store
            .store(
                "req-2".to_string(),
                latencies.clone(),
                "view2".to_string(),
                20,
            )
            .await;

        assert!(store.get("req-1").await.is_some());
        assert!(store.get("req-2").await.is_some());

        let count = store.flush().await;
        assert_eq!(count, 2);

        assert!(store.get("req-1").await.is_none());
        assert!(store.get("req-2").await.is_none());
    }
}
