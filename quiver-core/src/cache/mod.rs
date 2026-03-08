use arrow::record_batch::RecordBatch;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::Arc;
use std::time::Instant;

use crate::fanout::FanoutLatencies;

/// Cache key for request-level caching.
/// Includes feature view, sorted entities/features, and temporal information.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RequestCacheKey {
    pub feature_view: String,
    pub entities_hash: u64,
    pub features_hash: u64,
    pub as_of_timestamp: Option<i64>,
    pub entity_key: String,
}

impl RequestCacheKey {
    /// Create a new cache key with automatic sorting for consistency.
    pub fn new(
        feature_view: &str,
        entity_ids: &[String],
        feature_names: &[String],
        as_of_timestamp: Option<i64>,
        entity_key: &str,
    ) -> Self {
        let mut sorted_entities = entity_ids.to_vec();
        sorted_entities.sort();
        let entities_hash = Self::hash_vec(&sorted_entities);

        let mut sorted_features = feature_names.to_vec();
        sorted_features.sort();
        let features_hash = Self::hash_vec(&sorted_features);

        Self {
            feature_view: feature_view.to_string(),
            entities_hash,
            features_hash,
            as_of_timestamp,
            entity_key: entity_key.to_string(),
        }
    }

    /// Create a cache key with temporal rounding for improved hit rates.
    pub fn with_temporal_rounding(
        feature_view: &str,
        entity_ids: &[String],
        feature_names: &[String],
        as_of_timestamp: Option<i64>,
        entity_key: &str,
        rounding_seconds: i64,
    ) -> Self {
        let rounded_timestamp = as_of_timestamp.map(|ts| {
            (ts / (rounding_seconds * 1_000_000_000)) * (rounding_seconds * 1_000_000_000)
        });

        Self::new(
            feature_view,
            entity_ids,
            feature_names,
            rounded_timestamp,
            entity_key,
        )
    }

    fn hash_vec(vec: &[String]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for item in vec {
            item.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Cached result containing the RecordBatch and its metadata.
#[derive(Clone)]
pub struct CachedResult {
    pub batch: Arc<RecordBatch>,
    pub latencies: FanoutLatencies,
    pub cached_at: Instant,
}

/// Request-level cache for feature queries.
pub struct RequestCache {
    cache: Cache<RequestCacheKey, CachedResult>,
    config: CacheConfig,
}

/// Configuration for request caching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_entries: u64,
    pub default_ttl_seconds: u64,
    pub temporal_ttl_seconds: u64,
    pub temporal_rounding_seconds: i64,
    pub per_backend_cache: bool,
    pub size_limit_mb: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 50_000,
            default_ttl_seconds: 300,
            temporal_ttl_seconds: 3600,
            temporal_rounding_seconds: 60,
            per_backend_cache: true,
            size_limit_mb: 1024,
        }
    }
}

impl RequestCache {
    /// Create a new request cache with the given configuration.
    pub fn new(config: CacheConfig) -> Self {
        let cache = if config.enabled {
            Cache::builder()
                .max_capacity(config.max_entries)
                .build()
        } else {
            Cache::builder()
                .max_capacity(0)
                .build()
        };

        Self { cache, config }
    }

    /// Check if caching is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get a cached result if available and not expired.
    pub async fn get(&self, key: &RequestCacheKey) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }
        self.cache.get(key).await
    }

    /// Store a result in the cache with appropriate TTL.
    pub async fn store(
        &self,
        key: RequestCacheKey,
        batch: RecordBatch,
        latencies: FanoutLatencies,
    ) {
        if !self.config.enabled {
            return;
        }

        let result = CachedResult {
            batch: Arc::new(batch),
            latencies,
            cached_at: Instant::now(),
        };

        self.cache.insert(key, result).await;
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            enabled: self.config.enabled,
            max_entries: self.config.max_entries,
            default_ttl_seconds: self.config.default_ttl_seconds,
        }
    }

    /// Clear the cache (useful for testing or config changes).
    pub async fn clear(&self) {
        self.cache.invalidate_all();
    }
}

/// Cache statistics for observability.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub enabled: bool,
    pub max_entries: u64,
    pub default_ttl_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_consistency() {
        let key1 = RequestCacheKey::new(
            "user_features",
            &["user:2".to_string(), "user:1".to_string()],
            &["score".to_string(), "category".to_string()],
            None,
            "entity_id",
        );

        let key2 = RequestCacheKey::new(
            "user_features",
            &["user:1".to_string(), "user:2".to_string()],
            &["category".to_string(), "score".to_string()],
            None,
            "entity_id",
        );

        assert_eq!(key1, key2, "Cache keys should be equal regardless of input order");
    }

    #[test]
    fn test_temporal_rounding() {
        let ts1 = 1640995230_000_000_000i64; // 1640995230 + 30ns
        let ts2 = 1640995245_000_000_000i64; // 1640995245 + 45ns

        let key1 = RequestCacheKey::with_temporal_rounding(
            "features",
            &["entity:1".to_string()],
            &["feature:1".to_string()],
            Some(ts1),
            "id",
            60,
        );

        let key2 = RequestCacheKey::with_temporal_rounding(
            "features",
            &["entity:1".to_string()],
            &["feature:1".to_string()],
            Some(ts2),
            "id",
            60,
        );

        assert_eq!(
            key1, key2,
            "Timestamps within same minute should produce same cache key"
        );
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let config = CacheConfig {
            enabled: false,
            ..Default::default()
        };

        let cache = RequestCache::new(config);
        assert!(!cache.is_enabled());
    }

    #[tokio::test]
    async fn test_cache_enabled() {
        let config = CacheConfig {
            enabled: true,
            ..Default::default()
        };

        let cache = RequestCache::new(config);
        assert!(cache.is_enabled());
    }
}
