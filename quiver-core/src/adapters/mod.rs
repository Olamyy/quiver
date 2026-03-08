pub mod clickhouse;
pub mod memory;
pub mod postgres;
pub mod redis;
pub mod s3_parquet;
pub mod utils;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Standardized backend adapter interface.
///
/// This trait defines the contract that all backend adapters must implement to ensure
/// consistent behavior across different storage systems.
///
/// ## Design principles
/// - **Schema consistency**: For a given `(feature set, feature definitions)` adapters must produce
///   the same Arrow schema and column types.
/// - **Timeout behavior**: Operations must respect explicit timeouts and apply safe defaults
///   when timeouts are not provided.
/// - **Batching semantics**: Adapters declare limits and optimal sizes; callers may split requests
///   accordingly. Adapters should also avoid pathological per-row round-trips.
/// - **Error propagation**: Standardized error types with backend context.
///
/// ## Important note on schema consistency
/// Arrow type stability cannot be guaranteed if each adapter independently infers types from its
/// backend. To make schema consistency enforceable at scale, you typically need a single source
/// of truth for feature typing (e.g., a registry or a `FeatureSpec` passed in by the caller).
/// Combined resolution config for a specific feature, providing type and path overrides
#[derive(Debug, Clone)]
pub struct FeatureResolution {
    pub expected_type: arrow::datatypes::DataType,
    pub source_path: Option<crate::config::SourcePath>,
}

#[async_trait::async_trait]
pub trait BackendAdapter: Send + Sync {
    /// Backend identifier used in logs and error messages.
    fn name(&self) -> &str;

    /// Adapter capabilities including temporal, batching, and performance characteristics.
    fn capabilities(&self) -> AdapterCapabilities;

    /// Fetch features for entities with optional timeout.
    ///
    /// The returned RecordBatch must:
    /// - Have exactly one row per `entity_id` in the same order as requested
    /// - Include all requested feature columns, null-filled if missing
    ///
    /// # Arguments
    /// - `entity_ids` - Entity identifiers to fetch features for
    /// - `feature_names` - Feature names to retrieve
    /// - `entity_key` - The column name used as entity identifier (e.g. "user_id", "entity_id")
    /// - `as_of` - Point-in-time for temporal queries (`None` = current time)
    /// - `timeout` - Maximum time to wait for response (`None` = adapter default)
    ///
    /// # Errors
    /// - `AdapterError::Timeout` if operation exceeds timeout
    /// - `AdapterError::InvalidRequest` for malformed requests
    /// - `AdapterError::Internal` for backend failures
    ///
    /// ## NotFound semantics
    /// This API should generally **not** return `AdapterError::NotFound` for missing entities/features;
    /// instead it should null-fill. Reserve `NotFound` for APIs where existence is required.
    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError>;

    /// Fetch features for entities with explicit resolution configurations.
    /// This method allows the caller to specify both the expected Arrow types and optional
    /// specific source paths for each feature in a single combined lookup.
    ///
    /// # Arguments
    /// - `entity_ids` - Entity identifiers to fetch features for
    /// - `feature_names` - Feature names to retrieve
    /// - `entity_key` - The column name used as entity identifier
    /// - `resolutions` - Map of feature names to their combined FeatureResolution
    /// - `as_of` - Point-in-time for temporal queries (`None` = current time)
    /// - `timeout` - Maximum time to wait for response (`None` = adapter default)
    ///
    /// # Returns
    /// RecordBatch with features retrieved and converted to expected types
    ///
    /// # Default Implementation
    /// The default implementation calls `get()`. Adapters should override this to take
    /// advantage of type checking and per-feature source paths.
    async fn get_with_resolutions(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        _resolutions: &std::collections::HashMap<String, FeatureResolution>,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        self.get(entity_ids, feature_names, entity_key, as_of, timeout)
            .await
    }

    /// Fetch features for entities without null-filling (sparse result).
    ///
    /// Used during fanout execution when the merge layer will handle entity alignment.
    /// Returns only rows for entities that exist in the backend, in any order.
    /// The merge layer will then perform entity_id-based LEFT JOIN to align results.
    ///
    /// # Arguments
    /// - `entity_ids` - Entity identifiers to fetch features for (used for caching/sharding hints)
    /// - `feature_names` - Feature names to retrieve
    /// - `entity_key` - The column name used as entity identifier
    /// - `resolutions` - Map of feature names to their combined FeatureResolution
    /// - `as_of` - Point-in-time for temporal queries (`None` = current time)
    /// - `timeout` - Maximum time to wait for response (`None` = adapter default)
    ///
    /// # Returns
    /// RecordBatch with only rows that exist in the backend (no null-filling).
    /// May have fewer rows than requested and in any order.
    ///
    /// # Default Implementation
    /// Defaults to `get_with_resolutions()` which includes null-filling.
    /// Adapters should override this for efficient sparse retrieval.
    async fn get_sparse_with_resolutions(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        resolutions: &std::collections::HashMap<String, FeatureResolution>,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        self.get_with_resolutions(
            entity_ids,
            feature_names,
            entity_key,
            resolutions,
            as_of,
            timeout,
        )
        .await
    }

    /// Check backend health and return operational metrics.
    ///
    /// This method should perform a lightweight operation to verify the backend is accessible.
    ///
    /// ### Timeout guidance
    /// There is no timeout parameter here, so implementations should enforce a conservative internal
    /// timeout (e.g., a few seconds) to avoid hanging health endpoints under network failure.
    async fn health(&self) -> HealthStatus;

    /// Initialize adapter connections and validate configuration.
    ///
    /// Called once during startup to establish connections, validate configuration,
    /// and perform any necessary setup operations.
    async fn initialize(&mut self) -> Result<(), AdapterError> {
        Ok(())
    }

    /// Clean shutdown of adapter resources.
    ///
    /// Called during server shutdown to gracefully close connections and clean up resources.
    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        Ok(())
    }
}

/// Result ordering guarantee for adapter output.
///
/// Declares whether an adapter naturally returns results in a specific order,
/// which allows the fanout merge layer to skip expensive alignment operations.
///
/// Per RFC v0.3, this enables optimizing the merge pipeline from O(n log n) to O(n)
/// when all backends provide ordered output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingGuarantee {
    /// Results are returned in the exact order of the request entity_ids list.
    /// Enables direct column assembly without alignment overhead.
    ///
    /// Example: PostgreSQL adapter queries `WHERE entity_id IN (...) ORDER BY entity_ids[index]`
    OrderedByRequest,

    /// Results are sorted by entity_id (ascending).
    /// Requires one-pass merge to align with request order, but still O(n) not O(n*m).
    ///
    /// Example: DuckDB adapter returns `ORDER BY entity_id`
    OrderedByEntityId,

    /// Results order is arbitrary; requires full entity_id index for alignment.
    /// Incurs O(n) HashMap construction cost during merge.
    ///
    /// Example: Redis adapter returns results from parallel MGET calls without ordering
    Unordered,
}

/// Adapter capabilities and operational characteristics.
///
/// This structure provides metadata about adapter behavior that allows the resolver to make
/// informed decisions about request routing, batching, and timeout configuration.
#[derive(Debug, Clone, Copy)]
pub struct AdapterCapabilities {
    /// Temporal data support capabilities.
    pub temporal: TemporalCapability,

    /// Maximum number of entities that can be fetched in a single request.
    pub max_batch_size: Option<usize>,

    /// Optimal batch size for performance (used for request splitting).
    pub optimal_batch_size: Option<usize>,

    /// Typical latency for operations in milliseconds (used for timeout planning).
    pub typical_latency_ms: u32,

    /// Whether adapter supports parallel requests for the same feature set.
    pub supports_parallel_requests: bool,

    /// Ordering guarantee for result rows per RFC v0.3.
    /// Used by fanout merge to optimize alignment (skip for ordered, use HashMap for unordered).
    pub ordering_guarantee: OrderingGuarantee,
}

/// Temporal data capability metadata.
///
/// Prefer keeping this enum as capability metadata and moving policy decisions (like
/// "how recent is 'current'") into a resolver/config layer.
///
/// If you *do* need a helper here, avoid hard-coding thresholds that make behavior
/// time-dependent and hard to test. Pass "now" from the caller when possible.
#[derive(Debug, Clone, Copy)]
pub enum TemporalCapability {
    /// Only serves current/latest values.
    CurrentOnly {
        /// Typical latency in milliseconds.
        typical_latency_ms: u32,
    },

    /// Serves recent values with approximate recency.
    ApproximateRecency {
        typical_lag_seconds: u32,
        typical_latency_ms: u32,
    },

    /// Full time-travel support for historical queries.
    TimeTravel { typical_latency_ms: u32 },

    /// MVCC-style range queries with transaction semantics.
    MvccRange { typical_latency_ms: u32 },
}

impl TemporalCapability {
    /// Check whether this capability can serve data for the given timestamp.
    ///
    /// This method is a heuristic; callers with strict correctness requirements should not rely
    /// on it alone. Prefer making this decision in a resolver layer with configuration.
    pub fn can_serve_as_of(&self, as_of: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let age_seconds = (now - as_of).num_seconds().max(0) as u32;
        match self {
            Self::CurrentOnly { .. } => age_seconds < 5,
            Self::ApproximateRecency {
                typical_lag_seconds,
                ..
            } => age_seconds < typical_lag_seconds.saturating_mul(2),
            Self::TimeTravel { .. } => true,
            Self::MvccRange { .. } => true,
        }
    }

    /// Get the typical latency for this temporal capability.
    pub fn typical_latency_ms(&self) -> u32 {
        match self {
            Self::CurrentOnly { typical_latency_ms } => *typical_latency_ms,
            Self::ApproximateRecency {
                typical_latency_ms, ..
            } => *typical_latency_ms,
            Self::TimeTravel { typical_latency_ms } => *typical_latency_ms,
            Self::MvccRange { typical_latency_ms } => *typical_latency_ms,
        }
    }
}

/// Standardized health status with operational metrics.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the backend is operational.
    pub healthy: bool,

    /// Backend name for identification.
    pub backend: String,

    /// Optional descriptive message about health status.
    pub message: Option<String>,

    /// Last operation latency in milliseconds.
    pub latency_ms: Option<f64>,

    /// Timestamp of this health check.
    pub last_check: DateTime<Utc>,

    /// Whether adapter capabilities have been verified.
    pub capabilities_verified: bool,

    /// Estimated request capacity (requests per second).
    pub estimated_capacity: Option<f64>,
}

impl HealthStatus {
    /// Create a healthy status with minimal information.
    pub fn healthy(backend: &str) -> Self {
        Self {
            healthy: true,
            backend: backend.to_string(),
            message: None,
            latency_ms: None,
            last_check: Utc::now(),
            capabilities_verified: false,
            estimated_capacity: None,
        }
    }

    /// Create an unhealthy status with error message.
    pub fn unhealthy(backend: &str, message: impl Into<String>) -> Self {
        Self {
            healthy: false,
            backend: backend.to_string(),
            message: Some(message.into()),
            latency_ms: None,
            last_check: Utc::now(),
            capabilities_verified: false,
            estimated_capacity: None,
        }
    }
}

/// Standardized error types with consistent backend context.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("[{backend}] Internal error: {message}")]
    Internal { backend: String, message: String },

    /// Reserved for APIs where existence is required. `get()` should usually null-fill instead.
    #[error("[{backend}] Entity not found: {entity_id}")]
    NotFound { backend: String, entity_id: String },

    #[error("[{backend}] Invalid request: {message}")]
    InvalidRequest { backend: String, message: String },

    #[error("[{backend}] Timeout after {timeout_ms}ms")]
    Timeout { backend: String, timeout_ms: u64 },

    #[error("[{backend}] Connection failed: {message}")]
    ConnectionFailed { backend: String, message: String },

    #[error("[{backend}] Schema mismatch: {message}")]
    SchemaMismatch { backend: String, message: String },

    #[error("[{backend}] Resource limit exceeded: {message}")]
    ResourceLimitExceeded { backend: String, message: String },
}

impl AdapterError {
    /// Create an internal error with backend context.
    pub fn internal(backend: &str, msg: impl Into<String>) -> Self {
        Self::Internal {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    /// Create an invalid request error with backend context.
    pub fn invalid(backend: &str, msg: impl Into<String>) -> Self {
        Self::InvalidRequest {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    /// Create an Arrow-specific error with backend context.
    pub fn arrow(backend: &str, msg: impl Into<String>) -> Self {
        Self::Internal {
            backend: backend.to_string(),
            message: format!("Arrow error: {}", msg.into()),
        }
    }

    /// Create a timeout error with backend context.
    pub fn timeout(backend: &str, timeout_ms: u64) -> Self {
        Self::Timeout {
            backend: backend.to_string(),
            timeout_ms,
        }
    }

    /// Create a connection failed error with backend context.
    pub fn connection_failed(backend: &str, msg: impl Into<String>) -> Self {
        Self::ConnectionFailed {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    /// Create a schema mismatch error with backend context.
    pub fn schema_mismatch(backend: &str, msg: impl Into<String>) -> Self {
        Self::SchemaMismatch {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    /// Create a resource limit exceeded error with backend context.
    pub fn resource_limit_exceeded(backend: &str, msg: impl Into<String>) -> Self {
        Self::ResourceLimitExceeded {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }
}
