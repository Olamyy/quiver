pub mod memory;
pub mod redis;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Standardized backend adapter interface.
///
/// This trait defines the contract that all backend adapters must implement
/// to ensure consistent behavior across different storage systems. The interface
/// is designed to prevent divergent implementations that could cause operational
/// issues at scale.
///
/// # Design Principles
/// - **Schema consistency**: All adapters must return the same Arrow schema for the same features
/// - **Timeout behavior**: All operations must respect timeout parameters
/// - **Batching semantics**: Adapters must declare their batching capabilities and limits
/// - **Error propagation**: Standardized error types with backend context
#[async_trait::async_trait]
pub trait BackendAdapter: Send + Sync {
    /// Backend identifier used in logs and error messages.
    fn name(&self) -> &str;

    /// Adapter capabilities including temporal, batching, and performance characteristics.
    fn capabilities(&self) -> AdapterCapabilities;

    /// Describe the Arrow schema for the given feature names.
    ///
    /// This method must return a consistent schema for the same feature set.
    /// Schema changes should be handled through versioning mechanisms.
    ///
    /// # Errors
    /// - `AdapterError::InvalidRequest` if feature names are unknown
    /// - `AdapterError::Internal` if schema introspection fails
    async fn describe_schema(&self, feature_names: &[String]) -> Result<Schema, AdapterError>;

    /// Fetch features for entities with optional timeout.
    ///
    /// The returned RecordBatch must:
    /// - Have exactly one row per entity_id in the same order as requested
    /// - Include all requested feature columns, null-filled if missing
    /// - Match the schema from `describe_schema` for the same feature set
    ///
    /// # Arguments
    /// - `entity_ids` - Entity identifiers to fetch features for
    /// - `feature_names` - Feature names to retrieve
    /// - `as_of` - Point-in-time for temporal queries (None = current time)
    /// - `timeout` - Maximum time to wait for response (None = adapter default)
    ///
    /// # Errors
    /// - `AdapterError::Timeout` if operation exceeds timeout
    /// - `AdapterError::InvalidRequest` for malformed requests
    /// - `AdapterError::Internal` for backend failures
    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError>;

    /// Store features with optional configuration.
    ///
    /// # Arguments
    /// - `batch` - RecordBatch with features to store
    /// - `options` - Storage options including timeout and batch hints
    ///
    /// # Errors
    /// - `AdapterError::Timeout` if operation exceeds timeout
    /// - `AdapterError::InvalidRequest` for schema mismatches
    /// - `AdapterError::Internal` for backend failures
    async fn put(&self, batch: RecordBatch, options: &PutOptions) -> Result<(), AdapterError>;

    /// Check backend health and return operational metrics.
    ///
    /// This method should perform a lightweight operation to verify the backend
    /// is accessible and operational. It should not perform expensive operations
    /// that could impact serving performance.
    async fn health(&self) -> HealthStatus;

    /// Initialize adapter connections and validate configuration.
    ///
    /// Called once during startup to establish connections, validate
    /// configuration, and perform any necessary setup operations.
    ///
    /// # Errors
    /// - `AdapterError::Internal` if initialization fails
    async fn initialize(&mut self) -> Result<(), AdapterError> {
        // Default implementation - no initialization required
        Ok(())
    }

    /// Clean shutdown of adapter resources.
    ///
    /// Called during server shutdown to gracefully close connections
    /// and clean up resources.
    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        // Default implementation - no cleanup required
        Ok(())
    }
}

/// Adapter capabilities and operational characteristics.
///
/// This structure provides metadata about adapter behavior that allows
/// the resolver to make informed decisions about request routing, batching,
/// and timeout configuration.
#[derive(Debug, Clone)]
pub struct AdapterCapabilities {
    /// Temporal data support capabilities
    pub temporal: TemporalCapability,

    /// Maximum number of entities that can be fetched in a single request
    pub max_batch_size: Option<usize>,

    /// Optimal batch size for performance (used for request splitting)
    pub optimal_batch_size: Option<usize>,

    /// Typical latency for operations in milliseconds (used for timeout planning)
    pub typical_latency_ms: u32,

    /// Whether adapter supports parallel requests for the same feature set
    pub supports_parallel_requests: bool,
}

/// Temporal data capabilities with performance characteristics.
///
/// Enhanced to include latency information for timeout planning and
/// performance optimization.
#[derive(Debug, Clone, Copy)]
pub enum TemporalCapability {
    /// Only serves current/latest values
    CurrentOnly { typical_latency_ms: u32 },

    /// Serves recent values with approximate recency
    ApproximateRecency {
        typical_lag_seconds: u32,
        typical_latency_ms: u32,
    },

    /// Full time-travel support for historical queries
    TimeTravel { typical_latency_ms: u32 },

    /// MVCC-style range queries with transaction semantics
    MvccRange { typical_latency_ms: u32 },
}

impl TemporalCapability {
    /// Check if this adapter can serve data for the given timestamp.
    pub fn can_serve_as_of(&self, as_of: DateTime<Utc>) -> bool {
        let age_seconds = (Utc::now() - as_of).num_seconds().max(0) as u32;
        match self {
            Self::CurrentOnly { .. } => age_seconds < 5,
            Self::ApproximateRecency {
                typical_lag_seconds,
                ..
            } => age_seconds < typical_lag_seconds * 2,
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

/// Standardized health status with comprehensive operational metrics.
///
/// Enhanced to include additional metrics useful for operational monitoring
/// and capacity planning.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the backend is operational
    pub healthy: bool,

    /// Backend name for identification
    pub backend: String,

    /// Optional descriptive message about health status
    pub message: Option<String>,

    /// Last operation latency in milliseconds
    pub latency_ms: Option<f64>,

    /// Timestamp of this health check
    pub last_check: DateTime<Utc>,

    /// Whether adapter capabilities have been verified
    pub capabilities_verified: bool,

    /// Estimated request capacity (requests per second)
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

/// Options for put operations.
///
/// Provides standardized configuration for storage operations across
/// all backend adapters.
#[derive(Debug, Clone)]
pub struct PutOptions {
    /// Whether to upsert (update existing) or fail on conflicts
    pub upsert: bool,

    /// Hint about batch size for optimization
    pub batch_size_hint: Option<usize>,

    /// Maximum time to wait for operation completion
    pub timeout: Option<Duration>,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            upsert: true,
            batch_size_hint: None,
            timeout: None,
        }
    }
}

/// Standardized error types with consistent backend context.
///
/// Enhanced with additional error categories and improved error context
/// for better debugging and monitoring.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("[{backend}] Internal error: {message}")]
    Internal { backend: String, message: String },

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
