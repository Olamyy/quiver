pub mod memory;
pub mod redis;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};

#[async_trait::async_trait]
pub trait BackendAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn temporal_capability(&self) -> TemporalCapability;

    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        as_of: Option<DateTime<Utc>>,
    ) -> Result<RecordBatch, AdapterError>;

    async fn put(&self, batch: RecordBatch) -> Result<(), AdapterError>;

    async fn health(&self) -> HealthStatus;
}

#[derive(Debug, Clone, Copy)]
pub enum TemporalCapability {
    CurrentOnly,
    ApproximateRecency { typical_lag_seconds: u32 },
    TimeTravel,
    MvccRange,
}

impl TemporalCapability {
    pub fn can_serve_as_of(&self, as_of: DateTime<Utc>) -> bool {
        let age_seconds = (Utc::now() - as_of).num_seconds().max(0) as u32;
        match self {
            Self::CurrentOnly => age_seconds < 5,
            Self::ApproximateRecency {
                typical_lag_seconds,
            } => age_seconds < typical_lag_seconds * 2,
            Self::TimeTravel => true,
            Self::MvccRange => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub backend: String,
    pub message: Option<String>,
    pub latency_ms: Option<f64>,
}

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
}

impl AdapterError {
    pub fn internal(backend: &str, msg: impl Into<String>) -> Self {
        Self::Internal {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    pub fn invalid(backend: &str, msg: impl Into<String>) -> Self {
        Self::InvalidRequest {
            backend: backend.to_string(),
            message: msg.into(),
        }
    }

    pub fn arrow(backend: &str, msg: impl Into<String>) -> Self {
        Self::Internal {
            backend: backend.to_string(),
            message: format!("Arrow error: {}", msg.into()),
        }
    }

    pub fn timeout(backend: &str, timeout_ms: u64) -> Self {
        Self::Timeout {
            backend: backend.to_string(),
            timeout_ms,
        }
    }
}
