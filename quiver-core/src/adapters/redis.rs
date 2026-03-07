use crate::adapters::utils::{StreamingRecordBatchBuilder, convert_raw_to_scalar, validation};
use crate::adapters::{
    AdapterCapabilities, AdapterError, BackendAdapter, FeatureResolution, HealthStatus,
    TemporalCapability,
};
use crate::config::SourcePath;
use crate::validation::{RequestValidation, ValidationConfig};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::time::Duration;

const BACKEND_NAME: &str = "redis";
const DEFAULT_MGET_CHUNK_SIZE: usize = 1000;

#[derive(Clone)]
pub struct RedisAdapter {
    connection: redis::aio::MultiplexedConnection,
    default_source_path: SourcePath,
    timeout_default: Duration,
    health_timeout: Duration,
    capabilities: AdapterCapabilities,
    validation_config: ValidationConfig,
    mget_chunk_size: usize,
}

impl RedisAdapter {
    /// Create a new Redis adapter.
    pub async fn new(
        url: &str,
        password: Option<&str>,
        source_path: &SourcePath,
        timeout: Option<Duration>,
        tls_config: Option<&crate::config::AdapterTlsConfig>,
        validation_config: Option<ValidationConfig>,
        parameters: Option<&std::collections::HashMap<String, serde_json::Value>>,
    ) -> Result<Self, AdapterError> {
        if let SourcePath::Structured { .. } = source_path {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "source_path.table/column (Structured) is not supported for Redis; use a Template such as '{feature}:{entity}'",
            ));
        }

        if let SourcePath::Template(tmpl) = source_path
            && !tmpl.contains("{feature}")
            && !tmpl.contains("{entity}")
        {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "source_path template '{}' contains neither {{feature}} nor {{entity}} -- all keys would be identical",
                    tmpl
                ),
            ));
        }

        let _tls_enabled = if let Some(tls_cfg) = tls_config {
            tls_cfg.is_tls_enabled(url)
        } else {
            crate::config::is_tls_enabled_by_protocol(url)
        };

        let client = redis::Client::open(url).map_err(|e| {
            AdapterError::connection_failed(BACKEND_NAME, format!("Redis client error: {}", e))
        })?;

        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                AdapterError::connection_failed(BACKEND_NAME, format!("Connection failed: {}", e))
            })?;

        if let Some(pwd) = password {
            let mut auth_conn = connection.clone();
            let _: () = redis::cmd("AUTH")
                .arg(pwd)
                .query_async(&mut auth_conn)
                .await
                .map_err(|e| {
                    AdapterError::connection_failed(
                        BACKEND_NAME,
                        format!("Authentication failed: {}", e),
                    )
                })?;
        }

        let timeout_duration = timeout.unwrap_or(Duration::from_secs(5));

        let capabilities = AdapterCapabilities {
            temporal: TemporalCapability::CurrentOnly {
                typical_latency_ms: 5,
            },
            max_batch_size: Some(5_000),
            optimal_batch_size: Some(100),
            typical_latency_ms: 5,
            supports_parallel_requests: true,
        };

        let mget_chunk_size = parameters
            .and_then(|p| p.get("mget_chunk_size"))
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_MGET_CHUNK_SIZE);

        Ok(Self {
            connection,
            default_source_path: source_path.clone(),
            timeout_default: timeout_duration,
            health_timeout: Duration::from_secs(3),
            capabilities,
            validation_config: validation_config.unwrap_or_default(),
            mget_chunk_size,
        })
    }

    fn validate_key_component(component: &str, component_name: &str) -> Result<(), AdapterError> {
        if component.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!("{} cannot be empty", component_name),
            ));
        }

        if component.len() > 250 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "{} exceeds maximum length of 250 characters",
                    component_name
                ),
            ));
        }

        if component.chars().any(|c| {
            c.is_control()
                || c.is_whitespace()
                || matches!(c, '*' | '?' | '[' | ']' | '\\' | '\r' | '\n' | '\0')
        }) {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "{} contains invalid characters. Only alphanumeric, hyphens, underscores, periods, and colons are allowed",
                    component_name
                ),
            ));
        }

        Ok(())
    }

    fn validate_feature_names(&self, feature_names: &[String]) -> Result<(), AdapterError> {
        if !self
            .validation_config
            .should_validate_request(&RequestValidation::FeatureNameFormat)
        {
            return Ok(());
        }

        for feature_name in feature_names {
            Self::validate_key_component(feature_name, "feature_name")?;
        }
        Ok(())
    }

    fn validate_entity_ids(&self, entity_ids: &[String]) -> Result<(), AdapterError> {
        if !self
            .validation_config
            .should_validate_request(&RequestValidation::EntityIdFormat)
        {
            return Ok(());
        }

        for entity_id in entity_ids {
            Self::validate_key_component(entity_id, "entity_id")?;
        }
        Ok(())
    }

    fn validate_redis_key_safety(
        &self,
        feature_names: &[String],
        entity_ids: &[String],
    ) -> Result<(), AdapterError> {
        if !self
            .validation_config
            .should_validate_request(&RequestValidation::RedisKeySafety)
        {
            return Ok(());
        }

        self.validate_feature_names(feature_names)?;
        self.validate_entity_ids(entity_ids)?;
        Ok(())
    }

    fn build_key_unchecked(
        &self,
        feature_name: &str,
        entity_id: &str,
        source_path: &SourcePath,
    ) -> Result<String, AdapterError> {
        match source_path {
            SourcePath::Template(template) => Ok(template
                .replace("{feature}", feature_name)
                .replace("{entity}", entity_id)),
            SourcePath::Structured { .. } => Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "feature '{}': Structured source_path is not supported for Redis",
                    feature_name
                ),
            )),
        }
    }
}

#[async_trait::async_trait]
impl BackendAdapter for RedisAdapter {
    fn name(&self) -> &str {
        BACKEND_NAME
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities
    }

    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        let resolutions: HashMap<String, FeatureResolution> = feature_names
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    FeatureResolution {
                        expected_type: DataType::Utf8,
                        source_path: None,
                    },
                )
            })
            .collect();

        self.get_with_resolutions(
            entity_ids,
            feature_names,
            entity_key,
            &resolutions,
            as_of,
            timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_with_resolutions(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        resolutions: &HashMap<String, FeatureResolution>,
        _as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        if self
            .validation_config
            .should_validate_request(&RequestValidation::EntityIdsNotEmpty)
        {
            validation::validate_entity_ids_not_empty(entity_ids, BACKEND_NAME)?;
        }

        if self
            .validation_config
            .should_validate_request(&RequestValidation::FeatureNamesNotEmpty)
        {
            validation::validate_feature_names_not_empty(feature_names, BACKEND_NAME)?;
        }

        if self
            .validation_config
            .should_validate_request(&RequestValidation::MemoryConstraints)
        {
            validation::validate_memory_constraints(
                entity_ids.len(),
                feature_names.len(),
                Some(15),
                Some(128),
                BACKEND_NAME,
            )?;
        }

        if self
            .validation_config
            .should_validate_request(&RequestValidation::BatchSizeLimit)
        {
            validation::validate_batch_size(
                entity_ids.len(),
                self.capabilities.max_batch_size,
                BACKEND_NAME,
            )?;
        }

        let timeout_duration = timeout.unwrap_or(self.timeout_default);
        let timeout_budget = crate::timeout::TimeoutBudget::new(timeout_duration);

        // Use a fixed 500ms safety margin for Redis operations
        let redis_timeout = timeout_budget
            .adapter_timeout(500) // 500ms safety margin
            .unwrap_or(timeout_duration);

        self.validate_redis_key_safety(feature_names, entity_ids)?;

        // Build all keys and metadata for chunked execution
        // Note: This pre-allocates all keys for concurrent chunk processing.
        // Memory usage: ~(features × entities × 64 bytes) for keys + metadata
        // This is still much more efficient than the previous 2D ScalarValue matrix
        let mut all_keys = Vec::new();
        let mut key_metadata = Vec::new(); // (feature_idx, entity_idx, expected_type)

        for (feature_idx, feature_name) in feature_names.iter().enumerate() {
            let resolution = resolutions.get(feature_name);

            let effective_source_path = resolution
                .and_then(|r| r.source_path.as_ref())
                .unwrap_or(&self.default_source_path);

            let expected_type = resolution
                .map(|r| r.expected_type.clone())
                .unwrap_or(DataType::Utf8);

            for (entity_idx, entity_id) in entity_ids.iter().enumerate() {
                let key =
                    self.build_key_unchecked(feature_name, entity_id, effective_source_path)?;

                all_keys.push(key);
                key_metadata.push((
                    feature_idx,
                    entity_idx,
                    expected_type.clone(),
                    feature_name.clone(),
                ));
            }
        }

        // Chunked concurrent MGET requests
        let query_future = async {
            // Split keys and metadata into chunks
            let key_chunks: Vec<_> = all_keys.chunks(self.mget_chunk_size).collect();
            let metadata_chunks: Vec<_> = key_metadata.chunks(self.mget_chunk_size).collect();

            // Create futures for each chunk
            let chunk_futures: Vec<_> = key_chunks
                .into_iter()
                .zip(metadata_chunks)
                .enumerate()
                .map(|(chunk_idx, (keys_chunk, metadata_chunk))| {
                    let mut conn = self.connection.clone();
                    let keys_vec = keys_chunk.to_vec();
                    let metadata_vec = metadata_chunk.to_vec();

                    async move {
                        let raw_values: Vec<Option<String>> =
                            conn.mget(&keys_vec).await.map_err(|e| {
                                AdapterError::internal(
                                    BACKEND_NAME,
                                    format!(
                                        "Redis chunked MGET failed for chunk {}: {}",
                                        chunk_idx, e
                                    ),
                                )
                            })?;

                        Ok::<
                            (Vec<Option<String>>, Vec<(usize, usize, DataType, String)>),
                            AdapterError,
                        >((raw_values, metadata_vec))
                    }
                })
                .collect();

            // Execute all chunks concurrently
            let chunk_results = join_all(chunk_futures).await;

            // Collect feature types for streaming builder
            let feature_types: Vec<_> = feature_names
                .iter()
                .map(|name| {
                    resolutions
                        .get(name)
                        .map(|r| r.expected_type.clone())
                        .unwrap_or(DataType::Utf8)
                })
                .collect();

            // Create streaming builder - no 2D matrix allocation!
            let mut builder = StreamingRecordBatchBuilder::new_with_types(
                entity_ids,
                feature_names,
                entity_key,
                feature_types,
            )
            .map_err(|e| {
                AdapterError::internal(
                    BACKEND_NAME,
                    format!("Failed to create streaming builder: {}", e),
                )
            })?;

            // Process all chunk results and stream directly to Arrow builders
            for chunk_result in chunk_results {
                let (raw_values, metadata_chunk) = chunk_result?;

                for (key_idx_in_chunk, raw_value) in raw_values.into_iter().enumerate() {
                    let (feature_idx, entity_idx, expected_type, feature_name) =
                        &metadata_chunk[key_idx_in_chunk];

                    let converted_value = match raw_value {
                        Some(raw) => Some(convert_raw_to_scalar(
                            &raw,
                            expected_type,
                            BACKEND_NAME,
                            feature_name,
                        )?),
                        None => None,
                    };

                    // Stream directly to builder - no intermediate storage!
                    builder
                        .set_value(*feature_idx, *entity_idx, converted_value)
                        .map_err(|e| {
                            AdapterError::internal(
                                BACKEND_NAME,
                                format!("Failed to set value: {}", e),
                            )
                        })?;
                }
            }

            // Finish building RecordBatch
            builder.finish().map_err(|e| {
                AdapterError::internal(BACKEND_NAME, format!("Failed to finish RecordBatch: {}", e))
            })
        };

        let record_batch = tokio::time::timeout(redis_timeout, query_future)
            .await
            .map_err(|_| AdapterError::timeout(BACKEND_NAME, redis_timeout.as_millis() as u64))??;

        Ok(record_batch)
    }

    async fn health(&self) -> HealthStatus {
        let mut conn = self.connection.clone();
        let start = std::time::Instant::now();

        let healthy = tokio::time::timeout(
            self.health_timeout,
            redis::cmd("PING").query_async::<String>(&mut conn),
        )
        .await
        .is_ok_and(|result| result.is_ok());

        HealthStatus {
            healthy,
            backend: self.name().to_string(),
            message: if healthy {
                None
            } else {
                Some("Ping failed".to_string())
            },
            latency_ms: Some(start.elapsed().as_secs_f64() * 1000.0),
            last_check: Utc::now(),
            capabilities_verified: healthy,
            estimated_capacity: if healthy { Some(10_000.0) } else { None },
        }
    }

    async fn initialize(&mut self) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[tokio::test]
    async fn test_key_validation() {
        assert!(RedisAdapter::validate_key_component("valid_key", "test").is_ok());
        assert!(RedisAdapter::validate_key_component("valid123", "test").is_ok());
        assert!(RedisAdapter::validate_key_component("user:123", "test").is_ok());

        assert!(RedisAdapter::validate_key_component("", "test").is_err());
        assert!(RedisAdapter::validate_key_component("invalid key", "test").is_err());
        assert!(RedisAdapter::validate_key_component("invalid*key", "test").is_err());

        let long_key = "x".repeat(251);
        assert!(RedisAdapter::validate_key_component(&long_key, "test").is_err());
    }

    #[tokio::test]
    async fn test_source_path_validation() {
        let template_path = SourcePath::Template("{feature}:{entity}".to_string());
        let structured_path = SourcePath::Structured {
            table: "features".to_string(),
            column: Some("value".to_string()),
        };

        let result = RedisAdapter::new(
            "redis://localhost:6379",
            None,
            &template_path,
            Some(StdDuration::from_secs(5)),
            None,
            None,
            None,
        )
        .await;

        match result {
            Ok(_) => println!("✓ Template source path accepted"),
            Err(_) => println!("Redis adapter creation skipped - server not available"),
        }

        let result = RedisAdapter::new(
            "redis://localhost:6379",
            None,
            &structured_path,
            Some(StdDuration::from_secs(5)),
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err(), "Structured source path should be rejected");
        println!("✓ Structured source path correctly rejected");
    }
}
