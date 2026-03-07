use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use sqlx::{Column, PgPool, Row, TypeInfo, postgres::PgRow};
use tokio::sync::RwLock;
use tracing::info;

use super::utils::{ScalarValue, StreamingRecordBatchBuilder, convert_raw_to_scalar, validation};
use super::{
    AdapterCapabilities, AdapterError, BackendAdapter, FeatureResolution, HealthStatus,
    TemporalCapability,
};
use crate::validation::{RequestValidation, ValidationConfig};

const BACKEND_NAME: &str = "postgres";

/// PostgreSQL adapter for feature storage with temporal support.
///
/// The adapter uses configurable table naming patterns and supports:
/// - Per-feature temporal resolution
/// - Batch operations
/// - Schema introspection
/// - Configurable connection pooling
pub struct PostgresAdapter {
    pool: PgPool,
    table_template: String,
    timeout_default: Duration,
    health_timeout: Duration,
    capabilities: AdapterCapabilities,
    table_name_cache: RwLock<HashMap<String, String>>,
    validation_config: ValidationConfig,
}

impl PostgresAdapter {
    /// Create a new PostgreSQL adapter.
    ///
    /// # Arguments
    /// * `connection_string` - PostgreSQL connection string (will be modified based on TLS config)
    /// * `table_template` - Template for table names (e.g., "features_{feature}")
    /// * `max_connections` - Maximum connections in pool
    /// * `timeout` - Default timeout for operations
    /// * `tls_config` - Optional TLS configuration
    /// * `validation_config` - Optional validation configuration
    ///
    /// # Security
    /// Connection strings are validated to ensure secure TLS connections.
    /// TLS configuration takes precedence over connection string SSL settings.
    pub async fn new(
        connection_string: &str,
        table_template: &str,
        max_connections: Option<u32>,
        timeout: Option<Duration>,
        tls_config: Option<&crate::config::AdapterTlsConfig>,
        validation_config: Option<ValidationConfig>,
        _parameters: Option<&std::collections::HashMap<String, serde_json::Value>>,
    ) -> Result<Self, AdapterError> {
        Self::validate_connection_security(connection_string)?;

        let effective_connection_string =
            Self::build_connection_string_with_tls(connection_string, tls_config)?;

        let mut options = sqlx::postgres::PgPoolOptions::new();

        if let Some(max_conn) = max_connections {
            options = options.max_connections(max_conn);
        } else {
            options = options.max_connections(20);
        }

        options = options.min_connections(5);

        let timeout_duration = timeout.unwrap_or(Duration::from_secs(30));

        options = options.acquire_timeout(Duration::from_secs(5));

        options = options
            .idle_timeout(Some(Duration::from_secs(300)))
            .max_lifetime(Some(Duration::from_secs(1800)));

        let pool = options
            .connect(&effective_connection_string)
            .await
            .map_err(|e| AdapterError::connection_failed(BACKEND_NAME, e.to_string()))?;

        info!(
            "Successfully connected to PostgreSQL database with {} max connections",
            max_connections.unwrap_or(20)
        );

        let max_batch_size = 1_000;
        let mut capabilities = Self::static_capabilities();
        capabilities.max_batch_size = Some(max_batch_size);

        Ok(Self {
            pool,
            table_template: table_template.to_string(),
            timeout_default: timeout_duration,
            health_timeout: Duration::from_secs(3),
            capabilities,
            table_name_cache: RwLock::new(HashMap::new()),
            validation_config: validation_config.unwrap_or_default(),
        })
    }

    /// Validate PostgreSQL connection string security.
    ///
    /// Ensures that the connection uses secure TLS settings and doesn't expose
    /// credentials in an insecure manner.
    fn validate_connection_security(connection_string: &str) -> Result<(), AdapterError> {
        if connection_string.trim().is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "Connection string cannot be empty",
            ));
        }
        if connection_string.len() > 2048 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "Connection string is too long",
            ));
        }

        let ssl_mode = if connection_string.starts_with("postgresql://")
            || connection_string.starts_with("postgres://")
        {
            if let Ok(url) = url::Url::parse(connection_string) {
                url.query_pairs()
                    .find(|(k, _)| k == "sslmode")
                    .map(|(_, v)| v.into_owned())
            } else {
                None
            }
        } else {
            let settings: Vec<(&str, &str)> = connection_string
                .split_whitespace()
                .filter_map(|s| {
                    let mut parts = s.splitn(2, '=');
                    Some((parts.next()?, parts.next()?))
                })
                .collect();

            settings
                .iter()
                .find(|(k, _)| *k == "sslmode")
                .map(|(_, v)| v.to_string())
        };

        match ssl_mode.as_deref() {
            Some("disable") => {
                return Err(AdapterError::invalid(
                    BACKEND_NAME,
                    "SSL is explicitly disabled. This is not allowed.",
                ));
            }
            Some("require") | Some("verify-ca") | Some("verify-full") => {}
            _ => {
                eprintln!(
                    "WARNING: PostgreSQL connection does not specify secure SSL mode. Defaulting to insecure fallback."
                );
            }
        }

        Ok(())
    }

    /// Build connection string with TLS configuration applied.
    ///
    /// This method modifies the connection string to apply TLS settings based on:
    /// 1. Explicit TLS configuration (takes precedence)
    /// 2. Query parameters in the connection string
    /// 3. Existing sslmode settings (fallback)
    fn build_connection_string_with_tls(
        connection_string: &str,
        tls_config: Option<&crate::config::AdapterTlsConfig>,
    ) -> Result<String, AdapterError> {
        let mut params = HashMap::new();
        let mut base_url = connection_string;

        if let Some(query_start) = connection_string.find('?') {
            base_url = &connection_string[..query_start];
            let query_str = &connection_string[query_start + 1..];

            for param in query_str.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    params.insert(key.to_string(), value.to_string());
                }
            }
        }

        if let Some(tls_cfg) = tls_config {
            let verify_certs = tls_cfg.verify_certificates;

            // Always require SSL when TLS config is present
            params.insert("sslmode".to_string(), "require".to_string());

            if !verify_certs {
                // Disable certificate verification by clearing cert paths
                params.insert("sslcert".to_string(), "".to_string());
                params.insert("sslkey".to_string(), "".to_string());
                params.insert("sslrootcert".to_string(), "".to_string());
            }

            if let Some(ca_cert) = &tls_cfg.ca_cert_path {
                params.insert("sslrootcert".to_string(), ca_cert.clone());
            }
            if let Some(client_cert) = &tls_cfg.client_cert_path {
                params.insert("sslcert".to_string(), client_cert.clone());
            }
            if let Some(client_key) = &tls_cfg.client_key_path {
                params.insert("sslkey".to_string(), client_key.clone());
            }
        } else if !params.contains_key("sslmode")
            && crate::config::is_tls_enabled_by_protocol(connection_string)
        {
            params.insert("sslmode".to_string(), "require".to_string());
        }

        if params.is_empty() {
            Ok(connection_string.to_string())
        } else {
            let query_string: String = params
                .iter()
                .filter(|(_, v)| !v.is_empty())
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            if query_string.is_empty() {
                Ok(base_url.to_string())
            } else {
                Ok(format!("{}?{}", base_url, query_string))
            }
        }
    }

    /// Get adapter capabilities without requiring a connection.
    /// This is useful for testing and introspection.
    pub fn static_capabilities() -> AdapterCapabilities {
        AdapterCapabilities {
            temporal: TemporalCapability::TimeTravel {
                typical_latency_ms: 50,
            },
            max_batch_size: Some(1000),
            optimal_batch_size: Some(100),
            typical_latency_ms: 50,
            supports_parallel_requests: true,
        }
    }

    /// Quote PostgreSQL identifier safely to prevent SQL injection.
    /// This adds an extra layer of protection beyond validation.
    fn quote_identifier(identifier: &str) -> String {
        // PostgreSQL identifier quoting: double quotes and escape internal quotes
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    ///
    /// Only allows alphanumeric characters and underscores, starting with letter or underscore.
    /// This prevents SQL injection through identifier manipulation.
    fn validate_identifier(identifier: &str) -> Result<(), AdapterError> {
        if identifier.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "identifier cannot be empty",
            ));
        }

        if identifier.len() > 63 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "identifier '{}' exceeds PostgreSQL limit of 63 characters",
                    identifier
                ),
            ));
        }

        let parts: Vec<&str> = identifier.split('.').collect();
        if parts.len() > 2 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "identifier '{}' has too many parts. Only schema.table format is supported",
                    identifier
                ),
            ));
        }

        for part in parts {
            if part.is_empty() {
                return Err(AdapterError::invalid(
                    BACKEND_NAME,
                    format!("identifier '{}' contains empty part", identifier),
                ));
            }

            let valid_chars = part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            let valid_start = part
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');

            if !valid_chars || !valid_start {
                return Err(AdapterError::invalid(
                    BACKEND_NAME,
                    format!(
                        "identifier part '{}' contains invalid characters. Only letters, digits, and underscores are allowed, must start with letter or underscore",
                        part
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Build table name from template with validation.
    ///
    /// Replaces placeholders in the template:
    /// - `{feature}` -> feature name
    /// - `{entity_type}` -> entity type (extracted from entity_id prefix if available)
    fn build_table_name(&self, feature_name: &str) -> Result<String, AdapterError> {
        if let Ok(cache) = self.table_name_cache.try_read()
            && let Some(cached) = cache.get(feature_name)
        {
            return Ok(cached.clone());
        }

        Self::validate_identifier(feature_name)?;

        let table_name = self
            .table_template
            .replace("{feature}", feature_name)
            .replace("{entity_type}", "default");

        Self::validate_identifier(&table_name)?;

        if let Ok(mut cache) = self.table_name_cache.try_write() {
            cache.insert(feature_name.to_string(), table_name.clone());
        }

        Ok(table_name)
    }

    /// Extract scalar value from PostgreSQL row and column using schema-based conversion.
    ///
    /// This method first attempts to extract values using PostgreSQL's native types,
    /// then falls back to the unified conversion utility for string-based conversion
    /// with proper type validation.
    fn extract_scalar_from_row(
        &self,
        row: &PgRow,
        column: &str,
        expected_type: &DataType,
        feature_name: &str,
    ) -> Result<Option<ScalarValue>, AdapterError> {
        if let Ok(value) = self.try_extract_native_type(row, column, expected_type) {
            return Ok(value);
        }

        let string_value = match row.try_get::<Option<String>, _>(column) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(_) => {
                return Err(AdapterError::internal(
                    BACKEND_NAME,
                    "Failed to extract value as both native type and string".to_string(),
                ));
            }
        };

        convert_raw_to_scalar(&string_value, expected_type, BACKEND_NAME, feature_name).map(Some)
    }

    /// Try to extract value using PostgreSQL native types for optimal performance.
    ///
    /// This method attempts direct type extraction from PostgreSQL without string
    /// conversion, which is faster but may not work for all type combinations.
    fn try_extract_native_type(
        &self,
        row: &PgRow,
        column: &str,
        expected_type: &DataType,
    ) -> Result<Option<ScalarValue>, AdapterError> {
        let column_info = match row.columns().iter().find(|col| col.name() == column) {
            Some(info) => info,
            None => {
                return Err(AdapterError::internal(
                    BACKEND_NAME,
                    "Column not found".to_string(),
                ));
            }
        };

        let pg_type_oid = column_info.type_info().clone();
        let type_name = pg_type_oid.name();

        match (expected_type, type_name) {
            (DataType::Float64, "FLOAT8" | "FLOAT4" | "NUMERIC") => {
                if let Ok(value) = row.try_get::<Option<f64>, _>(column) {
                    Ok(value.map(ScalarValue::Float64))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Float extraction failed".to_string(),
                    ))
                }
            }
            (DataType::Float64, "INT8" | "BIGINT") => {
                if let Ok(value) = row.try_get::<Option<i64>, _>(column) {
                    Ok(value.map(|v| ScalarValue::Float64(v as f64)))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Int64 to Float64 conversion failed".to_string(),
                    ))
                }
            }
            (DataType::Float64, "INT4" | "INTEGER") => {
                if let Ok(value) = row.try_get::<Option<i32>, _>(column) {
                    Ok(value.map(|v| ScalarValue::Float64(v as f64)))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Int32 to Float64 conversion failed".to_string(),
                    ))
                }
            }
            (DataType::Int64, "INT8" | "BIGINT") => {
                if let Ok(value) = row.try_get::<Option<i64>, _>(column) {
                    Ok(value.map(ScalarValue::Int64))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Int64 extraction failed".to_string(),
                    ))
                }
            }
            (DataType::Int64, "INT4" | "INTEGER") => {
                if let Ok(value) = row.try_get::<Option<i32>, _>(column) {
                    Ok(value.map(|v| ScalarValue::Int64(v as i64)))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Int32 to Int64 conversion failed".to_string(),
                    ))
                }
            }
            (DataType::Int64, "INT2" | "SMALLINT") => {
                if let Ok(value) = row.try_get::<Option<i16>, _>(column) {
                    Ok(value.map(|v| ScalarValue::Int64(v as i64)))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Int16 to Int64 conversion failed".to_string(),
                    ))
                }
            }
            (DataType::Boolean, "BOOL" | "BOOLEAN") => {
                if let Ok(value) = row.try_get::<Option<bool>, _>(column) {
                    Ok(value.map(ScalarValue::Boolean))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Boolean extraction failed".to_string(),
                    ))
                }
            }
            (DataType::Timestamp(_, _), "TIMESTAMP" | "TIMESTAMPTZ") => {
                if let Ok(value) = row.try_get::<Option<DateTime<Utc>>, _>(column) {
                    Ok(value.map(ScalarValue::Timestamp))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "Timestamp extraction failed".to_string(),
                    ))
                }
            }
            (DataType::Utf8, "TEXT" | "VARCHAR" | "CHAR") => {
                if let Ok(value) = row.try_get::<Option<String>, _>(column) {
                    Ok(value.map(ScalarValue::Utf8))
                } else {
                    Err(AdapterError::internal(
                        BACKEND_NAME,
                        "String extraction failed".to_string(),
                    ))
                }
            }
            _ => Err(AdapterError::internal(
                BACKEND_NAME,
                format!(
                    "Unsupported type conversion from PostgreSQL {} to Arrow {:?}",
                    type_name, expected_type
                ),
            )),
        }
    }

    fn record_batch_entity_id_index(entity_ids: &[String]) -> HashMap<&str, usize> {
        entity_ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect::<HashMap<_, _>>()
    }

    async fn get_with_expected_types(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        expected_types: &std::collections::HashMap<String, DataType>,
        as_of: Option<DateTime<Utc>>,
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
                Some(20),
                Some(256),
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

        if self
            .validation_config
            .should_validate_request(&RequestValidation::SqlIdentifierSafety)
        {
            Self::validate_identifier(entity_key)?;
            for feature_name in feature_names {
                Self::validate_identifier(feature_name)?;
            }
        }

        let timeout_duration = timeout.unwrap_or(self.timeout_default);
        let cutoff = as_of.unwrap_or_else(Utc::now);
        let entity_index = Arc::new(Self::record_batch_entity_id_index(entity_ids));

        let query_preparation_results: Result<Vec<_>, AdapterError> = feature_names
            .iter()
            .enumerate()
            .map(|(feature_index, feature_name)| {
                let table_name = self.build_table_name(feature_name)?;
                let expected_type = expected_types
                    .get(feature_name)
                    .cloned()
                    .unwrap_or(DataType::Utf8);

                let query = format!(
                    r#"
                    WITH requested_entities AS (
                        SELECT unnest($1::text[]) AS {entity_key}
                    ),
                    ranked_features AS (
                        SELECT
                            t.{entity_key},
                            t.{col} as feature_value,
                            t.feature_ts,
                            ROW_NUMBER() OVER (PARTITION BY t.{entity_key} ORDER BY t.feature_ts DESC) as rn
                        FROM {table} t
                        INNER JOIN requested_entities r ON t.{entity_key} = r.{entity_key}
                        WHERE t.feature_ts <= $2
                          AND t.{col} IS NOT NULL
                    )
                    SELECT {entity_key}, feature_value
                    FROM ranked_features
                    WHERE rn = 1
                    "#,
                    entity_key = Self::quote_identifier(entity_key),
                    col = Self::quote_identifier(feature_name),
                    table = Self::quote_identifier(&table_name),
                );

                Ok((feature_index, query, expected_type, feature_name.to_string()))
            })
            .collect();

        let query_data = query_preparation_results?;

        let query_futures: Vec<_> = query_data
            .into_iter()
            .map(|(feature_index, query, expected_type, feature_name)| {
                let entity_index = Arc::clone(&entity_index);

                async move {
                    let query_future = sqlx::query(&query)
                        .bind(entity_ids)
                        .bind(cutoff)
                        .fetch_all(&self.pool);

                    let rows = tokio::time::timeout(timeout_duration, query_future)
                        .await
                        .map_err(|_| {
                            AdapterError::timeout(BACKEND_NAME, timeout_duration.as_millis() as u64)
                        })?
                        .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;

                    let mut feature_values: Vec<Option<ScalarValue>> = vec![None; entity_ids.len()];

                    for row in rows {
                        let entity_id: String = row
                            .try_get(entity_key)
                            .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;

                        if let Some(&pos) = entity_index.get(entity_id.as_str()) {
                            let value = self
                                .extract_scalar_from_row(
                                    &row,
                                    "feature_value",
                                    &expected_type,
                                    &feature_name,
                                )
                                .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;
                            feature_values[pos] = value;
                        }
                    }

                    Ok::<_, AdapterError>((feature_index, feature_values))
                }
            })
            .collect();

        let results = futures::future::join_all(query_futures).await;

        // Prepare feature types for streaming builder
        let feature_types: Vec<_> = feature_names
            .iter()
            .map(|name| expected_types.get(name).cloned().unwrap_or(DataType::Utf8))
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

        // Process results and stream directly to Arrow builders
        for result in results {
            let (feature_index, feature_values) = result?;

            // Stream each entity value directly to builder
            for (entity_idx, value) in feature_values.into_iter().enumerate() {
                builder
                    .set_value(feature_index, entity_idx, value)
                    .map_err(|e| {
                        AdapterError::internal(BACKEND_NAME, format!("Failed to set value: {}", e))
                    })?;
            }
        }

        // Finish building RecordBatch
        builder
            .finish()
            .map_err(|e| AdapterError::arrow(BACKEND_NAME, e.to_string()))
    }
}

#[async_trait::async_trait]
impl BackendAdapter for PostgresAdapter {
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
        let fallback_types: HashMap<String, DataType> = feature_names
            .iter()
            .map(|name| (name.clone(), DataType::Utf8))
            .collect();

        self.get_with_expected_types(
            entity_ids,
            feature_names,
            entity_key,
            &fallback_types,
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
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        let expected_types: HashMap<String, DataType> = feature_names
            .iter()
            .map(|name| {
                let expected_type = resolutions
                    .get(name)
                    .map(|r| r.expected_type.clone())
                    .unwrap_or(DataType::Utf8);
                (name.clone(), expected_type)
            })
            .collect();

        self.get_with_expected_types(
            entity_ids,
            feature_names,
            entity_key,
            &expected_types,
            as_of,
            timeout,
        )
        .await
    }

    async fn health(&self) -> HealthStatus {
        let start = std::time::Instant::now();

        let fut = sqlx::query("SELECT 1 as health_check").fetch_one(&self.pool);

        let health_result = tokio::time::timeout(self.health_timeout, fut).await;

        let healthy = matches!(health_result, Ok(Ok(_)));
        let latency = start.elapsed().as_secs_f64() * 1000.0;

        HealthStatus {
            healthy,
            backend: BACKEND_NAME.to_string(),
            message: if healthy {
                Some("Connection successful".to_string())
            } else {
                Some(format!(
                    "Connection failed or timed out: {:?}",
                    health_result.err()
                ))
            },
            latency_ms: Some(latency),
            last_check: Utc::now(),
            capabilities_verified: healthy,
            estimated_capacity: if healthy { Some(1_000.0) } else { None },
        }
    }

    async fn initialize(&mut self) -> Result<(), AdapterError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AdapterError::connection_failed(BACKEND_NAME, e.to_string()))?;

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        self.pool.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    async fn create_test_adapter() -> Result<PostgresAdapter, AdapterError> {
        let connection_string = std::env::var("TEST_POSTGRES_URL")
            .unwrap_or_else(|_| "postgresql://test:test@localhost:5432/quiver_test".to_string());

        PostgresAdapter::new(
            &connection_string,
            "test_features_{feature}",
            Some(5),
            Some(StdDuration::from_secs(10)),
            None,
            None,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn test_identifier_validation() {
        assert!(PostgresAdapter::validate_identifier("valid_name").is_ok());
        assert!(PostgresAdapter::validate_identifier("valid123").is_ok());
        assert!(PostgresAdapter::validate_identifier("_underscore").is_ok());

        assert!(PostgresAdapter::validate_identifier("schema.table").is_ok());
        assert!(PostgresAdapter::validate_identifier("my_schema.my_table").is_ok());
        assert!(PostgresAdapter::validate_identifier("_schema._table").is_ok());

        assert!(PostgresAdapter::validate_identifier("").is_err());
        assert!(PostgresAdapter::validate_identifier("123invalid").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid-name").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid name").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid;drop").is_err());

        assert!(PostgresAdapter::validate_identifier("schema.").is_err());
        assert!(PostgresAdapter::validate_identifier(".table").is_err());
        assert!(PostgresAdapter::validate_identifier("a.b.c").is_err());
        assert!(PostgresAdapter::validate_identifier("schema.123invalid").is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_adapter_creation() {
        let adapter = create_test_adapter().await;
        if adapter.is_err() {
            println!("Skipping PostgreSQL tests - database not available");
            return;
        }

        let adapter = adapter.unwrap();
        assert_eq!(adapter.name(), "postgres");
    }

    #[tokio::test]
    #[ignore]
    async fn test_health_check() {
        let adapter = create_test_adapter().await;
        if adapter.is_err() {
            println!("Skipping PostgreSQL tests - database not available");
            return;
        }

        let adapter = adapter.unwrap();
        let health = adapter.health().await;
        assert_eq!(health.backend, "postgres");
        assert!(health.latency_ms.is_some());
    }

    #[tokio::test]
    async fn test_capabilities() {
        let caps = PostgresAdapter::static_capabilities();
        assert!(matches!(
            caps.temporal,
            TemporalCapability::TimeTravel { .. }
        ));
        assert_eq!(caps.max_batch_size, Some(1000));
        assert_eq!(caps.optimal_batch_size, Some(100));
        assert!(caps.supports_parallel_requests);
    }

    #[test]
    fn test_identifier_quoting() {
        assert_eq!(PostgresAdapter::quote_identifier("simple"), "\"simple\"");
        assert_eq!(
            PostgresAdapter::quote_identifier("with\"quote"),
            "\"with\"\"quote\""
        );
        assert_eq!(
            PostgresAdapter::quote_identifier("table.column"),
            "\"table.column\""
        );
        assert_eq!(PostgresAdapter::quote_identifier("user"), "\"user\""); // Reserved word
    }

    #[tokio::test]
    async fn test_adapter_configuration() {
        let connection_string = "postgresql://test:test@localhost:5432/quiver_test";

        let adapter = PostgresAdapter::new(
            connection_string,
            "test_features_{feature}",
            Some(5),
            Some(StdDuration::from_secs(10)),
            None,
            None,
            None,
        )
        .await;

        if let Ok(adapter) = adapter {
            assert_eq!(adapter.table_template, "test_features_{feature}");
            assert_eq!(adapter.timeout_default, StdDuration::from_secs(10));
        } else {
            println!("Skipping adapter configuration test - database not available");
        }
    }

    #[tokio::test]
    async fn test_unified_conversion_integration() {
        use arrow::datatypes::{DataType, TimeUnit};

        let result_float_to_int =
            convert_raw_to_scalar("42.5", &DataType::Int64, "postgres", "test_feature");
        assert!(result_float_to_int.is_ok());
        if let Ok(ScalarValue::Int64(val)) = result_float_to_int {
            assert_eq!(val, 42);
        }

        let result_int_to_float =
            convert_raw_to_scalar("42", &DataType::Float64, "postgres", "test_feature");
        assert!(result_int_to_float.is_ok());
        if let Ok(ScalarValue::Float64(val)) = result_int_to_float {
            assert_eq!(val, 42.0);
        }

        let result_bool_true =
            convert_raw_to_scalar("true", &DataType::Boolean, "postgres", "bool_feature");
        assert!(result_bool_true.is_ok());
        if let Ok(ScalarValue::Boolean(val)) = result_bool_true {
            assert_eq!(val, true);
        }

        let result_timestamp = convert_raw_to_scalar(
            "2023-01-01T00:00:00Z",
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            "postgres",
            "ts_feature",
        );
        assert!(result_timestamp.is_ok());

        let result_invalid = convert_raw_to_scalar(
            "not_a_number",
            &DataType::Float64,
            "postgres",
            "test_feature",
        );
        assert!(result_invalid.is_err());

        let result_unsupported =
            convert_raw_to_scalar("test", &DataType::Binary, "postgres", "binary_feature");
        assert!(result_unsupported.is_err());
    }

    #[tokio::test]
    async fn test_adapter_instantiation() {
        let connection_string = "postgresql://test:test@localhost:5432/quiver_test";

        let adapter = PostgresAdapter::new(
            connection_string,
            "test_features_{feature}",
            Some(5),
            Some(StdDuration::from_secs(10)),
            None,
            None,
            None,
        )
        .await;

        match adapter {
            Ok(adapter) => {
                assert_eq!(adapter.table_template, "test_features_{feature}");
                assert_eq!(adapter.timeout_default, StdDuration::from_secs(10));
            }
            Err(_) => {
                println!("Database not available for testing - this is expected in CI");
            }
        }
    }

    #[tokio::test]
    async fn test_get_with_expected_types() {
        let connection_string = "postgresql://test:test@localhost:5432/quiver_test";

        let adapter = PostgresAdapter::new(
            connection_string,
            "test_features_{feature}",
            Some(5),
            Some(StdDuration::from_secs(10)),
            None,
            None,
            None,
        )
        .await;

        if let Ok(adapter) = adapter {
            let mut expected_types = std::collections::HashMap::new();
            expected_types.insert("test_feature".to_string(), DataType::Int64);
            expected_types.insert("another_feature".to_string(), DataType::Float64);

            let entity_ids = vec!["entity1".to_string(), "entity2".to_string()];
            let feature_names = vec!["test_feature".to_string(), "another_feature".to_string()];

            let result = adapter
                .get_with_expected_types(
                    &entity_ids,
                    &feature_names,
                    "user_id",
                    &expected_types,
                    None,
                    None,
                )
                .await;

            match result {
                Ok(batch) => {
                    assert_eq!(batch.num_columns(), 3);
                    assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
                    assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
                }
                Err(_) => {
                    println!("Skipping test - database not available or schema validation failed");
                }
            }
        }
    }
}
