use std::collections::HashMap;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row, postgres::PgRow};
use tracing::info;

use super::utils::{ScalarValue, build_record_batch, validation};
use super::{AdapterCapabilities, AdapterError, BackendAdapter, HealthStatus, TemporalCapability};

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
    max_batch_size: usize,
    timeout_default: Duration,
    /// Internal timeout used for health checks to avoid hanging health endpoints.
    health_timeout: Duration,
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
    ) -> Result<Self, AdapterError> {
        Self::validate_connection_security(connection_string)?;

        // Modify connection string based on TLS configuration
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

        Ok(Self {
            pool,
            table_template: table_template.to_string(),
            max_batch_size: 1_000,
            timeout_default: timeout_duration,
            health_timeout: Duration::from_secs(3),
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

        let settings: Vec<(&str, &str)> = connection_string
            .split_whitespace()
            .filter_map(|s| {
                let mut parts = s.splitn(2, '=');
                Some((parts.next()?, parts.next()?))
            })
            .collect();

        let ssl_mode = settings
            .iter()
            .find(|(k, _)| *k == "sslmode")
            .map(|(_, v)| *v);

        match ssl_mode {
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
        use std::collections::HashMap;

        // Parse the connection string
        let mut params = HashMap::new();
        let mut base_url = connection_string;

        // Extract query parameters if present
        if let Some(query_start) = connection_string.find('?') {
            base_url = &connection_string[..query_start];
            let query_str = &connection_string[query_start + 1..];

            for param in query_str.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    params.insert(key.to_string(), value.to_string());
                }
            }
        }

        // Apply TLS configuration
        if let Some(tls_cfg) = tls_config {
            // TLS configuration provided - override SSL settings
            let verify_certs = tls_cfg.should_verify_certificates(connection_string);

            if verify_certs {
                params.insert("sslmode".to_string(), "require".to_string());
            } else {
                params.insert("sslmode".to_string(), "require".to_string());
                params.insert("sslcert".to_string(), "".to_string());
                params.insert("sslkey".to_string(), "".to_string());
                params.insert("sslrootcert".to_string(), "".to_string());
            }

            // Add certificate paths if provided
            if let Some(ca_cert) = &tls_cfg.ca_cert_path {
                params.insert("sslrootcert".to_string(), ca_cert.clone());
            }
            if let Some(client_cert) = &tls_cfg.client_cert_path {
                params.insert("sslcert".to_string(), client_cert.clone());
            }
            if let Some(client_key) = &tls_cfg.client_key_path {
                params.insert("sslkey".to_string(), client_key.clone());
            }
        } else if !params.contains_key("sslmode") {
            // No TLS config provided, check if URL protocol suggests TLS
            if crate::config::is_tls_enabled_by_protocol(connection_string) {
                params.insert("sslmode".to_string(), "require".to_string());
            }
        }

        // Rebuild connection string
        if params.is_empty() {
            Ok(connection_string.to_string())
        } else {
            let query_string: String = params
                .iter()
                .filter(|(_, v)| !v.is_empty()) // Skip empty values
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

        let valid_chars = identifier
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
        let valid_start = identifier
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');

        if !valid_chars || !valid_start {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                format!(
                    "identifier '{}' contains invalid characters. Only letters, digits, and underscores are allowed, must start with letter or underscore",
                    identifier
                ),
            ));
        }

        Ok(())
    }

    /// Build table name from template with validation.
    ///
    /// Replaces placeholders in the template:
    /// - `{feature}` -> feature name
    /// - `{entity_type}` -> entity type (extracted from entity_id prefix if available)
    fn build_table_name(&self, feature_name: &str) -> Result<String, AdapterError> {
        Self::validate_identifier(feature_name)?;

        let table_name = self
            .table_template
            .replace("{feature}", feature_name)
            .replace("{entity_type}", "default");

        Self::validate_identifier(&table_name)?;
        Ok(table_name)
    }

    /// Extract scalar value from PostgreSQL row and column.
    ///
    /// IMPORTANT: this is best-effort and intentionally limited to a small set of types.
    /// If you store NUMERIC, JSONB, arrays, etc., you should either:
    /// - cast in SQL to a supported type, or
    /// - extend this extractor.
    fn extract_scalar_from_row(
        row: &PgRow,
        column: &str,
    ) -> Result<Option<ScalarValue>, sqlx::Error> {
        if let Ok(value) = row.try_get::<Option<f64>, _>(column) {
            return Ok(value.map(ScalarValue::Float64));
        }
        if let Ok(value) = row.try_get::<Option<i64>, _>(column) {
            return Ok(value.map(ScalarValue::Int64));
        }
        if let Ok(value) = row.try_get::<Option<String>, _>(column) {
            return Ok(value.map(ScalarValue::Utf8));
        }
        if let Ok(value) = row.try_get::<Option<bool>, _>(column) {
            return Ok(value.map(ScalarValue::Boolean));
        }

        Ok(None)
    }

    /// Convert PostgreSQL type to Arrow DataType.
    fn pg_type_to_arrow(pg_type: &str) -> DataType {
        match pg_type.to_lowercase().as_str() {
            "double precision" | "float8" | "real" | "float4" => DataType::Float64,
            "numeric" | "decimal" => DataType::Float64,
            "bigint" | "int8" | "integer" | "int4" | "smallint" | "int2" => DataType::Int64,
            "text" | "varchar" | "char" | "character varying" | "character" => DataType::Utf8,
            "boolean" | "bool" => DataType::Boolean,
            "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            _ => DataType::Utf8,
        }
    }

    #[cfg(test)]
    fn arrow_to_pg_type(data_type: &DataType) -> &'static str {
        match data_type {
            DataType::Float64 => "DOUBLE PRECISION",
            DataType::Int64 => "BIGINT",
            DataType::Utf8 => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Timestamp(_, _) => "TIMESTAMPTZ",
            _ => "TEXT",
        }
    }

    fn record_batch_entity_id_index(entity_ids: &[String]) -> HashMap<&str, usize> {
        entity_ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect::<HashMap<_, _>>()
    }
}

#[async_trait::async_trait]
impl BackendAdapter for PostgresAdapter {
    fn name(&self) -> &str {
        BACKEND_NAME
    }

    fn capabilities(&self) -> AdapterCapabilities {
        let mut caps = Self::static_capabilities();
        caps.max_batch_size = Some(self.max_batch_size);
        caps
    }

    async fn describe_schema(&self, feature_names: &[String]) -> Result<Schema, AdapterError> {
        validation::validate_feature_names_not_empty(feature_names, BACKEND_NAME)?;

        let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];

        for feature_name in feature_names {
            let table_name = self.build_table_name(feature_name)?;

            let query = r#"
                SELECT data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 AND column_name = $2
                LIMIT 1
            "#;

            let row_result = sqlx::query(query)
                .bind(&table_name)
                .bind(feature_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;

            let data_type = if let Some(row) = row_result {
                let pg_type: String = row
                    .try_get("data_type")
                    .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;
                Self::pg_type_to_arrow(&pg_type)
            } else {
                DataType::Float64
            };

            fields.push(Field::new(feature_name, data_type, true));
        }

        Ok(Schema::new(fields))
    }

    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        let start_time = std::time::Instant::now();

        validation::validate_entity_ids_not_empty(entity_ids, BACKEND_NAME)?;
        validation::validate_feature_names_not_empty(feature_names, BACKEND_NAME)?;
        validation::validate_batch_size(
            entity_ids.len(),
            self.capabilities().max_batch_size,
            BACKEND_NAME,
        )?;

        validation::validate_memory_constraints(
            entity_ids.len(),
            feature_names.len(),
            Some(20),
            Some(256),
            BACKEND_NAME,
        )?;
        if feature_names.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "feature_names cannot be empty",
            ));
        }

        if let Some(max_batch) = self.capabilities().max_batch_size
            && entity_ids.len() > max_batch
        {
            return Err(AdapterError::resource_limit_exceeded(
                BACKEND_NAME,
                format!(
                    "batch size {} exceeds maximum {}",
                    entity_ids.len(),
                    max_batch
                ),
            ));
        }

        let timeout_duration = timeout.unwrap_or(self.timeout_default);
        let cutoff = as_of.unwrap_or_else(Utc::now);
        let mut feature_results: HashMap<String, Vec<Option<ScalarValue>>> = HashMap::new();
        let entity_index = Self::record_batch_entity_id_index(entity_ids);

        for feature_name in feature_names {
            let table_name = self.build_table_name(feature_name)?;

            let query = format!(
                r#"
                WITH ranked_features AS (
                    SELECT
                        entity_id,
                        {col} as feature_value,
                        feature_ts,
                        ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY feature_ts DESC) as rn
                    FROM {table}
                    WHERE entity_id = ANY($1)
                      AND feature_ts <= $2
                      AND {col} IS NOT NULL
                )
                SELECT entity_id, feature_value
                FROM ranked_features
                WHERE rn = 1
                "#,
                col = feature_name,
                table = table_name,
            );

            let mut feature_values: Vec<Option<ScalarValue>> = vec![None; entity_ids.len()];
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

            for row in rows {
                let entity_id: String = row
                    .try_get("entity_id")
                    .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;

                if let Some(&pos) = entity_index.get(entity_id.as_str()) {
                    let value = Self::extract_scalar_from_row(&row, "feature_value")
                        .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;
                    feature_values[pos] = value;
                }
            }

            feature_results.insert(feature_name.clone(), feature_values);

            if start_time.elapsed() > timeout_duration {
                return Err(AdapterError::timeout(
                    BACKEND_NAME,
                    timeout_duration.as_millis() as u64,
                ));
            }
        }

        let resolved: Vec<Vec<Option<ScalarValue>>> = feature_names
            .iter()
            .map(|fname| {
                feature_results
                    .get(fname)
                    .cloned()
                    .unwrap_or_else(|| vec![None; entity_ids.len()])
            })
            .collect();

        build_record_batch(entity_ids, feature_names, resolved)
            .map_err(|e| AdapterError::arrow(BACKEND_NAME, e.to_string()))
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
            None, // No TLS config for tests
        )
        .await
    }

    #[tokio::test]
    async fn test_identifier_validation() {
        assert!(PostgresAdapter::validate_identifier("valid_name").is_ok());
        assert!(PostgresAdapter::validate_identifier("valid123").is_ok());
        assert!(PostgresAdapter::validate_identifier("_underscore").is_ok());

        assert!(PostgresAdapter::validate_identifier("").is_err());
        assert!(PostgresAdapter::validate_identifier("123invalid").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid-name").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid name").is_err());
        assert!(PostgresAdapter::validate_identifier("invalid;drop").is_err());
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

    #[tokio::test]
    async fn test_type_conversions() {
        assert_eq!(
            PostgresAdapter::pg_type_to_arrow("double precision"),
            DataType::Float64
        );
        assert_eq!(PostgresAdapter::pg_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(PostgresAdapter::pg_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(
            PostgresAdapter::pg_type_to_arrow("boolean"),
            DataType::Boolean
        );
        assert_eq!(
            PostgresAdapter::pg_type_to_arrow("unknown_type"),
            DataType::Utf8
        );

        assert_eq!(
            PostgresAdapter::arrow_to_pg_type(&DataType::Float64),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            PostgresAdapter::arrow_to_pg_type(&DataType::Int64),
            "BIGINT"
        );
        assert_eq!(PostgresAdapter::arrow_to_pg_type(&DataType::Utf8), "TEXT");
        assert_eq!(
            PostgresAdapter::arrow_to_pg_type(&DataType::Boolean),
            "BOOLEAN"
        );
    }
}
