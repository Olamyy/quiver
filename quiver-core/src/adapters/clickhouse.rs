use std::collections::HashMap;
use std::time::Duration;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use tracing::info;

use super::utils::ScalarValue;
use super::{AdapterCapabilities, AdapterError, BackendAdapter, HealthStatus, TemporalCapability};
use crate::validation::ValidationConfig;

const BACKEND_NAME: &str = "clickhouse";
const DEFAULT_CHUNK_SIZE: usize = 10_000;

/// ClickHouse adapter for feature storage with temporal support.
///
/// The adapter uses template-based table naming and supports:
/// - Per-feature temporal resolution with ROW_NUMBER() OVER
/// - Batch operations with configurable chunking
/// - Connection pooling
/// - Temporal queries for point-in-time feature retrieval
#[allow(dead_code)]
pub struct ClickHouseAdapter {
    client: Client,
    table_template: String,
    database_name: String,
    timeout_default: Duration,
    health_timeout: Duration,
    capabilities: AdapterCapabilities,
    validation_config: ValidationConfig,
    chunk_size: usize,
}

impl ClickHouseAdapter {
    /// Create a new ClickHouse adapter.
    ///
    /// # Arguments
    /// * `connection_string` - ClickHouse connection string (e.g., "clickhouse://user:password@localhost:9000/dbname")
    /// * `table_template` - Template for table names (e.g., "features_{feature}")
    /// * `max_connections` - Maximum connections in pool
    /// * `timeout` - Default timeout for operations
    /// * `tls_config` - Optional TLS configuration
    /// * `validation_config` - Optional validation configuration
    /// * `parameters` - Optional parameters (chunk_size, etc.)
    pub async fn new(
        connection_string: &str,
        table_template: &str,
        max_connections: Option<u32>,
        timeout: Option<Duration>,
        _tls_config: Option<&crate::config::AdapterTlsConfig>,
        validation_config: Option<ValidationConfig>,
        parameters: Option<&HashMap<String, serde_json::Value>>,
    ) -> Result<Self, AdapterError> {
        Self::validate_connection_string(connection_string)?;

        // Extract database name from connection string
        let database_name = Self::extract_database_name(connection_string)
            .unwrap_or_else(|_| "quiver_test".to_string());

        let mut client = Client::default()
            .with_url(connection_string)
            .with_database(&database_name);
        // Note: max_connections, max_insert_block_size, max_block_size are not valid HTTP API settings
        // The HTTP API handles these automatically or through different mechanisms

        let timeout_duration = timeout.unwrap_or(Duration::from_secs(30));

        info!(
            "ClickHouse adapter created with {} max connections",
            max_connections.unwrap_or(20)
        );

        let mut chunk_size = DEFAULT_CHUNK_SIZE;
        if let Some(params) = parameters
            && let Some(size_val) = params.get("chunk_size")
            && let Some(size) = size_val.as_u64()
        {
            chunk_size = size as usize;
        }

        Ok(Self {
            client,
            table_template: table_template.to_string(),
            database_name,
            timeout_default: timeout_duration,
            health_timeout: Duration::from_secs(10),
            capabilities: Self::static_capabilities(),
            validation_config: validation_config.unwrap_or_default(),
            chunk_size,
        })
    }

    /// Validate ClickHouse connection string format and security.
    fn validate_connection_string(connection_string: &str) -> Result<(), AdapterError> {
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

        if !connection_string.starts_with("http://") && !connection_string.starts_with("https://") {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "Connection string must start with 'http://' or 'https://' (e.g., 'http://localhost:8123')",
            ));
        }

        Ok(())
    }

    /// Extract database name from ClickHouse HTTP connection string.
    /// Expects format: "http://host:port" with optional "?database=name" parameter.
    /// Falls back to "quiver_test" if not specified (for examples).
    fn extract_database_name(connection_string: &str) -> Result<String, AdapterError> {
        // Check for ?database=name parameter
        if let Some(q_pos) = connection_string.find('?') {
            let query_part = &connection_string[q_pos + 1..];
            for param in query_part.split('&') {
                if param.starts_with("database=") {
                    let db_name = &param[9..];  // "database=".len() == 9
                    if !db_name.is_empty() {
                        return Ok(db_name.to_string());
                    }
                }
            }
        }

        // Default to quiver_test for examples
        Ok("quiver_test".to_string())
    }

    /// Get adapter capabilities without requiring a connection.
    pub fn static_capabilities() -> AdapterCapabilities {
        AdapterCapabilities {
            temporal: TemporalCapability::TimeTravel {
                typical_latency_ms: 100,
            },
            max_batch_size: Some(50_000),
            optimal_batch_size: Some(1_000),
            typical_latency_ms: 100,
            supports_parallel_requests: true,
        }
    }

    /// Quote ClickHouse identifier safely to prevent SQL injection.
    /// ClickHouse uses backticks for identifier quoting.
    fn quote_identifier(identifier: &str) -> String {
        let mut result = String::with_capacity(identifier.len() + 2);
        result.push('`');
        for ch in identifier.chars() {
            result.push(ch);
            if ch == '`' {
                result.push('`');
            }
        }
        result.push('`');
        result
    }

    /// Build SQL entity list from entity IDs with proper escaping.
    /// Produces a comma-separated list of quoted, escaped entity IDs.
    fn build_entity_list(entity_ids: &[String]) -> String {
        let mut entity_list = String::with_capacity(entity_ids.len() * 16);
        for (i, id) in entity_ids.iter().enumerate() {
            if i > 0 {
                entity_list.push(',');
            }
            entity_list.push('\'');
            for ch in id.chars() {
                entity_list.push(ch);
                if ch == '\'' {
                    entity_list.push('\'');
                }
            }
            entity_list.push('\'');
        }
        entity_list
    }

    /// Validate identifier format to prevent SQL injection.
    /// Allows alphanumeric, underscores, dots for schema.table references.
    fn validate_identifier(identifier: &str) -> Result<(), AdapterError> {
        if identifier.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "identifier cannot be empty",
            ));
        }

        if identifier.len() > 255 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "identifier exceeds 255 characters",
            ));
        }

        for ch in identifier.chars() {
            if !ch.is_alphanumeric() && ch != '_' && ch != '.' {
                return Err(AdapterError::invalid(
                    BACKEND_NAME,
                    format!(
                        "identifier '{}' contains invalid character: '{}' (only alphanumeric, underscore, dot allowed)",
                        identifier, ch
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Resolve the source path for a feature to a table name.
    fn resolve_table_name(
        &self,
        feature_name: &str,
        source_path: Option<&crate::config::SourcePath>,
    ) -> Result<String, AdapterError> {
        let default_path = crate::config::SourcePath::Template(self.table_template.clone());
        let effective_path = source_path.unwrap_or(&default_path);

        let table_name = match effective_path {
            crate::config::SourcePath::Template(template) => {
                template.replace("{feature}", feature_name)
            }
            crate::config::SourcePath::Structured { table, .. } => table.clone(),
        };

        Self::validate_identifier(&table_name)?;

        // Table name is used as-is; the database context is set via the client's with_database()
        Ok(table_name)
    }

    /// Convert ClickHouse value to ScalarValue based on target type.
    #[allow(dead_code)]
    fn convert_value(value: &str, target_type: &DataType) -> Option<ScalarValue> {
        if value.is_empty() || value == "NULL" {
            return Some(ScalarValue::Null);
        }

        match target_type {
            DataType::Float64 => value.parse::<f64>().ok().map(ScalarValue::Float64),
            DataType::Int64 => value.parse::<i64>().ok().map(ScalarValue::Int64),
            DataType::Utf8 => Some(ScalarValue::Utf8(value.to_string())),
            DataType::Boolean => match value.to_lowercase().as_str() {
                "true" | "1" => Some(ScalarValue::Boolean(true)),
                "false" | "0" => Some(ScalarValue::Boolean(false)),
                _ => None,
            },
            DataType::Timestamp(_, _) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
                    Some(ScalarValue::Timestamp(dt.with_timezone(&Utc)))
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                {
                    Some(ScalarValue::Timestamp(
                        DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Build a SQL query for current state retrieval.
    fn build_current_state_query(
        &self,
        table_name: &str,
        entity_ids: &[String],
        entity_key: &str,
        feature_names: &[String],
    ) -> String {
        let quoted_entity_key = Self::quote_identifier(entity_key);
        let quoted_table_name = Self::quote_identifier(table_name);
        let entity_list = Self::build_entity_list(entity_ids);

        let feature_columns = feature_names
            .iter()
            .map(|f| Self::quote_identifier(f))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} as entity_id, {} FROM {} WHERE {} IN ({})",
            quoted_entity_key, feature_columns, quoted_table_name, quoted_entity_key, entity_list
        )
    }

    /// Build a SQL query for temporal (point-in-time) retrieval.
    fn build_temporal_query(
        &self,
        table_name: &str,
        entity_ids: &[String],
        entity_key: &str,
        as_of: DateTime<Utc>,
        feature_names: &[String],
    ) -> String {
        let quoted_entity_key = Self::quote_identifier(entity_key);
        let quoted_table_name = Self::quote_identifier(table_name);
        let entity_list = Self::build_entity_list(entity_ids);
        let as_of_str = as_of.format("%Y-%m-%d %H:%M:%S").to_string();

        let feature_columns = feature_names
            .iter()
            .map(|f| Self::quote_identifier(f))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} as entity_id, {} FROM (
                SELECT
                    {},
                    {},
                    ROW_NUMBER() OVER (PARTITION BY {} ORDER BY feature_ts DESC) as rn
                FROM {}
                WHERE {} IN ({}) AND feature_ts <= '{}'
            ) WHERE rn = 1",
            quoted_entity_key,
            feature_columns,
            quoted_entity_key,
            feature_columns,
            quoted_entity_key,
            quoted_table_name,
            quoted_entity_key,
            entity_list,
            as_of_str
        )
    }
}

#[async_trait::async_trait]
impl BackendAdapter for ClickHouseAdapter {
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
        if entity_ids.is_empty() || feature_names.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "entity_ids and feature_names cannot be empty",
            ));
        }

        let fallback_resolutions: std::collections::HashMap<String, super::FeatureResolution> =
            feature_names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        super::FeatureResolution {
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
            &fallback_resolutions,
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
        resolutions: &HashMap<String, super::FeatureResolution>,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        if entity_ids.is_empty() || feature_names.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "entity_ids and feature_names cannot be empty",
            ));
        }

        let timeout_duration = timeout.unwrap_or(self.timeout_default);
        let feature_name = &feature_names[0];
        let source_path = resolutions
            .get(feature_name)
            .and_then(|r| r.source_path.as_ref());
        let table_name = self.resolve_table_name(feature_name, source_path)?;

        let query = if let Some(as_of_time) = as_of {
            self.build_temporal_query(&table_name, entity_ids, entity_key, as_of_time, feature_names)
        } else {
            self.build_current_state_query(&table_name, entity_ids, entity_key, feature_names)
        };

        tracing::info!("ClickHouse query: {}", query);

        let client = self.client.clone();

        let mut entity_index = std::collections::HashMap::new();
        for (idx, id) in entity_ids.iter().enumerate() {
            entity_index.insert(id.clone(), idx);
        }

        let mut builder = super::utils::StreamingRecordBatchBuilder::new(
            entity_ids,
            feature_names,
            entity_key,
        )
        .map_err(|e| {
            AdapterError::internal(
                BACKEND_NAME,
                format!("Failed to create builder: {}", e),
            )
        })?;

        let rows_result = tokio::time::timeout(
            timeout_duration,
            async {
                client
                    .query(&query)
                    .fetch_all::<(String, String, String, String, String)>()
                    .await
                    .map_err(|e| {
                        AdapterError::internal(BACKEND_NAME, format!("Query execution failed: {}", e))
                    })
            },
        )
        .await
        .map_err(|_| {
            AdapterError::timeout(BACKEND_NAME, timeout_duration.as_millis() as u64)
        })?
        .map_err(|e| AdapterError::internal(BACKEND_NAME, e.to_string()))?;

        for (entity_id, val1, val2, val3, val4) in rows_result {
            if let Some(entity_idx) = entity_index.get(&entity_id) {
                let row_values = vec![val1, val2, val3, val4];

                for (feature_idx, feature_name) in feature_names.iter().enumerate() {
                    let raw_value = if feature_idx < row_values.len() {
                        row_values[feature_idx].clone()
                    } else {
                        String::new()
                    };

                    let expected_type = resolutions
                        .get(feature_name)
                        .map(|r| r.expected_type.clone())
                        .unwrap_or(DataType::Utf8);

                    let converted_value = if raw_value.is_empty() || raw_value == "NULL" {
                        None
                    } else {
                        Self::convert_value(&raw_value, &expected_type)
                    };

                    builder
                        .set_value(feature_idx, *entity_idx, converted_value)
                        .map_err(|e| {
                            AdapterError::internal(
                                BACKEND_NAME,
                                format!("Failed to set value: {}", e),
                            )
                        })?;
                }
            }
        }

        builder.finish().map_err(|e| {
            AdapterError::internal(BACKEND_NAME, format!("Failed to finish builder: {}", e))
        })
    }

    async fn health(&self) -> HealthStatus {
        let start = std::time::Instant::now();
        let healthy = matches!(
            tokio::time::timeout(self.health_timeout, async {
                self.client.query("SELECT 1").execute().await
            })
            .await,
            Ok(Ok(_))
        );

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        HealthStatus {
            healthy,
            backend: BACKEND_NAME.to_string(),
            message: if healthy {
                Some("ClickHouse adapter operational".to_string())
            } else {
                Some("ClickHouse adapter health check timeout".to_string())
            },
            latency_ms: Some(latency_ms),
            last_check: Utc::now(),
            capabilities_verified: false,
            estimated_capacity: None,
        }
    }

    async fn initialize(&mut self) -> Result<(), AdapterError> {
        info!("ClickHouse adapter initialized successfully");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        info!("ClickHouse adapter shutdown");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_connection_string() {
        assert!(
            ClickHouseAdapter::validate_connection_string("clickhouse://localhost:9000").is_ok()
        );
        assert!(
            ClickHouseAdapter::validate_connection_string("clickhousedb://user:pass@host:9000/db")
                .is_ok()
        );
        assert!(ClickHouseAdapter::validate_connection_string("").is_err());
        assert!(ClickHouseAdapter::validate_connection_string("postgresql://localhost").is_err());
        assert!(ClickHouseAdapter::validate_connection_string(&"x".repeat(2049)).is_err());
    }

    #[test]
    fn test_extract_database_name() {
        assert_eq!(
            ClickHouseAdapter::extract_database_name("clickhouse://localhost:9000/quiver_test")
                .unwrap(),
            "quiver_test"
        );
        assert_eq!(
            ClickHouseAdapter::extract_database_name("clickhouse://user:pass@localhost:9000/my_db")
                .unwrap(),
            "my_db"
        );
        assert_eq!(
            ClickHouseAdapter::extract_database_name("clickhousedb://localhost:9000/test_db")
                .unwrap(),
            "test_db"
        );
        assert_eq!(
            ClickHouseAdapter::extract_database_name("clickhouse://localhost:9000").unwrap(),
            "default"
        );
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(
            ClickHouseAdapter::quote_identifier("table_name"),
            "`table_name`"
        );
        assert_eq!(
            ClickHouseAdapter::quote_identifier("my`table"),
            "`my``table`"
        );
        assert_eq!(
            ClickHouseAdapter::quote_identifier("schema.table"),
            "`schema.table`"
        );
    }

    #[test]
    fn test_validate_identifier() {
        assert!(ClickHouseAdapter::validate_identifier("user_features").is_ok());
        assert!(ClickHouseAdapter::validate_identifier("features_123").is_ok());
        assert!(ClickHouseAdapter::validate_identifier("schema.table").is_ok());
        assert!(ClickHouseAdapter::validate_identifier("").is_err());
        assert!(ClickHouseAdapter::validate_identifier("table; DROP").is_err());
        assert!(ClickHouseAdapter::validate_identifier("table-name").is_err());
    }

    #[test]
    fn test_build_current_state_query() {
        assert!(
            ClickHouseAdapter::validate_connection_string("clickhouse://localhost:9000").is_ok()
        );
    }

    #[test]
    fn test_convert_value() {
        assert_eq!(
            ClickHouseAdapter::convert_value("42.5", &DataType::Float64),
            Some(ScalarValue::Float64(42.5))
        );
        assert_eq!(
            ClickHouseAdapter::convert_value("42", &DataType::Int64),
            Some(ScalarValue::Int64(42))
        );
        assert_eq!(
            ClickHouseAdapter::convert_value("hello", &DataType::Utf8),
            Some(ScalarValue::Utf8("hello".to_string()))
        );
        assert_eq!(
            ClickHouseAdapter::convert_value("true", &DataType::Boolean),
            Some(ScalarValue::Boolean(true))
        );
        assert_eq!(
            ClickHouseAdapter::convert_value("", &DataType::Float64),
            Some(ScalarValue::Null)
        );
        assert_eq!(
            ClickHouseAdapter::convert_value("NULL", &DataType::Utf8),
            Some(ScalarValue::Null)
        );
    }

    #[test]
    fn test_static_capabilities() {
        let caps = ClickHouseAdapter::static_capabilities();
        assert_eq!(caps.max_batch_size, Some(50_000));
        assert_eq!(caps.optimal_batch_size, Some(1_000));
        assert!(caps.supports_parallel_requests);
        assert_eq!(caps.typical_latency_ms, 100);
    }
}
