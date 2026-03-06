use crate::adapters::{
    AdapterCapabilities, AdapterError, BackendAdapter, HealthStatus, TemporalCapability,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisAdapter {
    connection: redis::aio::MultiplexedConnection,
    key_template: String,
}

impl RedisAdapter {
    /// Create a new Redis adapter.
    ///
    /// # Security
    /// This method handles Redis authentication securely by using Redis AUTH command
    /// instead of embedding passwords in connection URLs, which prevents password
    /// exposure in logs or process lists.
    ///
    /// # Authentication
    /// Redis authentication is supported via the password parameter, which can be set
    /// securely using environment variables through the config system:
    /// `QUIVER_ADAPTERS__<ADAPTER_NAME>__PASSWORD=your_redis_password`
    ///
    /// If no password is provided, no authentication is used.
    ///
    /// # TLS Configuration
    /// TLS support is determined by:
    /// 1. Presence of tls_config parameter (explicit TLS configuration)
    /// 2. Connection URL protocol (rediss:// enables TLS, redis:// disables TLS)
    /// 3. Query parameters in URL (?tls_verify=false overrides certificate verification)
    pub async fn new(
        url: &str,
        password: Option<&str>,
        key_template: &str,
        tls_config: Option<&crate::config::AdapterTlsConfig>,
    ) -> Result<Self, AdapterError> {
        if url.contains('@') && (url.starts_with("redis://") || url.starts_with("rediss://")) {
            return Err(AdapterError::invalid(
                "redis",
                "Connection URL should not contain embedded credentials. Use password parameter instead.",
            ));
        }

        // Determine TLS settings
        let tls_enabled = if let Some(tls_cfg) = tls_config {
            // Explicit TLS config provided - TLS is enabled
            tls_cfg.is_tls_enabled(url)
        } else {
            // No TLS config - check URL protocol
            crate::config::is_tls_enabled_by_protocol(url)
        };

        // Convert URL to appropriate protocol based on TLS settings
        let effective_url = if tls_enabled && url.starts_with("redis://") {
            // Convert redis:// to rediss:// when TLS is explicitly enabled
            url.replacen("redis://", "rediss://", 1)
        } else if !tls_enabled && url.starts_with("rediss://") {
            // Convert rediss:// to redis:// when TLS is explicitly disabled
            url.replacen("rediss://", "redis://", 1)
        } else {
            url.to_string()
        };

        let client = redis::Client::open(effective_url.as_str()).map_err(|e| {
            AdapterError::internal("redis", format!("Failed to open Redis client: {}", e))
        })?;

        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                AdapterError::internal("redis", format!("Failed to connect to Redis: {}", e))
            })?;

        if let Some(pass) = password {
            let _: () = redis::cmd("AUTH")
                .arg(pass)
                .query_async(&mut connection)
                .await
                .map_err(|e| {
                    AdapterError::internal("redis", format!("Redis authentication failed: {}", e))
                })?;
        }

        Ok(Self {
            connection,
            key_template: key_template.to_string(),
        })
    }

    /// Validate a Redis key component to prevent injection attacks.
    ///
    /// Redis keys should not contain spaces, control characters, or special Redis characters
    /// that could be used for command injection or cause parsing issues.
    fn validate_key_component(component: &str, component_name: &str) -> Result<(), AdapterError> {
        if component.is_empty() {
            return Err(AdapterError::invalid(
                "redis",
                format!("{} cannot be empty", component_name),
            ));
        }

        if component.len() > 250 {
            return Err(AdapterError::invalid(
                "redis",
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
                "redis",
                format!(
                    "{} contains invalid characters. Only alphanumeric, hyphens, underscores, periods, and colons are allowed",
                    component_name
                ),
            ));
        }

        Ok(())
    }

    fn build_key(&self, feature_name: &str, entity_id: &str) -> Result<String, AdapterError> {
        Self::validate_key_component(feature_name, "feature_name")?;
        Self::validate_key_component(entity_id, "entity_id")?;

        Ok(self
            .key_template
            .replace("{feature}", feature_name)
            .replace("{entity}", entity_id))
    }

    /// Safely scan for keys matching a pattern using SCAN instead of KEYS.
    ///
    /// SCAN is safer than KEYS as it doesn't block the Redis server and limits
    /// the number of keys returned to prevent memory exhaustion.
    async fn scan_keys_safely(&self, pattern: &str) -> Result<Vec<String>, AdapterError> {
        let mut conn = self.connection.clone();
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        let max_keys = 100;
        let scan_count = 10;

        loop {
            let (new_cursor, batch_keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(scan_count)
                .query_async(&mut conn)
                .await
                .map_err(|e| {
                    AdapterError::internal("redis", format!("SCAN command failed: {}", e))
                })?;

            keys.extend(batch_keys);

            if new_cursor == 0 || keys.len() >= max_keys {
                break;
            }

            cursor = new_cursor;
        }

        keys.truncate(max_keys);
        Ok(keys)
    }
}

#[async_trait::async_trait]
impl BackendAdapter for RedisAdapter {
    fn name(&self) -> &str {
        "redis"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            temporal: TemporalCapability::CurrentOnly {
                typical_latency_ms: 5,
            },
            max_batch_size: Some(5_000),
            optimal_batch_size: Some(100),
            typical_latency_ms: 5,
            supports_parallel_requests: true,
        }
    }

    async fn describe_schema(&self, feature_names: &[String]) -> Result<Schema, AdapterError> {
        if feature_names.is_empty() {
            return Err(AdapterError::invalid(
                "redis",
                "feature_names cannot be empty",
            ));
        }

        let mut conn = self.connection.clone();
        let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];

        for feature_name in feature_names {
            let sample_key = self.build_key(feature_name, "*")?;
            let keys = self.scan_keys_safely(&sample_key).await?;

            let data_type = if let Some(key) = keys.first() {
                let value: Option<String> = conn.get(key).await.map_err(|e| {
                    AdapterError::internal("redis", format!("Failed to sample value: {}", e))
                })?;

                if let Some(v) = value {
                    if v.starts_with("f64:") {
                        DataType::Float64
                    } else if v.starts_with("i64:") {
                        DataType::Int64
                    } else if v.starts_with("str:") {
                        DataType::Utf8
                    } else if v.starts_with("bool:") {
                        DataType::Boolean
                    } else {
                        DataType::Float64
                    }
                } else {
                    DataType::Float64
                }
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
        _as_of: Option<DateTime<Utc>>,
        _timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        let _start_time = std::time::Instant::now();

        let capabilities = self.capabilities();
        if let Some(max_batch) = capabilities.max_batch_size
            && entity_ids.len() > max_batch
        {
            return Err(AdapterError::resource_limit_exceeded(
                "redis",
                format!(
                    "batch size {} exceeds maximum {}",
                    entity_ids.len(),
                    max_batch
                ),
            ));
        }

        crate::adapters::utils::validation::validate_memory_constraints(
            entity_ids.len(),
            feature_names.len(),
            Some(15),
            Some(128),
            "redis",
        )?;
        let mut conn = self.connection.clone();

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        let entity_id_array = arrow::array::StringArray::from(
            entity_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        );
        columns.push(Arc::new(entity_id_array));

        for feature in feature_names {
            let keys: Result<Vec<String>, AdapterError> = entity_ids
                .iter()
                .map(|entity_id| self.build_key(feature, entity_id))
                .collect();
            let keys = keys?;

            let raw_values: Vec<Option<String>> = conn.mget(&keys).await.map_err(|e| {
                AdapterError::internal(
                    "redis",
                    format!("MGET for feature '{}' failed: {}", feature, e),
                )
            })?;

            let mut parsed_values: Vec<(Option<String>, Option<f64>, Option<i64>, Option<bool>)> =
                Vec::new();
            let mut column_type = "null";

            for value in &raw_values {
                let (str_val, float_val, int_val, bool_val) = if let Some(v) = value {
                    if let Some(stripped) = v.strip_prefix("f64:") {
                        (None, stripped.parse::<f64>().ok(), None, None)
                    } else if let Some(stripped) = v.strip_prefix("i64:") {
                        (None, None, stripped.parse::<i64>().ok(), None)
                    } else if let Some(stripped) = v.strip_prefix("str:") {
                        (Some(stripped.to_string()), None, None, None)
                    } else if let Some(stripped) = v.strip_prefix("bool:") {
                        (None, None, None, stripped.parse::<bool>().ok())
                    } else {
                        (None, v.parse::<f64>().ok(), None, None)
                    }
                } else {
                    (None, None, None, None)
                };

                if column_type == "null" {
                    if str_val.is_some() {
                        column_type = "string";
                    } else if int_val.is_some() {
                        column_type = "int64";
                    } else if bool_val.is_some() {
                        column_type = "boolean";
                    } else if float_val.is_some() {
                        column_type = "float64";
                    }
                }

                parsed_values.push((str_val, float_val, int_val, bool_val));
            }

            match column_type {
                "string" => {
                    let values: Vec<Option<&str>> = parsed_values
                        .iter()
                        .map(|(s, _, _, _)| s.as_ref().map(|s| s.as_str()))
                        .collect();
                    columns.push(Arc::new(arrow::array::StringArray::from(values)));
                }
                "int64" => {
                    let values: Vec<Option<i64>> =
                        parsed_values.iter().map(|(_, _, i, _)| *i).collect();
                    columns.push(Arc::new(arrow::array::Int64Array::from(values)));
                }
                "boolean" => {
                    let values: Vec<Option<bool>> =
                        parsed_values.iter().map(|(_, _, _, b)| *b).collect();
                    columns.push(Arc::new(arrow::array::BooleanArray::from(values)));
                }
                _ => {
                    let values: Vec<Option<f64>> =
                        parsed_values.iter().map(|(_, f, _, _)| *f).collect();
                    columns.push(Arc::new(arrow::array::Float64Array::from(values)));
                }
            }
        }

        let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];
        for (i, feature) in feature_names.iter().enumerate() {
            let data_type = if let Some(col) = columns.get(i + 1) {
                match col.data_type() {
                    DataType::Float64 => DataType::Float64,
                    DataType::Int64 => DataType::Int64,
                    DataType::Utf8 => DataType::Utf8,
                    DataType::Boolean => DataType::Boolean,
                    _ => DataType::Float64,
                }
            } else {
                DataType::Float64
            };

            fields.push(Field::new(feature, data_type, true));
        }
        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, columns).map_err(|e| {
            AdapterError::internal("redis", format!("Failed to create RecordBatch: {}", e))
        })
    }

    async fn health(&self) -> HealthStatus {
        let mut conn = self.connection.clone();
        let start = std::time::Instant::now();
        let healthy = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .is_ok();
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
}
