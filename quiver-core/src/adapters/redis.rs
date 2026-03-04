use crate::adapters::{AdapterError, BackendAdapter};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use std::sync::Arc;

#[derive(Clone)]
pub struct RedisAdapter {
    connection: redis::aio::MultiplexedConnection,
    key_template: String,
}

impl RedisAdapter {
    /// Create a new Redis adapter.
    /// 
    /// # Authentication
    /// Redis authentication is supported via the password parameter, which can be set
    /// securely using environment variables through the config system:
    /// `QUIVER_ADAPTERS__<ADAPTER_NAME>__PASSWORD=your_redis_password`
    /// 
    /// If no password is provided, no authentication is used.
    pub async fn new(url: &str, password: Option<&str>, key_template: &str) -> Result<Self, AdapterError> {
        let connection_url = if let Some(pass) = password {
            // Simple URL construction with password - assumes redis://host:port format
            if let Some(without_scheme) = url.strip_prefix("redis://") {
                format!("redis://:{pass}@{without_scheme}")
            } else if let Some(without_scheme) = url.strip_prefix("rediss://") {
                format!("rediss://:{pass}@{without_scheme}")
            } else {
                url.to_string() // Use as-is if format is unexpected
            }
        } else {
            url.to_string()
        };
        
        let client = redis::Client::open(connection_url.as_str()).map_err(|e| {
            AdapterError::internal("redis", format!("Failed to open Redis client: {}", e))
        })?;
        
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                AdapterError::internal("redis", format!("Failed to connect to Redis: {}", e))
            })?;

        Ok(Self {
            connection,
            key_template: key_template.to_string(),
        })
    }

    fn build_key(&self, feature_name: &str, entity_id: &str) -> String {
        self.key_template
            .replace("{feature}", feature_name)
            .replace("{entity}", entity_id)
    }
}

#[async_trait::async_trait]
impl BackendAdapter for RedisAdapter {
    fn name(&self) -> &str {
        "redis"
    }

    fn temporal_capability(&self) -> crate::adapters::TemporalCapability {
        crate::adapters::TemporalCapability::CurrentOnly
    }

    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        _as_of: Option<DateTime<Utc>>,
    ) -> Result<RecordBatch, AdapterError> {
        let mut conn = self.connection.clone();

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        let entity_id_array = arrow::array::StringArray::from(
            entity_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        );
        columns.push(Arc::new(entity_id_array));

        for feature in feature_names {
            // Collect all keys for this feature
            let keys: Vec<String> = entity_ids
                .iter()
                .map(|entity_id| self.build_key(feature, entity_id))
                .collect();
            
            // Use MGET to batch retrieve all values for this feature
            let raw_values: Vec<Option<String>> = conn
                .mget(&keys)
                .await
                .map_err(|e| {
                    AdapterError::internal("redis", format!("MGET for feature '{}' failed: {}", feature, e))
                })?;
            
            // Parse typed values and determine the column type for this feature
            let mut parsed_values: Vec<(Option<String>, Option<f64>, Option<i64>, Option<bool>)> = Vec::new();
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
                        // Handle legacy values without type prefixes - assume f64
                        (None, v.parse::<f64>().ok(), None, None)
                    }
                } else {
                    (None, None, None, None)
                };
                
                // Determine column type from first non-null value
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
            
            // Build the appropriate Arrow array based on determined type
            match column_type {
                "string" => {
                    let values: Vec<Option<&str>> = parsed_values
                        .iter()
                        .map(|(s, _, _, _)| s.as_ref().map(|s| s.as_str()))
                        .collect();
                    columns.push(Arc::new(arrow::array::StringArray::from(values)));
                }
                "int64" => {
                    let values: Vec<Option<i64>> = parsed_values
                        .iter()
                        .map(|(_, _, i, _)| *i)
                        .collect();
                    columns.push(Arc::new(arrow::array::Int64Array::from(values)));
                }
                "boolean" => {
                    let values: Vec<Option<bool>> = parsed_values
                        .iter()
                        .map(|(_, _, _, b)| *b)
                        .collect();
                    columns.push(Arc::new(arrow::array::BooleanArray::from(values)));
                }
                _ => {
                    // Default to float64
                    let values: Vec<Option<f64>> = parsed_values
                        .iter()
                        .map(|(_, f, _, _)| *f)
                        .collect();
                    columns.push(Arc::new(arrow::array::Float64Array::from(values)));
                }
            }
        }

        let mut fields = vec![arrow::datatypes::Field::new(
            "entity_id",
            arrow::datatypes::DataType::Utf8,
            false,
        )];
        for (i, feature) in feature_names.iter().enumerate() {
            // Determine the data type from the column that was added
            let data_type = if let Some(col) = columns.get(i + 1) {
                match col.data_type() {
                    arrow::datatypes::DataType::Float64 => arrow::datatypes::DataType::Float64,
                    arrow::datatypes::DataType::Int64 => arrow::datatypes::DataType::Int64,
                    arrow::datatypes::DataType::Utf8 => arrow::datatypes::DataType::Utf8,
                    arrow::datatypes::DataType::Boolean => arrow::datatypes::DataType::Boolean,
                    _ => arrow::datatypes::DataType::Float64, // fallback
                }
            } else {
                arrow::datatypes::DataType::Float64 // fallback
            };
            
            fields.push(arrow::datatypes::Field::new(
                feature,
                data_type,
                true,
            ));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        RecordBatch::try_new(schema, columns).map_err(|e| {
            AdapterError::internal("redis", format!("Failed to create RecordBatch: {}", e))
        })
    }

    async fn put(&self, batch: RecordBatch) -> Result<(), AdapterError> {
        let mut conn = self.connection.clone();

        let schema = batch.schema();
        let entity_id_idx = schema
            .index_of("entity_id")
            .map_err(|e| AdapterError::internal("redis", e.to_string()))?;

        let entity_ids = batch
            .column(entity_id_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| AdapterError::internal("redis", "entity_id column must be String"))?;

        // Collect all SET and DEL operations for batch processing
        let mut set_pairs: Vec<(String, String)> = Vec::new();
        let mut del_keys: Vec<String> = Vec::new();

        for i in 0..batch.num_rows() {
            let entity_id = entity_ids.value(i);
            for (j, field) in schema.fields().iter().enumerate() {
                if j == entity_id_idx {
                    continue;
                }

                let key = self.build_key(field.name(), entity_id);
                let col = batch.column(j);

                if col.is_null(i) {
                    del_keys.push(key);
                } else {
                    // Support all types that memory adapter supports
                    if let Some(fcol) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                        let value = format!("f64:{}", fcol.value(i));
                        set_pairs.push((key, value));
                    } else if let Some(icol) =
                        col.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        let value = format!("i64:{}", icol.value(i));
                        set_pairs.push((key, value));
                    } else if let Some(scol) =
                        col.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        let value = format!("str:{}", scol.value(i));
                        set_pairs.push((key, value));
                    } else if let Some(bcol) =
                        col.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    {
                        let value = format!("bool:{}", bcol.value(i));
                        set_pairs.push((key, value));
                    } else {
                        return Err(AdapterError::invalid(
                            "redis",
                            format!("Unsupported column type for field '{}'. Supported types: Float64, Int64, String, Boolean", field.name())
                        ));
                    }
                }
            }
        }

        // Execute batch operations
        if !set_pairs.is_empty() {
            // Use MSET for batch SET operations
            let _: () = conn
                .mset(&set_pairs)
                .await
                .map_err(|e| AdapterError::internal("redis", format!("Batch SET failed for {} keys: {}", set_pairs.len(), e)))?;
        }

        if !del_keys.is_empty() {
            // Use DEL for batch DELETE operations
            let _: () = conn
                .del(&del_keys)
                .await
                .map_err(|e| AdapterError::internal("redis", format!("Batch DEL failed for {} keys: {}", del_keys.len(), e)))?;
        }

        Ok(())
    }

    async fn health(&self) -> crate::adapters::HealthStatus {
        let mut conn = self.connection.clone();
        let start = std::time::Instant::now();
        let healthy = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .is_ok();
        crate::adapters::HealthStatus {
            healthy,
            backend: self.name().to_string(),
            message: if healthy {
                None
            } else {
                Some("Ping failed".to_string())
            },
            latency_ms: Some(start.elapsed().as_secs_f64() * 1000.0),
        }
    }
}
