use crate::adapters::{AdapterError, BackendAdapter};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;

#[derive(Clone)]
pub struct RedisAdapter {
    connection: redis::aio::MultiplexedConnection,
}

impl RedisAdapter {
    pub async fn new(url: &str) -> Result<Self, AdapterError> {
        let client = redis::Client::open(url).map_err(|e| {
            AdapterError::internal("redis", format!("Failed to open Redis client: {}", e))
        })?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                AdapterError::internal("redis", format!("Failed to connect to Redis: {}", e))
            })?;

        Ok(Self { connection })
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

    async fn get(
        &self,
        _entity_ids: &[String],
        _feature_names: &[String],
        _as_of: Option<DateTime<Utc>>,
    ) -> Result<RecordBatch, AdapterError> {
        Err(AdapterError::internal(
            "redis",
            "Redis get not fully implemented yet",
        ))
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

        for i in 0..batch.num_rows() {
            let entity_id = entity_ids.value(i);
            for (j, field) in schema.fields().iter().enumerate() {
                if j == entity_id_idx {
                    continue;
                }

                let key = format!("quiver:f:{}:e:{}", field.name(), entity_id);
                let col = batch.column(j);

                if col.is_null(i) {
                    let _: () = conn
                        .del(&key)
                        .await
                        .map_err(|e| AdapterError::internal("redis", e.to_string()))?;
                } else {
                    if let Some(fcol) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                        let _: () = conn
                            .set(&key, fcol.value(i))
                            .await
                            .map_err(|e| AdapterError::internal("redis", e.to_string()))?;
                    } else if let Some(icol) =
                        col.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        let _: () = conn
                            .set(&key, icol.value(i))
                            .await
                            .map_err(|e| AdapterError::internal("redis", e.to_string()))?;
                    } else if let Some(scol) =
                        col.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        let _: () = conn
                            .set(&key, scol.value(i))
                            .await
                            .map_err(|e| AdapterError::internal("redis", e.to_string()))?;
                    }
                }
            }
        }

        Ok(())
    }
}
