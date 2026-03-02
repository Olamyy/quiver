use crate::adapters::{AdapterError, BackendAdapter, HealthStatus, TemporalCapability};
use arrow::array::{Array, StringArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;

pub struct MemoryAdapter {
    data: DashMap<String, RecordBatch>,
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

impl MemoryAdapter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl BackendAdapter for MemoryAdapter {
    fn name(&self) -> &str {
        "memory"
    }

    fn temporal_capability(&self) -> TemporalCapability {
        TemporalCapability::CurrentOnly
    }

    async fn get(
        &self,
        feature_view: &str,
        entity_ids: &[String],
        feature_names: &[String],
        _as_of: Option<DateTime<Utc>>,
    ) -> Result<RecordBatch, AdapterError> {
        let batch = self
            .data
            .get(feature_view)
            .ok_or_else(|| AdapterError::NotFound {
                backend: self.name().to_string(),
                entity_id: feature_view.to_string(),
            })?;

        let batch = batch.value();

        let schema = batch.schema();
        let entity_col_idx = schema.index_of("entity_id").map_err(|_| {
            AdapterError::internal(self.name(), "Missing 'entity_id' column in stored batch")
        })?;

        let entity_col = batch
            .column(entity_col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                AdapterError::internal(self.name(), "'entity_id' column is not a StringArray")
            })?;

        let mask = arrow::array::BooleanArray::from_unary(entity_col, |val| {
            entity_ids.iter().any(|id| id == val)
        });

        let filtered_batch = filter_record_batch(batch, &mask)
            .map_err(|e| AdapterError::internal(self.name(), format!("Filter failed: {}", e)))?;

        let projected_batch = filtered_batch
            .project(
                &feature_names
                    .iter()
                    .map(|f| {
                        schema.index_of(f).map_err(|_| {
                            AdapterError::internal(
                                self.name(),
                                format!("Feature '{}' not found in schema", f),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|e| {
                AdapterError::internal(self.name(), format!("Projection failed: {}", e))
            })?;

        Ok(projected_batch)
    }

    async fn put(&self, batch: RecordBatch, feature_view: &str) -> Result<(), AdapterError> {
        self.data.insert(feature_view.to_string(), batch);
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus {
            healthy: true,
            backend: self.name().to_string(),
            message: Some("Memory adapter is operational".to_string()),
            latency_ms: Some(0.0),
        }
    }
}
