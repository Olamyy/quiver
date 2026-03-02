use crate::adapters::{AdapterError, BackendAdapter};
use crate::proto::quiver::v1::FeatureViewMetadata;
use crate::registry::Registry;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::Arc;

pub struct Resolver {
    registry: Arc<dyn Registry>,
    adapters: DashMap<String, Arc<dyn BackendAdapter>>,
}

impl Resolver {
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self {
            registry,
            adapters: DashMap::new(),
        }
    }

    pub fn register_adapter(&self, name: String, adapter: Arc<dyn BackendAdapter>) {
        self.adapters.insert(name, adapter);
    }

    pub async fn resolve(
        &self,
        feature_view_name: &str,
        feature_names: &[String],
        entity_ids: &[String],
        as_of: Option<DateTime<Utc>>,
    ) -> Result<RecordBatch, ResolverError> {
        let metadata = self
            .registry
            .get_view(feature_view_name)
            .await
            .map_err(|e| ResolverError::Registry(e.to_string()))?;

        let backend_name = metadata.backend_routing.values().next().ok_or_else(|| {
            ResolverError::Internal("No backend routing defined for feature view".to_string())
        })?;

        let adapter = self
            .adapters
            .get(backend_name)
            .ok_or_else(|| ResolverError::BackendNotFound(backend_name.clone()))?;

        let batch = adapter
            .value()
            .get(feature_view_name, entity_ids, feature_names, as_of)
            .await
            .map_err(ResolverError::Adapter)?;

        Ok(batch)
    }

    pub async fn get_view_metadata(
        &self,
        name: &str,
    ) -> Result<FeatureViewMetadata, ResolverError> {
        self.registry
            .get_view(name)
            .await
            .map_err(|e| ResolverError::Registry(e.to_string()))
    }

    pub async fn get_arrow_schema(
        &self,
        name: &str,
    ) -> Result<arrow::datatypes::Schema, ResolverError> {
        let metadata = self.get_view_metadata(name).await?;

        let fields = metadata
            .columns
            .iter()
            .map(|col| {
                let dt = match col.arrow_type.as_str() {
                    "float64" => arrow::datatypes::DataType::Float64,
                    "int64" => arrow::datatypes::DataType::Int64,
                    "string" => arrow::datatypes::DataType::Utf8,
                    "bool" => arrow::datatypes::DataType::Boolean,
                    "timestamp_ns" => arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        Some("UTC".into()),
                    ),
                    _ => arrow::datatypes::DataType::Utf8,
                };
                arrow::datatypes::Field::new(&col.name, dt, col.nullable)
            })
            .collect::<Vec<_>>();

        Ok(arrow::datatypes::Schema::new(fields))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error("Registry error: {0}")]
    Registry(String),
    #[error("Adapter error: {0}")]
    Adapter(#[from] AdapterError),
    #[error("Backend not found: {0}")]
    BackendNotFound(String),
    #[error("Internal resolver error: {0}")]
    Internal(String),
}
