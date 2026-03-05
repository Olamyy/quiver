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

        let backend_name = feature_names
            .iter()
            .map(|fname| {
                metadata.backend_routing.get(fname).ok_or_else(|| {
                    ResolverError::Internal(format!(
                        "No backend routing for feature '{}' in view '{}'",
                        fname, feature_view_name
                    ))
                })
            })
            .collect::<Result<std::collections::HashSet<_>, _>>()?
            .into_iter()
            .next()
            .ok_or_else(|| ResolverError::Internal("Empty feature list".to_string()))?;

        let adapter = self
            .adapters
            .get(backend_name)
            .ok_or_else(|| ResolverError::BackendNotFound(backend_name.clone()))?;

        let batch = adapter
            .value()
            .get(entity_ids, feature_names, as_of, None)
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
                    other => {
                        return Err(ResolverError::Internal(format!(
                            "Unknown arrow_type '{}' for column '{}'",
                            other, col.name
                        )));
                    }
                };
                Ok(arrow::datatypes::Field::new(&col.name, dt, col.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(arrow::datatypes::Schema::new(fields))
    }

    pub async fn list_views(&self) -> Result<Vec<String>, ResolverError> {
        self.registry
            .list_views()
            .await
            .map_err(|e| ResolverError::Registry(e.to_string()))
    }

    pub async fn put(&self, backend_name: &str, batch: RecordBatch) -> Result<(), ResolverError> {
        let adapter = self
            .adapters
            .get(backend_name)
            .ok_or_else(|| ResolverError::BackendNotFound(backend_name.to_string()))?;

        use crate::adapters::PutOptions;

        adapter
            .value()
            .put(batch, &PutOptions::default())
            .await
            .map_err(ResolverError::Adapter)
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
