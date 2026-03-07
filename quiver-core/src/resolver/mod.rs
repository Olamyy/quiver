use crate::adapters::utils::parse_arrow_type_string;
use crate::adapters::{AdapterError, BackendAdapter};
use crate::config::{ColumnConfig, SourcePath};
use crate::proto::quiver::v1::FeatureViewMetadata;
use crate::registry::Registry;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct Resolver {
    registry: Arc<dyn Registry>,
    adapters: DashMap<String, Arc<dyn BackendAdapter>>,
    column_configs: DashMap<String, HashMap<String, ColumnConfig>>,
    view_source_paths: DashMap<String, SourcePath>,
}

impl Resolver {
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self {
            registry,
            adapters: DashMap::new(),
            column_configs: DashMap::new(),
            view_source_paths: DashMap::new(),
        }
    }

    pub fn register_adapter(&self, name: String, adapter: Arc<dyn BackendAdapter>) {
        self.adapters.insert(name, adapter);
    }

    pub fn register_view_columns(
        &self,
        view_name: String,
        columns: Vec<ColumnConfig>,
        view_source_path: Option<SourcePath>,
    ) {
        let column_map: HashMap<String, ColumnConfig> = columns
            .into_iter()
            .map(|col| (col.name.clone(), col))
            .collect();
        self.column_configs.insert(view_name.clone(), column_map);
        if let Some(sp) = view_source_path {
            self.view_source_paths.insert(view_name, sp);
        }
    }

    /// Convert arrow_type string to Arrow DataType using the shared utility.
    ///
    /// This function wraps the shared parse_arrow_type_string function and handles
    /// resolver-specific requirements like UTC timezone for timestamps.
    fn parse_arrow_type(
        arrow_type: &str,
        column_name: &str,
    ) -> Result<arrow::datatypes::DataType, ResolverError> {
        let normalized_type = if arrow_type == "timestamp_ns" {
            "timestamp"
        } else {
            arrow_type
        };

        let mut data_type = parse_arrow_type_string(normalized_type).map_err(|e| {
            ResolverError::Internal(format!(
                "Invalid arrow_type '{}' for column '{}': {}",
                arrow_type, column_name, e
            ))
        })?;

        if let arrow::datatypes::DataType::Timestamp(unit, _) = data_type {
            data_type = arrow::datatypes::DataType::Timestamp(unit, Some("UTC".into()));
        }

        Ok(data_type)
    }

    pub async fn resolve(
        &self,
        feature_view_name: &str,
        feature_names: &[String],
        entity_ids: &[String],
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
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

        let mut resolutions = std::collections::HashMap::new();

        for feature_name in feature_names {
            let mut expected_type = arrow::datatypes::DataType::Utf8;

            if let Some(column) = metadata
                .columns
                .iter()
                .find(|col| &col.name == feature_name)
            {
                expected_type = Self::parse_arrow_type(&column.arrow_type, &column.name)?;
            }

            let mut resolved_path = None;
            if let Some(cols) = self.column_configs.get(feature_view_name)
                && let Some(col) = cols.get(feature_name)
            {
                resolved_path = col.source_path.clone();
            }

            if resolved_path.is_none()
                && let Some(sp) = self.view_source_paths.get(feature_view_name)
            {
                resolved_path = Some(sp.clone());
            }

            resolutions.insert(
                feature_name.clone(),
                crate::adapters::FeatureResolution {
                    expected_type,
                    source_path: resolved_path,
                },
            );
        }

        let batch = adapter
            .value()
            .get_with_resolutions(
                entity_ids,
                feature_names,
                &metadata.entity_key,
                &resolutions,
                as_of,
                timeout,
            )
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
            .map(|col| -> Result<arrow::datatypes::Field, ResolverError> {
                let dt = Self::parse_arrow_type(&col.arrow_type, &col.name)?;
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
