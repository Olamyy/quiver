use crate::adapters::utils::parse_arrow_type_string;
use crate::adapters::{AdapterError, BackendAdapter, FeatureResolution};
use crate::config::{ColumnConfig, SourcePath};
use crate::fanout::metrics::{Backend as MetricsBackend, FanoutLatencies, Phase, Timer};
use crate::proto::quiver::v1::FeatureViewMetadata;
use crate::registry::Registry;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Strategy for handling null values in fanout merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialFailureStrategy {
    /// Fill missing entities with nulls (default behavior).
    NullFill,
    /// Treat missing entities as an error condition.
    Error,
    /// Use previous known value if missing (deferred to Phase 3).
    ForwardFill,
}

/// Result from a single backend during fanout execution.
///
/// Tracks the RecordBatch output from one backend along with metadata
/// about the execution (latency, status, errors).
#[derive(Debug, Clone)]
pub struct FanoutResult {
    /// Name of the backend that produced this result.
    pub backend: String,
    /// The RecordBatch containing feature data from this backend.
    pub batch: RecordBatch,
    /// Execution latency in milliseconds.
    pub latency_ms: u64,
    /// Status: "success", "timeout", "partial_failure", "error", etc.
    pub status: String,
    /// If a fallback backend was used, its name.
    pub fallback_used: Option<String>,
    /// Error message if status indicates failure.
    pub error: Option<String>,
}

/// Configuration for fanout execution behavior.
///
/// Controls whether multi-backend feature requests are executed in parallel
/// and how partial failures are handled.
#[derive(Debug, Clone)]
pub struct FanoutConfig {
    /// Enable parallel multi-backend feature fetching.
    pub enabled: bool,
    /// Maximum number of backends to dispatch concurrently.
    pub max_concurrent_backends: usize,
    /// Strategy for handling missing entities from backends.
    pub partial_failure_strategy: PartialFailureStrategy,
}

/// Resolver for mapping feature requests to backend adapters.
///
/// Handles feature view metadata lookup, backend routing, and adapter selection.
/// In Phase 2, this will support parallel multi-backend execution via fanout.
pub struct Resolver {
    registry: Arc<dyn Registry>,
    adapters: DashMap<String, Arc<dyn BackendAdapter>>,
    column_configs: DashMap<String, HashMap<String, ColumnConfig>>,
    view_source_paths: DashMap<String, SourcePath>,
    fanout_config: FanoutConfig,
    downtime_strategy: crate::config::DowntimeStrategy,
}

impl Resolver {
    /// Create a new Resolver with default fanout configuration.
    ///
    /// Default config: fanout enabled, 10 concurrent backends, NullFill strategy, Fail downtime strategy.
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self {
            registry,
            adapters: DashMap::new(),
            column_configs: DashMap::new(),
            view_source_paths: DashMap::new(),
            fanout_config: FanoutConfig {
                enabled: true,
                max_concurrent_backends: 10,
                partial_failure_strategy: PartialFailureStrategy::NullFill,
            },
            downtime_strategy: crate::config::DowntimeStrategy::Fail,
        }
    }

    /// Create a new Resolver with custom fanout configuration.
    pub fn with_fanout_config(registry: Arc<dyn Registry>, fanout_config: FanoutConfig) -> Self {
        Self {
            registry,
            adapters: DashMap::new(),
            column_configs: DashMap::new(),
            view_source_paths: DashMap::new(),
            fanout_config,
            downtime_strategy: crate::config::DowntimeStrategy::Fail,
        }
    }

    /// Create a new Resolver with downtime strategy.
    pub fn with_downtime_strategy(
        registry: Arc<dyn Registry>,
        downtime_strategy: crate::config::DowntimeStrategy,
    ) -> Self {
        Self {
            registry,
            adapters: DashMap::new(),
            column_configs: DashMap::new(),
            view_source_paths: DashMap::new(),
            fanout_config: FanoutConfig {
                enabled: true,
                max_concurrent_backends: 10,
                partial_failure_strategy: PartialFailureStrategy::NullFill,
            },
            downtime_strategy,
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

    /// Get the entity key for a feature view.
    pub async fn get_entity_key(&self, view_name: &str) -> Result<String, ResolverError> {
        match self.registry.get_view(view_name).await {
            Ok(view_info) => Ok(view_info.entity_key),
            Err(_) => Ok("entity_id".to_string()), // Fallback default
        }
    }

    /// Get the fanout configuration.
    pub fn fanout_config(&self) -> &FanoutConfig {
        &self.fanout_config
    }

    /// Group features by their backend routing.
    ///
    /// Takes a list of feature names and returns a HashMap mapping each backend
    /// to the list of features that should be fetched from it, based on the
    /// feature view's backend_routing configuration.
    ///
    /// # Errors
    ///
    /// Returns `ResolverError::Internal` if any feature has no backend routing defined.
    fn group_features_by_backend(
        &self,
        feature_names: &[String],
        metadata: &FeatureViewMetadata,
    ) -> Result<HashMap<String, Vec<String>>, ResolverError> {
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();

        for feature_name in feature_names {
            let backend_name = metadata
                .backend_routing
                .get(feature_name)
                .ok_or_else(|| {
                    ResolverError::Internal(format!(
                        "No backend routing for feature '{}'",
                        feature_name
                    ))
                })?
                .clone();

            groups
                .entry(backend_name)
                .or_default()
                .push(feature_name.clone());
        }

        Ok(groups)
    }

    /// Build a fallback mapping for features: feature_name -> (primary_backend, fallback_backend).
    ///
    /// Used to implement the UseFallback downtime strategy. Maps each feature to its primary
    /// and optional fallback backend for retry on timeout or error.
    fn build_fallback_map(
        &self,
        feature_names: &[String],
        metadata: &FeatureViewMetadata,
    ) -> Result<HashMap<String, (String, Option<String>)>, ResolverError> {
        let mut fallback_map: HashMap<String, (String, Option<String>)> = HashMap::new();

        for feature_name in feature_names {
            let primary_backend = metadata
                .backend_routing
                .get(feature_name)
                .ok_or_else(|| {
                    ResolverError::Internal(format!(
                        "No backend routing for feature '{}'",
                        feature_name
                    ))
                })?
                .clone();

            let fallback_backend = metadata
                .columns
                .iter()
                .find(|col| &col.name == feature_name)
                .and_then(|col| {
                    if col.fallback_source.is_empty() {
                        None
                    } else {
                        Some(col.fallback_source.clone())
                    }
                });

            fallback_map.insert(feature_name.clone(), (primary_backend, fallback_backend));
        }

        Ok(fallback_map)
    }

    /// Dispatch feature requests to multiple backends in parallel.
    ///
    /// Groups features by backend, then concurrently calls each backend's
    /// `get_with_resolutions()` method. Tracks execution latency and errors
    /// for each backend independently. Implements fallback retry logic when
    /// downtime_strategy is UseFallback.
    ///
    /// # Arguments
    ///
    /// * `backend_groups` - Map of backend_name -> features to fetch from that backend
    /// * `entity_ids` - List of entity IDs to fetch features for
    /// * `entity_key` - Name of the entity identifier field
    /// * `resolutions` - Map of feature_name -> FeatureResolution (type + source path)
    /// * `as_of` - Optional point-in-time timestamp for temporal queries
    /// * `timeout` - Optional timeout for the entire request
    /// * `fallback_map` - Map of feature_name -> (primary_backend, fallback_backend)
    /// * `downtime_strategy` - Downtime strategy from config
    ///
    /// # Returns
    ///
    /// A Vec of `FanoutResult` structs, one per backend. Each result contains:
    /// - The backend name
    /// - A RecordBatch with the fetched features (or error info)
    /// - Execution latency in milliseconds
    /// - Status ("success", "timeout", "error", etc.)
    /// - Optional fallback_used and error message
    ///
    /// # Errors
    ///
    /// Returns `ResolverError::Internal` if backend group is empty or adapter
    /// cannot be resolved.
    #[expect(clippy::too_many_arguments)]
    async fn dispatch_to_backends(
        &self,
        backend_groups: HashMap<String, Vec<String>>,
        entity_ids: &[String],
        entity_key: &str,
        resolutions: &HashMap<String, FeatureResolution>,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
        fallback_map: &HashMap<String, (String, Option<String>)>,
        downtime_strategy: crate::config::DowntimeStrategy,
    ) -> Result<Vec<FanoutResult>, ResolverError> {
        if backend_groups.is_empty() {
            return Err(ResolverError::Internal(
                "No backends to dispatch to".to_string(),
            ));
        }

        let tasks: Vec<_> = backend_groups
            .into_iter()
            .map(|(backend_name, features)| {
                let adapter = self
                    .adapters
                    .get(&backend_name)
                    .map(|a| a.clone())
                    .ok_or_else(|| ResolverError::BackendNotFound(backend_name.clone()));

                let backend_name_clone = backend_name.clone();
                let entity_key = entity_key.to_string();
                let resolutions = resolutions.clone();
                let entity_ids = entity_ids.to_vec();
                let fallback_map_clone = fallback_map.clone();
                let adapters = self.adapters.clone();

                async move {
                    match adapter {
                        Err(e) => FanoutResult {
                            backend: backend_name_clone,
                            batch: RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::new(
                                vec![] as Vec<arrow::datatypes::Field>,
                            ))),
                            latency_ms: 0,
                            status: "error".to_string(),
                            fallback_used: None,
                            error: Some(e.to_string()),
                        },
                        Ok(adapter) => {
                            let start = Instant::now();
                            match adapter
                                .get_sparse_with_resolutions(
                                    &entity_ids,
                                    &features,
                                    &entity_key,
                                    &resolutions,
                                    as_of,
                                    timeout,
                                )
                                .await
                            {
                                Ok(batch) => FanoutResult {
                                    backend: backend_name_clone,
                                    batch,
                                    latency_ms: start.elapsed().as_millis() as u64,
                                    status: "success".to_string(),
                                    fallback_used: None,
                                    error: None,
                                },
                                Err(e) => {
                                    let error_str = e.to_string();
                                    let is_timeout = error_str.contains("timeout");
                                    let status = if is_timeout { "timeout" } else { "error" };

                                    // Attempt fallback if strategy is UseFallback
                                    if downtime_strategy
                                        == crate::config::DowntimeStrategy::UseFallback
                                    {
                                        let should_retry = features.iter().any(|feature| {
                                            if let Some((primary, fallback_opt)) =
                                                fallback_map_clone.get(feature)
                                            {
                                                primary == &backend_name_clone
                                                    && fallback_opt.is_some()
                                            } else {
                                                false
                                            }
                                        });

                                        if should_retry
                                            && let Some(fallback_backend) =
                                                features.iter().find_map(|f| {
                                                    fallback_map_clone
                                                        .get(f)
                                                        .and_then(|(_, fb)| fb.as_ref())
                                                        .cloned()
                                                })
                                            && let Some(fallback_adapter) =
                                                adapters.get(&fallback_backend)
                                        {
                                            match fallback_adapter
                                                .get_sparse_with_resolutions(
                                                    &entity_ids,
                                                    &features,
                                                    &entity_key,
                                                    &resolutions,
                                                    as_of,
                                                    timeout,
                                                )
                                                .await
                                            {
                                                Ok(batch) => {
                                                    return FanoutResult {
                                                        backend: backend_name_clone,
                                                        batch,
                                                        latency_ms: start.elapsed().as_millis()
                                                            as u64,
                                                        status: "success".to_string(),
                                                        fallback_used: Some(
                                                            fallback_backend.to_string(),
                                                        ),
                                                        error: None,
                                                    };
                                                }
                                                Err(fallback_err) => {
                                                    return FanoutResult {
                                                        backend: backend_name_clone,
                                                        batch: RecordBatch::new_empty(Arc::new(
                                                            arrow::datatypes::Schema::new(vec![]
                                                                as Vec<arrow::datatypes::Field>),
                                                        )),
                                                        latency_ms: start.elapsed().as_millis()
                                                            as u64,
                                                        status: "error".to_string(),
                                                        fallback_used: Some(
                                                            fallback_backend.to_string(),
                                                        ),
                                                        error: Some(format!(
                                                            "Primary: {}; Fallback: {}",
                                                            error_str, fallback_err
                                                        )),
                                                    };
                                                }
                                            }
                                        }
                                    }

                                    // No fallback or fallback also failed
                                    FanoutResult {
                                        backend: backend_name_clone,
                                        batch: RecordBatch::new_empty(Arc::new(
                                            arrow::datatypes::Schema::new(
                                                vec![] as Vec<arrow::datatypes::Field>
                                            ),
                                        )),
                                        latency_ms: start.elapsed().as_millis() as u64,
                                        status: status.to_string(),
                                        fallback_used: None,
                                        error: Some(error_str),
                                    }
                                }
                            }
                        }
                    }
                }
            })
            .collect();

        Ok(join_all(tasks).await)
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
    ) -> Result<(RecordBatch, FanoutLatencies), ResolverError> {
        let mut metrics = FanoutLatencies::new();

        let registry_timer = Timer::start();
        let metadata = self
            .registry
            .get_view(feature_view_name)
            .await
            .map_err(|e| ResolverError::Registry(e.to_string()))?;
        metrics.record_phase(Phase::RegistryLookup, registry_timer.stop());

        let partition_timer = Timer::start();
        let backend_groups = self.group_features_by_backend(feature_names, &metadata)?;
        metrics.record_phase(Phase::Partition, partition_timer.stop());

        let mut resolutions = HashMap::new();

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
                FeatureResolution {
                    expected_type,
                    source_path: resolved_path,
                },
            );
        }

        match backend_groups.len() {
            0 => Err(ResolverError::Internal("Empty feature list".to_string())),
            1 => {
                let backend_name = backend_groups
                    .keys()
                    .next()
                    .ok_or_else(|| ResolverError::Internal("Empty backend groups".to_string()))?
                    .clone();

                let adapter = self
                    .adapters
                    .get(&backend_name)
                    .ok_or_else(|| ResolverError::BackendNotFound(backend_name.clone()))?;

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

                metrics.finalize();
                Ok((batch, metrics))
            }
            _ => {
                let dispatch_timer = Timer::start();

                // Build fallback mapping for UseFallback strategy
                let fallback_map = self.build_fallback_map(feature_names, &metadata)?;

                let fanout_results = self
                    .dispatch_to_backends(
                        backend_groups,
                        entity_ids,
                        &metadata.entity_key,
                        &resolutions,
                        as_of,
                        timeout,
                        &fallback_map,
                        self.downtime_strategy,
                    )
                    .await?;
                metrics.record_phase(Phase::Dispatch, dispatch_timer.stop());

                // Record per-backend latencies
                for result in &fanout_results {
                    if result.status == "success" {
                        let backend_type = MetricsBackend::from_name(&result.backend);
                        metrics.record_backend(backend_type, result.latency_ms as f64);
                    }
                }

                // Check for backend errors first
                let errors: Vec<_> = fanout_results
                    .iter()
                    .filter(|r| r.status != "success")
                    .map(|r| {
                        format!(
                            "{}: {}",
                            r.backend,
                            r.error.as_deref().unwrap_or("unknown error")
                        )
                    })
                    .collect();

                if !errors.is_empty() {
                    return Err(ResolverError::Internal(format!(
                        "Backends failed during fanout. Errors: {}",
                        errors.join("; ")
                    )));
                }

                // Merge results from successful backends
                let merge_timer = Timer::start();
                let backend_batches: Vec<_> =
                    fanout_results.iter().map(|r| r.batch.clone()).collect();

                let merged =
                    crate::fanout::FanoutMerger::merge(entity_ids, &backend_batches, feature_names)
                        .map_err(|e| ResolverError::Internal(format!("Merge failed: {}", e)))?;
                metrics.record_phase(Phase::Merge, merge_timer.stop());

                // Validate and detect partial failures per null strategy
                let validation_timer = Timer::start();
                let per_feature_strategies: Vec<_> = feature_names
                    .iter()
                    .map(|name| {
                        let strategy = metadata
                            .columns
                            .iter()
                            .find(|col| &col.name == name)
                            .map(|_col| {
                                // For now, use global fanout strategy; Phase 3 adds per-feature config
                                self.fanout_config.partial_failure_strategy
                            })
                            .unwrap_or(PartialFailureStrategy::NullFill);
                        (name.clone(), strategy)
                    })
                    .collect();

                crate::fanout::detect_partial_failure(
                    entity_ids.len(),
                    &merged,
                    &per_feature_strategies,
                )
                .map_err(|e| ResolverError::Internal(format!("Partial failure detected: {}", e)))?;
                metrics.record_phase(Phase::Validation, validation_timer.stop());

                // Finalize metrics
                metrics.record_phase(Phase::Serialization, 0.0); // Serialization happens at server level
                metrics.finalize();

                // Log metrics (will be integrated with tracing in Phase 3b)
                tracing::debug!(
                    "Fanout metrics - total: {:.2}ms, dispatch: {:.2}ms, merge: {:.2}ms, validation: {:.2}ms, critical_path: {:.2}ms",
                    metrics.total_ms,
                    metrics.dispatch_ms,
                    metrics.merge_ms,
                    metrics.validation_ms,
                    metrics.critical_path_ms
                );

                Ok((merged, metrics))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::quiver::v1::FeatureColumnSchema;

    fn make_test_metadata() -> FeatureViewMetadata {
        let mut backend_routing = HashMap::new();
        backend_routing.insert("feature_a".to_string(), "backend_1".to_string());
        backend_routing.insert("feature_b".to_string(), "backend_1".to_string());
        backend_routing.insert("feature_c".to_string(), "backend_2".to_string());
        backend_routing.insert("feature_d".to_string(), "backend_2".to_string());
        backend_routing.insert("feature_e".to_string(), "backend_3".to_string());

        FeatureViewMetadata {
            name: "test_view".to_string(),
            entity_type: "user".to_string(),
            entity_key: "user_id".to_string(),
            columns: vec![
                FeatureColumnSchema {
                    name: "feature_a".to_string(),
                    arrow_type: "float64".to_string(),
                    nullable: false,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "feature_b".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "feature_c".to_string(),
                    arrow_type: "bool".to_string(),
                    nullable: false,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "feature_d".to_string(),
                    arrow_type: "string".to_string(),
                    nullable: true,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "feature_e".to_string(),
                    arrow_type: "float64".to_string(),
                    nullable: true,
                    fallback_source: String::new(),
                },
            ],
            backend_routing,
            schema_version: 1,
        }
    }

    #[test]
    fn test_group_features_single_backend() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);
        let metadata = make_test_metadata();

        let features = vec!["feature_a".to_string(), "feature_b".to_string()];
        let result = resolver.group_features_by_backend(&features, &metadata);

        assert!(result.is_ok());
        let groups = result.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups.get("backend_1").unwrap(),
            &vec!["feature_a".to_string(), "feature_b".to_string()]
        );
    }

    #[test]
    fn test_group_features_multiple_backends() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);
        let metadata = make_test_metadata();

        let features = vec![
            "feature_a".to_string(),
            "feature_c".to_string(),
            "feature_e".to_string(),
        ];
        let result = resolver.group_features_by_backend(&features, &metadata);

        assert!(result.is_ok());
        let groups = result.unwrap();
        assert_eq!(groups.len(), 3);
        assert_eq!(
            groups.get("backend_1").unwrap(),
            &vec!["feature_a".to_string()]
        );
        assert_eq!(
            groups.get("backend_2").unwrap(),
            &vec!["feature_c".to_string()]
        );
        assert_eq!(
            groups.get("backend_3").unwrap(),
            &vec!["feature_e".to_string()]
        );
    }

    #[test]
    fn test_group_features_preserves_order() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);
        let metadata = make_test_metadata();

        let features = vec!["feature_b".to_string(), "feature_a".to_string()];
        let result = resolver.group_features_by_backend(&features, &metadata);

        assert!(result.is_ok());
        let groups = result.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups.get("backend_1").unwrap(),
            &vec!["feature_b".to_string(), "feature_a".to_string()]
        );
    }

    #[test]
    fn test_group_features_missing_routing() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);
        let metadata = make_test_metadata();

        let features = vec!["feature_a".to_string(), "unknown_feature".to_string()];
        let result = resolver.group_features_by_backend(&features, &metadata);

        assert!(result.is_err());
        match result.unwrap_err() {
            ResolverError::Internal(msg) => {
                assert!(msg.contains("No backend routing for feature"));
                assert!(msg.contains("unknown_feature"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_to_backends_empty_groups() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);

        let result = resolver
            .dispatch_to_backends(
                HashMap::new(),
                &["user:1000".to_string()],
                "user_id",
                &HashMap::new(),
                None,
                None,
                &HashMap::new(),
                crate::config::DowntimeStrategy::Fail,
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ResolverError::Internal(msg) => {
                assert!(msg.contains("No backends to dispatch to"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_to_backends_missing_adapter() {
        let registry = Arc::new(crate::registry::StaticRegistry::new());
        let resolver = Resolver::new(registry);

        let mut backend_groups = HashMap::new();
        backend_groups.insert(
            "nonexistent_backend".to_string(),
            vec!["feature_a".to_string()],
        );

        let result = resolver
            .dispatch_to_backends(
                backend_groups,
                &["user:1000".to_string()],
                "user_id",
                &HashMap::new(),
                None,
                None,
                &HashMap::new(),
                crate::config::DowntimeStrategy::Fail,
            )
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].backend, "nonexistent_backend");
        assert_eq!(results[0].status, "error");
        assert!(results[0].error.is_some());
        assert!(results[0].error.as_ref().unwrap().contains("not found"));
    }
}
