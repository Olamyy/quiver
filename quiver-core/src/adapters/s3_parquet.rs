//! Parquet file adapter supporting S3 and local filesystem storage via object_store.
//!
//! This adapter enables feature serving from Parquet files organized in S3 or local directories.
//!
//! Features:
//! - Unified object_store interface for S3 and local filesystems
//! - Single file reading without date partitioning (Phase 1)
//! - Date partitioning and time-travel queries (Phase 2)
//! - Entity filtering by entity_key column
//! - CurrentOnly temporal capability (Phase 1) / TimeTravel (Phase 2)

use crate::adapters::{
    AdapterCapabilities, AdapterError, BackendAdapter, FeatureResolution, HealthStatus,
    OrderingGuarantee, TemporalCapability,
};
use crate::config::{S3ParquetAdapterConfig, SourcePath};
use crate::validation::ValidationConfig;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use lru::LruCache;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_config;
use aws_config::BehaviorVersion;

const BACKEND_NAME: &str = "s3_parquet";

/// Date partitioning information extracted from source_path template.
///
/// Tracks which date components (year, month, day) are present in the path template.
/// Used to construct paths for partition discovery and selection.
#[derive(Debug, Clone)]
struct DatePartitioning {
    /// Whether path contains {year} placeholder
    has_year: bool,
    /// Whether path contains {month} placeholder
    has_month: bool,
    /// Whether path contains {day} placeholder
    has_day: bool,
    /// Whether path contains {date} placeholder (YYYY-MM-DD format)
    has_date: bool,
}

impl DatePartitioning {
    /// Parse date partitioning from source_path template.
    fn from_template(template: &str) -> Self {
        Self {
            has_year: template.contains("{year}"),
            has_month: template.contains("{month}"),
            has_day: template.contains("{day}"),
            has_date: template.contains("{date}"),
        }
    }

    /// Check if this template uses date partitioning.
    fn is_partitioned(&self) -> bool {
        self.has_year || self.has_month || self.has_day || self.has_date
    }

    /// Resolve path for a specific date, with feature name substitution.
    fn resolve_for_date(&self, template: &str, date: chrono::NaiveDate, feature: &str) -> String {
        let mut path = template.replace("{feature}", feature);

        if self.has_date {
            path = path.replace("{date}", &date.format("%Y-%m-%d").to_string());
        }
        if self.has_year {
            path = path.replace("{year}", &date.format("%Y").to_string());
        }
        if self.has_month {
            path = path.replace("{month}", &date.format("%m").to_string());
        }
        if self.has_day {
            path = path.replace("{day}", &date.format("%d").to_string());
        }

        path
    }
}

/// S3/Parquet adapter for feature serving from Parquet files.
pub struct S3ParquetAdapter {
    object_store: Arc<dyn ObjectStore>,
    source_path_template: String,
    timeout_default: Duration,
    capabilities: AdapterCapabilities,
    #[allow(dead_code)]
    validation_config: ValidationConfig,
    bucket: String,
    date_partitioning: DatePartitioning,
    max_days_back: u32,
    #[allow(dead_code)]
    schema_cache: Arc<Mutex<LruCache<String, Arc<ArrowSchema>>>>,
}

impl S3ParquetAdapter {
    /// Create a new S3/Parquet adapter.
    ///
    /// # Arguments
    /// - `config`: S3ParquetAdapterConfig with bucket, source_path, timeout
    /// - `aws_auth`: Optional AWS authentication config (if None, uses env vars or IAM roles)
    pub async fn new(
        config: &S3ParquetAdapterConfig,
        validation_config: Option<ValidationConfig>,
    ) -> Result<Self, AdapterError> {
        let source_path_template = match &config.source_path {
            SourcePath::Template(tmpl) => tmpl.clone(),
            SourcePath::Structured { table, .. } => table.clone(),
        };

        let object_store = if config.bucket.starts_with("s3://") {
            Self::create_s3_store(&config.bucket, config).await?
        } else if config.bucket.starts_with("file://") {
            Self::create_local_store(&config.bucket)?
        } else {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "bucket must start with 's3://' or 'file://'",
            ));
        };

        let timeout_duration = config
            .timeout_seconds
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));

        let date_partitioning = DatePartitioning::from_template(&source_path_template);

        let temporal_capability = if date_partitioning.is_partitioned() {
            TemporalCapability::TimeTravel {
                typical_latency_ms: 200,
            }
        } else {
            TemporalCapability::CurrentOnly {
                typical_latency_ms: 100,
            }
        };

        let capabilities = AdapterCapabilities {
            temporal: temporal_capability,
            max_batch_size: Some(100_000),
            optimal_batch_size: Some(10_000),
            typical_latency_ms: if date_partitioning.is_partitioned() {
                200
            } else {
                100
            },
            supports_parallel_requests: true,
            ordering_guarantee: OrderingGuarantee::Unordered,
        };

        let cache_size = NonZeroUsize::new(128).unwrap();
        let schema_cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

        Ok(Self {
            object_store,
            source_path_template,
            timeout_default: timeout_duration,
            capabilities,
            validation_config: validation_config.unwrap_or_default(),
            bucket: config.bucket.clone(),
            date_partitioning,
            max_days_back: config.max_days_back,
            schema_cache,
        })
    }

    /// Create an S3 object store from storage URI using AWS SDK credential chain.
    ///
    /// Loads credentials from multiple sources in order:
    /// 1. AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY environment variables
    /// 2. ~/.aws/credentials file (default profile or specified profile)
    /// 3. AWS SSO
    /// 4. Instance metadata service (EC2)
    ///
    /// Resolves region in priority order:
    /// 1. config.auth.region (can be a literal value or ${ENV_VAR} format)
    /// 2. AWS_REGION environment variable
    /// 3. SDK resolution (from ~/.aws/config or environment)
    async fn create_s3_store(
        bucket: &str,
        config: &S3ParquetAdapterConfig,
    ) -> Result<Arc<dyn ObjectStore>, AdapterError> {
        let bucket = bucket
            .strip_prefix("s3://")
            .ok_or_else(|| AdapterError::invalid(BACKEND_NAME, "Invalid S3 URI format"))?
            .split('/')
            .next()
            .ok_or_else(|| {
                AdapterError::invalid(BACKEND_NAME, "Could not extract bucket from S3 URI")
            })?;

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .load()
            .await;

        let creds = match sdk_config.credentials_provider() {
            Some(provider) => {
                use aws_credential_types::provider::ProvideCredentials;
                provider.provide_credentials().await
                    .map_err(|e| {
                        AdapterError::connection_failed(
                            BACKEND_NAME,
                            format!("Failed to load AWS credentials: {}", e),
                        )
                    })?
            }
            None => {
                return Err(AdapterError::connection_failed(
                    BACKEND_NAME,
                    "No AWS credentials provider found. Set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY or configure ~/.aws/credentials"
                ));
            }
        };

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id(creds.access_key_id())
            .with_secret_access_key(creds.secret_access_key());

        if let Some(token) = creds.session_token() {
            builder = builder.with_token(token);
            tracing::debug!("S3 adapter using temporary session credentials");
        } else {
            tracing::debug!("S3 adapter using static credentials");
        }

        let region = config.auth.region.as_ref()
            .cloned()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| sdk_config.region().map(|r| r.to_string()))
            .ok_or_else(|| {
                AdapterError::connection_failed(
                    BACKEND_NAME,
                    "AWS region not configured. Set auth.region in config, AWS_REGION environment variable, or configure in ~/.aws/config"
                )
            })?;

        builder = builder.with_region(&region);
        tracing::debug!("S3 adapter using region: {}", region);

        let store = builder.build().map_err(|e| {
            AdapterError::connection_failed(
                BACKEND_NAME,
                format!("Failed to build S3 store: {}", e),
            )
        })?;

        Ok(Arc::new(store))
    }

    /// Create a local filesystem object store from file URI.
    fn create_local_store(bucket: &str) -> Result<Arc<dyn ObjectStore>, AdapterError> {
        let path = bucket
            .strip_prefix("file://")
            .ok_or_else(|| AdapterError::invalid(BACKEND_NAME, "Invalid file URI format"))?;

        let store = LocalFileSystem::new_with_prefix(path).map_err(|e| {
            AdapterError::invalid(BACKEND_NAME, format!("Failed to create local store: {}", e))
        })?;

        Ok(Arc::new(store) as Arc<dyn ObjectStore>)
    }

    /// Resolve path template for a feature (no date partitioning).
    fn resolve_source_path(&self, feature_name: &str) -> String {
        self.source_path_template.replace("{feature}", feature_name)
    }

    /// Find the most recent partition on or before the given date.
    ///
    /// For partitioned storage, searches backwards from the given date to find
    /// the latest available partition. Returns the resolved path for that partition.
    async fn find_partition_for_date(
        &self,
        feature_name: &str,
        as_of: DateTime<Utc>,
    ) -> Result<String, AdapterError> {
        if !self.date_partitioning.is_partitioned() {
            return Ok(self.resolve_source_path(feature_name));
        }

        let search_date = as_of.naive_utc().date();

        for days_back in 0..self.max_days_back {
            let candidate_date = search_date - chrono::Duration::days(days_back as i64);
            let candidate_path = self.date_partitioning.resolve_for_date(
                &self.source_path_template,
                candidate_date,
                feature_name,
            );

            let obj_path = object_store::path::Path::from(candidate_path.clone());
            if self.object_store.head(&obj_path).await.is_ok() {
                tracing::info!(
                    "Found partition for {} at date {}: {}",
                    feature_name,
                    candidate_date,
                    candidate_path
                );
                return Ok(candidate_path);
            }
        }

        Err(AdapterError::internal(
            BACKEND_NAME,
            format!(
                "No partition found for feature {} within {} days of {}",
                feature_name, self.max_days_back, as_of
            ),
        ))
    }

    #[allow(dead_code)]
    async fn get_schema(&self, file_path: &str) -> Result<Arc<ArrowSchema>, AdapterError> {
        {
            let mut cache = self.schema_cache.lock().unwrap();
            if let Some(schema) = cache.get(file_path) {
                return Ok(schema.clone());
            }
        }

        let obj_path = object_store::path::Path::from(file_path);
        let result = self.object_store.get(&obj_path).await.map_err(|e| {
            AdapterError::internal(BACKEND_NAME, format!("Failed to read file: {}", e))
        })?;

        let bytes = result.bytes().await.map_err(|e| {
            AdapterError::internal(BACKEND_NAME, format!("Failed to read file bytes: {}", e))
        })?;

        let builder = ArrowReaderBuilder::try_new(bytes).map_err(|e| {
            AdapterError::arrow(BACKEND_NAME, format!("Failed to build Arrow reader: {}", e))
        })?;

        let schema: Arc<ArrowSchema> = builder.schema().clone();

        {
            let mut cache = self.schema_cache.lock().unwrap();
            cache.put(file_path.to_string(), schema.clone());
        }

        Ok(schema)
    }

    /// Read Parquet file and filter rows by entity_key.
    async fn read_parquet_file(
        &self,
        file_path: &str,
        entity_ids: &[String],
        entity_key: &str,
    ) -> Result<RecordBatch, AdapterError> {
        let obj_path = object_store::path::Path::from(file_path);
        let result = self.object_store.get(&obj_path).await.map_err(|e| {
            AdapterError::internal(BACKEND_NAME, format!("Failed to read file: {}", e))
        })?;

        let bytes = result.bytes().await.map_err(|e| {
            AdapterError::internal(BACKEND_NAME, format!("Failed to read file bytes: {}", e))
        })?;

        let builder = ArrowReaderBuilder::try_new(bytes).map_err(|e| {
            AdapterError::arrow(BACKEND_NAME, format!("Failed to build Arrow reader: {}", e))
        })?;

        let reader = builder.build().map_err(|e| {
            AdapterError::arrow(
                BACKEND_NAME,
                format!("Failed to create Arrow reader: {}", e),
            )
        })?;

        let mut all_batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                AdapterError::arrow(BACKEND_NAME, format!("Failed to read batch: {}", e))
            })?;
            all_batches.push(batch);
        }

        let combined_batch = if all_batches.is_empty() {
            let schema = Arc::new(arrow::datatypes::Schema::new(
                vec![] as Vec<arrow::datatypes::Field>
            ));
            RecordBatch::new_empty(schema)
        } else if all_batches.len() == 1 {
            all_batches.pop().unwrap()
        } else {
            arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
                .map_err(|e| AdapterError::arrow(BACKEND_NAME, format!("Concat failed: {}", e)))?
        };

        if entity_ids.is_empty() {
            return Ok(RecordBatch::new_empty(combined_batch.schema()));
        }

        if let Ok(entity_col_idx) = combined_batch.schema().index_of(entity_key) {
            let entity_array = combined_batch.column(entity_col_idx);
            let entity_set: std::collections::HashSet<_> = entity_ids.iter().cloned().collect();

            let mut indices = Vec::new();
            for row_idx in 0..combined_batch.num_rows() {
                if let Some(crate::adapters::utils::ScalarValue::Utf8(entity)) =
                    crate::adapters::utils::extract_scalar(entity_array, row_idx)
                    && entity_set.contains(&entity)
                {
                    indices.push(row_idx as u32);
                }
            }

            if indices.is_empty() {
                Ok(RecordBatch::new_empty(combined_batch.schema()))
            } else {
                let indices_array = arrow::array::UInt32Array::from(indices);
                let mut filtered_cols = Vec::new();

                for col in combined_batch.columns() {
                    let filtered =
                        arrow::compute::take(col, &indices_array, None).map_err(|e| {
                            AdapterError::arrow(BACKEND_NAME, format!("Filter failed: {}", e))
                        })?;
                    filtered_cols.push(filtered);
                }

                let filtered_batch = RecordBatch::try_new(combined_batch.schema(), filtered_cols)
                    .map_err(|e| {
                    AdapterError::arrow(BACKEND_NAME, format!("RecordBatch creation failed: {}", e))
                })?;

                Ok(filtered_batch)
            }
        } else {
            Ok(combined_batch)
        }
    }
}

#[async_trait::async_trait]
impl BackendAdapter for S3ParquetAdapter {
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
        let timeout_duration = timeout.unwrap_or(self.timeout_default);
        let deadline = std::time::Instant::now() + timeout_duration;

        if feature_names.len() != 1 {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "S3Parquet adapter supports reading one feature at a time",
            ));
        }

        let feature_name = &feature_names[0];

        let resolved_path = if let Some(as_of_time) = as_of {
            self.find_partition_for_date(feature_name, as_of_time)
                .await?
        } else {
            self.resolve_source_path(feature_name)
        };

        let now = std::time::Instant::now();
        if now > deadline {
            return Err(AdapterError::timeout(
                BACKEND_NAME,
                timeout_duration.as_millis() as u64,
            ));
        }

        self.read_parquet_file(&resolved_path, entity_ids, entity_key)
            .await
    }

    async fn get_with_resolutions(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        _resolutions: &std::collections::HashMap<String, FeatureResolution>,
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        self.get(entity_ids, feature_names, entity_key, as_of, timeout)
            .await
    }

    async fn health(&self) -> HealthStatus {
        let start = std::time::Instant::now();

        match self.object_store.list(None).next().await {
            Some(Ok(_)) => {
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                HealthStatus {
                    healthy: true,
                    backend: BACKEND_NAME.to_string(),
                    message: Some(format!("Connected to storage: {}", self.bucket)),
                    latency_ms: Some(elapsed_ms),
                    last_check: Utc::now(),
                    capabilities_verified: true,
                    estimated_capacity: None,
                }
            }
            Some(Err(e)) => HealthStatus::unhealthy(BACKEND_NAME, format!("Storage error: {}", e)),
            None => {
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                HealthStatus {
                    healthy: true,
                    backend: BACKEND_NAME.to_string(),
                    message: Some(
                        "Storage accessible (empty or list returned no items)".to_string(),
                    ),
                    latency_ms: Some(elapsed_ms),
                    last_check: Utc::now(),
                    capabilities_verified: true,
                    estimated_capacity: None,
                }
            }
        }
    }

    async fn initialize(&mut self) -> Result<(), AdapterError> {
        let health = self.health().await;

        if !health.healthy {
            return Err(AdapterError::internal(
                BACKEND_NAME,
                format!(
                    "Failed to initialize S3/Parquet adapter: {}",
                    health
                        .message
                        .unwrap_or_else(|| "Unknown error".to_string())
                ),
            ));
        }

        tracing::info!("S3/Parquet adapter initialized successfully");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_adapter(template: &str) -> S3ParquetAdapter {
        let date_partitioning = DatePartitioning::from_template(template);
        S3ParquetAdapter {
            object_store: Arc::new(LocalFileSystem::new()),
            source_path_template: template.to_string(),
            timeout_default: Duration::from_secs(30),
            capabilities: AdapterCapabilities {
                temporal: if date_partitioning.is_partitioned() {
                    TemporalCapability::TimeTravel {
                        typical_latency_ms: 200,
                    }
                } else {
                    TemporalCapability::CurrentOnly {
                        typical_latency_ms: 100,
                    }
                },
                max_batch_size: Some(100_000),
                optimal_batch_size: Some(10_000),
                typical_latency_ms: 100,
                supports_parallel_requests: true,
                ordering_guarantee: OrderingGuarantee::Unordered,
            },
            validation_config: ValidationConfig::default(),
            bucket: "file:///data".to_string(),
            date_partitioning,
            max_days_back: 365,
            schema_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(128).unwrap()))),
        }
    }

    #[test]
    fn test_resolve_source_path_with_feature_template() {
        let adapter = make_test_adapter("/features/{feature}.parquet");

        assert_eq!(
            adapter.resolve_source_path("user_score"),
            "/features/user_score.parquet"
        );
    }

    #[test]
    fn test_resolve_source_path_without_placeholder() {
        let adapter = make_test_adapter("/batch_2024_03_07.parquet");

        assert_eq!(
            adapter.resolve_source_path("user_score"),
            "/batch_2024_03_07.parquet"
        );
    }

    #[test]
    fn test_date_partitioning_detection() {
        let adapter_no_date = make_test_adapter("/features/{feature}.parquet");
        assert!(!adapter_no_date.date_partitioning.is_partitioned());

        let adapter_with_year = make_test_adapter("/year={year}/{feature}.parquet");
        assert!(adapter_with_year.date_partitioning.is_partitioned());
        assert!(adapter_with_year.date_partitioning.has_year);
        assert!(!adapter_with_year.date_partitioning.has_month);

        let adapter_full_date = make_test_adapter("/date={date}/{feature}.parquet");
        assert!(adapter_full_date.date_partitioning.is_partitioned());
        assert!(adapter_full_date.date_partitioning.has_date);
    }

    #[test]
    fn test_date_partitioning_path_resolution() {
        let adapter = make_test_adapter("/year={year}/month={month}/day={day}/{feature}.parquet");
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 7).unwrap();

        let path = adapter.date_partitioning.resolve_for_date(
            &adapter.source_path_template,
            date,
            "user_score",
        );

        assert_eq!(path, "/year=2024/month=03/day=07/user_score.parquet");
    }

    #[test]
    fn test_date_partitioning_with_full_date() {
        let adapter = make_test_adapter("/features/date={date}/{feature}.parquet");
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 7).unwrap();

        let path = adapter.date_partitioning.resolve_for_date(
            &adapter.source_path_template,
            date,
            "transaction_count",
        );

        assert_eq!(path, "/features/date=2024-03-07/transaction_count.parquet");
    }

    #[test]
    fn test_temporal_capability_depends_on_partitioning() {
        let adapter_no_part = make_test_adapter("/data/{feature}.parquet");
        if let TemporalCapability::CurrentOnly { .. } = adapter_no_part.capabilities.temporal {
        } else {
            panic!("Non-partitioned adapter should have CurrentOnly capability");
        }

        let adapter_with_part = make_test_adapter("/year={year}/{feature}.parquet");
        if let TemporalCapability::TimeTravel { .. } = adapter_with_part.capabilities.temporal {
        } else {
            panic!("Partitioned adapter should have TimeTravel capability");
        }
    }
}
