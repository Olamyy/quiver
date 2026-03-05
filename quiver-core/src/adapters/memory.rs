use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use super::{
    AdapterCapabilities, AdapterError, BackendAdapter, HealthStatus, PutOptions, TemporalCapability,
};

const BACKEND_NAME: &str = "memory";

/// A single typed feature value. Null signals "entity had no value for this
/// feature at the requested time" — the resolver will null-fill the column.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Float64(f64),
    Int64(i64),
    Utf8(String),
    Boolean(bool),
    Null,
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Float64(_) => DataType::Float64,
            Self::Int64(_) => DataType::Int64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Boolean(_) => DataType::Boolean,
            Self::Null => DataType::Float64,
        }
    }
}

/// One write event. Different pipelines writing different features at different
/// times produce separate rows for the same entity. Per-feature temporal
/// resolution picks the best row per (entity, feature) pair.
#[derive(Debug, Clone)]
struct FeatureRow {
    entity_id: String,
    features: HashMap<String, ScalarValue>,
    feature_ts: DateTime<Utc>,
}

pub struct MemoryAdapter {
    rows: RwLock<Vec<FeatureRow>>,
}

impl MemoryAdapter {
    pub fn new() -> Self {
        Self {
            rows: RwLock::new(Vec::new()),
        }
    }

    /// Seed with pre-built rows. Convenience constructor for tests.
    pub fn seed<E, F, K>(rows: impl IntoIterator<Item = (E, F, DateTime<Utc>)>) -> Self
    where
        E: Into<String>,
        F: IntoIterator<Item = (K, ScalarValue)>,
        K: Into<String>,
    {
        let adapter = Self::new();
        {
            let mut store = adapter.rows.write().unwrap();
            for (entity_id, features, ts) in rows {
                store.push(FeatureRow {
                    entity_id: entity_id.into(),
                    features: features.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                    feature_ts: ts,
                });
            }
        }
        adapter
    }

    /// Insert a single row. Useful in tests that build state incrementally.
    pub fn insert(
        &self,
        entity_id: impl Into<String>,
        features: impl IntoIterator<Item = (impl Into<String>, ScalarValue)>,
        feature_ts: DateTime<Utc>,
    ) {
        let mut store = self.rows.write().unwrap();
        store.push(FeatureRow {
            entity_id: entity_id.into(),
            features: features.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            feature_ts,
        });
    }

    /// Per-feature temporal resolution.
    ///
    /// For a given (entity, feature, as_of) triple, return the value from
    /// the row with the maximum feature_ts at or before `as_of` that contains
    /// that feature. Returns None if no such row exists.
    ///
    /// Correctness invariant from the RFC:
    ///   v.feature_ts = max(all feature_ts WHERE feature_ts <= as_of
    ///                      AND entity_id = entity
    ///                      AND feature_name in row.features)
    fn resolve_one(
        rows: &[FeatureRow],
        entity_id: &str,
        feature_name: &str,
        as_of: &DateTime<Utc>,
    ) -> Option<ScalarValue> {
        rows.iter()
            .filter(|r| {
                r.entity_id == entity_id
                    && r.feature_ts <= *as_of
                    && r.features.contains_key(feature_name)
            })
            .max_by_key(|r| r.feature_ts)
            .and_then(|r| r.features.get(feature_name).cloned())
    }
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl BackendAdapter for MemoryAdapter {
    fn name(&self) -> &str {
        BACKEND_NAME
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            temporal: TemporalCapability::TimeTravel {
                typical_latency_ms: 1,
            },
            max_batch_size: Some(10_000),
            optimal_batch_size: Some(1_000),
            typical_latency_ms: 1,
            supports_parallel_requests: true,
        }
    }

    async fn describe_schema(&self, feature_names: &[String]) -> Result<Schema, AdapterError> {
        if feature_names.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "feature_names cannot be empty",
            ));
        }

        let rows = self.rows.read().unwrap();
        let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];

        for feature_name in feature_names {
            let data_type = rows
                .iter()
                .find_map(|row| row.features.get(feature_name))
                .map(|value| value.data_type())
                .unwrap_or(DataType::Float64);

            fields.push(Field::new(feature_name, data_type, true));
        }

        Ok(Schema::new(fields))
    }

    async fn get(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
        as_of: Option<DateTime<Utc>>,
        timeout: Option<Duration>,
    ) -> Result<RecordBatch, AdapterError> {
        let start_time = std::time::Instant::now();

        if entity_ids.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "entity_ids cannot be empty",
            ));
        }
        if feature_names.is_empty() {
            return Err(AdapterError::invalid(
                BACKEND_NAME,
                "feature_names cannot be empty",
            ));
        }

        let capabilities = self.capabilities();
        if let Some(max_batch) = capabilities.max_batch_size
            && entity_ids.len() > max_batch
        {
            return Err(AdapterError::resource_limit_exceeded(
                BACKEND_NAME,
                format!(
                    "batch size {} exceeds maximum {}",
                    entity_ids.len(),
                    max_batch
                ),
            ));
        }

        let cutoff = as_of.unwrap_or_else(Utc::now);
        let rows = self.rows.read().unwrap();

        let resolved: Vec<Vec<Option<ScalarValue>>> = feature_names
            .iter()
            .map(|fname| {
                entity_ids
                    .iter()
                    .map(|eid| Self::resolve_one(&rows, eid, fname, &cutoff))
                    .collect()
            })
            .collect();

        drop(rows);

        if let Some(timeout_duration) = timeout
            && start_time.elapsed() > timeout_duration
        {
            return Err(AdapterError::timeout(
                BACKEND_NAME,
                timeout_duration.as_millis() as u64,
            ));
        }

        build_record_batch(entity_ids, feature_names, resolved)
            .map_err(|e| AdapterError::arrow(BACKEND_NAME, e.to_string()))
    }

    async fn put(&self, batch: RecordBatch, options: &PutOptions) -> Result<(), AdapterError> {
        let start_time = std::time::Instant::now();

        let entity_col = batch
            .column_by_name("entity_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                AdapterError::invalid(BACKEND_NAME, "batch must have a Utf8 'entity_id' column")
            })?;

        let ts_col = batch.column_by_name("_feature_ts").and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        });

        let feature_cols: Vec<(String, ArrayRef)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != "entity_id" && f.name() != "_feature_ts")
            .map(|(i, f)| (f.name().clone(), Arc::clone(batch.column(i))))
            .collect();

        let now = Utc::now();
        let mut store = self.rows.write().unwrap();

        for row_idx in 0..batch.num_rows() {
            if let Some(timeout_duration) = options.timeout
                && start_time.elapsed() > timeout_duration
            {
                return Err(AdapterError::timeout(
                    BACKEND_NAME,
                    timeout_duration.as_millis() as u64,
                ));
            }

            let entity_id = entity_col.value(row_idx).to_string();

            let feature_ts = ts_col
                .and_then(|col| {
                    if col.is_null(row_idx) {
                        None
                    } else {
                        let nanos = col.value(row_idx);
                        DateTime::from_timestamp(
                            nanos / 1_000_000_000,
                            (nanos % 1_000_000_000) as u32,
                        )
                    }
                })
                .unwrap_or(now);

            let mut features = HashMap::new();
            for (col_name, col_data) in &feature_cols {
                if col_data.is_null(row_idx) {
                    continue;
                }
                let value = extract_scalar(col_data, row_idx).ok_or_else(|| {
                    AdapterError::internal(
                        BACKEND_NAME,
                        format!("unsupported column type for '{}'", col_name),
                    )
                })?;
                features.insert(col_name.clone(), value);
            }

            // Handle upsert vs insert behavior
            if !options.upsert
                && store
                    .iter()
                    .any(|row| row.entity_id == entity_id && row.feature_ts == feature_ts)
            {
                return Err(AdapterError::invalid(
                    BACKEND_NAME,
                    format!(
                        "entity {} already exists at timestamp {}",
                        entity_id, feature_ts
                    ),
                ));
            }

            store.push(FeatureRow {
                entity_id,
                features,
                feature_ts,
            });
        }

        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        let count = self.rows.read().unwrap().len();
        HealthStatus {
            healthy: true,
            backend: BACKEND_NAME.to_string(),
            message: Some(format!("{} rows stored", count)),
            latency_ms: Some(0.1),
            last_check: Utc::now(),
            capabilities_verified: true,
            estimated_capacity: Some(100_000.0),
        }
    }
}

/// Build a RecordBatch from resolved per-feature values.
///
/// Column layout: entity_id first, then features in request order.
/// Missing values produce Arrow nulls — never errors.
fn build_record_batch(
    entity_ids: &[String],
    feature_names: &[String],
    resolved: Vec<Vec<Option<ScalarValue>>>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<Field> = Vec::with_capacity(feature_names.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(feature_names.len() + 1);

    fields.push(Field::new("entity_id", DataType::Utf8, false));
    let mut id_builder = StringBuilder::new();
    for eid in entity_ids {
        id_builder.append_value(eid);
    }
    columns.push(Arc::new(id_builder.finish()) as ArrayRef);

    for (feat_idx, feat_name) in feature_names.iter().enumerate() {
        let values = &resolved[feat_idx];

        let dtype = values
            .iter()
            .find_map(|v| v.as_ref())
            .map(|v| v.data_type())
            .unwrap_or(DataType::Float64);

        let (field, array) = build_column(feat_name, &dtype, values)?;
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn build_column(
    name: &str,
    dtype: &DataType,
    values: &[Option<ScalarValue>],
) -> Result<(Field, ArrayRef), arrow::error::ArrowError> {
    match dtype {
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for v in values {
                match v {
                    Some(ScalarValue::Float64(f)) => b.append_value(*f),
                    Some(ScalarValue::Int64(i)) => b.append_value(*i as f64),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Float64, true),
                Arc::new(b.finish()) as ArrayRef,
            ))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for v in values {
                match v {
                    Some(ScalarValue::Int64(i)) => b.append_value(*i),
                    Some(ScalarValue::Float64(f)) => b.append_value(*f as i64),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Int64, true),
                Arc::new(b.finish()) as ArrayRef,
            ))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    Some(ScalarValue::Utf8(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Utf8, true),
                Arc::new(b.finish()) as ArrayRef,
            ))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in values {
                match v {
                    Some(ScalarValue::Boolean(b2)) => b.append_value(*b2),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(b.finish()) as ArrayRef,
            ))
        }
        other => Err(arrow::error::ArrowError::InvalidArgumentError(format!(
            "MemoryAdapter: unsupported column type {:?}",
            other
        ))),
    }
}

fn extract_scalar(array: &ArrayRef, row: usize) -> Option<ScalarValue> {
    if array.is_null(row) {
        return Some(ScalarValue::Null);
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(ScalarValue::Float64(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(ScalarValue::Int64(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Some(ScalarValue::Utf8(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(ScalarValue::Boolean(a.value(row)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn ts(days_ago: i64) -> DateTime<Utc> {
        Utc::now() - Duration::days(days_ago)
    }

    fn make_adapter() -> MemoryAdapter {
        MemoryAdapter::seed([
            (
                "user:101",
                vec![
                    ("spend_30d", ScalarValue::Float64(450.0)),
                    ("session_count", ScalarValue::Int64(8)),
                ],
                ts(30),
            ),
            (
                "user:101",
                vec![("spend_30d", ScalarValue::Float64(890.0))],
                ts(7),
            ),
            (
                "user:101",
                vec![("session_count", ScalarValue::Int64(23))],
                ts(3),
            ),
            (
                "user:102",
                vec![
                    ("spend_30d", ScalarValue::Float64(120.0)),
                    ("session_count", ScalarValue::Int64(4)),
                ],
                ts(10),
            ),
        ])
    }

    #[tokio::test]
    async fn test_basic_get_returns_latest() {
        let adapter = make_adapter();
        let batch = adapter
            .get(
                &["user:101".to_string(), "user:102".to_string()],
                &["spend_30d".to_string(), "session_count".to_string()],
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 2);

        let spend = batch
            .column_by_name("spend_30d")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sessions = batch
            .column_by_name("session_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(spend.value(0), 890.0);
        assert_eq!(sessions.value(0), 23);
        assert_eq!(spend.value(1), 120.0);
        assert_eq!(sessions.value(1), 4);
    }

    #[tokio::test]
    async fn test_as_of_returns_historical_value() {
        let adapter = make_adapter();

        let batch = adapter
            .get(
                &["user:101".to_string()],
                &["spend_30d".to_string()],
                Some(ts(20)),
                None,
            )
            .await
            .unwrap();

        let spend = batch
            .column_by_name("spend_30d")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(
            spend.value(0),
            450.0,
            "expected 30-day value, not 7-day value"
        );
    }

    #[tokio::test]
    async fn test_per_feature_temporal_resolution() {
        let adapter = make_adapter();

        let batch = adapter
            .get(
                &["user:101".to_string()],
                &["spend_30d".to_string(), "session_count".to_string()],
                Some(ts(10)),
                None,
            )
            .await
            .unwrap();

        let spend = batch
            .column_by_name("spend_30d")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sessions = batch
            .column_by_name("session_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(spend.value(0), 450.0);
        assert_eq!(sessions.value(0), 8);
    }

    #[tokio::test]
    async fn test_missing_entity_produces_nulls() {
        let adapter = make_adapter();

        let batch = adapter
            .get(
                &[
                    "user:101".to_string(),
                    "user:999".to_string(),
                    "user:102".to_string(),
                ],
                &["spend_30d".to_string()],
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 3);

        let spend = batch
            .column_by_name("spend_30d")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!(!spend.is_null(0), "user:101 should be present");
        assert!(spend.is_null(1), "user:999 should be null");
        assert!(!spend.is_null(2), "user:102 should be present");
    }

    #[tokio::test]
    async fn test_entity_before_all_writes_is_null() {
        let adapter = make_adapter();

        let batch = adapter
            .get(
                &["user:101".to_string()],
                &["spend_30d".to_string()],
                Some(ts(40)),
                None,
            )
            .await
            .unwrap();

        let spend = batch
            .column_by_name("spend_30d")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!(spend.is_null(0));
    }

    #[tokio::test]
    async fn test_output_preserves_entity_order() {
        let adapter = make_adapter();

        let batch = adapter
            .get(
                &["user:102".to_string(), "user:101".to_string()],
                &["spend_30d".to_string()],
                None,
                None,
            )
            .await
            .unwrap();

        let entity_ids = batch
            .column_by_name("entity_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(entity_ids.value(0), "user:102");
        assert_eq!(entity_ids.value(1), "user:101");
    }

    #[tokio::test]
    async fn test_entity_id_column_always_present() {
        let adapter = make_adapter();
        let batch = adapter
            .get(
                &["user:101".to_string()],
                &["spend_30d".to_string()],
                None,
                None,
            )
            .await
            .unwrap();
        assert!(batch.column_by_name("entity_id").is_some());
    }

    #[tokio::test]
    async fn test_empty_entity_ids_errors() {
        let adapter = MemoryAdapter::new();
        let result = adapter
            .get(&[], &["spend_30d".to_string()], None, None)
            .await;
        assert!(matches!(result, Err(AdapterError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn test_empty_feature_names_errors() {
        let adapter = MemoryAdapter::new();
        let result = adapter
            .get(&["user:101".to_string()], &[], None, None)
            .await;
        assert!(matches!(result, Err(AdapterError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn test_health_always_healthy() {
        let adapter = make_adapter();
        let status = adapter.health().await;
        assert!(status.healthy);
        assert_eq!(status.backend, "memory");
    }
}
