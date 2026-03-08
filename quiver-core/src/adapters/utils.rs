//! Shared utilities for Arrow RecordBatch operations across all adapters.
//!
//! This module provides common functionality that was previously duplicated
//! across memory, redis, and postgres adapters, including:
//! - ScalarValue enum for type-agnostic value handling
//! - Arrow RecordBatch building utilities
//! - Column type conversion and building
//! - Array value extraction

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, PrimitiveBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::TimestampNanosecondType;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};

use super::AdapterError;

/// A unified scalar value type that can hold any supported feature value.
///
/// This enum provides a common representation for feature values across all
/// adapter implementations, supporting type coercion and null handling.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Float64(f64),
    Int64(i64),
    Utf8(String),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Null,
}

impl ScalarValue {
    /// Get the Arrow DataType corresponding to this scalar value.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Float64(_) => DataType::Float64,
            Self::Int64(_) => DataType::Int64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Boolean(_) => DataType::Boolean,
            Self::Timestamp(_) => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            Self::Null => DataType::Float64,
        }
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Convert to Option<T> for easier null handling.
    pub fn as_option(&self) -> Option<&Self> {
        if self.is_null() { None } else { Some(self) }
    }
}

/// Extract a scalar value from an Arrow array at the specified row index.
///
/// This function handles type detection and conversion from Arrow arrays
/// to the unified ScalarValue representation.
///
/// # Arguments
/// * `array` - The Arrow array to extract from
/// * `row` - The row index (0-based)
///
/// # Returns
/// * `Some(ScalarValue)` - The extracted value or ScalarValue::Null if null
/// * `None` - If the array type is not supported
pub fn extract_scalar(array: &ArrayRef, row: usize) -> Option<ScalarValue> {
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

/// Build an Arrow column from a vector of optional scalar values.
///
/// This function creates the appropriate Arrow array type based on the
/// data type and handles null values and type coercions appropriately.
///
/// # Arguments
/// * `name` - Column name for the resulting field
/// * `dtype` - Target Arrow data type
/// * `values` - Vector of optional scalar values
///
/// # Returns
/// * `Ok((Field, ArrayRef))` - The field definition and array data
/// * `Err(ArrowError)` - If the data type is unsupported
pub fn build_column(
    name: &str,
    dtype: &DataType,
    values: &[Option<ScalarValue>],
) -> Result<(Field, ArrayRef), arrow::error::ArrowError> {
    match dtype {
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
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
            let mut b = Int64Builder::with_capacity(values.len());
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
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 64);
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
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(ScalarValue::Boolean(b_val)) => b.append_value(*b_val),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(b.finish()) as ArrayRef,
            ))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let data_type = DataType::Timestamp(TimeUnit::Nanosecond, tz.clone());
            let mut b = PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(values.len());
            for v in values {
                match v {
                    Some(ScalarValue::Timestamp(dt)) => {
                        b.append_value(dt.timestamp_nanos_opt().unwrap_or(0))
                    }
                    _ => b.append_null(),
                }
            }
            let array_no_tz = b.finish();

            // Cast the ArrayData to have the timezone-aware type
            let array_data = array_no_tz.into_data();
            // Create a new ArrayData with same content but correct data type with timezone
            #[expect(clippy::bind_instead_of_map)]
            let null_bit_buffer = array_data.nulls().and_then(|nb| Some(nb.buffer().clone()));
            let buffers = array_data.buffers().to_vec();
            let child_data = array_data.child_data().to_vec();

            let array_data_with_tz = arrow::array::ArrayData::try_new(
                data_type.clone(),
                array_data.len(),
                null_bit_buffer,
                array_data.offset(),
                buffers,
                child_data,
            )
            .map_err(|e| arrow::error::ArrowError::InvalidArgumentError(e.to_string()))?;

            let array_with_tz =
                arrow::array::PrimitiveArray::<TimestampNanosecondType>::from(array_data_with_tz);

            Ok((
                Field::new(name, data_type, true),
                Arc::new(array_with_tz) as ArrayRef,
            ))
        }
        other => Err(arrow::error::ArrowError::InvalidArgumentError(format!(
            "Unsupported column type for Arrow conversion: {:?}",
            other
        ))),
    }
}

/// Build an Arrow RecordBatch from resolved feature values.
///
/// This function creates a complete RecordBatch with an entity_id column
/// followed by feature columns in the requested order. It handles type
/// detection and null values appropriately.
///
/// # Arguments
/// * `entity_ids` - Entity identifiers (preserved in order)
/// * `feature_names` - Feature names (preserved in order)  
/// * `resolved` - Per-feature vectors of optional scalar values
///
/// # Returns
/// * `Ok(RecordBatch)` - The constructed RecordBatch
/// * `Err(ArrowError)` - If RecordBatch construction fails
///
/// # Schema
/// The resulting RecordBatch has the following schema:
/// - `entity_id: Utf8` (non-nullable) - Entity identifiers
/// - `{feature_name}: {inferred_type}` (nullable) - Feature values
pub fn build_record_batch(
    entity_ids: &[String],
    feature_names: &[String],
    entity_key: &str,
    resolved: Vec<Vec<Option<ScalarValue>>>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<Field> = Vec::with_capacity(feature_names.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(feature_names.len() + 1);

    fields.push(Field::new(entity_key, DataType::Utf8, false));
    let mut id_builder = StringBuilder::with_capacity(entity_ids.len(), entity_ids.len() * 32);
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

pub fn build_record_batch_with_types(
    entity_ids: &[String],
    feature_names: &[String],
    entity_key: &str,
    resolved: Vec<Vec<Option<ScalarValue>>>,
    feature_types: &[DataType],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<Field> = Vec::with_capacity(feature_names.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(feature_names.len() + 1);

    fields.push(Field::new(entity_key, DataType::Utf8, false));
    let mut id_builder = StringBuilder::with_capacity(entity_ids.len(), entity_ids.len() * 32);
    for eid in entity_ids {
        id_builder.append_value(eid);
    }
    columns.push(Arc::new(id_builder.finish()) as ArrayRef);

    for (feat_idx, feat_name) in feature_names.iter().enumerate() {
        let values = &resolved[feat_idx];
        let dtype = if feat_idx < feature_types.len() {
            &feature_types[feat_idx]
        } else {
            &DataType::Utf8
        };

        let (field, array) = build_column(feat_name, dtype, values)?;
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

/// Memory-efficient RecordBatch builder that avoids 2D matrix allocation.
///
/// This approach processes feature data sequentially and builds Arrow columns
/// directly without creating an intermediate `Vec<Vec<Option<ScalarValue>>>` matrix.
/// This significantly reduces memory usage for large batches.
///
/// # Memory Benefits
/// - Eliminates 2D matrix allocation (features × entities × ~32 bytes)
/// - Processes one feature column at a time (streaming approach)
/// - Reduces peak memory usage from O(features × entities) to O(entities)
/// - Better for large-scale inference workloads
///
/// # Usage
/// This function is designed to be called directly with feature data collected
/// from adapters without intermediate storage.
pub fn build_record_batch_streaming(
    entity_ids: &[String],
    feature_names: &[String],
    entity_key: &str,
    mut feature_data_source: impl FnMut(
        usize,
    )
        -> Result<Vec<Option<ScalarValue>>, arrow::error::ArrowError>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<Field> = Vec::with_capacity(feature_names.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(feature_names.len() + 1);

    // Add entity_id column
    fields.push(Field::new(entity_key, DataType::Utf8, false));
    let mut id_builder = StringBuilder::with_capacity(entity_ids.len(), entity_ids.len() * 32);
    for eid in entity_ids {
        id_builder.append_value(eid);
    }
    columns.push(Arc::new(id_builder.finish()) as ArrayRef);

    // Process each feature column sequentially (no 2D matrix!)
    for (feat_idx, feat_name) in feature_names.iter().enumerate() {
        let values = feature_data_source(feat_idx)?;

        // Infer data type from first non-null value
        let dtype = values
            .iter()
            .find_map(|v| v.as_ref())
            .map(|v| v.data_type())
            .unwrap_or(DataType::Float64);

        let (field, array) = build_column(feat_name, &dtype, &values)?;
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

/// Simplified builder for cases where adapters still need to collect feature data.
///
/// This still allocates a 2D matrix but provides a cleaner interface compared to
/// the raw Vec<Vec<Option<ScalarValue>>> approach. For truly memory-efficient
/// processing, use build_record_batch_streaming instead.
pub struct StreamingRecordBatchBuilder {
    entity_ids: Vec<String>,
    feature_names: Vec<String>,
    entity_key: String,
    feature_data: Vec<Vec<Option<ScalarValue>>>,
    feature_types: Vec<DataType>,
}

impl StreamingRecordBatchBuilder {
    /// Create a new builder.
    pub fn new(
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
    ) -> Result<Self, arrow::error::ArrowError> {
        // Pre-allocate feature data arrays
        let feature_data: Vec<Vec<Option<ScalarValue>>> = (0..feature_names.len())
            .map(|_| vec![None; entity_ids.len()])
            .collect();

        let feature_types = vec![DataType::Utf8; feature_names.len()];

        Ok(StreamingRecordBatchBuilder {
            entity_ids: entity_ids.to_vec(),
            feature_names: feature_names.to_vec(),
            entity_key: entity_key.to_string(),
            feature_data,
            feature_types,
        })
    }

    /// Create a new builder with explicit type hints.
    pub fn new_with_types(
        entity_ids: &[String],
        feature_names: &[String],
        entity_key: &str,
        feature_types: Vec<DataType>,
    ) -> Result<Self, arrow::error::ArrowError> {
        let feature_data: Vec<Vec<Option<ScalarValue>>> = (0..feature_names.len())
            .map(|_| vec![None; entity_ids.len()])
            .collect();

        Ok(StreamingRecordBatchBuilder {
            entity_ids: entity_ids.to_vec(),
            feature_names: feature_names.to_vec(),
            entity_key: entity_key.to_string(),
            feature_data,
            feature_types,
        })
    }

    /// Set a feature value for a specific entity.
    pub fn set_value(
        &mut self,
        feature_idx: usize,
        entity_idx: usize,
        value: Option<ScalarValue>,
    ) -> Result<(), arrow::error::ArrowError> {
        if feature_idx >= self.feature_data.len() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "feature_idx {} out of bounds",
                feature_idx
            )));
        }

        if entity_idx >= self.entity_ids.len() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "entity_idx {} out of bounds",
                entity_idx
            )));
        }

        self.feature_data[feature_idx][entity_idx] = value;
        Ok(())
    }

    /// Finish building and create the RecordBatch.
    pub fn finish(self) -> Result<RecordBatch, arrow::error::ArrowError> {
        build_record_batch_with_types(
            &self.entity_ids,
            &self.feature_names,
            &self.entity_key,
            self.feature_data,
            &self.feature_types,
        )
    }
}

/// Common validation patterns used across adapters.
pub mod validation {
    use super::super::AdapterError;

    /// Validate that entity_ids is not empty.
    pub fn validate_entity_ids_not_empty(
        entity_ids: &[String],
        backend_name: &str,
    ) -> Result<(), AdapterError> {
        if entity_ids.is_empty() {
            return Err(AdapterError::invalid(
                backend_name,
                "entity_ids cannot be empty",
            ));
        }
        Ok(())
    }

    /// Validate that feature_names is not empty.
    pub fn validate_feature_names_not_empty(
        feature_names: &[String],
        backend_name: &str,
    ) -> Result<(), AdapterError> {
        if feature_names.is_empty() {
            return Err(AdapterError::invalid(
                backend_name,
                "feature_names cannot be empty",
            ));
        }
        Ok(())
    }

    /// Validate batch size against adapter capabilities.
    pub fn validate_batch_size(
        batch_size: usize,
        max_batch_size: Option<usize>,
        backend_name: &str,
    ) -> Result<(), AdapterError> {
        if let Some(max_batch) = max_batch_size
            && batch_size > max_batch
        {
            return Err(AdapterError::resource_limit_exceeded(
                backend_name,
                format!("batch size {} exceeds maximum {}", batch_size, max_batch),
            ));
        }
        Ok(())
    }

    /// Validate batch size against memory constraints for ML inference workloads.
    ///
    /// This function estimates the memory usage of a batch and validates it against
    /// reasonable limits for ML inference scenarios.
    pub fn validate_memory_constraints(
        entity_count: usize,
        feature_count: usize,
        avg_string_length: Option<usize>,
        max_memory_mb: Option<usize>,
        backend_name: &str,
    ) -> Result<(), AdapterError> {
        let estimated_memory_bytes =
            super::memory::estimate_batch_memory(entity_count, feature_count, avg_string_length);
        let max_memory_bytes = max_memory_mb.unwrap_or(256) * 1024 * 1024;

        if estimated_memory_bytes > max_memory_bytes {
            return Err(AdapterError::resource_limit_exceeded(
                backend_name,
                format!(
                    "Estimated batch memory usage ({} MB) exceeds limit ({} MB). Consider reducing batch size.",
                    estimated_memory_bytes / (1024 * 1024),
                    max_memory_bytes / (1024 * 1024)
                ),
            ));
        }

        Ok(())
    }
}

/// Create a fallback type map for features without cloning strings.
///
/// This helper creates a HashMap<&str, DataType> that maps feature names to DataType::Utf8
/// without allocating new strings, by borrowing from the input slice.
///
/// # Arguments
/// * `feature_names` - List of feature names to create fallback types for
///
/// # Returns
/// HashMap mapping feature names to DataType::Utf8
pub fn create_fallback_types(feature_names: &[String]) -> HashMap<&str, DataType> {
    feature_names
        .iter()
        .map(|name| (name.as_str(), DataType::Utf8))
        .collect()
}

/// Memory management utilities for ML inference workloads.
pub mod memory {

    /// Estimate memory usage for a RecordBatch with given dimensions.
    ///
    /// This provides a rough estimate of memory usage for Arrow RecordBatch
    /// construction, helping prevent out-of-memory conditions in ML inference scenarios.
    ///
    /// # Arguments
    /// * `entity_count` - Number of entities in the batch
    /// * `feature_count` - Number of features per entity
    /// * `avg_string_length` - Average length of string values (if any)
    ///
    /// # Returns
    /// Estimated memory usage in bytes
    pub fn estimate_batch_memory(
        entity_count: usize,
        feature_count: usize,
        avg_string_length: Option<usize>,
    ) -> usize {
        let avg_str_len = avg_string_length.unwrap_or(20);

        let entity_id_memory = entity_count * (avg_str_len + 4);
        let numeric_feature_memory = entity_count * feature_count * 8;
        let string_feature_memory = entity_count * (avg_str_len + 4);

        let total_base_memory = entity_id_memory + numeric_feature_memory + string_feature_memory;

        total_base_memory * 2
    }

    /// Calculate optimal batch size for given memory constraints.
    ///
    /// This function determines the maximum number of entities that can be processed
    /// in a single batch while staying within memory limits.
    ///
    /// # Arguments
    /// * `feature_count` - Number of features per entity
    /// * `max_memory_mb` - Maximum memory limit in megabytes
    /// * `avg_string_length` - Average string length for estimation
    ///
    /// # Returns
    /// Recommended batch size (number of entities)
    pub fn calculate_optimal_batch_size(
        feature_count: usize,
        max_memory_mb: usize,
        avg_string_length: Option<usize>,
    ) -> usize {
        let max_memory_bytes = max_memory_mb * 1024 * 1024;
        let avg_str_len = avg_string_length.unwrap_or(20);

        let bytes_per_entity = ((avg_str_len + 4) + (feature_count * 8) + (avg_str_len + 4)) * 2;

        if bytes_per_entity == 0 {
            return 1000;
        }

        let optimal_entities = max_memory_bytes / bytes_per_entity;

        optimal_entities.clamp(10, 10_000)
    }

    /// Memory pressure handling strategies for batch processing.
    #[derive(Debug, Clone)]
    pub enum MemoryPressureAction {
        /// Continue with current batch size
        Continue,
        /// Reduce batch size to the specified value
        ReduceBatchSize(usize),
        /// Split the request into multiple smaller batches
        SplitRequest(Vec<std::ops::Range<usize>>),
        /// Fail fast due to excessive memory requirements
        FailFast(String),
    }

    /// Determine action to take when memory pressure is detected.
    ///
    /// # Arguments
    /// * `current_memory_usage` - Current estimated memory usage in bytes
    /// * `max_allowed_memory` - Maximum allowed memory in bytes  
    /// * `current_batch_size` - Current number of entities in batch
    ///
    /// # Returns
    /// Recommended action to handle memory pressure
    pub fn handle_memory_pressure(
        current_memory_usage: usize,
        max_allowed_memory: usize,
        current_batch_size: usize,
    ) -> MemoryPressureAction {
        if current_memory_usage <= max_allowed_memory {
            return MemoryPressureAction::Continue;
        }

        if current_memory_usage > max_allowed_memory * 2 {
            return MemoryPressureAction::FailFast(format!(
                "Memory usage ({} MB) exceeds 2x the limit ({} MB)",
                current_memory_usage / (1024 * 1024),
                max_allowed_memory / (1024 * 1024)
            ));
        }

        let reduction_factor = max_allowed_memory as f64 / current_memory_usage as f64;
        let new_batch_size = ((current_batch_size as f64) * reduction_factor) as usize;
        let min_batch_size = 10;

        if new_batch_size >= min_batch_size {
            MemoryPressureAction::ReduceBatchSize(new_batch_size)
        } else {
            let chunks = current_batch_size.div_ceil(min_batch_size);
            let mut ranges = Vec::new();
            for i in 0..chunks {
                let start = i * min_batch_size;
                let end = std::cmp::min(start + min_batch_size, current_batch_size);
                if start < end {
                    ranges.push(start..end);
                }
            }
            MemoryPressureAction::SplitRequest(ranges)
        }
    }
}

/// Convert string arrow type to Arrow DataType.
///
/// This function maps the string arrow type from the config/proto to the actual
/// Arrow DataType enum, enabling schema-driven type validation.
///
/// # Arguments
/// * `arrow_type_str` - String representation of the Arrow type (e.g., "float64", "string")
///
/// # Returns
/// * `Ok(DataType)` - Successfully parsed Arrow type
/// * `Err(String)` - Unsupported or invalid arrow type string
///
/// # Supported Types
/// - "float64" -> DataType::Float64
/// - "int64" -> DataType::Int64  
/// - "string" -> DataType::Utf8
/// - "bool", "boolean" -> DataType::Boolean
/// - "timestamp" -> DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC"))
pub fn parse_arrow_type_string(arrow_type_str: &str) -> Result<DataType, String> {
    crate::types::QuiverType::from_config_string(arrow_type_str)
        .map(|qt| qt.to_arrow())
        .map_err(|e| e.message)
}

/// Extract expected types from FeatureViewMetadata for type validation.
///
/// This function converts the proto FeatureViewMetadata into a map of feature names
/// to Arrow DataTypes that can be used for schema validation.
///
/// # Arguments
/// * `view_metadata` - Feature view metadata from the registry
///
/// # Returns
/// * `Ok(HashMap<String, DataType>)` - Map of feature names to expected types
/// * `Err(String)` - Error parsing arrow types from metadata
pub fn extract_expected_types_from_metadata(
    view_metadata: &crate::proto::quiver::v1::FeatureViewMetadata,
) -> Result<std::collections::HashMap<String, DataType>, String> {
    let mut expected_types = std::collections::HashMap::new();

    for column in &view_metadata.columns {
        let arrow_type = parse_arrow_type_string(&column.arrow_type)?;
        expected_types.insert(column.name.clone(), arrow_type);
    }

    Ok(expected_types)
}

/// Re-export memory estimation for backward compatibility
pub use memory::estimate_batch_memory;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_value_data_types() {
        assert_eq!(ScalarValue::Float64(1.0).data_type(), DataType::Float64);
        assert_eq!(ScalarValue::Int64(1).data_type(), DataType::Int64);
        assert_eq!(
            ScalarValue::Utf8("test".to_string()).data_type(),
            DataType::Utf8
        );
        assert_eq!(ScalarValue::Boolean(true).data_type(), DataType::Boolean);
        assert_eq!(ScalarValue::Null.data_type(), DataType::Float64);
    }

    #[test]
    fn test_scalar_value_null_handling() {
        assert!(ScalarValue::Null.is_null());
        assert!(!ScalarValue::Float64(1.0).is_null());

        assert!(ScalarValue::Null.as_option().is_none());
        assert!(ScalarValue::Float64(1.0).as_option().is_some());
    }

    #[test]
    fn test_build_record_batch_basic() {
        let entity_ids = vec!["entity1".to_string(), "entity2".to_string()];
        let feature_names = vec!["feature1".to_string()];
        let resolved = vec![vec![
            Some(ScalarValue::Float64(1.0)),
            Some(ScalarValue::Float64(2.0)),
        ]];

        let batch = build_record_batch(&entity_ids, &feature_names, "entity_id", resolved).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "entity_id");
        assert_eq!(batch.schema().field(1).name(), "feature1");
    }

    #[test]
    fn test_build_record_batch_with_nulls() {
        let entity_ids = vec!["entity1".to_string(), "entity2".to_string()];
        let feature_names = vec!["feature1".to_string()];
        let resolved = vec![vec![Some(ScalarValue::Float64(1.0)), None]];

        let batch = build_record_batch(&entity_ids, &feature_names, "entity_id", resolved).unwrap();

        assert_eq!(batch.num_rows(), 2);

        let feature_col = batch.column(1);
        assert!(!feature_col.is_null(0));
        assert!(feature_col.is_null(1));
    }

    #[test]
    fn test_build_column_type_coercion() {
        let values = vec![
            Some(ScalarValue::Int64(10)),
            Some(ScalarValue::Float64(20.5)),
        ];

        let (field, array) = build_column("test", &DataType::Float64, &values).unwrap();

        assert_eq!(field.data_type(), &DataType::Float64);
        let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(float_array.value(0), 10.0);
        assert_eq!(float_array.value(1), 20.5);
    }

    #[test]
    fn test_batch_conversion_strict_mode() {
        let raw_values = vec![
            Some("1.5".to_string()),
            Some("2.7".to_string()),
            None,
            Some("3.14".to_string()),
        ];

        let result =
            convert_batch_to_scalars(&raw_values, &DataType::Float64, "test", "test_feature");

        assert!(result.is_ok());
        let scalars = result.unwrap();
        assert_eq!(scalars.len(), 4);

        if let Some(ScalarValue::Float64(v)) = &scalars[0] {
            assert_eq!(*v, 1.5);
        } else {
            panic!("Expected Float64(1.5)");
        }

        assert!(scalars[2].is_none());
    }

    #[test]
    fn test_batch_conversion_strict_mode_failure() {
        let raw_values = vec![Some("1.5".to_string()), Some("not_a_number".to_string())];

        let result =
            convert_batch_to_scalars(&raw_values, &DataType::Float64, "test", "test_feature");

        assert!(result.is_err());
    }

    #[test]
    fn test_batch_conversion_permissive_mode() {
        let raw_values = vec![Some("42.7".to_string()), Some("43".to_string()), None];

        let result =
            convert_batch_to_scalars(&raw_values, &DataType::Int64, "test", "test_feature");

        assert!(result.is_ok());
        let scalars = result.unwrap();

        if let Some(ScalarValue::Int64(v)) = &scalars[0] {
            assert_eq!(*v, 42);
        } else {
            panic!("Expected Int64(42)");
        }
    }

    #[test]
    fn test_batch_conversion_empty() {
        let raw_values: Vec<Option<String>> = vec![];

        let result =
            convert_batch_to_scalars(&raw_values, &DataType::Float64, "test", "test_feature");

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_arrow_type_string() {
        assert_eq!(
            parse_arrow_type_string("float64").unwrap(),
            DataType::Float64
        );
        assert_eq!(
            parse_arrow_type_string("DOUBLE").unwrap(),
            DataType::Float64
        );
        assert_eq!(parse_arrow_type_string("int64").unwrap(), DataType::Int64);
        assert_eq!(parse_arrow_type_string("LONG").unwrap(), DataType::Int64);
        assert_eq!(parse_arrow_type_string("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_arrow_type_string("UTF8").unwrap(), DataType::Utf8);
        assert_eq!(parse_arrow_type_string("bool").unwrap(), DataType::Boolean);
        assert_eq!(
            parse_arrow_type_string("BOOLEAN").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            parse_arrow_type_string("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        assert!(parse_arrow_type_string("unsupported").is_err());

        let empty_result = parse_arrow_type_string("");
        assert!(empty_result.is_err());
        assert!(
            empty_result
                .unwrap_err()
                .contains("Unsupported arrow type: ''")
        );

        let whitespace_result = parse_arrow_type_string("  ");
        assert!(whitespace_result.is_err());
        assert!(
            whitespace_result
                .unwrap_err()
                .contains("Unsupported arrow type: '  '")
        );
    }

    #[test]
    fn test_extract_expected_types_from_metadata() {
        use crate::proto::quiver::v1::{FeatureColumnSchema, FeatureViewMetadata};

        let metadata = FeatureViewMetadata {
            name: "test_view".to_string(),
            entity_type: "user".to_string(),
            entity_key: "user_id".to_string(),
            columns: vec![
                FeatureColumnSchema {
                    name: "score".to_string(),
                    arrow_type: "float64".to_string(),
                    nullable: true,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "count".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                    fallback_source: String::new(),
                },
                FeatureColumnSchema {
                    name: "active".to_string(),
                    arrow_type: "bool".to_string(),
                    nullable: true,
                    fallback_source: String::new(),
                },
            ],
            backend_routing: std::collections::HashMap::new(),
            schema_version: 1,
        };

        let expected_types = extract_expected_types_from_metadata(&metadata).unwrap();

        assert_eq!(expected_types.len(), 3);
        assert_eq!(*expected_types.get("score").unwrap(), DataType::Float64);
        assert_eq!(*expected_types.get("count").unwrap(), DataType::Int64);
        assert_eq!(*expected_types.get("active").unwrap(), DataType::Boolean);
    }
}

/// Convert a raw string value to a ScalarValue with optional type validation.
///
/// This function provides flexible type conversion that can either:
/// 1. Validate that the value matches the expected schema type (strict mode)
/// 2. Convert the value to the expected type with best effort (permissive mode)
///
/// # Arguments
/// * `raw_value` - The raw string value from the backend
/// * `expected_type` - The Arrow DataType from the schema
/// * `backend_name` - Name of the backend adapter (for error messages)
/// * `feature_name` - Name of the feature (for error messages)
///
/// # Returns
/// * `Ok(ScalarValue)` - Successfully converted value
/// * `Err(AdapterError)` - Type conversion failed
///
/// # Examples
/// ```
/// use quiver_core::adapters::utils::convert_raw_to_scalar;
/// use arrow::datatypes::DataType;
///
/// // Float to int conversion - converts "42.5" to 42
/// let result = convert_raw_to_scalar("42.5", &DataType::Int64, "redis", "count");
/// assert!(result.is_ok()); // Succeeds with truncation
///
/// // String to float conversion  
/// let result = convert_raw_to_scalar("3.14", &DataType::Float64, "redis", "score");
/// assert!(result.is_ok()); // Succeeds
///
/// // Invalid conversion - fails with clear error
/// let result = convert_raw_to_scalar("not_a_number", &DataType::Float64, "redis", "score");
/// assert!(result.is_err()); // Fails
/// ```
pub fn convert_raw_to_scalar(
    raw_value: &str,
    expected_type: &DataType,
    backend_name: &str,
    feature_name: &str,
) -> Result<ScalarValue, AdapterError> {
    match expected_type {
        DataType::Float64 => {
            if let Ok(f) = raw_value.parse::<f64>() {
                Ok(ScalarValue::Float64(f))
            } else if let Ok(i) = raw_value.parse::<i64>() {
                Ok(ScalarValue::Float64(i as f64))
            } else {
                Err(AdapterError::invalid(
                    backend_name,
                    format!(
                        "Feature '{}' cannot convert '{}' to float64",
                        feature_name, raw_value
                    ),
                ))
            }
        }
        DataType::Int64 => {
            if let Ok(i) = raw_value.parse::<i64>() {
                Ok(ScalarValue::Int64(i))
            } else if let Ok(f) = raw_value.parse::<f64>() {
                Ok(ScalarValue::Int64(f as i64))
            } else {
                Err(AdapterError::invalid(
                    backend_name,
                    format!(
                        "Feature '{}' cannot convert '{}' to int64",
                        feature_name, raw_value
                    ),
                ))
            }
        }
        DataType::Utf8 => Ok(ScalarValue::Utf8(raw_value.to_string())),
        DataType::Boolean => raw_value
            .to_lowercase()
            .parse::<bool>()
            .map(ScalarValue::Boolean)
            .map_err(|_| {
                AdapterError::invalid(
                    backend_name,
                    format!(
                        "Feature '{}' cannot convert '{}' to boolean",
                        feature_name, raw_value
                    ),
                )
            }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => raw_value
            .parse::<DateTime<Utc>>()
            .map(ScalarValue::Timestamp)
            .map_err(|_| {
                AdapterError::invalid(
                    backend_name,
                    format!(
                        "Feature '{}' cannot convert '{}' to timestamp",
                        feature_name, raw_value
                    ),
                )
            }),
        other => Err(AdapterError::invalid(
            backend_name,
            format!(
                "Feature '{}' has unsupported Arrow type: {:?}. Supported types: float64, int64, string, boolean, timestamp",
                feature_name, other
            ),
        )),
    }
}

/// Convert a batch of raw string values to ScalarValues.
///
/// This function provides optimized batch processing for type conversion.
///
/// # Arguments
/// * `raw_values` - Vector of optional raw string values from the backend
/// * `expected_type` - The Arrow DataType from the schema
/// * `backend_name` - Name of the backend adapter (for error messages)
/// * `feature_name` - Name of the feature (for error messages)
///
/// # Returns
/// * `Ok(Vec<Option<ScalarValue>>)` - Successfully converted values
/// * `Err(AdapterError)` - Type conversion failed
pub fn convert_batch_to_scalars(
    raw_values: &[Option<String>],
    expected_type: &DataType,
    backend_name: &str,
    feature_name: &str,
) -> Result<Vec<Option<ScalarValue>>, AdapterError> {
    if raw_values.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(raw_values.len());

    for raw_value in raw_values {
        let scalar_value = match raw_value {
            Some(raw) => Some(convert_raw_to_scalar(
                raw,
                expected_type,
                backend_name,
                feature_name,
            )?),
            None => None,
        };
        result.push(scalar_value);
    }

    Ok(result)
}
