//! Shared utilities for Arrow RecordBatch operations across all adapters.
//!
//! This module provides common functionality that was previously duplicated
//! across memory, redis, and postgres adapters, including:
//! - ScalarValue enum for type-agnostic value handling
//! - Arrow RecordBatch building utilities
//! - Column type conversion and building
//! - Array value extraction

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};

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
            Self::Timestamp(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
            Self::Null => DataType::Float64, // Default for nulls
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
                    Some(ScalarValue::Boolean(b_val)) => b.append_value(*b_val),
                    _ => b.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(b.finish()) as ArrayRef,
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

        // Rough calculation for Arrow RecordBatch memory usage:
        // - Numeric features: 8 bytes per value (f64/i64)
        // - String features: estimated string length + overhead
        // - Boolean features: 1 byte per value
        // - Entity IDs: estimated as strings
        // - Arrow overhead: ~2x multiplier for buffers, null bitmaps, etc.

        let entity_id_memory = entity_count * (avg_str_len + 4); // String + overhead
        let numeric_feature_memory = entity_count * feature_count * 8; // Assume most features are numeric
        let string_feature_memory = entity_count * (avg_str_len + 4); // Assume some string features

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

        let batch = build_record_batch(&entity_ids, &feature_names, resolved).unwrap();

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

        let batch = build_record_batch(&entity_ids, &feature_names, resolved).unwrap();

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
}
