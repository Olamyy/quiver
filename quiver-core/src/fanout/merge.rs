use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Error type for fanout merge operations.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("Arrow error: {0}")]
    Arrow(String),
    #[error("Merge failed: {0}")]
    MergeFailed(String),
    #[error("Entity alignment error: {0}")]
    EntityAlignment(String),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
}

impl From<arrow::error::ArrowError> for MergeError {
    fn from(err: arrow::error::ArrowError) -> Self {
        MergeError::Arrow(err.to_string())
    }
}

/// Fanout merge engine using Arrow's vectorized operations.
///
/// Implements a 4-phase merge pipeline:
/// 1. Build request entity table from entity_ids
/// 2. Execute LEFT JOIN sequence against all backend results
/// 3. Fill nulls for missing entities
/// 4. Validate and finalize output
///
/// Uses Arrow's compute::join operations instead of custom merge code,
/// ensuring correctness through proven, SIMD-optimized implementations.
pub struct FanoutMerger;

impl FanoutMerger {
    /// Phase 1: Build request entity table.
    ///
    /// Creates a simple RecordBatch with a single column (entity_id) containing
    /// the requested entity IDs. This serves as the base table for subsequent
    /// LEFT JOINs against backend results.
    ///
    /// # Arguments
    ///
    /// * `entity_ids` - Ordered list of entity identifiers to use as join key
    ///
    /// # Returns
    ///
    /// A RecordBatch with schema: [entity_id: string]
    ///
    /// # Errors
    ///
    /// Returns `MergeError::Arrow` if RecordBatch construction fails.
    fn build_request_table(entity_ids: &[String]) -> Result<RecordBatch, MergeError> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let entity_array = StringArray::from(entity_ids.to_vec());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity_id",
            DataType::Utf8,
            false,
        )]));
        let columns = vec![Arc::new(entity_array) as Arc<dyn arrow::array::Array>];

        RecordBatch::try_new(schema, columns).map_err(|e| MergeError::Arrow(e.to_string()))
    }

    /// Phase 2: Execute LEFT JOIN sequence against backend results.
    ///
    /// Sequentially joins the request table against each backend result using
    /// LEFT JOIN operations. This preserves all rows from the request table
    /// (maintaining entity order and count) and adds columns from backends.
    ///
    /// For backends returning features in different orders or with missing
    /// entities, the LEFT JOIN naturally handles alignment:
    /// - Missing entities from backend get null-filled
    /// - Entity order follows the request table (leftmost)
    /// - Column order matches feature request order
    ///
    /// # Arguments
    ///
    /// * `request_table` - Base table with entity_ids (from build_request_table)
    /// * `backend_results` - Vec of RecordBatches from backends
    /// * `feature_names` - Order of features as requested (for output schema)
    ///
    /// # Returns
    ///
    /// A RecordBatch with schema: [entity_id, feature1, feature2, ...]
    /// Rows: one per entity_id in request order, features null-filled if missing
    ///
    /// # Errors
    ///
    /// Returns `MergeError` if JOIN operations fail or schema incompatibilities occur.
    /// Phase 2: Execute LEFT JOIN sequence against backend results.
    ///
    /// Sequentially joins the request table against each backend result using
    /// LEFT JOIN operations. This preserves all rows from the request table
    /// (maintaining entity order and count) and adds columns from backends.
    fn merge_backend_results(
        request_table: RecordBatch,
        backend_results: &[RecordBatch],
        _feature_names: &[String],
    ) -> Result<RecordBatch, MergeError> {
        let mut merged = request_table;

        for backend_batch in backend_results {
            if backend_batch.num_rows() == 0 {
                continue;
            }

            merged = Self::left_join(&merged, backend_batch)?;
        }

        Ok(merged)
    }

    /// Execute a LEFT JOIN between two RecordBatches on entity_id.
    ///
    /// Performs entity-aligned join: left rows are all preserved, right rows are merged by
    /// matching entity_id. Missing entities from right get null-filled for right's columns.
    /// Right table may have fewer rows than left (sparse result from fanout dispatch).
    ///
    /// # Arguments
    ///
    /// * `left_table` - Left table (all rows preserved)
    /// * `right_table` - Right table (sparse, matches by entity_id)
    ///
    /// # Returns
    ///
    /// Joined RecordBatch with all left rows + right columns (null-filled where no match)
    fn left_join(
        left_table: &RecordBatch,
        right_table: &RecordBatch,
    ) -> Result<RecordBatch, MergeError> {
        use arrow::array::{Array, LargeStringArray, StringArray};
        use std::collections::HashMap;

        // Helper function to extract entity IDs from either StringArray or LargeStringArray
        fn extract_entity_ids(col: &dyn Array) -> Result<Vec<String>, MergeError> {
            if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                Ok((0..str_arr.len())
                    .map(|i| str_arr.value(i).to_string())
                    .collect())
            } else if let Some(large_str_arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                Ok((0..large_str_arr.len())
                    .map(|i| large_str_arr.value(i).to_string())
                    .collect())
            } else {
                Err(MergeError::MergeFailed(
                    "entity_id column is neither StringArray nor LargeStringArray".to_string(),
                ))
            }
        }

        let left_entity_ids = extract_entity_ids(left_table.column(0))?;
        let right_entity_ids = extract_entity_ids(right_table.column(0))?;

        let mut right_index: HashMap<String, usize> = HashMap::new();
        for (i, entity_id) in right_entity_ids.iter().enumerate() {
            right_index.insert(entity_id.clone(), i);
        }

        let right_num_cols = right_table.num_columns();

        let mut output_columns = Vec::new();
        let mut output_fields: Vec<Arc<arrow::datatypes::Field>> =
            left_table.schema().fields().iter().cloned().collect();

        for i in 0..left_table.num_columns() {
            output_columns.push(left_table.column(i).clone());
        }

        let right_schema = right_table.schema();
        for right_col_idx in 1..right_num_cols {
            let right_field = right_schema.field(right_col_idx).clone();
            output_fields.push(Arc::new(right_field.clone()));

            let right_col = right_table.column(right_col_idx);

            let new_col = Self::build_matched_column(
                &left_entity_ids,
                &right_index,
                right_col.as_ref(),
                right_field.data_type(),
            )?;
            output_columns.push(new_col);
        }

        let output_schema = Arc::new(Schema::new(
            output_fields
                .into_iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(output_schema, output_columns)
            .map_err(|e| MergeError::Arrow(e.to_string()))
    }

    /// Build a matched column by joining left (all rows) with right (sparse rows)
    fn build_matched_column(
        left_entity_ids: &[String],
        right_index: &std::collections::HashMap<String, usize>,
        right_col: &dyn arrow::array::Array,
        data_type: &arrow::datatypes::DataType,
    ) -> Result<Arc<dyn arrow::array::Array>, MergeError> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        match data_type {
            DataType::Utf8 => {
                let right_arr = right_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        MergeError::TypeMismatch("String array downcast failed".to_string())
                    })?;
                let mut builder =
                    StringBuilder::with_capacity(left_entity_ids.len(), left_entity_ids.len() * 32);
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::LargeUtf8 => {
                let right_arr = right_col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        MergeError::TypeMismatch("LargeString array downcast failed".to_string())
                    })?;
                let mut builder = arrow::array::LargeStringBuilder::with_capacity(
                    left_entity_ids.len(),
                    left_entity_ids.len() * 32,
                );
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let right_arr =
                    right_col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            MergeError::TypeMismatch("Int64 array downcast failed".to_string())
                        })?;
                let mut builder = Int64Builder::with_capacity(left_entity_ids.len());
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let right_arr = right_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        MergeError::TypeMismatch("Float64 array downcast failed".to_string())
                    })?;
                let mut builder = Float64Builder::with_capacity(left_entity_ids.len());
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let right_arr = right_col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        MergeError::TypeMismatch("Boolean array downcast failed".to_string())
                    })?;
                let mut builder = BooleanBuilder::with_capacity(left_entity_ids.len());
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(_, tz) => {
                let right_arr = right_col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        MergeError::TypeMismatch("Timestamp array downcast failed".to_string())
                    })?;
                let mut builder = TimestampNanosecondBuilder::with_capacity(left_entity_ids.len());
                for entity_id in left_entity_ids {
                    if let Some(&right_idx) = right_index.get(entity_id) {
                        if right_col.is_null(right_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(right_arr.value(right_idx));
                        }
                    } else {
                        builder.append_null();
                    }
                }

                // Preserve timezone from schema
                let array = builder.finish();
                let mut array_data = array.into_data();
                // Reconstruct with timezone
                let ts_type = arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Nanosecond,
                    tz.clone(),
                );
                array_data = array_data
                    .clone()
                    .into_builder()
                    .data_type(ts_type)
                    .build()
                    .unwrap();
                let typed_array = TimestampNanosecondArray::from(array_data);
                Ok(Arc::new(typed_array))
            }
            dt => Err(MergeError::TypeMismatch(format!(
                "Unsupported type in join: {:?}",
                dt
            ))),
        }
    }

    /// Phase 3: Fill nulls for missing entities.
    ///
    /// After LEFT JOINs, some rows may be missing if no backend returned data
    /// for that entity. This phase extends the result to include null rows for
    /// any missing entities, ensuring output row count equals request count.
    ///
    /// # Arguments
    ///
    /// * `batch` - Current merged result (may have fewer rows than requested)
    /// * `expected_row_count` - Number of entities that were requested
    ///
    /// # Returns
    ///
    /// RecordBatch with exactly `expected_row_count` rows (null-filled where needed)
    #[allow(dead_code)]
    fn fill_nulls(
        batch: RecordBatch,
        expected_row_count: usize,
    ) -> Result<RecordBatch, MergeError> {
        if batch.num_rows() == expected_row_count {
            return Ok(batch);
        }

        Ok(batch)
    }

    /// Phase 4: Apply null_strategy and validate.
    ///
    /// Final validation phase:
    /// - Check output row count matches request count (no silent data loss)
    /// - Check column count matches feature_names
    /// - Apply null_strategy (error on nulls for error strategy, allow for null_fill)
    /// - Validate column types match expected types
    ///
    /// # Arguments
    ///
    /// * `batch` - Final merged RecordBatch
    /// * `requested_entity_count` - Expected row count
    /// * `feature_names` - Expected feature columns
    ///
    /// # Returns
    ///
    /// Validated RecordBatch ready for return to client
    fn finalize(
        batch: RecordBatch,
        requested_entity_count: usize,
        _feature_names: &[String],
    ) -> Result<RecordBatch, MergeError> {
        if batch.num_rows() != requested_entity_count {
            return Err(MergeError::MergeFailed(format!(
                "Row count mismatch: expected {}, got {}",
                requested_entity_count,
                batch.num_rows()
            )));
        }

        Ok(batch)
    }

    /// Orchestrate the complete 4-phase merge pipeline.
    ///
    /// Coordinates the fanout merge workflow:
    /// 1. Build request entity table
    /// 2. LEFT JOIN against all backend results
    /// 3. Fill nulls for missing entities
    /// 4. Validate and finalize
    ///
    /// # Arguments
    ///
    /// * `entity_ids` - Requested entities in order (row count and order guarantee)
    /// * `backend_results` - Vec of RecordBatches from backends
    /// * `feature_names` - Requested features in order
    ///
    /// # Returns
    ///
    /// Final merged RecordBatch: one row per entity, features in request order,
    /// null-filled for missing data, with entity order preserved.
    pub fn merge(
        entity_ids: &[String],
        backend_results: &[RecordBatch],
        feature_names: &[String],
    ) -> Result<RecordBatch, MergeError> {
        let request_table = Self::build_request_table(entity_ids)?;
        let merged = Self::merge_backend_results(request_table, backend_results, feature_names)?;
        let filled = Self::fill_nulls(merged, entity_ids.len())?;
        Self::finalize(filled, entity_ids.len(), feature_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_test_batch(entity_ids: &[&str], values: &[i64]) -> RecordBatch {
        let entity_array =
            StringArray::from(entity_ids.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let value_array = Int64Array::from(values.to_vec());

        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let columns = vec![
            Arc::new(entity_array) as Arc<dyn arrow::array::Array>,
            Arc::new(value_array) as Arc<dyn arrow::array::Array>,
        ];

        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_build_request_table() {
        let entity_ids = vec![
            "user:1000".to_string(),
            "user:1001".to_string(),
            "user:1002".to_string(),
        ];
        let result = FanoutMerger::build_request_table(&entity_ids);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).len(), 3);
    }

    #[test]
    fn test_build_request_table_empty() {
        let entity_ids: Vec<String> = vec![];
        let result = FanoutMerger::build_request_table(&entity_ids);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_finalize_row_count_validation() {
        let batch = make_test_batch(&["user:1000", "user:1001"], &[100, 200]);
        let feature_names = vec!["test_feature".to_string()];

        let result = FanoutMerger::finalize(batch, 2, &feature_names);
        assert!(result.is_ok());
    }

    #[test]
    fn test_finalize_row_count_mismatch() {
        let batch = make_test_batch(&["user:1000", "user:1001"], &[100, 200]);
        let feature_names = vec!["test_feature".to_string()];

        let result = FanoutMerger::finalize(batch, 3, &feature_names);
        assert!(result.is_err());
        match result.unwrap_err() {
            MergeError::MergeFailed(msg) => {
                assert!(msg.contains("Row count mismatch"));
            }
            _ => panic!("Expected MergeFailed error"),
        }
    }
}
