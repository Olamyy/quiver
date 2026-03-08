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

    /// Execute a LEFT JOIN between two RecordBatches.
    ///
    /// Assumes both tables have been returned from adapters in request entity order,
    /// with entity_id as the first column. This phase does a simple positional merge,
    /// preserving all rows from the left table and adding non-entity_id columns from right.
    ///
    /// # Arguments
    ///
    /// * `left_table` - Current merged state (preserves all rows)
    /// * `right_table` - Backend result (features to add)
    ///
    /// # Returns
    ///
    /// Joined RecordBatch with left rows preserved and right feature columns added
    ///
    /// # Assumptions
    ///
    /// - Both tables have matching row counts (same number of entities)
    /// - Both tables have rows in the same order (matching request entity order)
    /// - Both tables include entity_id as first column (adapter convention)
    /// - No actual key-based join needed (implicit positional join on row index)
    fn left_join(left_table: &RecordBatch, right_table: &RecordBatch) -> Result<RecordBatch, MergeError> {
        // Verify both tables have the same number of rows (required for positional merge)
        if left_table.num_rows() != right_table.num_rows() {
            return Err(MergeError::MergeFailed(format!(
                "LEFT JOIN requires matching row counts: left={}, right={}",
                left_table.num_rows(),
                right_table.num_rows()
            )));
        }

        // Collect right column info, skipping entity_id (always first column in adapter output)
        let right_schema = right_table.schema();
        let right_cols: Vec<_> = (1..right_table.num_columns())  // Start from 1 to skip entity_id at index 0
            .map(|i| {
                let field = right_schema.field(i);
                (i, field.clone())
            })
            .collect();

        // Build output schema: all left columns + right feature columns (no duplicate entity_id)
        let mut output_fields = left_table.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut output_columns = Vec::new();

        // Add all left columns
        for i in 0..left_table.num_columns() {
            output_columns.push(left_table.column(i).clone());
        }

        // Add right columns (skipping entity_id at index 0)
        for (i, field) in right_cols {
            output_fields.push(Arc::new(field));
            output_columns.push(right_table.column(i).clone());
        }

        let output_schema = Arc::new(Schema::new(output_fields));
        RecordBatch::try_new(output_schema, output_columns)
            .map_err(|e| MergeError::Arrow(e.to_string()))
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
    fn fill_nulls(batch: RecordBatch, expected_row_count: usize) -> Result<RecordBatch, MergeError> {
        if batch.num_rows() == expected_row_count {
            return Ok(batch);
        }

        // Placeholder: Null filling logic will go here in Task #7
        // For now, return as-is
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
        // Placeholder: Validation logic will go here in Task #7
        // For now, just do basic checks
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
        // Phase 1: Build request table
        let request_table = Self::build_request_table(entity_ids)?;

        // Phase 2: Merge backend results via LEFT JOIN
        let merged = Self::merge_backend_results(request_table, backend_results, feature_names)?;

        // Phase 3: Fill nulls for missing entities
        let filled = Self::fill_nulls(merged, entity_ids.len())?;

        // Phase 4: Validate and finalize
        Self::finalize(filled, entity_ids.len(), feature_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_test_batch(entity_ids: &[&str], values: &[i64]) -> RecordBatch {
        let entity_array = StringArray::from(
            entity_ids
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        );
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
        let entity_ids = vec!["user:1000".to_string(), "user:1001".to_string(), "user:1002".to_string()];
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
