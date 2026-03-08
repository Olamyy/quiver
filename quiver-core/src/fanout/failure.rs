use arrow::record_batch::RecordBatch;

use crate::resolver::PartialFailureStrategy;

/// Error type for partial failure detection.
#[derive(Debug, thiserror::Error)]
pub enum PartialFailureError {
    #[error("Partial failure detected: {0}")]
    PartialFailure(String),

    #[error("Missing entities: {0}")]
    MissingEntities(String),

    #[error("Null strategy violation: {0}")]
    NullStrategyViolation(String),
}

/// Detects and reports partial failures in merged results.
///
/// Validates that:
/// 1. Output row count matches requested entity count (no silent data loss)
/// 2. Per-feature null policies are respected (per `null_strategy` config)
/// 3. Null counts align with expected patterns
///
/// # Arguments
///
/// * `requested_entity_count` - Number of entities requested in the original query
/// * `merged_batch` - The result of merging backend results
/// * `per_feature_strategies` - Per-feature null strategy configuration
///
/// # Returns
///
/// `Ok(())` if validation passes, `Err(PartialFailureError)` if issues found
pub fn detect_partial_failure(
    requested_entity_count: usize,
    merged_batch: &RecordBatch,
    per_feature_strategies: &[(String, PartialFailureStrategy)],
) -> Result<(), PartialFailureError> {
    let actual_row_count = merged_batch.num_rows();
    if actual_row_count != requested_entity_count {
        return Err(PartialFailureError::MissingEntities(format!(
            "Row count mismatch: expected {}, got {}",
            requested_entity_count, actual_row_count
        )));
    }

    for (feature_name, strategy) in per_feature_strategies {
        let col_idx = merged_batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == feature_name);

        if col_idx.is_none() {
            continue;
        }

        let col_idx = col_idx.unwrap();
        let column = merged_batch.column(col_idx);
        let null_count = column.null_count();

        match strategy {
            PartialFailureStrategy::Error if null_count > 0 => {
                return Err(PartialFailureError::NullStrategyViolation(format!(
                    "Feature '{}' has {} nulls but null_strategy='error' (policy violation)",
                    feature_name, null_count
                )));
            }
            PartialFailureStrategy::NullFill => {}
            PartialFailureStrategy::ForwardFill if null_count > 0 => {}
            _ => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch_with_nulls(entity_ids: &[&str], values: &[Option<i64>]) -> RecordBatch {
        let entity_array =
            StringArray::from(entity_ids.iter().map(|s| s.to_string()).collect::<Vec<_>>());

        let value_array = Int64Array::from(values.to_vec());

        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("feature1", DataType::Int64, true),
        ]));

        let columns = vec![
            Arc::new(entity_array) as Arc<dyn arrow::array::Array>,
            Arc::new(value_array) as Arc<dyn arrow::array::Array>,
        ];

        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_detect_partial_failure_row_count_mismatch() {
        let batch =
            make_test_batch_with_nulls(&["user:1000", "user:1001"], &[Some(100), Some(200)]);
        let strategies = vec![("feature1".to_string(), PartialFailureStrategy::NullFill)];

        let result = detect_partial_failure(3, &batch, &strategies); // 3 requested but batch has 2
        assert!(result.is_err());
        match result.unwrap_err() {
            PartialFailureError::MissingEntities(msg) => {
                assert!(msg.contains("Row count mismatch"));
            }
            _ => panic!("Expected MissingEntities error"),
        }
    }

    #[test]
    fn test_detect_partial_failure_error_strategy_with_nulls() {
        let batch = make_test_batch_with_nulls(&["user:1000", "user:1001"], &[Some(100), None]);
        let strategies = vec![("feature1".to_string(), PartialFailureStrategy::Error)];

        let result = detect_partial_failure(2, &batch, &strategies);
        assert!(result.is_err());
        match result.unwrap_err() {
            PartialFailureError::NullStrategyViolation(msg) => {
                assert!(msg.contains("null_strategy='error'"));
            }
            _ => panic!("Expected NullStrategyViolation error"),
        }
    }

    #[test]
    fn test_detect_partial_failure_null_fill_strategy_allows_nulls() {
        let batch = make_test_batch_with_nulls(&["user:1000", "user:1001"], &[Some(100), None]);
        let strategies = vec![("feature1".to_string(), PartialFailureStrategy::NullFill)];

        let result = detect_partial_failure(2, &batch, &strategies);
        assert!(result.is_ok());
    }

    #[test]
    fn test_detect_partial_failure_no_nulls_all_strategies() {
        let batch =
            make_test_batch_with_nulls(&["user:1000", "user:1001"], &[Some(100), Some(200)]);

        for strategy in [
            PartialFailureStrategy::Error,
            PartialFailureStrategy::NullFill,
            PartialFailureStrategy::ForwardFill,
        ] {
            let strategies = vec![("feature1".to_string(), strategy)];
            let result = detect_partial_failure(2, &batch, &strategies);
            assert!(result.is_ok());
        }
    }
}
