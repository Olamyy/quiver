use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use quiver_core::fanout::{FanoutMerger, PartialFailureError, detect_partial_failure};
use quiver_core::resolver::PartialFailureStrategy;
use std::sync::Arc;

fn make_backend_batch(entity_ids: &[&str], feature_name: &str, values: &[i64]) -> RecordBatch {
    let entity_array =
        StringArray::from(entity_ids.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let value_array = Int64Array::from(values.to_vec());

    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new(feature_name, DataType::Int64, false),
    ]));

    let columns = vec![
        Arc::new(entity_array) as Arc<dyn arrow::array::Array>,
        Arc::new(value_array) as Arc<dyn arrow::array::Array>,
    ];

    RecordBatch::try_new(schema, columns).unwrap()
}

#[test]
fn test_merge_two_backends_same_order() {
    let entity_ids = vec!["user:1000".to_string(), "user:1001".to_string()];

    let backend1 = make_backend_batch(&["user:1000", "user:1001"], "spend_30d", &[100, 200]);
    let backend2 = make_backend_batch(&["user:1000", "user:1001"], "is_premium", &[1, 0]);

    let result = FanoutMerger::merge(
        &entity_ids,
        &[backend1, backend2],
        &["spend_30d".to_string(), "is_premium".to_string()],
    );

    assert!(result.is_ok());
    let merged = result.unwrap();

    assert_eq!(merged.num_rows(), 2);
    assert_eq!(merged.num_columns(), 3);

    let schema = merged.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"entity_id"));
    assert!(names.contains(&"spend_30d"));
    assert!(names.contains(&"is_premium"));
}

#[test]
fn test_merge_preserves_entity_order() {
    let entity_ids = vec![
        "user:3000".to_string(),
        "user:1000".to_string(),
        "user:2000".to_string(),
    ];

    let backend1 = make_backend_batch(
        &["user:3000", "user:1000", "user:2000"],
        "score",
        &[30, 10, 20],
    );

    let result = FanoutMerger::merge(&entity_ids, &[backend1], &["score".to_string()]);

    assert!(result.is_ok());
    let merged = result.unwrap();

    assert_eq!(merged.num_rows(), 3);

    let entity_col = merged
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(entity_col.value(0), "user:3000");
    assert_eq!(entity_col.value(1), "user:1000");
    assert_eq!(entity_col.value(2), "user:2000");
}

#[test]
fn test_merge_multiple_features_from_multiple_backends() {
    let entity_ids = vec!["user:100".to_string(), "user:200".to_string()];

    // Backend 1: spend_30d, purchase_count
    let backend1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("spend_30d", DataType::Int64, false),
            Field::new("purchase_count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["user:100", "user:200"]))
                as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![1000, 2000])) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![50, 75])) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap();

    let backend2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("is_premium", DataType::Int64, false),
            Field::new("loyalty_score", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["user:100", "user:200"]))
                as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![1, 0])) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![500, 300])) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap();

    let result = FanoutMerger::merge(
        &entity_ids,
        &[backend1, backend2],
        &[
            "spend_30d".to_string(),
            "purchase_count".to_string(),
            "is_premium".to_string(),
            "loyalty_score".to_string(),
        ],
    );

    assert!(result.is_ok());
    let merged = result.unwrap();

    assert_eq!(merged.num_rows(), 2);
    assert_eq!(merged.num_columns(), 5);

    let schema = merged.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"entity_id"));
    assert!(names.contains(&"spend_30d"));
    assert!(names.contains(&"purchase_count"));
    assert!(names.contains(&"is_premium"));
    assert!(names.contains(&"loyalty_score"));
}

#[test]
fn test_partial_failure_detection_row_count_mismatch() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("feature1", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["user:100"])) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![100])) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap();

    let strategies = vec![("feature1".to_string(), PartialFailureStrategy::NullFill)];

    let result = detect_partial_failure(2, &batch, &strategies);
    assert!(result.is_err());
}

#[test]
fn test_partial_failure_detection_error_strategy_rejects_nulls() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("critical_feature", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["user:100", "user:200"]))
                as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![Some(100), None])) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap();

    let strategies = vec![(
        "critical_feature".to_string(),
        PartialFailureStrategy::Error,
    )];

    let result = detect_partial_failure(2, &batch, &strategies);
    assert!(result.is_err());
    match result.unwrap_err() {
        PartialFailureError::NullStrategyViolation(msg) => {
            assert!(msg.contains("null_strategy='error'"));
        }
        _ => panic!("Expected NullStrategyViolation"),
    }
}

#[test]
fn test_partial_failure_detection_null_fill_allows_nulls() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("optional_feature", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["user:100", "user:200"]))
                as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(vec![Some(100), None])) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap();

    let strategies = vec![(
        "optional_feature".to_string(),
        PartialFailureStrategy::NullFill,
    )];

    let result = detect_partial_failure(2, &batch, &strategies);
    assert!(result.is_ok());
}

#[test]
fn test_merge_single_backend_single_feature() {
    let entity_ids = vec!["user:1".to_string(), "user:2".to_string()];

    let backend = make_backend_batch(&["user:1", "user:2"], "score", &[100, 200]);

    let result = FanoutMerger::merge(&entity_ids, &[backend], &["score".to_string()]);

    assert!(result.is_ok());
    let merged = result.unwrap();
    assert_eq!(merged.num_rows(), 2);
    assert_eq!(merged.num_columns(), 2); // entity_id + score
}

#[test]
fn test_merge_empty_entity_list() {
    let entity_ids: Vec<String> = vec![];
    let backend_batches = vec![];
    let feature_names: Vec<String> = vec![];

    // Should handle empty gracefully
    let _result = FanoutMerger::merge(&entity_ids, &backend_batches, &feature_names);
}
