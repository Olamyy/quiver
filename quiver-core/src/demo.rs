use crate::adapters::BackendAdapter;
use crate::adapters::memory::MemoryAdapter;
use crate::proto::quiver::v1::{FeatureColumnSchema, FeatureViewMetadata};
use crate::registry::StaticRegistry;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn seed_demo_data(
    registry: Arc<StaticRegistry>,
    adapter: Arc<MemoryAdapter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut backend_routing = HashMap::new();
    backend_routing.insert("default".to_string(), "memory".to_string());

    let view = FeatureViewMetadata {
        name: "my_feature_view".to_string(),
        entity_type: "user".to_string(),
        entity_key: "user_id".to_string(),
        columns: vec![
            FeatureColumnSchema {
                name: "login_count".to_string(),
                arrow_type: "Int64".to_string(),
                nullable: false,
            },
            FeatureColumnSchema {
                name: "last_purchase_amount".to_string(),
                arrow_type: "Float64".to_string(),
                nullable: false,
            },
        ],
        backend_routing,
        schema_version: 1,
    };
    registry.register(view);

    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("login_count", DataType::Int64, false),
        Field::new("last_purchase_amount", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user_1", "user_2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Float64Array::from(vec![99.99, 149.50])),
        ],
    )?;

    adapter.put(batch).await?;

    Ok(())
}
