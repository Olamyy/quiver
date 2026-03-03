use arrow::array::{Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use futures::StreamExt;
use prost::Message;
use quiver_core::adapters::BackendAdapter;
use quiver_core::adapters::memory::MemoryAdapter;
use quiver_core::proto::quiver::v1::{EntityKey, FeatureRequest};
use quiver_core::registry::StaticRegistry;
use quiver_core::resolver::Resolver;
use quiver_core::server::QuiverFlightServer;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::test]
async fn test_ingestion_retrieval_loop() {
    let registry = Arc::new(StaticRegistry::new());
    let memory_adapter: Arc<dyn BackendAdapter> = Arc::new(MemoryAdapter::new());
    let resolver = Arc::new(Resolver::new(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>
    ));
    resolver.register_adapter("memory".to_string(), memory_adapter);

    let service = QuiverFlightServer::new(resolver.clone());

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(addr).unwrap();
    let real_addr = listener.local_addr().unwrap();
    drop(listener);

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
            .serve_with_shutdown(real_addr, async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", real_addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // 3. do_put (Ingest)
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("entity_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["u1", "u2"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0])),
        ],
    )
    .unwrap();

    let descriptor = FlightDescriptor::new_path(vec!["memory".to_string()]);
    let encoder = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::once(async move { Ok(batch) }));

    let put_stream = encoder.map(|res| res.unwrap());
    client.do_put(put_stream).await.unwrap();

    // 4. do_get (Retrieve)
    let mut backend_routing = std::collections::HashMap::new();
    backend_routing.insert("val".to_string(), "memory".to_string());

    registry.register(quiver_core::proto::quiver::v1::FeatureViewMetadata {
        name: "test_view".to_string(),
        entity_type: "user".to_string(),
        entity_key: "entity_id".to_string(),
        columns: vec![quiver_core::proto::quiver::v1::FeatureColumnSchema {
            name: "val".to_string(),
            arrow_type: "float64".to_string(),
            nullable: true,
        }],
        backend_routing,
        schema_version: 1,
    });

    let feature_request = FeatureRequest {
        feature_view: "test_view".to_string(),
        feature_names: vec!["val".to_string()],
        entities: vec![EntityKey {
            entity_id: "u1".to_string(),
            entity_type: "user".to_string(),
        }],
        as_of: None,
        freshness: None,
        context: None,
        output: None,
    };
    let ticket = Ticket {
        ticket: feature_request.encode_to_vec().into(),
    };

    let response = client.do_get(ticket).await.unwrap();
    let stream = response.into_inner();
    let mut reader =
        arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream.map(|res| {
            res.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))
        }));

    let result_batch = reader.next().await.unwrap().unwrap();
    assert_eq!(result_batch.num_rows(), 1);
    let val_col = result_batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(val_col.value(0), 10.0);

    let _ = tx.send(());
}
