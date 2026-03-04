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

    let service = QuiverFlightServer::new(resolver.clone(), None);

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", real_addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

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

#[tokio::test]
async fn test_get_flight_info() {
    let registry = Arc::new(StaticRegistry::new());
    let memory_adapter: Arc<dyn BackendAdapter> = Arc::new(MemoryAdapter::new());
    let resolver = Arc::new(Resolver::new(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>
    ));
    resolver.register_adapter("memory".to_string(), memory_adapter);

    registry.register(quiver_core::proto::quiver::v1::FeatureViewMetadata {
        name: "user_features".to_string(),
        entity_type: "user".to_string(),
        entity_key: "user_id".to_string(),
        columns: vec![
            quiver_core::proto::quiver::v1::FeatureColumnSchema {
                name: "entity_id".to_string(),
                arrow_type: "string".to_string(),
                nullable: false,
            },
            quiver_core::proto::quiver::v1::FeatureColumnSchema {
                name: "score".to_string(),
                arrow_type: "float64".to_string(),
                nullable: true,
            },
        ],
        backend_routing: {
            let mut routing = std::collections::HashMap::new();
            routing.insert("score".to_string(), "memory".to_string());
            routing
        },
        schema_version: 1,
    });

    let service = QuiverFlightServer::new(resolver.clone(), None);

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", real_addr))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut client = FlightServiceClient::new(channel);

    let descriptor = FlightDescriptor::new_path(vec!["user_features".to_string()]);
    let flight_info_response = client.get_flight_info(descriptor).await.unwrap();
    let flight_info = flight_info_response.into_inner();

    assert!(!flight_info.schema.is_empty(), "Schema should not be empty");
    assert_eq!(flight_info.endpoint.len(), 1, "Should have one endpoint");
    assert_eq!(
        flight_info.total_records, -1,
        "Total records should be unknown (-1) per F-01"
    );
    assert_eq!(
        flight_info.total_bytes, -1,
        "Total bytes should be unknown (-1) per F-01"
    );
    assert_eq!(
        flight_info.ordered, false,
        "Results should not be ordered per F-01"
    );

    assert!(
        flight_info.flight_descriptor.is_some(),
        "Flight descriptor should be preserved"
    );
    let returned_descriptor = flight_info.flight_descriptor.unwrap();
    assert_eq!(
        returned_descriptor.path,
        vec!["user_features"],
        "Path should be preserved"
    );

    let app_metadata = String::from_utf8(flight_info.app_metadata.to_vec()).unwrap();
    assert!(
        app_metadata.contains("entity_type:user"),
        "Should contain entity type metadata"
    );
    assert!(
        app_metadata.contains("schema_version:1"),
        "Should contain schema version metadata"
    );
    assert!(
        app_metadata.contains("backends:score:memory"),
        "Should contain backend routing metadata"
    );

    let endpoint = &flight_info.endpoint[0];
    assert!(
        endpoint.ticket.is_some(),
        "Endpoint should have a ticket for data retrieval"
    );
    assert!(
        endpoint.location.is_empty(),
        "Empty location means same server per F-01"
    );
    assert!(endpoint.expiration_time.is_none(), "No expiration time set");

    let schema_bytes = &flight_info.schema;
    assert!(!schema_bytes.is_empty(), "Schema bytes should not be empty");
    assert!(
        schema_bytes.len() > 50,
        "Schema should contain substantial metadata"
    );

    println!("F-01 get_flight_info implementation verified - returns proper FlightInfo structure");

    let _ = tx.send(());
}
