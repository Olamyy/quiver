use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::StreamExt;
use prost::Message;
use quiver_core::adapters::BackendAdapter;
use quiver_core::adapters::memory::MemoryAdapter;
use quiver_core::adapters::utils::ScalarValue;
use quiver_core::cache::RequestCache;
use quiver_core::config::{
    FilteredAdapterConfig, FilteredConfig, FilteredServerConfig, RegistryConfig,
};
use quiver_core::metrics::MetricsStore;
use quiver_core::proto::quiver::v1::{EntityKey, FeatureRequest};
use quiver_core::registry::StaticRegistry;
use quiver_core::resolver::Resolver;
use quiver_core::server::QuiverFlightServer;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Server;

fn create_test_filtered_config() -> FilteredConfig {
    FilteredConfig {
        server: FilteredServerConfig {
            host: "localhost".to_string(),
            port: 8815,
            max_concurrent_rpcs: None,
            max_message_size_mb: None,
            compression: None,
            timeout_seconds: None,
            fanout: Default::default(),
            observability: Default::default(),
        },
        registry: RegistryConfig::Static { views: vec![] },
        adapters: {
            let mut adapters = HashMap::new();
            adapters.insert("memory".to_string(), FilteredAdapterConfig::Memory);
            adapters
        },
    }
}

#[tokio::test]
async fn test_metadata_headers_present() {
    let registry = Arc::new(StaticRegistry::new());

    let memory_adapter = MemoryAdapter::seed([
        (
            "u1",
            [("val", ScalarValue::Float64(10.0))],
            chrono::Utc::now(),
        ),
        (
            "u2",
            [("val", ScalarValue::Float64(20.0))],
            chrono::Utc::now(),
        ),
    ]);
    let memory_adapter: Arc<dyn BackendAdapter> = Arc::new(memory_adapter);

    let resolver = Arc::new(Resolver::with_downtime_strategy(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>,
        quiver_core::config::DowntimeStrategy::Fail,
    ));
    resolver.register_adapter("memory".to_string(), memory_adapter);

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
            fallback_source: String::new(),
        }],
        backend_routing,
        schema_version: 1,
    });

    let metrics_store = Arc::new(MetricsStore::new());
    let request_cache = Arc::new(RequestCache::new(Default::default()));
    let service = QuiverFlightServer::new(
        resolver.clone(),
        None,
        create_test_filtered_config(),
        metrics_store,
        request_cache,
    );

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

    let feature_request = FeatureRequest {
        feature_view: "test_view".to_string(),
        feature_names: vec!["val".to_string()],
        entities: vec![
            EntityKey {
                entity_id: "u1".to_string(),
                entity_type: "user".to_string(),
            },
            EntityKey {
                entity_id: "u2".to_string(),
                entity_type: "user".to_string(),
            },
        ],
        as_of: None,
        freshness: None,
        context: None,
        output: None,
    };
    let ticket = Ticket {
        ticket: feature_request.encode_to_vec().into(),
    };

    let response = client.do_get(ticket).await.unwrap();
    let metadata = response.metadata();

    let request_id_header = metadata.get("x-quiver-request-id");
    assert!(
        request_id_header.is_some(),
        "x-quiver-request-id header must be present"
    );

    if let Some(request_id_bytes) = request_id_header {
        let request_id_str = std::str::from_utf8(request_id_bytes.as_bytes())
            .expect("request_id should be valid UTF-8");
        assert!(!request_id_str.is_empty(), "request_id must not be empty");
    }

    let from_cache_header = metadata.get("x-quiver-from-cache");
    assert!(
        from_cache_header.is_some(),
        "x-quiver-from-cache header must be present"
    );

    if let Some(from_cache_bytes) = from_cache_header {
        let from_cache_str = std::str::from_utf8(from_cache_bytes.as_bytes())
            .expect("from_cache should be valid UTF-8");
        assert!(
            from_cache_str == "true" || from_cache_str == "false",
            "from_cache must be 'true' or 'false'"
        );
    }

    let _ = tx.send(());
}

#[tokio::test]
async fn test_metadata_headers_cache_behavior() {
    let registry = Arc::new(StaticRegistry::new());

    let memory_adapter = MemoryAdapter::seed([(
        "u1",
        [("val", ScalarValue::Float64(10.0))],
        chrono::Utc::now(),
    )]);
    let memory_adapter: Arc<dyn BackendAdapter> = Arc::new(memory_adapter);

    let resolver = Arc::new(Resolver::with_downtime_strategy(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>,
        quiver_core::config::DowntimeStrategy::Fail,
    ));
    resolver.register_adapter("memory".to_string(), memory_adapter);

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
            fallback_source: String::new(),
        }],
        backend_routing,
        schema_version: 1,
    });

    let metrics_store = Arc::new(MetricsStore::new());
    let request_cache = Arc::new(RequestCache::new(Default::default()));
    let service = QuiverFlightServer::new(
        resolver.clone(),
        None,
        create_test_filtered_config(),
        metrics_store,
        request_cache,
    );

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

    let response1 = client.do_get(ticket.clone()).await.unwrap();
    let metadata1 = response1.metadata().clone();
    let mut stream1 = response1.into_inner();
    while let Some(_) = stream1.next().await {}

    let from_cache_1 = metadata1
        .get("x-quiver-from-cache")
        .and_then(|b| std::str::from_utf8(b.as_bytes()).ok());

    let response2 = client.do_get(ticket).await.unwrap();
    let metadata2 = response2.metadata().clone();
    let mut stream2 = response2.into_inner();
    while let Some(_) = stream2.next().await {}

    let from_cache_2 = metadata2
        .get("x-quiver-from-cache")
        .and_then(|b| std::str::from_utf8(b.as_bytes()).ok());

    assert_eq!(
        from_cache_1,
        Some("false"),
        "First request should not be from cache"
    );
    assert_eq!(
        from_cache_2,
        Some("true"),
        "Second identical request should be from cache"
    );

    let _ = tx.send(());
}
