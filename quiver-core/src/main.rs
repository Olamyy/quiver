use crate::adapters::memory::MemoryAdapter;
use crate::registry::StaticRegistry;
use crate::resolver::Resolver;
use crate::server::QuiverFlightServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use std::sync::Arc;
use tonic::transport::Server;

pub mod adapters;
pub mod demo;
pub mod proto;
pub mod registry;
pub mod resolver;
pub mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:8815".parse()?;

    let registry = Arc::new(StaticRegistry::new());
    registry.register(crate::proto::quiver::v1::FeatureViewMetadata {
        name: "user_features".to_string(),
        entity_type: "user".to_string(),
        entity_key: "entity_id".to_string(),
        columns: vec![
            crate::proto::quiver::v1::FeatureColumnSchema {
                name: "spend_30d".to_string(),
                arrow_type: "float64".to_string(),
                nullable: true,
            },
            crate::proto::quiver::v1::FeatureColumnSchema {
                name: "session_count".to_string(),
                arrow_type: "int64".to_string(),
                nullable: true,
            },
        ],
        backend_routing: [
            ("spend_30d".to_string(), "memory".to_string()),
            ("session_count".to_string(), "memory".to_string()),
        ]
        .into(),
        schema_version: 1,
    });

    let adapter = Arc::new(MemoryAdapter::seed([
        (
            "user:101",
            vec![
                (
                    "spend_30d",
                    crate::adapters::memory::ScalarValue::Float64(890.0),
                ),
                (
                    "session_count",
                    crate::adapters::memory::ScalarValue::Int64(23),
                ),
            ],
            chrono::Utc::now(),
        ),
        (
            "user:102",
            vec![
                (
                    "spend_30d",
                    crate::adapters::memory::ScalarValue::Float64(120.0),
                ),
                (
                    "session_count",
                    crate::adapters::memory::ScalarValue::Int64(4),
                ),
            ],
            chrono::Utc::now(),
        ),
    ]));

    let resolver = Arc::new(Resolver::new(registry));
    resolver.register_adapter("memory".to_string(), adapter);

    let server = QuiverFlightServer::new(resolver);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<FlightServiceServer<QuiverFlightServer>>()
        .await;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic::include_file_descriptor_set!(
            "quiver_descriptor"
        ))
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    tracing::info!("Quiver listening on {}", addr);

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(FlightServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
