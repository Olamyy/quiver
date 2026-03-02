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

    let memory_adapter = Arc::new(MemoryAdapter::new());

    let resolver = Arc::new(Resolver::new(registry.clone()));
    resolver.register_adapter("memory".to_string(), memory_adapter.clone());

    demo::seed_demo_data(registry.clone(), memory_adapter.clone()).await?;

    let server = QuiverFlightServer::new(resolver);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<QuiverFlightServer>().await;

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
