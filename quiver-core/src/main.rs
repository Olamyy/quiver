use crate::adapters::memory::MemoryAdapter;
use crate::registry::StaticRegistry;
use crate::resolver::Resolver;
use crate::server::QuiverFlightServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use std::sync::Arc;
use tonic::transport::Server;

pub mod adapters;
pub mod config;
pub mod demo;
pub mod proto;
pub mod registry;
pub mod resolver;
pub mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = config::Config::load()?;

    if cfg.adapters.is_empty() {
        return Err("Configuration error: No adapters defined".into());
    }

    let (view_count, adapter_count) = (
        match &cfg.registry {
            config::RegistryConfig::Static { views } => {
                if views.is_empty() {
                    return Err("Configuration error: Registry contains no feature views".into());
                }
                views.len()
            }
        },
        cfg.adapters.len(),
    );

    let addr = format!("{}:{}", cfg.server.host, cfg.server.port).parse()?;

    let registry = Arc::new(StaticRegistry::new());

    let config::RegistryConfig::Static { views } = cfg.registry;
    for view in views {
        let mut backend_routing = std::collections::HashMap::new();
        for col in &view.columns {
            backend_routing.insert(col.name.clone(), col.source.clone());
        }

        registry.register(crate::proto::quiver::v1::FeatureViewMetadata {
            name: view.name,
            entity_type: view.entity_type,
            entity_key: view.entity_key,
            columns: view
                .columns
                .into_iter()
                .map(|c| crate::proto::quiver::v1::FeatureColumnSchema {
                    name: c.name,
                    arrow_type: c.arrow_type,
                    nullable: c.nullable,
                })
                .collect(),
            backend_routing,
            schema_version: view.schema_version,
        });
    }

    let resolver = Arc::new(Resolver::new(registry));

    for (name, adapter_cfg) in cfg.adapters {
        match adapter_cfg {
            config::AdapterConfig::Memory => {
                let adapter = Arc::new(MemoryAdapter::new());
                resolver.register_adapter(name, adapter);
            }
            config::AdapterConfig::Redis { .. } => {
                tracing::warn!(
                    "Redis adapter configured for '{}' but not yet implemented",
                    name
                );
            }
        }
    }

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

    tracing::info!(
        r#"
   ____        _                
  / __ \__  __(_)   _____  _____
 / / / / / / / / | | / _ \/ ___/
/ /_/ / /_/ / /| |_| /  __/ /    
\___\_\__,_/_/ |____/\___/_/     
                                 
Quiver Flight Server starting up on {}
Loaded {} feature views and {} adapters
"#,
        addr,
        view_count,
        adapter_count
    );

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(FlightServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
