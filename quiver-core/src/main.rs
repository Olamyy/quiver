use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use quiver_core::adapters::memory::MemoryAdapter;
use quiver_core::adapters::redis::RedisAdapter;
use quiver_core::config;
use quiver_core::registry::StaticRegistry;
use quiver_core::resolver::Resolver;
use quiver_core::server::QuiverFlightServer;
use std::sync::Arc;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Identity, Server, ServerTlsConfig};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = config::Config::load(args.config.as_deref())?;

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

        registry.register(quiver_core::proto::quiver::v1::FeatureViewMetadata {
            name: view.name,
            entity_type: view.entity_type,
            entity_key: view.entity_key,
            columns: view
                .columns
                .into_iter()
                .map(|c| quiver_core::proto::quiver::v1::FeatureColumnSchema {
                    name: c.name,
                    arrow_type: c.arrow_type,
                    nullable: c.nullable,
                })
                .collect(),
            backend_routing,
            schema_version: view.schema_version,
        });
    }

    let resolver = Arc::new(Resolver::new(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>
    ));

    for (name, adapter_cfg) in cfg.adapters {
        match adapter_cfg {
            config::AdapterConfig::Memory => {
                let adapter = Arc::new(MemoryAdapter::new());
                resolver.register_adapter(
                    name,
                    adapter as Arc<dyn quiver_core::adapters::BackendAdapter>,
                );
            }
            config::AdapterConfig::Redis {
                connection,
                password,
                key_template,
            } => {
                let adapter = RedisAdapter::new(&connection, password.as_deref(), &key_template).await?;
                resolver.register_adapter(
                    name,
                    Arc::new(adapter) as Arc<dyn quiver_core::adapters::BackendAdapter>,
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

    let mut server_builder = Server::builder();

    if let Some(tls) = &cfg.server.tls {
        let cert = std::fs::read_to_string(&tls.cert_path)?;
        let key = std::fs::read_to_string(&tls.key_path)?;
        let identity = Identity::from_pem(cert, key);
        server_builder = server_builder.tls_config(ServerTlsConfig::new().identity(identity))?;
        tracing::info!("TLS enabled with certificate: {}", tls.cert_path);
    }

    if let Some(concurrency) = cfg.server.max_concurrent_rpcs {
        server_builder = server_builder.max_concurrent_streams(concurrency);
    }

    if let Some(timeout) = cfg.server.timeout_seconds {
        server_builder = server_builder.timeout(std::time::Duration::from_secs(timeout));
    }

    let mut flight_service = FlightServiceServer::new(server);
    if let Some(limit_mb) = cfg.server.max_message_size_mb {
        let limit_bytes = limit_mb * 1024 * 1024;
        flight_service = flight_service
            .max_decoding_message_size(limit_bytes)
            .max_encoding_message_size(limit_bytes);
    }

    if let Some(compression) = &cfg.server.compression {
        match compression {
            config::Compression::Gzip => {
                flight_service = flight_service
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip);
            }
            config::Compression::Zstd => {
                flight_service = flight_service
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Zstd);
            }
            config::Compression::None => {}
        }
    }

    server_builder
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(flight_service)
        .serve(addr)
        .await?;

    Ok(())
}
