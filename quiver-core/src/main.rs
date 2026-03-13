use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use quiver_core::adapters::BackendAdapter;
use quiver_core::adapters::clickhouse::ClickHouseAdapter;
use quiver_core::adapters::memory::MemoryAdapter;
use quiver_core::adapters::postgres::PostgresAdapter;
use quiver_core::adapters::redis::RedisAdapter;
use quiver_core::adapters::s3_parquet::S3ParquetAdapter;
use quiver_core::cache::RequestCache;
use quiver_core::config;
use quiver_core::logging::ConfigLogger;
use quiver_core::metrics::MetricsStore;
use quiver_core::observability::ObservabilityServer;
use quiver_core::proto::quiver::v1::observability_service_server::ObservabilityServiceServer;
use quiver_core::registry::StaticRegistry;
use quiver_core::resolver::Resolver;
use quiver_core::server::QuiverFlightServer;
use std::sync::Arc;
use std::time::Duration;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Identity, Server, ServerTlsConfig};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file. Can also be set via QUIVER_CONFIG environment variable.
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

    let config_path = args.config.or_else(|| std::env::var("QUIVER_CONFIG").ok());

    if let Some(ref path) = config_path {
        tracing::info!("Loading configuration from: {}", path);
    } else {
        tracing::error!("Configuration file not found.");
    };

    let cfg = config::Config::load(config_path.as_deref())?;

    if let Err(errors) = cfg.validate() {
        for e in &errors {
            tracing::error!("Config error: {}", e);
        }
        return Err(format!("{} config error(s) found", errors.len()).into());
    }

    ConfigLogger::log_startup_config(&cfg);

    let filtered_config = cfg.to_filtered();

    tracing::info!(
        "
   ____        _                
  / __ \\__  __(_)   _____  _____
 / / / / / / / / | | / _ \\/ ___/
/ /_/ / /_/ / /| |_| /  __/ /    
\\___\\_\\__,_/_/ |____/\\___/_/     
                                 
Quiver Flight Server starting up on {}:{}
Loaded {} feature views and {} adapters",
        cfg.server.host,
        cfg.server.port,
        match &cfg.registry {
            config::RegistryConfig::Static { views } => views.len(),
        },
        cfg.adapters.len(),
    );

    let addr = format!("{}:{}", cfg.server.host, cfg.server.port).parse()?;

    let registry = Arc::new(StaticRegistry::new());

    let resolver = Arc::new(Resolver::with_downtime_strategy(
        registry.clone() as Arc<dyn quiver_core::registry::Registry>,
        cfg.server.fanout.downtime_strategy,
    ));

    let config::RegistryConfig::Static { views } = cfg.registry;
    for view in views {
        let mut backend_routing = std::collections::HashMap::new();
        for col in &view.columns {
            backend_routing.insert(col.name.clone(), col.source.clone());
        }

        resolver.register_view_columns(
            view.name.clone(),
            view.columns.clone(),
            view.source_path.clone(),
        );

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
                    fallback_source: c.fallback_source.unwrap_or_default(),
                })
                .collect(),
            backend_routing,
            schema_version: 0,
        });
    }

    let adapter_init_futures: Vec<_> = cfg
        .adapters
        .into_iter()
        .map(|(name, adapter_cfg)| {
            let validation_config = cfg.server.validation.clone();
            async move {
                let result: Result<(String, Arc<dyn BackendAdapter>), Box<dyn std::error::Error>> =
                    match adapter_cfg {
                        config::AdapterConfig::Memory => {
                            let adapter = Arc::new(MemoryAdapter::new());
                            Ok((name, adapter as Arc<dyn BackendAdapter>))
                        }
                        config::AdapterConfig::Redis {
                            connection,
                            password,
                            source_path,
                            tls,
                            parameters,
                        } => {
                            let adapter = RedisAdapter::new(
                                &connection,
                                password.as_deref(),
                                &source_path,
                                None,
                                tls.as_ref(),
                                Some(validation_config),
                                Some(&parameters),
                            )
                            .await?;
                            Ok((name, Arc::new(adapter) as Arc<dyn BackendAdapter>))
                        }
                        config::AdapterConfig::Postgres {
                            connection_string,
                            source_path,
                            max_connections,
                            timeout_seconds,
                            tls,
                            parameters,
                        } => {
                            let timeout = timeout_seconds.map(Duration::from_secs);
                            let table_template = match &source_path {
                                quiver_core::config::SourcePath::Template(tmpl) => tmpl.as_str(),
                                quiver_core::config::SourcePath::Structured { table, .. } => {
                                    table.as_str()
                                }
                            };
                            let mut adapter = PostgresAdapter::new(
                                &connection_string,
                                table_template,
                                max_connections,
                                timeout,
                                tls.as_ref(),
                                Some(validation_config),
                                Some(&parameters),
                            )
                            .await?;

                            adapter.initialize().await?;

                            Ok((name, Arc::new(adapter) as Arc<dyn BackendAdapter>))
                        }
                        config::AdapterConfig::S3Parquet(s3_cfg) => {
                            let mut adapter =
                                S3ParquetAdapter::new(&s3_cfg, Some(validation_config)).await?;

                            adapter.initialize().await?;

                            Ok((name, Arc::new(adapter) as Arc<dyn BackendAdapter>))
                        }
                        config::AdapterConfig::ClickHouse {
                            connection_string,
                            source_path,
                            max_connections,
                            timeout_seconds,
                            tls,
                            parameters,
                        } => {
                            let timeout = timeout_seconds.map(Duration::from_secs);
                            let table_template = match &source_path {
                                quiver_core::config::SourcePath::Template(tmpl) => tmpl.as_str(),
                                quiver_core::config::SourcePath::Structured { table, .. } => {
                                    table.as_str()
                                }
                            };
                            let mut adapter = ClickHouseAdapter::new(
                                &connection_string,
                                table_template,
                                max_connections,
                                timeout,
                                tls.as_ref(),
                                Some(validation_config),
                                Some(&parameters),
                            )
                            .await?;

                            adapter.initialize().await?;

                            Ok((name, Arc::new(adapter) as Arc<dyn BackendAdapter>))
                        }
                    };
                result
            }
        })
        .collect();

    let adapter_results = futures::future::join_all(adapter_init_futures).await;

    for result in adapter_results {
        let (name, adapter) = result?;
        resolver.register_adapter(name, adapter);
    }

    let metrics_store = if cfg.server.observability.enabled {
        Arc::new(MetricsStore::with_ttl(
            cfg.server.observability.ttl_seconds,
            cfg.server.observability.max_entries,
        ))
    } else {
        Arc::new(MetricsStore::with_ttl(0, 0))
    };

    let request_cache = Arc::new(RequestCache::new(cfg.server.cache.clone()));

    let server = QuiverFlightServer::new(
        resolver,
        cfg.server.access_log.clone(),
        filtered_config,
        metrics_store.clone(),
        request_cache,
    );

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

    if cfg.server.observability.enabled {
        let observability_server = ObservabilityServer::new(metrics_store);
        let observability_service = ObservabilityServiceServer::new(observability_server);

        let observability_addr = format!("{}:8816", cfg.server.host).parse()?;

        tracing::info!("Starting observability service on {}", observability_addr);

        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(observability_service)
                .serve(observability_addr)
                .await
            {
                tracing::error!("Observability service error: {}", e);
            }
        });
    } else {
        tracing::info!("Observability service disabled");
    }

    server_builder
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(flight_service)
        .serve(addr)
        .await?;

    Ok(())
}
