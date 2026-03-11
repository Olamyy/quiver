use crate::config::{AdapterConfig, Config, SourcePath};
use std::collections::HashMap;

pub struct ConfigLogger;

impl ConfigLogger {
    pub fn log_startup_config(config: &Config) {
        Self::log_server_config(&config.server);
        Self::log_adapter_config(&config.adapters);
        Self::log_validation_config(&config.server.validation);
    }

    fn log_server_config(server: &crate::config::ServerConfig) {
        tracing::info!("=== Server Configuration ===");
        tracing::info!("Host: {}", server.host);
        tracing::info!("Port: {}", server.port);

        if let Some(timeout) = server.timeout_seconds {
            tracing::info!("Default Timeout: {}s", timeout);
        }

        if let Some(max_rpcs) = server.max_concurrent_rpcs {
            tracing::info!("Max Concurrent RPCs: {}", max_rpcs);
        }

        if let Some(max_msg) = server.max_message_size_mb {
            tracing::info!("Max Message Size: {}MB", max_msg);
        }

        if server.tls.is_some() {
            tracing::info!("TLS: Enabled");
        } else {
            tracing::warn!("TLS: Disabled (not recommended for production)");
        }

        if let Some(compression) = &server.compression {
            tracing::info!("Compression: {:?}", compression);
        }
    }

    fn log_adapter_config(adapters: &HashMap<String, AdapterConfig>) {
        if adapters.is_empty() {
            tracing::warn!("No adapters configured");
            return;
        }

        tracing::info!("=== Adapter Configuration ===");

        for (name, config) in adapters {
            match config {
                AdapterConfig::Memory => {
                    tracing::info!("Adapter '{}': type=memory", name);
                }
                AdapterConfig::Redis {
                    connection,
                    source_path,
                    parameters,
                    tls,
                    ..
                } => {
                    tracing::info!(
                        "Adapter '{}': type=redis, connection={}",
                        name,
                        Self::mask_credentials(connection)
                    );

                    let tls_status = if tls.is_some() { "enabled" } else { "disabled" };
                    tracing::info!("  └─ TLS: {}", tls_status);

                    match source_path {
                        SourcePath::Template(tmpl) => {
                            tracing::info!("  └─ Source Path: {} (template)", tmpl);
                        }
                        SourcePath::Structured { table, column } => {
                            let col_str = column
                                .as_ref()
                                .map(|c| format!(".{}", c))
                                .unwrap_or_default();
                            tracing::info!("  └─ Source Path: {}{} (structured)", table, col_str);
                        }
                    }

                    if let Some(chunk_size) = parameters.get("mget_chunk_size") {
                        tracing::info!("  └─ MGET Chunk Size: {}", chunk_size);
                    }
                }
                AdapterConfig::Postgres {
                    connection_string,
                    source_path,
                    max_connections,
                    timeout_seconds,
                    tls,
                    parameters,
                } => {
                    tracing::info!(
                        "Adapter '{}': type=postgres, connection={}",
                        name,
                        Self::mask_credentials(connection_string)
                    );

                    let tls_status = if tls.is_some() { "enabled" } else { "disabled" };
                    tracing::info!("  └─ TLS: {}", tls_status);

                    if !Self::is_secure_postgres_connection(connection_string) {
                        tracing::warn!(
                            "  └─ WARNING: PostgreSQL connection does not specify secure SSL mode. \
                             Defaulting to insecure fallback."
                        );
                    }

                    match source_path {
                        SourcePath::Template(tmpl) => {
                            tracing::info!("  └─ Source Path: {} (template)", tmpl);
                        }
                        SourcePath::Structured { table, column } => {
                            let col_str = column
                                .as_ref()
                                .map(|c| format!(".{}", c))
                                .unwrap_or_default();
                            tracing::info!("  └─ Source Path: {}{} (structured)", table, col_str);
                        }
                    }

                    if let Some(max_conn) = max_connections {
                        tracing::info!("  └─ Max Connections: {}", max_conn);
                    }

                    if let Some(timeout) = timeout_seconds {
                        tracing::info!("  └─ Timeout: {}s", timeout);
                    }

                    if !parameters.is_empty() {
                        tracing::info!("  └─ Parameters: {:?}", parameters);
                    }
                }
                AdapterConfig::S3Parquet(s3_cfg) => {
                    tracing::info!(
                        "Adapter '{}': type=s3_parquet, storage_uri={}",
                        name,
                        s3_cfg.storage_uri
                    );

                    match &s3_cfg.source_path {
                        SourcePath::Template(tmpl) => {
                            tracing::info!("  └─ Source Path: {} (template)", tmpl);
                        }
                        SourcePath::Structured { table, column } => {
                            let col_str = column
                                .as_ref()
                                .map(|c| format!(".{}", c))
                                .unwrap_or_default();
                            tracing::info!("  └─ Source Path: {}{} (structured)", table, col_str);
                        }
                    }

                    tracing::info!("  └─ AWS Auth: using environment variables");

                    if let Some(timeout) = s3_cfg.timeout_seconds {
                        tracing::info!("  └─ Timeout: {}s", timeout);
                    }
                }
                AdapterConfig::ClickHouse {
                    connection_string,
                    source_path,
                    max_connections,
                    timeout_seconds,
                    tls,
                    parameters,
                } => {
                    tracing::info!(
                        "Adapter '{}': type=clickhouse, connection={}",
                        name,
                        Self::mask_credentials(connection_string)
                    );

                    let tls_status = if tls.is_some() { "enabled" } else { "disabled" };
                    tracing::info!("  └─ TLS: {}", tls_status);

                    match source_path {
                        SourcePath::Template(tmpl) => {
                            tracing::info!("  └─ Source Path: {} (template)", tmpl);
                        }
                        SourcePath::Structured { table, column } => {
                            let col_str = column
                                .as_ref()
                                .map(|c| format!(".{}", c))
                                .unwrap_or_default();
                            tracing::info!("  └─ Source Path: {}{} (structured)", table, col_str);
                        }
                    }

                    if let Some(max_conn) = max_connections {
                        tracing::info!("  └─ Max Connections: {}", max_conn);
                    }

                    if let Some(timeout) = timeout_seconds {
                        tracing::info!("  └─ Timeout: {}s", timeout);
                    }

                    if let Some(chunk_size) = parameters.get("chunk_size") {
                        tracing::info!("  └─ Chunk Size: {}", chunk_size);
                    }
                }
            }
        }
    }

    fn log_validation_config(validation: &crate::validation::ValidationConfig) {
        tracing::info!("=== Validation Configuration ===");

        if validation.skip_all_validation {
            tracing::warn!("Validation: All validations disabled");
            return;
        }

        if validation.request_validations.is_empty() && validation.response_validations.is_empty() {
            tracing::info!("Validation: No validations enabled");
            return;
        }

        if !validation.request_validations.is_empty() {
            tracing::info!("Request Validations:");
            for v in &validation.request_validations {
                tracing::info!("  └─ {:?}", v);
            }
        }

        if !validation.response_validations.is_empty() {
            tracing::info!("Response Validations:");
            for v in &validation.response_validations {
                tracing::info!("  └─ {:?}", v);
            }
        }
    }

    fn mask_credentials(connection_str: &str) -> String {
        if let Ok(url) = url::Url::parse(connection_str) {
            let mut masked = url.clone();
            if !url.password().unwrap_or("").is_empty() {
                let _ = masked.set_password(Some("***"));
            }
            if !url.username().is_empty() && url.username() != "postgres" {
                let _ = masked.set_username(&format!("{}***", &url.username()[0..1]));
            }
            masked.to_string()
        } else {
            connection_str.to_string()
        }
    }

    fn is_secure_postgres_connection(connection_string: &str) -> bool {
        connection_string.contains("sslmode=require")
            || connection_string.contains("sslmode=verify-ca")
            || connection_string.contains("sslmode=verify-full")
    }
}
