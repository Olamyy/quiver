use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub registry: RegistryConfig,
    pub adapters: HashMap<String, AdapterConfig>,
}

/// Filtered config safe for client exposure (no credentials)
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FilteredConfig {
    pub server: FilteredServerConfig,
    pub registry: RegistryConfig,
    pub adapters: HashMap<String, FilteredAdapterConfig>,
}

/// Server config with TLS details removed
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FilteredServerConfig {
    pub host: String,
    pub port: u16,
    pub max_concurrent_rpcs: Option<u32>,
    pub max_message_size_mb: Option<usize>,
    pub compression: Option<Compression>,
    pub timeout_seconds: Option<u64>,
}

/// Adapter config with connection details removed
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FilteredAdapterConfig {
    Memory,
    Redis {
        key_template: String,
    },
    Postgres {
        table_template: String,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
    },
}

impl Config {
    /// Create a filtered version of the config safe for client exposure
    pub fn to_filtered(&self) -> FilteredConfig {
        FilteredConfig {
            server: FilteredServerConfig {
                host: self.server.host.clone(),
                port: self.server.port,
                max_concurrent_rpcs: self.server.max_concurrent_rpcs,
                max_message_size_mb: self.server.max_message_size_mb,
                compression: self.server.compression.clone(),
                timeout_seconds: self.server.timeout_seconds,
            },
            registry: self.registry.clone(),
            adapters: self
                .adapters
                .iter()
                .map(|(name, adapter)| {
                    let filtered = match adapter {
                        AdapterConfig::Memory => FilteredAdapterConfig::Memory,
                        AdapterConfig::Redis { key_template, .. } => FilteredAdapterConfig::Redis {
                            key_template: key_template.clone(),
                        },
                        AdapterConfig::Postgres {
                            table_template,
                            max_connections,
                            timeout_seconds,
                            ..
                        } => FilteredAdapterConfig::Postgres {
                            table_template: table_template.clone(),
                            max_connections: *max_connections,
                            timeout_seconds: *timeout_seconds,
                        },
                    };
                    (name.clone(), filtered)
                })
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

/// Universal TLS configuration for adapters
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AdapterTlsConfig {
    #[serde(default = "default_true")]
    pub verify_certificates: bool,
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Gzip,
    Zstd,
    None,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub tls: Option<TlsConfig>,
    pub max_concurrent_rpcs: Option<u32>,
    pub max_message_size_mb: Option<usize>,
    pub compression: Option<Compression>,
    pub timeout_seconds: Option<u64>,
    pub access_log: Option<AccessLogConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AccessLogConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_false")]
    pub include_request_body: bool,
    #[serde(default = "default_true")]
    pub include_response_metadata: bool,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8815
}

fn default_false() -> bool {
    false
}

fn default_log_format() -> String {
    "json".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RegistryConfig {
    Static { views: Vec<FeatureViewConfig> },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FeatureViewConfig {
    pub name: String,
    pub entity_type: String,
    pub entity_key: String,
    pub columns: Vec<ColumnConfig>,
    #[serde(default)]
    pub schema_version: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    pub arrow_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
    pub source: String,
}

fn default_true() -> bool {
    true
}

impl AdapterTlsConfig {
    /// Determine if TLS should be enabled based on config and connection string.
    ///
    /// Logic:
    /// 1. If TLS config exists, TLS is enabled
    /// 2. If no TLS config, check protocol (rediss://, postgresql with sslmode, etc.)
    pub fn is_tls_enabled(&self, _connection_string: &str) -> bool {
        true
    }

    /// Get the effective certificate verification setting, considering query parameters.
    ///
    /// Query parameters like ?tls_verify=false can override the config setting.
    pub fn should_verify_certificates(&self, connection_string: &str) -> bool {
        if let Ok(url) = url::Url::parse(connection_string) {
            for (key, value) in url.query_pairs() {
                if key == "tls_verify" {
                    return value.parse::<bool>().unwrap_or(self.verify_certificates);
                }
            }
        }

        self.verify_certificates
    }
}

/// Helper function to determine if TLS should be enabled based on connection string protocol
/// when no explicit TLS config is provided.
pub fn is_tls_enabled_by_protocol(connection_string: &str) -> bool {
    connection_string.starts_with("rediss://")
        || connection_string.starts_with("grpcs://")
        || connection_string.contains("sslmode=require")
        || connection_string.contains("sslmode=prefer")
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AdapterConfig {
    Memory,
    Redis {
        connection: String,
        password: Option<String>,
        #[serde(default = "default_redis_template")]
        key_template: String,
        tls: Option<AdapterTlsConfig>,
    },
    Postgres {
        connection_string: String,
        #[serde(default = "default_postgres_table_template")]
        table_template: String,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
        tls: Option<AdapterTlsConfig>,
    },
}

fn default_redis_template() -> String {
    "quiver:f:{feature}:e:{entity}".to_string()
}

fn default_postgres_table_template() -> String {
    "features_{feature}".to_string()
}

impl Config {
    pub fn load(config_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut builder = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 8815)?;

        if let Some(path) = config_path {
            let path_buf = std::path::PathBuf::from(path);
            if !path_buf.exists() {
                return Err(format!("Configuration file not found: {}", path).into());
            }
            builder = builder.add_source(config::File::from(path_buf).required(true));
        } else {
            builder = builder.add_source(config::File::with_name("config").required(false));
        }

        let s = builder
            .add_source(config::Environment::with_prefix("QUIVER").separator("__"))
            .build()?;

        Ok(s.try_deserialize()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_example_config() {
        let yaml = r#"
server:
  host: 127.0.0.1
  port: 9000

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: uid
      columns:
        - name: f1
          arrow_type: float64
          source: s1
        - name: f2
          arrow_type: int64
          source: s1

adapters:
  s1:
    type: memory
"#;
        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        assert_eq!(config.server.port, 9000);
        assert_eq!(config.adapters.len(), 1);
        let RegistryConfig::Static { views } = config.registry;
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].columns.len(), 2);
    }

    #[test]
    fn test_parse_postgres_config() {
        let yaml = r#"
server:
  host: 127.0.0.1
  port: 9000

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: uid
      columns:
        - name: spend_30d
          arrow_type: float64
          source: postgres_adapter
        - name: session_count
          arrow_type: int64
          source: postgres_adapter

adapters:
  postgres_adapter:
    type: postgres
    connection_string: "postgresql://user:password@localhost:5432/features"
    table_template: "features_{feature}"
    max_connections: 20
    timeout_seconds: 30
"#;
        let config: Config =
            serde_yaml::from_str(yaml).expect("Failed to parse YAML with PostgreSQL");
        assert_eq!(config.adapters.len(), 1);

        if let AdapterConfig::Postgres {
            connection_string,
            table_template,
            max_connections,
            timeout_seconds,
            tls: _,
        } = config.adapters.get("postgres_adapter").unwrap()
        {
            assert_eq!(
                connection_string,
                "postgresql://user:password@localhost:5432/features"
            );
            assert_eq!(table_template, "features_{feature}");
            assert_eq!(*max_connections, Some(20));
            assert_eq!(*timeout_seconds, Some(30));
        } else {
            panic!("Expected PostgreSQL adapter config");
        }
    }
}
