use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::validation::ValidationConfig;

/// Downtime strategy for handling backend failures during fanout.
///
/// Determines behavior when a backend fails, times out, or is unavailable.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DowntimeStrategy {
    /// Fail the entire request if any backend fails (default, safe).
    #[default]
    Fail,
    /// Return available data from successful backends, skip failed ones (partial results).
    ReturnAvailable,
    /// Try fallback backend if primary fails/times out (requires fallback_source configured).
    UseFallback,
}

/// Source path configuration - can be a string template or structured path
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SourcePath {
    /// String template (e.g., "features:{feature}:{entity}" for Redis, "user_features_{feature}" for Postgres)
    Template(String),
    /// Structured path for existing schemas
    Structured {
        table: String,
        column: Option<String>,
    },
}

/// Feature-specific source path information passed to adapters
#[derive(Debug, Clone)]
pub struct FeatureSourcePath {
    pub feature_name: String,
    pub source_path: SourcePath,
}

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
    pub fanout: FanoutServerConfig,
    pub observability: ObservabilityConfig,
}

/// Adapter config with connection details removed
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FilteredAdapterConfig {
    Memory,
    Redis {
        source_path: SourcePath,
    },
    Postgres {
        source_path: SourcePath,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
    },
    S3Parquet {
        storage_uri: String,
        source_path: SourcePath,
        timeout_seconds: Option<u64>,
    },
    ClickHouse {
        source_path: SourcePath,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
    },
}

impl Config {
    pub fn to_filtered(&self) -> FilteredConfig {
        FilteredConfig {
            server: FilteredServerConfig {
                host: self.server.host.clone(),
                port: self.server.port,
                max_concurrent_rpcs: self.server.max_concurrent_rpcs,
                max_message_size_mb: self.server.max_message_size_mb,
                compression: self.server.compression.clone(),
                timeout_seconds: self.server.timeout_seconds,
                fanout: self.server.fanout.clone(),
                observability: self.server.observability.clone(),
            },
            registry: self.registry.clone(),
            adapters: self
                .adapters
                .iter()
                .map(|(name, adapter)| {
                    let filtered = match adapter {
                        AdapterConfig::Memory => FilteredAdapterConfig::Memory,
                        AdapterConfig::Redis { source_path, .. } => FilteredAdapterConfig::Redis {
                            source_path: source_path.clone(),
                        },
                        AdapterConfig::Postgres {
                            source_path,
                            max_connections,
                            timeout_seconds,
                            ..
                        } => FilteredAdapterConfig::Postgres {
                            source_path: source_path.clone(),
                            max_connections: *max_connections,
                            timeout_seconds: *timeout_seconds,
                        },
                        AdapterConfig::S3Parquet(s3_cfg) => FilteredAdapterConfig::S3Parquet {
                            storage_uri: s3_cfg.storage_uri.clone(),
                            source_path: s3_cfg.source_path.clone(),
                            timeout_seconds: s3_cfg.timeout_seconds,
                        },
                        AdapterConfig::ClickHouse {
                            source_path,
                            max_connections,
                            timeout_seconds,
                            ..
                        } => FilteredAdapterConfig::ClickHouse {
                            source_path: source_path.clone(),
                            max_connections: *max_connections,
                            timeout_seconds: *timeout_seconds,
                        },
                    };
                    (name.clone(), filtered)
                })
                .collect(),
        }
    }

    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors: Vec<String> = Vec::new();

        if self.server.observability.enabled {
            if self.server.observability.ttl_seconds == 0 {
                errors.push("observability.ttl_seconds must be > 0 when observability is enabled".to_string());
            }
            if self.server.observability.max_entries == 0 {
                errors.push("observability.max_entries must be > 0 when observability is enabled".to_string());
            }
        }

        if self.server.fanout.enabled {
            if self.server.fanout.max_concurrent_backends == 0 {
                errors.push(
                    "fanout.max_concurrent_backends must be >= 1 when fanout is enabled"
                        .to_string(),
                );
            }

            match self.server.fanout.partial_failure_strategy.as_str() {
                "null_fill" | "error" | "forward_fill" => {}
                strategy => {
                    errors.push(format!(
                        "fanout.partial_failure_strategy '{}' is invalid; must be 'null_fill', 'error', or 'forward_fill'",
                        strategy
                    ));
                }
            }
        }

        for (adapter_name, adapter) in &self.adapters {
            match adapter {
                AdapterConfig::Redis { source_path, .. } => match source_path {
                    SourcePath::Structured { .. } => {
                        errors.push(format!(
                                "adapter '{}': source_path.table/column (Structured) is not supported for Redis; use a Template such as '{{feature}}:{{entity}}'",
                                adapter_name
                            ));
                    }
                    SourcePath::Template(tmpl) => {
                        if !tmpl.contains("{feature}") && !tmpl.contains("{entity}") {
                            errors.push(format!(
                                    "adapter '{}': source_path template '{}' contains neither {{feature}} nor {{entity}} -- all keys would be identical",
                                    adapter_name, tmpl
                                ));
                        }
                    }
                },
                AdapterConfig::S3Parquet(s3_cfg) => {
                    if s3_cfg.storage_uri.is_empty() {
                        errors.push(format!(
                            "adapter '{}': storage_uri cannot be empty",
                            adapter_name
                        ));
                    }
                    if !s3_cfg.storage_uri.starts_with("s3://")
                        && !s3_cfg.storage_uri.starts_with("file://")
                    {
                        errors.push(format!(
                            "adapter '{}': storage_uri must start with 's3://' or 'file://'",
                            adapter_name
                        ));
                    }
                }
                AdapterConfig::ClickHouse {
                    connection_string, ..
                } => {
                    if connection_string.is_empty() {
                        errors.push(format!(
                            "adapter '{}': connection_string cannot be empty",
                            adapter_name
                        ));
                    }
                    if !connection_string.contains("://") {
                        errors.push(format!(
                            "adapter '{}': connection_string must be a valid URL (e.g., 'clickhouse://host:port/db')",
                            adapter_name
                        ));
                    }
                }
                _ => {}
            }
        }

        let RegistryConfig::Static { views } = &self.registry;

        for view in views {
            if view.columns.is_empty() {
                errors.push(format!("view '{}' has no columns", view.name));
            }

            if self.server.fanout.downtime_strategy == DowntimeStrategy::UseFallback {
                for col in &view.columns {
                    if col.fallback_source.is_none() {
                        errors.push(format!(
                            "view '{}' column '{}': downtime_strategy='use_fallback' requires fallback_source to be defined",
                            view.name, col.name
                        ));
                    }
                }
            }

            for col in &view.columns {
                if let Some(adapter) = self.adapters.get(&col.source) {
                    if let AdapterConfig::Redis {
                        source_path: adapter_source_path,
                        ..
                    } = adapter
                    {
                        let effective_path = col
                            .source_path
                            .as_ref()
                            .or(view.source_path.as_ref())
                            .unwrap_or(adapter_source_path);

                        match effective_path {
                            SourcePath::Structured { .. } => {
                                errors.push(format!(
                                    "view '{}' column '{}': source_path.table/column (Structured) is not supported for Redis; use a Template such as '{{feature}}:{{entity}}'",
                                    view.name, col.name
                                ));
                            }
                            SourcePath::Template(tmpl) => {
                                if !tmpl.contains("{feature}") && !tmpl.contains("{entity}") {
                                    errors.push(format!(
                                        "view '{}' column '{}': source_path template '{}' contains neither {{feature}} nor {{entity}} -- all keys would be identical",
                                        view.name, col.name, tmpl
                                    ));
                                }
                            }
                        }
                    }
                } else {
                    errors.push(format!(
                        "view '{}' column '{}': source '{}' does not reference a defined adapter",
                        view.name, col.name, col.source
                    ));
                }

                if crate::adapters::utils::parse_arrow_type_string(&col.arrow_type).is_err() {
                    errors.push(format!(
                        "view '{}' column '{}': arrow_type '{}' is not a recognised Arrow type",
                        view.name, col.name, col.arrow_type
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
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
    #[serde(default)]
    pub validation: ValidationConfig,
    #[serde(default)]
    pub fanout: FanoutServerConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub cache: crate::cache::CacheConfig,
}

/// Fanout execution configuration.
///
/// Controls multi-backend feature fetching behavior per RFC v0.3.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FanoutServerConfig {
    /// Enable parallel multi-backend feature resolution.
    #[serde(default = "default_fanout_enabled")]
    pub enabled: bool,

    /// Maximum number of backends to dispatch concurrently.
    #[serde(default = "default_max_concurrent_backends")]
    pub max_concurrent_backends: usize,

    /// Strategy for handling partial failures when backends return fewer entities than requested.
    #[serde(default = "default_partial_failure_strategy")]
    pub partial_failure_strategy: String,

    /// Strategy for handling backend downtime (timeout, error, unavailable).
    #[serde(default)]
    pub downtime_strategy: DowntimeStrategy,
}

/// Observability service configuration.
///
/// Controls metrics collection and the observability service.
/// When disabled, no metrics are collected or stored, eliminating observability overhead (~1-3%).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObservabilityConfig {
    /// Enable metrics collection and observability service (default: true).
    /// When false, no request IDs are generated and no metrics are stored.
    /// This eliminates metrics recording overhead (~3-7 µs per request).
    #[serde(default = "default_observability_enabled")]
    pub enabled: bool,

    /// Time-to-live for stored metrics in seconds (default: 3600 = 1 hour).
    /// Metrics older than this are automatically evicted from the store.
    /// Ignored when observability is disabled.
    #[serde(default = "default_observability_ttl_seconds")]
    pub ttl_seconds: u64,

    /// Maximum number of metrics to store in memory (default: 10000).
    /// When this limit is reached, oldest entries are evicted (LRU).
    /// Ignored when observability is disabled.
    #[serde(default = "default_observability_max_entries")]
    pub max_entries: u32,
}

impl Default for FanoutServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_backends: 10,
            partial_failure_strategy: "null_fill".to_string(),
            downtime_strategy: DowntimeStrategy::Fail,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 3600,
            max_entries: 10000,
        }
    }
}

fn default_fanout_enabled() -> bool {
    true
}

fn default_max_concurrent_backends() -> usize {
    10
}

fn default_partial_failure_strategy() -> String {
    "null_fill".to_string()
}

fn default_observability_enabled() -> bool {
    true
}

fn default_observability_ttl_seconds() -> u64 {
    3600
}

fn default_observability_max_entries() -> u32 {
    10000
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
    pub source_path: Option<SourcePath>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    pub arrow_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
    pub source: String,
    pub source_path: Option<SourcePath>,
    /// Fallback backend to use if primary backend fails/times out.
    /// Required when downtime_strategy=use_fallback.
    pub fallback_source: Option<String>,
}

fn default_true() -> bool {
    true
}

impl AdapterTlsConfig {
    pub fn is_tls_enabled(&self, _connection_string: &str) -> bool {
        true
    }

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

/// S3/Parquet-specific adapter configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct S3ParquetAdapterConfig {
    pub storage_uri: String,
    #[serde(default = "default_s3_source_path")]
    pub source_path: SourcePath,
    pub timeout_seconds: Option<u64>,
    pub tls: Option<AdapterTlsConfig>,
    #[serde(default)]
    pub parameters: HashMap<String, serde_json::Value>,
    #[serde(default = "default_max_days_back")]
    pub max_days_back: u32,
}

fn default_s3_source_path() -> SourcePath {
    SourcePath::Template("{feature}.parquet".to_string())
}

fn default_max_days_back() -> u32 {
    365
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AdapterConfig {
    Memory,
    Redis {
        connection: String,
        password: Option<String>,
        #[serde(default = "default_redis_source_path")]
        source_path: SourcePath,
        tls: Option<AdapterTlsConfig>,
        #[serde(default)]
        parameters: HashMap<String, serde_json::Value>,
    },
    Postgres {
        connection_string: String,
        #[serde(default = "default_postgres_source_path")]
        source_path: SourcePath,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
        tls: Option<AdapterTlsConfig>,
        #[serde(default)]
        parameters: HashMap<String, serde_json::Value>,
    },
    S3Parquet(S3ParquetAdapterConfig),
    ClickHouse {
        connection_string: String,
        #[serde(default = "default_clickhouse_source_path")]
        source_path: SourcePath,
        max_connections: Option<u32>,
        timeout_seconds: Option<u64>,
        tls: Option<AdapterTlsConfig>,
        #[serde(default)]
        parameters: HashMap<String, serde_json::Value>,
    },
}

fn default_redis_source_path() -> SourcePath {
    SourcePath::Template("quiver:f:{feature}:e:{entity}".to_string())
}

fn default_postgres_source_path() -> SourcePath {
    SourcePath::Template("features_{feature}".to_string())
}

fn default_clickhouse_source_path() -> SourcePath {
    SourcePath::Template("features_{feature}".to_string())
}

/// Substitute ${ENV_VAR} placeholders with environment variable values
fn substitute_env_vars(config_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")?;

    let result = re.replace_all(config_str, |caps: &regex::Captures| {
        let var_name = &caps[1];
        match std::env::var(var_name) {
            Ok(value) => value,
            Err(_) => {
                tracing::warn!(
                    "Environment variable '{}' referenced in config but not set. Keeping placeholder.",
                    var_name
                );
                caps[0].to_string()
            }
        }
    });

    Ok(result.to_string())
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

            let config_str = std::fs::read_to_string(&path_buf)?;
            let substituted = substitute_env_vars(&config_str)?;

            let config_value: serde_yaml::Value = serde_yaml::from_str(&substituted)?;
            builder = builder.add_source(
                config::File::from_str(
                    &serde_yaml::to_string(&config_value)?,
                    config::FileFormat::Yaml,
                )
                .required(true),
            );
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
    source_path: "features_{feature}"
    max_connections: 20
    timeout_seconds: 30
"#;
        let config: Config =
            serde_yaml::from_str(yaml).expect("Failed to parse YAML with PostgreSQL");
        assert_eq!(config.adapters.len(), 1);

        if let AdapterConfig::Postgres {
            connection_string,
            source_path,
            max_connections,
            timeout_seconds,
            tls: _,
            parameters: _,
        } = config.adapters.get("postgres_adapter").unwrap()
        {
            assert_eq!(
                connection_string,
                "postgresql://user:password@localhost:5432/features"
            );
            if let SourcePath::Template(template) = source_path {
                assert_eq!(template, "features_{feature}");
            } else {
                panic!("Expected template source path");
            }
            assert_eq!(*max_connections, Some(20));
            assert_eq!(*timeout_seconds, Some(30));
        } else {
            panic!("Expected PostgreSQL adapter config");
        }
    }

    #[test]
    fn test_parse_structured_source_path() {
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
        - name: age
          arrow_type: int64
          source: postgres_adapter
          source_path:
            table: "user_demographics"
            column: "age"
        - name: country
          arrow_type: string
          source: postgres_adapter
          source_path:
            table: "user_demographics"

adapters:
  postgres_adapter:
    type: postgres
    connection_string: "postgresql://user:password@localhost:5432/features"
    source_path: "features_{feature}"
"#;
        let config: Config =
            serde_yaml::from_str(yaml).expect("Failed to parse structured source path");

        let columns = match &config.registry {
            RegistryConfig::Static { views } => &views[0].columns,
        };

        if let Some(SourcePath::Structured { table, column }) = &columns[0].source_path {
            assert_eq!(table, "user_demographics");
            assert_eq!(column, &Some("age".to_string()));
        } else {
            panic!("Expected structured source path for first column");
        }

        if let Some(SourcePath::Structured { table, column }) = &columns[1].source_path {
            assert_eq!(table, "user_demographics");
            assert_eq!(column, &None);
        } else {
            panic!("Expected structured source path for second column");
        }
    }

    #[test]
    fn test_column_level_source_path_integration() {
        let yaml = r#"
server:
  host: 127.0.0.1
  port: 9000

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: age
          arrow_type: int64
          source: postgres_adapter
          source_path:
            table: "user_demographics"
            column: "age"
        - name: country
          arrow_type: string
          source: postgres_adapter
          source_path:
            table: "user_demographics"
        - name: default_feature
          arrow_type: float64
          source: postgres_adapter

adapters:
  postgres_adapter:
    type: postgres
    connection_string: "postgresql://user:password@localhost:5432/features"
    source_path: "default_features_{feature}"
    max_connections: 20
    timeout_seconds: 30
"#;
        let config: Config =
            serde_yaml::from_str(yaml).expect("Failed to parse column-level config");

        let views = match &config.registry {
            RegistryConfig::Static { views } => views,
        };

        let user_features = &views[0];
        assert_eq!(user_features.name, "user_features");
        assert_eq!(user_features.columns.len(), 3);

        let age_column = &user_features.columns[0];
        assert_eq!(age_column.name, "age");
        if let Some(SourcePath::Structured { table, column }) = &age_column.source_path {
            assert_eq!(table, "user_demographics");
            assert_eq!(column, &Some("age".to_string()));
        } else {
            panic!("Expected structured source path for age column");
        }

        let country_column = &user_features.columns[1];
        assert_eq!(country_column.name, "country");
        if let Some(SourcePath::Structured { table, column }) = &country_column.source_path {
            assert_eq!(table, "user_demographics");
            assert_eq!(column, &None);
        } else {
            panic!("Expected structured source path for country column");
        }

        let default_column = &user_features.columns[2];
        assert_eq!(default_column.name, "default_feature");
        assert!(default_column.source_path.is_none());

        if let AdapterConfig::Postgres { source_path, .. } =
            config.adapters.get("postgres_adapter").unwrap()
        {
            if let SourcePath::Template(template) = source_path {
                assert_eq!(template, "default_features_{feature}");
            } else {
                panic!("Expected template source path for adapter");
            }
        } else {
            panic!("Expected PostgreSQL adapter config");
        }
    }

    #[test]
    fn test_env_var_substitution() {
        unsafe {
            std::env::set_var("TEST_PASSWORD", "secret123");
            std::env::set_var("TEST_REGION", "us-west-2");
        }

        let config_str = r#"
server:
  host: 0.0.0.0
  port: 8815

registry:
  type: static
  views: []

adapters:
  postgres_adapter:
    type: postgres
    connection_string: "postgresql://user:${TEST_PASSWORD}@localhost:5432/db"
  clickhouse_adapter:
    type: clickhouse
    connection_string: "clickhouse://default:${TEST_PASSWORD}@localhost:9000/quiver"
    source_path: "features"
"#;

        let result = substitute_env_vars(config_str).expect("Substitution failed");

        assert!(result.contains("secret123"));
        assert!(result.contains("postgresql://user:secret123@localhost"));
        assert!(result.contains("clickhouse://default:secret123@localhost"));
        assert!(!result.contains("${TEST_PASSWORD}"));

        unsafe {
            std::env::remove_var("TEST_PASSWORD");
            std::env::remove_var("TEST_REGION");
        }
    }

    #[test]
    fn test_env_var_substitution_missing_var() {
        let config_str = r#"
server:
  host: 0.0.0.0
  port: 8815

adapters:
  postgres_adapter:
    type: postgres
    connection_string: "postgresql://user:${NONEXISTENT_VAR}@localhost/db"
"#;

        // Should not fail, just keep the placeholder
        let result = substitute_env_vars(config_str).expect("Substitution should not fail");
        assert!(result.contains("${NONEXISTENT_VAR}"));
    }

    #[test]
    fn test_env_var_substitution_multiple_in_line() {
        unsafe {
            std::env::set_var("USER", "testuser");
            std::env::set_var("PASS", "testpass");
            std::env::set_var("HOST", "localhost");
        }

        let config_str = r#"connection_string: "${USER}:${PASS}@${HOST}:5432""#;
        let result = substitute_env_vars(config_str).expect("Substitution failed");

        assert_eq!(
            result,
            r#"connection_string: "testuser:testpass@localhost:5432""#
        );

        unsafe {
            std::env::remove_var("USER");
            std::env::remove_var("PASS");
            std::env::remove_var("HOST");
        }
    }
}
