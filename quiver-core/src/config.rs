use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub registry: RegistryConfig,
    pub adapters: HashMap<String, AdapterConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
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
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8815
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AdapterConfig {
    Memory,
    Redis {
        connection: String,
        password: Option<String>,
        #[serde(default = "default_redis_template")]
        key_template: String,
    },
}

fn default_redis_template() -> String {
    "quiver:f:{feature}:e:{entity}".to_string()
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
            // Default search path: look for config.yaml, config.json etc. in the current directory
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
}
