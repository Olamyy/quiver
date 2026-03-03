use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub registry: RegistryConfig,
    pub adapters: HashMap<String, AdapterConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
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
    },
}

impl Config {
    pub fn load() -> Result<Self, config::ConfigError> {
        let s = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 8815)?
            .add_source(config::File::with_name("config").required(true))
            .add_source(config::Environment::with_prefix("QUIVER").separator("__"))
            .build()?;

        s.try_deserialize()
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
