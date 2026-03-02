use crate::proto::quiver::v1::FeatureViewMetadata;
use dashmap::DashMap;

#[async_trait::async_trait]
pub trait Registry: Send + Sync {
    async fn get_view(&self, name: &str) -> Result<FeatureViewMetadata, RegistryError>;
    async fn list_views(&self) -> Result<Vec<String>, RegistryError>;
}

pub struct StaticRegistry {
    views: DashMap<String, FeatureViewMetadata>,
}

impl Default for StaticRegistry {
    fn default() -> Self {
        Self {
            views: DashMap::new(),
        }
    }
}

impl StaticRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, view: FeatureViewMetadata) {
        self.views.insert(view.name.clone(), view);
    }
}

#[async_trait::async_trait]
impl Registry for StaticRegistry {
    async fn get_view(&self, name: &str) -> Result<FeatureViewMetadata, RegistryError> {
        self.views
            .get(name)
            .map(|v| v.value().clone())
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))
    }

    async fn list_views(&self) -> Result<Vec<String>, RegistryError> {
        Ok(self.views.iter().map(|kv| kv.key().clone()).collect())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("Feature view not found: {0}")]
    NotFound(String),
    #[error("Internal registry error: {0}")]
    Internal(String),
}
