use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ValidationConfig {
    #[serde(default)]
    pub request_validations: Vec<RequestValidation>,
    #[serde(default)]
    pub response_validations: Vec<ResponseValidation>,
    #[serde(default)]
    pub skip_all_validation: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RequestValidation {
    FeatureNameFormat,
    EntityIdFormat,
    SqlIdentifierSafety,
    RedisKeySafety,
    BatchSizeLimit,
    TimeoutBounds,
    EntityIdsNotEmpty,
    FeatureNamesNotEmpty,
    MemoryConstraints,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseValidation {
    SchemaConsistency,
    DataTypeValidation,
    NullValuePolicies,
    TimestampFormat,
    RecordBatchStructure,
}

impl ValidationConfig {
    pub fn should_validate_request(&self, validation: &RequestValidation) -> bool {
        if self.skip_all_validation {
            return false;
        }
        self.request_validations.contains(validation)
    }
}
