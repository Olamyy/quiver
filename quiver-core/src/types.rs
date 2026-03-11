use arrow::datatypes::{DataType, TimeUnit};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum QuiverType {
    Float64,
    Int64,
    String,
    Boolean,
    Timestamp { timezone: Option<Arc<str>> },
}

#[derive(Debug, Clone)]
pub struct TypeError {
    pub message: String,
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TypeError {}

impl QuiverType {
    pub fn from_config_string(s: &str) -> Result<Self, TypeError> {
        match s.to_lowercase().as_str() {
            "float64" | "double" => Ok(QuiverType::Float64),
            "int64" | "long" => Ok(QuiverType::Int64),
            "string" | "utf8" => Ok(QuiverType::String),
            "bool" | "boolean" => Ok(QuiverType::Boolean),
            "timestamp" => Ok(QuiverType::Timestamp {
                timezone: Some(Arc::from("UTC")),
            }),
            other => Err(TypeError {
                message: format!(
                    "Unsupported arrow type: '{}'. Supported types: float64, int64, string, bool, timestamp",
                    other
                ),
            }),
        }
    }

    pub fn to_arrow(&self) -> DataType {
        match self {
            QuiverType::Float64 => DataType::Float64,
            QuiverType::Int64 => DataType::Int64,
            QuiverType::String => DataType::Utf8,
            QuiverType::Boolean => DataType::Boolean,
            QuiverType::Timestamp { timezone } => {
                DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone())
            }
        }
    }

    pub fn from_arrow(dtype: &DataType) -> Result<Self, TypeError> {
        match dtype {
            DataType::Float64 => Ok(QuiverType::Float64),
            DataType::Int64 => Ok(QuiverType::Int64),
            DataType::Utf8 => Ok(QuiverType::String),
            DataType::Boolean => Ok(QuiverType::Boolean),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => Ok(QuiverType::Timestamp {
                timezone: tz.clone(),
            }),
            other => Err(TypeError {
                message: format!("Cannot convert Arrow type {:?} to QuiverType", other),
            }),
        }
    }

    pub fn to_python_type(&self) -> String {
        match self {
            QuiverType::Float64 => "float".to_string(),
            QuiverType::Int64 => "int".to_string(),
            QuiverType::String => "str".to_string(),
            QuiverType::Boolean => "bool".to_string(),
            QuiverType::Timestamp { .. } => "datetime".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_config_string() {
        assert_eq!(
            QuiverType::from_config_string("float64").unwrap(),
            QuiverType::Float64
        );
        assert_eq!(
            QuiverType::from_config_string("double").unwrap(),
            QuiverType::Float64
        );
        assert_eq!(
            QuiverType::from_config_string("int64").unwrap(),
            QuiverType::Int64
        );
        assert_eq!(
            QuiverType::from_config_string("long").unwrap(),
            QuiverType::Int64
        );
        assert_eq!(
            QuiverType::from_config_string("string").unwrap(),
            QuiverType::String
        );
        assert_eq!(
            QuiverType::from_config_string("utf8").unwrap(),
            QuiverType::String
        );
        assert_eq!(
            QuiverType::from_config_string("bool").unwrap(),
            QuiverType::Boolean
        );
        assert_eq!(
            QuiverType::from_config_string("boolean").unwrap(),
            QuiverType::Boolean
        );

        let ts = QuiverType::from_config_string("timestamp").unwrap();
        assert!(matches!(ts, QuiverType::Timestamp { timezone: Some(_) }));
    }

    #[test]
    fn test_invalid_type() {
        assert!(QuiverType::from_config_string("invalid").is_err());
        assert!(QuiverType::from_config_string("decimal").is_err());
    }

    #[test]
    fn test_to_arrow() {
        assert_eq!(QuiverType::Float64.to_arrow(), DataType::Float64);
        assert_eq!(QuiverType::Int64.to_arrow(), DataType::Int64);
        assert_eq!(QuiverType::String.to_arrow(), DataType::Utf8);
        assert_eq!(QuiverType::Boolean.to_arrow(), DataType::Boolean);

        let ts = QuiverType::Timestamp {
            timezone: Some(Arc::from("UTC")),
        };
        assert_eq!(
            ts.to_arrow(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn test_from_arrow_roundtrip() {
        let types = vec![
            QuiverType::Float64,
            QuiverType::Int64,
            QuiverType::String,
            QuiverType::Boolean,
            QuiverType::Timestamp {
                timezone: Some(Arc::from("UTC")),
            },
        ];

        for quiver_type in types {
            let arrow_type = quiver_type.to_arrow();
            let roundtrip = QuiverType::from_arrow(&arrow_type).unwrap();
            assert_eq!(quiver_type, roundtrip);
        }
    }

    #[test]
    fn test_to_python_type() {
        assert_eq!(QuiverType::Float64.to_python_type(), "float");
        assert_eq!(QuiverType::Int64.to_python_type(), "int");
        assert_eq!(QuiverType::String.to_python_type(), "str");
        assert_eq!(QuiverType::Boolean.to_python_type(), "bool");
        assert_eq!(
            QuiverType::Timestamp {
                timezone: Some(Arc::from("UTC"))
            }
            .to_python_type(),
            "datetime"
        );
    }
}
