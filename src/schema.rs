use apache_avro::Schema;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum SchemaRegistryError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Schema not found: subject={0}, version={1}")]
    SchemaNotFound(String, String),

    #[error("Invalid schema registry URL: {0}")]
    InvalidUrl(String),

    #[error("Schema parsing error: {0}")]
    SchemaParseError(String),

    #[error("Avro deserialization error: {0}")]
    AvroError(#[from] apache_avro::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid Avro message format")]
    InvalidMessageFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub url: String,
    #[serde(default)]
    pub timeout_secs: u64,
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8081".to_string(),
            timeout_secs: 30,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SchemaResponse {
    pub schema: String,
}

#[derive(Debug, Deserialize)]
pub struct SchemaVersionResponse {
    pub id: i32,
    pub schema: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Deserialize)]
struct SubjectsResponse {
    #[serde(flatten)]
    subjects: Vec<String>,
}

pub struct SchemaRegistryClient {
    base_url: Url,
    client: Client,
    cache: HashMap<i32, Schema>,
}

impl std::fmt::Debug for SchemaRegistryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistryClient")
            .field("base_url", &self.base_url)
            .field("cache_size", &self.cache.len())
            .finish()
    }
}

impl SchemaRegistryClient {
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let base_url = Url::parse(&config.url)
            .map_err(|_| SchemaRegistryError::InvalidUrl(config.url.clone()))?;

        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;

        Ok(Self {
            base_url,
            client,
            cache: HashMap::new(),
        })
    }

    /// Get schema by ID
    pub fn get_schema_by_id(&mut self, schema_id: i32) -> Result<Schema, SchemaRegistryError> {
        // Check cache first
        if let Some(schema) = self.cache.get(&schema_id) {
            return Ok(schema.clone());
        }

        let url = self
            .base_url
            .join(&format!("/schemas/ids/{}", schema_id))
            .map_err(|e| SchemaRegistryError::InvalidUrl(e.to_string()))?;

        let response = self.client.get(url).send()?;

        if !response.status().is_success() {
            return Err(SchemaRegistryError::SchemaNotFound(
                schema_id.to_string(),
                "N/A".to_string(),
            ));
        }

        let schema_response: SchemaResponse = response.json()?;
        let schema = Schema::parse_str(&schema_response.schema)
            .map_err(|e| SchemaRegistryError::SchemaParseError(e.to_string()))?;

        // Cache the schema
        self.cache.insert(schema_id, schema.clone());

        Ok(schema)
    }

    /// Get latest schema for a subject
    pub fn get_latest_schema(
        &mut self,
        subject: &str,
    ) -> Result<SchemaVersionResponse, SchemaRegistryError> {
        let url = self
            .base_url
            .join(&format!("/subjects/{}/versions/latest", subject))
            .map_err(|e| SchemaRegistryError::InvalidUrl(e.to_string()))?;

        let response = self.client.get(url).send()?;

        if !response.status().is_success() {
            return Err(SchemaRegistryError::SchemaNotFound(
                subject.to_string(),
                "latest".to_string(),
            ));
        }

        Ok(response.json()?)
    }

    /// Get schema by subject and version
    pub fn get_schema_by_subject(
        &mut self,
        subject: &str,
        version: &str,
    ) -> Result<SchemaVersionResponse, SchemaRegistryError> {
        let url = self
            .base_url
            .join(&format!("/subjects/{}/versions/{}", subject, version))
            .map_err(|e| SchemaRegistryError::InvalidUrl(e.to_string()))?;

        let response = self.client.get(url).send()?;

        if !response.status().is_success() {
            return Err(SchemaRegistryError::SchemaNotFound(
                subject.to_string(),
                version.to_string(),
            ));
        }

        Ok(response.json()?)
    }

    /// List all subjects
    pub fn list_subjects(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let url = self
            .base_url
            .join("/subjects")
            .map_err(|e| SchemaRegistryError::InvalidUrl(e.to_string()))?;

        let response = self.client.get(url).send()?;

        if !response.status().is_success() {
            return Ok(Vec::new());
        }

        let subjects: Vec<String> = response.json()?;
        Ok(subjects)
    }

    /// Deserialize Avro message using schema ID from message
    pub fn deserialize_avro(
        &mut self,
        message: &[u8],
    ) -> Result<apache_avro::types::Value, SchemaRegistryError> {
        if message.len() < 5 {
            return Err(SchemaRegistryError::InvalidMessageFormat);
        }

        // First byte should be 0 (magic byte)
        if message[0] != 0 {
            return Err(SchemaRegistryError::InvalidMessageFormat);
        }

        // Next 4 bytes are schema ID (big-endian)
        let schema_id = i32::from_be_bytes([message[1], message[2], message[3], message[4]]);

        // Get schema from registry
        let schema = self.get_schema_by_id(schema_id)?;

        // Deserialize the rest of the message
        let value = apache_avro::from_avro_datum(&schema, &mut &message[5..], None)?;

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry_config_default() {
        let config = SchemaRegistryConfig::default();
        assert_eq!(config.url, "http://localhost:8081");
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_schema_registry_config_custom() {
        let config = SchemaRegistryConfig {
            url: "http://custom:8081".to_string(),
            timeout_secs: 60,
        };
        assert_eq!(config.url, "http://custom:8081");
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_invalid_url() {
        let config = SchemaRegistryConfig {
            url: "not a valid url".to_string(),
            timeout_secs: 30,
        };
        let result = SchemaRegistryClient::new(&config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SchemaRegistryError::InvalidUrl(_)));
    }

    #[test]
    fn test_valid_client_creation() {
        let config = SchemaRegistryConfig::default();
        let result = SchemaRegistryClient::new(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_message_format_too_short() {
        let config = SchemaRegistryConfig::default();
        let mut client = SchemaRegistryClient::new(&config).unwrap();

        let message = vec![0, 1, 2]; // Too short
        let result = client.deserialize_avro(&message);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SchemaRegistryError::InvalidMessageFormat));
    }

    #[test]
    fn test_invalid_message_format_wrong_magic_byte() {
        let config = SchemaRegistryConfig::default();
        let mut client = SchemaRegistryClient::new(&config).unwrap();

        let message = vec![1, 0, 0, 0, 1, 0]; // Wrong magic byte
        let result = client.deserialize_avro(&message);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SchemaRegistryError::InvalidMessageFormat));
    }

    #[test]
    fn test_serialize_deserialize_config() {
        let config = SchemaRegistryConfig {
            url: "http://test:8081".to_string(),
            timeout_secs: 45,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SchemaRegistryConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.url, deserialized.url);
        assert_eq!(config.timeout_secs, deserialized.timeout_secs);
    }
}
