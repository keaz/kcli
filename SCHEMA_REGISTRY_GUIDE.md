# Schema Registry Integration Guide

## Overview

kfcli now supports integration with Confluent Schema Registry for Avro schema management and message deserialization. This feature enables:

- List all schemas in the registry
- Retrieve schema definitions by subject and version
- Automatic Avro message deserialization using schema ID
- Schema caching for improved performance

## Configuration

### Adding Schema Registry to Your Environment

Edit your `~/.config/kfcli/config.toml` file to add schema registry configuration:

```toml
[dev]
brokers = "localhost:9092"
is_default = true

[dev.schema_registry]
url = "http://localhost:8081"
timeout_secs = 30
```

### Configuration Options

- `url`: Schema Registry URL (default: `http://localhost:8081`)
- `timeout_secs`: HTTP request timeout in seconds (default: 30)

## Commands

### List All Subjects

List all schema subjects registered in the Schema Registry:

```bash
kfcli schema list
```

**Example Output:**
```
Subjects:
  users-value
  orders-value
  products-value
```

### Get Schema by Subject

Retrieve a specific schema by subject name and version:

```bash
# Get latest version
kfcli schema get --subject users-value

# Get specific version
kfcli schema get --subject users-value --version 1
```

**Example Output:**
```
Subject: users-value
Version: 2
Schema ID: 42

Schema:
{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"email","type":"string"}]}
```

## Programmatic Usage

### Creating a Schema Registry Client

```rust
use kfcli::schema::{SchemaRegistryClient, SchemaRegistryConfig};

let config = SchemaRegistryConfig {
    url: "http://localhost:8081".to_string(),
    timeout_secs: 30,
};

let mut client = SchemaRegistryClient::new(&config)?;
```

### Listing Subjects

```rust
let subjects = client.list_subjects()?;
for subject in subjects {
    println!("Subject: {}", subject);
}
```

### Getting a Schema

```rust
// Get latest schema
let schema_info = client.get_latest_schema("users-value")?;
println!("Schema ID: {}", schema_info.id);
println!("Version: {}", schema_info.version);
println!("Schema: {}", schema_info.schema);

// Get specific version
let schema_info = client.get_schema_by_subject("users-value", "1")?;
```

### Deserializing Avro Messages

```rust
// Message format: [magic_byte(0)][schema_id(4 bytes)][avro_data]
let message: Vec<u8> = /* ... from Kafka ... */;

let value = client.deserialize_avro(&message)?;
println!("Deserialized: {:?}", value);
```

## Avro Message Format

Avro messages from Kafka (when using Confluent's Avro serializer) follow this wire format:

```
[0x00][schema_id: 4 bytes big-endian][avro_binary_data]
```

- **Byte 0**: Magic byte (always 0x00)
- **Bytes 1-4**: Schema ID (32-bit integer, big-endian)
- **Bytes 5+**: Avro-encoded data

The schema registry client automatically:
1. Extracts the schema ID from bytes 1-4
2. Fetches the corresponding schema from the registry
3. Uses the schema to deserialize the Avro data
4. Caches schemas to avoid repeated registry calls

## Architecture

### Schema Registry Client

The `SchemaRegistryClient` provides:

- **HTTP Client**: Uses `reqwest` for HTTP communication
- **Schema Cache**: In-memory cache (HashMap) for retrieved schemas
- **Error Handling**: Comprehensive error types for different failure scenarios

### Error Types

```rust
pub enum SchemaRegistryError {
    HttpError(reqwest::Error),
    SchemaNotFound(String, String),
    InvalidUrl(String),
    SchemaParseError(String),
    AvroError(apache_avro::Error),
    JsonError(serde_json::Error),
    InvalidMessageFormat,
}
```

### Dependencies

- **apache-avro 0.17.0**: Avro schema parsing and deserialization
- **reqwest 0.12.0**: HTTP client for Schema Registry API
- **url 2.5.0**: URL parsing and validation

## Testing

### Unit Tests

The implementation includes comprehensive unit tests:

```bash
cargo test schema
```

**Test Coverage:**
- Configuration serialization/deserialization
- URL validation
- Client creation
- Message format validation
- Invalid input handling

### Integration Testing

To test with a real Schema Registry:

1. Start Confluent Schema Registry:
   ```bash
   docker run -p 8081:8081 \
     -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
     -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092 \
     confluentinc/cp-schema-registry:latest
   ```

2. Register a test schema:
   ```bash
   curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"schema":"{\"type\":\"string\"}"}' \
     http://localhost:8081/subjects/test-value/versions
   ```

3. Test kfcli commands:
   ```bash
   kfcli schema list
   kfcli schema get --subject test-value
   ```

## Use Cases

### 1. Schema Discovery

Quickly discover what schemas are available in your registry:

```bash
kfcli schema list
```

### 2. Schema Inspection

View schema definitions before consuming messages:

```bash
kfcli schema get --subject users-value
```

### 3. Message Deserialization

Deserialize Avro-encoded messages from Kafka topics (future enhancement to tail command).

### 4. Schema Evolution Tracking

Check different versions of a schema:

```bash
kfcli schema get --subject users-value --version 1
kfcli schema get --subject users-value --version 2
```

## Limitations

1. **Avro Only**: Currently supports Avro schemas. Protobuf and JSON Schema support planned for future releases.
2. **Read-Only**: Schema registration/updates not yet implemented.
3. **No Authentication**: Basic HTTP only. HTTPS and authentication support coming soon.
4. **Cache Management**: No TTL or size limits on schema cache yet.

## Future Enhancements

- [ ] Schema registration and updates
- [ ] Protobuf schema support
- [ ] JSON Schema support
- [ ] Schema Registry authentication (Basic Auth, API Key)
- [ ] TLS/SSL support
- [ ] Schema compatibility checks
- [ ] Integration with tail command for automatic Avro deserialization
- [ ] Schema cache TTL and eviction
- [ ] Schema validation

## Troubleshooting

### "Schema registry not configured"

**Error**: `Schema registry not configured. Add schema_registry config to your environment.`

**Solution**: Add schema_registry configuration to your environment in `~/.config/kfcli/config.toml`:

```toml
[dev.schema_registry]
url = "http://localhost:8081"
timeout_secs = 30
```

### "HTTP request failed"

**Error**: `HTTP request failed: ...`

**Possible Causes**:
- Schema Registry is not running
- Incorrect URL
- Network connectivity issues
- Firewall blocking access

**Solution**: Verify Schema Registry is accessible:
```bash
curl http://localhost:8081/subjects
```

### "Schema not found"

**Error**: `Schema not found: subject=..., version=...`

**Possible Causes**:
- Subject doesn't exist
- Version doesn't exist
- Typo in subject name

**Solution**: List all subjects first:
```bash
kfcli schema list
```

### "Invalid message format"

**Error**: `Invalid Avro message format`

**Possible Causes**:
- Message is not Avro-encoded
- Message doesn't follow Confluent wire format
- Message is corrupted

**Solution**: Ensure messages are produced using Confluent's Avro serializer.

## Examples

### Complete Workflow

```bash
# 1. Configure schema registry
cat >> ~/.config/kfcli/config.toml << EOF
[dev.schema_registry]
url = "http://localhost:8081"
timeout_secs = 30
EOF

# 2. List all subjects
kfcli schema list

# 3. Get schema details
kfcli schema get --subject users-value

# 4. Get specific version
kfcli schema get --subject users-value --version 1
```

### Multiple Environments

```toml
[dev]
brokers = "localhost:9092"
is_default = true

[dev.schema_registry]
url = "http://localhost:8081"
timeout_secs = 30

[prod]
brokers = "prod-kafka:9092"
is_default = false

[prod.schema_registry]
url = "https://prod-schema-registry:8081"
timeout_secs = 60
```

Switch between environments:
```bash
kfcli config --activate prod
kfcli schema list
```

## API Reference

### SchemaRegistryConfig

```rust
pub struct SchemaRegistryConfig {
    pub url: String,
    pub timeout_secs: u64,
}
```

### SchemaRegistryClient

```rust
pub struct SchemaRegistryClient {
    // ...
}

impl SchemaRegistryClient {
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, SchemaRegistryError>;
    pub fn get_schema_by_id(&mut self, schema_id: i32) -> Result<Schema, SchemaRegistryError>;
    pub fn get_latest_schema(&mut self, subject: &str) -> Result<SchemaVersionResponse, SchemaRegistryError>;
    pub fn get_schema_by_subject(&mut self, subject: &str, version: &str) -> Result<SchemaVersionResponse, SchemaRegistryError>;
    pub fn list_subjects(&self) -> Result<Vec<String>, SchemaRegistryError>;
    pub fn deserialize_avro(&mut self, message: &[u8]) -> Result<apache_avro::types::Value, SchemaRegistryError>;
}
```

### SchemaVersionResponse

```rust
pub struct SchemaVersionResponse {
    pub id: i32,
    pub schema: String,
    pub subject: String,
    pub version: i32,
}
```

## Performance Considerations

### Schema Caching

The client maintains an in-memory cache of schemas by ID. This means:
- **First access**: Fetches from registry (network call)
- **Subsequent access**: Served from cache (instant)

### HTTP Timeouts

Configure appropriate timeouts based on your network:
- **Local development**: 30 seconds (default)
- **Production**: 10-15 seconds recommended
- **Slow networks**: Increase as needed

### Concurrent Access

The client is not thread-safe. For concurrent usage:
```rust
use std::sync::{Arc, Mutex};

let client = Arc::new(Mutex::new(SchemaRegistryClient::new(&config)?));
```

## Security Best Practices

1. **Use HTTPS** in production (when implemented)
2. **Network isolation**: Keep Schema Registry on private network
3. **Access control**: Limit who can access Schema Registry
4. **Audit logging**: Monitor schema registry access
5. **Backup schemas**: Regular backups of registry data

---

For more information, see:
- [Confluent Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Apache Avro Documentation](https://avro.apache.org/docs/)
- [kfcli TASKS.md](TASKS.md) for upcoming features
