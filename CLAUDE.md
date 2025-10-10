# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`kfcli` is a Rust-based command-line interface tool for monitoring and managing Apache Kafka clusters. It provides commands to inspect topics, brokers, consumer groups, and perform administrative operations.

## Building and Testing

### Build Commands
```bash
# Standard build
cargo build

# Release build
cargo build --release

# Build for specific target (Linux)
cargo build --release --target x86_64-unknown-linux-gnu

# Build for macOS (x86_64 only currently supported)
cargo build --release --target x86_64-apple-darwin
```

### Running Tests
```bash
# Run all tests
cargo test --verbose

# Run specific test
cargo test test_name
```

**Note:** Integration tests require a running Kafka broker at `localhost:9092`. The CI setup uses Docker to spin up a Kafka instance with pre-created topics (`topic-one`, `topic-two`, `topic-three`).

### Platform-Specific Dependencies

**Ubuntu/Linux:**
```bash
sudo apt-get update
sudo apt-get install -y clang libclang-dev llvm-dev
```

**macOS:**
```bash
brew install openssl librdkafka pkg-config zstd lz4 zlib
```

Set required environment variables for macOS:
```bash
export OPENSSL_DIR=$(brew --prefix openssl@3)
export PKG_CONFIG_PATH="$OPENSSL_DIR/lib/pkgconfig:$(brew --prefix librdkafka)/lib/pkgconfig"
```

## Architecture

### Module Structure

- **`cli.rs`**: Command-line interface definition using `clap`. Defines all commands, subcommands, and arguments. Handles shell completion generation.

- **`config.rs`**: Environment configuration management. Handles reading/writing config files at `~/.config/kfcli/config.toml`, managing multiple Kafka environments, and activating specific environments.

- **`kafka.rs`**: Core Kafka operations using `rdkafka`. Contains all business logic for:
  - Topic operations (list, details, tail)
  - Broker monitoring
  - Consumer group inspection and lag calculation
  - Admin operations (create/delete topics, add partitions)
  - Custom binary deserialization for Kafka consumer group assignments

- **`main.rs`**: Application entry point. Routes CLI commands to appropriate handlers and manages error handling.

### Configuration System

The tool uses a TOML-based configuration system stored at `~/.config/kfcli/config.toml`:

```toml
[dev]
brokers = "localhost:9092"
is_default = true

[prod]
brokers = "prodhost:9092"
is_default = false
```

- Users can manage multiple Kafka environments
- One environment is marked as default/active (`is_default = true`)
- Use `kfcli config --activate <env>` to switch environments
- Use `kfcli config --setup` to add new environments interactively
- Use `kfcli config` to view current active environment and all configured environments

### Key Implementation Details

**Custom Async Executor**: The codebase implements a minimal async executor (`block_on` function in `kafka.rs`) to avoid pulling in Tokio for simple async operations required by the `rdkafka` admin API.

**Consumer Group Assignment Deserialization**: Custom binary protocol parser (`deserialize_assignment`) that reads Kafka's wire format for consumer group partition assignments (version, topic count, topic names, partition lists).

**Filter System**: JSON-based filtering for topic tailing supports dot-notation path expressions (e.g., `data.attributes.name=19`) to filter messages by nested field values.

**Watermark-based Tailing**: The `tail` command can start consumption from a specific offset using the `--before` flag, calculated relative to the high watermark of each partition.

## Development Workflow

### Adding New Commands

1. Define command structure in `cli.rs` (add to `Command` enum and create argument struct)
2. Add command handler in `main.rs` (`handle_command` function)
3. Implement business logic in `kafka.rs` (create public function)
4. Consider if configuration is needed (add to `config.rs` if yes)

### Error Handling

- Use `thiserror` for custom error types (`KafkaError`, `ConfigError`)
- Propagate errors using `?` operator
- `main.rs` catches all errors and prints them to stderr before exiting with code 1

### Testing Strategy

- Unit tests in each module using `#[cfg(test)]`
- Tests that require Kafka check for connection errors and gracefully skip if unavailable
- Mock data for binary protocol parsing tests
- CI runs full test suite against live Kafka instance
