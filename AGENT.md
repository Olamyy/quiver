# Quiver Agent Guidelines

This document provides coding agents with essential information for working effectively in the Quiver codebase.

## Project Overview

Quiver is a high-performance gRPC server for feature serving in ML environments, built in Rust with Python client bindings. It uses Apache Arrow Flight protocol for zero-copy data transfer and supports multiple backend adapters (Memory, Redis, DuckDB, RonDB etc).

## Build, Lint & Test Commands

**IMPORTANT**: Use `make` commands instead of raw `cargo` commands for consistent development workflow.

### Quick Reference

```bash
make help                               # Show all available commands
make main                               # Run quality checks and tests (main workflow)
make install                           # Install dependencies
```

### Rust (quiver-core)

```bash
# Build and check  
make check                             # Quick syntax check
make build                             # Full build
make build-release                     # Optimized build

# Testing
make test-rs                           # Run all Rust tests
make test-rs-single TEST=test_name     # Run specific test
make test-rs-integration               # Run integration tests only
make testcov                          # Run tests with coverage (HTML report)

# Advanced testing (raw cargo if needed)
RUST_BACKTRACE=1 cargo test test_name  # Run with detailed backtraces
cargo test --release --all-features    # Run tests in release mode
cargo test --all-features -- --test-threads=1  # Single-threaded testing
cargo test -p quiver-core --test memory_adapter_tests  # Run specific integration test file

# Code quality
make format-rs                         # Format Rust code
make lint-rs                          # Lint with clippy (treats warnings as errors)
make quality                          # Format and lint all code
```

### Python (quiver-python)

```bash
# Development setup
make install-py                        # Install Python dependencies
make dev-py                           # Build and install Python package for development
make dev-py-release                   # Build Python package (release mode)

# Testing and quality
make pytest                           # Run Python tests
make format-py                        # Format Python code with ruff
make lint-py                         # Lint Python code with ruff
```
ruff check                              # Lint Python code
ruff format                             # Format Python code
```

### Protocol Buffers

```bash
# Regenerate protobuf bindings (from project root)
make proto-gen                          # Triggers proto generation via build.rs
make proto-check                        # Validate protocol buffer definitions
```

## Code Style Guidelines

### Rust Code Style

#### Import Organization
- **External crates first**, then **std imports**, then **internal imports**
- Group with blank lines between sections
- Use explicit imports, avoid glob imports

```rust
// External crates
use arrow_flight::{FlightData, FlightService};
use tonic::{Request, Response, Status};

// std imports  
use std::collections::HashMap;

// Internal imports
use crate::adapters::BackendAdapter;
use crate::proto::quiver::v1::FeatureRequest;
```

#### Naming Conventions
- **Modules**: `snake_case` (e.g., `memory.rs`, `redis.rs`)
- **Structs/Enums**: `PascalCase` (e.g., `QuiverFlightServer`, `AdapterError`)
- **Functions/Variables**: `snake_case` (e.g., `build_key`, `resolve_one`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `BACKEND_NAME`)

#### Type Usage
- Use `Arc<dyn Trait>` for thread-safe trait objects
- Prefer `DashMap` for concurrent hash maps
- Use `#[async_trait::async_trait]` for async traits
- Always use `Result<T, CustomError>` for fallible operations

#### Comments and Documentation
- **Avoid unnecessary comments**: If code is self-explanatory, don't add comments
- **Use doc comments when needed**: Prefer `///` doc comments over `//` for necessary explanations
- **Never add emojis**: Code should never contain emojis for any reason
- **Follow RFC design**: Architecture decisions are documented in `quiver-rfc-v2.md`

#### Lint Requirements
- **Never use `#[allow(...)]`**: Always use `#[expect(...)]` so unnecessary markers are caught
- **Function style**: Prefer `fn my_func(param: impl MyTrait)` over `fn my_func<T: MyTrait>(..., param: T)`
- **Import style**: Import types directly rather than using full paths (e.g., `use std::borrow::Cow;` then `Cow::Owned(...)`)

```rust
// Good - Use expect, not allow
#[expect(clippy::too_many_arguments)]
pub fn complex_function() { }

// Bad - allow doesn't catch unnecessary markers
#[allow(clippy::too_many_arguments)]
pub fn complex_function() { }

// Good - impl Trait syntax
fn process_data(adapter: impl BackendAdapter) { }

// Bad - Generic bound syntax  
fn process_data<T: BackendAdapter>(adapter: T) { }
```

#### Error Handling
All error types must use `thiserror`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("[{backend}] Internal error: {message}")]
    Internal { backend: String, message: String },
    
    #[error("[{backend}] Entity not found: {entity_id}")]
    NotFound { backend: String, entity_id: String },
}

impl AdapterError {
    pub fn internal(backend: &str, msg: impl Into<String>) -> Self {
        Self::Internal { backend: backend.to_string(), message: msg.into() }
    }
}
```

#### Documentation Standards
**IMPORTANT**: Every struct, enum, and public function must have comprehensive docstrings explaining:
- **What** it does (behavior)
- **Why** it exists (motivation/purpose)
- **Considerations** or potential issues when using it

```rust
/// Per-feature temporal resolution adapter.
///
/// Resolves features by finding the latest value at or before a given timestamp.
/// This implements the core temporal resolution logic defined in quiver-rfc-v2.md.
///
/// # Security Considerations
/// - Validates all input timestamps to prevent injection attacks
/// - Enforces resource limits to prevent DoS via large queries
pub struct TemporalResolver {
    backend: Arc<dyn BackendAdapter>,
}

/// Per-feature temporal resolution.
///
/// For a given (entity, feature, as_of) triple, return the value from
/// the row with the maximum feature_ts at or before `as_of`.
///
/// # Errors
/// - `AdapterError::NotFound` if no data exists for the entity
/// - `AdapterError::Internal` if backend query fails
pub async fn resolve_one(&self, request: &FeatureRequest) -> Result<RecordBatch, AdapterError>
```

### Configuration Patterns

Use structured configuration with serde:

```rust
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    pub adapters: HashMap<String, AdapterConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AdapterConfig {
    Memory { capacity: usize },
    Redis { url: String },
}
```

### Testing Conventions

#### Test Organization
- **Unit tests** in `#[cfg(test)]` modules within source files
- **Integration tests** in separate `tests/` directory
- **Consolidate related tests**: Don't create many small test files - consolidate related tests into single files
- **Descriptive test names**: Use `test_{scenario_description}` format
- **File naming**: Name by feature, not micro-variant (e.g., `memory_adapter_tests.rs`, not `memory_get_basic.rs`)

#### Test Structure and Quality
```rust
#[tokio::test]
async fn test_basic_get_returns_latest() {
    // Arrange
    let adapter = make_test_adapter().await;
    
    // Act
    let result = adapter.get(&request).await.unwrap();
    
    // Assert
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.column(0).len(), 2);
}
```

#### Test Assertions
- **Use descriptive assertions**: `assert_eq!(actual, expected, "Failed to get latest feature")` 
- **Test exact values**: Prefer `assert_eq!(msg, "expected message")` over `assert!(msg.contains("expected"))`
- **Test both success and error cases**: Always include error path testing

#### Test Utilities
- Create helper functions for common setup (`make_adapter()`, `ts()`)
- Use consistent seed data patterns
- Test both success and error cases

### Python Code Style

- Follow PEP 8 conventions
- Use type hints for public APIs
- Keep Python client simple and focused on gRPC communication
- Use `pyproject.toml` for dependencies

## Architecture Guidelines

### Core Patterns
- **Trait-based architecture**: Use traits for abstraction (`BackendAdapter`, `Registry`)
- **Async-first design**: All I/O operations must be async
- **Error context preservation**: Always include backend/component context in errors
- **Type-safe configuration**: Use structured config with compile-time validation

### Backend Adapter Implementation

When adding new adapters, implement the `BackendAdapter` trait:

```rust
#[async_trait::async_trait]
pub trait BackendAdapter: Send + Sync {
    async fn get(&self, request: &FeatureRequest) -> Result<RecordBatch, AdapterError>;
    async fn health_check(&self) -> Result<(), AdapterError>;
    fn backend_name(&self) -> &'static str;
}
```

### Arrow Integration
- Use `RecordBatch` for all data transfers
- Implement proper schema validation
- Handle Arrow array downcasting with error handling

## Security Considerations

- Always validate input data and configurations
- Use secure defaults for connection settings
- Document authentication requirements for backend adapters
- Never log sensitive information (tokens, passwords)

### Input Validation
- Validate all `FeatureRequest` parameters (entity_id, feature_names, timestamps)
- Sanitize configuration values before parsing
- Implement resource limits to prevent DoS attacks

### Backend Security
- Use secure connection strings for Redis/external backends
- Implement proper authentication flows
- Handle connection timeouts and retries securely
- Never expose internal error details to external clients

## CI/CD Requirements

All code must pass:
- `cargo fmt --all -- --check` (formatting)
- `cargo clippy --all-features -- -D warnings` (linting, treats warnings as errors)
- `cargo test --all-features` (tests on Ubuntu/macOS, stable/nightly Rust)
- Code coverage reporting via `cargo-tarpaulin`

## Common File Locations

- **Core Rust code**: `quiver-core/src/`
- **Python client**: `quiver-python/client/`
- **Protocol definitions**: `proto/v1/`
- **Configuration examples**: `conf.yaml`, `test-config.yaml`
- **Integration tests**: `quiver-core/tests/`

## Performance Guidelines

- Use Arrow Flight protocol for data transfer
- Implement proper connection pooling for backend adapters
- Use `DashMap` for concurrent access patterns
- Profile memory usage with large datasets
- Prefer zero-copy operations where possible

## Debugging Tips

- Set `RUST_BACKTRACE=1` for detailed error traces
- Use `tracing` for structured logging
- Test with `--nocapture` to see print statements
- Use `cargo-tarpaulin` for coverage analysis

When in doubt, follow the patterns established in existing code and prioritize type safety, comprehensive error handling, and thorough testing.