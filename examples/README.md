# Quiver Configuration Examples

This directory contains example configurations for different Quiver deployment scenarios. Each configuration demonstrates best practices for specific use cases.

## Available Examples

### 🚀 [`memory-only.yaml`](config/memory-only.yaml)
**Use Case**: Development, testing, and high-performance in-memory caching

- **Adapters**: Memory only
- **Best For**: 
  - Unit testing and development
  - High-frequency feature serving with sub-millisecond latency
  - Scenarios where features can fit in memory
- **Limitations**: No persistence, limited by available RAM

### 🏭 [`postgres-production.yaml`](config/postgres-production.yaml)  
**Use Case**: Production deployments with persistent feature storage

- **Adapters**: Multiple PostgreSQL databases
- **Best For**:
  - Production feature stores with historical data
  - Temporal queries and time-travel capabilities  
  - Multi-tenant feature serving
- **Features**: TLS encryption, connection pooling, multiple data sources

### ⚡ [`redis-realtime.yaml`](config/redis-realtime.yaml)
**Use Case**: Real-time ML inference with ultra-low latency

- **Adapters**: Multiple Redis instances
- **Best For**:
  - Real-time recommendation systems
  - Session-based features
  - High-throughput serving (5000+ concurrent requests)
- **Features**: TLS connections, optimized for current-only features

### 🔄 [`hybrid-multi-adapter.yaml`](config/hybrid-multi-adapter.yaml)
**Use Case**: Production ML systems combining real-time and historical features

- **Adapters**: Memory + Redis + PostgreSQL
- **Best For**:
  - Complex ML pipelines requiring diverse feature types
  - Combining real-time signals with historical context
  - Multi-modal feature serving
- **Features**: Multiple feature views, optimized data routing

### 🛠️ [`development.yaml`](config/development.yaml)
**Use Case**: Local development and testing

- **Adapters**: Local PostgreSQL + Redis
- **Best For**:
  - Developer workstations
  - CI/CD testing
  - Learning and experimentation
- **Features**: Relaxed security settings, detailed logging

## Configuration Structure

All configurations follow this structure:

```yaml
# Server settings
server:
  host: "0.0.0.0"
  port: 8815
  tls:                    # Optional TLS configuration
    cert_path: "..."
    key_path: "..."
  max_concurrent_rpcs: 1000
  timeout_seconds: 30
  access_log:
    enabled: true

# Feature registry
registry:
  type: static
  views:
    - name: "feature_view_name"
      entity_type: "user"
      entity_key: "user_id" 
      columns:
        - name: "feature_name"
          arrow_type: "float64"
          nullable: true
          source: "adapter_name"

# Backend adapters
adapters:
  adapter_name:
    type: postgres|redis|memory
    # Adapter-specific configuration
```

## Security Configuration

### Environment Variables
Sensitive configuration (passwords, connection strings) should be provided via environment variables:

```bash
# PostgreSQL password
export QUIVER__ADAPTERS__POSTGRES_MAIN__CONNECTION_STRING="postgresql://user:password@host:5432/db?sslmode=require"

# Redis password  
export QUIVER__ADAPTERS__REDIS_CACHE__PASSWORD="your_redis_password"
```

### TLS Configuration
Production deployments should always use TLS:

```yaml
server:
  tls:
    cert_path: "/etc/ssl/certs/quiver.pem"
    key_path: "/etc/ssl/private/quiver.key"

adapters:
  postgres_prod:
    connection_string: "...?sslmode=require"  # Enforce TLS
  
  redis_prod:
    connection: "rediss://..."  # Use TLS-enabled Redis
```

## Adapter-Specific Guidelines

### PostgreSQL Adapter
```yaml
adapters:
  postgres_example:
    type: postgres
    connection_string: "postgresql://user@host:5432/db?sslmode=require"
    table_template: "features_{feature}"  # Table naming pattern
    max_connections: 20                   # Connection pool size
    timeout_seconds: 30                   # Query timeout
```

**Requirements**:
- Database tables must exist with expected schema
- Connection string must include `sslmode=require` for production
- User must have SELECT permissions on feature tables

### Redis Adapter
```yaml
adapters:
  redis_example:
    type: redis
    connection: "rediss://host:6380"      # Use rediss:// for TLS
    key_template: "features:{entity}:{feature}"  # Key naming pattern
```

**Requirements**:
- Redis keys must follow the specified template
- Use `rediss://` protocol for TLS connections
- Provide password via environment variable

### Memory Adapter
```yaml
adapters:
  memory_example:
    type: memory
```

**Requirements**:
- Features must be loaded programmatically
- No configuration parameters needed
- Limited by available system memory

## Performance Tuning

### High-Throughput Configuration
```yaml
server:
  max_concurrent_rpcs: 5000    # Increase for high load
  max_message_size_mb: 128     # Larger batches
  timeout_seconds: 10          # Fast fail for real-time
  compression: gzip            # Network efficiency

adapters:
  postgres_perf:
    max_connections: 50        # Scale connection pool
    timeout_seconds: 60        # Longer timeout for complex queries
```

### Memory Optimization
```yaml
# Configure batch sizes based on your memory constraints
# Default memory limits:
# - PostgreSQL adapter: 256MB per batch
# - Redis adapter: 128MB per batch
# - Memory adapter: Limited by system RAM
```

## Getting Started

1. **Choose a configuration** that matches your use case
2. **Copy the example** to your deployment directory
3. **Update connection strings** and credentials
4. **Set environment variables** for sensitive data
5. **Test the configuration** with `make run`

## Example Commands

```bash
# Run with a specific configuration
QUIVER_CONFIG=examples/config/development.yaml make run

# Run with environment variables
export QUIVER__ADAPTERS__POSTGRES__CONNECTION_STRING="postgresql://..."
make run

# Test configuration validity
cargo run -- --config examples/config/postgres-production.yaml --validate
```

## Troubleshooting

### Common Issues

1. **Connection failures**: Check network connectivity and credentials
2. **TLS errors**: Ensure certificates are valid and accessible
3. **Permission denied**: Verify database user permissions
4. **Memory issues**: Reduce batch sizes or increase system memory
5. **Timeout errors**: Increase timeout values for slow queries

### Debug Logging
Enable detailed logging for troubleshooting:

```yaml
server:
  access_log:
    enabled: true
    format: "json"
    include_request_body: true
    include_response_metadata: true
```

## Contributing

When adding new configuration examples:

1. Follow the naming convention: `{use-case}.yaml`
2. Include comprehensive comments explaining each setting
3. Provide security recommendations
4. Update this README with the new example
5. Test the configuration in a realistic environment

## Support

For questions about configuration:
- Review the [main documentation](../README.md)
- Check [GitHub issues](https://github.com/anomalyco/opencode) for similar problems
- Open a new issue with your configuration and error details