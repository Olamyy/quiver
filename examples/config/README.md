# Quiver Configuration Examples

Each configuration file demonstrates a different feature store setup with a specific adapter and data model.

## Environment Variable Substitution

All config files support `${ENV_VAR}` style substitution for sensitive values like passwords, API keys, and credentials.

**Example:**
```yaml
adapters:
  postgres_adapter:
    connection_string: "postgresql://user:${POSTGRES_PASSWORD}@localhost/db"
```

**Usage:**
```bash
export POSTGRES_PASSWORD=secret123
make run CONFIG=examples/config/postgres/basic.yaml
```

If an environment variable is not set, the placeholder is left as-is with a warning logged. This allows configs to be committed safely without exposing secrets.

## Understanding `registry.type`

The `registry.type: static` field defines how Quiver discovers available features. It means the feature views (entity types, feature names, data types, adapter mappings) are defined statically in the config file itself.

Currently, only `static` is supported. Future versions may support dynamic registry fetching from external services (e.g., REST APIs, data catalogs).

## Configuration Levels

Configs come in two flavors:

- **Basic** - Minimal setup to get started quickly
- **Advanced** - Production-ready with validation, access logging, TLS, and tuning

## PostgreSQL

### `postgres/basic.yaml`
E-commerce user features served from PostgreSQL.

**Use for:** Development and testing with a simple schema.

**Features:**
- Single table: `ecommerce_data` (populated by `--scenario ecommerce`)
- Entity IDs: `user:101`, `user:102`, `user:103` (from seed data)
- Basic validation (entity IDs and feature names required)
- No TLS
- No access logging

**Setup:**
```bash
# Ingest ecommerce data (seed + 100 generated records)
uv run ingest.py --engine postgres --scenario ecommerce --seed --generate 100

# Start server
make run CONFIG=examples/config/postgres/basic.yaml

# Query from another terminal
uv run read.py
```

### `postgres/temporal.yaml`
Identical to basic.yaml (same schema and table).

**Use when:** You want to test temporal/"as of" queries. The server configuration is the same; only the client would specify `as_of` timestamps.

**Setup:** Same as basic.yaml

### `postgres/advanced.yaml`
Production-ready configuration with security and observability.

**Features:**
- Comprehensive validation (batch size, SQL injection safety, timeout bounds)
- Access logging in JSON format
- TLS for secure connections (requires cert files)
- Higher concurrency (100 concurrent RPCs)
- Compression (gzip)
- Multiple validations enabled (request + response)

**Setup:**
```bash
# Ingest ecommerce data (for load testing, use 5000+ records)
uv run ingest.py --engine postgres --scenario ecommerce --seed --generate 5000

# Note: TLS requires cert files at /etc/quiver/tls/server.crt and .key
# For development, comment out the tls section or generate self-signed certs
make run CONFIG=examples/config/postgres/advanced.yaml

# Query from another terminal
uv run read.py
```

**Configuration highlights:**
```yaml
server:
  tls:
    cert_path: "/etc/quiver/tls/server.crt"      # Server certificate
    key_path: "/etc/quiver/tls/server.key"       # Server private key
  access_log:
    enabled: true                                 # Request/response logging
    include_request_body: true                   # Log request details
  validation:                                    # Enable multiple validations
    - batch_size_limit
    - sql_identifier_safety
    - timeout_bounds

adapters:
  postgres_adapter:
    connection_string: "postgresql://...?sslmode=require"  # Require SSL
    tls:
      verify_certificates: true                           # Verify server certs
      ca_cert_path: "/etc/ssl/certs/ca-bundle.crt"        # CA certificate
```

## Redis

### `redis/basic.yaml`
Real-time session state served from Redis.

**Use for:** Development and testing with session data.

**Features:**
- Key template: `sessions:{session_id}` (populated by `--scenario sessions`)
- Entity IDs: `session:s001`, `session:s002`, `session:s003` (from seed data)
- Current state only (Redis hash storage)
- Basic validation
- No TLS or access logging

**Setup:**
```bash
# Ingest session data (seed + 100 generated records)
uv run ingest.py --engine redis --scenario sessions --seed --generate 100

# Start server
make run CONFIG=examples/config/redis/basic.yaml

# Query from another terminal
uv run read.py
```

**Note:** Redis adapter uses hash-based storage. Each entity ID becomes a Redis hash key with feature names as fields.

### `redis/advanced.yaml`
Production-ready configuration with validation, logging, and tuning.

**Features:**
- Comprehensive validation (batch size, Redis key safety)
- Access logging in JSON format
- TLS support
- Higher concurrency (200 concurrent RPCs)
- Configurable chunk size (5000 entities per batch)
- Entity IDs: `session:s001`, `session:s002`, etc.

**Setup:**
```bash
# Ingest session data (for load testing, use 10000+ records)
uv run ingest.py --engine redis --scenario sessions --seed --generate 10000

# Start server
make run CONFIG=examples/config/redis/advanced.yaml

# Query from another terminal
uv run read.py
```

**Configuration highlights:**
```yaml
server:
  max_concurrent_rpcs: 200              # Handle more concurrent requests
  compression: gzip                     # Compress responses
  access_log:
    enabled: true                       # Request/response logging
  validation:
    - redis_key_safety                  # Validate Redis key format
    - batch_size_limit                  # Enforce batch limits

adapters:
  redis_adapter:
    connection: "redis://:${REDIS_PASSWORD}@localhost:6379/0"
    parameters:
      mget_chunk_size: 5000             # Tune batch retrieval size
```

## ClickHouse

### `clickhouse/basic.yaml`
E-commerce user features served from ClickHouse.

**Use for:** Development and testing with analytical queries.

**Features:**
- Single table: `ecommerce_data`
- All features in one table with entity_id as join key
- Basic validation
- No TLS or access logging

**Setup:**
```bash
uv run ingest.py --engine clickhouse --seed --generate 100
make run CONFIG=examples/config/clickhouse/basic.yaml
```

### `clickhouse/temporal.yaml`
Time-series sensor measurements from ClickHouse with temporal queries.

**Use for:** Historical data with point-in-time lookups.

**Features:**
- Single table: `timeseries_data`
- Historical measurements with timestamps
- Support for "as of" queries to fetch data at specific points in time
- Sensor data: temperature, humidity, alert_triggered

**Setup:**
```bash
uv run ingest.py --engine clickhouse --seed --generate 500 --scenario timeseries
make run CONFIG=examples/config/clickhouse/temporal.yaml
```

### `clickhouse/single-table.yaml`
All features in a single denormalized ClickHouse table.

**Use for:** Simple analytics where all features live in one table.

**Features:**
- One table called `features` containing all feature columns
- Same e-commerce schema as basic.yaml
- Demonstrates single-table analytics pattern

**Setup:**
```bash
uv run ingest.py --engine clickhouse --seed --generate 100
make run CONFIG=examples/config/clickhouse/single-table.yaml
```

### `clickhouse/advanced.yaml`
Production-ready configuration with comprehensive security and observability.

**Features:**
- Multiple feature views (ecommerce + timeseries)
- Comprehensive validation (SQL injection safety, memory constraints)
- Access logging in JSON format
- TLS support
- Higher concurrency (100 concurrent RPCs)
- Larger message size (32MB) for batch queries
- Compression (zstd)
- Configurable chunk size (50K entities per batch)

**Setup:**
```bash
uv run ingest.py --engine clickhouse --seed --generate 10000
make run CONFIG=examples/config/clickhouse/advanced.yaml
```

**Configuration highlights:**
```yaml
server:
  max_concurrent_rpcs: 100              # Higher concurrency
  max_message_size_mb: 32               # Support larger batches
  compression: zstd                     # Better compression
  validation:
    - sql_identifier_safety             # Prevent SQL injection
    - memory_constraints                # Prevent OOM
    - timestamp_format                  # Validate timestamps

adapters:
  clickhouse_adapter:
    connection_string: "clickhouse://...?enable_http_compression=1"
    parameters:
      chunk_size: 50000                 # Batch retrieval size
```

## S3/Parquet

### `s3/basic.yaml`
E-commerce features stored as Parquet files in AWS S3.

**Use for:** Reading features from production data lake with environment variable credentials.

**Features:**
- Storage: AWS S3
- Uses AWS credentials from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`)
- One Parquet file per feature

**Setup:**
```bash
# Pass AWS credentials to docker-compose
AWS_ACCESS_KEY_ID=your_key \
AWS_SECRET_ACCESS_KEY=your_secret \
AWS_REGION=us-west-2 \
docker-compose up -d

# Credentials are now available in the container environment
make run CONFIG=examples/config/s3/basic.yaml
```

### `s3/with-credentials.yaml`
S3 configuration with explicit credentials in config file.

**Use for:** Multi-region deployments or when you need different credentials per service.

**Features:**
- Explicit AWS region and credentials in config
- Support for temporary credentials or assume-role scenarios
- Useful when different services need different AWS access levels

**Setup:**
```bash
# Edit the config to add your credentials:
# adapters:
#   s3_adapter:
#     auth:
#       access_key_id: "your_key"
#       secret_access_key: "your_secret"
#       region: "us-west-2"

docker-compose up -d

make run CONFIG=examples/config/s3/with-credentials.yaml
```

**Note:** For security in production, prefer passing credentials via environment variables (`s3/basic.yaml`) rather than hardcoding them in the config file.

### `s3/local.yaml`
Local filesystem stored as Parquet files.

**Use for:** Development or testing with local data.

**Features:**
- Storage: Local filesystem (file:// URI scheme)
- Same Parquet-based interface as S3

**Setup:**
```bash
# Create local parquet files in /data/features
mkdir -p /data/features
python ingest-s3.py --engine localstack --endpoint-url file:///data/features

make run CONFIG=examples/config/s3/local.yaml
```

## Multi-Adapter

Multi-adapter configs combine multiple data sources in a single Quiver server, allowing different features to come from different backends.

### `multi-adapter/postgres-s3.yaml`
PostgreSQL for transactional features + S3 for data lake features.

**Use for:** Combining transactional database with data lake.

**Features:**
- PostgreSQL: spend_30d, purchase_count, last_purchase_date
- S3: is_premium
- Single Quiver server reads from both sources

**Setup:**
```bash
uv run ingest.py --engine postgres --seed
AWS_BUCKET=my-bucket AWS_REGION=us-west-2 \
  AWS_ACCESS_KEY_ID=key AWS_SECRET_ACCESS_KEY=secret \
  make run CONFIG=examples/config/multi-adapter/postgres-s3.yaml
```

### `multi-adapter/redis-s3.yaml`
S3 for analytical features + Redis for real-time features.

**Use for:** Data lake analytics + Redis real-time cache.

**Features:**
- S3: spend_30d, purchase_count, last_purchase_date, is_premium
- Redis: active_user_id, request_count, last_activity for sessions
- Serves both analytical and real-time data from unified API

**Setup:**
```bash
uv run ingest.py --engine redis --seed
AWS_BUCKET=my-bucket AWS_REGION=us-west-2 \
  AWS_ACCESS_KEY_ID=key AWS_SECRET_ACCESS_KEY=secret \
  make run CONFIG=examples/config/multi-adapter/redis-s3.yaml
```

### `multi-adapter/postgres-redis-s3.yaml`
PostgreSQL + Redis + S3 together.

**Use for:** Testing transactional DB + cache + data lake in one config.

**Features:**
- PostgreSQL: spend_30d, purchase_count (transactional)
- S3: last_purchase_date (data lake)
- Redis: is_premium and all session features (real-time)
- Two feature views sourcing from different backends

**Setup:**
```bash
uv run ingest.py --engine postgres --seed
uv run ingest.py --engine redis --seed
AWS_BUCKET=my-bucket AWS_REGION=us-west-2 \
  AWS_ACCESS_KEY_ID=key AWS_SECRET_ACCESS_KEY=secret \
  POSTGRES_PASSWORD=quiver_test \
  make run CONFIG=examples/config/multi-adapter/postgres-redis-s3.yaml
```

## Memory

### `memory/dev.yaml`
In-memory feature store for rapid development.

**Use for:** Testing and development without external dependencies.

**Features:**
- Zero external dependencies (no database, cache, or cloud)
- Minimal validation for fast iteration
- Localhost only binding

**Setup:**
```bash
make run CONFIG=examples/config/memory/dev.yaml
```

## Quick Reference

| Config | Adapter | Entity Type | Level | Use Case |
|--------|---------|-------------|-------|----------|
| postgres/basic | PostgreSQL | user | Development | Simple schema |
| postgres/advanced | PostgreSQL | user | Production | Security + validation |
| redis/basic | Redis | session | Development | Real-time cache |
| redis/advanced | Redis | session | Production | High throughput |
| clickhouse/basic | ClickHouse | user | Development | OLAP queries |
| clickhouse/temporal | ClickHouse | sensor | Development | Time-series |
| clickhouse/advanced | ClickHouse | user/sensor | Production | Security + observability |
| s3/basic | S3/Parquet | user | Production | AWS S3 (env vars) |
| s3/with-credentials | S3/Parquet | user | Production | AWS S3 (explicit creds) |
| s3/local | S3/Parquet | user | Development | Local filesystem |
| multi-adapter/hybrid | PostgreSQL + Redis | user/session | Development | Cold path + hot path |
| multi-adapter/clickhouse-redis | ClickHouse + Redis | user/session | Development | Analytics + realtime |
| multi-adapter/all-adapters | PostgreSQL + Redis + ClickHouse | user/session/sensor | Development | All three adapters |
| memory/dev | Memory | user | Development | No external deps |

## Common Tasks

### Quick start (development)
```bash
# PostgreSQL
uv run ingest.py --engine postgres --seed
make run CONFIG=examples/config/postgres/basic.yaml

# Redis
uv run ingest.py --engine redis --seed
make run CONFIG=examples/config/redis/basic.yaml

# ClickHouse
uv run ingest.py --engine clickhouse --seed
make run CONFIG=examples/config/clickhouse/basic.yaml

# Memory (no ingestion needed)
make run CONFIG=examples/config/memory/dev.yaml
```

### Production deployments
```bash
# PostgreSQL with TLS and validation
uv run ingest.py --engine postgres --seed --generate 5000
make run CONFIG=examples/config/postgres/advanced.yaml

# Redis with high concurrency
uv run ingest.py --engine redis --seed --generate 10000
make run CONFIG=examples/config/redis/advanced.yaml

# ClickHouse with multiple views
uv run ingest.py --engine clickhouse --seed --generate 10000
uv run ingest.py --engine clickhouse --generate 5000 --scenario timeseries
make run CONFIG=examples/config/clickhouse/advanced.yaml
```

### Load testing
```bash
# Generate large datasets
uv run ingest.py --engine postgres --generate 100000
uv run ingest.py --engine clickhouse --generate 100000 --scenario ecommerce
uv run ingest.py --engine redis --generate 50000

# Serve and test
make run CONFIG=examples/config/{adapter}/basic.yaml
# (in another terminal)
uv run read.py --adapter {adapter}
```

### Time-series queries
```bash
# Load sensor data
uv run ingest.py --engine clickhouse --scenario timeseries --generate 10000

# Query with temporal support
make run CONFIG=examples/config/clickhouse/temporal.yaml
```

## Configuration Concepts

### Validation Levels

Quiver validates requests and responses to ensure data quality and security:

**Request validations:**
- `entity_ids_not_empty` - Reject requests with empty entity lists
- `feature_names_not_empty` - Reject requests with no features
- `batch_size_limit` - Enforce maximum batch size
- `sql_identifier_safety` - Prevent SQL injection attacks
- `redis_key_safety` - Validate Redis key format
- `timeout_bounds` - Ensure timeout values are reasonable
- `memory_constraints` - Prevent memory exhaustion from large batches

**Response validations:**
- `schema_consistency` - Verify response schema matches request
- `data_type_validation` - Validate each column's data type
- `null_value_policies` - Enforce nullable column rules
- `timestamp_format` - Validate timestamp formatting

Use `skip_all_validation: true` only in development environments where you trust all inputs.

### Access Logging

Enable JSON-formatted access logs for audit trails and monitoring:

```yaml
server:
  access_log:
    enabled: true
    format: json                    # json or custom format
    include_request_body: true      # Log request contents
    include_response_metadata: true # Log response info
```

### TLS Configuration

For production deployments, enable TLS on both the server and adapters:

```yaml
server:
  tls:
    cert_path: "/path/to/cert.pem"
    key_path: "/path/to/key.pem"

adapters:
  my_adapter:
    tls:
      verify_certificates: true
      ca_cert_path: "/path/to/ca.pem"
```

### Compression

Choose compression for better bandwidth efficiency:

```yaml
server:
  compression: gzip  # or zstd or none
```

- `gzip` - Good compression ratio, slower
- `zstd` - Faster compression, slightly larger
- `none` - No compression, fastest
