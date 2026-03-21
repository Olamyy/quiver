# Configuration

Quiver is configured entirely via a single YAML file. There is no database-backed config, no admin API, and no runtime mutation. Every aspect of the server's behaviour (which feature views exist, which backends serve which columns, caching policy, fanout strategy, observability) is declared in this file.

Pass the config path at startup:

```bash
quiver-server --config path/to/quiver.yaml
# or
QUIVER_CONFIG=path/to/quiver.yaml quiver-server
```

## Credential resolution

Quiver resolves configuration values in this order, with later sources winning:

1. **Config file** — values declared in the YAML file
2. **`QUIVER__*` environment variables** — override any config file value using the prefix `QUIVER__` with `__` as the path separator

```bash
QUIVER__SERVER__PORT=9000
QUIVER__ADAPTERS__POSTGRES__CONNECTION_STRING=postgresql://...
```

There is also a second mechanism for injecting secrets into config values without using the `QUIVER__*` override path: `${ENV_VAR}` placeholders. Any string field in the config file can reference an environment variable by name, and it will be substituted before the file is parsed:

```yaml
adapters:
  postgres:
    type: postgres
    connection_string: postgresql://quiver:${DB_PASSWORD}@localhost:5432/features
  redis:
    type: redis
    connection: redis://localhost:6379
    password: ${REDIS_PASSWORD}
```

If the referenced variable is not set, Quiver keeps the placeholder string and logs a warning. It does not fail at startup, so an unset variable will produce a connection error at request time rather than immediately.

The `${ENV_VAR}` approach is useful when the credential must live inside a DSN or URL (e.g. PostgreSQL connection strings). The `QUIVER__*` override approach is better when you want to replace a discrete field entirely.

---

## Structure

A complete config file has four top-level keys:

```yaml
server:    # server bind address, timeouts, fanout, cache, observability
registry:  # feature view definitions
adapters:  # backend connection configs
```

All sections are optional except `registry.views`. A minimal working config is:

```yaml
registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          source: memory

adapters:
  memory:
    type: memory
```

---

## `server`

Controls the Arrow Flight server on port `8815`.

```yaml
server:
  host: "0.0.0.0"
  port: 8815
  timeout_seconds: 30
  max_concurrent_rpcs: 100
  max_message_size_mb: 64
  compression: none
```

| Field | Default | Description |
|-------|---------|-------------|
| `host` | `0.0.0.0` | Bind address. Use `127.0.0.1` to restrict to loopback only (recommended in sidecar deployments). |
| `port` | `8815` | Arrow Flight gRPC port. |
| `timeout_seconds` | none | Global request timeout applied to every `do_get` call. Individual adapter timeouts take precedence within this budget. |
| `max_concurrent_rpcs` | none | Maximum concurrent gRPC streams. Requests beyond this limit are rejected with `RESOURCE_EXHAUSTED`. |
| `max_message_size_mb` | none | Maximum incoming message size. Increase for requests with large entity lists. |
| `compression` | `none` | Wire compression. `none`, `gzip`, or `zstd`. Arrow-native backends already send compressed columnar data; compression here adds overhead for those backends with little benefit. |

### `server.fanout`

Controls multi-backend parallel dispatch. Only relevant when a feature view routes columns to more than one adapter.

```yaml
server:
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill
    downtime_strategy: fail
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable fanout. Must be `true` for multi-backend feature views to work. |
| `max_concurrent_backends` | `10` | Maximum number of adapter calls dispatched in parallel per request. |
| `partial_failure_strategy` | `null_fill` | What to do when a backend returns results for only some requested entities. `null_fill` fills missing rows with nulls. `error` fails the request. |
| `downtime_strategy` | `fail` | What to do when a backend is entirely unavailable or times out. `fail` fails the entire request. `return_available` returns results from healthy backends only, with null columns for the failed backend. `use_fallback` routes to the `fallback_source` defined on each affected column. |

### `server.cache`

Controls in-process request-level caching. Cache keys are `(feature_view, entity_ids, feature_names)`.

```yaml
server:
  cache:
    enabled: true
    mode: ttl
    ttl_seconds: 60
    max_entries: 1000
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable caching. |
| `mode` | `ttl` | `ttl` expires entries after a fixed TTL; the next request after expiry blocks on the backend. `swr` (stale-while-revalidate) returns the stale entry immediately and refreshes in the background. |
| `ttl_seconds` | `60` | How long a cached result is considered fresh. |
| `max_entries` | `1000` | Maximum number of cached responses. When the limit is reached, the least recently used entry is evicted. |

Caching is most effective when traffic has high entity repetition. For purely batch inference with unique entity sets per request, it provides no benefit.

### `server.observability`

Controls the observability service on port `8816`. When enabled, every `do_get` response includes an `x-quiver-request-id` header that can be passed to the observability endpoint to retrieve per-stage latency breakdowns.

```yaml
server:
  observability:
    enabled: true
    ttl_seconds: 3600
    max_entries: 10000
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Start the observability server on port `8816`. |
| `ttl_seconds` | `3600` | How long to retain metrics for a given request ID. |
| `max_entries` | `10000` | Maximum number of request records held in memory. |

### `server.access_log`

```yaml
server:
  access_log:
    enabled: true
```

When enabled, Quiver logs one line per request to stdout: request ID, feature view, entity count, cache hit/miss, and total latency. Disabled by default.

---

## `registry`

Defines the feature views Quiver serves. Only `type: static` is supported. Views are loaded once at startup and are read-only for the lifetime of the server.

```yaml
registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: postgres
        - name: last_active
          arrow_type: "timestamp[ns, UTC]"
          nullable: true
          source: redis
```

### View fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique identifier. Clients pass this as `feature_view` in requests. |
| `entity_type` | Yes | Logical entity type (e.g. `user`, `item`). Used for documentation and validation only; not used in queries. |
| `entity_key` | Yes | The column name in the backend that holds the entity ID. Quiver uses this to construct `WHERE entity_key IN (...)` queries. |
| `columns` | Yes | List of feature column definitions. |

### Column fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Feature name exposed to clients. |
| `arrow_type` | Yes | Arrow type string. See [Arrow types](#arrow-types) below. |
| `nullable` | No | Whether null values are permitted. Default `true`. |
| `source` | Yes | Name of the adapter that serves this column. Must match a key under `adapters`. |
| `source_path` | No | Override the adapter-level `source_path` for this column only. |
| `fallback_source` | No | Adapter to fall back to if the primary `source` fails. Requires `downtime_strategy: use_fallback`. |

### Arrow types

Arrow types are declared as strings in config. The resolver parses these at startup and rejects unrecognised values. They are never silently coerced.

| Config string | Arrow type | Notes |
|---------------|-----------|-------|
| `float32`, `float64` | `Float32`, `Float64` | |
| `int32`, `int64` | `Int32`, `Int64` | |
| `string`, `utf8` | `Utf8` | |
| `bool` | `Boolean` | |
| `timestamp[ns, UTC]` | `Timestamp(Nanosecond, UTC)` | UTC is always enforced. Non-UTC timestamp strings are rejected. |
| `list<float32>` | `List(Float32)` | Suitable for embedding vectors. |
| `binary` | `Binary` | Raw bytes. |

---

## `adapters`

Each adapter is declared under a name that must match the `source` field on one or more columns. The `type` field selects the adapter implementation.

```yaml
adapters:
  my_redis:
    type: redis
    ...
  my_postgres:
    type: postgres
    ...
```

Column `source: my_redis` routes to the first adapter; `source: my_postgres` routes to the second. Names are arbitrary. They are internal identifiers, not connection strings.

### Memory

```yaml
adapters:
  memory:
    type: memory
```

In-process store with no external dependencies. Data is populated programmatically. Intended for local development and testing only.

### Redis

```yaml
adapters:
  redis:
    type: redis
    connection: redis://localhost:6379
    source_path: "features:{feature}"
    password: ${REDIS_PASSWORD}
    timeout_seconds: 2
    tls:
      enabled: false
```

| Field | Required | Description |
|-------|----------|-------------|
| `connection` | Yes | Redis URL. |
| `source_path` | Yes | Key template. `{feature}` is replaced with the feature column name; `{entity}` is replaced with the entity ID. |
| `password` | No | Redis AUTH password. |
| `timeout_seconds` | No | Per-request timeout. Default 2 seconds. |
| `tls.enabled` | No | Enable TLS for the Redis connection. |

#### Auth

Redis credentials are resolved in this order:

1. `QUIVER__ADAPTERS__<NAME>__PASSWORD` environment variable
2. `password` field in config (supports `${ENV_VAR}` substitution)

Do not put passwords in plain text in the config file. Use `password: ${REDIS_PASSWORD}` and set the environment variable in your deployment.

Redis does not support `as_of` queries. Columns sourced from Redis always return current values regardless of the `as_of` parameter.

### PostgreSQL

```yaml
adapters:
  postgres:
    type: postgres
    connection_string: postgresql://quiver:${DB_PASSWORD}@localhost:5432/features
    source_path:
      table: user_features
    max_connections: 10
    timeout_seconds: 5
```

| Field | Required | Description |
|-------|----------|-------------|
| `connection_string` | Yes | PostgreSQL connection URL. |
| `source_path.table` | Yes | Table to query. Quiver generates `SELECT <columns> FROM <table> WHERE <entity_key> IN (...)`. |
| `max_connections` | No | Connection pool size. Default 10. |
| `timeout_seconds` | No | Per-request timeout. Default 5 seconds. |

#### Auth

PostgreSQL credentials are embedded in the connection string. Credentials are resolved in this order:

1. `QUIVER__ADAPTERS__<NAME>__CONNECTION_STRING` environment variable (replaces the entire DSN)
2. `${ENV_VAR}` substitution within the `connection_string` field

The `${ENV_VAR}` approach is preferred when only the password needs to be injected:

```yaml
connection_string: postgresql://quiver:${DB_PASSWORD}@localhost:5432/features
```

When `as_of` is provided, the adapter fetches the latest row per entity where `valid_from <= as_of`. The table must have a `valid_from` timestamp column for point-in-time queries to work.

### S3 / Parquet

```yaml
adapters:
  s3:
    type: s3_parquet
    bucket: my-feature-bucket
    prefix: features/user/
    auth:
      region: eu-central-1
      access_key_id: ${AWS_ACCESS_KEY_ID}
      secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

| Field | Required | Description |
|-------|----------|-------------|
| `bucket` | Yes | S3 bucket name. |
| `prefix` | No | Key prefix to scope reads. |
| `auth.region` | Yes | AWS region. |
| `auth.access_key_id` | No | AWS access key ID. |
| `auth.secret_access_key` | No | AWS secret access key. |

#### Auth

S3 credentials are resolved in this order:

1. `auth.access_key_id` and `auth.secret_access_key` fields in config (support `${ENV_VAR}` substitution)
2. Standard AWS credential chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables, `~/.aws/credentials`, EC2 instance profile, ECS task role

In most production deployments running on AWS, omit the `auth` block entirely and let the instance profile or task role provide credentials. The explicit `auth` block is useful for local development or cross-account access.

Quiver uses predicate pushdown when reading Parquet files to avoid scanning columns not present in the request.

### ClickHouse

```yaml
adapters:
  clickhouse:
    type: clickhouse
    connection_string: http://quiver:${CLICKHOUSE_PASSWORD}@localhost:8123/features
    source_path:
      table: user_features
    max_connections: 5
    timeout_seconds: 10
```

| Field | Required | Description |
|-------|----------|-------------|
| `connection_string` | Yes | ClickHouse HTTP endpoint, including user and password in the URL. |
| `source_path.table` | Yes | Table to query. |
| `max_connections` | No | Connection pool size. Default 5. |
| `timeout_seconds` | No | Per-request timeout. Default 10 seconds. |

#### Auth

ClickHouse credentials are embedded in the connection string, following the same pattern as PostgreSQL. Credentials are resolved in this order:

1. `QUIVER__ADAPTERS__<NAME>__CONNECTION_STRING` environment variable (replaces the entire URL)
2. `${ENV_VAR}` substitution within the `connection_string` field

```yaml
connection_string: http://quiver:${CLICKHOUSE_PASSWORD}@localhost:8123/features
```

---

## Full example

A production-style config with two backends, caching, fanout, and observability enabled:

```yaml
server:
  host: "127.0.0.1"
  port: 8815
  timeout_seconds: 30
  compression: none

  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill
    downtime_strategy: return_available

  cache:
    enabled: true
    mode: ttl
    ttl_seconds: 30
    max_entries: 5000

  observability:
    enabled: true
    ttl_seconds: 3600
    max_entries: 10000

  access_log:
    enabled: true

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: postgres
        - name: country
          arrow_type: utf8
          nullable: true
          source: postgres
        - name: last_active
          arrow_type: "timestamp[ns, UTC]"
          nullable: true
          source: redis

adapters:
  postgres:
    type: postgres
    connection_string: postgresql://quiver:quiver@localhost:5432/features
    source_path:
      table: user_features
    max_connections: 10
    timeout_seconds: 5
  redis:
    type: redis
    connection: redis://localhost:6379
    source_path: "user:{entity}:last_active"
    timeout_seconds: 2
```
