# Architecture

Quiver is a feature serving system, not a feature store. It does not compute, transform, or materialize features. It serves features that already exist in your backends by routing requests to the right adapters, dispatching them in parallel, and returning the results as a single entity-aligned columnar batch.

This page explains each architectural layer in depth: what it does, why it exists, and how it interacts with the rest of the system.

---

## High-level diagram

```
Client (model server)
    │
    │  Arrow Flight  do_get(FeatureRequest)
    ▼
QuiverFlightServer                              port 8815
    │
    ├── StaticRegistry       maps feature names → backend + Arrow type
    ├── Resolver             groups features by adapter, dispatches, merges
    ├── RequestCache         TTL cache (default) or SWR — configurable per deployment
    │
    └── FanoutMerger         parallel async dispatch → Arrow LEFT JOIN
            ├── RedisAdapter
            ├── PostgresAdapter
            ├── S3ParquetAdapter
            ├── ClickHouseAdapter
            └── MemoryAdapter
                    │
                    ▼
            RecordBatch   (columnar, entity-aligned, one row per entity)

Observability server                            port 8816
    └── MetricsStore         per-request latency breakdowns (registry, dispatch, merge, serialize)
```

---

## Layer 1: Transport (Arrow Flight)

`QuiverFlightServer` implements the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) `FlightService` gRPC interface. It handles everything related to network I/O and serialization, and contains no business logic.

### What Arrow Flight is

Arrow Flight is a gRPC-based protocol for high-throughput columnar data transfer. Unlike a standard gRPC service that serializes data to/from protobuf row-by-row, Flight transmits Arrow `RecordBatch` objects directly over the wire.

For backends that already store data in Arrow format (S3/Parquet, ClickHouse), this is zero-copy end-to-end. There is no deserialization to rows and no re-serialization to columns. For JSON-backed stores like Redis and PostgreSQL, Quiver constructs an Arrow `RecordBatch` from the fetched row data before encoding it for Flight. The columnar construction happens once in the adapter. No reshaping occurs at the model server. The zero-copy guarantee applies from Quiver's cache layer outward regardless of backend format. Warm cache hits are always zero-copy.

### RPC surface

Quiver exposes three Arrow Flight RPCs:

| RPC | Description |
|-----|-------------|
| `do_get(Ticket)` | Primary feature retrieval endpoint. Accepts a `FeatureRequest` serialized into the `Ticket` bytes, returns a `FlightData` stream. |
| `get_schema(FlightDescriptor)` | Returns the Arrow schema for a feature view without fetching data. Useful for schema discovery at startup. |
| `list_flights(Criteria)` | Lists available feature views. |

`do_get` is the only endpoint used in the hot path. The others are used for tooling and introspection.

### What the transport layer handles

- Deserializes the `FeatureRequest` protobuf from the `Ticket` bytes in the incoming `do_get` call
- Validates the `as_of` timestamp if present. Invalid timestamps return `Status::invalid_argument` rather than silently falling back to `now()`
- Generates a UUID `request_id`, returned in the `x-quiver-request-id` gRPC response header
- Delegates to `Resolver.resolve()` to produce a `RecordBatch`
- Encodes the `RecordBatch` into a `FlightData` stream using `FlightDataEncoderBuilder`
- Sets the `x-quiver-from-cache` header when the result was served from the request cache
- Records per-request latency metrics to the shared `MetricsStore`
- Logs access events (request_id, feature_view, entity count, latency)

### What it does not handle

The transport layer does not know which adapters exist, what feature views are registered, or how to query any backend. All of that is delegated to the resolver.

---

## Layer 2: Resolution

The `Resolver` is the core of Quiver. It orchestrates the entire serving path: registry lookup, backend routing, type enforcement, parallel dispatch, and result merging.

### The resolve() function

```
resolve(feature_view, entity_ids, feature_names, as_of)
    → (RecordBatch, FanoutLatencies)
```

A single call to `resolve()` does everything needed to go from a logical feature request to a columnar result:

1. **Registry lookup.** Fetch the `FeatureView` definition for the requested `feature_view` name.
2. **Feature filtering.** Validate that every requested feature name exists in the view.
3. **Partition.** Group feature columns by their `source` adapter (e.g., `{redis: ["last_active"], postgres: ["score", "country"]}`).
4. **Type enforcement.** Parse Arrow type strings from config; enforce UTC timezone on timestamp columns.
5. **Resolution map.** Construct a `FeatureResolution` per feature: expected Arrow type + source path (table name, key template, etc.).
6. **Cache check.** Return immediately on a hit (TTL mode) or serve stale and schedule async refresh (SWR mode); proceed to dispatch on a miss.
7. **Parallel dispatch.** Fire all adapter calls concurrently using `tokio::join_all`.
8. **Arrow merge.** LEFT JOIN all sparse `RecordBatch` results on `entity_id`.
9. **Validation.** Verify output row count and schema match the request.
10. **Metrics.** Record per-stage latencies and return them alongside the `RecordBatch`.

### Feature partitioning

When a feature view has columns from multiple backends, the resolver splits the column list by `source`. For example, a view with `score` and `country` from `postgres` and `last_active` from `redis` produces two dispatch groups:

```
postgres → [score, country]   with FeatureResolution{type: float64, source_path: table:user_features}
redis    → [last_active]      with FeatureResolution{type: utf8, source_path: "user:{entity}:last_active"}
```

Each group is dispatched independently and in parallel.

### Arrow type system

Column types are declared in YAML config as strings (`"float64"`, `"utf8"`, `"timestamp[ns, UTC]"`). The resolver parses these into `DataType` values using `parse_arrow_type_string()` and passes them to each adapter as part of the `FeatureResolution`. Adapters use the expected type when constructing the output `RecordBatch` schema, ensuring schema consistency across calls.

Unrecognised type strings produce an error at startup. They are never silently coerced to a fallback type. Timestamp columns are always given a UTC timezone (`Timestamp(Nanosecond, Some("UTC"))`), enforced unconditionally at the resolver level to prevent timezone ambiguity propagating into model inputs.

### Temporal routing

When a request includes an `as_of` timestamp, the resolver applies clock-skew-aware routing. Backends that support point-in-time queries (PostgreSQL, ClickHouse) receive the `as_of` value directly. Redis returns current values only.

The default clock-skew margin is 30 seconds. Requests with `as_of` within 30 seconds of now are treated as current-time requests to avoid unnecessary latency penalties from temporal query paths.

!!! note
    
    The redis adapter __currently__ does not support temporal queries. This means that any feature columns sourced from Redis will return current values even if `as_of` is in the past. 
    For `as_of` queries requiring historical accuracy, use backends that support time travel natively (PostgreSQL, ClickHouse, S3/Parquet).

### The request cache

Before dispatching to adapters, the resolver checks the in-process cache keyed by `(feature_view, entity_ids, feature_names)`. A cache hit returns immediately without any backend calls.

Quiver supports two cache modes, configurable per deployment:

| Mode | Default | Behaviour |
|------|---------|-----------|
| `ttl` | Yes | Entries expire after a fixed TTL. On expiry, the next request blocks until the backend responds and repopulates the cache. |
| `swr` | No | Entries have a TTL but are served stale after expiry while a background refresh runs. The requesting client never blocks waiting for a refresh. |

```yaml
server:
  cache:
    mode: ttl          # "ttl" (default) or "swr"
    ttl_seconds: 60
```

**TTL mode** is the default because its consistency behaviour is straightforward. Once an entry expires, all subsequent requests see fresh data. Use this when stale feature values are unacceptable (e.g., fraud signals, real-time pricing).

**SWR mode** is better suited to traffic patterns with high entity repetition (e.g., fraud detection pipelines where the same user generates many events in a short window). Stale requests return immediately and the refresh happens asynchronously behind them, keeping tail latency low at the cost of serving briefly stale data. The staleness window is bounded by the TTL.

---

## Layer 3: Backend Adapters

Each adapter implements the `BackendAdapter` trait, which defines a uniform interface for all backend types.

### The trait contract

```rust
async fn get_with_resolutions(
    &self,
    entity_ids: &[String],
    resolutions: &HashMap<String, FeatureResolution>,
    entity_key: &str,
    as_of: Option<DateTime<Utc>>,
    timeout: Option<Duration>,
) -> Result<RecordBatch, AdapterError>
```

All adapters must satisfy two invariants:

1. **Row ordering.** The output `RecordBatch` must have exactly one row per entity in `entity_ids`, in the same order. The merge step depends on this invariant.
2. **Null-filling.** Missing entities must produce a null row, not an absent row. Dropping rows would silently break the LEFT JOIN merge.

If an adapter returns rows in an arbitrary order (e.g., PostgreSQL), it must re-sort before returning.

### Ordering hints

Adapters declare their ordering behaviour via a `capabilities()` method:

| Hint | Meaning |
|------|---------|
| `OrderedByRequest` | Rows are guaranteed to match the `entity_ids` input order. No re-sort needed. |
| `Unordered` | Quiver will re-sort the output using a lookup map. |

Redis and Memory adapters are `OrderedByRequest` because they build results by iterating `entity_ids` directly. PostgreSQL and ClickHouse are `Unordered` because query result order depends on the execution plan.

### Adapter reference

| Adapter | Backend | Notes |
|---------|---------|-------|
| `RedisAdapter` | Redis | Real-time key-value features via `HSET`; key templates (`user:{entity}:score`); current values only, no `as_of` support |
| `PostgresAdapter` | PostgreSQL | Historical batch features; point-in-time queries via `valid_from <= as_of` |
| `S3ParquetAdapter` | S3 + Parquet | Data lake features; zero-copy Arrow IPC; predicate pushdown for entity filtering |
| `ClickHouseAdapter` | ClickHouse | Analytical features; columnar native transport; complex aggregations |
| `MemoryAdapter` | In-process | Testing and local development only; no external services required |

### Source paths

Each adapter interprets the `source_path` field differently:

**Redis** — a key template with `{entity}` interpolated per entity:
```yaml
source_path: "user:{entity}:last_active"
# entity "1000" → Redis key "user:1000:last_active"
```

**PostgreSQL and ClickHouse** — a structured table reference:
```yaml
source_path:
  table: user_features
# SELECT <columns> FROM user_features WHERE user_id IN (...)
```

**S3/Parquet** — a bucket and prefix:
```yaml
source_path:
  bucket: my-feature-bucket
  prefix: features/user/
```

### Timeout handling

Every adapter call has a configurable timeout. If no timeout is specified in the request, adapters apply a per-backend default (typically 5 seconds for network-backed stores). Adapters that time out return `AdapterError::Timeout`, which the resolver converts to a fully null `RecordBatch` for that backend's columns rather than failing the entire request.

This behaviour is controlled by the `fanout.downtime_strategy` config field:

| Value | Behaviour |
|-------|-----------|
| `null_on_timeout` (default) | Timed-out backend columns are null-filled; request succeeds |
| `fail_fast` | Any backend timeout fails the entire request with an error |

---

## Layer 4: Configuration and Registry

### Configuration

Quiver is fully driven by a YAML config file. There is no dynamic config API, no admin UI, and no database-backed configuration. The entire serving topology (which feature views exist, which backends serve which columns, connection strings, timeout policies) is declared in a single file.

This is intentional. Configuration-as-code means feature views are version-controlled, diffable, and deployable alongside application code. Changing which backend serves a feature is a one-line YAML change with a diff.

The config is parsed at startup by `config.rs` using `serde`. Environment variables can override any field, which is the standard mechanism for injecting credentials in Kubernetes deployments.

### The StaticRegistry

`StaticRegistry` is the in-process registry that maps feature view names to their definitions. It is populated once at startup from the `registry.views` section of the config and is read-only for the lifetime of the server.

For each feature view, the registry stores:

- The entity type and entity key column name
- The list of column definitions: name, Arrow type, nullable flag, source adapter, source path override
- The backend routing table: which columns map to which adapters

The registry has no persistence layer. On restart, it repopulates from config. This is appropriate for Quiver's use case. Feature view schemas are stable, change infrequently, and are defined by operators rather than runtime events.

### FeatureResolution

The `FeatureResolution` struct is the unit of information passed from the resolver to each adapter for a single feature column:

```rust
pub struct FeatureResolution {
    /// The Arrow DataType this column should be cast to in the output RecordBatch.
    pub expected_type: DataType,
    /// The physical location of this feature in the backend (table name, key template, etc.).
    pub source_path: SourcePath,
}
```

By passing type and source path together, adapters have everything they need to build a correctly-typed `RecordBatch` without knowing anything about the feature view schema. This is the boundary that keeps adapters backend-specific and schema-agnostic.

---

## Data flow: end to end

This is the complete path a feature request takes through the system:

```
1.  Client sends:  do_get(Ticket{feature_view, entity_ids, feature_names, as_of})
2.  Transport:     deserialize FeatureRequest from Ticket bytes
3.  Transport:     validate as_of timestamp (error on invalid, not silent fallback)
4.  Transport:     generate request_id UUID
5.  Resolver:      registry.get_view(feature_view)         → FeatureView
6.  Resolver:      validate feature_names ⊆ view.columns
7.  Resolver:      check request cache
                     → hit (TTL mode): return immediately (steps 8–13 skipped)
                     → hit (SWR mode, stale): return stale, schedule async refresh
                     → miss: continue
8.  Resolver:      partition columns by source adapter
9.  Resolver:      build FeatureResolution map (type + source_path per column)
10. Resolver:      tokio::join_all([
                     redis_adapter.get_with_resolutions(...),
                     postgres_adapter.get_with_resolutions(...),
                   ])
                   → [RecordBatch, RecordBatch]  (sparse, one per backend)
11. Resolver:      Arrow LEFT JOIN on entity_id             → single RecordBatch
12. Resolver:      validate: row_count == entity_ids.len(), schema correct
13. Resolver:      record FanoutLatencies (per-stage timings)
14. Transport:     encode RecordBatch → FlightData stream
15. Transport:     set x-quiver-request-id, x-quiver-from-cache headers
16. Transport:     store FanoutLatencies in MetricsStore keyed by request_id
17. Client:        receives RecordBatch as pyarrow.Table
```

---

## Fanout and the Arrow merge

When a request spans multiple backends, the resolver dispatches all adapters concurrently. Each adapter returns a sparse `RecordBatch` — only the rows for entities it found data for. The merge step assembles these sparse batches into a single output batch that:

- Contains exactly one row per requested entity, in the same order as the input list
- Null-fills any entity not found in a given backend
- Uses Arrow's native join primitives to avoid row-level data movement

See [Fanout](fanout.md) for the complete 4-phase merge algorithm, partial failure handling, and `downtime_strategy` configuration.

---

## Observability

A second gRPC server runs on port `8816`. Every feature request generates a UUID that is returned in the `x-quiver-request-id` response header. Pass that UUID to the observability server to retrieve a per-stage latency breakdown:

| Stage | What it measures |
|-------|-----------------|
| `registry_ms` | Time to look up the feature view in the registry |
| `partition_ms` | Time to group features by backend |
| `dispatch_ms` | Wall-clock time for the parallel backend dispatch (equals the slowest adapter) |
| `adapter_ms[name]` | Per-adapter latency (recorded concurrently) |
| `merge_ms` | Time for the Arrow LEFT JOIN merge |
| `validate_ms` | Time for schema and row-count validation |
| `serialize_ms` | Time to encode the RecordBatch into FlightData |
| `total_ms` | End-to-end request latency from resolver entry to FlightData stream |

Metrics are stored in an in-process LRU cache (`MetricsStore`) with a 1-hour TTL. There is no external metrics store. For long-term retention, instrument the observability RPC and export to your existing monitoring system (Prometheus, Datadog, etc.).

!!! note

    In the sidecar deployment model, each pod has its own `MetricsStore`. A `request_id` is only resolvable against the observability endpoint of the pod that served it. Passing a `request_id` to a different pod returns nothing.

See the [Python Client](../reference/python-client.md) reference for how to retrieve and parse these metrics.

---

## Deployment model

Quiver is designed as a **sidecar**: one instance per model server pod, not a shared central service. This means:

- **No added network hop.** The model server calls Quiver over the pod's loopback interface (`localhost:8815`). Quiver then calls the backends, which are the same systems the model server would have called directly.
- **No single point of failure.** If a Quiver instance crashes, only its pod is affected. There is no shared serving infrastructure to take down.
- **Per-model configuration.** Different model servers can run different Quiver configurations: different feature views, different backends, different cache TTLs, different fanout strategies.

See [Kubernetes Sidecar](../guides/kubernetes-sidecar.md) for the deployment guide, including container spec examples and liveness probes.

---

## Latency characteristics

| Request type | Source | Typical latency |
|---|---|---|
| Warm cache hit | In-process request cache | <1ms |
| Cold miss, Arrow-native backend | S3/Parquet, ClickHouse | 2–15ms |
| Cold miss, JSON backend | Redis, PostgreSQL | 5–20ms + backend RTT |

!!! note

    The <1ms warm cache figure applies to any cache hit (TTL or SWR mode). The resolver returns a cached `RecordBatch` without any adapter calls. Cold miss latency includes backend RTT, adapter serialization, and the Arrow merge. For Arrow-native backends, the merge step is zero-copy; for JSON backends, it includes a deserialization step inside the adapter.
    
    Quiver's stated target is <10ms P99 on cold requests against network-backed stores. For use cases requiring sub-millisecond cold latency, in-process feature computation is the correct answer. The loopback network call adds ~50–100µs that cannot be eliminated by any sidecar architecture.