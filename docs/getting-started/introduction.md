# Introduction

## What is Quiver?

Quiver is a configurable feature serving system for machine learning inference. It resolves feature requests across multiple backends in a single gRPC call and returns the results as columnar data via [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) for direct consumption by model inference pipelines.

!!! note

    Quiver does not compute or transform features; it __only__ serves them.

The core abstraction is a [feature view](../concepts/feature-views.md): a named group of features for an entity type, with each column mapped to a backend adapter. Quiver's execution engine routes requests to the correct adapters, dispatches them in parallel, and merges the results into a single entity-aligned [`RecordBatch`](https://arrow.apache.org/docs/python/data.html#record-batches) before returning it to the caller.

Quiver provides:

- A high-performance Rust gRPC server exposing [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) endpoints for feature retrieval and [observability](../concepts/architecture.md)
- A [Python client SDK](../reference/python-client.md) for fetching features and converting them directly to `pandas`, `torch`, `numpy`, or raw Arrow
- A [YAML-based static registry](../concepts/configuration.md) that maps logical feature names to physical storage locations, defining column schemas and backend routing with no code changes required
- Request-level caching with configurable TTL or stale-while-revalidate (SWR) modes, keeping latency low on repeated entity requests
- Per-request observability: every response carries a `request_id` with per-stage latency breakdowns (registry, dispatch, merge, serialization) retrievable from a dedicated metrics endpoint

Quiver allows ML platform teams to:

- **Eliminate fan-out logic from model servers** by owning feature routing, [parallel dispatch](../concepts/fanout.md), and result merging in a single dedicated layer. Model servers make one call regardless of how many backends are involved.
- **Return columnar data ready for inference** by transporting features as Arrow `RecordBatch` objects. For Arrow-native backends (S3/Parquet, ClickHouse), this is zero-copy end-to-end. For all backends, data arrives in columnar format with no row-to-columnar reshaping at the model server.
- **Decouple features from infrastructure** by mapping logical feature names to physical backends in config. Migrating a feature from Redis to ClickHouse is a one-line YAML change that is invisible to every model server consuming it.
- **Serve the same features for training and inference** via [`as_of` queries](../concepts/temporal-queries.md). The same `client.get_features()` call that powers online inference can retrieve point-in-time feature values for training data, structurally preventing training/serving skew from diverging code paths.

---

## Who is Quiver for?

Quiver is primarily for ML engineers and platform teams who are building or operating real-time inference systems where features come from multiple backends.

**For ML engineers:** Quiver is the client you call at inference time. Instead of managing Redis, Postgres, and S3 connections inside your model server, you call `client.get_features()` and receive a table. You can focus on the model; Quiver handles routing, retries, merging, and null-filling for missing data.

**For ML platform / MLOps teams:** Quiver is infrastructure you deploy once per model server as a [sidecar](../guides/kubernetes-sidecar.md), not a shared centralized proxy. [Feature views](../concepts/feature-views.md) are defined in YAML and version-controlled. Teams can add new backends, change routing, or update schemas without touching model server code. The observability endpoint gives you per-request latency visibility across every stage of the serving path.

**For data engineers:** Quiver provides a clean boundary between your data systems and the models that consume them. You control what lives in Redis vs. PostgreSQL vs. S3. Quiver translates logical feature requests into physical queries against those systems. Model servers never need to know the difference.

---

## What Quiver is not

### Hard boundaries

- **A feature store.** Quiver does not compute, transform, backfill, or materialize features. It does not manage pipelines or schedule jobs. Tools like [Feast](https://feast.dev), [Tecton](https://www.tecton.ai), or [Hopsworks](https://www.hopsworks.ai) handle that. Quiver plugs in downstream. It serves features that already exist in your backends.
- **A database or data warehouse.** Quiver reads from your existing storage systems. It does not store feature data itself. The memory adapter exists for testing and local development only.
- **A streaming system.** Quiver does not ingest streams, manage Kafka topics, or run streaming pipelines. Redis is typically the real-time layer; Quiver serves from it.
- **An ETL or orchestration tool.** Quiver has no concept of jobs, DAGs, or schedules. It is a synchronous serving layer. It responds to requests, it does not initiate data movement.

### Soft boundaries

- **Point-in-time training dataset generation.** Quiver supports [`as_of` queries](../concepts/temporal-queries.md), and for moderate-scale training data needs with existing backends this works well. For large-scale offline dataset generation across billions of rows, a dedicated offline store is better suited.
- **Feature discovery and governance.** Quiver's registry is operational. It is not a feature catalog with lineage, ownership metadata, or search. Tools like [DataHub](https://datahubproject.io) complement Quiver for that.
- **Schema evolution and migrations.** Quiver reads schemas from YAML config. There is no migration system for evolving [feature view](../concepts/feature-views.md) schemas across environments.
- **Sub-millisecond latency requirements.** Quiver's [sidecar deployment](../guides/kubernetes-sidecar.md) adds a loopback network call. For use cases where features must be retrieved in under a millisecond on cold requests, in-process feature computation is the only correct answer. Quiver targets <10ms P99 on cold requests and <1ms on warm cache hits.

---

## How it works

Quiver exposes a primary [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) endpoint on port `8815`. Clients send a `FeatureRequest`: a [feature view](../concepts/feature-views.md) name, a list of entity IDs, and a list of feature names. They receive a [`RecordBatch`](https://arrow.apache.org/docs/python/data.html#record-batches) back.

```
Client (model server)
    │
    │  Arrow Flight  do_get(FeatureRequest)
    ▼
QuiverFlightServer                         port 8815
    │
    ├── StaticRegistry      maps feature names → backends + Arrow types
    ├── Resolver            groups features by adapter, enforces UTC timestamps
    ├── RequestCache        TTL (default) or SWR mode, configurable per deployment
    │
    └── FanoutMerger        parallel async dispatch → Arrow LEFT JOIN
            ├── RedisAdapter
            ├── PostgresAdapter
            ├── S3ParquetAdapter
            ├── ClickHouseAdapter
            └── MemoryAdapter
                    │
                    ▼
            RecordBatch   (columnar, entity-aligned)
```

Each adapter returns a sparse result with only the entities it has data for. The [merge step](../concepts/fanout.md) performs a 4-phase Arrow LEFT JOIN that guarantees the output batch exactly matches the requested entity order and count, with null-filling for any missing data. Backends that time out are treated as fully null rather than failing the entire request.

### Latency profile

Quiver's latency characteristics depend on where a request is served from:

| Request type | Source | Typical latency |
|---|---|---|
| Warm cache hit | In-process request cache | <1ms |
| Cold miss, Arrow-native backend | S3/Parquet, ClickHouse | 2-15ms |
| Cold miss, JSON backend | Redis, PostgreSQL | 5-20ms + backend RTT |

The <1ms figure on the landing page refers to warm cache hits. Cold requests against JSON-backed stores pay a serialization cost that Quiver does not eliminate. Teams with traffic patterns that produce high entity repetition (e.g. fraud detection, where the same user generates multiple events in a short window) will see the request cache do most of the work in production.

### Observability

A second gRPC server on port `8816` exposes a dedicated observability endpoint. Every response from port `8815` includes an `x-quiver-request-id` header; pass that ID to the observability endpoint to retrieve per-stage latency breakdowns covering registry lookup, fan-out dispatch, merge, and serialization.

---

## Deployment model

Quiver is designed to run as a **[sidecar](../guides/kubernetes-sidecar.md)**: one instance per model server pod, not a shared centralized service.

- **No added network hop.** Requests from the model server to Quiver travel over the pod's loopback interface. The backends Quiver talks to are the same backends the model server would have called directly.
- **No single point of failure.** Each model server pod is self-contained. A Quiver instance crashing affects only the pod it runs in.

Teams that need different latency characteristics, different backends, or different caching policies deploy Quiver with different [configuration](../concepts/configuration.md) per model server. The YAML registry is the only shared artifact.

---

## Use cases

- **Real-time recommendation serving.** A recommendation model needs user embeddings (Redis), item features (PostgreSQL), and precomputed affinity scores (S3). Quiver fans out to all three and returns a merged table in a single call.
- **Fraud detection at inference time.** Transaction features from ClickHouse, account history from PostgreSQL, and device signals from Redis, combined into one batch for a fraud model. Fan-out logic lives in Quiver, not the model server.
- **Training/serving consistency.** The same `client.get_features()` call used at inference time can be called with a per-entity `as_of` timestamp to retrieve point-in-time feature values for training. There is no separate code path to drift.
- **Multi-team feature sharing.** A platform team defines [feature views](../concepts/feature-views.md) in Quiver's registry. ML teams consume features by name without knowing which backend owns them. Backend migrations are transparent to model servers.

---

## Getting started

The fastest way to get started is the [Quickstart](quickstart.md), which walks through the architecture, request lifecycle, and a minimal working example.

| Section | Description |
|---|---|
| [Quickstart](quickstart.md) | Architecture, request lifecycle, and a minimal working example |
| [Installation](installation.md) | Install the server binary and Python client |
| [Concepts](../concepts/architecture.md) | Feature views, adapters, temporal queries, and fanout internals |
| [Guides](../guides/docker.md) | Docker, Kubernetes sidecar, and multi-backend configurations |
| [Configuration](../concepts/configuration.md) | Full YAML schema for server, registry, adapters, and fanout |
| [Python Client](../reference/python-client.md) | SDK reference for `Client`, `FeatureTable`, and observability |
