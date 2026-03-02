# Quiver — RFC v0.2

> An Arrow Flight feature server for unified feature serving across heterogeneous backends.
> One read endpoint. Consistent temporal semantics. Zero fan-out logic in model servers.

---

## Document Status

| Field | Value |
|---|---|
| Status | DRAFT |
| Authors | TBD |
| Created | 2026-03 |
| Supersedes | RFC v0.1 |
| Target Milestone | v0.1 Alpha |
| Slack / Discussion | TBD |

### Changelog from v0.1

| Section | Change | Reason |
|---|---|---|
| Problem Statement | Removed universal "serialization tax" framing | Arrow zero-copy only holds for Arrow-native backends; JSON-backed stores (Redis, Hopsworks REST, Feast server) still pay serialization cost on cold path |
| Problem Statement | Clarified fan-out argument | Hopsworks and Tecton already own fan-out internally; argument applies most cleanly to teams without a full platform |
| Design Principles | Qualified "Arrow all the way through" | Principle holds for greenfield Arrow-native stacks; partially holds for JSON-backed online stores where Arrow benefit is cache-layer only |
| Target User | Narrowed and segmented | Three distinct target segments with different value propositions; well-configured Hopsworks/Tecton teams are not primary targets |
| New section | §5: Value-Add by Existing Tooling | Honest per-platform breakdown of where Quiver adds value, where it doesn't, and what the realistic migration path looks like |

---

## Table of Contents

1. [Pre-Read](#1-pre-read)
2. [Problem Statement](#2-problem-statement)
3. [Product Vision](#3-product-vision)
4. [Non-Goals](#4-non-goals)
5. [Value-Add by Existing Tooling](#5-value-add-by-existing-tooling)
6. [Architecture Overview](#6-architecture-overview)
7. [Feature Specifications](#7-feature-specifications)
8. [Challenges as Engineering Problems](#8-challenges-as-engineering-problems)
9. [Dependencies](#9-dependencies)
10. [Phased Roadmap](#10-phased-roadmap)
11. [Open Questions](#11-open-questions)
12. [Glossary](#12-glossary)

---

## 1. Pre-Read

Before contributing to or evaluating Quiver, the following reading is strongly recommended. Each resource is tagged with the Quiver component it most directly informs.

### Apache Arrow and Arrow Flight

| Resource | URL | Relevant To |
|---|---|---|
| Apache Arrow Columnar Format Specification | https://arrow.apache.org/docs/format/Columnar.html | All layers — fundamental data model |
| Arrow Flight RPC Protocol Specification | https://arrow.apache.org/docs/format/Flight.html | Core transport — all RPCs |
| Arrow Flight SQL | https://arrow.apache.org/docs/format/FlightSql.html | Query interface design reference |
| Arrow IPC Streaming Format | https://arrow.apache.org/docs/format/IPC.html | Serialization in do_get / do_exchange |
| PyArrow Flight Documentation | https://arrow.apache.org/docs/python/flight.html | Python client + server implementation |
| Arrow Flight (Java) — reference server implementation | https://github.com/apache/arrow/tree/main/java/flight | Production server patterns |
| nanoarrow — C implementation of Arrow | https://github.com/apache/arrow-nanoarrow | Embedding Arrow in non-JVM services |
| Arrow Memory Model and Zero-Copy | https://arrow.apache.org/docs/python/memory.html | Zero-copy serving design |

### Arrow Flight in Production — Prior Art

| Resource | URL | Relevant To |
|---|---|---|
| Dremio Flight Server (production reference) | https://docs.dremio.com/software/client-applications/arrow-flight/ | Production Flight server patterns |
| DataFusion — Arrow-native query engine | https://github.com/apache/datafusion | DuckDB/DataFusion backend adapter |
| InfluxDB IOx — Arrow Flight for time series | https://github.com/influxdata/influxdb_iox | Temporal data + Flight patterns; reference Rust implementation |
| DuckDB Arrow integration | https://duckdb.org/docs/guides/python/arrow | DuckDB backend adapter design |
| Ballista — distributed DataFusion | https://github.com/apache/datafusion-ballista | Distributed execution reference |

### Feature Serving and the Serialization Problem

| Resource | URL | Relevant To |
|---|---|---|
| Uber Michelangelo — feature serving at scale | https://www.uber.com/en-GB/blog/michelangelo-machine-learning-platform/ | Problem context — why serving latency matters |
| Twitter Cortex — ML feature store internals | https://cortex.twitter.com | Multi-backend fan-out problem |
| LinkedIn Feathr — feature serving design | https://github.com/feathr-ai/feathr | Multi-backend adapter patterns |
| Feast online serving architecture | https://docs.feast.dev/getting-started/architecture | Comparison: what Quiver replaces/complements |
| Training-Serving Skew (Google Rules of ML) | https://developers.google.com/machine-learning/guides/rules-of-ml | Core problem: one code path for train + serve |

### gRPC and Protocol Buffers

| Resource | URL | Relevant To |
|---|---|---|
| gRPC Core Concepts | https://grpc.io/docs/what-is-grpc/core-concepts/ | Transport layer (Flight is gRPC-based) |
| gRPC Flow Control and Backpressure | https://grpc.io/blog/flow-control/ | do_exchange backpressure design |
| Protobuf Language Guide (proto3) | https://protobuf.dev/programming-guides/proto3/ | FeatureRequest descriptor design |
| gRPC bidirectional streaming | https://grpc.io/docs/languages/python/basics/#bidirectional-streaming-rpc | do_exchange implementation pattern |

### Caching Theory and SWR

| Resource | URL | Relevant To |
|---|---|---|
| RFC 5861 — HTTP Cache-Control Extensions | https://datatracker.ietf.org/doc/html/rfc5861 | Stale-while-revalidate definition |
| Cache Stampede / Thundering Herd | https://en.wikipedia.org/wiki/Cache_stampede | SWR stampede prevention (F-06) |

### Temporal Data and Point-in-Time Correctness

| Resource | URL | Relevant To |
|---|---|---|
| Slowly Changing Dimensions (Type 2) | https://en.wikipedia.org/wiki/Slowly_changing_dimension | Feature table temporal design |
| Delta Lake Time Travel | https://docs.delta.io/latest/delta-batch.html#-deltatimetravel | Delta adapter (F-09) |
| Bi-temporal data modeling | https://martinfowler.com/articles/bitemporal-history.html | Two-timestamp model (event time vs write time) |

### Observability and Middleware

| Resource | URL | Relevant To |
|---|---|---|
| OpenTelemetry gRPC instrumentation | https://opentelemetry.io/docs/instrumentation/python/manual/ | Flight middleware observability |
| Prometheus gRPC metrics | https://github.com/grpc-ecosystem/py-grpc-prometheus | Metrics exposition design |
| eBPF for observability (Cilium) | https://cilium.io/blog/2021/12/01/cilium-11/ | Inspiration: zero-instrumentation monitoring |

---

## 2. Problem Statement

### The Fan-Out Problem Lives in the Wrong Place

A single prediction request typically requires features from multiple sources: batch-computed user behavior stored in Redis, near-real-time transaction aggregates in Kafka, historical embeddings in Delta Lake. Today, the logic to decide which backends to query, issue requests in parallel, wait for responses, align entity keys, and assemble one feature vector lives in **application code in the model server**.

This fan-out code is:

- Duplicated across every model serving application in the organization
- Rarely tested (it is infrastructure glue, not business logic)
- A source of training/serving skew (each model assembles features differently)
- Invisible to the feature registry (no system knows how features are assembled at runtime)
- A coordination bottleneck when backend topology changes — every model server must be updated when a feature moves from Redis to DynamoDB

Quiver owns the fan-out. The model server asks for features by name. Quiver resolves which backends to query, assembles the result, and returns one RecordBatch. The model server has no knowledge of backends, key formats, or data locations.

This argument applies with full force to teams without a mature, centrally-managed feature store. For teams running Hopsworks or Tecton with properly-defined joined feature views, the fan-out is already owned by the platform — see §5 for the per-platform breakdown.

### The Two-Code-Path Problem

Training data generation and online serving are, in virtually every organization, different code paths:

- **Training:** Spark job reading from S3/Delta, joining feature groups, outputting Parquet
- **Serving:** Redis lookup, JSON deserialization, numpy construction

These paths diverge silently over time. A feature transformation updated in the batch pipeline is not automatically reflected in the serving path. This is **training/serving skew** — among the most common sources of ML production failures and among the hardest to detect because the discrepancy is silent until model performance degrades.

Quiver enforces one code path. `do_get` with an `as_of` timestamp in the past is training data generation. `do_get` with `as_of = now` is online serving. Same server, same temporal resolution logic, same backend adapters. Divergence is structurally prevented.

This argument applies with varying force depending on the platform in use — see §5.

### The Monitoring Gap

Existing feature stores monitor features in the offline store: statistics computed from materialized Parquet/Hive tables after batch pipelines run. This tells you what values were written to the store; it does not tell you what values are actually being served to models at prediction time.

The gap matters because:

- A Redis TTL issue can cause stale values to be served without any offline monitoring catching it
- A null rate spike in the online store is invisible until a model alert fires downstream
- Drift in served values (vs. training distribution) shows up in model metrics weeks before anyone traces it to the feature layer

Quiver's monitoring interceptor observes every RecordBatch in the serving path. Drift detection, null rate tracking, and value distributions are computed from actual serving traffic — not from offline store snapshots. This is the one value proposition that holds regardless of which feature store platform is already in use.

### The Serialization Tax (Backend-Dependent)

Every modern compute engine — Spark, DuckDB, Polars, DataFusion — uses Apache Arrow as its internal columnar format. Every modern ML framework — PyTorch, JAX, TensorFlow — can consume Arrow directly, zero-copy, via their respective array protocols.

For teams whose backend stack is already Arrow-native (DuckDB, Delta Lake via delta-rs, pgfactor), Quiver keeps data in Arrow format from computation to model input. No intermediate format, no re-serialization. This is a genuine cold-path latency improvement.

For teams whose online store speaks JSON (Redis with JSON values, Hopsworks REST API, Feast server, Tecton HTTP), the serialization tax on **cold cache misses** is not eliminated — it is transformed. The JSON-to-Arrow conversion happens inside Quiver rather than inside the model server. The benefit on cold requests is marginal; the benefit appears on warm cache hits where Quiver serves Arrow buffers from its in-process cache without touching the backend at all.

Teams should not adopt Quiver on the basis of cold-path serialization savings unless they are on Arrow-native backends or are willing to migrate to them.

---

## 3. Product Vision

### Name

**Quiver** — a quiver holds arrows and serves them to the archer on demand. Quiver holds Arrow-formatted feature data and serves it to model servers on demand. The metaphor is precise: a quiver is the interface between where arrows are made (the feature computation layer) and where they are used (the model serving layer).

### Design Principles

**1. Arrow where it matters.** For Arrow-native backends, data stays in Arrow from computation to model input. For JSON-backed backends, Arrow is the output format from the cache layer outward. "Arrow all the way through" is the goal for greenfield stacks; "Arrow from the cache outward" is the reality for most migration scenarios. The client API is Arrow in both cases.

**2. One endpoint, all backends.** The model server knows about Quiver. It does not know about Redis, DynamoDB, Kafka, Delta Lake, or DuckDB. Backend topology changes without model server changes.

**3. Temporal semantics at the server, not the client.** Point-in-time correctness is enforced by Quiver's temporal resolver. Clients specify `as_of`; the server handles routing to the appropriate backend. Training data generation and online serving are the same RPC with different `as_of` values.

**4. Stale-while-revalidate as a first-class primitive.** Freshness is a per-request policy, not a server configuration. Clients declare their tolerance for staleness; the server serves from cache and refreshes asynchronously. This is where Quiver's latency benefit is most reliable — regardless of backend serialization format.

**5. Observability without instrumentation.** Feature monitoring — value distributions, drift, null rates, latency — is middleware that intercepts the Arrow stream. Feature definitions do not need to be modified to be monitored. This is the one value proposition that holds for all backends without qualification.

**6. Earn the backends.** Start with one backend (DuckDB over Parquet). Add Redis, Kafka, and Delta as load and freshness requirements grow. Quiver degrades gracefully when backends are unavailable.

### Positioning

Quiver is **infrastructure middleware**, not a feature store. It does not own feature definitions, compute pipelines, or a feature registry. It is the serving layer that sits in front of whatever feature store — or combination of stores — a team already has.

Quiver is **not a replacement for Hopsworks, Tecton, or Databricks Feature Store** for teams that have adopted those platforms and configured them well. It is a complement or a sidecar, adding serving-time monitoring and Arrow output where those platforms don't provide them, and a migration path for teams whose platform lock-in is becoming a cost problem.

### Target User Segments

Quiver has three distinct target segments with meaningfully different value propositions. These should not be conflated in go-to-market.

**Segment A — No feature store (highest value)**

Teams running batch features from Parquet on S3, online features in Redis with hand-rolled key conventions, and fan-out logic scattered across model servers. No temporal correctness guarantees. No monitoring. Training data from a Spark job with different logic than the serving path. This segment gets the full Quiver value proposition: fan-out ownership, one code path, monitoring, and Arrow output.

**Segment B — Feast (high value)**

Feast deliberately does not own feature transformation or fan-out logic — both live in application code. The Feast server returns JSON. The training path (`get_historical_features`) and serving path (`get_online_features`) are different methods with different implementations. Monitoring requires external tooling bolted on. This segment benefits from Quiver's fan-out ownership, code path unification, and monitoring.

**Segment C — Greenfield (full value, best fit)**

Teams building a feature stack from scratch who choose Arrow-native backends: DuckDB for offline, delta-rs for historical, pgfactor for Postgres-backed features. Quiver's zero-copy argument holds end-to-end in this stack. This is the architecture Quiver is designed for.

**Anti-targets (proceed with caution)**

Teams with well-configured Hopsworks, Tecton, or Databricks Feature Store deployments. These platforms already solve fan-out, batch reads, training/serving unification, and have their own monitoring surfaces. The incremental value Quiver adds narrows to serving-time monitoring and Arrow output, which may not justify adding a new infrastructure component. See §5 for the detailed breakdown.

---

## 4. Non-Goals

| Non-Goal | Rationale |
|---|---|
| Feature registry / catalog | Quiver serves features; it does not define or register them. Registry integration is read-only. |
| Feature computation / transforms | Quiver does not compute features. It serves precomputed values. |
| Feature discoverability UI | No web UI. Quiver exposes `list_flights` and `get_flight_info` for programmatic discovery. |
| Write path / materialization | `do_put` is supported as an ingestion API for simple cases, but Quiver is not a materialization engine. |
| Multi-tenancy isolation (v1) | v1 targets single-tenant deployments. |
| Sub-millisecond P99 serving | Quiver targets <10ms P99 for cached single-entity lookups. Sub-millisecond requires in-process feature computation, which is out of scope. |
| Replacing well-integrated feature store platforms | Quiver complements or sidecars; it does not displace platforms that already solve the problems it addresses. |

---

## 5. Value-Add by Existing Tooling

This section documents what Quiver actually adds — and does not add — for teams already using specific feature store platforms or patterns. It is intended to inform go-to-market targeting and to prevent overclaiming in adoption conversations.

The honest answer is that Quiver's value is inversely proportional to how mature and well-configured the existing feature stack is. The highest-value adopters are teams with no feature store or with Feast. The lowest-value adopters are teams with a properly-configured Hopsworks or Tecton deployment.

---

### 5.1 No Feature Store (Raw Redis / Parquet / Postgres)

**Profile:** Features in Redis with hand-rolled key conventions, batch features in S3 Parquet, fan-out logic in every model server, training datasets from a Spark job or pandas script that doesn't share code with the serving path.

**What Quiver adds:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| Fan-out code duplicated in every model server | Resolver owns fan-out; model servers call one endpoint | High |
| Training/serving code path divergence | Same `do_get` for training (`as_of` in past) and serving (`as_of=now`) | High |
| No serving-time monitoring | Monitoring interceptor observes all traffic | High |
| Arrow serialization tax (DuckDB/Parquet backend) | Zero-copy from DuckDB to model server | High |
| Arrow serialization tax (Redis/JSON backend) | Applies on warm cache hits only; cold path still pays JSON cost | Medium |
| Ad-hoc SWR freshness semantics | Per-request staleness policy, async refresh | High |
| Backend topology changes cascade to model servers | Reconfigure Quiver; model servers unchanged | High |

**Migration effort:** Low. No existing feature store tooling to displace. Quiver is additive. Model servers are updated to use the Quiver client; Redis and S3 are unchanged.

**Recommended entry point:** Deploy Quiver with the DuckDB adapter pointing at existing Parquet files. Point the Redis adapter at the existing online store. Migrate one model server to the Quiver client. Measure latency and cache hit rate before expanding.

---

### 5.2 Feast

**Profile:** Features defined in Feast feature views, materialized to Redis or DynamoDB via Feast's offline-to-online pipeline, served via `FeatureStore.get_online_features()` which returns a Python dict. Training datasets generated via `FeatureStore.get_historical_features()` which runs a point-in-time SQL join on the offline store.

**What Feast already handles well:**
- Feature view registration and schema management
- Point-in-time correct historical feature joins (offline path)
- Multi-backend materialization (batch + streaming pipelines)

**What Feast leaves to application code or doesn't address:**
- Fan-out across multiple feature views at serving time (model server assembles these manually)
- A unified code path — `get_historical_features` and `get_online_features` are different methods with different implementations
- Arrow-native output — Feast returns Python dicts; calling code constructs numpy arrays
- Serving-time monitoring — Feast has no equivalent to Quiver's interceptor

**What Quiver adds:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| Fan-out across Feast feature views owned by model server | Resolver owns fan-out; Quiver reads Feast registry for routing | High |
| `get_historical_features` vs `get_online_features` code split | Single `do_get` for both; `as_of` controls routing | High |
| No serving-time monitoring | Monitoring interceptor | High |
| JSON dict → numpy array construction tax | Arrow output eliminates intermediate dict; warm cache hits are zero-copy | Medium (cold path depends on backend serialization) |
| SWR semantics not available | Per-request freshness policy | High |

**What Quiver does not add:**
- Feature definition or registry management (Feast keeps this)
- Offline store management or materialization (Feast keeps this)
- Significant cold-path serialization improvement if Feast's online store is Redis with JSON values

**Migration path:**

Phase 1 (1-2 days per model): Deploy Quiver with a Feast registry adapter and a Redis adapter using Feast's key pattern (`{feature_view}:{entity_id}`). Model server replaces `get_online_features()` calls with `quiver_client.get()`. Feast materialization pipelines and offline store are unchanged.

Phase 2 (optional): Replace training dataset generation with Quiver `do_get` + per-entity `as_of` timestamps. Validate against Feast's `get_historical_features` output on held-out historical data before cutting over.

Phase 3 (optional, long-term): Migrate feature definitions from Feast registry to pgfactor or YAML registry if Feast dependency is unwanted. Feast materialization pipelines are replaced by pipelines writing directly to backends Quiver serves.

**Relationship:** Quiver is a drop-in replacement for the Feast feature server while keeping the Feast registry and offline store. Many teams will stop at Phase 1 — using Quiver for serving while keeping Feast for everything else — and that is a stable, productive end state.

---

### 5.3 Databricks Feature Store

**Profile:** Feature tables defined in Databricks Feature Store, managed via the Unity Catalog or legacy feature store UI. Online store configured as DynamoDB, Cosmos DB, or MySQL. Training datasets generated via `FeatureLookup` in a Spark job (`create_training_set`). Online serving via Databricks Model Serving or a custom endpoint calling the Databricks Feature Store REST API.

**What Databricks Feature Store already handles well:**
- Feature table registration and Unity Catalog integration
- Batch materialization to online store via Databricks Jobs
- Point-in-time correct training datasets via `create_training_set` (Spark)
- Integration with Databricks Model Serving (automatic feature lookup at inference time)

**What Databricks Feature Store leaves unaddressed:**
- Non-Databricks model serving — teams serving models outside Databricks manage their own feature retrieval
- Serving-time monitoring outside of Databricks' built-in monitoring (which monitors the offline store, not online serving traffic)
- Arrow-native output — the Databricks Feature Store API returns Pandas DataFrames or dicts
- Training/serving code path unification for teams not using Databricks Model Serving

**What Quiver adds:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| Non-Databricks model server lacks clean feature retrieval | Quiver DynamoDB/MySQL adapter points at Databricks online store | High |
| Serving-time monitoring not available outside Databricks | Monitoring interceptor on all serving traffic | High |
| `create_training_set` Spark path different from serving path | `do_get` with `as_of` per entity replaces Spark job for training data | Medium (requires Delta adapter pointing at feature tables) |
| Arrow output for non-Databricks model servers | Quiver serves Arrow regardless of backend | Medium (cold path still pays JSON cost if online store is DynamoDB/MySQL) |

**What Quiver does not add for teams fully inside Databricks:**
- Anything. Teams using Databricks end-to-end (Jobs → Feature Store → Model Serving) already have a coherent integrated stack. Adding Quiver creates complexity without meaningful benefit.

**Migration path:**

Quiver is only relevant for the Databricks Feature Store when the model server lives outside Databricks. In that case, point Quiver's DynamoDB or MySQL adapter at the Databricks-managed online store. Databricks continues to own materialization; Quiver adds Arrow output and serving-time monitoring for the external model server.

The training dataset migration (replacing `create_training_set` with Quiver `do_get`) requires a Delta adapter pointing at the Delta tables that back Databricks feature tables. This is a meaningful improvement — the same code path for training and serving — but is a Phase 2 effort after the serving path is validated.

**Key caveat:** Databricks DynamoDB and MySQL online stores do not speak Arrow. Cold-path requests pay JSON/SDK deserialization costs. The Arrow benefit is cache-layer only.

---

### 5.4 Hopsworks

**Profile:** Feature groups defined via the `hsfs` Python SDK, stored in Hopsworks' offline store (Parquet/Hive on S3). Online store: RonDB (a MySQL NDB Cluster variant with genuine batch read support). Training datasets via `FeatureView.create_training_data()` (Spark temporal join). Online serving via `FeatureView.get_feature_vectors()` or the Hopsworks REST serving API.

**What Hopsworks already handles well:**
- Feature group registration and the Hopsworks registry UI
- Spark-based materialization pipelines with scheduling
- Batch entity reads against RonDB — `get_feature_vectors()` with multiple entities is a single round trip via RonDB's batch read capability
- Joined feature views that span multiple feature groups — a single API call can retrieve features from multiple groups simultaneously when the feature view is defined to join them
- Point-in-time correct training dataset generation via `create_training_data()`
- The `hsfs` SDK handles fan-out internally when feature views are properly defined

**What Hopsworks does not handle:**
- Arrow-native output — RonDB has no Arrow support; the Hopsworks REST API returns JSON. There is no zero-copy path from RonDB to model input, regardless of Quiver's involvement.
- Serving-time monitoring — Hopsworks monitors features in the offline store post-materialization. It does not observe what values are actually being served at prediction time.

**What Quiver adds for Hopsworks teams:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| No serving-time monitoring | Monitoring interceptor — the primary value proposition | High |
| JSON → numpy construction tax on warm cache hits | SWR cache serves Arrow IPC; warm hits are zero-copy | Medium |
| Cold-path JSON deserialization | **Not solved** — RonDB → Hopsworks REST → Quiver → JSON → Arrow adds steps | Negative |
| Fan-out across feature groups | **Not solved** — Hopsworks joined feature views already solve this more cleanly at the RonDB layer | None |
| Batch reads | **Not solved** — RonDB already supports batch reads natively | None |
| Training/serving code path divergence | Partial — Quiver `do_get` with `as_of` can replace `create_training_data()` if a DuckDB/Parquet adapter points at the Hopsworks offline store | Medium |

**The honest assessment:**

A team that has adopted Hopsworks and configured it correctly — joined feature views, batch reads, scheduled Spark pipelines — has already solved most of the problems Quiver claims to solve. The two-call fan-out problem that was used as a motivating example in earlier versions of this RFC is not a Hopsworks limitation: it is a consequence of not defining a joined feature view, which is a configuration mistake, not a platform gap.

Quiver's primary value for Hopsworks teams is **serving-time monitoring**, which Hopsworks does not provide. The SWR cache adds value for traffic patterns with meaningful entity repetition (the same user appearing multiple times in a short window — common in fraud detection). Neither of these alone is typically sufficient justification to add a new infrastructure component to a functioning platform deployment.

**When Quiver makes sense alongside Hopsworks:**
- The team has a specific serving-time monitoring requirement that Hopsworks cannot fulfill
- Traffic patterns show >60% entity repetition within the SWR window, making cache hit rates meaningful
- The team is migrating away from Hopsworks for cost or architectural reasons and needs a serving layer to bridge the migration

**When Quiver does not make sense alongside Hopsworks:**
- The deployment is well-configured (joined feature views, correct batch reads)
- Monitoring requirements are satisfied by Hopsworks' offline monitoring
- The team is not planning to change backend topology

**Migration path (if migrating away from Hopsworks):**

Phase 1: Deploy Quiver with an `hopsworks_rest` adapter that calls the Hopsworks serving API. This adds Arrow output and SWR cache but does not improve cold-path latency (two serialization hops). Value is monitoring only at this phase.

Phase 2: Replace the `hopsworks_rest` adapter with a direct RonDB MySQL adapter and a DuckDB adapter pointing at Hopsworks' S3/Parquet offline store. This is the step that actually improves cold-path latency and enables the one-code-path guarantee. Requires understanding RonDB's internal key schema, which is not publicly documented — engage Hopsworks support.

Phase 3 (if fully migrating): Replace Hopsworks' Spark materialization pipelines with pipelines writing to backends Quiver serves directly. This is a significant undertaking and should only be pursued when the Hopsworks contract cost justifies it.

---

### 5.5 Tecton

**Profile:** Features defined in Tecton's Python DSL, computed by Tecton-managed Spark/Flink pipelines, stored in DynamoDB or Redis (Tecton-managed), served via Tecton's managed feature server (REST or gRPC, returns JSON or Protobuf). Tecton is fully managed — the team operates neither the compute nor the serving layer.

**What Tecton already handles well:**
- Feature definition, versioning, and lineage
- Batch and streaming pipeline management (Tecton owns compute)
- Multi-feature fan-out at the serving layer (Tecton's server assembles multi-feature vectors)
- On-demand feature computation at serving time
- SLA-managed serving with latency guarantees

**What Tecton does not handle:**
- Arrow-native output — Tecton's serving API returns JSON or Protobuf
- Serving-time monitoring with the granularity that Quiver provides (Tecton has monitoring but at a coarser level)
- Portability — all features and pipelines are tied to Tecton's proprietary DSL and compute layer

**What Quiver adds for Tecton teams:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| No serving-time Arrow output | SWR cache serves Arrow to model server; cold path still JSON through Tecton | Medium (warm cache only) |
| Serving-time monitoring granularity | Monitoring interceptor on Quiver cache hits; misses go through Tecton unmonitored | Partial |
| Fan-out logic | **Not applicable** — Tecton already owns this | None |
| Training/serving code path | **Not applicable** — Tecton owns training dataset generation | None |
| Backend portability | **Not applicable** in Phase 1; requires extracting from Tecton entirely | None |

**The honest assessment:**

For teams on Tecton in Phase 1 (Quiver as a sidecar in front of Tecton's serving API), Quiver adds a caching layer with Arrow output. The cold path is Model Server → Quiver → Tecton server → DynamoDB/Redis — two network hops instead of one. Cache hits are faster; cache misses are slower. The net effect is positive only at cache hit rates above ~70%, which requires meaningful entity repetition in the traffic pattern.

Quiver in front of Tecton is a transitional architecture, not a destination. The only reason to do it is as Phase 1 of a migration away from Tecton.

**When Quiver makes sense alongside Tecton:**
- The team is actively planning to migrate off Tecton (usually cost-driven — Tecton is expensive at scale)
- Phase 1 proves the Quiver API and SWR semantics work for the team before the more disruptive migration steps

**When Quiver does not make sense alongside Tecton:**
- The team intends to remain on Tecton long-term
- Monitoring requirements are met by Tecton's built-in observability

**Migration path (if migrating away from Tecton):**

Phase 1: Deploy Quiver with a `tecton_rest` adapter. Value is SWR cache and Arrow output on warm hits. Cold path adds a hop.

Phase 2: Point Quiver's adapters at Tecton's underlying online store (DynamoDB/Redis) directly, bypassing the Tecton serving layer. Requires knowing Tecton's key format and value encoding — not publicly documented, may require support engagement or reverse engineering.

Phase 3: Extract feature definitions from Tecton's DSL and re-implement as dbt models or Spark jobs writing to DuckDB/Delta. This is the most significant step — Tecton's transformation logic must be reproduced and validated before decommissioning Tecton compute.

Full migration from a large Tecton deployment is typically a 6-12 month effort. The primary driver is contract cost, not architecture.

---

### 5.6 Greenfield (Arrow-Native Stack)

**Profile:** Teams building a feature stack from scratch, or teams migrating to a new stack, who choose Arrow-native backends throughout: DuckDB or DataFusion for offline/batch query, delta-rs for historical time-travel reads, pgfactor for Postgres-backed feature storage, Redis with Arrow IPC values (written by pgfactor's WAL publisher) for online serving.

**What Quiver adds:**

| Problem | Quiver Solution | Confidence |
|---|---|---|
| Zero-copy cold path from DuckDB to model server | DuckDB → Arrow → Flight → model server. No JSON anywhere | High |
| Zero-copy cold path from Delta to model server | delta-rs → Arrow → Flight → model server | High |
| Zero-copy warm path from Redis (Arrow IPC values) | Redis → Arrow IPC bytes → deserialize → Flight → model server | High |
| One code path for training and serving | `do_get` with `as_of` — temporal resolver routes to DuckDB/Delta for historical, Redis for current | High |
| Fan-out across backends | Resolver owns this | High |
| Serving-time monitoring | Monitoring interceptor | High |
| SWR semantics | Per-request freshness policy | High |

This is the architecture Quiver is designed for. All value propositions hold without qualification. The tradeoff is operational: the team must maintain DuckDB, Delta, Redis, and pgfactor as backend dependencies, which is a more complex operational posture than a single managed platform.

**The pgfactor + Quiver native stack:**

pgfactor (when built) is Quiver's native feature store companion. pgfactor owns feature definition, temporal correctness at write time, and WAL-based sync to Redis with Arrow IPC values. Quiver serves from both Postgres (offline/training) and Redis (online/serving) through a unified endpoint. The combination provides:

- Feature definition in SQL (pgfactor)
- Arrow zero-copy from both online and offline backends
- One code path for training and serving
- Serving-time monitoring
- No external infrastructure beyond Postgres and Redis

This is the target architecture for the joint pgfactor + Quiver roadmap.

---

### 5.7 Summary Matrix

The following matrix reflects Quiver's value-add for each platform. Ratings are: **High** (clear, unqualified benefit), **Medium** (benefit exists but qualified), **Low** (marginal benefit), **None** (platform already handles this or Quiver doesn't address it), **Negative** (Quiver makes this worse).

| Value Dimension | No Store | Feast | Databricks | Hopsworks | Tecton | Greenfield |
|---|---|---|---|---|---|---|
| Fan-out ownership | High | High | Medium | None | None | High |
| Training/serving unification | High | High | Medium | Low | None | High |
| Serving-time monitoring | High | High | High | High | Medium | High |
| Arrow zero-copy (cold path) | High* | Low | Low | Negative | Negative | High |
| Arrow zero-copy (warm cache) | High | Medium | Medium | Medium | Medium | High |
| SWR freshness semantics | High | High | High | Medium | Low | High |
| Backend decoupling | High | High | Medium | Low | Low | High |
| Operational overhead added | Low | Low | Medium | High | High | Low |

*Applies to DuckDB/Parquet backends. Redis/JSON backends are Medium.

**Target adoption priority:** Greenfield (best fit) → No feature store → Feast → Databricks (external serving only) → Hopsworks (monitoring wedge) → Tecton (migration path only)

---

## 6. Architecture Overview

### Component Map

```mermaid
graph TB
    subgraph Client Layer
        MS[Model Server\nPyTorch / TF / sklearn]
        TRAIN[Training Pipeline\nSpark / pandas / polars]
        NB[Notebook / Ad-hoc]
    end

    subgraph Quiver Server
        FE[Flight Endpoint\ngRPC / Arrow Flight]
        MID[Middleware Chain\nAuth · Metrics · Logging]
        FR[Feature Resolver\nDescriptor → RecordBatch]
        TR[Temporal Resolver\nPoint-in-time routing]
        SWR[SWR Cache Manager\nStale-while-revalidate]
        HOT[Hot Cache\nIn-process Arrow buffers]
        FO[Fan-out Coordinator\nParallel backend dispatch]
        MON[Monitoring Interceptor\nDrift · Null rate · Latency]
    end

    subgraph Backend Adapters
        DUCK[DuckDB Adapter\nParquet / local files]
        REDIS[Redis Adapter\nOnline store]
        DELTA[Delta / Paimon Adapter\nTime travel reads]
        KAFKA[Kafka Streams Adapter\nQueryable State]
        PG[Postgres Adapter\npgfactor / vanilla]
        CUSTOM[Custom Adapter\nPlugin interface]
    end

    subgraph External Backends
        S3[(S3 / Parquet)]
        RDB[(Redis)]
        DL[(Delta Lake / Paimon)]
        KS[(Kafka Streams\nState Store)]
        PGS[(PostgreSQL)]
    end

    subgraph Registry - Read Only
        REG[Feature Registry\nFeast / pgfactor / YAML\nSchema source of truth]
    end

    MS -->|Arrow Flight RPC| FE
    TRAIN -->|Arrow Flight RPC| FE
    NB -->|Arrow Flight RPC| FE

    FE --> MID
    MID --> FR
    FR --> TR
    TR --> SWR
    SWR --> HOT
    SWR --> FO
    FO --> DUCK
    FO --> REDIS
    FO --> DELTA
    FO --> KAFKA
    FO --> PG

    DUCK --> S3
    REDIS --> RDB
    DELTA --> DL
    KAFKA --> KS
    PG --> PGS

    FR --> REG
    MON -.->|intercepts stream| FE
    HOT -.->|cache hit short-circuit| FR
```

### Request Lifecycle: Online Serving

```mermaid
sequenceDiagram
    participant MS as Model Server
    participant FE as Flight Endpoint
    participant FR as Feature Resolver
    participant HOT as Hot Cache
    participant FO as Fan-out Coordinator
    participant REDIS as Redis Adapter
    participant DELTA as Delta Adapter
    participant MON as Monitoring Interceptor

    MS->>FE: do_exchange (open stream)
    MS->>FE: FeatureRequest{entities=[u:123], features=[spend_30d, session_count, embedding], as_of=now, freshness={max_stale=60s, async=true}}
    FE->>FR: resolve(request)
    FR->>HOT: lookup(entities, features, as_of)
    HOT-->>FR: HIT: spend_30d (age=12s, within 60s SWR window)
    HOT-->>FR: MISS: session_count, embedding
    FR->>FO: dispatch([session_count→Redis, embedding→Delta])
    FO->>REDIS: get(u:123, session_count)
    FO->>DELTA: get(u:123, embedding, as_of=now)
    REDIS-->>FO: RecordBatch{session_count}
    DELTA-->>FO: RecordBatch{embedding}
    FO-->>FR: merged RecordBatch{session_count, embedding}
    FR->>FR: merge with cached spend_30d
    FR->>HOT: async refresh spend_30d (SWR background)
    FR-->>FE: RecordBatch{spend_30d, session_count, embedding}
    FE->>MON: intercept(RecordBatch) — compute running stats
    FE-->>MS: RecordBatch (Arrow IPC, zero-copy from cache)
```

### Request Lifecycle: Training Data Generation

```mermaid
sequenceDiagram
    participant TRAIN as Training Pipeline
    participant FE as Flight Endpoint
    participant FR as Feature Resolver
    participant TR as Temporal Resolver
    participant DELTA as Delta Adapter
    participant PG as Postgres Adapter

    TRAIN->>FE: do_get(FeatureRequest{entities=[(u:1, t:2024-01-15), (u:2, t:2024-01-10)], features=[spend_30d, session_count], as_of=per_entity})
    FE->>FR: resolve(request)
    FR->>TR: route_by_as_of(entities_with_timestamps)
    TR->>TR: as_of in past → offline backends
    TR->>DELTA: time_travel_get(u:1, spend_30d, as_of=2024-01-15)
    TR->>PG: temporal_get(u:2, session_count, as_of=2024-01-10)
    DELTA-->>TR: RecordBatch (point-in-time value)
    PG-->>TR: RecordBatch (point-in-time value)
    TR-->>FR: merged, temporally-aligned RecordBatch
    FR-->>FE: RecordBatch
    FE-->>TRAIN: Arrow IPC stream — same schema as online serving
    Note over TRAIN,FE: Identical client call to online serving. as_of is the only difference.
```

### Deployment Topology

```mermaid
graph LR
    subgraph Minimal - Single Node
        A[Quiver Server\nDuckDB adapter\nParquet on local disk\nIn-process cache]
    end

    subgraph Standard - With Online Store
        B[Quiver Server\nDuckDB adapter\nRedis adapter\nIn-process cache]
        C[(Redis\nOnline store)]
        D[(S3 / Parquet\nOffline store)]
        B --- C
        B --- D
    end

    subgraph Production - Multi-Backend
        E[Quiver Server\nHA pair or\nstateless horizontal]
        F[(Redis Cluster)]
        G[(Delta Lake / S3)]
        H[(Kafka Streams\nState Stores)]
        I[(PostgreSQL\n+ pgfactor)]
        J[Feature Registry\nFeast / pgfactor]
        E --- F
        E --- G
        E --- H
        E --- I
        E -. reads schema .-> J
    end
```

---

## 7. Feature Specifications

> **Complexity Scale**
> - 🟢 **Low** — Standard Arrow Flight patterns, well-documented APIs, clear prior art.
> - 🟡 **Medium** — Requires deep Arrow/gRPC internals or novel composition of existing patterns.
> - 🔴 **High** — Research-level or sparsely-documented territory; high risk of correctness or performance surprises.

---

### F-01: Core Arrow Flight Server

**Complexity: 🟢 Low**

*Why:* PyArrow ships a complete, production-ready Arrow Flight server implementation (`pyarrow.flight.FlightServerBase`) that handles all gRPC transport concerns. The base implementation pattern is well-documented with multiple production deployments (Dremio, InfluxDB IOx) as reference. The server skeleton is low-complexity; the complexity lives in the components built on top of it (resolver, cache, adapters). A Rust implementation using the `arrow-flight` crate is equally well-supported and preferable for production performance — see OQ-1.

**Description:**
The core gRPC server implementing the Arrow Flight protocol. Exposes all six Flight RPCs, each mapped to a Quiver concept:

| Flight RPC | Quiver Mapping |
|---|---|
| `do_get(descriptor)` | Fetch features for entities — primary serving RPC |
| `do_put(descriptor, stream)` | Ingest precomputed features (write path) |
| `get_flight_info(descriptor)` | Describe a feature view: schema, metadata, backends |
| `list_flights(criteria)` | List all registered feature views |
| `do_action(action)` | Trigger operations: materialize, backfill, flush cache |
| `do_exchange(descriptor, stream)` | Bidirectional streaming: entities in, features out |

**Server configuration (YAML):**
```yaml
quiver:
  host: 0.0.0.0
  port: 8815          # Arrow Flight default port
  tls:
    cert: /etc/quiver/server.crt
    key: /etc/quiver/server.key
  max_concurrent_rpcs: 256
  max_message_size_mb: 64
  compression: lz4    # or zstd, none
```

**Python client usage:**
```python
import pyarrow.flight as fl
import pyarrow as pa

client = fl.connect("grpc+tls://quiver.internal:8815")

descriptor = fl.FlightDescriptor.for_command(
    FeatureRequest(
        feature_names=["spend_30d", "session_count"],
        entities=[EntityKey(entity_type="user", entity_id="123")],
        as_of=None,          # None = now (online serving)
        freshness=FreshnessPolicy(max_staleness_seconds=60, allow_async_refresh=True)
    ).SerializeToString()
)

reader = client.do_get(descriptor)
table = reader.read_all()
# table is a pyarrow.Table — zero copy into numpy/torch
tensor = torch.from_numpy(table.column("spend_30d").to_numpy())
```

---

### F-02: FeatureRequest Descriptor

**Complexity: 🟢 Low**

*Why:* This is a protobuf schema design problem, not an implementation problem. The descriptor is serialized into the `FlightDescriptor.cmd` bytes field — a well-understood pattern in Arrow Flight. The design choices require careful thought but no novel engineering.

**Description:**
The protobuf message embedded in `FlightDescriptor.cmd` that encodes what features to retrieve, for which entities, at what time, and with what freshness policy. This is Quiver's query language.

**Schema:**
```protobuf
syntax = "proto3";
package quiver.v1;

message FeatureRequest {
  repeated string feature_names   = 1;  // empty = all features in feature_view
  string feature_view             = 2;
  repeated EntityKey entities     = 3;
  optional Timestamp as_of        = 4;  // null = now (online serving)
  FreshnessPolicy freshness       = 5;
  RequestContext context          = 6;  // metadata for logging / lineage
  OutputOptions output            = 7;
}

message EntityKey {
  string entity_type  = 1;  // e.g. "user", "product"
  string entity_id    = 2;  // string representation of key value
}

message FreshnessPolicy {
  int64 max_staleness_seconds   = 1;  // 0 = always recompute
  bool  allow_async_refresh     = 2;  // stale-while-revalidate
  bool  require_all_features    = 3;  // false = return partial results on backend miss
}

message RequestContext {
  string request_id   = 1;
  string caller       = 2;  // model name / training job name
  string environment  = 3;  // "production", "staging", "training"
}

message OutputOptions {
  bool include_timestamps   = 1;  // include _feature_ts column in output
  bool include_freshness    = 2;  // include _is_stale column in output
  string null_strategy      = 3;  // "null", "forward_fill", "error"
}
```

---

### F-03: Feature Resolver

**Complexity: 🟡 Medium**

*Why:* The resolver must parse the `FeatureRequest`, look up feature metadata from the registry, determine which backend(s) hold each requested feature, route sub-requests to the correct backend adapters, and merge the results into a single, correctly-aligned Arrow RecordBatch. The alignment problem — ensuring one row per entity in request order, with NULLs for missing features, consistent column order — is the core engineering challenge.

**Description:**
The central component of Quiver. Takes a `FeatureRequest`, orchestrates backend lookups, and produces a single Arrow RecordBatch response.

**Resolver interface:**
```python
class FeatureResolver:
    def resolve(self, request: FeatureRequest) -> pa.RecordBatch:
        # 1. Resolve feature metadata from registry
        feature_meta = self.registry.lookup(request.feature_names)

        # 2. Check hot cache
        cache_result = self.hot_cache.get(request.entities, request.feature_names, request.as_of)

        # 3. Determine cache hits / misses
        hits, misses = self.partition_by_cache(cache_result, request)

        # 4. Fan-out misses to backends
        backend_results = self.fan_out.dispatch(misses, feature_meta)

        # 5. Merge hits + backend results, aligned to request entity order
        merged = self.align_and_merge(hits, backend_results, request)

        # 6. Apply null strategy
        return self.apply_null_strategy(merged, request.freshness.null_strategy)
```

**Schema consistency guarantee:** The output RecordBatch always has exactly the columns requested, in the order requested, with one row per entity in the order requested. Missing entities produce null rows. This is an invariant regardless of backend return order.

---

### F-04: Temporal Resolver

**Complexity: 🔴 High**

*Why:* The temporal resolver must make routing decisions based on `as_of` timestamp and backend capabilities. When `as_of` is in the past and the primary backend (Redis) only holds current values, the resolver must fall back to a time-travel-capable backend (Delta), apply the time-travel query correctly, and guarantee the result is the latest value available *at or before* `as_of`. Failure modes are silent in production. High complexity because the correctness envelope is large and testing requires generating and verifying historical feature datasets across multiple backends.

**Routing logic:**

```mermaid
flowchart TD
    A[FeatureRequest with as_of] --> B{as_of is null\nor now?}
    B -->|Yes - online serving| C[Route to online backends\nRedis / Kafka]
    B -->|No - historical| D{as_of within\nonline TTL window?}
    D -->|Yes - recent past| E[Route to online backend\nif TTL permits]
    D -->|No - historical| F[Route to offline backends\nDelta / Paimon / Postgres]
    C --> G[Return RecordBatch]
    E --> G
    F --> H{Backend supports\ntime travel?}
    H -->|Yes| I[Time-travel query\nat as_of]
    H -->|No| J[Scan + filter\nby timestamp column]
    I --> G
    J --> G
```

**Temporal capabilities registry (per backend):**
```yaml
backends:
  redis:
    temporal_capability: current_only
    ttl_seconds: 3600
  delta:
    temporal_capability: time_travel
    time_travel_granularity: snapshot
  postgres_pgfactor:
    temporal_capability: mvcc_range
    min_as_of: "now - retention_period"
  kafka_streams:
    temporal_capability: approximate_recency
    max_recency_seconds: 300
```

**Correctness invariant:** For a given `(entity, feature, as_of)` triple, return the value `v` such that `v._feature_ts = max(all _feature_ts WHERE _feature_ts <= as_of)`. If no such value exists, return NULL per `null_strategy`.

---

### F-05: In-Process Hot Cache

**Complexity: 🟡 Medium**

*Why:* An in-process cache using Arrow buffers is straightforward to implement. Complexity lies in eviction under Arrow's memory model — evicting a cached RecordBatch while a concurrent `do_get` holds a reference requires careful coordination to avoid use-after-free. Schema evolution (a feature's Arrow schema changes) requires version-tagged eviction.

**Description:**
In-process cache of Arrow RecordBatches keyed by `(entity, feature_set, schema_version)`. Eliminates backend round-trips for recently served entities.

```python
@dataclass
class CacheEntry:
    batch: pa.RecordBatch       # Arrow buffer — reference counted
    feature_ts: datetime        # timestamp of the feature value
    written_at: datetime        # wall clock when cached
    schema_version: int
    backend: str

class HotCache:
    max_size_bytes: int         # default: 512MB
    eviction_policy: str        # "lru" or "ttl"

    def get(self, entities, features, as_of) -> CacheResult: ...
    def put(self, batch, metadata) -> None: ...
    def invalidate_schema(self, feature_name, new_version) -> None: ...
    def stats(self) -> CacheStats: ...
```

**Metrics exposed:**
```
quiver_cache_hit_rate{feature_view, entity_type}
quiver_cache_size_bytes
quiver_cache_evictions_total
quiver_cache_entry_age_seconds{p50, p95, p99}
```

---

### F-06: Stale-While-Revalidate Cache Manager

**Complexity: 🟡 Medium**

*Why:* The SWR pattern is well-understood (RFC 5861). The complexity is in async refresh coordination — multiple concurrent requests for the same stale entity should not each trigger a backend lookup (cache stampede). Standard solution: per-entity in-flight refresh deduplication.

**Description:**
Manages stale-while-revalidate semantics of the hot cache. Serves stale cached values immediately while triggering background recomputation. Deduplicates concurrent refresh requests for the same entity.

**SWR state machine per cache entry:**

```mermaid
stateDiagram-v2
    [*] --> MISS: first request
    MISS --> FRESH: backend lookup completes
    FRESH --> STALE: age > max_staleness
    STALE --> FRESH: background refresh completes
    STALE --> STALE: concurrent request (serve stale, no new refresh)
    FRESH --> MISS: eviction or schema invalidation
```

**Stampede prevention:**
```python
class SWRManager:
    _in_flight: dict[CacheKey, asyncio.Future]

    async def get_or_refresh(self, key, request):
        entry = self.cache.get(key)

        if entry and entry.is_fresh(request.freshness):
            return entry.batch                          # fresh hit

        if entry and entry.is_stale(request.freshness):
            if key not in self._in_flight:
                self._in_flight[key] = asyncio.create_task(self.refresh(key, request))
            return entry.batch                          # serve stale immediately

        if key in self._in_flight:
            return await self._in_flight[key]          # wait for in-flight refresh

        result = await self.refresh(key, request)      # cold miss — block
        return result
```

---

### F-07: Backend Adapter Interface + DuckDB Adapter

**Complexity: 🟢 Low (interface) / 🟡 Medium (DuckDB adapter)**

**Description:**
The protocol that all backend adapters implement. The DuckDB adapter is the reference implementation and the default for local development and lightweight production.

**Adapter Protocol:**
```python
class BackendAdapter(Protocol):
    name: str
    temporal_capability: TemporalCapability

    def get(
        self,
        entities: pa.RecordBatch,
        feature_names: list[str],
        as_of: datetime | None,
        schema: pa.Schema
    ) -> pa.RecordBatch: ...

    def put(self, batch: pa.RecordBatch, metadata: FeatureMetadata) -> None: ...
    def health(self) -> HealthStatus: ...
```

**DuckDB adapter — temporal query:**
```python
class DuckDBAdapter:
    def get(self, entities, feature_names, as_of, schema):
        self.conn.register("_req_entities", entities)
        cols = ", ".join(f"f.{c}" for c in feature_names)

        result = self.conn.execute(f"""
            SELECT e.entity_id, {cols}
            FROM _req_entities e
            LEFT JOIN (
                SELECT * FROM features f2
                WHERE f2._feature_ts <= COALESCE(?, NOW())
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY _feature_ts DESC
                ) = 1
            ) f ON f.entity_id = e.entity_id
        """, [as_of]).arrow()   # returns pa.Table directly — no intermediate format
        return self.align_to_schema(result, schema, entities)
```

---

### F-08: Redis Adapter

**Complexity: 🟢 Low**

*Why:* Redis MGET is well-understood. The serialization format — Arrow IPC bytes (from pgfactor WAL publisher) or JSON (legacy) — determines the deserialization path. Arrow IPC deserialization is a `pa.ipc.read_message()` call.

**Important caveat:** Most production Redis deployments store feature values as JSON, not Arrow IPC. For these deployments, the Redis adapter pays JSON deserialization cost on every cold cache miss. The Arrow benefit is limited to warm cache hits served from Quiver's in-process cache. This should be communicated clearly in deployment documentation rather than elided.

**Configuration:**
```yaml
backends:
  redis:
    type: redis
    connection: redis://prod-cache:6379/0
    key_pattern: "pgf:{entity_type}:{entity_id}"
    serialization: arrow    # or json — most existing Redis deployments use json
    pool_size: 20
    temporal_capability: current_only
```

---

### F-09: Delta Lake / Paimon Adapter

**Complexity: 🔴 High**

*Why:* Delta Lake time travel requires reading the transaction log to find the correct snapshot, then reading data files with entity-key predicate pushdown. For large tables with many small files, file I/O dominates even with predicate pushdown. Paimon's Python API is immature and may require JVM bridging for v1.

**Description:**
Offline store adapter supporting time-travel reads against Delta Lake and Apache Paimon. Primary backend for historical `as_of` queries in training data generation.

```python
class DeltaAdapter:
    def get(self, entities, feature_names, as_of, schema):
        dt = DeltaTable(self.table_uri, storage_options=self.storage_opts)
        if as_of:
            dt.load_with_datetime(as_of.isoformat())

        entity_ids = entities.column("entity_id").to_pylist()
        result = dt.to_pyarrow(
            columns=["entity_id"] + feature_names,
            filters=[("entity_id", "in", entity_ids)]
        )
        return self.apply_temporal_filter(result, as_of)
```

---

### F-10: Monitoring Interceptor

**Complexity: 🟡 Medium**

*Why:* Arrow Flight's middleware API provides clean interception of all RPCs. The complexity is in online statistics computation — computing running mean, variance, null rate, and approximate quantiles on a stream of RecordBatches without blocking the serving path.

**Description:**
Flight middleware interceptor that observes all feature serving traffic and computes running statistics — value distributions, null rates, drift signals, and request latency — without any instrumentation in feature definitions or backend adapters.

This is the one Quiver value proposition that applies universally across all backend types and all existing feature store platforms. Whether Quiver is serving from DuckDB (Arrow-native) or Hopsworks REST (JSON), the monitoring interceptor observes the RecordBatch that leaves Quiver's cache and computes statistics from it. No backend modification required.

**Statistics computed per feature column:**

| Metric | Algorithm | Memory |
|---|---|---|
| Null rate | Exact (count nulls / total) | O(1) |
| Mean, variance | Welford's online algorithm | O(1) |
| Approximate quantiles (p50, p95, p99) | t-digest | ~1KB |
| Approximate cardinality | HyperLogLog | ~1KB |
| Distribution shift (drift) | PSI vs. registered baseline | ~10KB |

**Metrics exposed (Prometheus):**
```
quiver_feature_null_rate{feature, entity_type, environment}
quiver_feature_value_p50{feature}
quiver_feature_value_p99{feature}
quiver_feature_drift_psi{feature}      # > 0.2 = significant drift
quiver_request_latency_ms{rpc, p50, p95, p99}
quiver_backend_latency_ms{backend, rpc}
quiver_cache_hit_ratio{feature_view}
```

---

### F-11: do_exchange Bidirectional Streaming

**Complexity: 🔴 High**

*Why:* `do_exchange` is the least-used and least-documented Arrow Flight RPC. Bidirectional streaming requires careful flow control — if the client sends entity keys faster than the server resolves features, the server must apply backpressure via gRPC's flow control rather than silently dropping or buffering unboundedly. PyArrow's interaction between Arrow IPC write path and gRPC flow control is sparsely documented.

**Description:**
Bidirectional streaming RPC enabling continuous entity-feature exchange over a single long-lived connection. Optimized for model servers doing high-frequency prediction where opening a new connection per request is expensive.

```python
def do_exchange(self, context, reader, writer):
    async def process():
        async for chunk in self._read_chunks(reader):
            entities = chunk.data
            request = FeatureRequest.FromString(chunk.app_metadata)
            result = await self.resolver.resolve_async(request, entities)
            writer.write_with_metadata(result, request.request_id.encode())
    asyncio.new_event_loop().run_until_complete(process())
```

---

### F-12: Registry Integration (Read-Only)

**Complexity: 🟢 Low**

**Description:**
Quiver reads feature metadata from an external registry at startup and on configurable refresh intervals. Quiver does not own feature definitions.

**Supported registries:**
```yaml
# pgfactor
registry:
  type: pgfactor
  connection: "postgres://user:pass@pgfactor-host:5432/features"
  refresh_interval: 60s

# Feast
registry:
  type: feast
  repo_path: /etc/quiver/feature_repo

# Static YAML
registry:
  type: yaml
  path: /etc/quiver/features.yaml
```

---

## 8. Challenges as Engineering Problems

### C-01: Multi-Backend Result Alignment

**Problem:** When features are fetched from multiple backends simultaneously, each backend returns results in arbitrary order with potentially different entity subsets. Assembling one correctly-aligned RecordBatch from N heterogeneous responses is the core correctness challenge.

**Mitigation:**
- Entity alignment via Arrow join using `pyarrow.compute.take()` after building an index on `entity_id`
- Schema normalization before merge — type mismatches cast to canonical schema before alignment
- Test suite covering: all entities found; some entities missing; entity in cache + backend (cache wins); backends return different ordering; backends return extra entities not in request

---

### C-02: Backpressure in do_exchange

**Problem:** In a high-throughput `do_exchange` session, the model server can send entity keys faster than Quiver resolves features. Without backpressure, server-side buffers fill and the process crashes or drops data.

**Mitigation:**
- gRPC flow control applies backpressure at the transport level automatically
- Bounded async queue between entity reader coroutine and resolver coroutine — configurable size
- `quiver_exchange_backpressure_events_total` metric for visibility

---

### C-03: Arrow Schema Evolution at Runtime

**Problem:** If a feature's schema changes while Quiver is serving, in-flight responses may be mid-stream when the change is detected. Cached entries under the old schema must be invalidated without corrupting in-flight reads.

**Mitigation:**
- Schema version tags on all cache entries; stale entries evicted on registry refresh
- No mid-stream schema changes — an active `do_exchange` session continues with its opening schema until closed
- `do_action("reload_schema")` for explicit reload; schema version returned in response metadata

---

### C-04: Delta / Paimon Time Travel Performance

**Problem:** Point-in-time reads from Delta require reading the transaction log then reading data files with predicate pushdown. Large tables with many small files make this prohibitive even with entity-key filtering.

**Mitigation:**
- Z-order clustering on entity key column — documented as a deployment requirement for the Delta adapter
- Snapshot metadata caching in process memory — repeated training runs at the same `as_of` reuse cached snapshot info
- Parallel file reads via configurable thread pool
- Benchmark requirement: publish latency for 100, 10K, and 1M entities against a 100M-row table, Z-ordered and not

---

### C-05: Hot Cache Memory Pressure

**Problem:** Large feature views (50+ float64 columns, 1M+ entities) can push cache memory requirements into tens of gigabytes.

**Mitigation:**
- Column-level caching — cache only the requested columns, not the full feature view
- Arrow dictionary encoding at cache write time for high-cardinality string features
- Hard `max_size_bytes` bound with LRU eviction
- Memory-mapped IPC file fallback for deployments where cache exceeds available RAM

---

### C-06: Kafka Streams State Store Querying

**Problem:** Kafka Streams Interactive Queries API is designed for single-key lookups. Batch entity fetches require N parallel HTTP requests.

**Mitigation:**
- Parallel HTTP requests with configurable concurrency limit
- Partition-aware dispatch — group entities by Kafka partition key and route directly to the correct Streams instance
- Document Flink Queryable State as the preferred alternative for high-cardinality batch entity lookups

---

## 9. Dependencies

### Hard Dependencies

| Dependency | Version | Purpose | Risk |
|---|---|---|---|
| Python | 3.11+ | Server runtime (or Rust — see OQ-1) | Low |
| pyarrow | 14.0+ | Arrow Flight server + IPC | Low — mature, stable API |
| grpcio | 1.60+ | gRPC transport (used by pyarrow.flight) | Low |
| protobuf | 4.0+ | FeatureRequest descriptor serialization | Low |

### Backend-Specific Dependencies

| Dependency | Backend | Notes |
|---|---|---|
| redis-py | Redis adapter | Arrow IPC bytes or JSON via MGET |
| delta-rs | Delta Lake adapter | Rust bindings, Arrow-native |
| duckdb | DuckDB adapter | Arrow-native query results |
| psycopg3 | Postgres / pgfactor adapter | Binary protocol |
| confluent-kafka | Kafka Streams adapter | HTTP client to Interactive Queries API |
| pypaimon | Paimon adapter | Immature in v1; may require JVM bridge |

### Observability Dependencies

| Dependency | Purpose | Required |
|---|---|---|
| prometheus-client | Metrics exposition | Yes |
| opentelemetry-sdk | Distributed tracing | No (recommended) |
| tdigest | Approximate quantiles | Yes |

### Build and Test Dependencies

| Dependency | Purpose |
|---|---|
| pytest-asyncio | Async test support |
| hypothesis | Property-based testing for resolver correctness |
| testcontainers | Spin up Redis, Postgres, Kafka in CI |
| locust | Load testing for Flight endpoint |

---

## 10. Phased Roadmap

### v0.1 Alpha — Core Serving

**Goal:** Prove the Arrow Flight serving model with DuckDB + Redis backends and in-process SWR cache. Target: Segment A (no feature store) and greenfield teams.

| Feature | Status |
|---|---|
| F-01: Core Arrow Flight Server | Include |
| F-02: FeatureRequest Descriptor | Include |
| F-03: Feature Resolver (single backend, no fan-out) | Include (simplified) |
| F-04: Temporal Resolver (DuckDB only) | Include |
| F-05: Hot Cache (basic LRU, no SWR) | Include |
| F-07: DuckDB Adapter | Include |
| F-08: Redis Adapter (JSON + Arrow IPC) | Include |
| F-12: Registry Integration (YAML only) | Include |
| HTTP fallback server (stdlib, no pyarrow required) | Include — for adoption in restricted environments |

---

### v0.2 Beta — Fan-out, SWR, and Registry

**Goal:** Multi-backend fan-out, stale-while-revalidate, Feast/pgfactor registry integration, monitoring. Target: Segment B (Feast) and platform teams.

| Feature | Status |
|---|---|
| F-03: Feature Resolver (full fan-out) | Include |
| F-06: SWR Cache Manager | Include |
| F-09: Delta Lake Adapter | Include |
| F-10: Monitoring Interceptor (null rate + latency + basic drift) | Include |
| F-12: Registry (Feast + pgfactor) | Include |
| Hopsworks REST adapter | Include — for monitoring wedge use case |

---

### v0.3 — Streaming, Full Observability, and Bidirectional Serving

**Goal:** Kafka Streams adapter, full drift detection (PSI), do_exchange for high-frequency model servers.

| Feature | Status |
|---|---|
| F-11: do_exchange Bidirectional Streaming | Include |
| Kafka Streams Adapter | Include |
| F-10: Full drift detection (PSI vs. baselines) | Include |
| Paimon Adapter (native pypaimon or JVM bridge) | Include |
| Tecton REST adapter | Include — for migration path use case |

---

### v1.0 — Production Release

**Goal:** Rust implementation (if OQ-1 resolves to Rust), full HA deployment, published benchmarks.

| Feature | Status |
|---|---|
| Rust server rewrite (if chosen) | Include |
| HA deployment (stateless horizontal scaling) | Include |
| Multi-tenancy (namespace isolation) | Include |
| Published benchmark suite per backend type | Include |
| Documentation site | Include |

---

## 11. Open Questions

| # | Question | Options | Blocking |
|---|---|---|---|
| OQ-1 | Python or Rust for server implementation? | Python: faster to start, larger contributor pool. Rust: better P99, memory safety for cache management, harder to contribute. Recommended path: Python prototype for validation, Rust rewrite for v1. | Yes, before any code |
| OQ-2 | Feature views spanning multiple backends — routing strategy? | Per-column backend routing in a single feature view vs. separate views merged by resolver. Impacts registry schema. | Yes, before F-03 |
| OQ-3 | Per-instance vs. shared (Redis-backed) hot cache? | Per-instance: simpler, no coordination. Shared: better hit rate for HA deployments, adds Redis dependency for the cache layer itself. | Yes, before F-05 |
| OQ-4 | Entity key representation: string serialization vs. typed? | String: backend-agnostic, simple. Typed: avoids string→type round-trip in each adapter, more complex descriptor. | Yes, before F-02 |
| OQ-5 | Should Quiver maintain its own feature registry or remain read-only? | Read-only: cleaner separation. Own registry: enables Quiver-only deployments without Feast/pgfactor. | No — v2 concern |
| OQ-6 | Authentication: mTLS only or JWT support? | mTLS: simpler. JWT: more flexible for multi-tenant. | No, but pre-v1 |
| OQ-7 | Paimon adapter v1: native pypaimon vs. JVM subprocess bridge? | Subprocess: works now, ugly. pypaimon: cleaner, API maturity unclear. | No — v0.3 concern |
| OQ-8 | Slim Docker image + optional extras vs. fat image with all backends? | Slim: explicit dependencies. Fat: easier getting started. | No |
| OQ-9 | How to communicate serialization format dependency to users? (Arrow zero-copy only holds for Arrow-native backends; JSON-backed stores only benefit on cache hits.) | Documentation only, or runtime warning when JSON backend is configured? | No, but pre-v0.1 |

---

## 12. Glossary

| Term | Definition in Quiver context |
|---|---|
| Arrow Flight | A gRPC-based RPC framework from Apache Arrow for high-performance data transfer using the Arrow columnar format |
| do_get | Arrow Flight RPC for fetching a data stream from the server — primary Quiver serving RPC |
| do_exchange | Arrow Flight bidirectional streaming RPC — entities in, features out, over a single persistent connection |
| FeatureRequest | Protobuf message embedded in a `FlightDescriptor.cmd` that encodes which features to retrieve, for which entities, at what time, and with what freshness policy |
| Feature resolver | The Quiver component that takes a `FeatureRequest`, routes to backends, and assembles a single aligned Arrow RecordBatch |
| Temporal resolver | The Quiver component that routes feature lookups to the appropriate backend based on `as_of` timestamp and backend temporal capabilities |
| Hot cache | An in-process cache of Arrow RecordBatches, bounded by a configurable memory pool, managed by the SWR Cache Manager |
| SWR (Stale-While-Revalidate) | A caching pattern: serve a stale cached value immediately while triggering an asynchronous background refresh. From RFC 5861. |
| Fan-out | Dispatching sub-requests to multiple backends simultaneously to assemble a single feature vector from features stored across different systems |
| Backend adapter | A pluggable component implementing the Quiver adapter protocol for a specific storage backend |
| Temporal capability | A property of a backend describing supported historical queries: `current_only`, `approximate_recency`, `time_travel`, `mvcc_range` |
| Training/serving skew | Discrepancy between feature values used during model training and feature values seen at prediction time. Quiver prevents this by using the same `do_get` path for both. |
| Schema version | A monotonically increasing integer on each feature view schema version in the registry. Used by the hot cache to invalidate stale entries on schema change. |
| Monitoring interceptor | Arrow Flight middleware that observes every RecordBatch sent by the server and computes running statistics without modifying feature definitions or adapters |
| PSI (Population Stability Index) | A statistical measure of distribution shift. PSI > 0.2 indicates significant drift. |
| Arrow-native backend | A backend that returns Arrow-format data natively (DuckDB, delta-rs, pgfactor). Distinct from JSON-backed backends (Redis with JSON values, Hopsworks REST, Feast server) where Arrow conversion happens inside Quiver. |
| JSON-backed backend | A backend that returns row-oriented JSON or Protobuf (Redis with JSON values, Hopsworks REST API, Tecton serving API). Cold-path Arrow benefit is not present; warm-cache benefit remains. |
