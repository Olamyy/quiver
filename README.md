# Quiver

**Quiver** is an experimental, Arrow‑native feature serving layer for machine learning inference.

It sits between model servers and feature backends, resolving feature requests, executing parallel retrieval across data sources, and returning columnar feature data via Arrow Flight.

Quiver focuses on a part of the ML stack that is often overlooked: **the serving path from computed features to model inputs**.

---

# Overview

Modern ML systems typically invest heavily in feature computation and model training, but the real‑time path that delivers features to models often evolves organically inside application code.

Typical inference pipelines end up performing several expensive transformations:

1. Fetch feature rows from multiple backends
2. Deserialize row‑oriented data
3. Join results across sources
4. Convert rows into arrays
5. Convert arrays into tensors

This results in unnecessary latency, duplicated logic across services, and limited observability into what features models actually consume.

Quiver extracts this responsibility into a dedicated infrastructure layer.

---

# Motivation

In many production inference systems the serving path resembles the following pipeline.

```
Feature Computation
(Spark / Polars / DuckDB)
        |
        v
Serialize rows
(JSON / Protobuf)
        |
        v
Online store
(Redis / key‑value)
        |
        v
Application server
        |
        |-- fetch features from multiple systems
        |-- deserialize rows
        |-- reshape arrays
        |-- convert to tensors
        v
Model inference
```

Several inefficiencies appear in this design.

## Repeated serialization

Features are often converted from columnar formats used during computation into row formats for storage, then converted back into arrays for inference.

Each request incurs repeated serialization and deserialization overhead.

## Row‑oriented data in a columnar workload

Machine learning frameworks operate on vectors and tensors, yet many feature serving systems return row‑oriented data structures.

This mismatch forces additional reshaping and copying inside model servers.

## Fan‑out logic inside model services

When features reside in multiple systems (for example Redis, Parquet, or a feature store), model servers must coordinate several backend requests.

This logic is often duplicated across services and grows increasingly complex as feature sets expand.

## Limited observability

Monitoring systems frequently track feature pipelines or model outputs, but rarely capture statistics about **the actual features sent to models at serving time**.

---

# What Quiver Does

Quiver provides a unified layer that resolves feature requests and returns columnar feature batches optimized for inference workloads.

Key capabilities include:

### Feature resolution

Model servers request logical feature names rather than backend‑specific queries.

The Quiver resolver determines:

* which backend stores each feature
* which entity key should be used
* the Arrow schema for the result

This decouples model servers from storage details.

### Parallel backend execution

Feature requests are executed in parallel across multiple backends, eliminating sequential round‑trips from model servers.

### Columnar feature transport

Quiver returns feature data as Arrow `RecordBatch` objects via Arrow Flight.

Columnar batches enable efficient vectorized processing and straightforward conversion into tensors.

### Stale‑While‑Revalidate caching

Quiver includes an optional SWR cache that allows low‑latency serving while asynchronously refreshing stale values.

### Serving‑time observability

Because Quiver sits directly on the serving path, it can observe feature distributions and latency without requiring instrumentation inside model services.

---

# Design Goals

Quiver is built around several principles.

## Arrow‑native data path

Whenever possible, Quiver keeps features in Arrow columnar buffers from storage through inference.

This avoids repeated row‑to‑column transformations and allows efficient batch processing.

## Decoupled feature resolution

Model servers should not need to know where features are stored or how they are retrieved.

All backend routing logic lives inside Quiver.

## Vectorized serving

Inference workloads frequently request features for many entities simultaneously. Quiver is optimized for returning batches of features rather than individual rows.

## Minimal application complexity

Model services should ideally perform only two operations:

1. request features
2. run inference

---

# Architecture

The Quiver server consists of several cooperating components.

```
Clients
  |
  v
Arrow Flight Endpoint
  |
  v
Feature Resolver
  |
  v
Cache Layer (optional SWR)
  |
  v
Execution Engine
  |
  +---- Redis
  +---- Feature store
  +---- Parquet / object storage
```

## Flight endpoint

Exposes the Arrow Flight RPC interface used by clients to request feature batches.

## Feature resolver

Maps logical feature requests to backend locations and determines how features should be assembled.

## Cache layer

Stores Arrow batches in memory and optionally supports stale‑while‑revalidate behavior.

## Execution engine

Coordinates parallel retrieval from backend adapters and merges results into a single Arrow batch.

---

# Feature Request Model

A typical request includes:

* a feature view
* one or more entities
* a subset of feature columns

Example:

```
feature_view: user_features
entities: ["user_123", "user_456"]
features: ["score", "country"]
```

The resolver determines which backends contain the requested features and executes the request accordingly.

---

# Observability

Quiver can observe feature traffic directly and produce useful metrics such as:

* feature null rates
* value distributions
* request latency
* backend latency

These metrics can help detect issues like feature drift or missing data that might otherwise go unnoticed until model performance degrades.

---

# Current Status

Quiver is currently in **v0.1 alpha**.

The project is intended for experimentation and early evaluation.

Implemented components include:

* Arrow Flight server
* basic feature resolver
* in‑memory adapter
* static feature registry
* gRPC health checks
* Python client

---

# Quick Start

## Prerequisites

* Rust
* Protobuf compiler
* grpcurl

## Build

```
 git clone <repository-url>
 cd quiver
 make
```

## Start the server

```
make run
```

## Verify health

```
grpcurl -plaintext localhost:8815 grpc.health.v1.Health/Check
```

---

# Configuration

Quiver uses a YAML configuration file combined with environment variables.

Configuration precedence:

1. environment variables
2. config file
3. defaults

Example configuration:

```
server:
  host: "127.0.0.1"
  port: 8815

registry:
  type: static
  views:
    - name: "user_features"
      entity_type: "user"
      entity_key: "user_id"
      columns:
        - name: "user_id"
          arrow_type: "string"
          nullable: false
          source: "memory"

        - name: "score"
          arrow_type: "float64"
          nullable: true
          source: "memory"

adapters:
  memory:
    type: memory
```

---

# Python Client

Quiver includes a Python client. [See Python Client Documentation](quiver-python/README.md)


Example usage:

```
import quiver

client = quiver.Client("localhost:8815")

features = client.get_features(
    feature_view="user_features",
    entities=["user_123", "user_456"],
    features=["score"]
)

print(features.to_pandas())

client.close()
```

---

# Project Structure

```
quiver/

quiver-core/
  Rust server implementation

quiver-python/
  Python client

proto/
  RPC definitions

configs/
  Example configurations
```

---

# Development

Common commands:

```
make build
make test
make run
```

---

# Roadmap

## v0.1 — Foundations

Core feature serving prototype.

Goals:

* Arrow Flight server
* basic resolver
* in‑memory adapter
* static registry

## v0.2 — Multi‑backend execution

Introduce real fan‑out capabilities.

Goals:

* Redis adapter
* Parquet / object storage adapter
* parallel execution engine
* result merging

## v0.3 — Caching and freshness

Introduce serving‑time performance features.

Goals:

* stale‑while‑revalidate cache
* configurable TTL policies
* cache metrics

## v0.4 — Observability

Expose metrics for feature traffic.

Goals:

* feature distribution metrics
* latency breakdowns
* OpenTelemetry support

## v0.5 — Registry integrations

Integrate with external feature registries.

Goals:

* Feast registry integration
* schema versioning

## v0.6 — Streaming adapters

Add real‑time feature sources.

Goals:

* Kafka adapter
* streaming feature ingestion

## v1.0 — Production readiness

Stabilize APIs and deployment model.

Goals:

* horizontal scaling
* multi‑tenant support
* performance benchmarks

---

# Contributing

Contributions and experiments are welcome.

Please open issues or pull requests if you would like to help shape the project.

---

# License

Apache 2.0
