# Quiver

**Quiver** is an experimental, Arrow‑native feature serving layer for machine learning inference.

It sits between model servers and feature backends, resolving feature requests, executing parallel retrieval across data sources, and returning columnar feature data via Arrow Flight.

---

# Overview and Motivation

ML systems typically invest heavily in feature computation and model training, but the real‑time path for feature delivery to models often evolves organically inside application code. 
For example, a typical ML inference pipeline ends up performing several expensive transformations:

1. Fetch feature rows from multiple backends
2. Deserialize row‑oriented data
3. Join results across sources
4. Convert rows into arrays
5. Convert arrays into tensors

This results in unnecessary latency, duplicated logic across services, and limited observability into what features models actually consume.

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
(Redis / key‑value / PostgreSQL / S3 / Parquet/ ClickHouse)
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

This already shows multiple inefficiencies.

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

### Request-level caching

Quiver includes optional caching with configurable TTL policies for repeated feature requests.

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

# Supported Adapters

- Memory (in-memory)
- Redis
- PostgreSQL
- Parquet / S3
- ClickHouse*

*Available with extended configuration

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

Quiver collects 11 instrumentation points during feature resolution and exposes them via:

* Response headers: `x-quiver-request-id`, `x-quiver-from-cache`
* Observability service: `localhost:8816`
* Python client: `client.get_metrics(request_id)`

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

## Configuration Examples

The [`examples/`](examples/) directory contains configuration examples for different deployment scenarios:


### Docker Development Environment

For local testing with real databases:

```bash
cd examples/docker
docker-compose up -d
QUIVER_CONFIG="examples/config/development.yaml" make run
```

## Basic Configuration Structure

```yaml
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

See the [examples directory](examples/) for complete configuration examples and detailed documentation.

---

# Python Client

Quiver includes a Python client with support for exporting to pandas, NumPy, PyTorch, and TensorFlow.

[See Python Client Documentation](quiver-python/README.md)

Example:

```python
import quiver

client = quiver.Client("localhost:8815")

features = client.get_features(
    feature_view="user_features",
    entities=["user_123", "user_456"],
    features=["score"]
)

print(features.to_pandas())
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

| Version | Milestone | Status |
|---------|-----------|--------|
| **v0.1** | Foundations | Complete |
| **v0.2** | Multi‑backend execution | Complete |
| **v0.3** | Caching and freshness | Complete |
| **v0.4** | Observability | Complete |
| **v0.5** | Request tracing | Complete |
| **v0.6** | Benchmarks | In progress |

