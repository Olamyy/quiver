# Quiver

[![Rust Build](https://img.shields.io/github/actions/workflow/status/Olamyy/quiver/rust-ci.yml?branch=main&label=build)](https://github.com/Olamyy/quiver/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://github.com/Olamyy/quiver/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/Olamyy/quiver?style=flat&sort=semver&color=blue)](https://github.com/Olamyy/quiver/releases)


**Quiver** is an Arrow-native feature serving layer for machine learning inference.
It resolves feature requests across multiple backends and returns columnar data via Arrow Flight for direct consumption by model inference pipelines.

---

# The Problem

Traditional ML feature serving has several inefficiencies:

- **Repeated serialization**: Features move from columnar (computation) → row (storage) → columnar (inference), incurring overhead at each step.
- **Row-oriented APIs**: Most serving systems return rows despite ML frameworks expecting columnar tensors, forcing additional reshaping inside model servers.
- **Distributed fan-out**: When features span multiple backends (Redis, PostgreSQL, S3), model servers must orchestrate parallel requests, duplicate routing logic, and handle partial failures.
- **Blind serving**: Most systems don't capture metrics about actual features sent to models at serving time.

---

# What Quiver Solves

- **Feature resolution**: Map logical feature requests to physical backend locations without coupling model servers to storage details.
- **Parallel execution**: Fetch from multiple backends concurrently, not sequentially.
- **Columnar transport**: Return Arrow RecordBatch objects for direct tensor conversion.
- **Request caching**: Optional TTL-based caching for repeated feature requests.
- **Serving-time observability**: Capture feature latency and distributions without instrumenting model servers.

---

# Architecture

The Quiver server consists of several components.

```
Clients (Model Servers)
  ↓
Arrow Flight Endpoint (gRPC)
  ↓
Feature Resolver
  ├─ Registry lookup
  ├─ Feature → Backend routing
  ├─ Schema validation
  └─ Type enforcement
  ↓
Cache Layer (Request-level)
  ├─ TTL-based expiration
  ├─ Request deduplication
  └─ Observability metrics storage
  ↓
Execution Engine (Parallel Dispatch)
  ├─ Redis adapter
  ├─ PostgreSQL adapter
  ├─ S3/Parquet adapter
  ├─ ClickHouse adapter
  └─ Memory adapter
  ↓
Result Assembly & Arrow Flight Response
```

## Flight endpoint

Exposes the Arrow Flight RPC interface used by clients to request feature batches.

## Feature resolver

Maps logical feature requests to backend locations and determines how features should be assembled.

## Cache layer

Stores Arrow batches in memory and optionally supports request caching.

## Execution engine

Coordinates parallel retrieval from backend adapters and merges results into a single Arrow batch.

---

## Supported Adapters

| Adapter | Status | Use Case | Features |
|---------|--------|----------|----------|
| **Memory** | Production | Testing & debugging | Fast, in-process |
| **Redis** | Production | Real-time features | HSET-based, sub-ms latency |
| **PostgreSQL** | Production | Historical data | Complex queries, temporal support |
| **S3/Parquet** | Production | Data lake features | Columnar format, large datasets |
| **ClickHouse** | Production | Analytical queries | OLAP workloads |

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
docker run -p 8815:8815 -p 8816:8816 \
  -v $(pwd)/config.yaml:/etc/quiver/config.yaml \
  ghcr.io/olamyy/quiver-server:latest \
  --config /etc/quiver/config.yaml
```

### Option 2: Binary Download (Fast, No Build Required)

```bash
# Download the latest release for your platform
curl -L https://github.com/Olamyy/quiver/releases/download/v0.0.1/quiver-server-v0.0.1-aarch64-apple-darwin.tar.gz | tar xz

# Run the server
./quiver-core --config your-config.yaml
```

See [installation guide](INSTALL.md) for all platform options and checksums.

### Option 3: Build from Source

```bash
git clone https://github.com/Olamyy/quiver.git
cd quiver
make install
make build-release
./quiver-core/target/release/quiver-core --config config.yaml
```

---

## Try It Out

### 1. Start Backend Services

```bash
cd examples
docker-compose up -d

# Verify all services are running
docker-compose ps
```

### 2. Ingest Test Data

```bash
cd examples
uv run ingest.py postgres    # Load PostgreSQL features
# or
uv run ingest.py redis       # Load Redis features
```

### 3. Start Quiver Server

```bash
make run CONFIG=examples/config/postgres/basic.yaml
```

Server runs on port 8815 (Arrow Flight) and 8816 (observability).

### 4. Query Features

```python
import quiver

client = quiver.Client("localhost:8815")

features = client.get_features(
    feature_view="user_features",
    entities=["user_1000", "user_1001", "user_1002"],
    features=["score", "country"]
)

df = features.to_pandas()  # or .to_numpy(), .to_torch(), .to_tf()
print(df)
```

---

## Documentation

Full documentation is at **[olamyy.github.io/quiver](https://olamyy.github.io/quiver)**.

| Section | |
|---|---|
| [Quickstart](https://olamyy.github.io/quiver/getting-started/quickstart/) | Step-by-step tutorial with PostgreSQL and Redis |
| [Architecture](https://olamyy.github.io/quiver/concepts/architecture/) | How the serving path works end to end |
| [Configuration](https://olamyy.github.io/quiver/concepts/configuration/) | Full YAML reference with auth and adapter options |
| [Python Client](https://olamyy.github.io/quiver/reference/python-client/) | SDK reference for `Client` and `FeatureTable` |
