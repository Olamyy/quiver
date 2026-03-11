# Quiver

[![Rust Build](https://img.shields.io/github/actions/workflow/status/Olamyy/quiver/rust-ci.yml?branch=main&label=build)](https://github.com/Olamyy/quiver/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://github.com/Olamyy/quiver/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/Olamyy/quiver?style=flat&sort=semver&color=blue)](https://github.com/Olamyy/quiver/releases)


**Quiver** is an experimental, Arrow‑native feature serving layer for machine learning inference. 
It sits between model servers and feature backends, to resolve feature retrieval requests across multiple feature engines, and returns columnar feature data via Arrow Flight.

---

# Overview and Motivation

In many production ML inference systems the serving path resembles the following pipeline.

```
Feature Computation (Spark / Polars / DuckDB)
        |
        v
Serialize rows (JSON / Protobuf)
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

This approach creates a lot of inefficiencies:

- Repeated serialization: Features are often converted from columnar formats used during computation into row formats for storage, then converted back into arrays for inference. Each request incurs repeated serialization and deserialization overhead.
- Row‑oriented data in a columnar workload: Machine learning frameworks operate on vectors and tensors, yet many feature serving systems return row‑oriented data structures. This mismatch forces additional reshaping and copying inside model servers.
- Fan‑out logic inside model services: When features reside in multiple systems (for example Redis, Parquet, or a feature store), model servers must coordinate several backend requests. This logic is often duplicated across services and grows increasingly complex as feature sets expand.
- Monitoring systems frequently track feature pipelines or model outputs, but rarely capture statistics about **the actual features sent to models at serving time**.

---

# What Quiver Does

Quiver provides a unified layer that resolves feature requests and returns columnar feature batches optimized for inference workloads. It has the following capabilities:

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

Quiver returns feature data as Arrow `RecordBatch` objects via Arrow Flight. Columnar batches enable efficient vectorized processing and straightforward conversion into tensors.

### Request-level caching

Quiver includes optional caching with configurable TTL policies for repeated feature requests.

### Serving‑time observability

Because Quiver sits directly on the serving path, it can observe feature distributions and latency without requiring instrumentation inside model services.

---

# Design Goals

Quiver is built around several principles.

## Arrow‑native data path

Whenever possible, Quiver keeps features in Arrow columnar buffers from storage through inference. This avoids repeated row‑to‑column transformations and allows efficient batch processing.

## Decoupled feature resolution

Model servers should not need to know where features are stored or how they are retrieved. All backend routing logic lives inside Quiver.

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
| **ClickHouse** | Experimental | Analytical queries | OLAP workloads |

---

## Quick Start

Choose your preferred installation method:

### Option 1: Docker (Recommended for Quick Testing)

Get Quiver running in seconds with a single command:

```bash
# Pull and run the latest Docker image
docker pull ghcr.io/olamyy/quiver-server:latest
docker run -p 8815:8815 -p 8816:8816 \
  ghcr.io/olamyy/quiver-server:latest \
  --config /etc/quiver/config.yaml
```

Then mount your config file:

```bash
docker run -p 8815:8815 -p 8816:8816 \
  -v $(pwd)/your-config.yaml:/etc/quiver/config.yaml \
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

### Option 3: Build from Source (For Contributors)

```bash
# Clone and install dependencies
git clone https://github.com/Olamyy/quiver.git
cd quiver
make install

# Build and run
make build-release
./quiver-core/target/release/quiver-core --config your-config.yaml
```

---

## Complete End-to-End Example

Want to try Quiver with real backends? Follow this full walkthrough:

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

In a new terminal, start the Quiver server:

```bash
make run CONFIG=examples/config/postgres/basic.yaml
```

The server will start on **port 8815** (Arrow Flight) and **8816** (Observability metrics).

### 4. Query Features via Python Client

Quiver includes a Python client with support for exporting to pandas, NumPy, PyTorch, and TensorFlow.

```python
import quiver
from pprint import pprint

# Connect to Quiver
client = quiver.Client("localhost:8815")

# Request features for multiple entities
features = client.get_features(
    feature_view="user_features",
    entities=["user_1000", "user_1001", "user_1002"],
    features=["score", "country"]
)

# Export to pandas, numpy, or ML frameworks
df = features.to_pandas()
print(df)

# Get metrics for the request
metrics = client.get_metrics(request_id="...")
pprint(metrics)
```

**Example Output:**

```
     entity         score  country
0  user_1000  0.892345      USA
1  user_1001  0.654321      Canada
2  user_1002  0.445678      Mexico
```

---

## Next Steps

For more detailed installation, configuration, and advanced usage, see the [**Installation Guide**](INSTALL.md).

- Different backends (Redis, PostgreSQL, S3, ClickHouse)
- Custom feature configurations
- Performance tuning
- Observability and metrics
- Production deployment strategies
