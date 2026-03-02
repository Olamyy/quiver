# Quiver

Quiver is an __experimental__ in-flight grpc server for serving features across different backends.

> Quiver holds Arrow-formatted feature data and serves it to model servers on demand. Think of it as **Triton for feature serving**.

---

## Why?

Feature stores are really good at registering, versioning and computing features. However, an area that has not received as much attention is the path from computed feature to model input. Today, the serving path for a lot of inference workloads looks something like this:

```mermaid
graph TD
    subgraph Offline_Processing ["Feature Computation (Batch/Stream)"]
        A["<b>Feature Computation</b><br/>(Spark / DuckDB / Polars)<br/><i>Internal: Arrow Columnar</i>"]
    end

    subgraph Serialization_Layer ["Data Transformation"]
        B["<b>Serialize</b><br/>(JSON / Protobuf / MessagePack)<br/><i>Row-Oriented Format</i>"]
    end

    subgraph Storage_Layer ["Online Feature Store"]
        C[("<b>Online Store</b><br/>(Redis GET/SET)")]
    end

    subgraph Serving_Layer ["Real-time Inference"]
        D["<b>Read & Deserialize</b><br/>(From Row-Oriented Format)"]
        E["<b>Re-serialize / Cast</b><br/>(NumPy Array / Torch Tensor)"]
        F{{"<b>Model Inference</b>"}}
    end

    %% Flow connections
    A -->|Convert to Rows| B
    B -->|Write| C
    C -->|Fetch| D
    D -->|Reshape| E
    E --> F

    %% Styling
    style A fill:#d4e1f5,stroke:#2b5797
    style C fill:#f8cecc,stroke:#b85450
    style F fill:#d5e8d4,stroke:#82b366
```

From the diagram above, several bottlenecks become apparent:
- **Redundant SerDe**: Serialization happens on every request, costing CPU cycles and increasing latency.
- **Impedance Mismatch**: Row-oriented formats (JSON/Protobuf) are fundamentally at odds with the columnar nature of modern data processing and ML.
- **Fan-Out logic in Model Servers**: Application code becomes bloated with infrastructure glue for querying multiple backends.

Quiver aims to optimize the inference path from computation to model input by:

- **Zero-Copy Memory Mapping**: For Arrow-native backends (Parquet, Delta Lake, Snowflake), data maintains its columnar memory layout from disk to model memory. No CPU-bound serialization ever occurs.
- **Transcoding Cache**: For legacy row-oriented backends (Redis/RonDB), Quiver keeps data as Arrow buffers in memory on the first cache miss. Subsequent "warm" hits are served as zero-copy Arrow buffers, bypassing the slow database path.
- **Kernel-Level Alignment**: By serving Arrow directly, Quiver aligns perfectly with the internal memory representation of NumPy, PyTorch, and JAX, enabling O(1) transfer to model tensors.
- **Observability without Instrumentation**: A monitoring interceptor observes the Arrow stream directly to track drift, null rates, and latency without modifying feature definitions.

---

## The Vision: Triton for Features

While NVIDIA Triton optimizes the compute-bound execution of models on GPUs, Quiver optimizes the I/O-bound orchestration of the data that feeds them.

In a traditional stack, model servers are burdened with the overhead of feature orchestration—querying disparate databases, parsing row-oriented data, and manually aligning entity keys into tensors. Quiver offloads this data preparation phase into a dedicated, Rust-based infrastructure layer.

---


## Architecture

```mermaid
graph TB
    subgraph Clients
        MS[Model Server]
        TP[Training Pipeline]
    end

    subgraph Quiver Server (Rust)
        FE[Flight Endpoint]
        FR[Feature Resolver]
        SWR[SWR Cache]
        FO[Fan-out Coordinator]
    end

    subgraph Backends
        REDIS[(Redis)]
        HOP[(Hopsworks / RonDB)]
        S3[(S3 / Parquet)]
    end

    MS -->|Arrow Flight| FE
    FE --> FR
    FR --> SWR
    SWR -->|Miss| FO
    FO --> REDIS
    FO --> HOP
    FO --> S3
```

## Current Status: v0.1 Alpha

Quiver is in early development. The core Arrow flight server is complete. The server is implemented in Rust for high performance and zero-copy serving.

### What's Possible Now
- **Arrow Flight Protocol**: Implements the standard `FlightService` for unified data transport.
- **Unified Resolver**: Decouples model servers from backend specifics.
- **Memory Adapter**: High-performance in-memory backend for testing and fast lookups.
- **Static Registry**: Centralized management of feature schemas and backend routing.
- **gRPC Health Check**: Native support for standard health probes (`grpc.health.v1.Health`).
- **gRPC Reflection**: Integrated service discovery for tools like `grpcurl`.
- **Demo Seeding**: Automatic population of test data (`my_feature_view`) for immediate verification.

---

## Getting Started

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install)
- [grpcurl](https://github.com/fullstorydev/grpcurl) (for manual testing)

### Run the Server
```bash
cd quiver-core
cargo run
```
The server starts on `0.0.0.0:8815` by default.

---

## Exploring the API

### List Services
You can discover all available services:
```bash
grpcurl -plaintext localhost:8815 list
```

### Fetch a Schema
Retrieve the Arrow schema for the pre-seeded `my_feature_view`:
```bash
grpcurl -plaintext -d '{"path": ["my_feature_view"]}' \
  localhost:8815 arrow.flight.protocol.FlightService/GetSchema
```

### Check Health
```bash
grpcurl -plaintext localhost:8815 grpc.health.v1.Health/Check
```

---

## Python Client

Quiver provides a Python client that wraps `pyarrow.flight` to handle Protobuf serialization and tensor conversion.

from quiver import QuiverClient

client = QuiverClient("grpc://localhost:8815")

# Fetch features for multiple entities in one call
features = client.get_features(
    view="transaction_features",
    entities=[{"user_id": "123"}, {"merchant_id": "456"}],
    max_staleness=60
)

# Zero-copy handover to PyTorch
X = features.to_tensor() 
model.predict(X)



## Roadmap

- v0.1 Alpha: Core Rust server, DuckDB/Redis adapters, basic hot cache. (Current)
- v0.2 Beta: Multi-backend fan-out, SWR cache manager, Feast registry integration.
- v0.3: Kafka Streams adapter, bidirectional do_exchange for high-frequency serving.
- v1.0: Production HA deployment, multi-tenancy, and performance benchmarks.
