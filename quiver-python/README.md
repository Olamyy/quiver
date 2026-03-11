# Quiver Python Client

High‑performance Python client for the Quiver feature serving system. The client provides a convenient interface for retrieving features from a Quiver server and converting them into formats commonly used in machine learning pipelines.

Internally, feature data is transported as Apache Arrow RecordBatches and can be exported into multiple ML frameworks with minimal overhead.

---

# Overview

The Quiver Python client connects to a running Quiver server and allows applications to retrieve feature data using a simple API.

Typical usage pattern:

1. Connect to a Quiver server
2. Request features for one or more entities
3. Convert the returned Arrow batch into a format suitable for model inference

The client is designed for inference workloads where low latency, batching, and reliability are important.

---

# Key Features

## Arrow‑native feature retrieval

Feature data is returned from the server as Arrow RecordBatches, enabling efficient columnar processing and fast conversion to other data formats.

## Batch requests

Multiple feature requests can be executed in parallel through a single client call, reducing network round‑trips and improving throughput.

## ML framework integrations

Returned feature batches can be converted into formats compatible with common ML frameworks.

Supported exports include:

* pandas DataFrame
* NumPy arrays
* PyTorch tensors
* TensorFlow tensors
* Polars DataFrame

## Automatic retries

The client automatically retries transient failures and manages connection lifecycle.

## Point‑in‑time queries

When supported by the backend, features can be requested as they existed at a specific timestamp.

---

# Prerequisites

* Python 3.10 or newer
* A running Quiver server

Refer to the main Quiver repository for server installation and configuration instructions.

---

# Installation

## Core Installation

```
uv add quiver-client
```

This installs the core dependencies required for feature retrieval.

---

## Optional ML Framework Support

Install optional integrations depending on your workflow.

```
uv add quiver-client[pandas]
uv add quiver-client[numpy]
uv add quiver-client[torch]
uv add quiver-client[tensorflow]
uv add quiver-client[polars]
```

Multiple integrations can be installed together.

```
uv add quiver-client[pandas,torch]
```

---

# Core Concepts

## FeatureBatch

The `get_features` API returns a `FeatureBatch` object.

A `FeatureBatch` is a lightweight wrapper around an Arrow RecordBatch that provides convenient export methods for common ML frameworks.

Example conversions:

```
features.to_pandas()
features.to_numpy()
features.to_torch()
features.to_tensorflow()
features.to_polars()
features.to_arrow()
```

All conversions originate from the same underlying Arrow batch.

---

# Basic Usage

```
import quiver

client = quiver.Client("localhost:8815")

features = client.get_features(
    feature_view="user_features",
    entities=["user_123", "user_456"],
    features=["age", "tier", "total_orders"]
)

print(features.to_pandas())

client.close()
```

---

# Batch Requests

Batch requests allow multiple feature views to be retrieved in parallel with a single network round‑trip.

```
import quiver

requests = [
    quiver.FeatureRequest(
        feature_view="user_features",
        entities=["user_123", "user_456"],
        features=["age", "tier"]
    ),
    quiver.FeatureRequest(
        feature_view="item_features",
        entities=["item_789"],
        features=["category", "price"]
    )
]

results = client.get_features_batch(requests)

for result in results:
    print(len(result))
```

---

# Exporting Feature Data

Returned features can be exported into several formats depending on the ML stack used.

```
features = client.get_features(
    feature_view="user_features",
    entities=["user_123"],
    features=["age", "tier", "total_orders"]
)

# pandas
features.to_pandas()

# NumPy
features.to_numpy(columns=["age", "total_orders"])

# PyTorch
features.to_torch(columns=["age", "total_orders"])

# TensorFlow
features.to_tensorflow(columns=["age", "total_orders"])

# Polars
features.to_polars()

# Arrow
features.to_arrow()
```

---

# Point‑in‑Time Queries

Some backends support retrieving features as they existed at a specific timestamp.

Example:

```
from datetime import datetime

features = client.get_features(
    feature_view="user_features",
    entities=["user_123"],
    features=["age", "tier"],
    as_of=datetime(2024, 1, 15, 10, 30, 0)
)
```

Support for point‑in‑time queries depends on the capabilities of the underlying backend.

---

# Context Manager Usage

The client supports context manager syntax for automatic cleanup.

```
import quiver

with quiver.Client("localhost:8815") as client:
    features = client.get_features(
        feature_view="user_features",
        entities=["user_123"],
        features=["age", "tier"]
    )

    df = features.to_pandas()
```

---

# Client Configuration

```
client = quiver.Client(
    address="localhost:8815",
    timeout=30.0,
    max_retries=3,
    compression="gzip",
    default_null_strategy="fill_null",
    validate_connection=True
)
```

Configuration options:

* `timeout` — request timeout in seconds
* `max_retries` — retry attempts for transient errors
* `compression` — response compression (gzip or zstd)
* `default_null_strategy` — how missing feature values are handled
* `validate_connection` — test server connectivity on startup

---

# Discovering Feature Views

The client can list feature views registered in the server.

```
views = client.list_feature_views()

for view in views:
    print(view.name)
    print(view.schema)
```

---

# Inference Pipeline Example

```
import torch
import torch.nn as nn
import quiver

class ModelService:

    def __init__(self, client):
        self.client = client
        self.model = self.load_model()

    def load_model(self):
        return nn.Linear(3, 1)

    def predict(self, entity_ids):

        features = self.client.get_features(
            feature_view="user_features",
            entities=entity_ids,
            features=["age", "tier_encoded", "total_orders"]
        )

        tensor = features.to_torch(
            columns=["age", "tier_encoded", "total_orders"]
        )

        with torch.no_grad():
            predictions = self.model(tensor)

        return predictions

with quiver.Client("localhost:8815") as client:
    service = ModelService(client)
    preds = service.predict(["user_123", "user_456"])
```

---

# Error Handling

Common exceptions raised by the client:

* `QuiverConnectionError`
* `QuiverFeatureViewNotFound`
* `QuiverFeatureNotFound`
* `QuiverTimeoutError`
* `QuiverValidationError`
* `QuiverServerError`

Example:

```
import quiver

try:

    client = quiver.Client("localhost:8815")

    features = client.get_features(
        "user_features",
        ["user_123"],
        ["age"]
    )

except quiver.QuiverConnectionError:
    print("Cannot connect to server")

except quiver.QuiverFeatureViewNotFound:
    print("Unknown feature view")

except quiver.QuiverTimeoutError:
    print("Request timed out")

finally:
    client.close()
```

---

# Performance Tips

## Reuse client instances

Create one client per service rather than reconnecting on every request.

```
client = quiver.Client("localhost:8815")
```

## Prefer batch requests

Fetching many entities in a single request is significantly more efficient than issuing many small requests.

## Tune request timeouts

Large batch requests may require longer timeouts depending on backend latency.

---

# Troubleshooting

Test server connectivity:

```
client = quiver.Client("localhost:8815", validate_connection=True)
```

List available feature views:

```
views = client.list_feature_views()

for view in views:
    print(view.name, view.schema)
```

---

