# Python Client

## Installation

```bash
pip install "quiver-python @ git+https://github.com/Olamyy/quiver.git#subdirectory=quiver-python"
```

## Basic usage

```python
from quiver import Client

client = Client("localhost:8815")

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001"],
    features=["score", "country"],
)

# Convert to your preferred format
df = table.to_pandas()
arr = table.to_numpy()
tensor = table.to_torch()
```

## Client options

```python
client = Client(
    address="localhost:8815",
    timeout=30.0,           # request timeout in seconds
    max_retries=3,          # retry attempts on transient failures
    compression="gzip",     # optional: "gzip" or "zstd"
    validate_connection=True,  # ping server on init
)
```

## Temporal queries

```python
from datetime import datetime, timezone

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000"],
    features=["score"],
    as_of=datetime(2024, 6, 1, tzinfo=timezone.utc),
)
```

## Observability

Each request exposes a request ID for latency introspection:

```python
table = client.get_features(...)

request_id = client.get_last_request_id()
from_cache = client.get_last_from_cache()

metrics = client.get_metrics(request_id)
print(f"Total: {metrics['total_ms']:.1f}ms")
print(f"Merge: {metrics['merge_ms']:.1f}ms")
print(f"Redis: {metrics['backend_redis_ms']:.1f}ms")
```

## Context manager

```python
with Client("localhost:8815") as client:
    table = client.get_features(...)
```

## API Reference

::: quiver.client.Client
