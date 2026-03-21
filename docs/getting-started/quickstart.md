# Quickstart

## Prerequisites

- Python 3.9+
- Docker and Docker Compose (for running real backends)
- `uv` or `pip` for Python package management

No Rust toolchain is required if you use the Python package directly.

## Overview

In this tutorial we will:

1. Start PostgreSQL and Redis using Docker Compose
2. Define a [feature view](../concepts/feature-views.md) mapping user features to a PostgreSQL backend
3. Ingest seed data into PostgreSQL
4. Start a Quiver server and connect it to the registry
5. Fetch features over Arrow Flight from the Python client
6. Add a second backend (Redis) and observe Quiver fan-out with no client changes
7. Retrieve per-request latency metrics from the observability endpoint

This tutorial uses a ride-sharing scenario: we have user entities with a `score` feature (a driver reliability score computed offline and stored in PostgreSQL) and a `last_active` feature (a real-time timestamp stored in Redis). Both need to be available at inference time. Without Quiver, the model server would query both backends directly, join the results, and reshape them into tensors. Quiver does all of that with a single `get_features()` call.

!!! note
    If you want to skip the Docker setup entirely, replace `source: postgres` with `source: memory` in the config and skip Steps 1 and 3. The memory adapter requires no external services and is suitable for local development.

---

## Step 1: Start backend services

Clone the repo and start the backend services:

```bash
git clone https://github.com/Olamyy/quiver
cd quiver/examples
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

```
NAME         STATUS    PORTS
postgres     running   0.0.0.0:5432->5432/tcp
redis        running   0.0.0.0:6379->6379/tcp
clickhouse   running   0.0.0.0:8123->8123/tcp
```

---

## Step 2: Define a feature view

A [feature view](../concepts/feature-views.md) is the core config primitive in Quiver. It names a group of features for an entity type, declares the Arrow schema for each column, and maps each column to a backend adapter.

Create `quiver.yaml` in the project root:

```yaml
server:
  host: "127.0.0.1"
  port: 8815

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: postgres
        - name: country
          arrow_type: utf8
          nullable: true
          source: postgres

adapters:
  postgres:
    type: postgres
    connection_string: postgresql://quiver:quiver@localhost:5432/quiver_test
    source_path:
      table: user_features
```

What this config does:

- `registry.views` declares one feature view, `user_features`, for the `user` entity type. The entity key (`user_id`) is the column Quiver uses to align results across backends.
- `columns` defines two features: `score` (a `float64`) and `country` (a string). Both are served from the `postgres` adapter.
- `adapters.postgres` configures the PostgreSQL connection. The `source_path.table` tells the adapter which table to query.

The `arrow_type` field maps directly to an [Apache Arrow](https://arrow.apache.org/docs/python/api/datatypes.html) data type. Quiver uses these types to build the output `RecordBatch` schema, so your model always receives data in the shape it expects.

---

## Step 3: Ingest test data

The `examples/ingest.py` script loads seed data into the configured backend. Run it with `uv`:

```bash
cd examples
uv run ingest.py postgres
```

```
Ingesting user_features into postgres...
Inserted 10 rows for entities: user:1000 ... user:1009
Done.
```

This inserts rows into the `user_features` table in PostgreSQL. Each row has a `user_id`, a `score` value, and a `country` code. These are the entities and feature values we will fetch in Step 6.

---

## Step 4: Start the Quiver server

```bash
quiver-server --config quiver.yaml
```

```
Quiver Flight Server starting on 127.0.0.1:8815
Registry: 1 feature view(s) loaded — user_features
Adapters: postgres
Observability: 127.0.0.1:8816
Ready.
```

The server is now listening on port `8815` for Arrow Flight requests, and on port `8816` for observability queries. The registry has loaded the `user_features` view from the config and knows that `score` and `country` should be fetched from the `postgres` adapter.

Leave this running and open a new terminal for the next steps.

---

## Step 5: Install the Python client

=== "uv"

    ```bash
    uv add quiver-python
    ```

=== "pip"

    ```bash
    pip install quiver-python
    ```

---

## Step 6: Fetch features

```python
from quiver import Client

client = Client("localhost:8815")

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001", "user:1002"],
    features=["score", "country"],
)

print(table.to_pandas())
```

```
     user_id  score country
0  user:1000   0.92      DE
1  user:1001   0.74      AT
2  user:1002   0.88      US
```

`get_features()` sends a single Arrow Flight RPC to the Quiver server. Quiver looks up the feature view, routes `score` and `country` to the PostgreSQL adapter, executes the query, and returns the results as a `RecordBatch` aligned to your entity list.

Note that the output rows are in the same order as the requested entity list. Quiver guarantees this regardless of how the backend returns results, or how many backends are involved. If an entity has no data in a backend, that row is null-filled rather than dropped.

The return value is a `FeatureTable` that wraps a `pyarrow.Table`. You can convert it to whatever format your model expects:

```python
table.to_pandas()   # pd.DataFrame
table.to_arrow()    # pyarrow.Table (zero-copy)
table.to_torch()    # torch.Tensor
table.to_numpy()    # np.ndarray
```

---

## Step 7: Add a second backend

This is where Quiver's value becomes concrete. Suppose `last_active` is a real-time feature that the application writes to Redis on every user action. It cannot live in PostgreSQL because it needs to be updated at write time without a batch job. But at inference time, the model needs it alongside the batch features from PostgreSQL.

Without Quiver, the model server would need to query both backends, handle partial failures from each, and join the results on `user_id`. With Quiver, this is a config change.

Update `quiver.yaml` to add the Redis adapter and a new column:

```yaml
registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: postgres
        - name: country
          arrow_type: utf8
          nullable: true
          source: postgres
        - name: last_active
          arrow_type: utf8
          nullable: true
          source: redis       # this column now comes from Redis

adapters:
  postgres:
    type: postgres
    connection_string: postgresql://quiver:quiver@localhost:5432/quiver_test
    source_path:
      table: user_features
  redis:
    type: redis
    connection: redis://localhost:6379
    source_path: "user:{entity}:last_active"
```

The `source_path` for Redis is a key template. `{entity}` is interpolated with each entity ID at request time, so `user:1000` maps to the Redis key `user:user:1000:last_active`.

Ingest some Redis data:

```bash
uv run ingest.py redis
```

Restart the server and run the same client code, adding `last_active` to the feature list:

```python
table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001", "user:1002"],
    features=["score", "country", "last_active"],
)

print(table.to_pandas())
```

```
     user_id  score country          last_active
0  user:1000   0.92      DE  2024-11-01 08:32:10
1  user:1001   0.74      AT  2024-11-01 09:14:55
2  user:1002   0.88      US                 None
```

`user:1002` has no `last_active` entry in Redis, so Quiver null-fills that cell. The row is still present and correctly ordered. The model server received one table from one call, despite the data coming from two different backends queried in parallel.

See [Fanout](../concepts/fanout.md) for a detailed explanation of how parallel dispatch and the Arrow LEFT JOIN merge work.

---

## Step 8: Fetch features at a point in time

Quiver supports `as_of` queries for [temporal feature retrieval](../concepts/temporal-queries.md). This is useful for generating training data with the same code path used at inference time, structurally preventing training/serving skew.

Pass an `as_of` timestamp to retrieve the feature values that would have been served at that moment:

```python
from datetime import datetime, timezone

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001"],
    features=["score", "country"],
    as_of=datetime(2024, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
)

print(table.to_pandas())
```

```
     user_id  score country
0  user:1000   0.81      DE
1  user:1001   0.69      AT
```

The scores differ from the current values because Quiver returned the feature values that existed as of October 1st. PostgreSQL and ClickHouse adapters support point-in-time queries natively. The Redis adapter returns current values only; for historical Redis data, route that feature to a backend that supports [time travel](../concepts/temporal-queries.md).

---

## Step 9: Inspect request metrics

Every response from Quiver includes an `x-quiver-request-id` header. Pass it to the observability endpoint on port `8816` to retrieve a breakdown of time spent in each stage of the serving path:

```python
request_id = client.last_request_id

metrics = client.get_metrics(request_id)

print(f"Total:      {metrics['total_ms']:.2f}ms")
print(f"Registry:   {metrics['registry_ms']:.2f}ms")
print(f"Dispatch:   {metrics['dispatch_ms']:.2f}ms")
print(f"  postgres: {metrics['adapter_ms']['postgres']:.2f}ms")
print(f"  redis:    {metrics['adapter_ms']['redis']:.2f}ms")
print(f"Merge:      {metrics['merge_ms']:.2f}ms")
print(f"Serialize:  {metrics['serialize_ms']:.2f}ms")
```

```
Total:      4.31ms
Registry:   0.12ms
Dispatch:   3.44ms
  postgres: 3.21ms
  redis:    1.05ms
Merge:      0.48ms
Serialize:  0.27ms
```

Dispatch time is dominated by PostgreSQL here. Redis completed in 1ms but the overall dispatch waited for the slowest adapter. Quiver dispatches both concurrently, so total dispatch time equals the slowest backend, not the sum.

---

## Next steps

- [Feature Views](../concepts/feature-views.md): model entities, columns, and backend routing in depth
- [Configuration](../concepts/configuration.md): full YAML schema for server, registry, adapters, and fanout
- [Temporal Queries](../concepts/temporal-queries.md): point-in-time retrieval, clock-skew handling, and training data generation
- [Fanout](../concepts/fanout.md): parallel dispatch, the 4-phase Arrow merge, and partial failure handling
- [Kubernetes sidecar](../guides/kubernetes-sidecar.md): deploy Quiver as a sidecar alongside your model server
