# Temporal Queries

Quiver supports **point-in-time** feature retrieval via the `as_of` parameter. This lets you ask "what were the feature values for these entities at time T?", which is essential for training data generation and backtesting.

## Usage

```python
from datetime import datetime, timezone

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001"],
    features=["score", "country"],
    as_of=datetime(2024, 6, 1, tzinfo=timezone.utc),
)
```

When `as_of` is `None`, Quiver returns current values (point-in-time at request time).

## How adapters handle it

Each adapter implements temporal semantics differently:

| Adapter | Temporal behaviour |
|---------|-------------------|
| **PostgreSQL** | `WHERE valid_from <= as_of ORDER BY valid_from DESC LIMIT 1` |
| **Redis** | No temporal support. Always returns current value. |
| **S3/Parquet** | Filters on a timestamp partition column |
| **Memory** | No temporal support |
| **ClickHouse** | Configurable via query template |

## Clock-skew aware routing

When a fanout request spans multiple backends and `as_of` is near "now", Quiver applies a **30-second clock-skew margin**. If `as_of >= now - 30s`, requests are routed to real-time backends (Redis) instead of historical ones. This prevents serving stale data when the client clock is slightly ahead of the server.

## UTC requirement

All timestamps **must** be UTC. The resolver enforces this. `Timestamp` columns without a UTC timezone annotation are rejected at config validation time.

```yaml
# Correct
- name: last_seen
  arrow_type: "timestamp[ns, UTC]"

# Will be rejected at startup
- name: last_seen
  arrow_type: "timestamp[ns]"
```
