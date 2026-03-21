# Multi-Backend Fanout

This guide shows how to configure Quiver to serve features from multiple backends in a single request.

## Example: Redis + PostgreSQL

```yaml
server:
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill
    downtime_strategy: return_available

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: realtime_score
          arrow_type: float64
          nullable: true
          source: redis           # low-latency real-time feature

        - name: lifetime_value
          arrow_type: float64
          nullable: true
          source: postgres        # historical aggregate

        - name: country
          arrow_type: string
          nullable: true
          source: postgres

adapters:
  redis:
    type: redis
    connection: redis://localhost:6379
    source_path: "features:{feature}"

  postgres:
    type: postgres
    connection_string: postgresql://user:pass@localhost:5432/features
    source_path:
      table: user_features
    max_connections: 10
```

## What happens at request time

```
get_features("user_features", ["user:1000"], ["realtime_score", "lifetime_value", "country"])

  → partition:
      redis   ← ["realtime_score"]
      postgres ← ["lifetime_value", "country"]

  → dispatch (parallel):
      redis.get(...)    →  RecordBatch(realtime_score)
      postgres.get(...) →  RecordBatch(lifetime_value, country)

  → merge (Arrow LEFT JOIN on entity_id):
      RecordBatch(realtime_score, lifetime_value, country)
```

Total latency is bounded by `max(redis_latency, postgres_latency)`, not their sum.
