# Fanout

**Fanout** is Quiver's mechanism for serving features from multiple backends in a single request. When a feature view spans Redis, PostgreSQL, and S3, Quiver dispatches to all three in parallel and merges the results before returning.

## How it works

1. **Partition.** Group feature columns by their `source` adapter.
2. **Dispatch.** Fire all backend requests concurrently (`join_all`).
3. **Merge.** Arrow LEFT JOIN on `entity_id`, null-filling missing rows.
4. **Validate.** Verify row count and schema match the request.

The merge uses Arrow's native join primitives, avoiding Python-level data movement.

## Configuration

```yaml
server:
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill   # null_fill | error | forward_fill
    downtime_strategy: return_available   # fail | return_available | use_fallback
```

### `partial_failure_strategy`

What to do when one backend returns partial results (some entities missing):

| Value | Behaviour |
|-------|-----------|
| `null_fill` | Fill missing rows with nulls (default) |
| `error` | Fail the entire request |
| `forward_fill` | Use the last known value |

### `downtime_strategy`

What to do when a backend is entirely unavailable:

| Value | Behaviour |
|-------|-----------|
| `fail` | Fail the entire request (default, safe) |
| `return_available` | Return features from healthy backends only |
| `use_fallback` | Route to `fallback_source` if primary fails |

## Fallback sources

Configure per-column fallbacks for graceful degradation:

```yaml
columns:
  - name: score
    arrow_type: float64
    source: redis
    fallback_source: postgres   # used when redis is down
```

Requires `downtime_strategy: use_fallback`.

## Ordering guarantees

Adapters declare their ordering capability:

- `OrderedByRequest`: rows guaranteed in entity_ids order (fast path, no re-sort)
- `Unordered`: Quiver re-sorts after fetch

Redis and Memory adapters are `OrderedByRequest`. PostgreSQL and ClickHouse are `Unordered`.
