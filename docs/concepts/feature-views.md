# Feature Views

A **feature view** is the primary unit of organization in Quiver. It defines a set of features for an entity type, where each feature comes from, and how it should be typed.

## Configuration

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
          source: redis

        - name: purchase_count
          arrow_type: int64
          nullable: true
          source: postgres

        - name: last_seen
          arrow_type: "timestamp[ns, UTC]"
          nullable: true
          source: postgres
```

## Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | ✓ | Unique identifier used in client requests |
| `entity_type` | ✓ | Logical entity type (e.g. `user`, `item`) |
| `entity_key` | ✓ | Column name used to match entity IDs in the backend |
| `columns` | ✓ | List of feature column definitions |

## Column fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | ✓ | Feature name exposed to clients |
| `arrow_type` | ✓ | Arrow type string (see below) |
| `nullable` | | Whether null values are allowed (default: `true`) |
| `source` | ✓ | Name of the adapter that serves this feature |
| `source_path` | | Override the default source path for this column |
| `fallback_source` | | Fallback adapter if primary fails (requires `downtime_strategy: use_fallback`) |

## Arrow types

Common type strings:

| Type string | Arrow type |
|-------------|-----------|
| `float32`, `float64` | Float |
| `int32`, `int64` | Integer |
| `string`, `utf8` | String |
| `bool` | Boolean |
| `timestamp[ns, UTC]` | Timestamp (UTC, nanoseconds) |
| `list<float32>` | List of floats (embeddings) |

!!! note
    Timestamp columns always have UTC enforced. The resolver rejects non-UTC timestamps.

## Multi-backend views

A single feature view can route different columns to different backends:

```yaml
- name: user_features
  entity_type: user
  entity_key: user_id
  columns:
    - name: realtime_score   # low-latency, from Redis
      arrow_type: float64
      source: redis
    - name: lifetime_value   # historical, from Postgres
      arrow_type: float64
      source: postgres
```

Quiver fans out to both backends in parallel and merges the results before returning.
