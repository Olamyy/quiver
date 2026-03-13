# Comparative Benchmark Suite: Quiver vs Application Fan-Out

Compare Quiver's centralized feature serving against typical application-level fan-out patterns used by ML services today.

## Overview

This suite measures the performance difference between:

1. **Direct Backend Access** (`baseline_app.py`) - What ML services typically do: parallel calls to Redis, PostgreSQL, S3, then merge in application code
2. **Quiver Server** (`baseline_quiver.py`) - Centralized feature serving via gRPC Arrow Flight with server-side merge

---

## Quick Start (5 minutes)

### 1. Start Backend Services
```bash
cd ../examples
docker-compose up -d
```

### 2. Ingest Test Data
```bash
cd ../examples
uv run ingest.py postgres --count 100
```

### 3. Start Quiver Server (in separate terminal)
```bash
make run CONFIG=examples/config/multi-adapter/redis-postgres-s3.yaml
```

### 4. Run Benchmarks (in benchmarks directory)
```bash
cd benchmarks
uv sync

# Run direct backend access benchmark
uv run baseline_app.py --scenario redis-postgres-s3 --entity-count 100 --duration-secs 10

# Run Quiver benchmark (in another terminal)
uv run baseline_quiver.py --scenario redis-postgres-s3 --entity-count 100 --duration-secs 10
```

---

## Benchmarks

### baseline_app.py
Simulates typical ML service feature fetching with **application-level fan-out**:
- Creates connection pools to each backend
- For each request:
  - Issues **parallel** requests to all backends
  - Waits for all responses
  - Merges results in application code
- Measures end-to-end latency

**Usage:**
```bash
uv run baseline_app.py \
  --scenario redis-postgres-s3 \
  --entity-count 100 \
  --duration-secs 10
```

**Options:**
- `--scenario` - Benchmark scenario: `redis-postgres` or `redis-postgres-s3` (default: redis-postgres)
- `--entity-count` - Number of entities per request (default: 100)
- `--duration-secs` - Benchmark duration (default: 10)

### baseline_quiver.py
Uses **Quiver Python client** for centralized feature serving:
- Connects to Quiver server (already running)
- For each request:
  - Issues **single** FeatureRequest to Quiver
  - Receives Arrow-encoded response
  - Deserializes to dict/pandas
- Measures end-to-end latency

**Usage:**
```bash
uv run baseline_quiver.py \
  --scenario redis-postgres-s3 \
  --entity-count 100 \
  --duration-secs 10
```

**Options:**
- `--scenario` - Benchmark scenario: `redis-postgres` or `redis-postgres-s3` (default: redis-postgres)
- `--entity-count` - Number of entities per request (default: 100)
- `--duration-secs` - Benchmark duration (default: 10)
- `--server` - Quiver server address (default: localhost:8815)

---

## Scenarios

### Scenario: redis-postgres (2x Backend)
- **Backends:** Redis + PostgreSQL
- **Features:**
  - ecommerce_view: 4 features (from both Redis and PostgreSQL)
  - timeseries_view: 3 features (from both Redis and PostgreSQL)
- **Entity Count:** Configurable (default: 100)

### Scenario: redis-postgres-s3 (3x Backend)
- **Backends:** Redis + PostgreSQL + S3/Parquet
- **Features:**
  - ecommerce_view: 4 features (from Redis + PostgreSQL)
  - timeseries_view: 3 features (from PostgreSQL + S3)
- **Entity Count:** Configurable (default: 100)

---

## Output

Benchmarks generate CSV and JSON results in `results/` directory:

**Files created:**
- `baseline_app_<scenario>_<timestamp>.json` - Direct backend metrics
- `baseline_app_<scenario>.csv` - Direct backend sample data
- `baseline_quiver_<scenario>_<timestamp>.json` - Quiver metrics
- `baseline_quiver_<scenario>.csv` - Quiver sample data

**Metrics collected:**
- P50, P95, P99 latency (milliseconds)
- Throughput (requests/second)
- CPU peak (%)
- Memory peak (MB)
- Request count

**Sample output:**
```
Benchmark Results:
  P50 Latency:   150.32 ms
  P95 Latency:   175.48 ms
  P99 Latency:   198.76 ms
  Throughput:    6 req/s
  CPU Peak:      42.3%
  Memory Peak:   256 MB
  Requests:      87
JSON saved to: results/baseline_app_redis-postgres-s3_1710338847.json
CSV saved to: results/baseline_app_redis-postgres-s3.csv (100 entities)
```

---

## Configuration

All benchmarks connect to local backends:
- **Redis:** `localhost:6379`
- **PostgreSQL:** `localhost:5432` (user: `quiver`, password: `quiver_test`, db: `quiver_test`)
- **S3/Parquet:** Mocked S3 storage (created during `ingest.py`)
- **Quiver Server:** `localhost:8815`

---

## Data Ingestion

Use the ingest script to populate test data into backends.

**Full ingest (all backends):**
```bash
cd ../examples
uv run ingest.py postgres --count 100
```

This ingest script:
- Creates 100 entities per scenario
- Populates Redis with hashes
- Populates PostgreSQL with tables
- Generates S3/Parquet files

**Options:**
- `--count N` - Number of entities (default: 100)

---

## Running Multiple Benchmarks

### Compare different entity counts:
```bash
for count in 10 100 1000; do
  echo "=== Entity Count: $count ==="
  uv run baseline_app.py --entity-count $count --duration-secs 5
  uv run baseline_quiver.py --entity-count $count --duration-secs 5
done
```

### Compare different scenarios:
```bash
# 2x backend
uv run baseline_app.py --scenario redis-postgres --duration-secs 10
uv run baseline_quiver.py --scenario redis-postgres --duration-secs 10

# 3x backend
uv run baseline_app.py --scenario redis-postgres-s3 --duration-secs 10
uv run baseline_quiver.py --scenario redis-postgres-s3 --duration-secs 10
```

### Multiple runs for averaging:
```bash
for i in {1..3}; do
  echo "=== Run $i ==="
  uv run baseline_app.py --duration-secs 20
  uv run baseline_quiver.py --duration-secs 20
done
```

---

## Troubleshooting

### "Connection refused" errors

**Check backend status:**
```bash
cd ../examples
docker-compose ps
docker-compose logs redis
docker-compose logs postgres
```

**Restart backends:**
```bash
cd ../examples
docker-compose down -v
docker-compose up -d
```

### Server connection issues

**Check Quiver server is running:**
```bash
# Should see startup logs
make run CONFIG=examples/config/multi-adapter/redis-postgres-s3.yaml
```

**Test server connectivity:**
```bash
python3 << 'EOF'
from quiver import Client
client = Client("localhost:8815")
print(client.get_server_info())
EOF
```

### Latency seems high

1. **Check backend health:**
   ```bash
   docker-compose exec redis redis-cli ping
   docker-compose exec postgres psql -U quiver -d quiver_test -c "SELECT 1"
   ```

2. **Check system load:**
   ```bash
   top
   ```

3. **Test with fewer entities:**
   ```bash
   uv run baseline_app.py --entity-count 10 --duration-secs 5
   ```

### Results seem inconsistent

- Run for longer duration: `--duration-secs 30`
- Run on a quieter machine
- Check CSV results for sample row variance

---

## Performance Tuning

### Entity Count Impact
Test how latency scales with entity count:
```bash
for count in 1 10 100 1000; do
  echo "Entity Count: $count"
  uv run baseline_app.py --entity-count $count --duration-secs 5 | grep "P50"
done
```

### Scenario Impact
Compare fanout efficiency across backends:
```bash
# Single backend performance (baseline)
make run CONFIG=examples/config/redis/basic.yaml &
uv run baseline_quiver.py --scenario redis-postgres --entity-count 100

# Two backends (verify fanout efficiency)
make run CONFIG=examples/config/multi-adapter/redis-postgres.yaml &
uv run baseline_quiver.py --scenario redis-postgres --entity-count 100

# Three backends (see if fanout degrades with complexity)
make run CONFIG=examples/config/multi-adapter/redis-postgres-s3.yaml &
uv run baseline_quiver.py --scenario redis-postgres-s3 --entity-count 100
```

---

## References

- Quiver RFC v0.3: Multi-backend fanout architecture
- Arrow Flight Protocol: https://arrow.apache.org/docs/format/Flight.html
- Quiver Python Client: ../quiver-python/README.md
