# Quiver Examples

This directory contains example configurations, data, and scripts to quickly get started with Quiver.

## Quick Start (4 Steps)

### 1. Start Services
```bash
cd examples
docker-compose up -d
```

Starts PostgreSQL, Redis, and ClickHouse. Use `docker-compose down` to stop.

**Check services are healthy:**
```bash
docker-compose ps
```

### 2. Choose Your Adapter

Pick one of:

**PostgreSQL:**
```bash
uv run ingest.py --engine postgres --seed
make run CONFIG=examples/config/postgres/basic.yaml
```

**Redis:**
```bash
uv run ingest.py --engine redis --seed
make run CONFIG=examples/config/redis/basic.yaml
```

**ClickHouse:**
```bash
uv run ingest.py --engine clickhouse --seed
make run CONFIG=examples/config/clickhouse/basic.yaml
```

**S3/Parquet (with AWS credentials):**
```bash
# Pass AWS credentials to docker-compose
AWS_ACCESS_KEY_ID=your_key \
AWS_SECRET_ACCESS_KEY=your_secret \
AWS_REGION=us-west-2 \
docker-compose up -d

# Then serve with S3 config (credentials are available in environment)
make run CONFIG=examples/config/s3/basic.yaml
```

### 3. Query Features (in another terminal)
```bash
uv run read.py --adapter postgres
uv run read.py --adapter redis
uv run read.py --adapter clickhouse
```

### 4. Customize
Modify the corresponding YAML config file in `examples/config/{adapter}/` for your use case.

## Directory Structure

```
examples/
├── docker-compose.yaml   # Services: PostgreSQL, Redis, ClickHouse
│                        # (Pass AWS creds as env vars for S3)
├── config/              # Quiver server configurations (13 files)
│   ├── postgres/
│   │   ├── basic.yaml    # Simple PostgreSQL setup
│   │   ├── temporal.yaml # Same as basic (for consistency)
│   │   └── advanced.yaml # Production with TLS + validation
│   ├── redis/
│   │   ├── basic.yaml    # Redis session store
│   │   └── advanced.yaml # Production with high concurrency
│   ├── clickhouse/
│   │   ├── basic.yaml    # Simple ClickHouse setup
│   │   ├── temporal.yaml # Time-series sensor data
│   │   ├── single-table.yaml
│   │   └── advanced.yaml # Production with multiple views
│   ├── s3/
│   │   ├── basic.yaml         # AWS S3 (env var credentials)
│   │   ├── with-credentials.yaml # AWS S3 (hardcoded credentials)
│   │   └── local.yaml         # Local filesystem testing
│   ├── memory/
│   │   └── dev.yaml      # In-memory (no external deps)
│   └── README.md         # Configuration guide with setup examples
├── data/
│   └── fixtures.json     # Test data (3 scenarios: ecommerce, timeseries, sessions)
├── ingest.py             # Load test data: postgres, redis, clickhouse
└── read.py               # Query features via Quiver client
```

## Scripts

### ingest.py
Load test data into your chosen adapter.

**Options:**
- `--engine {postgres,redis,clickhouse}` - Which adapter to load into (required)
- `--seed` - Load seed data from fixtures.json
- `--generate N` - Generate N random records per scenario
- `--scenario {ecommerce,timeseries,sessions}` - Load specific scenario (default: all)

**Examples:**
```bash
# Load seed data only
uv run ingest.py --engine postgres --seed

# Generate 10,000 synthetic records
uv run ingest.py --engine clickhouse --generate 10000

# Mix seed + synthetic (1000 generated per scenario)
uv run ingest.py --engine postgres --seed --generate 1000

# Time-series data only
uv run ingest.py --engine clickhouse --scenario timeseries --generate 500
```

### read.py
Query features from a running Quiver server.

**Options:**
- `--adapter {postgres,redis,clickhouse}` - Which adapter to demo (required)
- `--server localhost:8815` - Quiver server address
- `--entity-type user` - Entity type to query
- `--feature-name spend_30d` - Specific feature (optional)

**Examples:**
```bash
# Demo PostgreSQL features
uv run read.py --adapter postgres

# Demo with custom server
uv run read.py --adapter clickhouse --server myserver:9000

# Demo Redis session features
uv run read.py --adapter redis
```

## Data Scenarios

Three test scenarios with different schemas:

### ecommerce
E-commerce user purchase history.

**Entities:** `user:101`, `user:102`, `user:103`, ...
**Features:**
- `spend_30d` (float64) - Total spend in last 30 days
- `purchase_count` (int64) - Number of purchases
- `last_purchase_date` (timestamp) - Most recent purchase
- `is_premium` (bool) - Premium subscription status

### timeseries
Sensor readings with timestamps.

**Entities:** `sensor:001`, `sensor:002`, ...
**Features:**
- `temperature` (float64) - Celsius
- `humidity` (float64) - Percentage
- `alert_triggered` (bool) - Alert status

### sessions
Real-time session state (Redis only, via key-value).

**Entities:** `session:s001`, `session:s002`, ...
**Features:**
- `active_user_id` (string) - Associated user
- `request_count` (int64) - Requests in session
- `last_activity` (timestamp) - Last activity time

## Common Tasks

### Run with PostgreSQL
```bash
uv run ingest.py --engine postgres --seed
make run CONFIG=examples/config/postgres/basic.yaml
# In another terminal:
uv run read.py --adapter postgres
```

### Run with large dataset
```bash
uv run ingest.py --engine clickhouse --generate 100000
make run CONFIG=examples/config/clickhouse/basic.yaml
```

### Time-series queries
```bash
uv run ingest.py --engine clickhouse --scenario timeseries --generate 5000
make run CONFIG=examples/config/clickhouse/temporal.yaml
uv run read.py --adapter clickhouse
```

### Check what's in your database
```bash
# PostgreSQL
docker-compose exec postgres psql -U quiver -d quiver_test -c "SELECT COUNT(*) FROM ecommerce_data;"

# Redis
docker-compose exec redis redis-cli KEYS 'sessions:*' | wc -l

# ClickHouse
docker-compose exec clickhouse clickhouse-client -q "SELECT COUNT(*) FROM quiver_test.ecommerce_data;"
```

### Reset databases
```bash
# Stop services, remove volumes, start again
docker-compose down -v
docker-compose up -d
```

## Next Steps

1. **Explore configs:** Read `config/README.md` for details on each configuration
2. **Modify data:** Edit `data/fixtures.json` to change seed data
3. **Custom adapter:** Copy one of the configs and modify for your schema
4. **Scale up:** Use `--generate` with larger numbers to test performance
5. **Temporal queries:** Try `config/clickhouse/temporal.yaml` for point-in-time feature lookups

## Troubleshooting

### "Connection refused"
Services might not be ready. Check health:
```bash
docker-compose ps
docker-compose logs postgres
docker-compose logs redis
docker-compose logs clickhouse
```

### "Table not found"
Ensure you ran `ingest.py` first:
```bash
uv run ingest.py --engine postgres --seed
```

### "No such module: quiver"
Install the Python client:
```bash
pip install quiver-client
# Or if developing locally:
pip install -e ../quiver-python
```

### Quiver server won't start
Check the config file exists and is valid YAML:
```bash
ls examples/config/postgres/basic.yaml
make run CONFIG=examples/config/postgres/basic.yaml
```
