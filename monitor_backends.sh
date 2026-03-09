#!/bin/bash
# Monitor backends during benchmark

echo "=== Redis Monitor ==="
docker-compose exec redis redis-cli MONITOR &
REDIS_PID=$!

echo ""
echo "=== PostgreSQL Query Monitor ==="
docker-compose exec postgres psql -U quiver -d quiver_test -c "SELECT query FROM pg_stat_statements ORDER BY query_start DESC LIMIT 5;" &

# Give monitors time to start
sleep 2

echo ""
echo "=== Starting Benchmark ==="
cd /Users/ola.wahab/Desktop/work/personal/quiver

# Run single scenario with verbose output
BENCH_BATCH_SIZES=1 cargo bench --bench throughput -- scenario_1_redis_baseline 2>&1 | grep -E "\[BENCH|time:|Found"

# Clean up
kill $REDIS_PID 2>/dev/null
