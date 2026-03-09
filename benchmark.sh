#!/usr/bin/env bash
#
# Complete benchmark workflow: setup, ingest, start server, run benchmarks
#
# Usage:
#   ./benchmark.sh                    # Full flow: setup + ingest + server + bench
#   ./benchmark.sh --scenario 1       # Setup + ingest + server + specific scenario
#   ./benchmark.sh --batch-sizes 100,1000  # Custom batch sizes
#   ./benchmark.sh --help             # Show all options
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENTITY_COUNT=100
SCENARIO=""
BATCH_SIZES=""
SKIP_INGEST=false
SKIP_SERVER=false
SKIP_BENCH=false
CONFIG_SCENARIO=1

# Scenario to config mapping (can't use associative arrays in pure sh)
get_config_name() {
    case $1 in
        1) echo "redis-baseline" ;;
        2) echo "postgres-baseline" ;;
        3) echo "fanout-2x" ;;
        4) echo "fanout-3x" ;;
        *) echo "" ;;
    esac
}

print_help() {
    cat << 'EOF'
Complete Benchmark Workflow Script

Usage:
  ./benchmark.sh [OPTIONS]

Options:
  --scenario N              Run specific scenario (1-4, default: 1)
  --batch-sizes S1,S2,...   Custom batch sizes (default: 1,10,100,1000,10000)
  --entity-count N          Entities per batch (default: 100, must match ingest)
  --skip-ingest             Skip data ingestion step
  --skip-server             Skip server startup step
  --skip-bench              Skip benchmark execution step
  --help                    Show this help message

Scenarios:
  1  Redis baseline (single backend)
  2  PostgreSQL baseline (single backend)
  3  Fanout 2x (Redis + PostgreSQL parallel)
  4  Fanout 3x (all three backends parallel)

Examples:
  # Full workflow with default settings
  ./benchmark.sh

  # Quick iteration: skip ingest, use scenario 3
  ./benchmark.sh --scenario 3 --skip-ingest

  # Test with custom batch sizes
  ./benchmark.sh --batch-sizes 100,1000

  # Only run ingest and server (no benchmarks)
  ./benchmark.sh --skip-bench

EOF
}

log_section() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}\n"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_error() {
    echo -e "${RED}✗ $1${NC}"
}

log_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --scenario)
            SCENARIO="$2"
            CONFIG_SCENARIO="$2"
            shift 2
            ;;
        --batch-sizes)
            BATCH_SIZES="$2"
            shift 2
            ;;
        --entity-count)
            ENTITY_COUNT="$2"
            shift 2
            ;;
        --skip-ingest)
            SKIP_INGEST=true
            shift
            ;;
        --skip-server)
            SKIP_SERVER=true
            shift
            ;;
        --skip-bench)
            SKIP_BENCH=true
            shift
            ;;
        --help)
            print_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_help
            exit 1
            ;;
    esac
done

# Validate scenario
if [[ -n "$SCENARIO" && ! "$SCENARIO" =~ ^[1-4]$ ]]; then
    log_error "Invalid scenario: $SCENARIO. Must be 1-4."
    exit 1
fi

# Set defaults
SCENARIO=${SCENARIO:-1}
BATCH_SIZES=${BATCH_SIZES:-"1,10,100,1000,10000"}
CONFIG_NAME=$(get_config_name "$SCENARIO")

log_section "Quiver Benchmark Suite - Complete Workflow"
echo "Configuration:"
echo "  Scenario:      $SCENARIO ($CONFIG_NAME)"
echo "  Entity Count:  $ENTITY_COUNT"
echo "  Batch Sizes:   $BATCH_SIZES"
echo "  Skip Ingest:   $SKIP_INGEST"
echo "  Skip Server:   $SKIP_SERVER"
echo "  Skip Bench:    $SKIP_BENCH"

# Step 1: Verify backends are running
log_section "Step 1: Verify Backends"

if ! docker ps -q --filter "name=examples-postgres" &>/dev/null; then
    log_error "PostgreSQL container not running"
    echo "Start backends with: cd examples && docker-compose up -d"
    exit 1
fi
log_success "PostgreSQL running"

if ! docker ps -q --filter "name=examples-redis" &>/dev/null; then
    log_error "Redis container not running"
    echo "Start backends with: cd examples && docker-compose up -d"
    exit 1
fi
log_success "Redis running"

# Step 2: Clear and ingest data
if [[ "$SKIP_INGEST" != "true" ]]; then
    log_section "Step 2: Clear Data & Ingest $ENTITY_COUNT Entities"

    log_info "Clearing and re-ingesting data..."
    cd "$SCRIPT_DIR/examples"

    # Run ingestion with specified entity count
    log_info "Ingesting PostgreSQL data ($ENTITY_COUNT entities)..."
    uv run ingest.py postgres --count "$ENTITY_COUNT" > /dev/null 2>&1 || {
        log_error "PostgreSQL ingestion failed"
        exit 1
    }
    log_success "PostgreSQL ingested"

    log_info "Ingesting Redis data ($ENTITY_COUNT entities)..."
    uv run ingest.py redis --count "$ENTITY_COUNT" > /dev/null 2>&1 || {
        log_error "Redis ingestion failed"
        exit 1
    }
    log_success "Redis ingested"

    log_info "Ingesting ClickHouse data ($ENTITY_COUNT entities)..."
    uv run ingest.py clickhouse --count "$ENTITY_COUNT" > /dev/null 2>&1 || {
        log_error "ClickHouse ingestion failed (optional)"
    }
    log_success "ClickHouse ingested (optional)"
else
    log_section "Step 2: Skipped (Using Existing Data)"
fi

# Step 3: Kill any existing server
log_section "Step 3: Clean Up Old Server Instance"

if pgrep -f "target/release/quiver-core" &>/dev/null; then
    log_info "Killing existing Quiver server..."
    pkill -f "target/release/quiver-core" || true
    sleep 1
    log_success "Server killed"
else
    log_info "No existing server running"
fi

# Step 4: Start server
if [[ "$SKIP_SERVER" != "true" ]]; then
    log_section "Step 4: Start Quiver Server"

    CONFIG_PATH="examples/config/benchmark/${CONFIG_NAME}.yaml"

    if [[ ! -f "$SCRIPT_DIR/$CONFIG_PATH" ]]; then
        log_error "Config file not found: $CONFIG_PATH"
        exit 1
    fi

    log_info "Starting server with config: $CONFIG_NAME"
    log_info "Config path: $CONFIG_PATH"

    cd "$SCRIPT_DIR/quiver-core"
    cargo build --release > /dev/null 2>&1 || {
        log_error "Build failed"
        exit 1
    }
    log_success "Build complete"

    log_info "Spawning server on port 8815..."
    cargo run --release -- --config "../$CONFIG_PATH" > /tmp/quiver-server.log 2>&1 &
    SERVER_PID=$!

    # Wait for server to be ready
    log_info "Waiting for server readiness..."
    TIMEOUT=10
    COUNTER=0
    while ! nc -z 127.0.0.1 8815 2>/dev/null; do
        if [ $COUNTER -ge $TIMEOUT ]; then
            log_error "Server failed to start within ${TIMEOUT}s"
            echo "Server logs:"
            tail -20 /tmp/quiver-server.log
            kill $SERVER_PID 2>/dev/null || true
            exit 1
        fi
        sleep 0.5
        COUNTER=$((COUNTER + 1))
    done

    log_success "Server ready on port 8815 (PID: $SERVER_PID)"
else
    log_section "Step 4: Skipped (Using Existing Server)"
fi

# Step 5: Run benchmarks
if [[ "$SKIP_BENCH" != "true" ]]; then
    log_section "Step 5: Run Benchmarks"

    log_info "Benchmark configuration:"
    echo "  Scenario:     $SCENARIO ($CONFIG_NAME)"
    echo "  Batch Sizes:  $BATCH_SIZES"
    echo "  Duration:     ~3-5 minutes for baseline"

    cd "$SCRIPT_DIR/quiver-core"

    # Run with custom batch sizes
    log_info "Starting benchmarks..."
    BENCH_BATCH_SIZES="$BATCH_SIZES" cargo bench --bench throughput -- "scenario_$SCENARIO" || {
        log_error "Benchmarks failed"
        exit 1
    }

    log_success "Benchmarks complete"

else
    log_section "Step 5: Skipped (No Benchmarks)"
fi

# Step 6: Summary
log_section "Workflow Complete"

if [[ "$SKIP_BENCH" != "true" ]]; then
    echo "Results available at:"
    echo "  ${SCRIPT_DIR}/quiver-core/target/criterion/report/index.html"
    echo ""
    echo "View with:"
    echo "  open ${SCRIPT_DIR}/quiver-core/target/criterion/report/index.html"
fi

if [[ "$SKIP_SERVER" != "true" ]]; then
    echo ""
    echo "Server is still running (PID: $SERVER_PID)"
    echo "Stop with: pkill -f 'target/release/quiver-core'"
fi

log_success "Done!"
