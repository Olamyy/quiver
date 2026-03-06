#!/bin/bash

set -e

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$EXAMPLES_DIR")"

echo "Quiver Configuration Quick Start"
echo "==================================="
echo

echo "Available configurations:"
echo

configs=(
    "memory-only.yaml|In-Memory|Development, testing, high-performance caching"
    "development.yaml|Development|Local development with PostgreSQL + Redis"
    "postgres-production.yaml|PostgreSQL Production|Production with persistent storage"
    "redis-realtime.yaml|Redis Real-time|Ultra-low latency real-time inference"
    "hybrid-multi-adapter.yaml|Hybrid Multi-Adapter|Complex ML pipelines with mixed adapters"
)

for i in "${!configs[@]}"; do
    IFS='|' read -r file icon desc <<< "${configs[$i]}"
    printf "%d) %s %-20s - %s\n" $((i+1)) "$icon" "$file" "$desc"
done

echo
read -p "Select a configuration (1-${#configs[@]}): " choice
echo

if [[ ! "$choice" =~ ^[1-9][0-9]*$ ]] || [ "$choice" -gt "${#configs[@]}" ] || [ "$choice" -lt 1 ]; then
    echo "Invalid choice. Please run the script again."
    exit 1
fi

selected_config="${configs[$((choice-1))]}"
IFS='|' read -r config_file icon desc <<< "$selected_config"

echo "Selected: $icon $config_file"
echo "Description: $desc"
echo

if [[ ! -f "$EXAMPLES_DIR/config/$config_file" ]]; then
    echo "Configuration file not found: $config_file"
    exit 1
fi

echo "Dependency checks..."
needs_postgres=false
needs_redis=false

if grep -q "type: postgres" "$EXAMPLES_DIR/config/$config_file"; then
    needs_postgres=true
    echo "This configuration requires PostgreSQL"
fi

if grep -q "type: redis" "$EXAMPLES_DIR/config/$config_file"; then
    needs_redis=true
    echo "This configuration requires Redis"
fi

if [[ "$needs_postgres" == false && "$needs_redis" == false ]]; then
    echo "No external dependencies required"
fi

echo

echo "Setup Instructions"
echo "==================="

if [[ "$needs_postgres" == true ]]; then
    echo
    echo "PostgreSQL Setup:"
    echo "1. Install PostgreSQL server"
    echo "2. Create database and tables for your features"
    echo "3. Set connection string:"
    echo "   export QUIVER__ADAPTERS__POSTGRES__CONNECTION_STRING=\"postgresql://user:password@localhost:5432/dbname?sslmode=require\""
    echo
fi

if [[ "$needs_redis" == true ]]; then
    echo "Redis Setup:"
    echo "1. Install and start Redis server"
    echo "2. Set password if required:"
    echo "   export QUIVER__ADAPTERS__REDIS__PASSWORD=\"your_password\""
    echo
fi

echo "Environment Variables:"
echo "Check the configuration file for any credentials that should be set via environment variables."
echo "Look for comments indicating QUIVER__ADAPTERS__<NAME>__<PARAM> variables."
echo

echo "Ready to start Quiver with this configuration?"
read -p "Start now? (y/N): " start_now

if [[ "$start_now" =~ ^[Yy]$ ]]; then
    echo
    echo "Starting Quiver..."
    echo "Configuration: examples/config/$config_file"
    echo
    
    cd "$PROJECT_ROOT"
    QUIVER_CONFIG="$EXAMPLES_DIR/config/$config_file" make run
else
    echo
    echo "To start Quiver later with this configuration:"
    echo "   cd $(realpath --relative-to=. "$PROJECT_ROOT")"
    echo "   QUIVER_CONFIG=\"examples/config/$config_file\" make run"
    echo
    echo "Or set it as default:"
    echo "   cp examples/config/$config_file config.yaml"
    echo "   make run"
fi

echo
echo "For more information:"
echo "   - Review examples/README.md"
echo "   - Check the main README.md"
echo "   - Visit the configuration documentation"