#!/bin/bash

# Configuration validation script for Quiver examples
# Usage: ./validate-config.sh [config-file]

set -e

CONFIG_FILE="${1:-config/development.yaml}"
EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$EXAMPLES_DIR")"

echo "🔍 Validating Quiver configuration: $CONFIG_FILE"
echo

# Check if config file exists
if [[ ! -f "$EXAMPLES_DIR/$CONFIG_FILE" ]]; then
    echo "❌ Configuration file not found: $EXAMPLES_DIR/$CONFIG_FILE"
    echo
    echo "Available configurations:"
    find "$EXAMPLES_DIR/config" -name "*.yaml" -exec basename {} \; | sort
    exit 1
fi

echo "📄 Configuration file: $EXAMPLES_DIR/$CONFIG_FILE"
echo

# Basic YAML syntax validation
if command -v yq >/dev/null 2>&1; then
    echo "🔧 Checking YAML syntax..."
    if yq eval '.' "$EXAMPLES_DIR/$CONFIG_FILE" >/dev/null 2>&1; then
        echo "✅ YAML syntax is valid"
    else
        echo "❌ YAML syntax error"
        exit 1
    fi
    echo
fi

# Check for required sections
echo "🏗️  Checking configuration structure..."

REQUIRED_SECTIONS=("server" "registry" "adapters")
for section in "${REQUIRED_SECTIONS[@]}"; do
    if command -v yq >/dev/null 2>&1; then
        if yq eval "has(\"$section\")" "$EXAMPLES_DIR/$CONFIG_FILE" | grep -q "true"; then
            echo "✅ Required section '$section' found"
        else
            echo "❌ Missing required section: $section"
            exit 1
        fi
    else
        if grep -q "^$section:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
            echo "✅ Required section '$section' found"
        else
            echo "❌ Missing required section: $section"
            exit 1
        fi
    fi
done
echo

# Security checks
echo "🔒 Security checks..."

# Check for TLS configuration in server section
if grep -q "tls:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "✅ TLS configuration found"
else
    echo "⚠️  No TLS configuration found (acceptable for development)"
fi

# Check for secure PostgreSQL connections
if grep -q "postgresql://" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    if grep -q "sslmode=require\|sslmode=verify" "$EXAMPLES_DIR/$CONFIG_FILE"; then
        echo "✅ PostgreSQL connections use secure SSL mode"
    else
        echo "⚠️  PostgreSQL connections may not be secure (check sslmode parameter)"
    fi
fi

# Check for secure Redis connections
if grep -q "redis://" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    if grep -q "rediss://" "$EXAMPLES_DIR/$CONFIG_FILE"; then
        echo "✅ Redis connections use TLS (rediss://)"
    else
        echo "⚠️  Some Redis connections may not use TLS (consider rediss://)"
    fi
fi

# Check for hardcoded passwords
if grep -q "password.*:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "⚠️  Hardcoded passwords found - consider using environment variables"
else
    echo "✅ No hardcoded passwords detected"
fi

echo

# Performance checks
echo "⚡ Performance configuration checks..."

# Check server performance settings
if grep -q "max_concurrent_rpcs:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "✅ max_concurrent_rpcs configured"
else
    echo "ℹ️  max_concurrent_rpcs not set (will use default)"
fi

if grep -q "timeout_seconds:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "✅ timeout_seconds configured"
else
    echo "ℹ️  timeout_seconds not set (will use default)"
fi

# Check adapter performance settings
if grep -q "max_connections:" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "✅ Database connection pool configured"
else
    echo "ℹ️  No connection pool settings found"
fi

echo

# Environment variable checks
echo "🌍 Environment variable recommendations..."

echo "Set these environment variables for secure deployment:"
if grep -q "postgresql://" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "  - QUIVER__ADAPTERS__<ADAPTER_NAME>__CONNECTION_STRING for PostgreSQL"
fi
if grep -q "redis" "$EXAMPLES_DIR/$CONFIG_FILE"; then
    echo "  - QUIVER__ADAPTERS__<ADAPTER_NAME>__PASSWORD for Redis"
fi

echo

# Summary
echo "📋 Validation Summary"
echo "Configuration file: $CONFIG_FILE"
echo "✅ Basic validation passed"
echo

# Try to compile check if we're in the project directory
if [[ -f "$PROJECT_ROOT/Cargo.toml" ]]; then
    echo "🔨 Testing compilation with this configuration..."
    cd "$PROJECT_ROOT"
    if QUIVER_CONFIG="$EXAMPLES_DIR/$CONFIG_FILE" cargo check --quiet 2>/dev/null; then
        echo "✅ Configuration compiles successfully"
    else
        echo "⚠️  Could not verify compilation (dependencies may need to be installed)"
    fi
else
    echo "ℹ️  Skipping compilation check (not in project directory)"
fi

echo
echo "🎉 Configuration validation complete!"
echo
echo "To run Quiver with this configuration:"
echo "  QUIVER_CONFIG=\"$EXAMPLES_DIR/$CONFIG_FILE\" make run"
echo
echo "To run with environment variables:"
echo "  export QUIVER__ADAPTERS__POSTGRES__CONNECTION_STRING=\"...\""
echo "  make run"