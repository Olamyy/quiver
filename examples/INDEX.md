# Quiver Examples Directory

This directory provides comprehensive configuration examples and tools for different Quiver deployment scenarios.

## Directory Structure

```
examples/
├── README.md                           # Main examples documentation
├── quick-start.sh                      # Interactive configuration selector
├── validate-config.sh                  # Configuration validation tool
│
├── config/                             # Configuration examples
│   ├── memory-only.yaml                # In-memory development/testing
│   ├── development.yaml                # Local development setup  
│   ├── postgres-production.yaml        # Production PostgreSQL deployment
│   ├── redis-realtime.yaml            # Real-time Redis-based serving
│   └── hybrid-multi-adapter.yaml      # Multi-adapter ML pipeline
│
└── docker/                            # Docker development environment
    ├── docker-compose.yaml            # PostgreSQL + Redis services
    ├── init-db.sql                    # Sample database schema/data
    └── README.md                      # Docker setup documentation
```

## Quick Start

1. **Choose a configuration interactively:**
   ```bash
   ./examples/quick-start.sh
   ```

2. **Or pick directly from examples:**
   ```bash
   # Development with in-memory storage
   QUIVER_CONFIG="examples/config/memory-only.yaml" make run
   
   # Local development with databases
   cd examples/docker && docker-compose up -d
   cd ../.. && QUIVER_CONFIG="examples/config/development.yaml" make run
   ```

3. **Validate your configuration:**
   ```bash
   ./examples/validate-config.sh config/my-config.yaml
   ```

## Configuration Examples

| Configuration | Use Case | Adapters | Best For |
|---------------|----------|----------|----------|
| [`memory-only.yaml`](config/memory-only.yaml) | Development/Testing | Memory | Fast prototyping, unit tests |
| [`development.yaml`](config/development.yaml) | Local Development | PostgreSQL + Redis | Full-stack local development |
| [`postgres-production.yaml`](config/postgres-production.yaml) | Production | PostgreSQL | Enterprise deployments with persistence |
| [`redis-realtime.yaml`](config/redis-realtime.yaml) | Real-time Inference | Redis | Ultra-low latency serving (<5ms) |
| [`hybrid-multi-adapter.yaml`](config/hybrid-multi-adapter.yaml) | Complex ML | Memory + PostgreSQL + Redis | Multi-modal feature serving |

## Tools

### Interactive Configuration Selector
```bash
./examples/quick-start.sh
```
- Shows available configurations
- Validates dependencies  
- Provides setup instructions
- Can start Quiver directly

### Configuration Validator
```bash
./examples/validate-config.sh [config-file]
```
- YAML syntax validation
- Security checks (TLS, credentials)
- Performance configuration review
- Environment variable recommendations

### Docker Development Environment
```bash
cd examples/docker
docker-compose up -d  # Start PostgreSQL + Redis
```
- Pre-configured databases with sample data
- Optional admin interfaces (pgAdmin, Redis Commander)
- Ready-to-use for development and testing

## Usage Patterns

### Environment-based Configuration
```bash
# Set sensitive credentials via environment variables
export QUIVER__ADAPTERS__POSTGRES__CONNECTION_STRING="postgresql://..."
export QUIVER__ADAPTERS__REDIS__PASSWORD="..."

# Run with configuration
QUIVER_CONFIG="examples/config/production.yaml" make run
```

### Configuration Inheritance
```bash
# Copy example as starting point
cp examples/config/development.yaml config.yaml

# Customize for your environment
vim config.yaml

# Run with default config location
make run
```

### Docker Development Workflow
```bash
# 1. Start development databases
cd examples/docker
docker-compose up -d

# 2. Verify services are healthy
docker-compose ps

# 3. Run Quiver with database-dependent configuration
cd ../..
QUIVER_CONFIG="examples/config/development.yaml" make run

# 4. Optional: Access admin interfaces
open http://localhost:8080  # pgAdmin
open http://localhost:8081  # Redis Commander
```

## Learn More

- **[Main README](../README.md)** - Project overview and getting started
- **[Examples README](README.md)** - Detailed configuration documentation
- **[Docker README](docker/README.md)** - Development environment setup
- **Configuration files** - Inline comments explain each setting

## Contributing

When adding new examples:
1. Follow naming convention: `{use-case}.yaml`
2. Add comprehensive comments
3. Include security recommendations  
4. Update this index
5. Test with validation script

## Example Commands

```bash
# Validate all example configurations
for config in examples/config/*.yaml; do
  echo "Validating $config..."
  ./examples/validate-config.sh "$config"
done

# Test configuration loading
QUIVER_CONFIG="examples/config/memory-only.yaml" cargo check

# Start with Docker databases
cd examples/docker && docker-compose up -d
cd ../.. && ./examples/quick-start.sh

# Reset Docker environment  
cd examples/docker && docker-compose down -v && docker-compose up -d
```