# Docker Development Environment for Quiver

This directory contains Docker Compose configurations to quickly set up development databases for testing Quiver configurations.

## Quick Start

1. **Start the databases:**
   ```bash
   cd examples/docker
   docker-compose up -d
   ```

2. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

3. **Use with Quiver configurations:**
   ```bash
   cd ../../  # Back to project root
   QUIVER_CONFIG="examples/config/development.yaml" make run
   ```

## Services

### PostgreSQL (`postgres`)
- **Port**: 5432
- **Database**: `quiver_features`
- **User**: `quiver_user`
- **Password**: `quiver_dev_password`
- **Sample data**: Automatically loaded from `init-db.sql`

**Connection string for Quiver:**
```
postgresql://quiver_user:quiver_dev_password@localhost:5432/quiver_features?sslmode=prefer
```

### Redis with Password (`redis`)
- **Port**: 6379
- **Password**: `quiver_dev_redis_password`

**Connection for Quiver:**
```yaml
adapters:
  redis_example:
    type: redis
    connection: "redis://localhost:6379"
    # Set password via environment variable:
    # QUIVER__ADAPTERS__REDIS_EXAMPLE__PASSWORD=quiver_dev_redis_password
```

### Redis without Password (`redis-simple`)
- **Port**: 6380
- **Password**: None

**Connection for Quiver:**
```yaml
adapters:
  redis_simple:
    type: redis
    connection: "redis://localhost:6380"
```

## Admin Interfaces (Optional)

Start with admin interfaces for database management:

```bash
docker-compose --profile admin up -d
```

### pgAdmin (PostgreSQL)
- **URL**: http://localhost:8080
- **Email**: admin@quiver.dev
- **Password**: admin

To connect to PostgreSQL:
- **Host**: postgres
- **Port**: 5432
- **Database**: quiver_features
- **Username**: quiver_user
- **Password**: quiver_dev_password

### Redis Commander
- **URL**: http://localhost:8081
- **Connections**: 
  - `local` (with password)
  - `simple` (no password)

## Sample Data

The PostgreSQL instance comes pre-loaded with sample data:

### Tables
- `user_features_age` - User age data with temporal history
- `user_features_score` - User scores with temporal history
- `user_features_country` - User country data

### Sample Entities
- `user_1`, `user_2`, `user_3` with various feature values

### Redis Test Data
Populate Redis with test data:

```bash
# Connect to Redis with password
docker exec -it examples-redis-1 redis-cli -a quiver_dev_redis_password

# Set some test data
SET "features:user_1:session_count" "5"
SET "features:user_1:is_online" "true"
SET "features:user_2:session_count" "12"
SET "features:user_2:is_online" "false"

# Connect to simple Redis (no password)
docker exec -it examples-redis-simple-1 redis-cli

# Set test data
SET "dev:user_1:current_score" "95.5"
SET "dev:user_2:current_score" "87.3"
```

## Configuration Examples

### Development Configuration
Create a `docker-development.yaml` based on the development example:

```yaml
adapters:
  postgres_docker:
    type: postgres
    connection_string: "postgresql://quiver_user:quiver_dev_password@localhost:5432/quiver_features?sslmode=prefer"
    table_template: "user_features_{feature}"
    max_connections: 5
    timeout_seconds: 10
  
  redis_docker:
    type: redis
    connection: "redis://localhost:6379"
    key_template: "features:{entity}:{feature}"
    # Password: quiver_dev_redis_password (set via env var)
  
  redis_simple_docker:
    type: redis
    connection: "redis://localhost:6380"
    key_template: "dev:{entity}:{feature}"
```

### Environment Variables
```bash
export QUIVER__ADAPTERS__REDIS_DOCKER__PASSWORD="quiver_dev_redis_password"
```

## Commands

```bash
# Start all services
docker-compose up -d

# Start with admin interfaces
docker-compose --profile admin up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Stop and remove data
docker-compose down -v

# Reset database (reload init script)
docker-compose down postgres
docker volume rm examples_postgres_data
docker-compose up -d postgres
```

## Testing Your Configuration

1. **Start the Docker services:**
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready:**
   ```bash
   docker-compose ps  # All services should show "healthy"
   ```

3. **Test PostgreSQL connection:**
   ```bash
   docker exec -it examples-postgres-1 psql -U quiver_user -d quiver_features -c "SELECT * FROM user_features_summary;"
   ```

4. **Run Quiver with Docker-based configuration:**
   ```bash
   cd ../../
   QUIVER__ADAPTERS__REDIS_DOCKER__PASSWORD="quiver_dev_redis_password" \
   QUIVER_CONFIG="examples/config/development.yaml" make run
   ```

## Troubleshooting

### PostgreSQL Issues
- **Connection refused**: Check if PostgreSQL is running with `docker-compose ps`
- **Authentication failed**: Verify username/password in connection string
- **Database not found**: Ensure init script ran successfully

### Redis Issues  
- **Connection refused**: Check if Redis is running and port is correct
- **Authentication failed**: Verify password for the authenticated Redis instance
- **Wrong database**: Use port 6379 for authenticated, 6380 for simple

### General Issues
- **Port conflicts**: Change ports in `docker-compose.yaml` if they conflict with existing services
- **Permission issues**: Ensure Docker daemon is running and you have permissions
- **Data persistence**: Use `docker-compose down -v` to reset all data

## Cleanup

To completely remove all Docker resources:

```bash
docker-compose down -v --remove-orphans
docker system prune  # Optional: clean up unused Docker resources
```