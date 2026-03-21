# Docker

## Run with Docker

```bash
docker run -p 8815:8815 -p 8816:8816 \
  -v $(pwd)/config.yaml:/etc/quiver/config.yaml \
  ghcr.io/olamyy/quiver:latest
```

The server reads config from `QUIVER_CONFIG` env var or the `--config` flag:

```bash
docker run -p 8815:8815 \
  -e QUIVER_CONFIG=/etc/quiver/config.yaml \
  -v $(pwd)/config.yaml:/etc/quiver/config.yaml \
  ghcr.io/olamyy/quiver:latest
```

## Docker Compose

A typical setup with Quiver + PostgreSQL + Redis:

```yaml
services:
  quiver:
    image: ghcr.io/olamyy/quiver:latest
    ports:
      - "8815:8815"
      - "8816:8816"
    environment:
      QUIVER_CONFIG: /etc/quiver/config.yaml
      RUST_LOG: info
    volumes:
      - ./config.yaml:/etc/quiver/config.yaml
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: quiver
      POSTGRES_PASSWORD: quiver
      POSTGRES_DB: features
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U quiver"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
```

## Logging

Set `RUST_LOG` to control verbosity:

| Value | Output |
|-------|--------|
| `error` | Errors only |
| `warn` | Warnings + errors |
| `info` | Startup, requests (default) |
| `debug` | Detailed resolver steps |
| `trace` | Full adapter-level tracing |

```bash
docker run -e RUST_LOG=debug ...
```
