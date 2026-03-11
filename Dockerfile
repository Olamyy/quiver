# Stage 1: Builder
FROM rust:bookworm AS builder

WORKDIR /build

# Install system dependencies
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy source code
COPY . .

# Build release binary
RUN cd quiver-core && \
    cargo build --release --all-features && \
    strip target/release/quiver-core

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install ca-certificates and netcat for healthcheck
RUN apt-get update && \
    apt-get install -y ca-certificates netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 quiver

# Copy binary from builder
COPY --from=builder /build/quiver-core/target/release/quiver-core /usr/local/bin/

# Set working directory
WORKDIR /home/quiver

# Change ownership
RUN chown -R quiver:quiver /home/quiver

# Switch to non-root user
USER quiver

# Expose ports (8815: Arrow Flight, 8816: Observability)
EXPOSE 8815 8816

# Health check - Verify Arrow Flight server is listening on port 8815
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 8815 || exit 1

# Default entry point
# Usage: docker run -e QUIVER_CONFIG=/config/app.yaml -v /path/to/config.yaml:/config/app.yaml quiver-server
# Or:    docker run -v /path/to/config.yaml:/config/app.yaml quiver-server --config /config/app.yaml
ENTRYPOINT ["quiver-core"]
CMD []
