# Installation

## Server

### Docker (recommended)

```bash
docker run -p 8815:8815 -p 8816:8816 \
  -v $(pwd)/config.yaml:/etc/quiver/config.yaml \
  ghcr.io/olamyy/quiver:latest
```

### Binary

Download a pre-built binary from [GitHub Releases](https://github.com/Olamyy/quiver/releases):

=== "macOS (Apple Silicon)"
    ```bash
    curl -L https://github.com/Olamyy/quiver/releases/latest/download/quiver-server-aarch64-apple-darwin.tar.gz | tar xz
    ./quiver-core --config config.yaml
    ```

=== "macOS (Intel)"
    ```bash
    curl -L https://github.com/Olamyy/quiver/releases/latest/download/quiver-server-x86_64-apple-darwin.tar.gz | tar xz
    ./quiver-core --config config.yaml
    ```

=== "Linux (x86_64)"
    ```bash
    curl -L https://github.com/Olamyy/quiver/releases/latest/download/quiver-server-x86_64-unknown-linux-gnu.tar.gz | tar xz
    ./quiver-core --config config.yaml
    ```

### From Source

Requires Rust and `protoc`:

```bash
git clone https://github.com/Olamyy/quiver.git
cd quiver
make install       # installs Rust, protoc, Python deps
make build-release
./quiver-core/target/release/quiver-core --config config.yaml
```

---

## Python Client

```bash
pip install "quiver-python @ git+https://github.com/Olamyy/quiver.git#subdirectory=quiver-python"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add "quiver-python @ git+https://github.com/Olamyy/quiver.git#subdirectory=quiver-python"
```

---

## Ports

| Port | Service |
|------|---------|
| `8815` | Arrow Flight (feature serving) |
| `8816` | Observability service |
