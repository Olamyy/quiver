# Installation Guide

Quiver supports multiple installation methods. Choose the one that best fits your deployment model.

## Quick Start

**Docker (Recommended for Cloud/Kubernetes)**:
```bash
docker pull ghcr.io/olamyy/quiver-server:latest
docker run -p 8815:8815 ghcr.io/olamyy/quiver-server:latest --help
```

**Binary Download (Recommended for Servers)**:
```bash
# Download latest release (replace VERSION with actual release tag)
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-x86_64-linux-gnu.tar.gz | tar xz
./quiver-core --help
```

**Build from Source (Recommended for Contributors)**:
```bash
git clone https://github.com/Olamyy/quiver.git
cd quiver
make build-release
./quiver-core/target/release/quiver-core --help
```

---

## Installation Methods

### 1. Docker (Multi-Platform, Cloud-Native)

#### Quick Start
```bash
# Pull latest image
docker pull ghcr.io/olamyy/quiver-server:latest

# Run with default settings
docker run -p 8815:8815 -p 8816:8816 ghcr.io/olamyy/quiver-server:latest --config /etc/quiver/config.yaml
```

#### With Configuration File
```bash
# Create config file
cat > quiver-config.yaml << 'EOF'
server:
  host: "0.0.0.0"
  port: 8815
  timeout_seconds: 30

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: redis

adapters:
  redis:
    type: redis
    url: redis://redis-host:6379
    source_path: "features:{feature}"
EOF

# Run with config
docker run -v $(pwd)/quiver-config.yaml:/etc/quiver/config.yaml \
  -p 8815:8815 -p 8816:8816 \
  ghcr.io/olamyy/quiver-server:latest --config /etc/quiver/config.yaml
```

#### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: quiver-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: quiver
  template:
    metadata:
      labels:
        app: quiver
    spec:
      containers:
      - name: quiver
        image: ghcr.io/olamyy/quiver-server:latest
        ports:
        - name: flight
          containerPort: 8815
        - name: observability
          containerPort: 8816
        volumeMounts:
        - name: config
          mountPath: /etc/quiver
        livenessProbe:
          exec:
            command:
            - /usr/local/bin/quiver-core
            - --version
          initialDelaySeconds: 10
          periodSeconds: 30
      volumes:
      - name: config
        configMap:
          name: quiver-config
```

#### Available Tags
- `latest` - Most recent stable release
- `vX.Y.Z` - Specific version tag (e.g., v1.0.0)
- `vX.Y` - Latest patch of X.Y series
- `vX` - Latest release of X major version

---

### 2. Binary Download (Simple, No Build Required)

#### Download Latest Release
```bash
# Set version (e.g., 0.1.0)
VERSION="latest"

# Detect platform
if [[ "$OSTYPE" == "darwin"* ]]; then
  if [[ $(uname -m) == "arm64" ]]; then
    PLATFORM="aarch64-apple-darwin"
  else
    PLATFORM="x86_64-apple-darwin"
  fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
  PLATFORM="x86_64-unknown-linux-gnu"
else
  echo "Unsupported platform"
  exit 1
fi

# Download and extract
ARCHIVE="quiver-server-v${VERSION}-${PLATFORM}.tar.gz"
curl -L "https://github.com/Olamyy/quiver/releases/download/v${VERSION}/${ARCHIVE}" | tar xz

# Run
./quiver-core --version
```

#### Manual Platform Selection
```bash
# Linux x86_64
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-x86_64-unknown-linux-gnu.tar.gz | tar xz

# macOS Intel
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-x86_64-apple-darwin.tar.gz | tar xz

# macOS ARM64 (Apple Silicon)
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-aarch64-apple-darwin.tar.gz | tar xz

# Windows x86_64
# Download from: https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-x86_64-pc-windows-msvc.zip
```

#### Verify Checksum
```bash
# Download checksums (replace VERSION with actual release tag)
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/SHA256SUMS.txt -o SHA256SUMS.txt

# Verify (Linux/macOS)
sha256sum -c SHA256SUMS.txt

# Verify (macOS alternative)
shasum -a 256 -c SHA256SUMS.txt

# Verify (Windows - PowerShell)
Get-Content SHA256SUMS.txt | ForEach-Object {
  $parts = $_ -split '  '
  $file = $parts[1]
  $expected = $parts[0]
  $actual = (Get-FileHash -Path $file -Algorithm SHA256).Hash.ToLower()
  if ($actual -eq $expected) { Write-Host "$file OK" } else { Write-Host "$file FAILED" }
}
```

#### Install to System Path
```bash
# Extract to current directory (replace VERSION with actual release tag)
tar xzf quiver-server-vVERSION-x86_64-linux-gnu.tar.gz

# Install to /usr/local/bin (requires sudo)
sudo install -m 755 quiver-core /usr/local/bin/quiver-core

# Now run from anywhere
quiver-core --version
```

---

### 3. Build from Source (For Contributors/Custom Builds)

#### Requirements
- Rust 1.70+ ([Install Rust](https://rustup.rs/))
- Protoc 3.20+ ([Install protoc](https://grpc.io/docs/protoc-installation/))
- Cargo (installed with Rust)

#### Clone and Build
```bash
# Clone repository
git clone https://github.com/Olamyy/quiver.git
cd quiver

# Install dependencies
make install

# Build release binary
make build-release

# Binary location
./quiver-core/target/release/quiver-core --version
```

#### Development Build (Debug Mode)
```bash
# Faster build time, slower runtime
make build

# Run tests
make test

# Run specific test
make test-rs-single TEST=test_name

# Full quality pipeline
make ci-check
```

#### Install Locally
```bash
# Copy binary to ~/.local/bin
cp quiver-core/target/release/quiver-core ~/.local/bin/

# Or system-wide
sudo cp quiver-core/target/release/quiver-core /usr/local/bin/

# Verify
quiver-core --version
```

---

### 4. Cargo Install (For Developers)

#### Install Latest
```bash
# Install from GitHub releases (once published to crates.io)
cargo install quiver-core

# Install specific version
cargo install quiver-core --version X.Y.Z

# Verify
quiver-core --version
```

#### Build Time
- First install: ~3-5 minutes (builds from source)
- Subsequent installs: ~1 minute (uses cached dependencies)

#### Uninstall
```bash
cargo uninstall quiver-core
```

---

### 5. Homebrew (macOS/Linux, Phase 2+)

#### Install via Homebrew (Coming Soon)
```bash
# Add tap
brew tap olamyy/quiver

# Install
brew install quiver-server

# Verify
quiver-core --version

# Upgrade
brew upgrade quiver-server

# Uninstall
brew uninstall quiver-server
```

**Note**: Homebrew support will be available in Phase 2. Currently, use Docker or binary downloads.

---

## Choosing Your Installation Method

| Method | Best For | Setup Time | Build Time | Platform Support |
|--------|----------|-----------|-----------|------------------|
| **Docker** | Cloud, Kubernetes, CI/CD | <1 min | Pre-built | All (auto-detected) |
| **Binary Download** | Servers, simple deployments | <1 min | N/A | Linux, macOS, Windows |
| **Source Build** | Contributors, custom builds | 5-10 min | 3-5 min | Linux, macOS, Windows |
| **Cargo Install** | Developers, local testing | 1-3 min | 3-5 min | Linux, macOS, Windows |
| **Homebrew** | macOS/Linux convenience | <1 min | Pre-built | macOS, Linux |

### Quick Decision Tree

1. **Using Kubernetes or Docker?** → Use Docker
2. **Need it now on a server?** → Download binary
3. **Contributing or modifying code?** → Build from source
4. **Developer on macOS/Linux?** → Use Cargo Install or Homebrew
5. **Windows desktop?** → Download binary or build from source

---

## Configuration

### Quick Start Config
```yaml
server:
  host: "127.0.0.1"
  port: 8815
  timeout_seconds: 30

registry:
  type: static
  views:
    - name: user_features
      entity_type: user
      entity_key: user_id
      columns:
        - name: score
          arrow_type: float64
          nullable: true
          source: redis

adapters:
  redis:
    type: redis
    url: redis://localhost:6379
    source_path: "features:{feature}"
```

### See Also
- Full configuration reference: `examples/config/`
- Configuration guide: `CLAUDE.md` (Architecture section)
- Example configurations: `examples/config/multi-adapter/`

---

## Verification

### Test Installation
```bash
# Check version
quiver-core --version

# Check help
quiver-core --help

# (For Docker)
docker run ghcr.io/olamyy/quiver-server:latest --version
```

### Test Connectivity
```bash
# Start server with example config
quiver-core --config examples/config/postgres/basic.yaml

# In another terminal, test with grpcurl
grpcurl -plaintext localhost:8815 list

# Or test with Python client
python -c "from quiver_client import QuiverClient; c = QuiverClient('localhost:8815'); print(c.get_schema('user_features'))"
```

---

## Troubleshooting

### Binary Not Found After Download
```bash
# Ensure it's extracted
tar xzf quiver-server-vVERSION-*.tar.gz

# Verify extraction
ls -la quiver-core

# Make executable (if needed)
chmod +x quiver-core

# Run with explicit path
./quiver-core --version
```

### Permission Denied
```bash
# Make binary executable
chmod +x quiver-core

# Or install to system path with sudo
sudo install -m 755 quiver-core /usr/local/bin/
```

### Docker Image Too Large
```bash
# Pull from GHCR (should be ~100MB)
docker pull ghcr.io/olamyy/quiver-server:latest

# Check size
docker images | grep quiver-server

# Expected: ~100MB (with slim base image)
```

### Version Mismatch
```bash
# Verify installed version matches expected
quiver-core --version

# Compare with release
curl -s https://api.github.com/repos/Olamyy/quiver/releases/latest | jq .tag_name
```

---

## Upgrading

### Docker
```bash
# Pull new image
docker pull ghcr.io/olamyy/quiver-server:vVERSION

# Update container reference and restart
docker run ghcr.io/olamyy/quiver-server:vVERSION ...
```

### Binary
```bash
# Download new version
curl -L https://github.com/Olamyy/quiver/releases/download/vVERSION/quiver-server-vVERSION-x86_64-linux-gnu.tar.gz | tar xz

# Replace old binary
sudo install -m 755 quiver-core /usr/local/bin/quiver-core
```

### Source Build
```bash
git pull origin main
git checkout vVERSION
make build-release
```

### Cargo
```bash
cargo install quiver-core --version 0.7.1 --force
```

---

## Uninstalling

### Docker
```bash
# Remove image
docker rmi ghcr.io/olamyy/quiver-server:vVERSION

# Remove containers
docker rm <container-id>
```

### Binary
```bash
# If in PATH
rm /usr/local/bin/quiver-core

# Or local
rm ./quiver-core
```

### Cargo
```bash
cargo uninstall quiver-core
```

---

## Support & Issues

- **Documentation**: See [README.md](README.md) and [CLAUDE.md](CLAUDE.md)
- **Issues**: Report at https://github.com/Olamyy/quiver/issues
