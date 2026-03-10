#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DIST_DIR="${PROJECT_ROOT}/dist"

# Parse arguments
TARGET=""
HELP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --target)
            TARGET="$2"
            shift 2
            ;;
        --help)
            HELP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            HELP=true
            shift
            ;;
    esac
done

# Show help
if [ "$HELP" = true ]; then
    cat << 'EOF'
Usage: ./scripts/build-release.sh [OPTIONS]

Build Quiver release binaries.

Options:
  --target TARGET    Build for specific target (e.g., x86_64-unknown-linux-gnu)
                     If not specified, builds for current platform
  --help             Show this help message

Examples:
  # Build for current platform
  ./scripts/build-release.sh

  # Build for Linux x86_64
  ./scripts/build-release.sh --target x86_64-unknown-linux-gnu

  # Build for macOS ARM64
  ./scripts/build-release.sh --target aarch64-apple-darwin

EOF
    exit 0
fi

# Get version from Cargo.toml
VERSION=$(grep '^version' "$PROJECT_ROOT/quiver-core/Cargo.toml" | head -1 | sed 's/version = "\(.*\)"/\1/')

echo -e "${GREEN}🔨 Building Quiver v${VERSION}${NC}"

# Create dist directory
mkdir -p "$DIST_DIR"

# Determine target
if [ -z "$TARGET" ]; then
    echo -e "${YELLOW}ℹ️  Target not specified, detecting current platform...${NC}"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if [[ $(uname -m) == "arm64" ]]; then
            TARGET="aarch64-apple-darwin"
        else
            TARGET="x86_64-apple-darwin"
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        TARGET="x86_64-unknown-linux-gnu"
    elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        TARGET="x86_64-pc-windows-msvc"
    else
        echo -e "${RED}❌ Unable to detect platform${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}ℹ️  Building for target: ${TARGET}${NC}"

# Add target if not already installed
rustup target add "$TARGET" 2>/dev/null || true

# Build
echo -e "${YELLOW}ℹ️  Building binary...${NC}"
cd "$PROJECT_ROOT/quiver-core"
cargo build --release --target "$TARGET" --all-features

BINARY_PATH="target/${TARGET}/release/quiver-core"
if [ "$TARGET" = "x86_64-pc-windows-msvc" ]; then
    BINARY_PATH="${BINARY_PATH}.exe"
fi

if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}❌ Binary not found at ${BINARY_PATH}${NC}"
    exit 1
fi

cd "$PROJECT_ROOT"

# Create archive
ARCHIVE_NAME="quiver-server-v${VERSION}-${TARGET}"

if [[ "$TARGET" == *"windows"* ]]; then
    # Windows: create zip
    if command -v powershell &> /dev/null; then
        powershell -Command "Compress-Archive -Path 'quiver-core/target/${TARGET}/release/quiver-core.exe' -DestinationPath '${DIST_DIR}/${ARCHIVE_NAME}.zip' -Force"
        ARCHIVE_PATH="${DIST_DIR}/${ARCHIVE_NAME}.zip"
    else
        echo -e "${RED}❌ PowerShell not found, cannot create zip archive${NC}"
        exit 1
    fi
else
    # Unix: create tar.gz
    tar czf "${DIST_DIR}/${ARCHIVE_NAME}.tar.gz" \
        -C "quiver-core/target/${TARGET}/release" \
        quiver-core
    ARCHIVE_PATH="${DIST_DIR}/${ARCHIVE_NAME}.tar.gz"
fi

# Generate checksum
if command -v shasum &> /dev/null; then
    CHECKSUM=$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')
    echo "$CHECKSUM  $(basename "$ARCHIVE_PATH")" > "${ARCHIVE_PATH}.sha256"
elif command -v sha256sum &> /dev/null; then
    CHECKSUM=$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')
    echo "$CHECKSUM  $(basename "$ARCHIVE_PATH")" > "${ARCHIVE_PATH}.sha256"
else
    echo -e "${YELLOW}⚠️  Warning: sha256sum/shasum not found, skipping checksum generation${NC}"
fi

echo -e "${GREEN}✅ Build complete!${NC}"
echo ""
echo "📦 Artifacts:"
ls -lh "$DIST_DIR"
echo ""
echo "🔍 To verify checksum:"
if [ -f "${ARCHIVE_PATH}.sha256" ]; then
    echo "  sha256sum -c ${ARCHIVE_PATH##*/}.sha256"
fi
