#!/bin/bash
# Build Lambda layer for x86_64 or arm64 architecture
#
# Usage:
#   ./build_layer.sh x86_64    # Build for Intel/AMD
#   ./build_layer.sh arm64     # Build for Graviton2
#   ./build_layer.sh both      # Build both architectures
#
# Requirements:
#   - Docker or Podman
#   - For cross-arch builds (e.g., arm64 on x86_64):
#       Fedora/RHEL: sudo dnf install qemu-user-static
#       Ubuntu/Debian: sudo apt install qemu-user-static

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_VERSION="3.11"

# Use podman if docker isn't available
if command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
elif command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
else
    echo "Error: Neither docker nor podman found"
    exit 1
fi

# Dependencies for the Lambda function
PACKAGES="numpy pandas pyarrow h5coro pyproj"

build_layer() {
    local ARCH=$1
    local PLATFORM=""
    local OUTPUT_DIR="${SCRIPT_DIR}/layer_${ARCH}"
    local ZIP_NAME="lambda_layer_${ARCH}.zip"

    case $ARCH in
        x86_64)
            PLATFORM="linux/amd64"
            ;;
        arm64)
            PLATFORM="linux/arm64"
            ;;
        *)
            echo "Unknown architecture: $ARCH"
            exit 1
            ;;
    esac

    echo "============================================================"
    echo "Building Lambda layer for ${ARCH}"
    echo "============================================================"

    # Clean previous build
    rm -rf "$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR/python"

    # Build using container runtime with the correct platform
    # :Z flag for SELinux relabeling on Fedora/RHEL
    $CONTAINER_CMD run --rm --platform "$PLATFORM" \
        -v "$OUTPUT_DIR/python:/out:Z" \
        python:${PYTHON_VERSION}-slim \
        bash -c "
            pip install --upgrade pip && \
            pip install ${PACKAGES} -t /out --no-cache-dir && \
            # Remove unnecessary files to reduce size
            find /out -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true && \
            find /out -type d -name 'tests' -exec rm -rf {} + 2>/dev/null || true && \
            find /out -type d -name 'test' -exec rm -rf {} + 2>/dev/null || true && \
            find /out -name '*.pyc' -delete 2>/dev/null || true && \
            find /out -name '*.pyo' -delete 2>/dev/null || true && \
            # Strip binaries
            find /out -name '*.so' -exec strip {} \; 2>/dev/null || true
        "

    # Create zip file
    echo "Creating ${ZIP_NAME}..."
    cd "$OUTPUT_DIR"
    zip -r9 "${SCRIPT_DIR}/${ZIP_NAME}" python
    cd "$SCRIPT_DIR"

    # Report size
    SIZE=$(du -h "${ZIP_NAME}" | cut -f1)
    echo "Created ${ZIP_NAME}: ${SIZE}"

    # Check against Lambda limit (250MB unzipped, 50MB zipped for direct upload)
    BYTES=$(stat -c%s "${ZIP_NAME}" 2>/dev/null || stat -f%z "${ZIP_NAME}")
    if [ "$BYTES" -gt 52428800 ]; then
        echo "WARNING: Layer exceeds 50MB direct upload limit. Use S3 upload."
    fi

    # Cleanup
    rm -rf "$OUTPUT_DIR"

    echo "Done building ${ARCH} layer"
    echo ""
}

# Parse arguments
case "${1:-both}" in
    x86_64|x86|amd64)
        build_layer x86_64
        ;;
    arm64|arm|graviton)
        build_layer arm64
        ;;
    both|all)
        build_layer x86_64
        build_layer arm64
        ;;
    *)
        echo "Usage: $0 [x86_64|arm64|both]"
        echo ""
        echo "Options:"
        echo "  x86_64  - Build for Intel/AMD architecture"
        echo "  arm64   - Build for ARM/Graviton2 architecture"
        echo "  both    - Build for both architectures (default)"
        exit 1
        ;;
esac

echo "============================================================"
echo "Layer build complete!"
echo "============================================================"
ls -lh lambda_layer_*.zip 2>/dev/null || echo "No layer zips found"
