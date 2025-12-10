#!/bin/bash
# Build Lambda layer matching v14 (xagg-complete-stack:14)
#
# Target: ~60MB zipped, ~180MB unzipped (under 250MB Lambda limit)
#
# Usage:
#   ./build_layer_v14.sh [x86_64|arm64]

set -e

ARCH="${1:-x86_64}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/layer_build"
ZIP_NAME="lambda_layer_${ARCH}.zip"

echo "============================================================"
echo "Building Lambda layer for ${ARCH}"
echo "============================================================"

# Clean previous build
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/python"

# Install packages
echo "Installing earthaccess (brings boto3, s3fs, fsspec, aiohttp, etc.)..."
pip install earthaccess -t "$OUTPUT_DIR/python" --no-cache-dir

echo "Installing scientific stack..."
pip install numpy pandas -t "$OUTPUT_DIR/python" --no-cache-dir

echo "Installing fastparquet + cramjam (NOT pyarrow)..."
pip install fastparquet cramjam -t "$OUTPUT_DIR/python" --no-cache-dir

echo "Installing healpy + astropy..."
pip install healpy astropy -t "$OUTPUT_DIR/python" --no-cache-dir

echo "Installing shapely..."
pip install shapely -t "$OUTPUT_DIR/python" --no-cache-dir

echo "Installing h5coro and mortie (--no-deps)..."
pip install h5coro --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir
pip install mortie --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir

# Remove bloat
echo "Removing bloat..."
rm -rf "$OUTPUT_DIR/python"/pyarrow* \
       "$OUTPUT_DIR/python"/pyproj* \
       "$OUTPUT_DIR/python"/xarray* \
       "$OUTPUT_DIR/python"/matplotlib* \
       "$OUTPUT_DIR/python"/lonboard*

# Clean up caches and tests
find "$OUTPUT_DIR/python" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyc" -delete 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyo" -delete 2>/dev/null || true

# Strip debug symbols from shared libraries
echo "Stripping binaries..."
find "$OUTPUT_DIR/python" -name "*.so" -exec strip {} \; 2>/dev/null || true

# Report unzipped size
echo ""
echo "Installed packages:"
ls -1 "$OUTPUT_DIR/python" | grep -E "^[a-z]" | head -50
echo ""
UNZIPPED_SIZE=$(du -sh "$OUTPUT_DIR/python" | cut -f1)
echo "Unzipped size: ${UNZIPPED_SIZE}"

# Create zip
echo ""
echo "Creating ${ZIP_NAME}..."
cd "$OUTPUT_DIR"
zip -r9q "${SCRIPT_DIR}/${ZIP_NAME}" python
cd "$SCRIPT_DIR"

# Report final sizes
ZIPPED_SIZE=$(du -h "${ZIP_NAME}" | cut -f1)
ZIPPED_BYTES=$(stat -c%s "${ZIP_NAME}" 2>/dev/null || stat -f%z "${ZIP_NAME}")

echo ""
echo "============================================================"
echo "Build complete!"
echo "============================================================"
echo "  Arch:     ${ARCH}"
echo "  Zipped:   ${ZIPPED_SIZE} (${ZIPPED_BYTES} bytes)"
echo "  Unzipped: ${UNZIPPED_SIZE}"
echo ""
ls -lh "${SCRIPT_DIR}/${ZIP_NAME}"

# Cleanup build dir
rm -rf "$OUTPUT_DIR"
