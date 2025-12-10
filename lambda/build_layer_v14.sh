#!/bin/bash
# Build Lambda layer for xagg (single layer, under 250MB unzipped)
#
# Usage:
#   ./build_layer_v14.sh [x86_64|arm64]

set -e

ARCH="${1:-x86_64}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/layer_build"
ZIP_NAME="lambda_layer_${ARCH}.zip"

# Find Python 3.11
if command -v python3.11 &> /dev/null; then
    PYTHON=python3.11
    PIP="python3.11 -m pip"
elif python3 -c "import sys; sys.exit(0 if sys.version_info[:2] == (3,11) else 1)" 2>/dev/null; then
    PYTHON=python3
    PIP=pip
else
    echo "ERROR: Python 3.11 required"
    exit 1
fi
PYTHON_VERSION=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

# Check architecture matches
MACHINE_ARCH=$(uname -m)
if [[ "$ARCH" == "arm64" && "$MACHINE_ARCH" != "aarch64" ]]; then
    echo "ERROR: Building arm64 layer on $MACHINE_ARCH machine"
    exit 1
fi
if [[ "$ARCH" == "x86_64" && "$MACHINE_ARCH" != "x86_64" ]]; then
    echo "ERROR: Building x86_64 layer on $MACHINE_ARCH machine"
    exit 1
fi

echo "============================================================"
echo "Building Lambda layer for ${ARCH}"
echo "Python: ${PYTHON_VERSION}, Machine: ${MACHINE_ARCH}"
echo "============================================================"

# Clean previous build
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/python"

# Create constraints file to prevent numpy upgrade
CONSTRAINTS="$OUTPUT_DIR/constraints.txt"
echo "numpy<2.3" > "$CONSTRAINTS"

# Install all packages with numpy constraint
# Pin pandas and h5coro to known working versions
echo "Installing packages with numpy<2.3 constraint..."
$PIP install \
    "numpy>=2.0,<2.3" \
    "pandas==2.3.2" fastparquet cramjam \
    healpy astropy \
    earthaccess shapely \
    -c "$CONSTRAINTS" \
    -t "$OUTPUT_DIR/python" \
    --no-cache-dir

echo "Installing h5coro and mortie (--no-deps)..."
$PIP install "h5coro==0.0.8" mortie --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir

# Verify numpy version
NUMPY_VERSION=$(ls "$OUTPUT_DIR/python" | grep -E "^numpy-" | head -1)
echo "Installed: $NUMPY_VERSION"
if [[ "$NUMPY_VERSION" == *"2.3"* ]]; then
    echo "ERROR: numpy 2.3.x installed - this breaks Lambda!"
    exit 1
fi

# Remove bloat
echo "Removing bloat..."
rm -rf "$OUTPUT_DIR/python"/pyarrow* \
       "$OUTPUT_DIR/python"/pyproj* \
       "$OUTPUT_DIR/python"/xarray* \
       "$OUTPUT_DIR/python"/matplotlib* \
       "$OUTPUT_DIR/python"/lonboard* \
       "$OUTPUT_DIR/python"/boto3* \
       "$OUTPUT_DIR/python"/botocore* 2>/dev/null || true

# Patch astropy to remove test runner (requires pytest at import time)
# This removes the TestRunner import and test() function from astropy/__init__.py
echo "Patching astropy to remove pytest dependency..."
ASTROPY_INIT="$OUTPUT_DIR/python/astropy/__init__.py"
if [ -f "$ASTROPY_INIT" ]; then
    # Remove "tests" and "test" from __all__ list
    sed -i 's/"tests",/# "tests",  # removed - requires pytest/' "$ASTROPY_INIT"
    sed -i 's/"test",/# "test",  # removed - requires pytest/' "$ASTROPY_INIT"

    # Comment out the TestRunner import and test function creation (lines 179-184)
    sed -i 's/^from \.tests\.runner import TestRunner$/# from .tests.runner import TestRunner  # removed - requires pytest/' "$ASTROPY_INIT"
    sed -i 's/^with warnings\.catch_warnings():$/# with warnings.catch_warnings():  # removed - requires pytest/' "$ASTROPY_INIT"
    sed -i 's/^    warnings\.filterwarnings("ignore", category=PendingDeprecationWarning)$/# warnings.filterwarnings("ignore", category=PendingDeprecationWarning)/' "$ASTROPY_INIT"
    sed -i 's/^    test = TestRunner\.make_test_runner_in(__path__\[0\])$/# test = TestRunner.make_test_runner_in(__path__[0])/' "$ASTROPY_INIT"

    # Add a dummy test attribute to prevent AttributeError
    echo "" >> "$ASTROPY_INIT"
    echo "# Dummy test attribute (pytest not available in Lambda)" >> "$ASTROPY_INIT"
    echo "test = None" >> "$ASTROPY_INIT"

    echo "  - Patched astropy/__init__.py"
fi

# Clean up caches and tests (now safe to remove astropy/tests since we patched __init__)
find "$OUTPUT_DIR/python" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyc" -delete 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyo" -delete 2>/dev/null || true

# Strip debug symbols from shared libraries
echo "Stripping binaries..."
find "$OUTPUT_DIR/python" -name "*.so" -exec strip {} \; 2>/dev/null || true

# Report unzipped size
UNZIPPED_SIZE=$(du -sh "$OUTPUT_DIR/python" | cut -f1)
UNZIPPED_BYTES=$(du -sb "$OUTPUT_DIR/python" | cut -f1)
echo ""
echo "Unzipped size: ${UNZIPPED_SIZE} (${UNZIPPED_BYTES} bytes)"

if [ "$UNZIPPED_BYTES" -gt 262144000 ]; then
    echo "ERROR: Exceeds 250MB Lambda limit!"
    exit 1
fi

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
