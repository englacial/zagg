#!/bin/bash
# Build ARM64 Lambda layer on Apple Silicon Mac
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/layer_build_arm64"
ZIP_NAME="lambda_layer_arm64.zip"

echo "============================================================"
echo "Building ARM64 Lambda layer (Apple Silicon)"
echo "============================================================"

# Verify we're on ARM
if [[ "$(uname -m)" != "arm64" ]]; then
    echo "WARNING: Not running on ARM - build will use emulation (slower)"
fi

# Clean previous build
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/python"

# Build using manylinux_2_28 container
# glibc 2.28 is compatible with Lambda's glibc 2.34
docker run --rm --platform linux/arm64 \
    -v "$OUTPUT_DIR:/out" \
    quay.io/pypa/manylinux_2_28_aarch64 \
    bash -c '
        # Use Python 3.12 (Lambda Python 3.12 uses AL2023 with glibc 2.34)
        # Python 3.11 Lambda still uses AL2 with glibc 2.26, which is too old for healpy
        PYTHON=/opt/python/cp312-cp312/bin/python
        PIP="$PYTHON -m pip"

        echo "Python: $($PYTHON --version)"
        echo "pip: $($PIP --version)"

        # Create constraints to prevent numpy upgrade
        echo "numpy<2.3" > /tmp/constraints.txt

        # Install packages
        echo ""
        echo "Installing packages..."

        # Build NumPy from source with 64KB page alignment for Lambda ARM64
        # The pre-built wheels have 4KB alignment which causes ELF load errors
        export LDFLAGS="-Wl,-z,max-page-size=0x10000"
        export NPY_BLAS_ORDER=openblas
        $PIP install "numpy==2.2.6" --no-binary numpy -t /out/python --no-cache-dir

        # Install remaining packages with numpy pinned
        echo "numpy==2.2.6" > /tmp/constraints.txt
        $PIP install \
            "pandas==2.2.3" fastparquet cramjam \
            healpy astropy \
            earthaccess shapely \
            -c /tmp/constraints.txt \
            -t /out/python \
            --no-cache-dir

        # Install h5coro and mortie without deps
        echo ""
        echo "Installing h5coro and mortie (no deps)..."
        $PIP install "h5coro==0.0.8" mortie --no-deps -t /out/python --no-cache-dir

        # Verify numpy version
        NUMPY_VER=$(ls /out/python | grep -E "^numpy-" | head -1)
        echo ""
        echo "Installed: $NUMPY_VER"
        if [[ "$NUMPY_VER" != "numpy-2.2.6.dist-info" ]]; then
            echo "WARNING: Expected numpy 2.2.6, got $NUMPY_VER"
        fi

        # Remove bloat (packages already in Lambda or not needed)
        # NOTE: Keep botocore - aiobotocore needs 1.41.x but Lambda has 1.40.4
        echo ""
        echo "Removing bloat..."
        rm -rf /out/python/pyarrow* \
               /out/python/pyproj* \
               /out/python/xarray* \
               /out/python/matplotlib* \
               /out/python/lonboard* \
               /out/python/boto3* 2>/dev/null || true

        # Patch astropy to remove pytest dependency
        echo "Patching astropy..."
        ASTROPY_INIT="/out/python/astropy/__init__.py"
        if [ -f "$ASTROPY_INIT" ]; then
            sed -i "s/\"tests\",/# \"tests\",  # removed/" "$ASTROPY_INIT"
            sed -i "s/\"test\",/# \"test\",  # removed/" "$ASTROPY_INIT"
            sed -i "s/^from \\.tests\\.runner import TestRunner$/# from .tests.runner import TestRunner  # removed/" "$ASTROPY_INIT"
            sed -i "s/^with warnings\\.catch_warnings():$/# with warnings.catch_warnings():  # removed/" "$ASTROPY_INIT"
            sed -i "s/^    warnings\\.filterwarnings.*PendingDeprecationWarning.*$/# (removed)/" "$ASTROPY_INIT"
            sed -i "s/^    test = TestRunner\\.make_test_runner_in.*$/# (removed)/" "$ASTROPY_INIT"
            echo "" >> "$ASTROPY_INIT"
            echo "test = None  # pytest not available in Lambda" >> "$ASTROPY_INIT"
        fi

        # Clean caches and tests
        echo "Cleaning caches..."
        find /out/python -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
        find /out/python -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
        find /out/python -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
        find /out/python -name "*.pyc" -delete 2>/dev/null || true
        find /out/python -name "*.pyo" -delete 2>/dev/null || true

        # Strip debug symbols
        echo "Stripping binaries..."
        find /out/python -name "*.so" -exec strip {} \; 2>/dev/null || true

        # Remove duplicate/stale .so files in .libs directories
        echo "Removing duplicate .libs entries..."
        # Keep only the newest openblas in numpy.libs
        cd /out/python/numpy.libs 2>/dev/null && ls -t libopenblas64*.so 2>/dev/null | tail -n +2 | xargs rm -f 2>/dev/null || true
        cd /out/python/numpy.libs 2>/dev/null && ls -t libscipy_openblas64*.so 2>/dev/null | tail -n +2 | xargs rm -f 2>/dev/null || true
        cd /out/python/numpy.libs 2>/dev/null && ls -t libgfortran*.so* 2>/dev/null | tail -n +2 | xargs rm -f 2>/dev/null || true
        # Keep only the newest healpy libs
        cd /out/python/healpy.libs 2>/dev/null && ls -t libhealpix*.so* 2>/dev/null | tail -n +2 | xargs rm -f 2>/dev/null || true
        cd /out/python/healpy.libs 2>/dev/null && ls -t libcfitsio*.so* 2>/dev/null | tail -n +2 | xargs rm -f 2>/dev/null || true

        # Remove astropy IERS data (large, not needed for our use case)
        echo "Removing astropy IERS data..."
        rm -rf /out/python/astropy_iers_data/data/*.all 2>/dev/null || true
        rm -rf /out/python/astropy_iers_data/data/eopc04* 2>/dev/null || true

        # Report size
        echo ""
        UNZIPPED=$(du -sh /out/python | cut -f1)
        echo "Unzipped size: $UNZIPPED"
    '

# Verify size limit
UNZIPPED_BYTES=$(du -sb "$OUTPUT_DIR/python" 2>/dev/null || stat -f%z "$OUTPUT_DIR/python" 2>/dev/null || echo "0")
# macOS doesn't have du -sb, use alternative
if [[ "$OSTYPE" == "darwin"* ]]; then
    UNZIPPED_BYTES=$(find "$OUTPUT_DIR/python" -type f -exec stat -f%z {} + | awk '{s+=$1} END {print s}')
fi
if [ "$UNZIPPED_BYTES" -gt 262144000 ]; then
    echo "ERROR: Exceeds 250MB Lambda limit!"
    exit 1
fi

# Create zip
echo ""
echo "Creating ${ZIP_NAME}..."
cd "$OUTPUT_DIR"
zip -r9q "${SCRIPT_DIR}/../layers/${ZIP_NAME}" python
cd "$SCRIPT_DIR"

# Report
ZIPPED_SIZE=$(du -h "${ZIP_NAME}" | cut -f1)
UNZIPPED_SIZE=$(du -sh "$OUTPUT_DIR/python" | cut -f1)

echo ""
echo "============================================================"
echo "Build complete!"
echo "============================================================"
echo "  Arch:     arm64 (Graviton2)"
echo "  Zipped:   ${ZIPPED_SIZE}"
echo "  Unzipped: ${UNZIPPED_SIZE}"
echo ""
ls -lh "${SCRIPT_DIR}/../layers/${ZIP_NAME}"

# Cleanup
rm -rf "$OUTPUT_DIR"
