#!/bin/bash
# Build the zagg Lambda layer (single layer, under 250MB unzipped).
#
# Usage:
#   ./build_layer.sh [x86_64|arm64]
#
# Runs inside an arch-matched manylinux_2_28 container (cp312) — see
# .github/workflows/lambda-build.yml:
#   x86_64 -> quay.io/pypa/manylinux_2_28_x86_64
#   arm64  -> quay.io/pypa/manylinux_2_28_aarch64

set -e

ARCH="${1:-x86_64}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/layer_build"
ZIP_NAME="lambda_layer_${ARCH}.zip"

# Both arches target the Python 3.12 Lambda runtime (AL2023, glibc 2.34), which
# is compatible with the manylinux_2_28 wheels of geo deps like pyproj. Build in
# a manylinux_2_28 image (cp312 at /opt/python/cp312-cp312) for both.
PYTHON=$(command -v python3.12 || echo /opt/python/cp312-cp312/bin/python)
PIP="$PYTHON -m pip"
PYVER=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

# Sanity: machine arch must match the requested layer arch.
MACHINE_ARCH=$(uname -m)
if [[ "$ARCH" == "arm64" && "$MACHINE_ARCH" != "aarch64" ]]; then
    echo "ERROR: building arm64 layer on $MACHINE_ARCH machine"; exit 1
fi
if [[ "$ARCH" == "x86_64" && "$MACHINE_ARCH" != "x86_64" ]]; then
    echo "ERROR: building x86_64 layer on $MACHINE_ARCH machine"; exit 1
fi

echo "============================================================"
echo "Building Lambda layer for ${ARCH} (Python ${PYVER}, machine ${MACHINE_ARCH})"
echo "============================================================"

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/python"

CONSTRAINTS="$OUTPUT_DIR/constraints.txt"
echo "numpy<2.3" > "$CONSTRAINTS"

# numpy: arm64 Lambda requires 64KB page alignment, so build from source with
# the right LDFLAGS; x86_64 uses the prebuilt wheel.
echo "Installing numpy..."
if [[ "$ARCH" == "arm64" ]]; then
    export LDFLAGS="-Wl,-z,max-page-size=0x10000"
    export NPY_BLAS_ORDER=openblas
    $PIP install "numpy==2.2.6" --no-binary numpy -t "$OUTPUT_DIR/python" --no-cache-dir
else
    $PIP install "numpy>=2.0,<2.3" -t "$OUTPUT_DIR/python" --no-cache-dir
fi

# Core processing deps. pyproj + odc-geo (and affine/cachetools) are required:
# zagg.grids imports odc.geo at module load, and rectilinear assign reprojects
# lat/lon -> grid CRS at processing time. zarr/obstore/pydantic-zarr are NOT
# here -- they are function-level deps installed by build_function.sh.
echo "Installing processing deps..."
# arro3-core (issue #130) is the deployed-worker Arrow carrier, replacing pyarrow:
# ~7 MB, zero required deps, importable (pyarrow's bindings hard-link a ~100 MB
# unstrippable C++ core that can't fit the gate while importable). pyarrow is NOT
# installed here -- its only use is off-Lambda zagg.catalog. Keep the pin in sync
# with the `lambda` extra in pyproject.toml.
$PIP install \
    "pandas==2.2.3" "arro3-core==0.8.1" fastparquet cramjam \
    shapely pyproj odc-geo affine cachetools \
    -c "$CONSTRAINTS" \
    -t "$OUTPUT_DIR/python" \
    --no-cache-dir

echo "Installing h5coro and mortie (--no-deps)..."
$PIP install "h5coro==1.0.4" mortie --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir

# Verify numpy stayed < 2.3
NUMPY_VERSION=$(ls "$OUTPUT_DIR/python" | grep -E "^numpy-" | head -1)
echo "Installed: $NUMPY_VERSION"
if [[ "$NUMPY_VERSION" == *"2.3"* ]]; then
    echo "ERROR: numpy 2.3.x installed - this breaks Lambda!"; exit 1
fi

# Remove bloat. pyproj is intentionally NOT stripped (rectilinear/odc-geo assign
# needs it). pyarrow is no longer installed (issue #130): the worker's Arrow carrier
# is arro3-core, which needs no component strip, so the whole pyarrow strip block is
# gone with it.
echo "Removing bloat..."
rm -rf "$OUTPUT_DIR/python"/xarray* \
       "$OUTPUT_DIR/python"/matplotlib* \
       "$OUTPUT_DIR/python"/lonboard* \
       "$OUTPUT_DIR/python"/boto3* \
       "$OUTPUT_DIR/python"/botocore* 2>/dev/null || true

# Clean caches/tests and strip debug symbols.
find "$OUTPUT_DIR/python" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyc" -delete 2>/dev/null || true
find "$OUTPUT_DIR/python" -name "*.pyo" -delete 2>/dev/null || true
echo "Stripping binaries..."
find "$OUTPUT_DIR/python" -name "*.so" -exec strip {} \; 2>/dev/null || true

# Report unzipped size and enforce the 250MB Lambda limit.
UNZIPPED_SIZE=$(du -sh "$OUTPUT_DIR/python" | cut -f1)
UNZIPPED_BYTES=$(du -sb "$OUTPUT_DIR/python" | cut -f1)
echo ""
echo "Unzipped size: ${UNZIPPED_SIZE} (${UNZIPPED_BYTES} bytes)"
if [ "$UNZIPPED_BYTES" -gt 262144000 ]; then
    echo "ERROR: Exceeds 250MB Lambda limit!"; exit 1
fi

# Create zip
mkdir -p "${SCRIPT_DIR}/../layers"
echo "Creating ${ZIP_NAME}..."
cd "$OUTPUT_DIR"
zip -r9q "${SCRIPT_DIR}/../layers/${ZIP_NAME}" python
cd "$SCRIPT_DIR"

ZIPPED_SIZE=$(du -h "../layers/${ZIP_NAME}" | cut -f1)
echo ""
echo "============================================================"
echo "Build complete!"
echo "  Arch:     ${ARCH}"
echo "  Zipped:   ${ZIPPED_SIZE}"
echo "  Unzipped: ${UNZIPPED_SIZE}"
echo "============================================================"
ls -lh "${SCRIPT_DIR}/../layers/${ZIP_NAME}"

rm -rf "$OUTPUT_DIR"
