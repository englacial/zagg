#!/bin/bash
# Build Lambda function code zip (handler + magg package + non-layer deps)
#
# Usage:
#   ./build_function.sh              # auto-detect arch and python
#   ./build_function.sh --check-size # also verify combined size with layer
#
# The Lambda layer provides heavy deps (numpy, pandas, healpy, etc).
# This script builds the function code with lighter deps (zarr, obstore, etc)
# that pip resolves transitively — no more manual dep discovery.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/../builds"
BUILD_DIR="$(mktemp -d)"
CHECK_SIZE=false

for arg in "$@"; do
    case "$arg" in
        --check-size) CHECK_SIZE=true ;;
    esac
done

trap "rm -rf $BUILD_DIR" EXIT

# Detect architecture
MACHINE_ARCH=$(uname -m)
case "$MACHINE_ARCH" in
    x86_64)  ARCH_LABEL="x86_64" ;;
    aarch64) ARCH_LABEL="arm64" ;;
    arm64)   ARCH_LABEL="arm64" ;;
    *) echo "ERROR: Unknown architecture: $MACHINE_ARCH"; exit 1 ;;
esac

# Detect Python version
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")
ZIP_NAME="lambda_function_${ARCH_LABEL}_py${PY_VER}.zip"

echo "============================================================"
echo "Building Lambda function code"
echo "  Arch: ${ARCH_LABEL}, Python: ${PY_VER}, Machine: ${MACHINE_ARCH}"
echo "============================================================"

# --- Copy our code ---
echo ""
echo "Copying handler and magg package..."
cp "$REPO_ROOT/deployment/aws/lambda_handler.py" "$BUILD_DIR/"
cp -r "$REPO_ROOT/src/magg" "$BUILD_DIR/magg"

# --- Install function-level dependencies ---
# These are packages NOT in the Lambda layer.
# pip resolves transitive deps automatically — no manual dep hunting.
echo ""
echo "Installing function dependencies (pip resolves transitive deps)..."
pip3 install --target "$BUILD_DIR" --no-cache-dir \
    "obstore>=0.8.2" \
    "zarr>=3.1.5" \
    "pydantic-zarr>=0.9.1" \
    "pandera"

# --- Remove packages already in the Lambda layer ---
# The layer provides these (plus their transitive deps). Removing them from
# function code avoids duplication and saves space within the 250MB limit.
LAYER_PACKAGES=(
    # Core scientific (in layer)
    numpy scipy pandas
    # IO (in layer)
    fastparquet cramjam pyarrow
    # Geo (in layer)
    # Data access (in layer)
    earthaccess shapely h5coro mortie
    # AWS (provided by Lambda runtime or layer)
    boto3 botocore s3fs fsspec
    # Common transitive deps (in layer via earthaccess/pandas/etc)
    requests urllib3 certifi charset_normalizer idna
    python_dateutil pytz tzdata six packaging
    setuptools pip wheel _distutils_hack distutils
)

echo ""
echo "Removing layer-overlap packages..."
for pkg in "${LAYER_PACKAGES[@]}"; do
    # Remove package dir, .libs dir, dist-info, and any variant-named files
    rm -rf "$BUILD_DIR/${pkg}" \
           "$BUILD_DIR/${pkg}"[-_.]* \
           "$BUILD_DIR/${pkg}".libs \
           2>/dev/null || true
    # Handle hyphen/underscore variants
    alt="${pkg//-/_}"
    [ "$alt" != "$pkg" ] && rm -rf "$BUILD_DIR/${alt}" "$BUILD_DIR/${alt}"[-_.]* "$BUILD_DIR/${alt}".libs 2>/dev/null || true
    alt="${pkg//_/-}"
    [ "$alt" != "$pkg" ] && rm -rf "$BUILD_DIR/${alt}" "$BUILD_DIR/${alt}"[-_.]* "$BUILD_DIR/${alt}".libs 2>/dev/null || true
done

# --- Clean build artifacts ---
echo "Cleaning caches and test directories..."
find "$BUILD_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -name "*.pyc" -delete 2>/dev/null || true
find "$BUILD_DIR" -name "*.pyo" -delete 2>/dev/null || true

# --- Strip debug symbols from native libraries ---
echo "Stripping binaries..."
find "$BUILD_DIR" -name "*.so" -exec strip --strip-debug {} \; 2>/dev/null || true

# --- Report ---
UNZIPPED_BYTES=$(du -sb "$BUILD_DIR" | cut -f1)
UNZIPPED_SIZE=$(du -sh "$BUILD_DIR" | cut -f1)

echo ""
echo "Contents (top-level):"
ls -1 "$BUILD_DIR" | head -40
ITEM_COUNT=$(ls -1 "$BUILD_DIR" | wc -l)
[ "$ITEM_COUNT" -gt 40 ] && echo "  ... ($ITEM_COUNT total items)"

echo ""
echo "Function code: ${UNZIPPED_SIZE} (${UNZIPPED_BYTES} bytes)"

# Function code budget: 30MB leaves room for the ~220MB layer
FUNCTION_BUDGET=$((30 * 1024 * 1024))
if [ "$UNZIPPED_BYTES" -gt "$FUNCTION_BUDGET" ]; then
    echo "WARNING: Function code exceeds 30MB budget!"
    echo "  Top directories by size:"
    du -sh "$BUILD_DIR"/*/ 2>/dev/null | sort -rh | head -10
fi

# --- Create zip ---
mkdir -p "$OUTPUT_DIR"
cd "$BUILD_DIR" && zip -r9q "${OUTPUT_DIR}/${ZIP_NAME}" .
cd "$SCRIPT_DIR"

ZIPPED_BYTES=$(stat -c%s "${OUTPUT_DIR}/${ZIP_NAME}" 2>/dev/null || stat -f%z "${OUTPUT_DIR}/${ZIP_NAME}")
ZIPPED_SIZE=$(du -h "${OUTPUT_DIR}/${ZIP_NAME}" | cut -f1)

echo ""
echo "============================================================"
echo "Build complete!"
echo "============================================================"
echo "  Arch:     ${ARCH_LABEL}"
echo "  Python:   ${PY_VER}"
echo "  Zipped:   ${ZIPPED_SIZE} (${ZIPPED_BYTES} bytes)"
echo "  Unzipped: ${UNZIPPED_SIZE} (${UNZIPPED_BYTES} bytes)"
echo "  Output:   ${OUTPUT_DIR}/${ZIP_NAME}"

# --- Optional: check combined size with layer ---
if [ "$CHECK_SIZE" = true ]; then
    LAYER_ZIP="${SCRIPT_DIR}/../layers/lambda_layer_${ARCH_LABEL}.zip"
    if [ -f "$LAYER_ZIP" ]; then
        # Measure layer unzipped size
        LAYER_TMP="$(mktemp -d)"
        unzip -qo "$LAYER_ZIP" -d "$LAYER_TMP"
        LAYER_BYTES=$(du -sb "$LAYER_TMP" | cut -f1)
        rm -rf "$LAYER_TMP"

        COMBINED=$((LAYER_BYTES + UNZIPPED_BYTES))
        LIMIT=$((250 * 1024 * 1024))

        echo ""
        echo "Combined size check:"
        echo "  Layer:    $(numfmt --to=iec $LAYER_BYTES)"
        echo "  Function: $(numfmt --to=iec $UNZIPPED_BYTES)"
        echo "  Combined: $(numfmt --to=iec $COMBINED)"
        echo "  Limit:    $(numfmt --to=iec $LIMIT)"

        if [ "$COMBINED" -gt "$LIMIT" ]; then
            echo "ERROR: Combined size exceeds 250MB Lambda limit!"
            exit 1
        else
            echo "  Status:   OK ($(numfmt --to=iec $((LIMIT - COMBINED))) headroom)"
        fi
    else
        echo ""
        echo "WARNING: Layer zip not found at $LAYER_ZIP — skipping combined size check"
    fi
fi
