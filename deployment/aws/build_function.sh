#!/bin/bash
# Build Lambda function code zip (handler + zagg package + non-layer deps)
#
# Usage:
#   ./build_function.sh              # auto-detect arch and python
#   ./build_function.sh --check-size # also verify combined size with layer
#
# The Lambda layer provides heavy deps (numpy, pandas, pyproj, etc).
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

# Prefer python3.12 (the Lambda runtime target, and what zagg's
# requires-python floor accepts) — same intent as build_layer.sh. Scan every
# PATH match and take the first one that actually carries pip: a uv/venv
# python shadows the system one but ships without pip, and `python -m pip`
# there dies with "No module named pip". CI's setup-python 3.12 has pip.
PYTHON=""
for cand in $(type -ap python3.12) $(type -ap python3); do
    if "$cand" -m pip --version >/dev/null 2>&1; then PYTHON="$cand"; break; fi
done
if [ -z "$PYTHON" ]; then
    echo "ERROR: no python3 with pip on PATH"; exit 1
fi
PIP="$PYTHON -m pip"

# Detect Python version
PY_VER=$($PYTHON -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")
ZIP_NAME="lambda_function_${ARCH_LABEL}_py${PY_VER}.zip"

echo "============================================================"
echo "Building Lambda function code"
echo "  Arch: ${ARCH_LABEL}, Python: ${PY_VER}, Machine: ${MACHINE_ARCH}"
echo "============================================================"

# --- Copy our code ---
echo ""
echo "Copying handler..."
cp "$REPO_ROOT/deployment/aws/lambda_handler.py" "$BUILD_DIR/"

# --- Install zagg itself (issue #546) ---
# A raw `cp -r src/zagg` ships no `_version.py` (hatch-vcs generates that file
# only during a package build), so every worker-side write recorded
# `zagg_version: 0.0.0+unknown`. Installing the repo through pip runs the
# hatchling + hatch-vcs backend, which stamps `zagg/_version.py` from the git
# tag. A shallow CI checkout (actions/checkout default) carries no tags, so
# deepen it first; on a full clone `describe` succeeds and nothing is fetched.
if ! git -C "$REPO_ROOT" describe --tags >/dev/null 2>&1; then
    git -C "$REPO_ROOT" fetch --quiet --tags --unshallow 2>/dev/null \
        || git -C "$REPO_ROOT" fetch --quiet --tags 2>/dev/null \
        || true
fi
echo ""
echo "Installing zagg (hatch-vcs stamps zagg/_version.py)..."
$PIP install --target "$BUILD_DIR" --no-deps --no-cache-dir "$REPO_ROOT"

# Assert the stamp: 0.0.0* is the zagg/__init__.py fallback sentinel and
# 0.1.dev* is setuptools-scm's no-tag fallback — either would put an unusable
# version in every worker-written artifact, undetectable from the store
# (issue #546). Mirrored in tests/test_lambda_build.py.
ZAGG_BUILD_VERSION=$($PYTHON -c "
import runpy, sys
print(runpy.run_path(sys.argv[1])['__version__'])
" "$BUILD_DIR/zagg/_version.py")
echo "zagg version stamped: ${ZAGG_BUILD_VERSION}"
case "$ZAGG_BUILD_VERSION" in
    ""|0.0.0*|0.1.dev*)
        echo "ERROR: zagg version resolved to '${ZAGG_BUILD_VERSION}' — workers built from"
        echo "       this zip would write zagg_version 0.0.0+unknown-class artifacts"
        echo "       (issue #546). Make a git tag reachable (git fetch --tags --unshallow)"
        echo "       or set SETUPTOOLS_SCM_PRETEND_VERSION_FOR_ZAGG before building."
        exit 1 ;;
esac

# pip --target materializes [project.scripts] launchers under bin/ — dead
# weight in a Lambda zip, with shebangs pointing at the build machine. Removed
# here, before the deps install, and not with the other cleanup below: pip
# resolves a --target collision by SKIPPING the colliding top-level item with a
# warning ("Target directory .../bin already exists"), so a surviving bin/ from
# this install silently drops the next one's.
rm -rf "$BUILD_DIR/bin"

# --- Install function-level dependencies ---
# These are packages NOT in the Lambda layer.
# pip resolves transitive deps automatically — no manual dep hunting.
echo ""
echo "Installing function dependencies (pip resolves transitive deps)..."
$PIP install --target "$BUILD_DIR" --no-cache-dir \
    "obstore>=0.8.2" \
    "zarr>=3.1.5" \
    "pydantic-zarr>=0.9.1" \
    "pyyaml"

# --- Remove packages already in the Lambda layer ---
# The layer provides these (plus their transitive deps). Removing them from
# function code avoids duplication and saves space within the 250MB limit.
LAYER_PACKAGES=(
    # Core scientific (in layer)
    numpy scipy pandas
    # IO (in layer)
    fastparquet cramjam pyarrow
    # Geo (in layer)
    pyproj odc affine cachetools
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
# bin/ from the deps install (zagg's was removed before it, see above).
rm -rf "$BUILD_DIR/bin"
find "$BUILD_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
# Strip dist-info except for packages whose code calls importlib.metadata.version()
# at runtime. zarr / pydantic_zarr do this for in-package version checks; without
# their dist-info, calls into ArraySpec.from_zarr fail with PackageNotFoundError.
find "$BUILD_DIR" -type d -name "*.dist-info" \
    ! -name "zarr-*" ! -name "pydantic_zarr-*" \
    -exec rm -rf {} + 2>/dev/null || true
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

# Function code budget: 32MB (espg ruling 2026-08-24, PR #511 question 1) —
# an early-warning tripwire under AWS's 50MB direct-upload zip limit, leaving
# room for the ~220MB layer; mirrored in tests/test_lambda_build.py.
FUNCTION_BUDGET=$((32 * 1024 * 1024))
if [ "$UNZIPPED_BYTES" -gt "$FUNCTION_BUDGET" ]; then
    echo "WARNING: Function code exceeds 32MB budget!"
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
