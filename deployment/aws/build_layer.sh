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

# Every exact layer pin is single-sourced from the `lambda` extra in
# pyproject.toml (PR #436, extending the issue-322 mortie pattern below): one
# edit site, and a pin that drifts from the extra cannot exist. The
# assignments run before any install and fail the build loudly under `set -e`
# when the extra stops carrying an exact pin for a requested name.
lambda_pin() {
    $PYTHON -c '
import pathlib, sys, tomllib
extra = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["project"]["optional-dependencies"]["lambda"]
print(next(d for d in extra if "==" in d and d.split("==")[0].strip() == sys.argv[2]))
' "${SCRIPT_DIR}/../../pyproject.toml" "$1"
}
NUMPY_PIN=$(lambda_pin numpy)
PANDAS_PIN=$(lambda_pin pandas)
ARRO3_PIN=$(lambda_pin arro3-core)
H5CORO_PIN=$(lambda_pin h5coro)
HIDEFIX_PIN=$(lambda_pin h5coro-hidefix)
ASYNC_TIFF_PIN=$(lambda_pin async-tiff)
XARRAY_PIN=$(lambda_pin xarray)
H5NETCDF_PIN=$(lambda_pin h5netcdf)
H5PY_PIN=$(lambda_pin h5py)
ICECHUNK_PIN=$(lambda_pin icechunk)

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
# the right LDFLAGS; x86_64 uses the prebuilt wheel. Both arches install the
# extra's exact pin, so the two layers carry the same numpy.
echo "Installing numpy ($NUMPY_PIN)..."
if [[ "$ARCH" == "arm64" ]]; then
    export LDFLAGS="-Wl,-z,max-page-size=0x10000"
    export NPY_BLAS_ORDER=openblas
    $PIP install "$NUMPY_PIN" --no-binary numpy -t "$OUTPUT_DIR/python" --no-cache-dir
else
    $PIP install "$NUMPY_PIN" -t "$OUTPUT_DIR/python" --no-cache-dir
fi

# Core processing deps. pyproj + odc-geo (and affine/cachetools) are required:
# zagg.grids imports odc.geo at module load, and rectilinear assign reprojects
# lat/lon -> grid CRS at processing time. zarr/obstore/pydantic-zarr are NOT
# here -- they are function-level deps installed by build_function.sh.
echo "Installing processing deps..."
# arro3-core (issue #130) is the deployed-worker Arrow carrier, replacing pyarrow:
# ~7 MB, zero required deps, importable (pyarrow's bindings hard-link a ~100 MB
# unstrippable C++ core that can't fit the gate while importable). pyarrow is NOT
# installed here -- its only use is off-Lambda zagg.catalog.
# xarray + h5netcdf/h5py (issue #220): the temporal worker (process_event)
# reads collections/masks as xarray datasets and opens NetCDF4/HDF5 bytes
# in-memory through the h5netcdf engine (h5coro is a byte-range reader, not
# an xarray engine). All exact pins here come from the `lambda` extra
# (lambda_pin above).
$PIP install \
    "$PANDAS_PIN" "$ARRO3_PIN" fastparquet cramjam \
    "$XARRAY_PIN" "$H5NETCDF_PIN" "$H5PY_PIN" \
    shapely pyproj odc-geo affine cachetools \
    -c "$CONSTRAINTS" \
    -t "$OUTPUT_DIR/python" \
    --no-cache-dir

# h5coro-hidefix (issue #149): compiled hidefix chunk reader backing the
# `sidecar` index backend. abi3 manylinux_2_28 wheel (~2.2 MB) on both arches;
# its only runtime dep is numpy (already installed above). The pin comes from
# the `lambda` extra (lambda_pin above).
# mortie's version spec is single-sourced from pyproject.toml (issue #322):
# --no-deps means pip never resolves the runtime floor declared there, so an
# unpinned install silently accepted whatever was latest at build time -- even
# a release below the floor. Passing the declared spec (a) makes pip error and
# abort under `set -e` when the floor is ahead of what PyPI has published
# (pyproject L30-38 documents that state for h5coro-hidefix), and (b) leaves
# one edit site: a future floor bump or exact pin there reaches the layer with
# no second edit. The layer's mortie still floats to latest-above-floor, so
# this is floor enforcement, not pinning.
MORTIE_SPEC=$($PYTHON -c '
import pathlib, re, sys, tomllib
deps = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["project"]["dependencies"]
print(next(d for d in deps if re.match(r"mortie($|[\s<>=!~\[])", d)))
' "${SCRIPT_DIR}/../../pyproject.toml")
echo "Installing h5coro ($H5CORO_PIN), h5coro-hidefix ($HIDEFIX_PIN) and mortie ('$MORTIE_SPEC' from pyproject, --no-deps)..."
$PIP install "$H5CORO_PIN" "$HIDEFIX_PIN" "$MORTIE_SPEC" --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir

# Assert the derived spec actually landed: mortie is the one dep whose spec is
# computed, and pip exits 0 on specs it skips (a PEP 508 marker that does not
# match the build environment is "Ignoring mortie: markers ... don't match"),
# which would ship a mortie-less layer past the size-only CI gates.
if ! ls "$OUTPUT_DIR/python" | grep -qE "^mortie-"; then
    echo "ERROR: mortie ('$MORTIE_SPEC') missing from layer - check the spec in pyproject.toml!"; exit 1
fi

# async-tiff (issue #218): the raster worker's GeoTIFF/COG decode engine
# (mode="process_raster"). abi3 manylinux_2_28 wheel (~4 MB) on both arches;
# its only dep is obspec (pure-python, tiny). The pin comes from the `lambda`
# extra (lambda_pin above).
echo "Installing async-tiff ($ASYNC_TIFF_PIN, +obspec)..."
$PIP install "$ASYNC_TIFF_PIN" obspec -t "$OUTPUT_DIR/python" --no-cache-dir

# icechunk (issue #580): the worker-side Icechunk companion-repo writer
# (mode="icechunk_init" + the per-leaf refs commit). cp312-abi3 manylinux
# wheel (~17 MB) on both arches; --no-deps because its only dep, zarr, is a
# function-zip dep (build_function.sh). The pin comes from the `lambda` extra
# (lambda_pin above).
echo "Installing icechunk ($ICECHUNK_PIN, --no-deps)..."
$PIP install "$ICECHUNK_PIN" --no-deps -t "$OUTPUT_DIR/python" --no-cache-dir

# Verify numpy stayed < 2.3
NUMPY_VERSION=$(ls "$OUTPUT_DIR/python" | grep -E "^numpy-" | head -1)
echo "Installed: $NUMPY_VERSION"
if [[ "$NUMPY_VERSION" == *"2.3"* ]]; then
    echo "ERROR: numpy 2.3.x installed - this breaks Lambda!"; exit 1
fi

# Remove bloat. pyproj is intentionally NOT stripped (rectilinear/odc-geo assign
# needs it). pyarrow is no longer installed (issue #130): the worker's Arrow carrier
# is arro3-core, which needs no component strip, so the whole pyarrow strip block is
# gone with it. xarray is intentionally NOT stripped (issue #220): the temporal
# worker (process_event) imports it and opens NetCDF bytes via h5netcdf/h5py --
# the pins live in the [lambda] extra.
echo "Removing bloat..."
rm -rf "$OUTPUT_DIR/python"/matplotlib* \
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
