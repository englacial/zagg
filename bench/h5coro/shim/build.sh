#!/usr/bin/env bash
# Reproducible build + gated replay for the sliderule C++ H5Coro shim
# (phase 3 of issue #149). Runs everything inside a podman linux/arm64
# container; the host sliderule clone is mounted read-only and never touched.
#
#   bash bench/h5coro/shim/build.sh all        # image + build + smoke + replay
#   bash bench/h5coro/shim/build.sh image      # deps image (Containerfile.shim)
#   bash bench/h5coro/shim/build.sh build      # sliderule (minimal) + shim .so
#   bash bench/h5coro/shim/build.sh smoke      # h5shim vs h5coro on one granule
#   bash bench/h5coro/shim/build.sh replay     # gated rows for o10 + o9
#
# Env overrides: SLIDERULE_SRC (default ~/software/sliderule), GRANULE_DIR
# (default ~/ignore/zagg_neon_atl03_test_shard/granules), SHIM_BUILD_DIR
# (writable scratch for the out-of-tree build; default $TMPDIR/zagg-shim-build).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"     # bench/h5coro/shim
REPO="$(cd "$HERE/../../.." && pwd)"      # zagg repo root
SLIDERULE_SRC="${SLIDERULE_SRC:-$HOME/software/sliderule}"
GRANULE_DIR="${GRANULE_DIR:-$HOME/ignore/zagg_neon_atl03_test_shard/granules}"
BUILD_DIR="${SHIM_BUILD_DIR:-${TMPDIR:-/tmp}/zagg-shim-build}"
IMAGE=zagg-bench-shim

mkdir -p "$BUILD_DIR"

run() { # run a command in the shim container (granules mounted ro when present)
    local vols=(-v "$SLIDERULE_SRC":/sliderule-src:ro -v "$BUILD_DIR":/build -v "$REPO":/work)
    [ -d "$GRANULE_DIR" ] && vols+=(-v "$GRANULE_DIR":/granules:ro)
    podman run --rm "${vols[@]}" \
        -e PYTHONPATH=/build/shim-build -e LD_LIBRARY_PATH=/build/sr-build \
        "$IMAGE" "$@"
}

cmd_image() {
    podman build -t "$IMAGE" -f "$HERE/Containerfile.shim" "$HERE"
}

cmd_build() {
    # container-side build script (regenerated each run; lives in the scratch mount)
    cat > "$BUILD_DIR/container_build.sh" <<'EOF'
set -euxo pipefail

# 1. writable copy of the clone (the mount is ro), then the consolidated
#    workaround patch: Asset ctor exposed for the shim + four minimal-build
#    fixes (core does not compile with the aws/geo packages OFF upstream).
#    Rationale for each hunk is in the patch header.
rm -rf /build/sliderule
mkdir -p /build/sliderule
tar -C /sliderule-src --exclude=.git -cf - . | tar -C /build/sliderule -xf -
patch -p1 -d /build/sliderule < /work/bench/h5coro/shim/sliderule-minimal-build.patch

# 2. minimal sliderule: core + streaming + h5coro, shared lib, Release (the
#    default build type is Debug when .git exists, which turns on
#    clang-tidy/cppcheck). Streaming must stay ON: core's LuaLibrarySys and
#    h5coro's H5DatasetDevice use DeviceObject from the streaming package
#    unconditionally; the package itself has no external deps.
#    H5CORO_THREAD_POOL_SIZE=4 keeps the reader pool small (default is 128;
#    the replay issues serial H5Coro::read calls and never touches the pool).
cmake -S /build/sliderule -B /build/sr-build \
    -DCMAKE_BUILD_TYPE=Release -DSHARED_LIBRARY=ON \
    -DH5CORO_THREAD_POOL_SIZE=4 \
    -DUSE_H5CORO_PACKAGE=ON -DUSE_STREAMING_PACKAGE=ON \
    -DUSE_AWS_PACKAGE=OFF -DUSE_ARROW_PACKAGE=OFF -DUSE_GEO_PACKAGE=OFF \
    -DUSE_CRE_PACKAGE=OFF -DUSE_LAS_PACKAGE=OFF \
    -DUSE_CCSDS_PACKAGE=OFF -DUSE_LEGACY_PACKAGE=OFF -DUSE_HDF_PACKAGE=OFF \
    -DUSE_BATHY_DATASET=OFF -DUSE_BLUETOPO_DATASET=OFF -DUSE_CASALS_DATASET=OFF \
    -DUSE_GEBCO_DATASET=OFF -DUSE_GEDI_DATASET=OFF -DUSE_ICESAT2_DATASET=OFF \
    -DUSE_LANDSAT_DATASET=OFF -DUSE_OPENDATA_DATASET=OFF -DUSE_PGC_DATASET=OFF \
    -DUSE_SWOT_DATASET=OFF -DUSE_USGS3DEP_DATASET=OFF -DUSE_GEDTM_DATASET=OFF \
    -DUSE_NISAR_DATASET=OFF -DUSE_SLIDERULEEARTH_TARGET=OFF
cmake --build /build/sr-build --target slideruleLib -j"$(nproc)"

# 3. the pybind11 shim, linked against the freshly built libsliderule
cmake -S /work/bench/h5coro/shim -B /build/shim-build \
    -DCMAKE_BUILD_TYPE=Release \
    -DSLIDERULE_SRC=/build/sliderule -DSLIDERULE_BUILD=/build/sr-build \
    -DH5CORO_THREAD_POOL_SIZE=4 \
    -Dpybind11_DIR="$(python -c 'import pybind11; print(pybind11.get_cmake_dir())')"
cmake --build /build/shim-build -j"$(nproc)"

# 4. footprint datum for the report (Lambda layer sizing)
{
    echo '== shim + libsliderule sizes =='
    ls -l /build/shim-build/h5shim*.so /build/sr-build/libsliderule.so*
    echo '== ldd h5shim =='
    ldd /build/shim-build/h5shim*.so
    echo '== ldd libsliderule =='
    ldd /build/sr-build/libsliderule.so
} | tee /build/footprint.txt
EOF
    run bash /build/container_build.sh
}

cmd_smoke() {
    cat > "$BUILD_DIR/smoke.py" <<'EOF'
"""h5shim vs h5coro (same container) on one granule: full read + 2-D row slice."""
import os
import sys

import numpy as np

import h5shim
from h5coro import filedriver
from h5coro.h5coro import H5Coro

print("footprint:", h5shim.footprint())
gran = sorted(f for f in os.listdir("/granules") if f.endswith(".h5"))[0]
path = f"/granules/{gran}"
full = "/gt1l/geolocation/reference_photon_lat"
sliced = "/gt1l/heights/signal_conf_ph"

shim_out = h5shim.read(path, [(full, None, None), (sliced, 100, 300)])
h5 = H5Coro(path, filedriver.FileDriver, errorChecking=True, verbose=False)
promise = h5.readDatasets([full, {"dataset": sliced, "hyperslice": [(100, 300)]}], block=True)
ref = [np.asarray(promise[full]), np.asarray(promise[sliced])]

for name, got, want in zip((full, sliced), shim_out, ref):
    assert got.dtype == want.dtype, (name, got.dtype, want.dtype)
    assert got.shape == want.shape, (name, got.shape, want.shape)
    assert got.tobytes() == want.tobytes(), name
    print(f"OK {name}: dtype={got.dtype} shape={got.shape}")
print("smoke test passed on", gran)
EOF
    run python /build/smoke.py
}

cmd_replay() {
    for wl in o10 o9; do
        run python /work/bench/h5coro/bench_replay.py \
            --requests "/work/bench/h5coro/requests/$wl.json" \
            --granule-dir /granules \
            --adapter shim --variant sliderule-cpp-shim-linux-arm64 \
            --baseline "/work/bench/h5coro/results/checksums_$wl.json"
    done
}

case "${1:-all}" in
    image)  cmd_image ;;
    build)  cmd_build ;;
    smoke)  cmd_smoke ;;
    replay) cmd_replay ;;
    all)    cmd_image; cmd_build; cmd_smoke; cmd_replay ;;
    *) echo "usage: $0 [image|build|smoke|replay|all]" >&2; exit 1 ;;
esac
