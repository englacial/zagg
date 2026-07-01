# sliderule C++ H5Coro shim — build recipe + measured rows (issue #149, phase 3)

A benchmark-only pybind11 module (`bench/h5coro/shim/`) over sliderule's C++
H5Coro, replaying the frozen o10/o9 workloads through `bench_replay.py
--adapter shim` under the same sha256 correctness gate as every other variant.
Everything builds and runs in a podman linux/arm64 container (`build.sh all`);
the host sliderule clone is mounted read-only and never modified.

## Build recipe (summary)

- Image `zagg-bench-shim` (`Containerfile.shim`): the replay image
  (python:3.12-slim + numpy + h5coro==1.0.4) + cmake/g++/make/patch,
  liblua5.3-dev, libcurl4-openssl-dev, rapidjson-dev, libreadline-dev,
  zlib1g-dev, uuid-dev, and pip pybind11.
- sliderule v5.4.3 (`ce4be309`, 2026-06-16) built minimal, out-of-tree in a
  scratch mount: `-DSHARED_LIBRARY=ON -DCMAKE_BUILD_TYPE=Release
  -DH5CORO_THREAD_POOL_SIZE=4 -DUSE_H5CORO_PACKAGE=ON
  -DUSE_STREAMING_PACKAGE=ON` and OFF for aws/arrow/geo/cre/las/ccsds/legacy/
  hdf packages, all `USE_*_DATASET` options, and the slideruleearth target.
  Release matters: with a `.git` present the tree defaults to Debug, which
  turns on clang-tidy/cppcheck. Only the `slideruleLib` target is built.
- Streaming cannot be toggled off: core's `LuaLibrarySys` and h5coro's
  `H5DatasetDevice` use `DeviceObject` from the streaming package
  unconditionally (the package itself has no external deps).
- The shim (`shim.cpp`, ~180 lines) links `libsliderule.so` directly and calls
  `initcore()` + `inith5coro()` at import, then serial `H5Coro::read` calls
  (GIL released around each), one `H5Coro::Context` per granule.

## Workarounds (all in `shim/sliderule-minimal-build.patch`, applied with `patch -p1` to the container-side copy)

1. `Asset.h` — the `Asset` constructor and `attributes_t` are private
   (Lua-only construction upstream); the shim constructs one directly with
   `lua_State* == NULL` (LuaObject tolerates NULL). Benchmark-only change.
2. `core/CMakeLists.txt` — `OutputLib.cpp` uses `uuid/uuid.h` but upstream
   only links libuuid via the (disabled) geo package; link it in core.
3. `DeduplicateRunner.cpp` — unguarded `#include "GeoLib.h"` (geo pkg, unused).
4. `EndpointObject.cpp` — unguarded `#include "SecretManager.h"` (aws pkg);
   all uses already sit under `#ifdef __aws__`.
5. `RequestParameters.cpp` — `samplers` (`FieldMap<GeoFields>`) is declared
   under `#ifdef __geo__` in the header but used unguarded in the .cpp; the
   three sampler lua methods, their meta-table entries, and the sampler
   branches of `luaExport`/`luaEncode` get the same guard.

Upstream clearly always builds with aws/geo ON — (2)-(5) are genuine minimal-
build gaps, not benchmark hacks. Also non-obvious: `RequestParameters.h`
(reached via `H5CoroLib.h`) consumes a `BUILDINFO` define normally injected by
the server application's CMake; the shim defines its own.

## valtype / dtype mapping

`H5Coro::read` is called with `RecordObject::DYNAMIC`, which skips the
INTEGER/REAL translation paths entirely — the returned buffer is the file's
stored type (verified in `H5CoroLib.cpp:460ff`). `info_t.datatype` maps
INT8/16/32/64 -> i1/i2/i4/i8, UINT8/16/32/64 -> u1/u2/u4/u8, FLOAT -> f4,
DOUBLE -> f8, TIME8 -> i8; anything else raises. Shape comes from
`info_t.shape[3]` (trailing zeros = unused dims), so full 1-D reads give
`(n,)` and row-sliced `signal_conf_ph` gives `(rows, 5)` — a 1-D
`[start, end)` slice with `slicendims=1` leaves dim 1 full-range inside
`H5Dataset` (dims beyond `slicendims` default to `[0, EOR)`). Data is copied
into numpy and the H5Coro buffer freed with
`operator delete[](p, std::align_val_t(8))`, matching the allocation.
Checksums (dtype + shape + bytes) matched the phase-1 h5coro-1.0.4 baseline
on every array of both workloads on the first gated run — no dtype/shape
iteration was needed after the single-granule smoke test passed.

## Footprint (the Lambda layer-size datum)

- `h5shim.cpython-312-aarch64-linux-gnu.so`: **200,632 B (0.19 MiB)**
- `libsliderule.so.5.4.3` (minimal build): **2,064,320 B (1.97 MiB)**
- Non-default direct deps of libsliderule (Debian arm64 sizes): liblua5.3
  0.25 MiB, libcurl 0.94 MiB (plus its TLS/krb/ldap chain, several MiB),
  libreadline 0.40 MiB, libuuid 0.06 MiB, libz 0.13 MiB.

So the read path itself is ~2.2 MiB; lua/curl/readline are dragged in by
core's Lua/HTTP machinery, which the read path never exercises — a production
integration could plausibly stub those out, but as-built the honest bundle is
roughly 3–5 MiB plus whatever of curl's chain the base image lacks. Either
way it is far under the 250 MB layer gate.

## Measured rows (gated, linux/arm64 container, serial, no other load)

| variant                        | o10 wall s | o9 wall s | correctness |
|--------------------------------|-----------:|----------:|-------------|
| h5coro-1.0.4                   |       93.5 |     129.0 | pass        |
| h5coro-numpy                   |       24.5 |      34.0 | pass        |
| hidefix-0.12.0                 |       16.0 |      24.0 | pass        |
| sliderule-cpp-shim (this)      |       10.3 |      14.2 | pass        |

Rows: `replay_o10_sliderule-cpp-shim-linux-arm64.json` (wall 10.3 s, cpu
9.4 s, rss 187 MB, 2212 arrays) and `replay_o9_...json` (wall 14.2 s, cpu
13.2 s, rss 208 MB, 3528 arrays); gate `pass` with no missing keys on both.
An earlier identically-configured run measured 11.7 s / 16.0 s (cooler page
cache), so treat ~±15% as run-to-run spread; the ordering vs the other
variants is far outside it. Net: the C++ engine is ~9x the shipped pure-Python
h5coro 1.0.4, ~2.4x the numpy-patched variant, and ~1.5–1.7x hidefix on these
workloads.
