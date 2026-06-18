"""Real-data region timing benchmark for the ATL03 aggregation handoff (issue #30).

This is the real-data (phase-3) counterpart to the synthetic ``handoff_bench.py``:
it drives the actual catalog -> shard-map -> ``runner.agg()`` path against
ICESat-2 ATL03 granules over three 10 km x 10 km AOIs, swept across two date
ranges (calendar 2025 and the full 2018 -> Jan 2026 mission), and records
wall-time, peak RSS, observation/cell counts, and output bytes for the
``pandas`` and ``arrow`` handoff carriers -- asserting their scalar outputs are
byte-for-byte identical (#30's parity criterion on real data).

Three regions span the density regimes the synthetic harness can't reproduce:

  * ``neon_maryland``   -- NEON site, land/vegetation (moderate photon return)
  * ``russell_glacier`` -- Kangerlussuaq / Russell Glacier, on the ice (high return)
  * ``bahamas``         -- shallow-water bathymetry (sparse, attenuated subsurface)

The optional ``--hard`` flag adds an ``antarctica_88s`` AOI at 88 deg S, where RGT
convergence maximizes per-cell overlap (the worst case for the old mask loop). It
is deferred / behind the flag by default (per @espg) because it is far more
expensive than the other three.

Confidence filter and grid come straight from the shipped ``atl03`` template:
``signal_conf_ph != -2`` (drop only TEP photons, across all surface types),
rectilinear grid. The ``output.grid.bounds`` are overridden per region to the
AOI's 10 km box so the grid covers just the patch. (HEALPix order-19 -- the
~10 m match -- waits on mortie #35, so this first pass is rectilinear only.)

Phase 3 adds a second template sweep -- ``atl03_waveform_counts`` -- alongside
the scalar ``atl03`` run: wall-times for both templates are printed side-by-side
so the overhead of the 128-bin vector histogram over plain scalars is visible.
The waveform store is also checked to confirm the ``waveform_counts`` array has
the expected trailing shape of 128.

**This script is NOT run in CI**: it needs ``earthaccess``/NSIDC-S3 credentials
(CMR-STAC query + byte-range HDF5 reads) and is slow. It lives under
``benchmarks/`` (not ``tests/``) and is meant for a credentialed session. It must
import and construct cleanly and stay ruff-clean regardless.

Run (in a credentialed session)::

    uv run python benchmarks/region_timing.py --windows 1y --max-cells 4
    uv run python benchmarks/region_timing.py --hard --out results.txt
"""

import argparse
import copy
import resource
import sys
import tempfile
import time
from dataclasses import dataclass

import numpy as np

from zagg.config import default_config, get_data_vars
from zagg.runner import agg

# Time windows (per @espg). Each is an explicit ``(start, end)`` date range
# (inclusive start, exclusive end) driving the catalog query. The AOI only has
# ~10 granules for a whole year, so the earlier {1d,5d,15d,30d,91d} sweep was too
# sparse to be meaningful; we benchmark two ranges instead:
#
#   * ``1y``  -- calendar 2025 (2025-01-01 .. 2026-01-01).
#   * ``all`` -- the full mission, 2018-01-01 .. 2026-01-01. The upper bound is
#     CLIPPED at 2026-01-01 so future runs stay stable and don't drift as new
#     granules land (otherwise benchmark numbers would creep every run).
WINDOWS = {
    "1y": ("2025-01-01", "2026-01-01"),
    "all": ("2018-01-01", "2026-01-01"),
}

# Carriers compared head-to-head. Both must produce identical scalar outputs.
HANDOFFS = ("pandas", "arrow")


@dataclass(frozen=True)
class Region:
    """A 10 km x 10 km AOI, as a ``(lon_min, lat_min, lon_max, lat_max)`` bbox."""

    name: str
    lon_min: float
    lat_min: float
    lon_max: float
    lat_max: float

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        return (self.lon_min, self.lat_min, self.lon_max, self.lat_max)


def _box(name: str, lon: float, lat: float) -> Region:
    """A ~10 km x 10 km box centered on (lon, lat).

    0.09 deg of latitude is ~10 km; the longitude span is scaled by
    ``1/cos(lat)`` so the east-west extent stays ~10 km at high latitude.
    """
    half_lat = 0.045
    half_lon = 0.045 / max(np.cos(np.radians(lat)), 1e-3)
    return Region(name, lon - half_lon, lat - half_lat, lon + half_lon, lat + half_lat)


# Region centers. Maryland NEON (NEON SCBI/SERC area), Russell Glacier on the ice
# east of Kangerlussuaq, and a Bahamas bank for bathymetry.
REGIONS = [
    _box("neon_maryland", lon=-76.56, lat=38.89),
    _box("russell_glacier", lon=-50.0, lat=67.09),
    _box("bahamas", lon=-76.0, lat=24.0),
]

# Deferred / behind --hard: 88 deg S, where RGT convergence maximizes overlap.
HARD_REGION = _box("antarctica_88s", lon=0.0, lat=-88.0)


@dataclass
class Record:
    """One (region x window x template x handoff) measurement."""

    region: str
    window: str
    template: str
    handoff: str
    wall_s: float
    peak_rss_mb: float
    total_obs: int
    cells_with_data: int
    output_bytes: int


def _peak_rss_mb() -> float:
    """Process peak resident set size in MB (ru_maxrss is KB on Linux, bytes on macOS)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 1e3 if sys.platform != "darwin" else rss / 1e6


def _region_config(region: Region):
    """An ``atl03`` config whose rectilinear grid is clipped to the region bbox."""
    cfg = default_config("atl03")
    cfg = copy.deepcopy(cfg)
    cfg.output["grid"]["bounds"] = list(region.bbox)
    return cfg


def _region_waveform_config(region: Region):
    """An ``atl03_waveform_counts`` config clipped to the region bbox."""
    cfg = default_config("atl03_waveform_counts")
    cfg = copy.deepcopy(cfg)
    cfg.output["grid"]["bounds"] = list(region.bbox)
    return cfg


def _store_arrays(store_path: str, cfg) -> dict[str, np.ndarray]:
    """Read each aggregated data variable out of a written store as a dense array.

    Works for both scalar (1-D) and vector (N-D) variables; the trailing
    dimension(s) are preserved so callers can check shapes.
    """
    import zarr

    from zagg.grids import from_config

    group_path = from_config(cfg).group_path
    grp = zarr.open_group(store_path, mode="r")
    return {name: np.asarray(grp[f"{group_path}/{name}"][:]) for name in get_data_vars(cfg)}


def _assert_parity(a: dict[str, np.ndarray], b: dict[str, np.ndarray], ctx: str) -> None:
    """Assert two stores' outputs are byte-for-byte identical (NaN-aware for floats)."""
    assert a.keys() == b.keys(), f"{ctx}: variable mismatch {a.keys()} vs {b.keys()}"
    for name in a:
        x, y = a[name], b[name]
        if not np.array_equal(x, y, equal_nan=np.issubdtype(x.dtype, np.floating)):
            raise AssertionError(f"{ctx}: '{name}' differs between pandas and arrow")


def _output_bytes(store_path: str) -> int:
    """Total bytes written under a local store directory."""
    import os

    total = 0
    for root, _dirs, files in os.walk(store_path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def run_one(
    region: Region,
    window: str,
    date_range: tuple[str, str],
    *,
    version: str,
    max_cells: int | None,
    max_workers: int | None,
) -> list[Record]:
    """Build the catalog for (region, window), run both carriers, assert parity."""
    from zagg.catalog import make_shardmap
    from zagg.catalog.sources import Query
    from zagg.grids import from_config

    cfg = _region_config(region)
    start, end = date_range
    query = Query("ATL03", version, start, end, region=region.bbox)

    with tempfile.TemporaryDirectory() as tmp:
        catalog_path = f"{tmp}/shardmap.json"
        grid = from_config(cfg)
        make_shardmap(query, grid).to_json(catalog_path)

        records: list[Record] = []
        arrays: dict[str, dict] = {}
        for handoff in HANDOFFS:
            store_path = f"{tmp}/out_{handoff}.zarr"
            t0 = time.perf_counter()
            summary = agg(
                cfg,
                catalog=catalog_path,
                store=store_path,
                handoff=handoff,
                max_cells=max_cells,
                max_workers=max_workers,
                overwrite=True,
            )
            wall = time.perf_counter() - t0
            arrays[handoff] = _store_arrays(store_path, cfg)
            records.append(
                Record(
                    region=region.name,
                    window=window,
                    template="atl03",
                    handoff=handoff,
                    wall_s=wall,
                    peak_rss_mb=_peak_rss_mb(),
                    total_obs=int(summary.get("total_obs", 0)),
                    cells_with_data=int(summary.get("cells_with_data", 0)),
                    output_bytes=_output_bytes(store_path),
                )
            )
        _assert_parity(
            arrays["pandas"],
            arrays["arrow"],
            ctx=f"{region.name}/{window}/atl03",
        )
    return records


def run_one_waveform(
    region: Region,
    window: str,
    date_range: tuple[str, str],
    *,
    version: str,
    max_cells: int | None,
    max_workers: int | None,
) -> list[Record]:
    """Run the ``atl03_waveform_counts`` template for (region, window).

    Runs both carriers and asserts parity (both produce identical vector output).
    Also confirms the ``waveform_counts`` array carries the expected 128-element
    trailing dimension so the shape contract is exercised on real data.
    """
    from zagg.catalog import make_shardmap
    from zagg.catalog.sources import Query
    from zagg.grids import from_config

    cfg = _region_waveform_config(region)
    start, end = date_range
    query = Query("ATL03", version, start, end, region=region.bbox)

    with tempfile.TemporaryDirectory() as tmp:
        catalog_path = f"{tmp}/shardmap.json"
        grid = from_config(cfg)
        make_shardmap(query, grid).to_json(catalog_path)

        records: list[Record] = []
        arrays: dict[str, dict] = {}
        for handoff in HANDOFFS:
            store_path = f"{tmp}/wf_{handoff}.zarr"
            t0 = time.perf_counter()
            summary = agg(
                cfg,
                catalog=catalog_path,
                store=store_path,
                handoff=handoff,
                max_cells=max_cells,
                max_workers=max_workers,
                overwrite=True,
            )
            wall = time.perf_counter() - t0
            arrays[handoff] = _store_arrays(store_path, cfg)
            records.append(
                Record(
                    region=region.name,
                    window=window,
                    template="atl03_waveform",
                    handoff=handoff,
                    wall_s=wall,
                    peak_rss_mb=_peak_rss_mb(),
                    total_obs=int(summary.get("total_obs", 0)),
                    cells_with_data=int(summary.get("cells_with_data", 0)),
                    output_bytes=_output_bytes(store_path),
                )
            )
        _assert_parity(
            arrays["pandas"],
            arrays["arrow"],
            ctx=f"{region.name}/{window}/atl03_waveform_counts",
        )
        # Shape check: waveform_counts must carry the 128-element trailing dim.
        wf = arrays["pandas"]["waveform_counts"]
        if wf.ndim != 2 or wf.shape[1] != 128:
            raise AssertionError(
                f"{region.name}/{window}: waveform_counts shape {wf.shape} expected (..., 128)"
            )
    return records


def format_table(records: list[Record]) -> str:
    """Render the collected records as a fixed-width text table."""
    header = (
        f"{'region':<18}{'window':>8}{'template':>16}{'handoff':>10}{'wall_s':>10}"
        f"{'rss_MB':>10}{'obs':>12}{'cells':>10}{'out_MB':>10}"
    )
    lines = [header, "-" * len(header)]
    for r in records:
        lines.append(
            f"{r.region:<18}{r.window:>8}{r.template:>16}{r.handoff:>10}{r.wall_s:>10.3f}"
            f"{r.peak_rss_mb:>10.1f}{r.total_obs:>12,}{r.cells_with_data:>10,}"
            f"{r.output_bytes / 1e6:>10.2f}"
        )
    return "\n".join(lines)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--windows",
        default=",".join(WINDOWS),
        help="Comma-separated time windows (default: both). Choices: " + ", ".join(WINDOWS),
    )
    ap.add_argument(
        "--hard",
        action="store_true",
        help="Also run the deferred 88 deg S max-overlap AOI (slow).",
    )
    ap.add_argument(
        "--max-cells",
        type=int,
        default=None,
        help="Cap cells processed per run (for a quick smoke).",
    )
    ap.add_argument("--max-workers", type=int, default=None, help="Local worker cap.")
    ap.add_argument("--version", default="007", help="ATL03 product version (default: 007).")
    ap.add_argument(
        "--out",
        default=None,
        help="Also append the results table to this file.",
    )
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    windows = []
    for w in args.windows.split(","):
        w = w.strip()
        if w not in WINDOWS:
            raise SystemExit(f"unknown window {w!r}; choices: {', '.join(WINDOWS)}")
        windows.append(w)

    regions = list(REGIONS) + ([HARD_REGION] if args.hard else [])

    records: list[Record] = []
    for region in regions:
        for window in windows:
            records.extend(
                run_one(
                    region,
                    window,
                    WINDOWS[window],
                    version=args.version,
                    max_cells=args.max_cells,
                    max_workers=args.max_workers,
                )
            )
            records.extend(
                run_one_waveform(
                    region,
                    window,
                    WINDOWS[window],
                    version=args.version,
                    max_cells=args.max_cells,
                    max_workers=args.max_workers,
                )
            )

    table = format_table(records)
    print(table)
    if args.out:
        with open(args.out, "a") as f:
            f.write(table + "\n")


if __name__ == "__main__":
    main()
