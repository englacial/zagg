"""Generate the Antarctic latitude-ring AOIs the 88S sweep benchmarks over (issue #148).

The 88S stress target has always been one ring — ``antarctic_88s.geojson``, the
band ``[-88.0, -87.7]`` at the ICESat-2 turning latitude. The reassessment asks
for a *sweep* of them (85, 85.5, 86, 86.5, 87, 87.5, 88 degrees south), so the
ring stops being a one-off fixture and becomes a family: one band per latitude,
all built to the same rules. Those rules are non-obvious enough that hand-editing
seven geojsons would be the wrong way to get them, so they live here:

* **The band is ``[-L, -L+0.3]``** — anchored on the named latitude and 0.3 deg
  tall, equatorward. That is the band the committed 88S ring uses, and the band
  every granule count on issue #148 was measured over, so the sweep rows are
  comparable to the pinned 88S row only if the whole family keeps it.
* **A full-longitude ring MUST be sectorized.** A single ``-180..180`` rectangle
  collapses under spherical polygon fill: mortie traces its edges as great
  circles, so coverage degenerates to an antimeridian sliver (10 cells instead of
  ~564 at o9). The ring is therefore a MultiPolygon of eight 45-deg sectors.
* **Vertices every 1 deg of longitude.** A sector's north and south edges are
  parallels, not great circles; sampling them densely keeps the filled band from
  bowing poleward between vertices.

The eight sector seams land on multiples of 45 deg, which is also the HEALPix
base-cell boundary — the geometry that ``espg/mortie#103`` mis-fills. That is a
property of the committed 88S ring, kept here deliberately so the sweep bands
are the same shape as the row they extend rather than quietly a different one;
the analysis on issue #148 works around it by enumerating ring cells through
``grid.assign``/``grid.shards_of`` instead of ``coverage()``. Moving the seams
off the base-cell grid (a 22.5 deg offset makes mortie's fill match point-sampled
ground truth) would re-pin the committed 88S row, so it is not this script's
call to make.

Run from a zagg checkout::

    uv run python tools/make_lat_ring_aoi.py 85 85.5 86 86.5 87 87.5
    uv run python tools/make_lat_ring_aoi.py --check 88

``--check`` compares the generated geometry against the committed file and
writes nothing — which is how the committed 88S ring stays the reference the
rest of the family is generated to match. A plain run refuses to replace a
committed band whose bytes differ (``--force`` is the way through): the shipped
88S ring's prose is not what this script writes, and its geometry is, so an
overwrite would pass every test while deleting the note issue #148 leans on.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "tests" / "data" / "benchmark"

#: Band height in degrees of latitude, equatorward of the named latitude. The
#: committed 88S ring is ``[-88.0, -87.7]``; every issue #148 granule count is
#: measured over a band this tall, so the sweep rows only compare to the pinned
#: 88S row while the whole family shares it.
BAND_DEG = 0.3

#: Sector count for the full-longitude ring. Eight 45-deg sectors, because one
#: ``-180..180`` rectangle collapses to an antimeridian sliver under spherical
#: polygon fill (see the module docstring).
N_SECTORS = 8

#: Longitude spacing of the sampled vertices along a sector's parallels.
LON_STEP_DEG = 1.0


def band_slug(lat: float) -> str:
    """The filename stem's latitude part: ``85.5 -> '85_5'``, ``88.0 -> '88'``.

    A period is legal in a filename but reads as an extension boundary, so the
    fractional bands take the underscore the committed naming already implies
    (``antarctic_88s.geojson`` has no separator to be consistent with).
    """
    text = f"{lat:g}"
    return text.replace(".", "_")


def aoi_filename(lat: float) -> str:
    """The committed geojson name for a band, e.g. ``antarctic_86_5s.geojson``."""
    return f"antarctic_{band_slug(lat)}s.geojson"


def sector_ring(
    lon_beg: float, lon_end: float, lat_south: float, lat_north: float
) -> list[list[float]]:
    """One sector's closed outer ring: east along the south edge, west back.

    Both edges are sampled every ``LON_STEP_DEG`` so the filled band tracks the
    parallels instead of bowing poleward between distant vertices. The spacing is
    the module constant rather than an argument: the only thing a per-call step
    could do here is round to a different vertex count than the family shares.
    """
    n = round((lon_end - lon_beg) / LON_STEP_DEG)
    lons = [lon_beg + i * LON_STEP_DEG for i in range(n + 1)]
    ring = [[lon, lat_south] for lon in lons]
    ring += [[lon, lat_north] for lon in reversed(lons)]
    ring.append([lon_beg, lat_south])
    return ring


def ring_geometry(lat: float, band: float = BAND_DEG) -> dict:
    """The MultiPolygon for the band ``[-lat, -lat + band]``, as ``N_SECTORS`` sectors."""
    lat_south, lat_north = -abs(lat), -abs(lat) + band
    width = 360.0 / N_SECTORS
    return {
        "type": "MultiPolygon",
        "coordinates": [
            [sector_ring(-180.0 + i * width, -180.0 + (i + 1) * width, lat_south, lat_north)]
            for i in range(N_SECTORS)
        ],
    }


def ring_feature_collection(lat: float, band: float = BAND_DEG) -> dict:
    """The full geojson document for one band, properties and all."""
    lat_north = -abs(lat) + band
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": f"Antarctic {abs(lat):g}S ring "
                    f"(ICESat-2 latitude sweep band, issue #148)",
                    "note": (
                        f"Latitude band [{-abs(lat):g}, {lat_north:g}] deg, one row of the "
                        "88S reassessment sweep: the same 0.3-deg band as the pinned 88S "
                        "stress ring, stepped equatorward, so granule counts across the "
                        "sweep are comparable. Generated by tools/make_lat_ring_aoi.py -- "
                        f"a MultiPolygon of {N_SECTORS} 45-deg sectors with vertices every "
                        f"{LON_STEP_DEG:g} deg of longitude, because a single lat/lon "
                        "rectangle spanning -180..180 collapses under spherical polygon "
                        "fill (mortie traces its edges as great circles, leaving an "
                        "antimeridian sliver). HEALPix is CRS-agnostic; a rectilinear "
                        "analog at this latitude would need a polar CRS (e.g. EPSG:3031). "
                        "See issues #121 / #148."
                    ),
                    "source_crs": "EPSG:4326",
                },
                "geometry": ring_geometry(lat, band),
            }
        ],
    }


def dumps(doc: dict) -> str:
    """Serialize like the committed ``antarctic_88s.geojson``: prose readable, coordinates compact.

    The properties block is worth reading in a diff; the ~750 coordinate pairs
    are not, and one line per pair would make the file 20x longer for nothing.
    """
    feature = doc["features"][0]
    props = json.dumps(feature["properties"], indent=2)
    geometry = json.dumps(feature["geometry"], separators=(",", ":"))
    return (
        '{\n"type": "FeatureCollection",\n"features": [\n{\n"type": "Feature",\n'
        f'"properties": {props},\n"geometry": {geometry}\n}}\n]\n}}\n'
    )


def geometry_of(path: Path) -> dict:
    """The single feature's geometry from a committed ring geojson."""
    return json.loads(path.read_text())["features"][0]["geometry"]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "latitudes",
        nargs="+",
        type=float,
        help="band latitudes in degrees SOUTH, e.g. 85 85.5 86 86.5 87 87.5 88",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="compare the generated geometry against the committed file; write nothing.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=BENCH,
        help="where the geojsons land (default: tests/data/benchmark).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace a committed band whose bytes differ (it loses that file's prose).",
    )
    args = parser.parse_args(argv)

    failed = False
    for lat in args.latitudes:
        path = args.out_dir / aoi_filename(lat)
        doc = ring_feature_collection(lat)
        if args.check:
            if not path.exists():
                print(f"{path.name}: MISSING")
                failed = True
            elif geometry_of(path) == doc["features"][0]["geometry"]:
                print(f"{path.name}: geometry matches")
            else:
                print(f"{path.name}: GEOMETRY DIFFERS from the generator")
                failed = True
            continue
        text = dumps(doc)
        # Refuse to clobber a committed band. The shipped 88S ring is the
        # reference this whole family is generated to match, and its prose is
        # NOT what the generator writes -- it names the turning-latitude stress
        # rationale, which the generic sweep-band note does not. Since the
        # geometry is identical, an overwrite would pass every test in the suite
        # while quietly deleting that. Silence on an unchanged file keeps the
        # writer idempotent; --force is the deliberate way through.
        if path.exists() and path.read_text() != text and not args.force:
            print(f"{path.name}: EXISTS and differs -- re-run with --force to replace it")
            failed = True
            continue
        path.write_text(text)
        print(
            f"wrote {path.name} ({N_SECTORS} sectors, band [{-abs(lat):g}, "
            f"{-abs(lat) + BAND_DEG:g}])"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
