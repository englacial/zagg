"""Build the CONUS (contiguous US, lower-48 + DC) boundary polygon (issue #202).

This is the *polygon reference* the CONUS cost-estimate shard map is built over
(leg 4a of issue #202). It is a documented public outline, not a survey product:
the 48 contiguous states plus DC, unioned into one MultiPolygon and cleaned.

Provenance
----------
Source: ``us-states.json`` from PublicaMundi/MappingAPI (a widely-used,
Census-derived, simplified US state outline; MIT-licensed). We keep every
feature except Alaska (FIPS ``02``), Hawaii (``15``) and Puerto Rico (``72``),
``unary_union`` the remainder, and ``buffer(0)`` to heal the seams between
adjacent state rings. No further simplification is applied (the source is
already coarse, ~800 vertices), so the outline is faithful to the source.

The result is written to ``conus.geojson`` (one Feature, MultiPolygon geometry)
with provenance recorded in the feature ``properties``. Area is reported in an
equal-area projection (EPSG:5070, CONUS Albers).

Run: ``python data/conus/build_conus_polygon.py`` (needs network for the source,
or pass ``--states-json`` a local copy).
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

import pyproj
import shapely
from shapely.geometry import mapping, shape
from shapely.ops import transform, unary_union

SOURCE_URL = (
    "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
)
# FIPS ids dropped to reduce the union to the contiguous 48 (+ DC, id 11).
NON_CONUS_FIPS = {"02", "15", "72"}  # Alaska, Hawaii, Puerto Rico
EQUAL_AREA_CRS = "EPSG:5070"  # NAD83 / CONUS Albers, metres


def load_states(states_json: str | None) -> dict:
    if states_json:
        return json.loads(Path(states_json).read_text())
    with urllib.request.urlopen(SOURCE_URL, timeout=60) as resp:  # noqa: S310 (trusted raw source)
        return json.loads(resp.read())


def build_conus(states: dict):
    geoms = [
        shape(f["geometry"]).buffer(0) for f in states["features"] if f["id"] not in NON_CONUS_FIPS
    ]
    conus = unary_union(geoms).buffer(0)
    return conus, len(geoms)


def area_km2(geom) -> float:
    to_albers = pyproj.Transformer.from_crs("EPSG:4326", EQUAL_AREA_CRS, always_xy=True).transform
    return transform(to_albers, geom).area / 1e6


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--states-json", default=None, help="Local us-states.json (else fetch SOURCE_URL)"
    )
    ap.add_argument(
        "--out",
        default=str(Path(__file__).with_name("conus.geojson")),
        help="Output GeoJSON path",
    )
    args = ap.parse_args(argv)

    states = load_states(args.states_json)
    conus, n_states = build_conus(states)
    area = area_km2(conus)
    bounds = [round(b, 6) for b in conus.bounds]
    n_parts = len(conus.geoms) if conus.geom_type == "MultiPolygon" else 1

    feature = {
        "type": "Feature",
        "properties": {
            "name": "CONUS (contiguous United States, lower-48 + DC)",
            "source": SOURCE_URL,
            "provenance": (
                "unary_union of the 48 contiguous states + DC from PublicaMundi/"
                "MappingAPI us-states.json (Census-derived simplified outline), "
                "Alaska/Hawaii/Puerto_Rico dropped, buffer(0) cleaned. No extra "
                "simplification."
            ),
            "area_km2_epsg5070": round(area, 1),
            "bbox_lonlat": bounds,
            "n_source_states": n_states,
            "n_parts": n_parts,
            "issue": 202,
        },
        "geometry": mapping(conus),
    }
    fc = {"type": "FeatureCollection", "features": [feature]}
    Path(args.out).write_text(json.dumps(fc))
    print(f"wrote {args.out}: {n_parts} part(s), bbox={bounds}, area={area:,.0f} km^2")
    print(f"shapely {shapely.__version__}; source states kept={n_states}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
