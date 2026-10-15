"""The Antarctic latitude-ring AOI family the 88S sweep runs over (issue #148).

``tools/make_lat_ring_aoi.py`` generates the sweep bands; the committed
``antarctic_88s.geojson`` is the reference it must reproduce, because that ring
is what every granule count on issue #148 was measured over. The tests here are
the acceptance side of that: the generator reproduces the reference exactly, and
every band it emits carries the invariants that make a full-longitude ring
usable at all -- sectorized (a single ``-180..180`` rectangle collapses to an
antimeridian sliver under spherical polygon fill), densely sampled along its
parallels, and anchored on the same 0.3-deg band.

Offline and fast: geometry only, no CMR, no grid fill, no AWS.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from zagg.catalog import load_polygon, polygon_to_bbox

REPO = Path(__file__).resolve().parents[1]
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(REPO / "tools"))

import make_lat_ring_aoi as ring  # noqa: E402

#: The reassessment sweep's bands, in degrees SOUTH. 88 is the pinned stress
#: ring that already shipped; the other six extend it equatorward.
SWEEP_LATS = [85.0, 85.5, 86.0, 86.5, 87.0, 87.5, 88.0]


def test_generator_reproduces_the_committed_88s_ring():
    """The committed stress ring IS the generator's reference output.

    If this fails the family has silently split in two: the pinned 88S row would
    be a different shape from the sweep rows extending it, and their granule
    counts would stop being comparable -- which is the whole point of holding the
    band height and the sectorization fixed across the sweep.
    """
    committed = ring.geometry_of(BENCH / "antarctic_88s.geojson")
    assert committed == ring.ring_geometry(88.0)


@pytest.mark.parametrize("lat", SWEEP_LATS)
def test_every_sweep_band_is_committed_and_loads(lat):
    """Each band exists, parses through ``load_polygon``, and covers its own band."""
    path = BENCH / ring.aoi_filename(lat)
    assert path.exists(), f"{path.name} is missing"
    parts = load_polygon(str(path))
    assert len(parts) == ring.N_SECTORS
    xmin, ymin, xmax, ymax = polygon_to_bbox(parts)
    assert (xmin, xmax) == (-180.0, 180.0)
    assert ymin == pytest.approx(-lat)
    assert ymax == pytest.approx(-lat + ring.BAND_DEG)


@pytest.mark.parametrize("lat", SWEEP_LATS)
def test_band_is_sectorized_into_a_seamless_ring(lat):
    """Eight sectors tile ``-180..180`` exactly: no gap, no overlap, none full-width.

    A sector spanning the whole ring is the failure mode this shape exists to
    avoid (mortie traces the rectangle's edges as great circles and the fill
    degenerates to an antimeridian sliver -- 10 cells instead of ~564 at o9), so
    the width assertion is load-bearing, not decorative.
    """
    geometry = ring.geometry_of(BENCH / ring.aoi_filename(lat))
    spans = []
    for polygon in geometry["coordinates"]:
        lons = [lon for lon, _ in polygon[0]]
        spans.append((min(lons), max(lons)))
    spans.sort()
    assert spans[0][0] == -180.0 and spans[-1][1] == 180.0
    for (_, prev_end), (start, _) in zip(spans, spans[1:]):
        assert start == prev_end, "sector seams must meet exactly"
    for start, end in spans:
        assert end - start == pytest.approx(360.0 / ring.N_SECTORS)


@pytest.mark.parametrize("lat", SWEEP_LATS)
def test_sector_edges_are_sampled_parallels(lat):
    """Every ring is closed, two-latitude, and sampled every ``LON_STEP_DEG``.

    The north and south edges are parallels rather than great circles, so the
    vertices have to be dense enough that the filled band does not bow poleward
    between them.
    """
    geometry = ring.geometry_of(BENCH / ring.aoi_filename(lat))
    for polygon in geometry["coordinates"]:
        outer = polygon[0]
        assert outer[0] == outer[-1], "outer ring must close"
        assert sorted({lat_ for _, lat_ in outer}) == pytest.approx([-lat, -lat + ring.BAND_DEG])
        south = [lon for lon, lat_ in outer if lat_ == -lat]
        steps = {round(b - a, 6) for a, b in zip(south, south[1:]) if b > a}
        assert steps == {ring.LON_STEP_DEG}


def test_band_slug_and_filename_keep_the_committed_naming():
    """``88`` stays bare (the shipped name) and the half degrees take an underscore."""
    assert ring.aoi_filename(88.0) == "antarctic_88s.geojson"
    assert ring.aoi_filename(86.5) == "antarctic_86_5s.geojson"
    assert ring.band_slug(85.0) == "85"


def test_check_mode_passes_on_the_committed_family(tmp_path):
    """``--check`` agrees with every committed band and writes nothing while doing it."""
    assert ring.main(["--check", *(str(v) for v in SWEEP_LATS)]) == 0
    assert ring.main(["--check", "--out-dir", str(tmp_path), *(str(v) for v in SWEEP_LATS)]) == 1
    assert not list(tmp_path.iterdir()), "--check must never write"


def test_check_mode_reports_a_band_that_was_never_committed(tmp_path):
    """A band missing from the tree fails ``--check`` -- the family is incomplete, not fine."""
    assert ring.main(["--check", "--out-dir", str(tmp_path), "86"]) == 1


def test_check_mode_reports_a_band_whose_geometry_moved(tmp_path):
    """A band edited away from the generator fails ``--check`` rather than passing quietly."""
    doc = ring.ring_feature_collection(87.0)
    doc["features"][0]["geometry"]["coordinates"][0][0][0][1] += 0.05
    (tmp_path / ring.aoi_filename(87.0)).write_text(json.dumps(doc))
    assert ring.main(["--check", "--out-dir", str(tmp_path), "87"]) == 1


def test_regenerating_a_band_is_byte_stable(tmp_path):
    """Re-running the writer reproduces the committed bytes, not just the geometry.

    Serialization drift would show up as a whole-file diff on the next run and
    bury the one line that actually moved, so the format is part of the contract.
    """
    ring.main(["--out-dir", str(tmp_path), "86.5"])
    assert (tmp_path / "antarctic_86_5s.geojson").read_text() == (
        BENCH / "antarctic_86_5s.geojson"
    ).read_text()


def test_writing_88_leaves_the_shipped_ring_alone():
    """The documented latitude list includes 88, and must not rewrite its prose.

    The generator's note is the generic sweep-band one; the shipped ring's names
    the turning-latitude stress rationale. Geometry is identical either way, so
    an overwrite would pass every other test here while deleting the prose issue
    #148 leans on -- which is why the writer refuses instead.
    """
    shipped = BENCH / "antarctic_88s.geojson"
    before = shipped.read_text()
    assert ring.main(["85", "85.5", "86", "86.5", "87", "87.5", "88"]) == 1
    assert shipped.read_text() == before
    assert "turning-latitude stress target" in before


def test_force_replaces_a_committed_band(tmp_path):
    """``--force`` is the deliberate way through the guard, and an unchanged band is not blocked."""
    path = tmp_path / ring.aoi_filename(87.0)
    path.write_text("not a ring")
    assert ring.main(["--out-dir", str(tmp_path), "87"]) == 1
    assert path.read_text() == "not a ring"
    assert ring.main(["--out-dir", str(tmp_path), "--force", "87"]) == 0
    assert ring.geometry_of(path) == ring.ring_geometry(87.0)
    # Rewriting an identical band is idempotent, not an "EXISTS and differs".
    assert ring.main(["--out-dir", str(tmp_path), "87"]) == 0


def test_cli_entrypoint_runs():
    """The documented invocation works as a script, not only as an import."""
    proc = subprocess.run(
        [sys.executable, str(REPO / "tools" / "make_lat_ring_aoi.py"), "--check", "88"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "geometry matches" in proc.stdout
