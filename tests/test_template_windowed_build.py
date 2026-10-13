"""A store built from each windowed hive template is the v4 shape (issue #638).

The packaged ATL03 hive templates are windowed yearly from birth, so a run
from either — the local backend over ATL03-shaped fake granules, only the
h5coro transport stubbed — must come out with one leaf per (shard, year), the
``all`` row allocated beside them, the declared ladder folded by the staged
sweep the template chains by default (issue #620), an Icechunk repo at the
root with one row per window plus ``all`` (issue #604), and a ``pyramid_check``
report clean but for the one leg issue #649 is open on (the leaf tier's
all-time member, xfailed below). Nothing but the template's own ``data_source``
paths is spelled here: a template that stopped validating, lost its
``delta_time`` column or its windowing block, or declared a ladder the leaf
worker cannot write would fail this file by name.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import test_windowed_emit as emit
from obstore.exceptions import GenericError
from zarr.storage import WrapperStore

from zagg import hive, runner
from zagg.config import default_config, get_windowing
from zagg.grids.morton import morton_decimal
from zagg.hive import MANIFEST_NAME
from zagg.pyramid_check import format_report, validate_pyramid

TEMPLATES = ["atl03_tdigest_strata_healpix", "atl03_tdigest_healpix_hive"]
WINDOWS = ("2018", "2019", "2020")
#: Two order-9 shards under one order-8 parent (Sierra Nevada; the issue
#: #602 measurement cell is nearby), so the ladder has a shared node.
SITES = ((37.80, -119.50), (37.81, -119.48))
SURFACES = 5
DAY = 86400.0


def _shard(site, order):
    from mortie import geo2mort

    return int(geo2mort(np.array([site[0]]), np.array([site[1]]), order=order)[0])


def _granule(site, days, seed, groups):
    """An ATL03-shaped fake: six beam groups, photons linked from segments.

    Every group carries the same photons — ``len(days) * 20`` of them at the
    site, in three segments — under the template's own dataset paths, so the
    read plan (``spatial_index: segments``, ``pad: 1``), the segment link
    (``ph_index_beg`` origin 1), the 2-D ``signal_conf_ph`` column selector
    and the TEP filter all run against the paths the template declares.
    """
    rng = np.random.default_rng(seed)
    per_day = 20
    n = len(days) * per_day
    lat = np.full(n, site[0]) + rng.normal(0, 1e-4, n)
    lon = np.full(n, site[1]) + rng.normal(0, 1e-4, n)
    conf = rng.integers(0, 5, (n, SURFACES)).astype("i1")
    conf[: n // 4, 0] = -2  # TEP photons, dropped by the template's filter
    seg = n // 3
    arrays = {}
    for g in groups:
        arrays.update(
            {
                f"/{g}/heights/lat_ph": lat,
                f"/{g}/heights/lon_ph": lon,
                f"/{g}/heights/h_ph": rng.normal(2000.0, 5.0, n).astype("f4"),
                f"/{g}/heights/delta_time": np.repeat(np.asarray(days, dtype="f8") * DAY, per_day),
                f"/{g}/heights/signal_conf_ph": conf,
                f"/{g}/geolocation/reference_photon_lat": np.full(3, site[0]),
                f"/{g}/geolocation/reference_photon_lon": np.full(3, site[1]),
                f"/{g}/geolocation/ph_index_beg": np.array([1, seg + 1, 2 * seg + 1]),
                f"/{g}/geolocation/segment_ph_cnt": np.array([seg, seg, n - 2 * seg]),
            }
        )
    return emit._FakeH5(arrays)


def _build(monkeypatch, tmp_path, name):
    """Run the template over the fakes on the local backend; return the root."""
    cfg = default_config(name)
    groups = list(cfg.data_source["groups"])
    grid = cfg.output["grid"]
    fakes, records = {}, []
    for i, site in enumerate(SITES):
        recs = []
        for j, (tag, days, start, end) in enumerate(
            (
                ("A", [300.0, 400.0], "2018-10-28T00:00:00Z", "2019-02-07T00:00:00Z"),
                ("B", [800.0, 801.0], "2020-03-10T00:00:00Z", "2020-03-13T00:00:00Z"),
                ("C", [729.5, 730.25], "2019-12-31T12:00:00Z", "2020-01-01T12:00:00Z"),
            )
        ):
            rec = emit._timed_rec(f"{tag}{i}", start, end)
            fakes[rec["s3"]] = _granule(site, days, seed=10 * i + j, groups=groups)
            recs.append(rec)
        records.append(recs)
    catalog = {
        "metadata": {"short_name": "ATL03", "version": "007"},
        "grid_signature": {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": int(grid["parent_order"]),
            "child_order": int(grid["child_order"]),
            "layout": "fullsphere",
        },
        "shard_keys": [_shard(site, int(grid["parent_order"])) for site in SITES],
        "granules": records,
    }
    path = tmp_path / f"{name}.catalog.json"
    path.write_text(json.dumps(catalog))
    monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
    emit._patch(monkeypatch, fakes)
    root = tmp_path / name
    runner.agg(cfg, catalog=str(path), store=str(root), backend="local")
    return cfg, root, catalog["shard_keys"]


@pytest.fixture(scope="module", params=TEMPLATES)
def built(request, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("template-store")
    with pytest.MonkeyPatch.context() as monkeypatch:
        cfg, root, shards = _build(monkeypatch, tmp_path, request.param)
    return request.param, cfg, root, shards


def test_every_window_has_its_leaf_and_the_all_row_is_allocated(built):
    name, cfg, root, shards = built
    assert get_windowing(cfg)["schedule"] == "yearly"
    for shard in shards:
        for label in WINDOWS:
            leaf = hive.shard_leaf_path(str(root), shard, window=label)
            stamp = hive.read_commit(_store(leaf))
            assert stamp["window"] == label and stamp["spec"] == "morton-hive/2"
    manifest = json.loads((root / MANIFEST_NAME).read_text())
    assert manifest["temporal"]["schedule"] == "yearly"
    assert manifest["pyramid"]["spec"] == "zagg-pyramid/2"
    assert manifest["pyramid"]["overview"]["all_time"] is True
    # The leaf tier is one column entry at the shard node; the rest is the
    # fixed every-order ladder above it, down to node 0.
    assert manifest["pyramid"]["overviews"][0]["cells"] == [16, 15, 14, 13]
    assert [e["node"] for e in manifest["pyramid"]["overviews"]] == list(range(9, -1, -1))


def test_the_icechunk_repo_holds_one_row_per_window_plus_all(built):
    import os

    from zagg.icechunk_refs import read_block, repo_path
    from zagg.icechunk_rows import ALL_ROW

    name, cfg, root, shards = built
    assert os.path.isdir(repo_path(str(root)))
    block = read_block(str(root), store_kwargs={})
    assert block is not None
    # Up the ladder, not per leaf: the Icechunk commit default keys on the
    # EXPLICIT output.sweep: "stages" (icechunk_refs.ladder_walks,
    # PR #622 ruling (1)), and the templates spell it (espg, PR #639) so a
    # templated run commits one node at a time -- the regime that scales.
    assert block["commit"] == "ladder"
    assert ALL_ROW in block["rows"] and set(WINDOWS) <= set(block["rows"])
    assert block["rows_per_manifest"] == 1


@pytest.fixture(scope="module")
def report(built):
    """One full-mode ``validate_pyramid`` per template, for the two tests that
    read it: the walk is the slowest thing in this file and both want the same
    store in the same state. Read-only -- neither test mutates the report -- so
    sharing it couples nothing and can hide no failure.
    """
    return validate_pyramid(str(built[2]), full=True)


def test_pyramid_check_passes(built, report):
    name, cfg, root, shards = built
    # ``passed`` tolerates a skip; the value legs must actually run.
    for check in ("materialization", "columns", "readback", "counts", "digests"):
        assert report["checks"][check]["status"] == "pass", format_report(report)
    # Exactly one leg is red -- the issue #650 all-time one that
    # ``test_the_all_time_leg_fails_until_649`` owns. This is the verdict for
    # every OTHER leg too: a strict xfail fires only on a pass, so it cannot
    # carry them, and without this line declaration, coordinates, records,
    # composition and idempotency would be asserted by nothing in this file.
    failed = sorted(n for n, c in report["checks"].items() if c["status"] == "fail")
    assert failed == ["all_time"], format_report(report)
    assert report["windows"] == list(WINDOWS)
    assert report["roster"]["leaves"] == len(shards) * len(WINDOWS)
    assert report["all_time_nodes"]["missing"] == []
    assert report["columns"]["missing"] == [] and report["columns"]["partial"] == []
    # The all-time fold is there above the shard and missing AT it: the leaf
    # tier's ``all.pyramid.zarr`` is what issue #649 has yet to write, so the
    # declared leaf levels have no all-time member and the store's default
    # view is short its finest four. ``test_the_all_time_leg_fails_until_649``
    # owns that verdict; everything else above is green already.
    assert report["all_time_columns"] == {
        "declared": len(shards),
        "materialized": 0,
        "missing": sorted(morton_decimal(k) for k in shards),
    }


@pytest.mark.xfail(
    strict=True,
    reason="issue #649: a windowed run writes no all.pyramid.zarr, so the declared leaf-column "
    "levels have no all-time member and the issue #650 all_time leg fails by leaf. Flips to "
    "PASS when the shard-level close of #649 lands — delete the marker then.",
)
def test_the_all_time_leg_fails_until_649(report):
    # Only the leg the reason names. ``passed`` would widen the flip
    # condition: if #649 landed while some other leg was red this would stay
    # xfailed and the marker undeleted. "No other leg is red" is
    # ``test_pyramid_check_passes``'s job, and it asserts it by name.
    assert report["checks"]["all_time"]["status"] == "pass", format_report(report)


#: Per-shard leaf ``count`` totals by year, from the fixture: each granule
#: puts 20 photons per day in every group, the TEP filter drops the first
#: quarter (10, all from its first day), times 6 groups. A: day 300 (2018,
#: 10 kept) and day 400 (2019, 20). B: days 800/801 (2020, 30). C: day
#: 729.5 (2019-12-31T12:00, 10) and day 730.25 (2020-01-01T06:00, 20).
WINDOW_COUNTS = {"2018": 10 * 6, "2019": (20 + 10) * 6, "2020": (30 + 20) * 6}


def test_each_photon_lands_in_its_own_year(built):
    """Window membership, not just conservation: an epoch, scale or GPS/UTC
    slip that moved C's 2019 photons into 2020 conserves every ladder total
    but changes these."""
    import zarr

    from zagg.store import open_store

    name, cfg, root, shards = built
    for shard in shards:
        for label, expected in WINDOW_COUNTS.items():
            # A versioned leaf's arrays live under its stamp's ``current``.
            data, _ = hive.resolve_leaf(hive.shard_leaf_path(str(root), shard, window=label))
            grp = zarr.open_group(
                open_store(data),
                path=str(cfg.output["grid"]["child_order"]),
                mode="r",
                zarr_format=3,
            )
            assert int(np.asarray(grp["count"][:]).sum()) == expected, (shard, label)


def _store(leaf):
    from zagg.store import open_store

    return open_store(str(leaf))


# ── a leaf write the store refuses (issue #651) ──────────────────────────────


class _RefusingStore(WrapperStore):
    """The leaf store, raising ``error`` on its first chunk PUT."""

    def __init__(self, store, error, state):
        super().__init__(store)
        self._error, self._state = error, state

    async def set(self, key, value):
        if "/c/" in key and not self._state.get("raised"):
            self._state["raised"] = True
            raise self._error
        await super().set(key, value)


#: The two shapes the classifier tells apart: object_store could not parse the
#: body (the post-mortem's class; no status to key on), and a 5xx the client
#: already retried for the full policy.
_FATAL = "Generic S3 error: Got invalid DeleteObjects response: EOF while parsing"
_RETRIED = (
    "Error after 12 retries in 180s, max_retries:12, retry_timeout:180s, "
    "source:HTTP status server error (503 Service Unavailable) for url"
)


@pytest.mark.parametrize(
    ("message", "error_class", "emitted"),
    [(_FATAL, "store-fatal", ["2018", "2019"]), (_RETRIED, "transient", list(WINDOWS))],
    ids=["store-fatal", "transient"],
)
def test_a_refused_leaf_write_ends_the_unit_at_that_window(
    monkeypatch, tmp_path, message, error_class, emitted
):
    """A fatal-shaped error on the first chunk PUT of the second window ends the
    unit there: no later window is aggregated or opened, the earlier window
    keeps its stamp, the failed one is unstamped debris, and the result names
    the class. The retried shape keeps the per-window isolation (issue #586):
    the window fails alone and the rest of the shard lands.
    """
    import zagg.store as store_mod

    name = TEMPLATES[0]
    shard0 = _shard(SITES[0], int(default_config(name).output["grid"]["parent_order"]))
    root = str(tmp_path / name)
    leaf = {w: hive.shard_leaf_path(root, shard0, window=w) for w in WINDOWS}
    real_open, opens, state = store_mod.open_store, [], {}

    def _open(path, *args, **kwargs):
        opens.append(str(path))
        store = real_open(path, *args, **kwargs)
        if str(path).startswith(leaf["2019"]) and not kwargs.get("read_only"):
            return _RefusingStore(store, GenericError(message), state)
        return store

    metas, real_seam = {}, hive.process_and_write_hive

    def _seam(shard_key, *args, **kwargs):
        metas[int(shard_key)] = meta = real_seam(shard_key, *args, **kwargs)
        return meta

    # ``sinks()`` is the argument to ``aggregate``: one call per aggregated window.
    aggregated, real_sinks = [], hive._LeafUnit.sinks

    def _sinks(self):
        aggregated.append((int(self.shard_key), self.label))
        return real_sinks(self)

    monkeypatch.setattr(store_mod, "open_store", _open)
    monkeypatch.setattr(hive, "process_and_write_hive", _seam)
    monkeypatch.setattr(hive._LeafUnit, "sinks", _sinks)
    _cfg, built, shards = _build(monkeypatch, tmp_path, name)
    assert str(built) == root and shards[0] == shard0 and state["raised"]

    meta, other = metas[shard0], metas[shards[1]]
    assert [m["window"] for m in meta["windows"]] == emitted
    assert [w for s, w in aggregated if s == shard0] == emitted
    assert meta["error_class"] == meta["windows"][1]["error_class"] == error_class
    assert message in meta["windows"][1]["error"]
    assert other["error"] is None and [m["window"] for m in other["windows"]] == list(WINDOWS)
    # The window before the failure keeps its stamp; the failed one is debris.
    assert hive.resolve_leaf(leaf["2018"])[1] is not None
    assert hive.resolve_leaf(leaf["2019"])[1] is None
    if error_class == "store-fatal":
        # The bound: the unit returned after the failing window -- the window
        # after it was never aggregated, never opened, never written.
        assert meta["windows_unattempted"] == ["2020"]
        assert not any(o.startswith(leaf["2020"]) for o in opens)
        assert not Path(leaf["2020"]).exists()
    else:
        assert "windows_unattempted" not in meta
        assert hive.resolve_leaf(leaf["2020"])[1] is not None
