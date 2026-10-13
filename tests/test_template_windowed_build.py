"""A store built from each windowed hive template is the v4 shape (issue #638).

The packaged ATL03 hive templates are windowed yearly from birth, so a run
from either — the local backend over ATL03-shaped fake granules, only the
h5coro transport stubbed — must come out with one leaf per (shard, year), the
``all`` row allocated beside them, the declared ladder folded by the staged
sweep the template chains by default (issue #620), an Icechunk repo at the
root with one row per window plus ``all`` (issue #604), the leaf tier's
all-time member folded beside each shard's window columns (issue #649), and a
clean ``pyramid_check`` report. Nothing but the template's own ``data_source``
paths is spelled here: a template that stopped validating, lost its
``delta_time`` column or its windowing block, or declared a ladder the leaf
worker cannot write would fail this file by name.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import test_windowed_emit as emit

from zagg import hive, runner
from zagg.config import default_config, get_windowing
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
    # No leg is red; the all-time leg in particular, which until issue #649's
    # close was the one failing leg of this store. A strict xfail cannot carry
    # the other legs, so they are asserted by name here too.
    failed = sorted(n for n, c in report["checks"].items() if c["status"] == "fail")
    assert failed == [], format_report(report)
    assert report["checks"]["all_time"]["status"] == "pass", format_report(report)
    assert report["windows"] == list(WINDOWS)
    assert report["roster"]["leaves"] == len(shards) * len(WINDOWS)
    assert report["all_time_nodes"]["missing"] == []
    assert report["columns"]["missing"] == [] and report["columns"]["partial"] == []
    # The all-time fold is there above the shard AND at it: every leaf carries
    # ``all.pyramid.zarr`` beside its window columns (issue #649), so the
    # declared leaf levels have their all-time member and the store's default
    # view reaches its finest four.
    assert report["all_time_columns"] == {
        "declared": len(shards),
        "materialized": len(shards),
        "missing": [],
    }


def test_the_all_time_column_is_the_kway_fold_of_the_window_columns(built):
    """Issue #649's resident path on the v4 shape: at every declared leaf-column
    level the ``all`` member's exact fields are the window members' sum, its
    digests their k-way fold and its composition word the §3.4 merge of their
    ``(word, n)`` pairs, in ascending window order — the close law, per
    field class off the member's own recorded ``fields``."""
    import zarr

    from zagg.column import COLUMN_ATTR
    from zagg.stats.composition import merge_composition_kway
    from zagg.store import open_store
    from zagg.sweep_overview import decode_digest, fold_digests, payload_weight
    from zagg.windows import SCHEDULE_NONE_TOKEN

    name, cfg, root, shards = built
    for shard in shards:
        node = hive.shard_leaf_path(str(root), shard).rsplit("/", 1)[0]
        columns = {
            w: zarr.open_group(open_store(f"{node}/{w}.pyramid.zarr"), mode="r", zarr_format=3)
            for w in (*WINDOWS, SCHEDULE_NONE_TOKEN)
        }
        block = dict(columns[SCHEDULE_NONE_TOKEN].attrs)[COLUMN_ATTR]
        assert block["window"] == SCHEDULE_NONE_TOKEN
        assert block["source_windows"] == {"folded": 3, "missing": 0, "unreadable": 0}
        windows = [dict(columns[w].attrs)[COLUMN_ATTR] for w in WINDOWS]
        fields = block["fields"]
        assert {m["class"] for m in fields.values()} >= {"exact", "approximate"}
        for res in map(str, (16, 15, 14, 13, 12, 11, 10, 9)):
            assert block["groups"][res]["merges_from_raw"] == 1 + max(
                w["groups"][res]["merges_from_raw"] for w in windows
            )
            all_time = columns[SCHEDULE_NONE_TOKEN][res]
            members = [columns[w][res] for w in WINDOWS]
            populated = np.flatnonzero(np.asarray(all_time["count"][:]))
            assert len(populated)
            for field, meta in fields.items():
                if meta["class"] == "exact":
                    assert meta["method"] == "sum"
                    np.testing.assert_array_equal(
                        all_time[field][:], sum(np.asarray(m[field][:]) for m in members)
                    )
                    continue
                if meta["class"] == "approximate":
                    stored = [np.asarray(m[field][:]) for m in members]
                    folded = np.asarray(all_time[field][:])
                    for j in populated:
                        cell = [decode_digest(d[j], meta["dtype"]) for d in stored if len(d[j])]
                        expected = (
                            fold_digests(cell, delta=meta["overview_delta"], dtype=meta["dtype"])
                            if cell
                            else b""
                        )
                        assert bytes(folded[j]) == expected, (shard, res, field, j)
                    continue
                of, of_dtype = meta["of"], fields[meta["of"]]["dtype"]
                words = [np.asarray(m[field][:]) for m in members]
                weights = [np.asarray(m[of][:]) for m in members]
                folded = np.asarray(all_time[field][:])
                for j in populated:
                    parts = [
                        (int(w[j]), n)
                        for w, d in zip(words, weights)
                        if (n := payload_weight(d[j], of_dtype)) > 0
                    ]
                    expected = merge_composition_kway(parts) if parts else 0
                    assert int(folded[j]) == expected, (shard, res, field, j)


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
