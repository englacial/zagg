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

The build runs through a store that REFUSES to delete a chunk key the way
the source.coop bucket did on 2026-10-12 (issue #653): one window holds
noise only, so its leaf, column and sweep slabs for the signal stratum are
all fill, and zarr's answer to an all-fill slab is a ``delete`` of a key that
does not exist. The writers skip those slabs instead, and the proof is the
build itself — plus a key-for-key match against a permissive build, since a
skipped block and a deleted one both leave no object.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pytest
import test_windowed_emit as emit
from obstore.exceptions import GenericError
from zarr.storage import LocalStore, WrapperStore

from zagg import hive, runner
from zagg.config import default_config, get_windowing
from zagg.grids.morton import morton_decimal
from zagg.hive import MANIFEST_NAME
from zagg.pyramid_check import format_report, validate_pyramid

TEMPLATES = ["atl03_tdigest_strata_healpix", "atl03_tdigest_healpix_hive"]
#: 2021 is the stub window (issue #653): photons, but no signal photon, so
#: every ``where``-predicated signal slab is all fill. A window with NO
#: photons gets no leaf at all (``hive`` gates the leaf write on
#: ``chunk_results``), which is why the stub is noise-only, not empty.
WINDOWS = ("2018", "2019", "2020", "2021")
#: Two order-9 shards under one order-8 parent (Sierra Nevada; the issue
#: #602 measurement cell is nearby), so the ladder has a shared node.
SITES = ((37.80, -119.50), (37.81, -119.48))
SURFACES = 5
DAY = 86400.0


def _shard(site, order):
    from mortie import geo2mort

    return int(geo2mort(np.array([site[0]]), np.array([site[1]]), order=order)[0])


def _granule(site, days, seed, groups, *, noise_only=False):
    """An ATL03-shaped fake: six beam groups, photons linked from segments.

    Every group carries the same photons — ``len(days) * 20`` of them at the
    site, in three segments — under the template's own dataset paths, so the
    read plan (``spatial_index: segments``, ``pad: 1``), the segment link
    (``ph_index_beg`` origin 1), the 2-D ``signal_conf_ph`` column selector
    and the TEP filter all run against the paths the template declares.
    ``noise_only`` zeroes every confidence (no TEP photon either): the stub
    window's shape, in which no photon satisfies the signal predicate.
    """
    rng = np.random.default_rng(seed)
    per_day = 20
    n = len(days) * per_day
    lat = np.full(n, site[0]) + rng.normal(0, 1e-4, n)
    lon = np.full(n, site[1]) + rng.normal(0, 1e-4, n)
    conf = rng.integers(0, 5, (n, SURFACES)).astype("i1")
    if noise_only:
        conf[:] = 0
    else:
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


def _is_chunk(key):
    """Whether a store key (or ``delete_dir`` prefix) addresses chunk data."""
    return "/c/" in f"/{key}/"


class _NoChunkDeletes(WrapperStore):
    """A store that fails a chunk-key delete the way the bucket did (issue #653).

    The leaf/column clears (``delete_dir("")`` on a prefix) and sidecar deletes
    are still allowed — they remove keys that exist — and counted in
    ``deletes``; a delete aimed at chunk data is recorded in ``attempted`` and
    raised as the obstore error S3's ``<Error>``-in-a-2xx answer produces.
    """

    attempted: list = []
    deletes: list = []

    def _refuse(self, key):
        self.attempted.append(key)
        raise GenericError(
            "Generic S3 error: Error performing DeleteObjects request: Got invalid "
            f"DeleteObjects response for {key!r} (issue #653)"
        )

    async def delete(self, key):
        if _is_chunk(key):
            self._refuse(key)
        self.deletes.append(key)
        await self._store.delete(key)

    async def delete_dir(self, prefix):
        if _is_chunk(prefix):
            self._refuse(prefix)
        self.deletes.append(prefix)
        await self._store.delete_dir(prefix)


def _refuse_chunk_deletes(monkeypatch):
    """Route every writable local store the build opens through the refuser."""
    import zagg.store

    real = zagg.store.open_store

    def wrapped(path, *args, **kwargs):
        store = real(path, *args, **kwargs)
        if isinstance(store, LocalStore) and not store.read_only:
            return _NoChunkDeletes(store)
        return store

    monkeypatch.setattr(zagg.store, "open_store", wrapped)
    monkeypatch.setattr(runner, "open_store", wrapped)


def _build(monkeypatch, tmp_path, name, *, refuse_chunk_deletes):
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
                ("D", [1200.0], "2021-04-15T00:00:00Z", "2021-04-16T00:00:00Z"),
            )
        ):
            rec = emit._timed_rec(f"{tag}{i}", start, end)
            fakes[rec["s3"]] = _granule(
                site, days, seed=10 * i + j, groups=groups, noise_only=tag == "D"
            )
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
    if refuse_chunk_deletes:
        _refuse_chunk_deletes(monkeypatch)
    root = tmp_path / name
    runner.agg(cfg, catalog=str(path), store=str(root), backend="local")
    return cfg, root, catalog["shard_keys"]


class _Built(NamedTuple):
    name: str
    cfg: object
    root: object
    shards: list
    #: chunk-key deletes the refusing store was asked for (must stay empty)
    attempted: list
    #: the deletes it allowed (prefix clears, sidecars)
    deletes: list
    #: the same build through a permissive store, for the key-set match
    permissive: object


@pytest.fixture(scope="module", params=TEMPLATES)
def built(request, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("template-store")
    _NoChunkDeletes.attempted, _NoChunkDeletes.deletes = [], []
    with pytest.MonkeyPatch.context() as monkeypatch:
        cfg, root, shards = _build(monkeypatch, tmp_path, request.param, refuse_chunk_deletes=True)
    attempted, deletes = _NoChunkDeletes.attempted, _NoChunkDeletes.deletes
    with pytest.MonkeyPatch.context() as monkeypatch:
        _, permissive, _ = _build(
            monkeypatch,
            tmp_path_factory.mktemp("permissive-store"),
            request.param,
            refuse_chunk_deletes=False,
        )
    return _Built(request.param, cfg, root, shards, attempted, deletes, permissive)


def test_every_window_has_its_leaf_and_the_all_row_is_allocated(built):
    name, cfg, root, shards = built[:4]
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

    name, cfg, root, shards = built[:4]
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
    return validate_pyramid(str(built.root), full=True)


def test_pyramid_check_passes(built, report):
    name, cfg, root, shards = built[:4]
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
#: 729.5 (2019-12-31T12:00, 10) and day 730.25 (2020-01-01T06:00, 20). D:
#: day 1200 (2021, 20 noise photons, none TEP).
WINDOW_COUNTS = {"2018": 10 * 6, "2019": (20 + 10) * 6, "2020": (30 + 20) * 6, "2021": 20 * 6}


def test_each_photon_lands_in_its_own_year(built):
    """Window membership, not just conservation: an epoch, scale or GPS/UTC
    slip that moved C's 2019 photons into 2020 conserves every ladder total
    but changes these."""
    import zarr

    from zagg.store import open_store

    name, cfg, root, shards = built[:4]
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


def _objects(root):
    """Every object key under the store's ``*.zarr`` prefixes, root-relative.

    Only the Zarr objects: the Icechunk repo beside them names its snapshots
    and manifests by random id, and the run-stats sidecars carry run ids. A
    versioned leaf's version directory (``run-<run_id>-<hash>``, spec §1.5)
    is run-specific too, so it is normalized.
    """
    return {
        "/".join(_RUN_DIR.sub("run-*", part) for part in p.relative_to(root).parts)
        for p in root.rglob("*")
        if p.is_file() and any(part.endswith(".zarr") for part in p.relative_to(root).parts)
    }


_RUN_DIR = re.compile(r"^run-[0-9a-f]{32}-[0-9a-f]{8}$")


def test_the_build_never_asks_the_store_to_delete_a_chunk(built):
    """Issue #653: an all-fill slab is skipped, not written-as-delete.

    The build already proved it by finishing against the refusing store; the
    assertions name the evidence. Only the strata template has a slab the stub
    window leaves all fill (its signal stratum and ``composition``), so only
    there is the skipped object's absence visible — the single-digest template
    writes every window's one digest and is the control: nothing populated is
    ever skipped (``test_skipped_blocks_leave_no_object_either_way``).
    """
    name, cfg, root, shards = built[:4]
    assert built.attempted == []
    # The refusal is scoped: the per-leaf template clear (``delete_dir("")``,
    # D4) and the sidecar deletes went through, at least one per leaf.
    assert len(built.deletes) >= len(shards) * len(WINDOWS)
    if name != TEMPLATES[0]:
        return
    base = str(cfg.output["grid"]["child_order"])
    for shard in shards:
        leaf = {
            label: Path(hive.resolve_leaf(hive.shard_leaf_path(str(root), shard, window=label))[0])
            for label in ("2020", "2021")
        }
        for label in leaf:
            assert (leaf[label] / base / "count" / "c" / "0").exists(), label
        # Populated in 2020, all fill in 2021 → one object vs none, and the
        # 2021 array still reads as fill (absence is the encoding, spec §4).
        assert (leaf["2020"] / base / "h_tdigest_signal" / "c" / "0").exists()
        assert not (leaf["2021"] / base / "h_tdigest_signal").joinpath("c").exists()
        import zarr

        arr = zarr.open_array(_store(leaf["2021"]), path=f"{base}/h_tdigest_signal", mode="r")
        assert all(v == b"" for v in np.asarray(arr[:]).flat)


def test_skipped_blocks_leave_no_object_either_way(built):
    """A skipped all-fill block and zarr's delete of it both leave no key — and
    the guard never skipped a block zarr would have written."""
    assert _objects(built.root) == _objects(built.permissive)
