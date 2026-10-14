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

The build runs through a store that REFUSES to delete a chunk key the way
the source.coop bucket did on 2026-10-12 (issue #653): one window holds
noise only, so its leaf, column and sweep slabs for the signal stratum are
all fill, and zarr's answer to an all-fill slab is a ``delete`` of a key that
does not exist. The writers skip those slabs instead. Finishing the build
proves nothing: a refused write is caught and logged per window (``hive``'s
``_emit``) or per column (the leaf-column ``except Exception``), and
``runner.agg`` returns normally. The proof is the refuser recording no
chunk-key delete (``attempted`` empty), ``pyramid_check`` and the per-year
counts showing every leaf landed, and a key-for-key match against a
guard-off permissive build, since a skipped block and a deleted one both
leave no object.
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


def _build(monkeypatch, tmp_path, name, *, refuse_chunk_deletes=False):
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
    #: the same build with the guard off through a permissive store (zarr's
    #: own delete-on-all-fill, as before #653), for the key-set match; None
    #: for the single-digest template, which has no all-fill slab to compare
    permissive: object


@pytest.fixture(scope="module", params=TEMPLATES)
def built(request, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("template-store")
    _NoChunkDeletes.attempted, _NoChunkDeletes.deletes = [], []
    with pytest.MonkeyPatch.context() as monkeypatch:
        cfg, root, shards = _build(monkeypatch, tmp_path, request.param, refuse_chunk_deletes=True)
    attempted, deletes = _NoChunkDeletes.attempted, _NoChunkDeletes.deletes
    permissive = None
    if request.param == TEMPLATES[0]:
        with pytest.MonkeyPatch.context() as monkeypatch:
            # The pre-#653 writers: the guard off, so every all-fill slab goes
            # to zarr, which deletes its key. Every call site looks ``all_fill``
            # up on this module at call time, so one patch reaches them all.
            import zagg.processing.write

            monkeypatch.setattr(zagg.processing.write, "all_fill", lambda array, values: False)
            _, permissive, _ = _build(
                monkeypatch, tmp_path_factory.mktemp("permissive-store"), request.param
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


def test_the_icechunk_all_row_reaches_the_leaf_column_levels(built):
    """Issue #649 phase 4: the repo's ``all`` row at the declared column levels
    reads the shard's all-time column, not fill — what the probe measured as
    zero at cells 13 and 16 on v4."""
    import zarr

    from zagg.icechunk_refs import open_repo, read_block
    from zagg.icechunk_rows import ALL_ROW
    from zagg.store import open_store

    name, cfg, root, shards = built[:4]
    rows = read_block(str(root), store_kwargs={})["rows"]
    session = open_repo(str(root), store_kwargs={}).readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    from zagg.grids import HealpixGrid

    grid = HealpixGrid(parent_order=9, child_order=19, config=cfg)
    for shard in shards:
        (rank,) = grid.block_index(shard)
        node = hive.shard_leaf_path(str(root), shard).rsplit("/", 1)[0]
        member = zarr.open_group(open_store(f"{node}/all.pyramid.zarr"), mode="r", zarr_format=3)
        for level in (16, 15, 14, 13):
            n = 4 ** (level - 9)
            got = np.asarray(
                group[str(level)]["count"][rows.index(ALL_ROW), rank * n : (rank + 1) * n]
            )
            want = np.asarray(member[str(level)]["count"][:])
            assert want.any(), (shard, level)
            np.testing.assert_array_equal(got, want)
            # The digest payloads too (PR #666 review finding (6)): the vlen
            # arrays of the sharded levels 14-16 are where a wrong inner-chunk
            # offset or length would land, and ``count`` would not show it.
            digests = [k for k, _a in member[str(level)].arrays() if k.startswith("h_tdigest")]
            payloads = 0
            for field in digests:
                stored = [bytes(b) for b in member[str(level)][field][:]]
                payloads += any(stored)
                via_repo = group[str(level)][field][rows.index(ALL_ROW), rank * n : (rank + 1) * n]
                assert [bytes(a) for a in via_repo] == stored, (shard, level, field)
            assert payloads, (shard, level, digests)  # some digest field holds bytes here
            for w in WINDOWS:
                column = zarr.open_group(
                    open_store(f"{node}/{w}.pyramid.zarr"), mode="r", zarr_format=3
                )
                np.testing.assert_array_equal(
                    group[str(level)]["count"][rows.index(w), rank * n : (rank + 1) * n],
                    column[str(level)]["count"][:],
                )


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
        "checked": len(shards),
        "stale": [],
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

    name, cfg, root, shards = built[:4]
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


_NO_FILL_SLAB = "the single-digest template has no all-fill slab: the refusal alone is its check"


def test_the_build_never_asks_the_store_to_delete_a_chunk(built):
    """Issue #653: an all-fill slab is skipped, not written-as-delete.

    The build finishing proves nothing — a refused write is caught and logged
    per window/column and ``runner.agg`` still returns — so the proof is
    ``attempted`` staying empty (with ``test_pyramid_check_passes`` and
    ``test_each_photon_lands_in_its_own_year`` showing every leaf landed).
    Both templates run it: for the strata one the stub window's signal
    stratum and ``composition`` are all fill, so this is the #653 regression
    guard; the single-digest one writes its one digest in every window, so
    for it this only checks that the refusal stays scoped.
    """
    name, cfg, root, shards = built[:4]
    assert built.attempted == []
    # The refusal is scoped: the per-leaf template clear (``delete_dir("")``,
    # D4) and the sidecar deletes went through, at least one per leaf.
    assert len(built.deletes) >= len(shards) * len(WINDOWS)


def test_the_stub_windows_fill_slab_has_no_object(built):
    """The skipped object's absence, where there is one to skip: the strata
    template's signal stratum, populated in 2020 and all fill in 2021."""
    name, cfg, root, shards = built[:4]
    if name != TEMPLATES[0]:
        pytest.skip(_NO_FILL_SLAB)
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
    the guard never skipped a block zarr would have written.

    The twin runs with ``all_fill`` patched to ``False``, so it is zarr's own
    pre-#653 behaviour (delete-on-all-fill); key-set equality therefore means
    the guard skipped exactly the blocks zarr would have deleted, no more.
    Strata template only: the single-digest one has nothing to skip, so its
    twin would match by construction and is not built.
    """
    if built.permissive is None:
        pytest.skip(_NO_FILL_SLAB)
    assert _objects(built.root) == _objects(built.permissive)


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
    ("message", "store_error_class", "emitted"),
    [(_FATAL, "store-fatal", ["2018", "2019"]), (_RETRIED, "transient", list(WINDOWS))],
    ids=["store-fatal", "transient"],
)
def test_a_refused_leaf_write_ends_the_unit_at_that_window(
    monkeypatch, tmp_path, message, store_error_class, emitted
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
    assert meta["store_error_class"] == meta["windows"][1]["store_error_class"] == store_error_class
    assert message in meta["windows"][1]["error"]
    assert other["error"] is None and [m["window"] for m in other["windows"]] == list(WINDOWS)
    # The window before the failure keeps its stamp; the failed one is debris.
    assert hive.resolve_leaf(leaf["2018"])[1] is not None
    assert hive.resolve_leaf(leaf["2019"])[1] is None
    later = [w for w in WINDOWS if w > "2019"]  # 2020 and the #653 stub window 2021
    if store_error_class == "store-fatal":
        # The bound: the unit returned after the failing window -- the windows
        # after it were never aggregated, never opened, never written.
        assert meta["windows_unattempted"] == later
        for w in later:
            assert not any(o.startswith(leaf[w]) for o in opens)
            assert not Path(leaf[w]).exists()
    else:
        assert "windows_unattempted" not in meta
        for w in later:
            assert hive.resolve_leaf(leaf[w])[1] is not None
