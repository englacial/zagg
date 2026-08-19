"""Tests for the benchmark object-count metric (issue #240).

Pins the expected-count model in ``.github/scripts/bench_objects.py`` against
REAL local stores written through the production template + writers (the
sharded flat path and the hive leaf path), so a model drift or a sharded-write
bypass (the issue #215 object blow-up) fails here before it reaches the
harness. No AWS: the LIST helper rides ``zagg.store.open_object_store``, which
treats a local path and ``s3://`` identically.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(SCRIPTS))

import bench_objects  # noqa: E402

from zagg.config import PipelineConfig, load_config  # noqa: E402
from zagg.grids import HealpixGrid, from_config  # noqa: E402
from zagg.grids.morton import morton_word  # noqa: E402
from zagg.processing import (  # noqa: E402
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
    write_shard_to_zarr,
)
from zagg.stats.tdigest import build_tdigest  # noqa: E402
from zagg.store import open_store  # noqa: E402

# Two order-6 shards (decimal morton ids), as in test_readers.
_KEY_A, _KEY_B = "1121121", "2431123"


def _cfg(sharded=True):
    """Benchmark-shaped minimal config: coords + count + ragged t-digest."""
    grid = {"type": "healpix", "parent_order": 6, "child_order": 12, "chunk_inner": 8}
    if sharded:
        grid["sharded"] = True
    return PipelineConfig(
        data_source={"groups": ["g"]},
        aggregation={
            "coordinates": {
                "morton": {"dtype": "uint64", "fill_value": 0},
            },
            "variables": {
                "count": {"function": "len", "source": "h", "dtype": "int32", "fill_value": 0},
                "h_tdigest": {
                    "function": "zagg.stats.tdigest.build_tdigest",
                    "source": "h",
                    "kind": "ragged",
                    "inner_shape": [2],
                    "dtype": "float32",
                    "fill_value": 0,
                },
            },
        },
        output={"grid": grid},
    )


def _grid(sharded=True):
    cfg = _cfg(sharded=sharded)
    return HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=sharded)


def _chunk_carrier(grid, children):
    """Full inner-chunk carrier: coords + a non-fill count column."""
    coords = grid.coords_of(children)
    df = pd.DataFrame({"count": np.ones(len(children), dtype=np.int32)})
    for name, vals in coords.items():
        df[name] = vals
    return df


def _digest():
    return build_tdigest(np.array([1.0, 2.0, 3.0]), delta=16)


def _write_flat_shard(grid, store, word, *, sharded):
    """One populated shard through the production writers (all chunks)."""
    chunk_results = []
    for block, children in grid.iter_chunks(word):
        carrier = _chunk_carrier(grid, children)
        ragged = {"h_tdigest": ([_digest()], [11])}
        if sharded:
            chunk_results.append((block, carrier, ragged))
        else:
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block)
            write_ragged_to_zarr(ragged, store, grid=grid, chunk_idx=block)
    if sharded:
        write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=word)


# --- expected model against the committed benchmark configs -----------------


def test_expected_counts_live_matrix_config():
    # The live-matrix config (flat, sharded, K=256): root + group + 3 array
    # zarr.json objects (morton/count/h_tdigest — the legacy cell_ids array is
    # gone since the D16 flip, issue #304), then one object per array per shard.
    config = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9.yaml"))
    exp = bench_objects.expected_object_counts(from_config(config), n_shards=1)
    # Metadata is EXACT: the run-level stats parquet (issue #297) is per-run
    # root telemetry, tallied in its own unbounded bucket since issue #362.
    assert exp == {
        "metadata": 5,
        "metadata_min": 5,
        "per_shard_min": 3,
        "per_shard_max": 3,
        "total_min": 8,
        "total_max": 8,
        "exact": True,
    }


def test_expected_counts_aoimask_config_adds_one_array():
    # The mask arm adds the aoi_mask bool array: one more zarr.json + one more
    # sharded object per shard.
    config = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9_aoimask.yaml"))
    exp = bench_objects.expected_object_counts(from_config(config), n_shards=4)
    assert exp["metadata"] == 6
    assert exp["per_shard_min"] == exp["per_shard_max"] == 4
    assert exp["exact"] is True
    assert exp["total_max"] == 6 + 4 * 4


def test_expected_counts_unsharded_is_bounded_not_exact():
    # Unsharded at K=16: dense arrays write 1..K chunk objects (empty inner
    # chunks are omitted), ragged 0..K -- a bounded, non-exact expectation.
    exp = bench_objects.expected_object_counts(_grid(sharded=False), n_shards=1)
    k = 16
    assert exp["metadata"] == 5
    assert exp["exact"] is False
    assert exp["per_shard_min"] == 2  # one populated chunk x 2 dense arrays
    assert exp["per_shard_max"] == 3 * k


def test_hive_metadata_ceiling_covers_sweep_written_root_moc():
    # The store-root coverage.moc has TWO writers and only ONE honours
    # ``output.coverage_moc``: the end-of-run write in ``runner.agg`` (gated on
    # ``get_coverage_moc``) and ``zagg.sweep.MocFamily.finish`` ->
    # ``hive.write_root_coverage``, reached from ``sweep_after_run`` with
    # ``DEFAULT_FAMILIES`` (which carries "moc") and gated only on
    # ``get_sweep``. So ``coverage_moc: false`` plus the default sweep still
    # lands manifest + aggregation.yaml + coverage.moc = 3 root metadata
    # objects; a knob-gated ceiling of 2 would hard-fail that correct store.
    # The ceiling counts the MOC unconditionally; the floor stays the manifest.
    from zagg.config import default_config, get_coverage_moc, get_sweep
    from zagg.sweep import DEFAULT_FAMILIES

    cfg = default_config("atl06")
    cfg.output["store_layout"] = "hive"
    cfg.output["coverage_moc"] = False
    assert get_coverage_moc(cfg) is False  # writer one is OFF...
    assert get_sweep(cfg) is True and "moc" in DEFAULT_FAMILIES  # ...writer two is ON

    exp = bench_objects.expected_object_counts(from_config(cfg), n_shards=1, store_layout="hive")
    assert exp["metadata"] == 3 and exp["metadata_min"] == 1
    measured = {
        "objects_total": 3 + exp["per_shard_max"],
        "objects_metadata": 3,  # manifest + aggregation.yaml + the sweep's MOC
        "objects_per_shard": {_KEY_A: exp["per_shard_max"]},
        "objects_other": 0,
        "other_keys": [],
    }
    assert bench_objects.object_count_mismatch(measured, exp) is None


def test_expected_counts_unknown_layout_raises():
    with pytest.raises(ValueError, match="store_layout"):
        bench_objects.expected_object_counts(_grid(), n_shards=1, store_layout="tree")


# --- measured counts on real stores (flat) ----------------------------------


def test_flat_sharded_store_matches_model(tmp_path):
    # Two populated shards through the production sharded writer: the LIST
    # helper's measured counts equal the model exactly, attributed per shard.
    grid = _grid(sharded=True)
    root = str(tmp_path / "store")
    store = open_store(root)
    grid.emit_template(store)
    words = [morton_word(_KEY_A), morton_word(_KEY_B)]
    for word in words:
        _write_flat_shard(grid, store, word, sharded=True)

    measured = bench_objects.store_object_counts(root, grid=grid, shard_keys=words)
    expected = bench_objects.expected_object_counts(grid, n_shards=2)
    assert expected["exact"] is True
    # Direct writes (no runner) -> no run parquet; the metadata window is
    # exact either way now that root telemetry has its own bucket (#362).
    assert measured["objects_total"] == expected["total_min"] == 5 + 2 * 3
    assert measured["objects_metadata"] == expected["metadata_min"] == 5
    assert measured["objects_telemetry"] == 0
    assert measured["objects_other"] == 0
    assert measured["objects_per_shard"] == {_KEY_A: 3, _KEY_B: 3}
    assert bench_objects.object_count_mismatch(measured, expected) is None


def test_flat_sharded_bypass_is_detected(tmp_path):
    # The issue #215 regression: the same data lands as K per-inner-chunk
    # objects instead of one sharded object per array. Against the sharded
    # model this must read as a hard mismatch.
    grid_flat = _grid(sharded=False)
    root = str(tmp_path / "store")
    store = open_store(root)
    grid_flat.emit_template(store)
    word = morton_word(_KEY_A)
    _write_flat_shard(grid_flat, store, word, sharded=False)

    grid_sharded = _grid(sharded=True)
    measured = bench_objects.store_object_counts(root, grid=grid_sharded, shard_keys=[word])
    expected = bench_objects.expected_object_counts(grid_sharded, n_shards=1)
    mismatch = bench_objects.object_count_mismatch(measured, expected)
    assert mismatch is not None
    assert "total objects" in mismatch

    # Against its OWN (unsharded, bounded) model the same store is in range:
    # all 16 chunks populated -> 16 objects x 3 arrays, attributed to the shard.
    measured_own = bench_objects.store_object_counts(root, grid=grid_flat, shard_keys=[word])
    expected_own = bench_objects.expected_object_counts(grid_flat, n_shards=1)
    assert measured_own["objects_per_shard"] == {_KEY_A: 48}
    assert measured_own["objects_other"] == 0
    assert bench_objects.object_count_mismatch(measured_own, expected_own) is None


def test_flat_stray_object_is_flagged(tmp_path):
    grid = _grid(sharded=True)
    root = str(tmp_path / "store")
    store = open_store(root)
    grid.emit_template(store)
    word = morton_word(_KEY_A)
    _write_flat_shard(grid, store, word, sharded=True)
    (tmp_path / "store" / "stray.debris").write_text("junk")

    measured = bench_objects.store_object_counts(root, grid=grid, shard_keys=[word])
    expected = bench_objects.expected_object_counts(grid, n_shards=1)
    assert measured["objects_other"] == 1
    assert measured["other_keys"] == ["stray.debris"]
    mismatch = bench_objects.object_count_mismatch(measured, expected)
    assert mismatch is not None and "unrecognized" in mismatch


# --- measured counts on a real hive store -----------------------------------


def test_hive_store_matches_model(tmp_path, monkeypatch):
    # End-to-end through the local runner on the hive layout: leaf metadata +
    # dense chunks + the single leaf ragged object + the coverage sidecar per
    # shard, manifest + root coverage MOC at the store root.
    import json

    import zagg.processing as processing
    from zagg import hive, runner
    from zagg.config import (
        default_config,
        get_agg_fields,
        get_data_vars,
        get_output_signature,
    )
    from zagg.runner import agg

    cfg = default_config("atl06")
    cfg.output["store_layout"] = "hive"
    # A ragged field so the leaf carries its whole-leaf vlen array (issue #209).
    cfg.aggregation["variables"]["h"] = {
        "function": "np.sort",
        "source": "h_li",
        "kind": "ragged",
        "inner_shape": [1],
        "dtype": "float32",
        "fill_value": 0,
    }
    grid = from_config(cfg)
    word = morton_word(_KEY_A)

    def carrier(shard_key):
        coords = grid.chunk_coords(shard_key)
        n = len(coords["morton"])
        agg_fields = get_agg_fields(cfg)
        df = pd.DataFrame(
            {
                var: np.ones(n, dtype=np.int32 if var == "count" else np.float32)
                for var in get_data_vars(cfg)
                if get_output_signature(agg_fields[var])["kind"] != "ragged"
            }
        )
        for name, vals in coords.items():
            df[name] = vals
        return df

    def fake_process_shard(g, shard_key, urls, **kwargs):
        kwargs["write_chunk"](
            grid.block_index(int(shard_key)),
            carrier(shard_key),
            {"h": ([np.array([1.0, 2.0], dtype=np.float32)], [0])},
        )
        if kwargs.get("occupied_out") is not None:
            kwargs["occupied_out"].append(np.asarray(grid.children(shard_key)[:5]))
        meta = {
            "shard_key": int(shard_key),
            "cells_with_data": 5,
            "total_obs": 7,
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": None,
        }
        return pd.DataFrame(), meta

    monkeypatch.setattr(processing, "process_shard", fake_process_shard)
    monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": grid.spatial_signature(),
        "shard_keys": [int(word)],
        "granules": [[{"id": "g1", "s3": "s3://b/g1.h5", "https": "https://h/g1.h5"}]],
    }
    cat_path = tmp_path / "catalog.json"
    cat_path.write_text(json.dumps(catalog))
    root = str(tmp_path / "out")
    agg(cfg, catalog=str(cat_path), store=root, backend="local")

    measured = bench_objects.store_object_counts(
        root, grid=grid, shard_keys=[word], store_layout="hive"
    )
    expected = bench_objects.expected_object_counts(grid, n_shards=1, store_layout="hive")
    # K == 1 leaf: every per-array count is deterministic, so the hive model
    # is exact here and the real store matches it object-for-object.
    assert expected["exact"] is True
    # Through the runner: manifest + aggregation.yaml (issue #299) + root MOC.
    assert measured["objects_metadata"] == expected["metadata"] == 3
    # Every object is classified — including the D20 `{window}.stats.json`
    # sidecar each overview leaf now carries (issue #342), which the classifier
    # buckets with its overview since issue #362.
    assert measured["objects_other"] == 0
    assert list(measured["objects_per_shard"]) == [_KEY_A]
    # The run stats parquet (#297) and the sweep run record (#353) are per-run
    # root telemetry: their own unbounded bucket since issue #362.
    assert measured["objects_telemetry"] == 2
    # The end-of-run sweep lands its rollups (issue #300) and overview zarrs
    # (issue #201, with their #342 sidecars) in their own buckets (second-pass
    # D9 caches); the write-path total excludes all three.
    assert measured["objects_rollups"] > 0
    assert measured["objects_sweep"] > 0
    assert any(k.endswith("/all.stats.json") for k in bench_objects.list_store_keys(root))
    write_path = (
        measured["objects_total"]
        - measured["objects_rollups"]
        - measured["objects_sweep"]
        - measured["objects_telemetry"]
    )
    assert write_path == expected["total_max"]
    assert bench_objects.object_count_mismatch(measured, expected) is None
    # Attribution really is the leaf prefix.
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    assert any(k.startswith(leaf) for k in bench_objects.list_store_keys(root))


def test_hive_misplaced_rollup_counts_into_shard(monkeypatch):
    # A `.rollup.json` name is a second-pass sweep cache ONLY when it sits
    # outside every leaf `.zarr/` prefix (issue #300 review). A legit rollup at
    # the digit node lands in ``objects_rollups``; a hand-planted
    # `{leaf}.zarr/stats.rollup.json` is a write-path bypass and must surface
    # as one of that shard's data objects (tripping the exact #215 guard), not
    # vanish into the bucket.
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")  # {node}.zarr
    node = leaf.rsplit("/", 1)[0]  # the digit node dir (rollup lives here)
    keys = [
        f"{leaf}/count/c/0",  # normal in-leaf data object -> this shard
        f"{node}/stats.rollup.json",  # legit sibling rollup -> rollup bucket
        f"{leaf}/stats.rollup.json",  # MISPLACED in-leaf rollup -> this shard
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_rollups"] == 1  # only the sibling rollup
    assert measured["objects_per_shard"] == {label: 2}  # data + misplaced rollup
    assert measured["objects_other"] == 0
    assert not any(k.endswith("stats.rollup.json") for k in measured["other_keys"])


def test_measured_keys_are_the_documented_contract(monkeypatch):
    # The bucket names are the record's grammar, and every reader of them
    # (``write_path_total``) goes through ``.get(..., 0)`` — so a half-done
    # rename reads as "zero objects in that bucket" instead of failing. Pin
    # the key SET, which is what issue #433's ``objects_overviews`` ->
    # ``objects_sweep`` rename moved.
    grid = from_config(_cfg())
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: [])
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[int(morton_word(_KEY_A))], store_layout="hive"
    )
    assert set(measured) == {
        "objects_total",
        "objects_metadata",
        "objects_per_shard",
        "objects_rollups",
        "objects_sweep",
        "objects_telemetry",
        "objects_other",
        "other_keys",
    }


def test_hive_overview_zarrs_count_into_their_own_bucket(monkeypatch):
    # Sweep overview zarrs (issue #201) are `{window}.zarr` / `all.zarr` at a
    # digit node — window tokens can never parse as morton ids, so they get
    # their own D9 bucket; a stray *id*-named zarr outside the dispatched leaf
    # set stays a loud ``other`` finding, and anything inside a leaf prefix
    # still attributes to that shard (the #215 guard is untouched).
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    node = leaf.rsplit("/", 1)[0]
    base = node.split("/", 1)[0]
    keys = [
        f"{leaf}/count/c/0",  # in-leaf data -> this shard
        f"{base}/all.zarr/zarr.json",  # all-time overview at the base node
        f"{node}/2019.zarr/2/count/c/0",  # per-window overview at the node
        f"{node}/overview.rollup.json",  # the family's envelope -> rollups
        f"{base}/-311.zarr/zarr.json",  # stray ID-named zarr -> other
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_sweep"] == 2
    assert measured["objects_rollups"] == 1
    assert measured["objects_per_shard"] == {label: 1}
    assert measured["objects_other"] == 1
    assert measured["other_keys"] == [f"{base}/-311.zarr/zarr.json"]


def test_hive_columns_split_by_who_wrote_them(monkeypatch):
    # Issue #418, the leaf-only scope: `{window}.pyramid.zarr` is one basename
    # grammar with two writers. Under a DISPATCHED leaf's node it is the leaf
    # worker's own write-path artifact (audited, per shard, so the #215 guard
    # covers it); at an ancestor node it is a STAGE column the sweep wrote,
    # which rides the sweep bucket outside the audited total — as does its
    # D20 sidecar, while the leaf column's sidecar stays that shard's.
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    node = leaf.rsplit("/", 1)[0]
    base = node.split("/", 1)[0]
    keys = [
        f"{leaf}/count/c/0",  # in-leaf data -> this shard
        f"{node}/all.pyramid.zarr/zarr.json",  # the LEAF column -> this shard
        f"{node}/all.pyramid.zarr/6/count/c/0",  # ... and its group object
        f"{node}/all.pyramid.stats.json",  # ... and its D20 sidecar
        f"{base}/all.pyramid.zarr/zarr.json",  # a STAGE column -> sweep
        f"{base}/all.pyramid.stats.json",  # ... and its sidecar
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_per_shard"] == {label: 4}
    assert measured["objects_sweep"] == 2
    assert measured["objects_other"] == 0


def test_column_term_is_zero_without_a_v2_declaration():
    # The term keys off the store's own declaration: a `/1` block, a
    # declared-off block and a `/2` block with no leaf-node entry all cost
    # nothing, so a pre-#384 store keeps its pre-#384 count exactly.
    from zagg.pyramid import PYRAMID_SPEC_V2

    assert bench_objects._column_objects(None, 6) == 0
    assert bench_objects._column_objects({"spec": "zagg-pyramid/1", "overview": {}}, 6) == 0
    coarse = {
        "spec": PYRAMID_SPEC_V2,
        "overviews": [{"node": 5, "cells": [7]}],
        "overview": {"fields": {"count": {"class": "exact"}}},
    }
    assert bench_objects._column_objects(coarse, 6) == 0
    # One declared leaf resolution two orders down: groups 8, 7, 6 — one root
    # zarr.json, per group a group zarr.json + (morton + one composable field)
    # * (zarr.json + chunk), and the sidecar. A `none`-class field costs
    # nothing: it exists at native resolution only and no column carries it.
    block = {
        "spec": PYRAMID_SPEC_V2,
        "overviews": [{"node": 6, "cells": [8]}, {"node": 5, "cells": [7]}],
        "overview": {"fields": {"count": {"class": "exact"}, "h": {"class": "none"}}},
    }
    assert bench_objects._column_objects(block, 6) == 1 + 3 * (1 + 2 * 2) + 1


def test_hive_root_telemetry_accumulates_without_a_finding(monkeypatch):
    # Issue #362 (the red-main regression): a benchmark store reused across
    # runs accumulates one ``stats_{ts}_{run_id}.parquet`` (issue #297) and one
    # ``sweep_stats_{ts}.json`` (issue #353) PER RUN — per-run-unique names, so
    # no fixed metadata ceiling can hold. They ride their own unbounded bucket:
    # store-root metadata stays the tight [1, 3] window and the audited
    # write-path total nets them out, however many have piled up.
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    runs = [f"{h:02d}" for h in range(12)]  # twelve runs into one store
    keys = [hive.MANIFEST_NAME, f"{leaf}/count/c/0"]
    keys += [f"stats_202607{r}T000000Z_run{r}.parquet" for r in runs]
    keys += [f"sweep_stats_202607{r}T000000Z.json" for r in runs]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_telemetry"] == 24
    assert measured["objects_metadata"] == 1  # the manifest alone
    assert measured["objects_per_shard"] == {label: 1}
    assert measured["objects_other"] == 0
    expected = {
        "metadata": 3,
        "metadata_min": 1,
        "per_shard_min": 1,
        "per_shard_max": 1,
        "total_min": 2,
        "total_max": 4,
        "exact": True,
    }
    assert bench_objects.object_count_mismatch(measured, expected) is None


def test_hive_node_stats_sidecars_classified(monkeypatch):
    # The PR #356 classifier gap (issue #362): overview leaves carry a D23
    # ``{stem}.stats.json`` sidecar (``telemetry.sidecar_key``, morton-hive/3)
    # at the ANCESTOR node they fold to — an overview object, not an
    # unclassifiable one. A dispatched LEAF node's sidecars keep attributing to
    # their shard across BOTH stems of BOTH grammars — legacy ``stats.json`` /
    # ``shardmap.json`` and D23 ``{window}.stats.json`` /
    # ``{window}.shardmap.json`` (``zagg.sweep.submap_key``) — and a
    # sidecar-named object misplaced INSIDE a leaf prefix still counts as that
    # shard's data object (the issue #215 tripwire's leaf-membership-first
    # ordering is untouched).
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    node = leaf.rsplit("/", 1)[0]  # the dispatched leaf's node dir
    base = node.split("/", 1)[0]  # an ancestor node (overviews fold here)
    keys = [
        hive.MANIFEST_NAME,
        f"{leaf}/count/c/0",  # in-leaf data -> this shard
        f"{node}/stats.json",  # legacy leaf sidecar -> this shard
        f"{node}/shardmap.json",  # leaf sub-map (issue #300) -> this shard
        f"{node}/2019.stats.json",  # D23-named leaf sidecar -> this shard
        f"{node}/2019.shardmap.json",  # D23-named leaf sub-map -> this shard
        f"{leaf}/2019.stats.json",  # MISPLACED in-leaf sidecar -> this shard
        f"{base}/all.zarr/zarr.json",  # the overview zarr itself
        f"{base}/all.stats.json",  # its D23 sidecar -> sweep
        f"{base}/2019.stats.json",  # a per-window overview sidecar
        "stats_20260731T000000Z_run0.parquet",
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_sweep"] == 3  # the zarr + its two sidecars
    assert measured["objects_per_shard"] == {label: 6}
    assert measured["objects_metadata"] == 1
    assert measured["objects_telemetry"] == 1
    # Everything above is classified: no unrecognized keys anywhere.
    assert measured["objects_other"] == 0
    assert measured["other_keys"] == []


def test_hive_stray_stats_json_stays_a_loud_other(monkeypatch):
    # The D23 overview-sidecar branch is ANCHORED to nodes that actually hold
    # an overview zarr (``_overview_node``), not to the ``.stats.json`` suffix
    # alone: a sidecar at the store ROOT, under an arbitrary deep prefix, or at
    # an UNDISPATCHED leaf's node is not an overview sidecar and must stay a
    # loud ``other`` — ``objects_other`` is the "the model knows every object
    # the run writes" contract. A dispatched leaf's sidecar still attributes to
    # its shard (leaf-prefix membership runs first, issue #215).
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    node = leaf.rsplit("/", 1)[0]  # the dispatched leaf's node dir
    base = node.split("/", 1)[0]  # an ancestor node (overviews fold here)
    stray = hive.shard_leaf_path("", int(morton_word(_KEY_B))).lstrip("/").rsplit("/", 1)[0]
    strays = ["all.stats.json", "random/deep/path/x.stats.json", f"{stray}/2019.stats.json"]
    keys = [
        hive.MANIFEST_NAME,
        f"{leaf}/count/c/0",  # in-leaf data -> this shard
        f"{node}/2019.stats.json",  # dispatched leaf's D23 sidecar -> this shard
        f"{base}/all.zarr/zarr.json",  # a real overview zarr -> sweep
        f"{base}/all.stats.json",  # its sidecar, anchored to that node
        *strays,
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: keys)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_sweep"] == 2  # the overview zarr + its sidecar
    assert measured["objects_per_shard"] == {label: 2}
    assert measured["objects_metadata"] == 1
    assert measured["objects_other"] == 3
    assert sorted(measured["other_keys"]) == sorted(strays)


def test_status_prefix_objects_excluded_everywhere(monkeypatch):
    # Issue #327 (ratified amendment): the per-run status channel — per-shard
    # status objects, the dispatch manifest, the issue #151 result envelopes —
    # is run telemetry, never write-path store objects. It must not land in
    # ANY bucket (total, metadata, per-shard, other), so the #215/#240
    # tripwire keeps its exact assertions with the channel active.
    from zagg import hive

    grid = from_config(_cfg())
    word = int(morton_word(_KEY_A))
    label = grid.shard_label(word)
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    data = [hive.MANIFEST_NAME, f"{leaf}/count/c/0"]
    telemetry = [
        "out.zarr.status/run-abc123/shard-42.json",
        "out.zarr.status/run-abc123/manifest.json",
        "out.zarr.status/deadbeef/-4211324.json",  # a #151 result envelope
    ]
    monkeypatch.setattr(bench_objects, "list_store_keys", lambda *a, **k: data + telemetry)
    measured = bench_objects.store_object_counts(
        "unused", grid=grid, shard_keys=[word], store_layout="hive"
    )
    assert measured["objects_total"] == 2
    assert measured["objects_metadata"] == 1
    assert measured["objects_per_shard"] == {label: 1}
    assert measured["objects_other"] == 0


def test_is_status_object_classification():
    assert bench_objects._is_status_object("out.zarr.status/run-a/shard-1.json")
    assert bench_objects._is_status_object("parent/out.zarr.status/run-a/manifest.json")
    # The run parquet stays store-root METADATA (issue #297), not a status key...
    assert not bench_objects._is_status_object("stats_20260101T000000Z_ab.parquet")
    # ...an in-store data object is never telemetry, and a bare file whose NAME
    # ends in .status is not under a status prefix.
    assert not bench_objects._is_status_object("0/012/x.zarr/count/c/0")
    assert not bench_objects._is_status_object("shard-1.status")


# --- mismatch helper (pure) --------------------------------------------------


def test_mismatch_exact_flags_total_and_per_shard():
    expected = {
        "metadata": 6,
        "metadata_min": 6,
        "per_shard_min": 4,
        "per_shard_max": 4,
        "total_min": 10,
        "total_max": 10,
        "exact": True,
    }
    ok = {
        "objects_total": 10,
        "objects_metadata": 6,
        "objects_per_shard": {"1121121": 4},
        "objects_other": 0,
        "other_keys": [],
    }
    assert bench_objects.object_count_mismatch(ok, expected) is None
    blowup = dict(ok, objects_total=1030, objects_per_shard={"1121121": 1024})
    msg = bench_objects.object_count_mismatch(blowup, expected)
    assert "1030" in msg and "1121121" in msg


def test_mismatch_bounded_checks_range_only():
    expected = {
        "metadata": 6,
        "metadata_min": 6,
        "per_shard_min": 3,
        "per_shard_max": 64,
        "total_min": 9,
        "total_max": 70,
        "exact": False,
    }
    inside = {
        "objects_total": 40,
        "objects_metadata": 6,
        "objects_per_shard": {"x": 34},
        "objects_other": 0,
        "other_keys": [],
    }
    assert bench_objects.object_count_mismatch(inside, expected) is None
    over = dict(inside, objects_total=71)
    assert "outside" in bench_objects.object_count_mismatch(over, expected)


# --- run_benchmark._measure_objects (end-to-end, local store) ----------------


def test_measure_objects_end_to_end(tmp_path):
    # The per-merge harness's measurement helper against a real sharded store:
    # clean run -> exact expectation recorded, no mismatch.
    import run_benchmark

    grid = _grid(sharded=True)
    root = str(tmp_path / "store")
    store = open_store(root)
    grid.emit_template(store)
    word = morton_word(_KEY_A)
    _write_flat_shard(grid, store, word, sharded=True)

    cfg = _cfg(sharded=True)
    cfg.output["store_layout"] = "flat"  # a flat-store measurement (issue #253 defaults hive)
    payload = run_benchmark._measure_objects(cfg, grid, root, word, region="us-west-2")
    assert payload == {
        "objects_total": 8,  # 5 metadata + 3 shard objects
        # Nothing to net out on a freshly written store, so the audited
        # write-path count equals the gross total here (issue #362).
        "objects_write_path": 8,
        "objects_expected": 8,  # exact: root telemetry is its own bucket (#362)
        "objects_per_shard": {_KEY_A: 3},
        "objects_telemetry": 0,
        "objects_mismatch": None,
    }


def test_measure_objects_reports_the_netted_count(tmp_path):
    # The reported write-path count must be the SAME figure the mismatch check
    # audits (issue #362) -- so a store carrying D9 caches and accumulated
    # per-run telemetry stays green AND reports a number comparable to
    # ``objects_expected``, instead of a gross total that reads as a blowup.
    measured = {
        "objects_total": 48,
        "objects_metadata": 3,
        "objects_per_shard": {"1121121": 13},
        "objects_rollups": 5,
        "objects_sweep": 3,
        "objects_telemetry": 24,
        "objects_other": 0,
        "other_keys": [],
    }
    assert bench_objects.write_path_total(measured) == 16
    expected = {
        "metadata": 3,
        "metadata_min": 1,
        "per_shard_min": 13,
        "per_shard_max": 13,
        "total_min": 14,
        "total_max": 16,
        "exact": True,
    }
    # Green on the netted figure, which is 32 objects below the gross total.
    assert bench_objects.object_count_mismatch(measured, expected) is None


def test_measure_objects_flags_bypass(tmp_path):
    # A store written per-inner-chunk (the issue #215 bypass) while the config
    # and grid promise sharded output (as run_target derives them) must come
    # back with a mismatch description for main() to hard-fail on.
    import run_benchmark

    grid_flat = _grid(sharded=False)
    root = str(tmp_path / "store")
    store = open_store(root)
    grid_flat.emit_template(store)
    word = morton_word(_KEY_A)
    _write_flat_shard(grid_flat, store, word, sharded=False)

    cfg = _cfg(sharded=True)
    cfg.output["store_layout"] = "flat"  # a flat-store measurement (issue #253 defaults hive)
    payload = run_benchmark._measure_objects(
        cfg, _grid(sharded=True), root, word, region="us-west-2"
    )
    assert payload["objects_mismatch"] is not None
    assert payload["objects_total"] == 5 + 48  # metadata + 16 chunks x 3 arrays
    assert payload["objects_expected"] == 8  # exact metadata + 3 sharded objects


# --- review folds (PR #242) ---------------------------------------------------


def test_flat_model_requires_fullsphere(tmp_path):
    # The flat block arithmetic assumes fullsphere HEALPix: a rect grid must
    # fail loudly (NotImplementedError), not mis-attribute or die on a bare
    # AttributeError (review, PR #242). (The dense HEALPix layout the fence
    # also guarded was removed — issue #88.)
    from zagg.grids import RectilinearGrid

    rect = RectilinearGrid(
        crs="EPSG:32618",
        resolution=10,
        bounds=[358300, 4299600, 370300, 4311600],
        chunk_shape=(300, 300),
    )
    for grid in (rect,):
        with pytest.raises(NotImplementedError, match="fullsphere"):
            bench_objects.expected_object_counts(grid, n_shards=1)
        with pytest.raises(NotImplementedError, match="fullsphere"):
            bench_objects.store_object_counts(str(tmp_path), grid=grid, shard_keys=[])
    # The hive path attributes by leaf prefix (layout-agnostic) -- unaffected.
    assert bench_objects.expected_object_counts(
        _grid(sharded=False), n_shards=1, store_layout="hive"
    )


def test_list_store_keys_absent_local_path_raises(tmp_path):
    # open_object_store mkdir's an absent local path; a mistyped store must
    # fail as "not found", not count as an empty store (review, PR #242).
    missing = tmp_path / "typo.zarr"
    with pytest.raises(FileNotFoundError, match="store not found"):
        bench_objects.list_store_keys(str(missing))
    assert not missing.exists()  # and no stray directory was created


def test_mismatch_flags_metadata_drift():
    # Metadata is checked unconditionally: an extra zarr.json (the issue #215
    # CSR-subgroup footprint) offset by a missing data object must name the
    # metadata bucket, not just the total (review, PR #242).
    expected = {
        "metadata": 6,
        "metadata_min": 6,
        "per_shard_min": 4,
        "per_shard_max": 4,
        "total_min": 10,
        "total_max": 10,
        "exact": True,
    }
    measured = {
        "objects_total": 10,  # compensated: +1 metadata, -1 data
        "objects_metadata": 7,
        "objects_per_shard": {"1121121": 3},
        "objects_other": 0,
        "other_keys": [],
    }
    msg = bench_objects.object_count_mismatch(measured, expected)
    assert "metadata objects 7 != expected 6" in msg


def test_expected_counts_sane_for_every_manifest_config():
    # Every config referenced by either manifest (live matrix, provisional,
    # 88s, cached, full-AOI) must resolve through the model without error and
    # with sane structure -- so an unpinned provisional target can't hit the
    # hard-fail tripwire with a config the model has never seen (review,
    # PR #242).
    import json

    configs = set()
    for manifest_name in ("targets.json", "targets_full_aoi_neon.json"):
        manifest = json.loads((BENCH / manifest_name).read_text())
        for block in ("targets", "provisional_targets"):
            for tname, t in manifest.get(block, {}).items():
                if isinstance(t, dict) and "config" in t:
                    configs.add(t["config"])
    assert configs  # the manifests define targets
    for rel in sorted(configs):
        grid = from_config(load_config(str(BENCH / rel)))
        exp = bench_objects.expected_object_counts(grid, n_shards=1)
        assert exp["metadata"] >= 3, rel  # root + group + >=1 array
        assert 1 <= exp["per_shard_min"] <= exp["per_shard_max"], rel
        assert exp["total_max"] == exp["metadata"] + exp["per_shard_max"], rel


@pytest.mark.parametrize("pyramid", [True, False], ids=["default", "pyramid-off"])
def test_hive_sharded_store_matches_model(tmp_path, monkeypatch, pyramid):
    # Post issue #236: a sharded K>1 hive leaf writes ONE ShardingCodec object
    # per dense array (and one ragged object), so the hive model is EXACT.
    # End-to-end through the local runner (real process_and_write_hive +
    # write_leaf_to_zarr; only process_shard is faked, honoring the accumulate
    # contract), mirroring test_hive.test_local_hive_sharded_leaf_single_object.
    #
    # Both declaration shapes (issue #418): the DEFAULT, where the issue #384
    # /2 flip makes every leaf worker write its own column artifact, and the
    # `pyramid: false` opt-out that writes none. The audited posture is the
    # default one — no escape hatch — and the opt-out is kept so the column
    # term is pinned as a term (it must vanish, not merely be tolerated).
    import json

    import test_hive as th

    import zagg.processing as processing
    from zagg import runner
    from zagg.config import default_config
    from zagg.runner import agg

    cfg = default_config("atl06")
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"]["chunk_inner"] = 8  # K = 16; sharded defaults True (#236)
    if not pyramid:
        cfg.output["pyramid"] = False
    cfg.aggregation["variables"]["h"] = {
        "function": "np.sort",
        "source": "h_li",
        "kind": "ragged",
        "inner_shape": [1],
        "dtype": "float32",
        "fill_value": 0,
    }
    grid = from_config(cfg)
    assert grid.sharded is True and grid.chunks_per_shard == 16
    shard = th._shard_word()
    fake = th._sharded_accumulate_fake(
        grid,
        th.TestProcessAndWriteHiveSharded._chunk_carrier,
        th.TestProcessAndWriteHiveSharded._meta,
        {0: {"h": ([np.array([2.5], dtype=np.float32)], [1])}},
        occupied=grid.children(shard)[:3],
    )
    monkeypatch.setattr(processing, "process_shard", fake)
    monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": grid.spatial_signature(),
        "shard_keys": [int(shard)],
        "granules": [[{"id": "g1", "s3": "s3://b/g1.h5", "https": "https://h/g1.h5"}]],
    }
    cat_path = tmp_path / "catalog.json"
    cat_path.write_text(json.dumps(catalog))
    root = str(tmp_path / "out")
    agg(cfg, catalog=str(cat_path), store=root, backend="local")

    from zagg.hive import read_manifest

    block = read_manifest(root)["pyramid"]
    expected = bench_objects.expected_object_counts(
        grid, n_shards=1, store_layout="hive", pyramid=block
    )
    measured = bench_objects.store_object_counts(
        root, grid=grid, shard_keys=[shard], store_layout="hive"
    )
    # Exact: per leaf = root+group zarr.json (2) + one zarr.json AND one data
    # object per array + the coverage sidecar + the stats.json sibling
    # (issue #297); store root = manifest + aggregation.yaml (issue #299)
    # + MOC (the run parquet / sweep record are telemetry — issue #362).
    n_arrays = len(grid.shard_spec().members)
    assert expected["exact"] is True
    # The leaf pyramid column (issue #418): one root zarr.json + per declared
    # group a group zarr.json and a zarr.json + chunk object for `morton` and
    # each composable field, + the D20 sidecar. Zero under the opt-out.
    column = bench_objects._column_objects(block, grid.parent_order)
    assert column == (1 + 3 * (1 + 2 * (1 + 3)) + 1 if pyramid else 0)
    # ... + the stats.json, granules.json and shardmap.json siblings
    # (issues #297/#388/#300).
    assert expected["per_shard_max"] == 2 + 2 * n_arrays + 1 + 3 + column
    assert expected["metadata"] == 3
    # Sweep rollups (issue #300), overview zarrs (issue #201 — with the D20
    # `{window}.stats.json` sidecar each overview leaf carries since issue
    # #342) and per-run root telemetry (issue #362) ride their own buckets,
    # outside the write-path total this model audits. Nothing is left over.
    write_path = (
        measured["objects_total"]
        - measured["objects_rollups"]
        - measured["objects_sweep"]
        - measured["objects_telemetry"]
    )
    assert write_path == expected["total_max"]
    assert measured["objects_other"] == 0
    assert bench_objects.object_count_mismatch(measured, expected) is None
