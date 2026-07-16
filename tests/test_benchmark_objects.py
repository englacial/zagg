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
                "cell_ids": {"dtype": "uint64", "fill_value": 0},
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
    # The live-matrix config (flat, sharded, K=256): root + group + 4 array
    # zarr.json objects, then exactly one object per array per shard.
    config = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9.yaml"))
    exp = bench_objects.expected_object_counts(from_config(config), n_shards=1)
    assert exp == {
        "metadata": 6,
        "per_shard_min": 4,
        "per_shard_max": 4,
        "total_min": 10,
        "total_max": 10,
        "exact": True,
    }


def test_expected_counts_aoimask_config_adds_one_array():
    # The mask arm adds the aoi_mask bool array: one more zarr.json + one more
    # sharded object per shard.
    config = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9_aoimask.yaml"))
    exp = bench_objects.expected_object_counts(from_config(config), n_shards=4)
    assert exp["metadata"] == 7
    assert exp["per_shard_min"] == exp["per_shard_max"] == 5
    assert exp["exact"] is True
    assert exp["total_max"] == 7 + 4 * 5


def test_expected_counts_unsharded_is_bounded_not_exact():
    # Unsharded at K=16: dense arrays write 1..K chunk objects (empty inner
    # chunks are omitted), ragged 0..K -- a bounded, non-exact expectation.
    exp = bench_objects.expected_object_counts(_grid(sharded=False), n_shards=1)
    k = 16
    assert exp["metadata"] == 6
    assert exp["exact"] is False
    assert exp["per_shard_min"] == 3  # one populated chunk x 3 dense arrays
    assert exp["per_shard_max"] == 4 * k


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
    assert measured["objects_total"] == expected["total_max"] == 6 + 2 * 4
    assert measured["objects_metadata"] == expected["metadata"] == 6
    assert measured["objects_other"] == 0
    assert measured["objects_per_shard"] == {_KEY_A: 4, _KEY_B: 4}
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
    # all 16 chunks populated -> 16 objects x 4 arrays, attributed to the shard.
    measured_own = bench_objects.store_object_counts(root, grid=grid_flat, shard_keys=[word])
    expected_own = bench_objects.expected_object_counts(grid_flat, n_shards=1)
    assert measured_own["objects_per_shard"] == {_KEY_A: 64}
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
        get_coverage_moc,
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
        n = len(coords["cell_ids"])
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
    expected = bench_objects.expected_object_counts(
        grid, n_shards=1, store_layout="hive", coverage_moc=get_coverage_moc(cfg)
    )
    # K == 1 leaf: every per-array count is deterministic, so the hive model
    # is exact here and the real store matches it object-for-object.
    assert expected["exact"] is True
    assert measured["objects_metadata"] == expected["metadata"] == 2
    assert measured["objects_other"] == 0
    assert list(measured["objects_per_shard"]) == [_KEY_A]
    assert measured["objects_total"] == expected["total_max"]
    assert bench_objects.object_count_mismatch(measured, expected) is None
    # Attribution really is the leaf prefix.
    leaf = hive.shard_leaf_path("", word).lstrip("/")
    assert any(k.startswith(leaf) for k in bench_objects.list_store_keys(root))


# --- mismatch helper (pure) --------------------------------------------------


def test_mismatch_exact_flags_total_and_per_shard():
    expected = {
        "metadata": 6,
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

    payload = run_benchmark._measure_objects(
        _cfg(sharded=True), grid, root, word, region="us-west-2"
    )
    assert payload == {
        "objects_total": 10,  # 6 metadata + 4 shard objects
        "objects_expected": 10,
        "objects_per_shard": {_KEY_A: 4},
        "objects_mismatch": None,
    }


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

    payload = run_benchmark._measure_objects(
        _cfg(sharded=True), _grid(sharded=True), root, word, region="us-west-2"
    )
    assert payload["objects_mismatch"] is not None
    assert payload["objects_total"] == 6 + 64  # metadata + 16 chunks x 4 arrays
    assert payload["objects_expected"] == 10


# --- review folds (PR #242) ---------------------------------------------------


def test_flat_model_requires_fullsphere(tmp_path):
    # The flat block arithmetic assumes fullsphere HEALPix: a dense-layout grid
    # or a rect grid must fail loudly (NotImplementedError), not mis-attribute
    # or die on a bare AttributeError (review, PR #242).
    from zagg.grids import RectilinearGrid

    cfg = _cfg(sharded=False)
    dense = HealpixGrid(6, 12, layout="dense", config=cfg, populated_shards=[1])
    rect = RectilinearGrid(
        crs="EPSG:32618",
        resolution=10,
        bounds=[358300, 4299600, 370300, 4311600],
        chunk_shape=(300, 300),
    )
    for grid in (dense, rect):
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


def test_hive_sharded_store_matches_model(tmp_path, monkeypatch):
    # Post issue #236: a sharded K>1 hive leaf writes ONE ShardingCodec object
    # per dense array (and one ragged object), so the hive model is EXACT.
    # End-to-end through the local runner (real process_and_write_hive +
    # write_leaf_to_zarr; only process_shard is faked, honoring the accumulate
    # contract), mirroring test_hive.test_local_hive_sharded_leaf_single_object.
    import json

    import test_hive as th

    import zagg.processing as processing
    from zagg import runner
    from zagg.config import default_config, get_coverage_moc
    from zagg.runner import agg

    cfg = default_config("atl06")
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"]["chunk_inner"] = 8  # K = 16; sharded defaults True (#236)
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

    expected = bench_objects.expected_object_counts(
        grid, n_shards=1, store_layout="hive", coverage_moc=get_coverage_moc(cfg)
    )
    measured = bench_objects.store_object_counts(
        root, grid=grid, shard_keys=[shard], store_layout="hive"
    )
    # Exact: per leaf = root+group zarr.json (2) + one zarr.json AND one data
    # object per array + the coverage sidecar; store root = manifest + MOC.
    n_arrays = len(grid.shard_spec().members)
    assert expected["exact"] is True
    assert expected["per_shard_max"] == 2 + 2 * n_arrays + 1
    assert expected["metadata"] == 2
    assert measured["objects_total"] == expected["total_max"]
    assert measured["objects_other"] == 0
    assert bench_objects.object_count_mismatch(measured, expected) is None
