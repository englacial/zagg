"""Tests for the morton-hive store layout — issue #199 phase 2.

Covers the config flag, leaf-path computation + node invariant (D2/D3/D5),
the ``morton_hive.json`` manifest (D6), the commit stamp / debris / torn-write
retry semantics (D4), and the local runner's hive write path.
"""

import json
import os
from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest
import zarr
from zarr.storage import MemoryStore

from zagg import hive
from zagg.config import default_config, get_data_vars, validate_config
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_decimal, morton_word


@pytest.fixture
def cfg():
    return default_config("atl06")


def _shard_word(order=6):
    """A real southern packed shard word (decimal form ``-5112333`` at order 6)."""
    from mortie import geo2mort

    return int(geo2mort(np.array([-78.5]), np.array([-132.0]), order=order)[0])


# ── config flag ──────────────────────────────────────────────────────────────


class TestStoreLayoutConfig:
    def test_default_is_flat(self, cfg):
        from zagg.config import get_store_layout

        assert get_store_layout(cfg) == "flat"
        validate_config(cfg)  # flat default validates unchanged

    def test_hive_accepted_for_healpix(self, cfg):
        cfg.output["store_layout"] = "hive"
        validate_config(cfg)

    def test_null_key_falls_back_to_flat(self, cfg):
        from zagg.config import get_store_layout

        cfg.output["store_layout"] = None
        assert get_store_layout(cfg) == "flat"
        validate_config(cfg)

    def test_unknown_value_rejected(self, cfg):
        cfg.output["store_layout"] = "tree"
        with pytest.raises(ValueError, match="store_layout"):
            validate_config(cfg)

    def test_hive_rejects_rectilinear(self, cfg):
        cfg.output["store_layout"] = "hive"
        cfg.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        with pytest.raises(ValueError, match="healpix"):
            validate_config(cfg)

    def test_hive_rejects_sharded(self, cfg):
        cfg.output["store_layout"] = "hive"
        cfg.output.setdefault("grid", {})["sharded"] = True
        with pytest.raises(ValueError, match="sharded"):
            validate_config(cfg)

    def test_hive_rejects_consolidate_metadata(self, cfg):
        cfg.output["store_layout"] = "hive"
        cfg.output["consolidate_metadata"] = True
        with pytest.raises(ValueError, match="consolidate"):
            validate_config(cfg)


# ── leaf paths + node invariant ──────────────────────────────────────────────


class TestLeafPath:
    def test_matches_mortie_hive_path(self):
        # The convention is owned by the mortie spec: zagg's leaf path must be
        # exactly mortie's hive_path under the store root.
        from mortie import MortonIndexArray

        word = _shard_word()
        expected = MortonIndexArray.from_words(np.asarray([word], dtype=np.uint64)).hive_path(
            root="s3://b/root"
        )[0]
        assert hive.shard_leaf_path("s3://b/root", word) == expected

    def test_one_digit_per_level_full_id_leaf(self):
        # D2/D3: sign+base, one digit per order, full decimal id at the leaf.
        word = _shard_word()
        assert morton_decimal(word) == "-5112333"
        assert hive.shard_leaf_path("root", word) == "root/-5/1/1/2/3/3/3/-5112333.zarr"

    def test_trailing_slash_root_normalized(self):
        word = _shard_word()
        assert hive.shard_leaf_path("root/", word) == hive.shard_leaf_path("root", word)

    def test_negative_key_rejected(self):
        # A signed legacy id is the DECIMAL form, not a packed word.
        with pytest.raises(ValueError, match="packed morton word"):
            hive.shard_leaf_path("root", -4211322)

    def test_node_invariant_accepts_computed_paths(self):
        for order in (1, 6, 11):
            word = _shard_word(order)
            s = morton_decimal(word)
            head = 2 if s.startswith("-") else 1
            rel = "/".join([s[:head], *s[head:]]) + f"/{s}.zarr"
            hive.check_node_invariant(rel)

    @pytest.mark.parametrize(
        "bad",
        [
            "-4211322.zarr",  # bare leaf: no digit chain at all
            "0/1/01.zarr",  # base digit 0
            "-4/5/-45.zarr",  # order digit outside 1..4
            "-4/2/-43.zarr",  # leaf id does not match the chain
            "-4/2/-42",  # not a .zarr leaf
            "-4/21/-421.zarr",  # grouped digits (one digit per level, D2)
        ],
    )
    def test_node_invariant_rejects(self, bad):
        with pytest.raises(ValueError, match="node invariant"):
            hive.check_node_invariant(bad)


# ── manifest (D6) ────────────────────────────────────────────────────────────


class TestManifest:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_build_contents(self, cfg):
        m = hive.build_manifest(self._grid(cfg), dataset={"short_name": "ATL06", "version": "007"})
        assert m["spec"] == "morton-hive/1"
        assert m["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert m["cell_order"] == 8
        assert m["shard_order"] == 6
        # Explicit split schedule: one digit per level down to the shard order.
        assert m["split_schedule"] == [1] * 6
        # Declared-only in round one (populated by the pyramid sweep, D11).
        assert m["pyramid"] == {"orders": [], "aggregation": {}}
        assert m["generated_at"]

    def test_ensure_write_read_round_trip(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        m = hive.build_manifest(self._grid(cfg))
        assert hive.ensure_manifest(root, m) == m
        assert hive.read_manifest(root) == m
        # The object is the root-only exception: it lives at the root, as JSON.
        assert json.loads((tmp_path / "store" / hive.MANIFEST_NAME).read_text()) == m

    def test_rerun_with_matching_manifest_is_accepted(self, cfg, tmp_path):
        # Retry semantics (D4): a rerun into the same root must proceed.
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        hive.ensure_manifest(root, hive.build_manifest(grid))
        again = hive.build_manifest(grid)  # fresh generated_at
        assert hive.ensure_manifest(root, again)["spec"] == "morton-hive/1"

    def test_rerun_ignores_sweep_mutated_pyramid(self, cfg, tmp_path):
        # The pyramid block is populated/updated by the §7 sweep BY DESIGN
        # (D11), so the resume match-check must not compare it — else the
        # first sweep would brick every later resume (review finding, PR #205).
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        swept = hive.build_manifest(grid)
        swept["pyramid"] = {"orders": [4, 5], "aggregation": {"count": "sum"}}
        hive.ensure_manifest(root, swept)
        # A later run's fresh (declared-only) manifest still resumes, and the
        # sweep's pyramid declaration is preserved, not clobbered.
        resumed = hive.ensure_manifest(root, hive.build_manifest(grid))
        assert resumed["pyramid"] == swept["pyramid"]

    def test_mismatched_manifest_says_clear_the_root(self, cfg, tmp_path):
        # overwrite=True replaces the manifest ONLY; the remedy must not
        # suggest it for an orders change (review finding, PR #205).
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(self._grid(cfg)))
        other = HealpixGrid(parent_order=5, child_order=8, layout="fullsphere", config=cfg)
        with pytest.raises(ValueError, match="clear the store root"):
            hive.ensure_manifest(root, hive.build_manifest(other))

    def test_overwrite_replaces_when_tree_is_empty(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(self._grid(cfg)))
        other = HealpixGrid(parent_order=5, child_order=8, layout="fullsphere", config=cfg)
        hive.ensure_manifest(root, hive.build_manifest(other), overwrite=True)
        assert hive.read_manifest(root)["shard_order"] == 5

    def test_overwrite_with_changed_orders_refuses_over_existing_shards(self, cfg, tmp_path):
        # Committed leaves from the old orders would survive a manifest-only
        # "re-template" as walker-discoverable, stamped, seemingly-legal
        # mixed-order data (D2) — refuse via one delimiter-LIST (review
        # finding, PR #205).
        root = tmp_path / "store"
        hive.ensure_manifest(str(root), hive.build_manifest(self._grid(cfg)))
        (root / "-5" / "1").mkdir(parents=True)  # a {sign+base} child exists
        (root / "-5" / "1" / "obj").write_text("x")
        other = HealpixGrid(parent_order=5, child_order=8, layout="fullsphere", config=cfg)
        with pytest.raises(ValueError, match="clear the store root first"):
            hive.ensure_manifest(str(root), hive.build_manifest(other), overwrite=True)

    def test_overwrite_with_same_orders_allowed_over_existing_shards(self, cfg, tmp_path):
        # Same frozen keys -> replacing the manifest is safe even with data.
        root = tmp_path / "store"
        grid = self._grid(cfg)
        hive.ensure_manifest(str(root), hive.build_manifest(grid))
        (root / "-5" / "1").mkdir(parents=True)
        (root / "-5" / "1" / "obj").write_text("x")
        hive.ensure_manifest(str(root), hive.build_manifest(grid), overwrite=True)

    def test_read_absent_returns_none(self, tmp_path):
        assert hive.read_manifest(str(tmp_path / "empty")) is None


# ── leaf template + commit stamp (D3/D4) ─────────────────────────────────────


class TestLeafTemplateAndStamp:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_leaf_template_is_shard_sized(self, cfg):
        g = self._grid(cfg)
        store = MemoryStore()
        g.emit_shard_template(store, overwrite=True)
        grp = zarr.open_group(store, path=g.group_path, mode="r", zarr_format=3)
        for name in ("morton", "cell_ids", *get_data_vars(cfg)):
            assert grp[name].shape == (g.cells_per_shard,)
            assert grp[name].chunks == (g.cells_per_chunk,)

    def test_leaf_has_root_group_for_the_stamp(self, cfg):
        # D4: the stamp is one attrs update on an object that exists anyway.
        store = MemoryStore()
        self._grid(cfg).emit_shard_template(store, overwrite=True)
        root = zarr.open_group(store, path="", mode="r", zarr_format=3)
        assert hive.COMMIT_ATTR not in root.attrs  # fresh leaf is unstamped

    def test_emit_is_idempotent_with_overwrite(self, cfg):
        store = MemoryStore()
        g = self._grid(cfg)
        g.emit_shard_template(store, overwrite=True)
        g.emit_shard_template(store, overwrite=True)  # retry over debris

    def test_sharded_grid_rejected(self, cfg):
        g = HealpixGrid(6, 10, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        with pytest.raises(ValueError, match="sharded"):
            g.emit_shard_template(MemoryStore())

    def test_stamp_round_trip_and_debris_semantics(self, cfg):
        store = MemoryStore()
        self._grid(cfg).emit_shard_template(store, overwrite=True)
        # An unstamped prefix is debris: present, but not complete.
        assert hive.read_commit(store) is None
        hive.stamp_commit(store, cells_with_data=5, granule_count=2)
        stamp = hive.read_commit(store)
        assert stamp["complete"] is True
        assert stamp["spec"] == hive.HIVE_SPEC
        assert stamp["cells_with_data"] == 5
        assert stamp["granule_count"] == 2
        assert stamp["written_at"]

    def test_read_commit_absent_leaf_is_none(self):
        # Walker termination: no leaf at all is the same answer as debris.
        assert hive.read_commit(MemoryStore()) is None


# ── local write path (runner) ────────────────────────────────────────────────


def _rec(n):
    return {"id": f"g{n}", "s3": f"s3://bucket/granule{n}.h5", "https": f"https://h/g{n}.h5"}


class TestProcessAndWriteHive:
    """Drive ``hive.process_and_write_hive`` with a fake ``process_shard`` that
    streams REAL carriers, so the leaf template, dense write, CSR naming, and
    stamp ordering are all exercised against real zarr stores."""

    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def _carrier(self, grid, shard):
        coords = grid.chunk_coords(shard)
        n = len(coords["cell_ids"])
        df = pd.DataFrame(
            {
                var: np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
                for var in get_data_vars(grid.config)
            }
        )
        for name, vals in coords.items():
            df[name] = vals
        return df

    def _meta(self, shard, error=None):
        return {
            "shard_key": int(shard),
            "cells_with_data": 5,
            "total_obs": 7,
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": error,
        }

    def _run(self, monkeypatch, cfg, tmp_path, fake):
        import zagg.processing as processing

        monkeypatch.setattr(processing, "process_shard", fake)
        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        meta = hive.process_and_write_hive(
            shard, ["s3://bucket/granule1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        return grid, shard, root, meta

    def _streaming_fake(self, grid, ragged=None):
        def fake(g, shard_key, urls, **kwargs):
            carrier = self._carrier(grid, shard_key)
            kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, ragged or {})
            return pd.DataFrame(), self._meta(shard_key)

        return fake

    def test_leaf_written_and_stamped(self, monkeypatch, cfg, tmp_path):
        grid_probe = self._grid(cfg)
        fake = self._streaming_fake(grid_probe, ragged={"h": ([np.array([1.0, 2.0])], [0])})
        grid, shard, root, meta = self._run(monkeypatch, cfg, tmp_path, fake)

        leaf = hive.shard_leaf_path(root, shard)
        from zagg.store import open_store

        leaf_store = open_store(leaf)
        # Dense data landed at the leaf-LOCAL block 0.
        grp = zarr.open_group(leaf_store, path=grid.group_path, mode="r", zarr_format=3)
        np.testing.assert_array_equal(
            np.asarray(grp["cell_ids"][:]),
            np.asarray(grid.chunk_coords(shard)["cell_ids"]),
        )
        # CSR subgroup named by the shard label (decimal morton string).
        label = morton_decimal(shard)
        assert morton_word(label) == shard
        sub = zarr.open_group(leaf_store, path=f"{grid.group_path}/h/{label}", mode="r")
        assert "values" in sub.array_keys()
        # The commit stamp is present and carries the worker's counters (D4).
        stamp = hive.read_commit(leaf_store)
        assert stamp["complete"] is True
        assert stamp["cells_with_data"] == meta["cells_with_data"]
        assert stamp["granule_count"] == meta["granule_count"]

    def test_no_data_shard_leaves_no_prefix(self, monkeypatch, cfg, tmp_path):
        # The leaf is created lazily on the first chunk write, so a no-data
        # shard leaves NO .zarr/ prefix (absence stays trustworthy).
        def fake(g, shard_key, urls, **kwargs):
            return pd.DataFrame(), self._meta(shard_key, error="No granules found")

        grid, shard, root, meta = self._run(monkeypatch, cfg, tmp_path, fake)
        leaf = hive.shard_leaf_path(root, shard)
        assert not os.path.exists(leaf)

    def test_torn_write_leaves_debris_then_retry_succeeds(self, monkeypatch, cfg, tmp_path):
        # Torn-write simulation: the worker dies after the dense write, before
        # the stamp. The prefix exists (debris), read_commit says incomplete,
        # and a clean retry overwrites it WHOLESALE and stamps. The torn
        # attempt also writes a CSR subgroup the retry does NOT rewrite, so
        # the wholesale claim is pinned against upstream drift: if the leaf
        # re-template merely re-put metadata instead of delete_dir-ing the
        # prefix, the stale subgroup would survive inside a leaf whose stamp
        # certifies it complete (review finding, PR #205).
        import zagg.processing as processing
        from zagg.store import open_store

        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        leaf = hive.shard_leaf_path(root, shard)

        def torn(g, shard_key, urls, **kwargs):
            carrier = self._carrier(grid, shard_key)
            stale_ragged = {"h": ([np.array([1.0])], [0])}
            kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, stale_ragged)
            raise RuntimeError("worker died mid-shard")

        monkeypatch.setattr(processing, "process_shard", torn)
        with pytest.raises(RuntimeError, match="died mid-shard"):
            hive.process_and_write_hive(
                shard, ["s3://bucket/g1.h5"], grid, {}, root, cfg, store_kwargs={}
            )
        assert os.path.exists(leaf)  # the prefix exists...
        assert hive.read_commit(open_store(leaf)) is None  # ...but is debris
        # No stamp -> no coverage visible either (issue #200): the tier-0
        # payload rides the stamp, so a torn worker never publishes coverage.
        assert hive.read_coverage(open_store(leaf)) is None
        stale = os.path.join(leaf, grid.group_path, "h", morton_decimal(shard))
        assert os.path.exists(stale)  # the torn attempt's CSR subgroup

        # Retry (no ragged this time): same leaf, overwritten wholesale —
        # the stale subgroup is GONE — and stamped at the end.
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid))
        hive.process_and_write_hive(
            shard, ["s3://bucket/g1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        assert hive.read_commit(open_store(leaf))["complete"] is True
        assert not os.path.exists(stale), "stale torn-write object survived the re-template"

    def test_errored_shard_is_not_stamped(self, monkeypatch, cfg, tmp_path):
        # A shard that wrote chunks but ended in error stays unstamped debris.
        from zagg.store import open_store

        grid_probe = self._grid(cfg)

        def fake(g, shard_key, urls, **kwargs):
            carrier = self._carrier(grid_probe, shard_key)
            kwargs["write_chunk"](grid_probe.block_index(int(shard_key)), carrier, {})
            return pd.DataFrame(), self._meta(shard_key, error="No data after filtering (1 ...)")

        grid, shard, root, _meta = self._run(monkeypatch, cfg, tmp_path, fake)
        leaf = hive.shard_leaf_path(root, shard)
        assert os.path.exists(leaf)
        assert hive.read_commit(open_store(leaf)) is None

    def test_tree_walk_node_invariant(self, monkeypatch, cfg, tmp_path):
        # Walker semantics (D5): below the root only digit dirs and *.zarr
        # nodes; no zarr metadata above the leaf; the root additionally holds
        # only the manifest. A LIST with no digit children is thus a
        # definitive "nothing finer exists".
        grid_probe = self._grid(cfg)
        fake = self._streaming_fake(grid_probe)
        grid, shard, root, _meta = self._run(monkeypatch, cfg, tmp_path, fake)
        hive.ensure_manifest(root, hive.build_manifest(grid))

        for dirpath, dirnames, filenames in os.walk(root):
            if dirpath == root:
                assert filenames == [hive.MANIFEST_NAME]
                base = [d[1:] if d.startswith("-") else d for d in dirnames]
                assert all(len(b) == 1 and b in "123456" for b in base)
                continue
            if dirpath.endswith(".zarr") or ".zarr" + os.sep in dirpath:
                continue  # inside a leaf: vanilla zarr v3, its own business
            # An intermediate digit node: no objects (zarr.json or otherwise),
            # only digit children and leaf dirs.
            assert filenames == [], f"object above the leaf at {dirpath}: {filenames}"
            for d in dirnames:
                assert d.endswith(".zarr") or (len(d) == 1 and d in "1234"), (
                    f"non-hive child {d!r} at {dirpath}"
                )

    def test_stamp_is_the_final_write(self, monkeypatch, cfg, tmp_path):
        """D4 ordering pin (review finding, PR #205): the commit stamp is the
        shard's LAST write — presence certifies everything before it landed.
        ONE test covers BOTH backends: the local dispatcher and the Lambda
        handler execute this same ``process_and_write_hive`` function, so the
        op ordering cannot diverge between them."""
        import zagg.processing as processing

        ops: list = []

        def rec(name, fn):
            def wrapped(*a, **k):
                ops.append(name)
                return fn(*a, **k)

            return wrapped

        grid = self._grid(cfg)
        fake = self._streaming_fake(grid, ragged={"h": ([np.array([1.0])], [0])})
        monkeypatch.setattr(processing, "process_shard", fake)
        monkeypatch.setattr(
            processing, "write_dataframe_to_zarr", rec("dense", processing.write_dataframe_to_zarr)
        )
        monkeypatch.setattr(
            processing, "write_ragged_to_zarr", rec("ragged", processing.write_ragged_to_zarr)
        )
        monkeypatch.setattr(hive, "stamp_commit", rec("stamp", hive.stamp_commit))
        hive.process_and_write_hive(
            _shard_word(), ["s3://b/g1.h5"], grid, {}, str(tmp_path / "store"), cfg, store_kwargs={}
        )
        assert ops == ["dense", "ragged", "stamp"]


class TestLeafBlockIndex:
    def test_k1_maps_to_zero(self, cfg):
        g = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        shard = _shard_word()
        (block,) = [b for b, _ in g.iter_chunks(shard)]
        assert hive.leaf_block_index(g, block, shard) == (0,)

    def test_k_gt_1_enumerates_local_ordinals(self, cfg):
        g = HealpixGrid(6, 10, layout="fullsphere", config=cfg, chunk_inner=8)
        assert g.chunks_per_shard == 16
        shard = _shard_word()
        locals_ = [hive.leaf_block_index(g, b, shard) for b, _ in g.iter_chunks(shard)]
        assert locals_ == [(i,) for i in range(16)]


class TestRunnerWiring:
    """The local backend writes the manifest (no shared template) under hive;
    the lambda backend dispatches hive runs (issue #199 phase 3), threading
    the manifest's dataset identity through the setup invoke."""

    def _catalog(self, tmp_path):
        shard = _shard_word()
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 6,
                "child_order": 12,
                "layout": "fullsphere",
            },
            "shard_keys": [shard],
            "granules": [[_rec(1)]],
        }
        p = tmp_path / "catalog.json"
        p.write_text(json.dumps(catalog))
        return str(p), shard

    def test_local_hive_writes_manifest_not_template(self, monkeypatch, cfg, tmp_path):
        from zagg import runner
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        calls = []

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            calls.append((int(shard_key), store_root))
            return {"shard_key": int(shard_key), "error": None, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        agg(cfg, catalog=catalog_path, store=root, backend="local")

        assert calls == [(shard, root)]
        # Template time wrote ONLY the manifest — no shared zarr template (D5).
        assert sorted(os.listdir(root)) == [hive.MANIFEST_NAME]
        assert hive.read_manifest(root)["shard_order"] == 6

    def test_lambda_hive_dispatches_with_manifest_dataset(self, monkeypatch, cfg, tmp_path):
        # Issue #199 phase 3: hive is wired to the lambda backend. The setup
        # invoke carries the manifest's dataset identity (from the ShardMap
        # metadata, same source as the local path) and the per-cell events need
        # NO new keys — the worker derives everything from the config dict.
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        captured: dict = {}

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )
        monkeypatch.setattr(
            runner, "_invoke_lambda_setup", lambda *a, **kw: captured.update(setup=kw)
        )
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)

        def fake_cell(client, chunk_idx, shard_key, *a, **k):
            captured["cell_shard_key"] = shard_key
            return {
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": shard_key,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_cell", fake_cell)
        agg(cfg, catalog=catalog_path, store="s3://out/product", backend="lambda")

        assert captured["setup"]["dataset"] == {"short_name": "ATL06", "version": "007"}
        # store_layout rides in the config dict already serialized into events.
        assert captured["setup"]["config_dict"]["output"]["store_layout"] == "hive"
        # The per-cell event schema is unchanged: shard_key stays the packed int.
        assert captured["cell_shard_key"] == shard

    def test_lambda_flat_setup_omits_dataset(self, monkeypatch, cfg, tmp_path):
        # Flat runs keep their setup call byte-identical: dataset stays None.
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport
        from zagg.runner import agg

        catalog_path, shard = self._catalog(tmp_path)
        captured: dict = {}

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )
        monkeypatch.setattr(
            runner, "_invoke_lambda_setup", lambda *a, **kw: captured.update(setup=kw)
        )
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_cell",
            lambda *a, **k: {
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": shard,
            },
        )
        agg(cfg, catalog=catalog_path, store="s3://out/x.zarr", backend="lambda")
        assert captured["setup"]["dataset"] is None


class TestInvokeLambdaSetupEvent:
    """Pin the ACTUAL setup event on the wire and the stale-deployment guard
    (review findings, PR #205). The dispatch tests above monkeypatch
    ``_invoke_lambda_setup`` at kwarg level, so the event-shaping conditional
    (``dataset`` added only when set) and the layout-echo assertion are
    exercised here directly, with a mocked boto3 client capturing ``Payload``."""

    @staticmethod
    def _client(body: dict):
        from unittest.mock import MagicMock

        payload = MagicMock()
        payload.read.return_value = json.dumps(
            {"statusCode": 200, "body": json.dumps(body)}
        ).encode()
        client = MagicMock()
        client.invoke.return_value = {"Payload": payload, "FunctionError": None}
        return client

    @staticmethod
    def _invoke(client, config_dict, dataset=None):
        from zagg.runner import _invoke_lambda_setup

        _invoke_lambda_setup(
            client,
            "process-shard",
            "s3://out/product",
            parent_order=6,
            child_order=12,
            n_parent_cells=None,
            overwrite=False,
            config_dict=config_dict,
            dataset=dataset,
        )
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_hive_event_carries_dataset(self, cfg):
        cfg.output["store_layout"] = "hive"
        client = self._client({"ok": True, "mode": "setup", "layout": "hive"})
        event = self._invoke(client, asdict(cfg), dataset={"short_name": "ATL06", "version": "007"})
        assert event["dataset"] == {"short_name": "ATL06", "version": "007"}

    def test_flat_event_omits_dataset_and_matches_baseline(self, cfg):
        # The byte-identity claim, pinned on the wire: no "dataset" key, and
        # the event is exactly the pre-phase-3 flat setup event.
        config_dict = asdict(cfg)
        client = self._client({"ok": True, "mode": "setup", "layout": "flat"})
        event = self._invoke(client, config_dict)
        assert "dataset" not in event
        assert event == {
            "mode": "setup",
            "store_path": "s3://out/product",
            "parent_order": 6,
            "child_order": 12,
            "n_parent_cells": None,
            "overwrite": False,
            "config": config_dict,
        }

    def test_flat_without_layout_echo_unaffected(self, cfg):
        # Old deployed functions return the echo-less body: flat dispatch must
        # keep working against them.
        self._invoke(self._client({"ok": True, "mode": "setup"}), asdict(cfg))

    @pytest.mark.parametrize(
        "body",
        [
            {"ok": True, "mode": "setup"},  # pre-phase-3 function: no echo
            {"ok": True, "mode": "setup", "layout": "flat"},  # wrong layout acted on
        ],
    )
    def test_hive_without_hive_echo_fails_fast(self, cfg, body):
        # Stale-deployment guard: an old function would emit the flat GLOBAL
        # template at the hive root and return a 200 the dispatcher couldn't
        # tell apart — the layout echo makes that fail at setup, pre-fan-out.
        cfg.output["store_layout"] = "hive"
        with pytest.raises(RuntimeError, match="redeploy"):
            self._invoke(
                self._client(body), asdict(cfg), dataset={"short_name": "A", "version": "1"}
            )
