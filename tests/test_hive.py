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
from zagg.grids.morton import morton_decimal


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

    def test_hive_accepts_sharded(self, cfg):
        # issue #236: the ShardingCodec IS vanilla zarr v3, so a sharded leaf
        # stays self-describing (D3) — hive + sharded validates and writes.
        cfg.output["store_layout"] = "hive"
        cfg.output.setdefault("grid", {})["sharded"] = True
        validate_config(cfg)

    def test_hive_rejects_shard_order(self, cfg):
        # The issue #133 object split is flat-only: a hive leaf's arrays are
        # one whole-leaf object each, so shard_order would be silently ignored.
        cfg.output["store_layout"] = "hive"
        cfg.output.setdefault("grid", {})["chunk_inner"] = 8
        cfg.output["grid"]["shard_order"] = 7
        with pytest.raises(ValueError, match="shard_order"):
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

    def test_sharded_leaf_template_shards_whole_leaf(self, cfg):
        # issue #236: a sharded grid's leaf template wraps every dense array in
        # a ShardingCodec whose outer chunk spans the WHOLE leaf (one object per
        # array, written at leaf block 0); the inner read chunk is unchanged.
        g = HealpixGrid(6, 10, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        store = MemoryStore()
        g.emit_shard_template(store, overwrite=True)
        grp = zarr.open_group(store, path=g.group_path, mode="r", zarr_format=3)
        for name in ("morton", "cell_ids", *get_data_vars(cfg)):
            assert grp[name].shape == (g.cells_per_shard,)
            assert grp[name].shards == (g.cells_per_shard,)
            assert grp[name].chunks == (g.cells_per_chunk,)

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
    streams REAL carriers, so the leaf template, dense write, ragged vlen
    layout (issue #209), and stamp ordering are all exercised against real
    zarr stores."""

    def _grid(self, cfg):
        # Declare the ragged field the streaming fakes emit, so the leaf
        # template carries its vlen-bytes array (issue #209).
        cfg.aggregation["variables"].setdefault(
            "h",
            {
                "function": "np.sort",
                "source": "h_li",
                "kind": "ragged",
                "inner_shape": [1],
                "dtype": "float32",
                "fill_value": 0,
            },
        )
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def _carrier(self, grid, shard):
        from zagg.config import get_agg_fields, get_output_signature

        coords = grid.chunk_coords(shard)
        n = len(coords["cell_ids"])
        agg = get_agg_fields(grid.config)
        df = pd.DataFrame(
            {
                var: np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
                for var in get_data_vars(grid.config)
                if get_output_signature(agg[var])["kind"] != "ragged"
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

    def _streaming_fake(self, grid, ragged=None, occupied=None):
        def fake(g, shard_key, urls, **kwargs):
            carrier = self._carrier(grid, shard_key)
            kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, ragged or {})
            if occupied is not None and kwargs.get("occupied_out") is not None:
                kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
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
        # The ragged payload sits in the leaf's vlen-bytes array at its cell
        # position (issue #209), as ONE data object.
        ragged_arr = zarr.open_group(leaf_store, path=grid.group_path, mode="r")["h"]
        np.testing.assert_array_equal(np.frombuffer(ragged_arr[0:1][0], "<f4"), [1.0, 2.0])
        chunk_dir = os.path.join(leaf, grid.group_path, "h", "c")
        assert sum(len(files) for _d, _s, files in os.walk(chunk_dir)) == 1
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
        # and a clean retry overwrites it WHOLESALE and stamps. A stray object
        # planted in the debris (one the retry does NOT rewrite) pins the
        # wholesale claim against upstream drift: if the leaf re-template
        # merely re-put metadata instead of delete_dir-ing the prefix, it
        # would survive inside a leaf whose stamp certifies it complete
        # (review finding, PR #205). The torn attempt's streamed ragged never
        # lands at all — the leaf ragged write is a single post-stream object
        # (issue #209), so a torn worker leaves no partial ragged data.
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
        # The torn attempt's ragged was accumulated, never written (issue #209).
        assert not os.path.exists(os.path.join(leaf, grid.group_path, "h", "c"))
        stale = os.path.join(leaf, grid.group_path, "stale-debris")
        with open(stale, "w") as fh:
            fh.write("torn attempt")
        # Plant a sidecar in the debris too: the one leaf object zarr does NOT
        # own must also fall to the wholesale wipe (PR #208 round 2) — this
        # goes red if the re-template ever drifts to node-by-node rewrites.
        hive.write_coverage_sidecar(leaf, b"torn-attempt sidecar")
        sidecar = os.path.join(leaf, hive.COVERAGE_SIDECAR)
        assert os.path.exists(sidecar)

        # Retry (no ragged this time): same leaf, overwritten wholesale —
        # the planted debris is GONE — and stamped at the end.
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid))
        hive.process_and_write_hive(
            shard, ["s3://bucket/g1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        assert hive.read_commit(open_store(leaf))["complete"] is True
        assert not os.path.exists(stale), "stale torn-write object survived the re-template"
        assert not os.path.exists(sidecar), "torn attempt's sidecar survived the re-template"

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
        shard = _shard_word()
        fake = self._streaming_fake(
            grid,
            ragged={"h": ([np.array([1.0])], [0])},
            occupied=grid.children(shard)[:2],
        )
        monkeypatch.setattr(processing, "process_shard", fake)
        monkeypatch.setattr(
            processing, "write_dataframe_to_zarr", rec("dense", processing.write_dataframe_to_zarr)
        )
        monkeypatch.setattr(
            processing,
            "write_ragged_leaf_to_zarr",
            rec("ragged", processing.write_ragged_leaf_to_zarr),
        )
        monkeypatch.setattr(
            hive, "write_coverage_sidecar", rec("sidecar", hive.write_coverage_sidecar)
        )
        monkeypatch.setattr(hive, "stamp_commit", rec("stamp", hive.stamp_commit))
        hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, str(tmp_path / "store"), cfg, store_kwargs={}
        )
        # The coverage sidecar (issue #200 phase 2) lands BEFORE the stamp:
        # the stamp stays the leaf's final write, so an unstamped prefix's
        # sidecar is debris like everything else in it.
        assert ops == ["dense", "ragged", "sidecar", "stamp"]


class TestProcessAndWriteHiveSharded:
    """Issue #236: with a sharded K>1 grid the shared hive worker path
    accumulates the K chunk carriers (``write_chunk=None``) and writes the
    leaf ONCE — one ShardingCodec object per dense array and per ragged field,
    byte-identical to the flat sharded path — with the D4 stamp still the
    leaf's FINAL write and the K==1 explicit-``sharded: true`` no-op matching
    the flat contract (issue #215)."""

    def _grid(self, cfg, **kw):
        cfg.aggregation["variables"].setdefault(
            "h",
            {
                "function": "np.sort",
                "source": "h_li",
                "kind": "ragged",
                "inner_shape": [1],
                "dtype": "float32",
                "fill_value": 0,
            },
        )
        # K = 16 chunks x 16 cells; sharded defaults True (issue #236).
        return HealpixGrid(
            parent_order=6, child_order=10, layout="fullsphere", config=cfg, chunk_inner=8, **kw
        )

    @staticmethod
    def _chunk_carrier(grid, children):
        from zagg.config import get_agg_fields, get_output_signature

        coords = grid.coords_of(children)
        n = len(children)
        agg = get_agg_fields(grid.config)
        # Distinct per-cell values so a chunk-placement bug cannot cancel out.
        vals = (np.asarray(children, dtype=np.float64) % 997.0).astype(np.float32)
        df = pd.DataFrame(
            {
                var: (np.arange(n, dtype=np.int32) if var == "count" else vals)
                for var in get_data_vars(grid.config)
                if get_output_signature(agg[var])["kind"] != "ragged"
            }
        )
        for name, v in coords.items():
            df[name] = v
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

    def _accumulate_fake(self, grid, ragged_by_local=None, occupied=None, error=None):
        """A ``process_shard`` fake honoring the accumulate contract: fills
        ``chunk_results`` (asserting the issue #236 switch passed no
        ``write_chunk``), every 4th inner chunk entirely empty."""

        def fake(g, shard_key, urls, **kwargs):
            sink = kwargs.get("chunk_results")
            assert sink is not None and kwargs.get("write_chunk") is None
            shard_block = grid.block_index(int(shard_key))[0]
            for block, children in grid.iter_chunks(int(shard_key)):
                local = int(block[0]) - shard_block * grid.chunks_per_shard
                if local % 4 == 3:
                    sink.append((block, pd.DataFrame(), {}))
                    continue
                ragged = (ragged_by_local or {}).get(local, {})
                sink.append((block, self._chunk_carrier(grid, children), ragged))
            if occupied is not None and kwargs.get("occupied_out") is not None:
                kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
            return pd.DataFrame(), self._meta(shard_key, error=error)

        return fake

    @staticmethod
    def _leaf_object_count(leaf, grid, name):
        chunk_dir = os.path.join(leaf, grid.group_path, name, "c")
        return sum(len(files) for _d, _s, files in os.walk(chunk_dir))

    def test_single_object_per_array_and_flat_parity(self, monkeypatch, cfg, tmp_path):
        """THE issue #236 acceptance gate: every leaf array is ONE object, and
        its contents equal the flat sharded store's shard region for the same
        chunk results — dense, ragged, coords."""
        import zagg.processing as processing
        from zagg.processing import write_shard_to_zarr
        from zagg.store import open_store

        grid = self._grid(cfg)
        assert grid.sharded is True and grid.chunks_per_shard == 16
        shard = _shard_word()
        ragged_by_local = {
            0: {"h": ([np.array([1.0, 2.0])], [0])},
            5: {"h": ([np.array([3.5])], [7])},
        }
        occupied = grid.children(shard)[:3]
        fake = self._accumulate_fake(grid, ragged_by_local, occupied=occupied)

        monkeypatch.setattr(processing, "process_shard", fake)
        root = str(tmp_path / "store")
        meta = hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        assert meta["error"] is None

        # Flat reference: the same fake's chunk_results through the flat
        # sharded writer (issue #108) on the full-sphere template.
        chunk_results: list = []
        fake(grid, shard, [], chunk_results=chunk_results, write_chunk=None)
        flat = MemoryStore()
        grid.emit_template(flat)
        write_shard_to_zarr(chunk_results, flat, grid=grid, shard_key=shard)

        leaf = hive.shard_leaf_path(root, shard)
        leaf_store = open_store(leaf)
        base = grid.block_index(shard)[0] * grid.cells_per_shard
        names = ["morton", "cell_ids", "h", *get_data_vars(cfg)]
        for name in names:
            # ONE ShardingCodec object per array (was K per-chunk objects).
            assert self._leaf_object_count(leaf, grid, name) == 1, name
            flat_arr = zarr.open_array(flat, path=f"{grid.group_path}/{name}", mode="r")
            leaf_arr = zarr.open_array(leaf_store, path=f"{grid.group_path}/{name}", mode="r")
            np.testing.assert_array_equal(
                flat_arr[base : base + grid.cells_per_shard], leaf_arr[:], err_msg=name
            )
        # Stamp + coverage sidecar unaffected: stamp present, sidecar ONE object.
        assert hive.read_commit(leaf_store)["complete"] is True
        assert os.path.isfile(os.path.join(leaf, hive.COVERAGE_SIDECAR))

    def test_stamp_is_the_final_write_sharded(self, monkeypatch, cfg, tmp_path):
        """The sharded leaf write order is pinned: ONE leaf write (dense +
        ragged) -> coverage sidecar -> stamp; the streaming writers never
        run."""
        import zagg.processing as processing

        ops: list = []

        def rec(name, fn):
            def wrapped(*a, **k):
                ops.append(name)
                return fn(*a, **k)

            return wrapped

        grid = self._grid(cfg)
        shard = _shard_word()
        fake = self._accumulate_fake(
            grid,
            {0: {"h": ([np.array([1.0])], [0])}},
            occupied=grid.children(shard)[:2],
        )
        monkeypatch.setattr(processing, "process_shard", fake)
        monkeypatch.setattr(
            processing, "write_leaf_to_zarr", rec("leaf", processing.write_leaf_to_zarr)
        )
        monkeypatch.setattr(
            processing, "write_dataframe_to_zarr", rec("dense", processing.write_dataframe_to_zarr)
        )
        monkeypatch.setattr(
            processing,
            "write_ragged_leaf_to_zarr",
            rec("ragged", processing.write_ragged_leaf_to_zarr),
        )
        monkeypatch.setattr(
            hive, "write_coverage_sidecar", rec("sidecar", hive.write_coverage_sidecar)
        )
        monkeypatch.setattr(hive, "stamp_commit", rec("stamp", hive.stamp_commit))
        hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, str(tmp_path / "store"), cfg, store_kwargs={}
        )
        assert ops == ["leaf", "sidecar", "stamp"]

    def test_error_shard_leaves_no_prefix(self, monkeypatch, cfg, tmp_path):
        # An errored shard skips the whole-leaf write; the template is lazy, so
        # no .zarr/ prefix is ever created (absence stays trustworthy — D4).
        import zagg.processing as processing

        grid = self._grid(cfg)
        shard = _shard_word()
        monkeypatch.setattr(processing, "process_shard", self._accumulate_fake(grid, error="boom"))
        root = str(tmp_path / "store")
        meta = hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        assert meta["error"] == "boom"
        assert not os.path.exists(hive.shard_leaf_path(root, shard))

    def test_k1_explicit_sharded_true_is_noop(self, monkeypatch, cfg, tmp_path):
        """K==1 no-op parity, matching flat (issue #215): explicit
        ``sharded: true`` with nothing to bundle silently disables — the leaf's
        file set and bytes are identical to an explicit ``sharded: false``
        run (stamp compared modulo its timestamp)."""
        import zagg.processing as processing

        cfg.aggregation["variables"].setdefault(
            "h",
            {
                "function": "np.sort",
                "source": "h_li",
                "kind": "ragged",
                "inner_shape": [1],
                "dtype": "float32",
                "fill_value": 0,
            },
        )
        shard = _shard_word()
        outs: dict = {}
        for tag, sharded in (("on", True), ("off", False)):
            g = HealpixGrid(6, 8, layout="fullsphere", config=cfg, sharded=sharded)
            assert g.sharded is False  # K==1: silently disabled either way

            def fake(gg, shard_key, urls, **kwargs):
                # K==1 keeps the streaming path: the switch must pass write_chunk.
                carrier = self._chunk_carrier(g, g.children(int(shard_key)))
                kwargs["write_chunk"](
                    g.block_index(int(shard_key)), carrier, {"h": ([np.array([1.0, 2.0])], [0])}
                )
                return pd.DataFrame(), self._meta(shard_key)

            monkeypatch.setattr(processing, "process_shard", fake)
            root = str(tmp_path / tag)
            hive.process_and_write_hive(shard, ["s3://b/g1.h5"], g, {}, root, cfg, store_kwargs={})
            leaf = hive.shard_leaf_path(root, shard)
            files = {}
            for dirpath, _dirs, filenames in os.walk(leaf):
                for f in filenames:
                    p = os.path.join(dirpath, f)
                    with open(p, "rb") as fh:
                        files[os.path.relpath(p, leaf)] = fh.read()
            outs[tag] = files
        assert sorted(outs["on"]) == sorted(outs["off"])
        for rel in outs["on"]:
            if rel == "zarr.json":
                on = json.loads(outs["on"][rel])
                off = json.loads(outs["off"][rel])
                on["attributes"][hive.COMMIT_ATTR].pop("written_at")
                off["attributes"][hive.COMMIT_ATTR].pop("written_at")
                assert on == off
            else:
                assert outs["on"][rel] == outs["off"][rel], rel


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
        # Template time wrote ONLY the manifest — no shared zarr template
        # (D5). The end-of-run root coverage.moc (issue #200 phase 3,
        # default-on for hive) is the only other root object.
        assert sorted(os.listdir(root)) == [hive.ROOT_COVERAGE_NAME, hive.MANIFEST_NAME]
        assert hive.read_manifest(root)["shard_order"] == 6

    def test_local_hive_sharded_leaf_single_object(self, monkeypatch, cfg, tmp_path):
        """Issue #236 through the LOCAL dispatcher: a sharded K>1 hive run
        drives the REAL ``process_and_write_hive`` (only ``process_shard`` is
        faked, honoring the accumulate contract), so each leaf array lands as
        ONE ShardingCodec object and the leaf is stamped complete."""
        import zagg.processing as processing
        from zagg.grids import from_config
        from zagg.runner import agg
        from zagg.store import open_store

        cfg.output["store_layout"] = "hive"
        cfg.output.setdefault("grid", {})["chunk_inner"] = 8
        cfg.aggregation["variables"]["h"] = {
            "function": "np.sort",
            "source": "h_li",
            "kind": "ragged",
            "inner_shape": [1],
            "dtype": "float32",
            "fill_value": 0,
        }
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        # The runner builds this same grid from the config (K = 16 inner
        # chunks; hive defaults sharded now — issue #236).
        grid = from_config(cfg, parent_order=6)
        assert grid.sharded is True and grid.chunks_per_shard == 16

        helper = TestProcessAndWriteHiveSharded()
        fake = helper._accumulate_fake(grid, {0: {"h": ([np.array([2.5])], [1])}})

        from zagg import runner

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        monkeypatch.setattr(processing, "process_shard", fake)
        agg(cfg, catalog=catalog_path, store=root, backend="local")

        leaf = hive.shard_leaf_path(root, shard)
        for name in ("morton", "cell_ids", "h"):
            chunk_dir = os.path.join(leaf, grid.group_path, name, "c")
            n_objects = sum(len(files) for _d, _s, files in os.walk(chunk_dir))
            assert n_objects == 1, name
        assert hive.read_commit(open_store(leaf))["complete"] is True

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
