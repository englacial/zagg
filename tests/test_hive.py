"""Tests for the morton-hive store layout — issue #199 phase 2.

Covers the config flag, leaf-path computation + node invariant (D2/D3/D5),
the ``morton_hive.json`` manifest (D6), the commit stamp / debris / torn-write
retry semantics (D4), and the local runner's hive write path.
"""

import json
import logging
import os
import time
from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest
import zarr
from zarr.storage import MemoryStore

from zagg import hive
from zagg.config import default_config, get_data_vars, validate_config
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_decimal, morton_words


@pytest.fixture
def cfg():
    return default_config("atl06")


def _shard_word(order=6):
    """A real southern packed shard word (decimal form ``-5112333`` at order 6)."""
    from mortie import geo2mort

    return int(geo2mort(-78.5, -132.0, order=order)[0])


# ── config flag ──────────────────────────────────────────────────────────────


class TestStoreLayoutConfig:
    def test_default_is_hive_for_healpix(self, cfg):
        # Issue #253: HEALPix point aggregation defaults to hive.
        from zagg.config import get_store_layout

        assert get_store_layout(cfg) == "hive"
        validate_config(cfg)  # defaulted hive validates unchanged

    def test_default_is_flat_for_rectilinear(self, cfg):
        from zagg.config import get_store_layout

        cfg.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 500,
            "bounds": [0, 0, 5000, 5000],
        }
        assert get_store_layout(cfg) == "flat"
        validate_config(cfg)

    def test_default_is_hive_for_raster(self):
        # The default is grid-keyed, not pipeline-keyed (issue #253 rework on
        # the landed issue #247 raster hive path): a healpix raster config
        # with no store_layout resolves hive and validates.
        from zagg.config import default_config, get_store_layout

        cfg = default_config("sentinel2_l2a")
        assert (cfg.data_source or {}).get("reader") == "raster"
        assert get_store_layout(cfg) == "hive"
        validate_config(cfg)

    def test_explicit_hive_accepted_for_raster(self):
        # Raster + hive is legal since issue #247 (the issue #239 stopgap and
        # this PR's interim carve-out are both gone).
        from zagg.config import default_config

        cfg = default_config("sentinel2_l2a")
        cfg.output["store_layout"] = "hive"
        validate_config(cfg)

    def test_explicit_flat_still_accepted(self, cfg):
        # Deprecated but valid (interop/debug) until #251 phase 3.
        from zagg.config import get_store_layout

        cfg.output["store_layout"] = "flat"
        assert get_store_layout(cfg) == "flat"
        validate_config(cfg)

    def test_hive_accepted_for_healpix(self, cfg):
        cfg.output["store_layout"] = "hive"
        validate_config(cfg)

    def test_null_key_falls_back_to_default(self, cfg):
        from zagg.config import get_store_layout

        cfg.output["store_layout"] = None
        assert get_store_layout(cfg) == "hive"
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
        # Template-time overview declaration (issue #201): default every-2
        # schedule below the shard order, per-field D24 classes; the sweep
        # adds `materialized` actuals later (D11).
        overview = m["pyramid"]["overview"]
        assert overview["orders"] == [4, 2, 0]
        assert overview["fields"]["count"]["class"] == "exact"
        assert overview["fields"]["h_mean"] == {"class": "none"}
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

    def test_validate_fresh_root_returns_none_and_writes_nothing(self, cfg, tmp_path):
        # The read-only precheck (issue #252): a fresh root has nothing to
        # match, returns None, and must NOT write the manifest — that stays
        # for the finalize ensure_manifest.
        root = str(tmp_path / "store")
        assert hive.validate_manifest(root, hive.build_manifest(self._grid(cfg))) is None
        assert hive.read_manifest(root) is None

    def test_validate_matching_returns_existing(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        written = hive.ensure_manifest(root, hive.build_manifest(grid))
        assert hive.validate_manifest(root, hive.build_manifest(grid)) == written

    def test_validate_mismatch_raises(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(self._grid(cfg)))
        other = HealpixGrid(parent_order=5, child_order=8, layout="fullsphere", config=cfg)
        with pytest.raises(ValueError, match="does not match this run"):
            hive.validate_manifest(root, hive.build_manifest(other))


# ── leaf template + commit stamp (D3/D4) ─────────────────────────────────────


class TestSemanticManifest:
    """Issue #299 phase 3: semantic_hash + path_grouping frozen keys, the
    aggregation.yaml derived core, and the append-spec seam."""

    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_manifest_carries_semantic_hash_and_grouping(self, cfg):
        from zagg.semantics import semantic_hash

        m = hive.build_manifest(self._grid(cfg))
        assert m["semantic_hash"] == semantic_hash(cfg) and len(m["semantic_hash"]) == 64
        assert m["path_grouping"] == 1

    def test_append_to_pre299_manifest_accepted(self, cfg, tmp_path):
        # A pre-#299 manifest lacks semantic_hash/path_grouping; appends must
        # not refuse on the new keys (absent-side tolerance / absent=>1).
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        old = hive.build_manifest(grid)
        del old["semantic_hash"]
        del old["path_grouping"]
        hive.ensure_manifest(root, old)
        fresh = hive.build_manifest(grid)
        assert hive.validate_manifest(root, fresh) == old
        # And the accepted manifest (no second PUT) is the existing one.
        assert hive.ensure_manifest(root, fresh) == old

    def test_semantic_mismatch_refuses(self, cfg, tmp_path):
        # Both manifests carry the hash and they differ -> frozen-key refusal,
        # exactly like an orders mismatch (D19).
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(self._grid(cfg)))
        import copy

        other_cfg = copy.deepcopy(cfg)
        other_cfg.aggregation["variables"]["count"]["dtype"] = "int64"
        other = hive.build_manifest(self._grid(other_cfg))
        with pytest.raises(ValueError, match="does not match this run"):
            hive.validate_manifest(root, other)

    def test_overwrite_semantic_mismatch_refuses_over_existing_shards(self, cfg, tmp_path):
        # Issue #341 hash-guard ruling: overwrite does NOT bypass the
        # semantic-hash refusal — the hash is a frozen key, so a changed
        # aggregation block over a data-carrying store refuses even with
        # overwrite=True (same posture as an orders change).
        import copy

        root = tmp_path / "store"
        hive.ensure_manifest(str(root), hive.build_manifest(self._grid(cfg)))
        (root / "-5" / "1").mkdir(parents=True)
        (root / "-5" / "1" / "obj").write_text("x")
        other_cfg = copy.deepcopy(cfg)
        other_cfg.aggregation["variables"]["count"]["dtype"] = "int64"
        other = hive.build_manifest(self._grid(other_cfg))
        with pytest.raises(ValueError, match="clear the store root first"):
            hive.ensure_manifest(str(root), other, overwrite=True)

    def test_overwrite_pre_hash_store_with_data_warns_and_stamps_hash(self, cfg, tmp_path, caplog):
        # Issue #341: a pre-#299 manifest carries no hash, so the frozen
        # comparison EXEMPTS it and overwrite proceeds — the one legitimate
        # bypass. It must be loud (semantic compatibility of the existing data
        # is unverifiable) and must leave a coherent manifest: the rewrite
        # stamps the run's hash.
        root = tmp_path / "store"
        grid = self._grid(cfg)
        old = hive.build_manifest(grid)
        del old["semantic_hash"]
        hive.ensure_manifest(str(root), old)
        (root / "-5" / "1").mkdir(parents=True)
        (root / "-5" / "1" / "obj").write_text("x")
        fresh = hive.build_manifest(grid)
        with caplog.at_level("WARNING"):
            hive.ensure_manifest(str(root), fresh, overwrite=True)
        assert "predates semantic hashing" in caplog.text
        assert hive.read_manifest(str(root))["semantic_hash"] == fresh["semantic_hash"]

    def test_overwrite_dropping_a_hash_from_a_hashed_store_warns(self, cfg, tmp_path, caplog):
        # Fold review: the exemption in ``_frozen_matches`` is symmetric (either
        # side missing the hash drops it from the comparison), so the guard must
        # be too. This is the REVERSE direction — the existing store carries a
        # hash and this run's manifest does not — which un-provenances a #299
        # store rather than merely failing to verify one. Latent today
        # (build_manifest always stamps) but the strictly worse direction.
        root = tmp_path / "store"
        grid = self._grid(cfg)
        hive.ensure_manifest(str(root), hive.build_manifest(grid))
        (root / "-5" / "1").mkdir(parents=True)
        (root / "-5" / "1" / "obj").write_text("x")
        unhashed = hive.build_manifest(grid)
        del unhashed["semantic_hash"]
        with caplog.at_level("WARNING"):
            hive.ensure_manifest(str(root), unhashed, overwrite=True)
        assert "DROPS the recorded hash" in caplog.text
        assert "un-provenanced" in caplog.text

    def test_overwrite_both_hashes_present_is_silent(self, cfg, tmp_path, caplog):
        # Both sides hashed and equal -> the comparison actually happened, so
        # neither exemption warning fires (the normal resume/redo path).
        root = tmp_path / "store"
        grid = self._grid(cfg)
        hive.ensure_manifest(str(root), hive.build_manifest(grid))
        (root / "-5" / "1").mkdir(parents=True)
        (root / "-5" / "1" / "obj").write_text("x")
        with caplog.at_level("WARNING"):
            hive.ensure_manifest(str(root), hive.build_manifest(grid), overwrite=True)
        assert "semantic hash" not in caplog.text
        assert "predates semantic hashing" not in caplog.text

    def test_overwrite_pre_hash_store_empty_tree_is_silent(self, cfg, tmp_path, caplog):
        # No shard data -> nothing whose compatibility could be in question;
        # the manifest is simply replaced (hash stamped), no warning.
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        old = hive.build_manifest(grid)
        del old["semantic_hash"]
        hive.ensure_manifest(root, old)
        fresh = hive.build_manifest(grid)
        with caplog.at_level("WARNING"):
            hive.ensure_manifest(root, fresh, overwrite=True)
        assert "predates semantic hashing" not in caplog.text
        assert hive.read_manifest(root)["semantic_hash"] == fresh["semantic_hash"]

    def test_grouping_mismatch_refuses(self, cfg, tmp_path):
        # path_grouping IS frozen (path shape): an explicit different value
        # refuses; only ABSENT normalizes to 1.
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        grouped = hive.build_manifest(grid)
        grouped["path_grouping"] = 3
        hive.ensure_manifest(root, grouped)
        with pytest.raises(ValueError, match="does not match this run"):
            hive.validate_manifest(root, hive.build_manifest(grid))

    def test_aggregation_core_written_with_manifest(self, cfg, tmp_path):
        import yaml as _yaml

        from zagg.semantics import semantic_core

        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(self._grid(cfg)), config=cfg)
        payload = (tmp_path / "store" / hive.AGGREGATION_CORE_NAME).read_text()
        assert _yaml.safe_load(payload) == semantic_core(cfg)
        # Deterministic: a rewrite is byte-identical (sorted-key dump).
        hive.write_semantic_core(root, cfg)
        assert (tmp_path / "store" / hive.AGGREGATION_CORE_NAME).read_text() == payload

    def test_aggregation_core_write_is_fail_open(self, cfg, monkeypatch, caplog):
        import obstore

        def boom(*a, **k):
            raise OSError("no")

        monkeypatch.setattr(obstore, "put", boom)
        with caplog.at_level("WARNING"):
            hive.write_semantic_core("/nonexistent-root-zzz", cfg)
        assert any("fail-open" in r.message for r in caplog.records)


class TestProductRoots:
    """D19 named product roots (issue #299 phase 2): additive — a product
    subtree is a complete, unmodified morton-hive store."""

    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_name_grammar(self):
        for good in ("atl06", "atl06_h_li", "s2-serc-2025", "x", "88s_o9"):
            assert hive.validate_product_name(good) == good
        for bad in ("ATL06", "a b", "", "a/b", "a.b", "café", None, 7):
            with pytest.raises((ValueError, TypeError)):
                hive.validate_product_name(bad)

    def test_name_length_cap(self):
        # D19 length cap (espg ruling, mortie spec §6.5): 192 accepts, 193
        # rejects — the boundary of the POSIX-255-less-13-decoration budget.
        assert hive.validate_product_name("a" * hive.PRODUCT_NAME_MAX) == "a" * 192
        assert hive.PRODUCT_NAME_MAX == 192
        with pytest.raises(ValueError, match="caps names"):
            hive.validate_product_name("a" * (hive.PRODUCT_NAME_MAX + 1))

    def test_base_component_exclusion(self):
        # Names shaped like hive base components would make the walker's
        # child classification ambiguous (D19).
        for bad in ("1", "6", "-1", "-6", "3"):
            with pytest.raises(ValueError, match="base-component"):
                hive.validate_product_name(bad)
        # Digit-LEADING names longer than a base component are fine.
        assert hive.validate_product_name("2019_run") == "2019_run"

    def test_product_root_join(self):
        assert hive.product_root("s3://b/root/", "atl06") == "s3://b/root/atl06"
        with pytest.raises(ValueError, match="grammar"):
            hive.product_root("s3://b/root", "BAD")

    def test_leaf_path_under_product_root_is_unchanged(self, cfg):
        # A product subtree is byte-identical to a bare store: the same leaf
        # path arithmetic applies below the product root.
        word = _shard_word()
        bare = hive.shard_leaf_path("s3://b/root", word)
        under = hive.shard_leaf_path(hive.product_root("s3://b/root", "atl06"), word)
        assert under == bare.replace("s3://b/root/", "s3://b/root/atl06/")

    def test_effective_store_root(self, cfg):
        assert hive.effective_store_root("s3://b/root", cfg) == "s3://b/root"
        cfg.output["product_name"] = "atl06_h_li"
        assert hive.effective_store_root("s3://b/root", cfg) == "s3://b/root/atl06_h_li"

    def test_product_name_validated_at_config_load(self, cfg):
        from zagg.config import validate_config

        cfg.output["product_name"] = "UPPER"
        with pytest.raises(ValueError, match="grammar"):
            validate_config(cfg)

    def test_classify_store_root(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        (tmp_path / "store").mkdir()
        assert hive.classify_store_root(root) == "empty"
        # A product directory: manifests only under {name}/.
        m = hive.build_manifest(self._grid(cfg))
        hive.ensure_manifest(hive.product_root(root, "atl06"), m)
        assert hive.classify_store_root(root) == "products"
        # A manifest at the root wins: bare single-product store.
        bare = str(tmp_path / "bare")
        hive.ensure_manifest(bare, m)
        assert hive.classify_store_root(bare) == "bare"

    def test_mid_write_bare_store_not_misread(self, cfg, tmp_path):
        # The manifest write is async (issue #252): digit-shaped children
        # without a manifest are a bare store mid-first-run, never "products".
        root = tmp_path / "store"
        (root / "3" / "1").mkdir(parents=True)
        (root / "3" / "1" / "obj").write_text("x")
        assert hive.classify_store_root(str(root)) == "bare"

    def test_list_products(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        m = hive.build_manifest(self._grid(cfg))
        hive.ensure_manifest(hive.product_root(root, "atl06"), m)
        hive.ensure_manifest(hive.product_root(root, "atl03_tdigest"), m)
        # An undiscoverable (manifest-less) child prefix is skipped, not an error.
        (tmp_path / "store" / "debris_product").mkdir()
        (tmp_path / "store" / "debris_product" / "junk").write_text("x")
        products = hive.list_products(root)
        assert sorted(products) == ["atl03_tdigest", "atl06"]
        assert products["atl06"]["spec"] == m["spec"]

    def test_legacy_multiscales_product_is_excluded_loudly(self, cfg, tmp_path, caplog):
        # The §4.10 reservation is retroactive on discovery (issue #394):
        # a product predating it is filtered out — it cannot be addressed
        # either — but the filtering is REPORTED, never silent.
        root = str(tmp_path / "store")
        m = hive.build_manifest(self._grid(cfg))
        hive.ensure_manifest(f"{root}/multiscales", m)  # product_root refuses the name
        with caplog.at_level("WARNING"):
            assert hive.list_products(root) == {}
            assert hive.classify_store_root(root) == "empty"
        assert len([r for r in caplog.records if "is a PRODUCT" in r.message]) == 2

    def test_companion_group_is_not_warned_about(self, cfg, tmp_path, caplog):
        # The ordinary /2 store: the companion carries no manifest, so
        # discovery passes over it in silence (the finding-1 lesson — a
        # warning on the normal shape trains operators to ignore the log).
        root = tmp_path / "store"
        (root / "multiscales").mkdir(parents=True)
        (root / "multiscales" / "zarr.json").write_text("{}")
        hive.ensure_manifest(
            hive.product_root(str(root), "atl06"), hive.build_manifest(self._grid(cfg))
        )
        with caplog.at_level("WARNING"):
            assert sorted(hive.list_products(str(root))) == ["atl06"]
            assert hive.classify_store_root(str(root)) == "products"
        assert not [r for r in caplog.records if "multiscales" in r.message]


class TestLeafTemplateAndStamp:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_leaf_template_is_shard_sized(self, cfg):
        g = self._grid(cfg)
        store = MemoryStore()
        g.emit_shard_template(store, overwrite=True)
        grp = zarr.open_group(store, path=g.group_path, mode="r", zarr_format=3)
        for name in ("morton", *get_data_vars(cfg)):
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

    @staticmethod
    def _put(store, key, payload=b"\x00" * 16):
        from zarr.core.buffer import default_buffer_prototype
        from zarr.core.sync import sync

        sync(store.set(key, default_buffer_prototype().buffer.from_bytes(payload)))

    @staticmethod
    def _exists(store, key):
        from zarr.core.sync import sync

        return sync(store.exists(key))

    def test_overwrite_survives_orphan_member_dir(self, cfg):
        # Issue #341 (Bug A regression): a member dir holding chunk objects but
        # no zarr.json — the shape a retired post-#337 ``cell_ids`` member leaves.
        # The leaf prefix is cleared wholesale up front, so the re-template never
        # parses the orphan and the objects do not survive as pseudo-data.
        #
        # NOTE (fold review): on the pinned zarr 3.2.1 the enumeration would
        # warn-skip this orphan rather than raise, so this pins the WHOLESALE
        # replacement (the orphan's chunks are gone), not a crash fix — the
        # observed "No array found in store ... at path 19/cell_ids" came from the
        # stale deploy on fresh stores, not from orphans. See
        # HealpixGrid.emit_shard_template's docstring.
        store = MemoryStore()
        g = self._grid(cfg)
        g.emit_shard_template(store, overwrite=True)
        self._put(store, f"{g.group_path}/cell_ids/c/0")  # orphan: chunks, no zarr.json
        g.emit_shard_template(store, overwrite=True)  # must not raise
        assert not self._exists(store, f"{g.group_path}/cell_ids/c/0")  # and it is gone

    def test_overwrite_clears_retired_members(self, cfg):
        # Issue #341 (Bug A): schema narrowing over an existing leaf round-trips
        # clean — the retired member's metadata AND chunk objects are deleted,
        # not left masquerading as data.
        import copy

        store = MemoryStore()
        g = self._grid(cfg)
        g.emit_shard_template(store, overwrite=True)
        self._put(store, f"{g.group_path}/h_max/c/0")  # data in the member being dropped

        cfg2 = copy.deepcopy(cfg)
        del cfg2.aggregation["variables"]["h_max"]
        g2 = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg2)
        g2.emit_shard_template(store, overwrite=True)

        assert not self._exists(store, f"{g.group_path}/h_max/zarr.json")
        assert not self._exists(store, f"{g.group_path}/h_max/c/0")
        grp = zarr.open_group(store, path=g2.group_path, mode="r", zarr_format=3)
        members = dict(grp.members())
        assert "h_max" not in members
        for name in ("morton", *get_data_vars(cfg2)):
            assert grp[name].shape == (g2.cells_per_shard,)

    def test_overwrite_clears_coverage_sidecar_without_warning(self, cfg):
        # The pre-#341 overwrite walked the existing prefix and warned on the
        # coverage sidecar (suppressed at the call site); the clear-first
        # template deletes it silently — no enumeration warning at all.
        import warnings as _warnings

        store = MemoryStore()
        g = self._grid(cfg)
        g.emit_shard_template(store, overwrite=True)
        self._put(store, hive.COVERAGE_SIDECAR)
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            g.emit_shard_template(store, overwrite=True)
        assert not self._exists(store, hive.COVERAGE_SIDECAR)
        assert not [w for w in caught if hive.COVERAGE_SIDECAR in str(w.message)]

    def test_overwrite_clear_is_scoped_to_the_leaf(self, cfg, tmp_path):
        # The store handed to emit_shard_template is rooted AT the leaf, so the
        # up-front clear can only touch the leaf prefix — sibling objects (the
        # run's ``.zarr.status/`` results live outside the leaf) survive.
        #
        # NOTE (fold review): ``open_store`` on a non-s3 path returns a zarr
        # ``LocalStore``, whose ``delete_dir("")`` is ``shutil.rmtree(root)`` —
        # so on THIS backend the sibling is safe by filesystem semantics and this
        # assertion cannot fail whatever the code does. It pins the local/debug
        # path only; the prefix-string hazard lives on the fleet's obstore-backed
        # store and is pinned by the test below.
        from zagg.store import open_store

        leaf = tmp_path / "leaf.zarr"
        status = tmp_path / "leaf.zarr.status"
        status.mkdir()
        (status / "r.json").write_text("{}")
        g = self._grid(cfg)
        g.emit_shard_template(open_store(str(leaf)), overwrite=True)
        g.emit_shard_template(open_store(str(leaf)), overwrite=True)
        assert (status / "r.json").read_text() == "{}"

    def test_overwrite_clear_is_scoped_on_an_obstore_prefix_store(self, cfg, tmp_path):
        # Fold review: the real hazard is STRING-prefix, and it only exists on the
        # fleet backend — ``open_store("s3://…")`` builds
        # ``zarr.storage.ObjectStore(store=S3Store(bucket, prefix=…))``, and
        # zarr's ``ObjectStore.delete_dir`` deliberately leaves the EMPTY prefix
        # un-slashed, so scoping rests entirely on obstore's prefix store
        # re-adding the delimiter. It does (obstore's ``PrefixStore`` resolves
        # ``list(None)`` to the store's own prefix path and object_store's
        # ``format_prefix`` appends DELIMITER), but that is a property of two
        # third-party layers and no test here could regress it: every other
        # delete_dir test uses MemoryStore or zarr LocalStore, neither of which
        # can. This pins it against an obstore/zarr bump, using obstore's own
        # LocalStore in prefix mode — the same ``ObjectStore`` + prefix-store
        # composition the S3 path uses.
        import obstore
        from zarr.storage import ObjectStore

        root = tmp_path / "root"
        leaf = root / "12" / "34.zarr"
        leaf.mkdir(parents=True)
        # (a) a name-prefix sibling of the leaf: the adversarial shape a missing
        # delimiter would sweep up.
        twin = root / "12" / "34.zarr.status"
        twin.mkdir()
        (twin / "r.json").write_text("{}")
        # (b) the REAL geometry: status objects live beside the store ROOT,
        # several digit-tree levels above any leaf.
        real_status = tmp_path / "root.zarr.status" / "run-1"
        real_status.mkdir(parents=True)
        (real_status / "34.json").write_text("{}")

        store = ObjectStore(store=obstore.store.LocalStore(prefix=str(leaf)))
        g = self._grid(cfg)
        g.emit_shard_template(store, overwrite=True)
        self._put(store, f"{g.group_path}/h_max/c/0")  # in-leaf debris
        g.emit_shard_template(store, overwrite=True)

        # The clear really ran (otherwise the survival assertions are vacuous)...
        assert not self._exists(store, f"{g.group_path}/h_max/c/0")
        # ...and touched nothing outside the leaf prefix.
        assert (twin / "r.json").read_text() == "{}"
        assert (real_status / "34.json").read_text() == "{}"

    def test_sharded_leaf_template_shards_whole_leaf(self, cfg):
        # issue #236: a sharded grid's leaf template wraps every dense array in
        # a ShardingCodec whose outer chunk spans the WHOLE leaf (one object per
        # array, written at leaf block 0); the inner read chunk is unchanged.
        g = HealpixGrid(6, 10, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        store = MemoryStore()
        g.emit_shard_template(store, overwrite=True)
        grp = zarr.open_group(store, path=g.group_path, mode="r", zarr_format=3)
        for name in ("morton", *get_data_vars(cfg)):
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

    def test_leaf_clear_under_a_live_writer_leaves_debris_not_corruption(self, cfg):
        # Fold review on the clear-first template: the redundant-duplicate-writer
        # window WIDENED, and this pins what it widened to.
        #
        # Two live workers on one shard is reachable — dispatch classifies
        # retryability off the Invoke call's exception string, and an Event invoke
        # Lambda accepted whose HTTP response timed out is indistinguishable from
        # one it never got. Writer A commits and stamps; writer B (the redundant
        # retry) re-templates and dies mid-write.
        #
        # Pre-#341 the leaf would still hold A's objects under A's stamp
        # (stale-but-complete). Now B's clear removes them first, so the leaf is
        # EMPTY and UNSTAMPED. That is a real loss of an already-successful leaf —
        # but not silent corruption: the stamp is written last, so read_commit
        # reports debris and the shard stays re-dispatchable, which is the property
        # the walker and the retry model actually depend on.
        store = MemoryStore()
        g = self._grid(cfg)

        # Writer A: template, write, stamp -> complete.
        g.emit_shard_template(store, overwrite=True)
        self._put(store, f"{g.group_path}/h_max/c/0")
        hive.stamp_commit(store, cells_with_data=5, granule_count=2)
        assert hive.read_commit(store)["complete"] is True

        # Writer B: re-templates the same leaf (the clear fires) and then dies
        # before writing anything or stamping.
        g.emit_shard_template(store, overwrite=True)

        assert not self._exists(store, f"{g.group_path}/h_max/c/0")  # A's data is gone
        assert hive.read_commit(store) is None  # ...and it reads as debris, not complete


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
        n = len(coords["morton"])
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
            np.asarray(grp["morton"][:]),
            morton_words(grid.chunk_coords(shard)["morton"]),
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
            # An intermediate digit node: no ZARR metadata, only digit
            # children, leaf dirs, and the leaf's own JSON siblings (the D20
            # stats sidecar, its issue #388 granule-id list, the D22 sub-map).
            assert all(f.endswith(".json") for f in filenames), (
                f"non-JSON object above the leaf at {dirpath}: {filenames}"
            )
            assert "zarr.json" not in filenames, f"zarr metadata above the leaf at {dirpath}"
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


# ── leaf skip-if-current + contraction guard (issue #388 phase 2) ────────────


class TestLeafSkipIfCurrent:
    """The worker-side identity gate on ``process_and_write_hive``: a unit
    whose planned ``(semantic_hash, granule-id set)`` pair matches the leaf's
    recorded D20 sidecar no-ops the fold and writes NOTHING; an unflagged
    contraction refuses; everything else rewrites exactly as today."""

    URLS = ["s3://bucket/granule1.h5", "s3://bucket/granule2.h5"]

    # Shared with the seam tests above (same fake-worker contract).
    _grid = TestProcessAndWriteHive._grid
    _carrier = TestProcessAndWriteHive._carrier
    _meta = TestProcessAndWriteHive._meta
    _streaming_fake = TestProcessAndWriteHive._streaming_fake

    def _write_leaf(self, monkeypatch, cfg, tmp_path):
        """First run: the REAL seam writes + stamps the leaf; then the sidecar
        the runner would write (issue #297), carrying the #388 identity."""
        import zagg.processing as processing
        from zagg.telemetry import build_record, write_sidecar

        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid))
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}
        )
        record = build_record(
            shard_key=int(shard),
            metadata=meta,
            granule_ids=list(self.URLS),
            run_id="r1",
            semantic_hash=meta["semantic_hash"],
        )
        write_sidecar(hive.shard_leaf_path(root, shard), record)
        return grid, shard, root, record

    def _counting_fake(self, monkeypatch, grid):
        import zagg.processing as processing

        calls: list = []
        fake = self._streaming_fake(grid)

        def counting(*a, **k):
            calls.append(int(a[1]))
            return fake(*a, **k)

        monkeypatch.setattr(processing, "process_shard", counting)
        return calls

    def _arm_boom(self, monkeypatch):
        import zagg.processing as processing

        def boom(*_a, **_k):
            raise AssertionError("fold ran on a gated unit")

        monkeypatch.setattr(processing, "process_shard", boom)

    @staticmethod
    def _tree(root):
        """Every file under ``root`` with its mtime — the wrote-nothing pin."""
        out = {}
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                p = os.path.join(dirpath, name)
                out[os.path.relpath(p, root)] = os.stat(p).st_mtime_ns
        return out

    @staticmethod
    def _contents(root):
        """Every file under ``root`` with its bytes — the no-content-change pin
        (the phase-3 lifecycle touch legitimately moves mtimes on a skip)."""
        out = {}
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                p = os.path.join(dirpath, name)
                with open(p, "rb") as fh:
                    out[os.path.relpath(p, root)] = fh.read()
        return out

    @staticmethod
    def _age(root, epoch=10_000):
        """Backdate every file under ``root`` so a touch is unambiguous even
        on coarse-mtime filesystems."""
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                os.utime(os.path.join(dirpath, name), (epoch, epoch))
        return epoch * 10**9  # ns threshold

    def test_identical_rerun_skips_and_writes_nothing(self, monkeypatch, cfg, tmp_path):
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        before = self._contents(root)
        aged_ns = self._age(root)
        self._arm_boom(monkeypatch)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["current"] is True and meta["identity"] == "equal"
        assert meta["shard_key"] == int(shard)
        assert meta["total_obs"] == 0 and meta.get("error") is None
        # The unit wrote NOTHING: same object set, every byte identical.
        assert self._contents(root) == before
        # ...but the lifecycle touch refreshed every object's timestamp
        # (issue #388 phase 3): the purge clock resets on a skip.
        after = self._tree(root)
        assert set(after) == set(before)
        assert all(mtime > aged_ns for mtime in after.values())
        assert meta["touched_objects"] == len(after) and meta["touch_failed"] == 0
        # A touch that RAN carries no not-applicable key: the issue #495
        # phase-4 field is conditional, so an unpublished record is
        # byte-identical to what it was before that phase.
        assert "touch_skipped_paths" not in meta

    def test_a_published_skip_is_recorded_not_silently_zero(self, monkeypatch, cfg, tmp_path):
        # Issue #495 phase 4 (review finding on PR #496). The touch is NOT
        # APPLICABLE on a published bucket, but touched_objects: 0 /
        # touch_failed: 0 is byte-identical to a touch that never ran -- so
        # the not-applicable path count has to reach the durable record, or
        # an operator auditing a published campaign cannot tell the two
        # apart. The touch itself is pinned in tests/test_lifecycle.py; this
        # pins the seam that WRITES the record.
        import zagg.lifecycle as lifecycle_mod

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        self._arm_boom(monkeypatch)
        monkeypatch.setattr(
            lifecycle_mod,
            "touch_current_unit",
            lambda *_a, **_k: {"touched": 0, "failed": 0, "skipped_paths": 4},
        )
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["current"] is True
        assert meta["touched_objects"] == 0 and meta["touch_failed"] == 0
        assert meta["touch_skipped_paths"] == 4

    def test_gate_is_off_by_default(self, monkeypatch, cfg, tmp_path):
        # Byte-identical default: without skip_if_current the seam rewrites
        # unconditionally, exactly as today (the deployed handler's posture).
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}
        )
        assert calls == [int(shard)]
        assert "current" not in meta and "identity" not in meta

    def test_contraction_refuses_and_names_missing(self, monkeypatch, cfg, tmp_path):
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        before = self._tree(root)
        self._arm_boom(monkeypatch)
        meta = hive.process_and_write_hive(
            shard, [self.URLS[0]], grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["refused"] is True and meta["identity"] == "contraction"
        # Named in the CANONICAL id space (espg-ruled 2026-08-17): the
        # driver-stripped bare granule id, so the refusal reads the same
        # whichever driver the run used.
        assert meta["missing_granules"] == ["granule2.h5"]
        # A refusal writes nothing either: the committed leaf is protected —
        # and it is NOT touched (only a certified-current unit resets the
        # purge clock, issue #388 phase 3).
        assert self._tree(root) == before
        assert "touched_objects" not in meta

    def test_mixed_add_and_drop_refuses(self, monkeypatch, cfg, tmp_path):
        # The ruled predicate is ``recorded ∖ planned ≠ ∅``, NOT strict-subset:
        # a shardmap that GREW while silently dropping an old granule (an
        # upstream purge behind a fresh catalog query) still trips the guard.
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        self._arm_boom(monkeypatch)
        planned = [self.URLS[0], "s3://bucket/granule3.h5", "s3://bucket/granule4.h5"]
        meta = hive.process_and_write_hive(
            shard, planned, grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["refused"] is True and meta["identity"] == "mixed"
        assert meta["missing_granules"] == ["granule2.h5"]

    def test_allow_contraction_rewrites_wholesale(self, monkeypatch, cfg, tmp_path):
        from zagg.store import open_store

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard,
            [self.URLS[0]],
            grid,
            {},
            root,
            cfg,
            store_kwargs={},
            skip_if_current=True,
            allow_contraction=True,
        )
        # A flagged contraction is a NORMAL update: fold ran, leaf re-stamped,
        # no refusal — the classification still rides for the run stats.
        assert calls == [int(shard)]
        assert "refused" not in meta and meta["identity"] == "contraction"
        leaf = hive.shard_leaf_path(root, shard)
        assert hive.read_commit(open_store(leaf))["complete"] is True

    def test_expansion_rewrites(self, monkeypatch, cfg, tmp_path):
        # A new cycle's granules: planned ⊇ recorded never trips the guard.
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard,
            list(self.URLS) + ["s3://bucket/granule3.h5"],
            grid,
            {},
            root,
            cfg,
            store_kwargs={},
            skip_if_current=True,
        )
        assert calls == [int(shard)] and meta["identity"] == "expansion"
        assert "refused" not in meta and "current" not in meta

    def test_semantic_mismatch_rewrites(self, monkeypatch, cfg, tmp_path):
        # Same id set under a different semantic hash: never a skip, never a
        # refusal — a semantic change over covered inputs is a normal rewrite.
        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard,
            list(self.URLS),
            grid,
            {},
            root,
            cfg,
            store_kwargs={},
            skip_if_current=True,
            semantic_hash="f" * 64,
        )
        assert calls == [int(shard)]
        assert meta["identity"] == "semantic-mismatch"
        # A caller-passed hash is recorded as given (the run-config hash).
        assert meta["semantic_hash"] == "f" * 64

    def test_unrecorded_ids_rewrites_with_its_own_classification(self, monkeypatch, cfg, tmp_path):
        # A leaf written before issue #388 has no granule-id sibling beside
        # its sidecar, so any mismatch classifies ``unrecorded-ids`` and
        # rewrites — the guard is INERT there, and the classification is what
        # the run stats count apart (cells_unrecorded).
        from zagg.telemetry import granule_ids_path

        grid, shard, root, record = self._write_leaf(monkeypatch, cfg, tmp_path)
        os.remove(granule_ids_path(hive.shard_leaf_path(root, shard)))
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, [self.URLS[0]], grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert calls == [int(shard)]
        assert meta["identity"] == "unrecorded-ids" and "refused" not in meta

    def test_a_clamped_per_cell_config_still_reads_current(self, monkeypatch, cfg, tmp_path):
        # The issue #415 epoch's (7)(b) half, pinned at the seam where the
        # drift actually bit rather than in ``semantics`` alone: the
        # dispatcher hands a 2-granule cell a data_source clamped to
        # ``granule_workers: 2`` (``runner._clamped_data_source``, issue #184)
        # and the seam hashes THAT config through its ``semantic_hash=None``
        # fallback. Pre-epoch the clamp moved the hash, so a small shard read
        # its own leaf as ``semantic-mismatch`` and rewrote on every rerun.
        from dataclasses import replace

        from zagg.runner import _clamped_data_source

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        clamped = _clamped_data_source(cfg.data_source, len(self.URLS))
        assert clamped is not None and clamped["granule_workers"] == len(self.URLS)
        self._arm_boom(monkeypatch)
        meta = hive.process_and_write_hive(
            shard,
            list(self.URLS),
            grid,
            {},
            root,
            replace(cfg, data_source=clamped),
            store_kwargs={},
            skip_if_current=True,
        )
        assert meta["current"] is True and meta["identity"] == "equal"

    def test_a_credential_migration_still_reads_current(self, monkeypatch, cfg, tmp_path):
        # The credentials_provider half of the epoch (espg-ruled 2026-08-17,
        # issue #449), pinned where the cost would land: a store written
        # WITHOUT a provider, then rerun under a config that names one — the
        # MERRA-2/gesdisc migration over unchanged data. The bytes a leaf holds
        # do not depend on which registry minted the credentials that fetched
        # them, so the gate must still read `equal` and the fold must not run.
        from dataclasses import replace

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        migrated = {**cfg.data_source, "credentials_provider": "gesdisc"}
        self._arm_boom(monkeypatch)
        meta = hive.process_and_write_hive(
            shard,
            list(self.URLS),
            grid,
            {},
            root,
            replace(cfg, data_source=migrated),
            store_kwargs={},
            skip_if_current=True,
        )
        assert meta["current"] is True and meta["identity"] == "equal"

    def test_a_driver_switch_still_reads_current(self, monkeypatch, cfg, tmp_path):
        # The canonical granule-identity half of the epoch (espg-ruled
        # 2026-08-17, PR #420 question (1)(b)), pinned where the cost would
        # land: a store written from s3 hrefs, then rerun with the https hrefs
        # of the SAME granules (``runner._resolve_urls`` picks by
        # data_source.driver, and the driver is packaging in the D19 core). The
        # gate must read `equal` and the fold must not run — pre-epoch every id
        # was renamed, the recorded catalog hash could not be reproduced, and
        # the whole store rewrote itself for a fetch-mechanism change.
        from dataclasses import replace

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        https = [u.replace("s3://bucket/", "https://host/some/prefix/") for u in self.URLS]
        self._arm_boom(monkeypatch)
        meta = hive.process_and_write_hive(
            shard,
            https,
            grid,
            {},
            root,
            replace(cfg, data_source={**cfg.data_source, "driver": "https"}),
            store_kwargs={},
            skip_if_current=True,
        )
        assert meta["current"] is True and meta["identity"] == "equal"

    def test_a_sharded_flip_defeats_the_skip(self, monkeypatch, cfg, tmp_path):
        # The issue #415 epoch's (8)(c) half, at the gate: flipping
        # output.grid.sharded over an existing store changes the leaf's
        # object set while the granule set holds, so PRE-epoch both identity
        # halves agreed and the unit read `equal` — the store kept its old
        # layout forever. The widened core makes it a semantic change.
        import copy

        _grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        flipped = copy.deepcopy(cfg)
        # HEALPix output defaults to sharded (issue #215), so this is a real
        # flip; the grid is rebuilt from the flipped config, since reusing the
        # leaf's grid would carry the OLD sharding into the rewrite.
        flipped.output["grid"]["sharded"] = False
        flipped_grid = self._grid(flipped)
        calls = self._counting_fake(monkeypatch, flipped_grid)
        meta = hive.process_and_write_hive(
            shard,
            list(self.URLS),
            flipped_grid,
            {},
            root,
            flipped,
            store_kwargs={},
            skip_if_current=True,
        )
        assert calls == [int(shard)] and meta["identity"] == "semantic-mismatch"

    def test_no_sidecar_rewrites(self, monkeypatch, cfg, tmp_path):
        # No sidecar (fresh store): unverifiable, so today's rewrite.
        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert calls == [int(shard)] and meta["identity"] == "no-sidecar"

    @pytest.mark.parametrize("body", [b"[]", b'"x"', b"5", b"not json"])
    def test_a_non_object_sidecar_degrades_to_rewrite(self, monkeypatch, cfg, tmp_path, body):
        # Fail-open means fail-open: ``read_sidecar`` returns whatever the JSON
        # decoded to, so a valid-but-not-an-object body must not raise out of
        # the seam and fail the unit.
        import obstore

        from zagg.store import open_object_store
        from zagg.telemetry import sidecar_key

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        prefix, _, name = hive.shard_leaf_path(root, shard).rstrip("/").rpartition("/")
        obstore.put(open_object_store(prefix), sidecar_key(name), body)
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert calls == [int(shard)] and meta["identity"] == "no-sidecar"
        assert meta.get("error") is None

    def test_destroyed_leaf_with_surviving_sidecar_rebuilds(self, monkeypatch, cfg, tmp_path):
        # The sidecar is a SIBLING of the leaf .zarr, so a prefix-scoped
        # lifecycle purge (the very reaping issue #388 exists to outrun) takes
        # the leaf and leaves the record. Identity alone would certify the
        # absent leaf ``current`` FOREVER — nothing rewrites the sidecar, so
        # every later rerun would skip too. The D4 stamp is the precondition.
        import shutil

        from zagg.store import open_store

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        leaf = hive.shard_leaf_path(root, shard)
        shutil.rmtree(leaf)
        assert hive.read_commit(open_store(leaf)) is None
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert calls == [int(shard)] and meta["identity"] == "unstamped-leaf"
        assert "current" not in meta
        assert hive.read_commit(open_store(leaf))["complete"] is True

    def test_unstamped_debris_with_surviving_sidecar_rebuilds(self, monkeypatch, cfg, tmp_path):
        # The issue #341 clear-then-die state: the prefix is there, the stamp
        # is not. ``dedup.shard_status`` calls that a miss; so does the gate.
        from zagg.store import open_store

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        leaf = hive.shard_leaf_path(root, shard)
        group = zarr.open_group(open_store(leaf), path="", mode="r+", zarr_format=3)
        del group.attrs[hive.COMMIT_ATTR]
        assert hive.read_commit(open_store(leaf)) is None
        calls = self._counting_fake(monkeypatch, grid)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert calls == [int(shard)] and meta["identity"] == "unstamped-leaf"

    def test_touch_failure_never_unskips_the_unit(self, monkeypatch, cfg, tmp_path):
        # Fail-open (issue #388 phase 3): a touch failure logs and counts —
        # it NEVER fails the unit and never degrades the skip to a rewrite.
        import zagg.lifecycle as lifecycle

        grid, shard, root, _record = self._write_leaf(monkeypatch, cfg, tmp_path)
        self._arm_boom(monkeypatch)

        def explode(*_a, **_k):
            raise RuntimeError("S3 down")

        monkeypatch.setattr(lifecycle, "touch_current_unit", explode)
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["current"] is True and meta.get("error") is None
        assert meta["touched_objects"] == 0 and meta["touch_failed"] == 1

    def test_seam_stamps_semantic_hash(self, monkeypatch, cfg, tmp_path):
        # The seam stamps the D19 hash into its returned metadata so a caller
        # that never resolved it (the Lambda handler) still records the
        # identity half via build_record's validated metadata fallback.
        import zagg.processing as processing
        from zagg.semantics import semantic_hash as semhash

        grid = self._grid(cfg)
        shard = _shard_word()
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid))
        meta = hive.process_and_write_hive(
            shard, list(self.URLS), grid, {}, str(tmp_path / "store"), cfg, store_kwargs={}
        )
        assert meta["semantic_hash"] == semhash(cfg)


def _sharded_accumulate_fake(
    grid, chunk_carrier, meta, ragged_by_local=None, occupied=None, error=None
):
    """A ``process_shard`` fake honoring the sharded accumulate contract (issue
    #236): fills ``chunk_results`` (asserting the switch passed no
    ``write_chunk``), every 4th inner chunk entirely empty. ``chunk_carrier``
    builds one chunk's carrier and ``meta`` the returned metadata, so both the
    dispatcher-level tests here and the runner-wiring test can share one fake
    without cross-class instantiation."""

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
            sink.append((block, chunk_carrier(grid, children), ragged))
        if occupied is not None and kwargs.get("occupied_out") is not None:
            kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
        return pd.DataFrame(), meta(shard_key, error=error)

    return fake


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

    @staticmethod
    def _meta(shard, error=None):
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
        return _sharded_accumulate_fake(
            grid,
            self._chunk_carrier,
            self._meta,
            ragged_by_local=ragged_by_local,
            occupied=occupied,
            error=error,
        )

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
        names = ["morton", "h", *get_data_vars(cfg)]
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
        # The trailing second stamp is the LEAF COLUMN's own commit (§4.6 —
        # written after the leaf's, stamp-last in its own D4 order): since the
        # issue #384 /2 default flip, a default declaration writes the column.
        assert ops == ["leaf", "sidecar", "stamp", "stamp"]

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

    def test_torn_write_leaves_debris_then_retry_succeeds(self, monkeypatch, cfg, tmp_path):
        # Sharded twin of the streaming torn-write test: the sharded switch
        # defers every dense+ragged write to ONE post-stream
        # ``write_leaf_to_zarr``, so a worker that dies inside/after that write
        # (before ``stamp_commit``) leaves an UNSTAMPED prefix — debris. The
        # template is emitted (prefix exists) and the arrays land, but no stamp
        # follows, so ``read_commit`` is None. A clean retry overwrites the leaf
        # WHOLESALE and stamps. A stray object planted in the debris (one the
        # retry does NOT rewrite) pins the wholesale claim: a metadata-only
        # re-template would leave it inside a leaf whose stamp certifies it
        # complete (review findings, PR #205/#208).
        import zagg.processing as processing
        from zagg.store import open_store

        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        leaf = hive.shard_leaf_path(root, shard)

        fake = self._accumulate_fake(grid, {0: {"h": ([np.array([1.0])], [0])}})
        monkeypatch.setattr(processing, "process_shard", fake)
        real_leaf_write = processing.write_leaf_to_zarr

        def torn_leaf(*a, **k):
            real_leaf_write(*a, **k)  # the arrays land (prefix exists)...
            raise RuntimeError("worker died mid-shard")  # ...but no stamp follows

        monkeypatch.setattr(processing, "write_leaf_to_zarr", torn_leaf)
        with pytest.raises(RuntimeError, match="died mid-shard"):
            hive.process_and_write_hive(
                shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={}
            )
        assert os.path.exists(leaf)  # the prefix exists...
        assert hive.read_commit(open_store(leaf)) is None  # ...but is debris
        # No stamp -> no coverage visible either (issue #200): a torn worker
        # never publishes coverage.
        assert hive.read_coverage(open_store(leaf)) is None
        stale = os.path.join(leaf, grid.group_path, "stale-debris")
        with open(stale, "w") as fh:
            fh.write("torn attempt")
        # Plant a sidecar in the debris too: the one leaf object zarr does NOT
        # own must also fall to the wholesale wipe (PR #208 round 2).
        hive.write_coverage_sidecar(leaf, b"torn-attempt sidecar")
        sidecar = os.path.join(leaf, hive.COVERAGE_SIDECAR)
        assert os.path.exists(sidecar)

        # Retry with the real leaf writer: same leaf, overwritten wholesale —
        # the planted debris is GONE — and stamped at the end.
        monkeypatch.setattr(processing, "write_leaf_to_zarr", real_leaf_write)
        hive.process_and_write_hive(shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={})
        assert hive.read_commit(open_store(leaf))["complete"] is True
        assert not os.path.exists(stale), "stale torn-write object survived the re-template"
        assert not os.path.exists(sidecar), "torn attempt's sidecar survived the re-template"

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


class TestHiveProfileWritePhase:
    """Issues #249/#297: the hive worker's ``phase_timings`` carry an additive
    ``write`` phase next to process_shard's read/index/aggregate — the same
    split the flat Lambda handler has carried since issue #100. Collection is
    always-on since issue #297 (the stats sidecar needs complete timings);
    ``profile`` no longer changes this function's metadata."""

    _grid = TestProcessAndWriteHive._grid
    _carrier = TestProcessAndWriteHive._carrier
    _meta = TestProcessAndWriteHive._meta

    # The read/index/aggregate values the profiled process_shard fake seeds,
    # so tests can pin that the write split leaves them untouched.
    _SHARD_PHASES = {"read": 1.0, "index": 0.5, "aggregate": 0.25}

    def _profiled_fake(self, grid, ragged=None, error=None):
        """Streaming fake honoring the real (always-on, issue #297) contract:
        ``metadata['phase_timings']`` is always seeded."""

        def fake(g, shard_key, urls, **kwargs):
            meta = self._meta(shard_key, error=error)
            meta["phase_timings"] = dict(self._SHARD_PHASES)
            if error is None:
                carrier = self._carrier(grid, shard_key)
                kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, ragged or {})
            return pd.DataFrame(), meta

        return fake

    def _run(self, monkeypatch, cfg, tmp_path, fake, *, profile=False, name="store"):
        import zagg.processing as processing

        monkeypatch.setattr(processing, "process_shard", fake)
        grid = self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / name)
        meta = hive.process_and_write_hive(
            shard,
            ["s3://bucket/granule1.h5"],
            grid,
            {},
            root,
            cfg,
            store_kwargs={},
            profile=profile,
        )
        return grid, shard, root, meta

    def test_write_phase_added_nonnegative(self, monkeypatch, cfg, tmp_path):
        fake = self._profiled_fake(self._grid(cfg), ragged={"h": ([np.array([1.0, 2.0])], [0])})
        _grid, _shard, _root, meta = self._run(monkeypatch, cfg, tmp_path, fake)
        timings = meta["phase_timings"]
        # Additive: the process_shard phases keep their names and values.
        assert set(timings) == {"read", "index", "aggregate", "write", "hash"}
        assert {k: timings[k] for k in self._SHARD_PHASES} == self._SHARD_PHASES
        assert timings["write"] >= 0.0

    def test_sharded_leaf_write_counted(self, monkeypatch, cfg, tmp_path):
        # K>1 sharded: the single post-stream write_leaf_to_zarr pass lands in
        # the same write bucket.
        import zagg.processing as processing

        sharded_helper = TestProcessAndWriteHiveSharded()
        grid = sharded_helper._grid(cfg)

        def meta_with_phases(shard_key, error=None):
            meta = self._meta(shard_key, error=error)
            meta["phase_timings"] = dict(self._SHARD_PHASES)
            return meta

        fake = _sharded_accumulate_fake(grid, sharded_helper._chunk_carrier, meta_with_phases)
        monkeypatch.setattr(processing, "process_shard", fake)
        shard = _shard_word()
        meta = hive.process_and_write_hive(
            shard,
            ["s3://b/g1.h5"],
            grid,
            {},
            str(tmp_path / "store"),
            cfg,
            store_kwargs={},
        )
        assert meta["phase_timings"]["write"] >= 0.0
        # "column" rides the sharded default since the issue #384 /2 flip
        # (the sharded grid resolves an interior chunk order).
        assert set(meta["phase_timings"]) == {
            "read",
            "index",
            "aggregate",
            "write",
            "hash",
            "column",
        }

    def test_errored_shard_omits_write(self, monkeypatch, cfg, tmp_path):
        # Same gate as the flat handler (issue #100): a shard that wrote no
        # leaf carries no write phase — read/index/aggregate stay as reported.
        fake = self._profiled_fake(self._grid(cfg), error="No granules found")
        _grid, _shard, root, meta = self._run(monkeypatch, cfg, tmp_path, fake)
        assert meta["phase_timings"] == self._SHARD_PHASES
        assert "write" not in meta["phase_timings"]

    def test_default_path_collects_write_phase(self, monkeypatch, cfg, tmp_path):
        # Always-on collection (issue #297): the write bracket is stamped
        # without any profile flag — the sidecar record is complete by default.
        fake = self._profiled_fake(self._grid(cfg), ragged={"h": ([np.array([1.0, 2.0])], [0])})
        _grid, shard, root, meta = self._run(monkeypatch, cfg, tmp_path, fake)
        assert set(meta["phase_timings"]) == {"read", "index", "aggregate", "write", "hash"}
        # The leaf still landed, fully stamped.
        from zagg.store import open_store

        leaf = hive.shard_leaf_path(root, shard)
        assert hive.read_commit(open_store(leaf))["complete"] is True

    def test_write_phase_covers_the_granule_id_sibling(self, monkeypatch, cfg, tmp_path):
        # The bracket must account for EVERY PUT the seam makes (issue #388):
        # phase_timings["write"] flattens to phase_write on the D20 record and
        # into every run parquet, so a PUT outside it silently under-reports.
        import zagg.telemetry as telemetry

        real = telemetry.write_granule_ids

        def slow(*a, **k):
            time.sleep(0.05)
            return real(*a, **k)

        monkeypatch.setattr(telemetry, "write_granule_ids", slow)
        fake = self._profiled_fake(self._grid(cfg), ragged={"h": ([np.array([1.0, 2.0])], [0])})
        _grid, _shard, _root, meta = self._run(monkeypatch, cfg, tmp_path, fake)
        assert meta["phase_timings"]["write"] >= 0.05

    def test_sharded_default_path_stamps_write_when_timed(self, monkeypatch, cfg, tmp_path):
        # Sharded edition: the post-stream write_leaf_to_zarr bracket lands in
        # the always-on write bucket too (a fake without phase_timings gets no
        # write key — the "populated phase_timings" gate is unchanged).
        import zagg.processing as processing

        sharded_helper = TestProcessAndWriteHiveSharded()
        grid = sharded_helper._grid(cfg)
        fake = _sharded_accumulate_fake(grid, sharded_helper._chunk_carrier, self._meta)
        monkeypatch.setattr(processing, "process_shard", fake)
        shard = _shard_word()
        root = str(tmp_path / "store")
        meta = hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={}
        )
        # The fake seeds no phase_timings, so none appear (the write stamp
        # rides an existing dict); the sharded leaf still landed, stamped.
        assert "phase_timings" not in meta
        from zagg.store import open_store

        leaf = hive.shard_leaf_path(root, shard)
        assert hive.read_commit(open_store(leaf))["complete"] is True

    def test_profiled_leaf_bytes_match_unprofiled(self, monkeypatch, cfg, tmp_path):
        # Parity: profiling changes the returned metadata only — the leaf's
        # file set and bytes are identical (stamp compared modulo timestamp),
        # the same comparison the K==1 sharded no-op test pins.
        grid_probe = self._grid(cfg)
        ragged = {"h": ([np.array([1.0, 2.0])], [0])}
        outs: dict = {}
        leaves: dict = {}
        for tag, profile in (("on", True), ("off", False)):
            fake = self._profiled_fake(grid_probe, ragged=ragged)
            _grid, shard, root, meta = self._run(
                monkeypatch, cfg, tmp_path, fake, profile=profile, name=tag
            )
            leaf = hive.shard_leaf_path(root, shard)
            leaves[tag] = meta
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
        # And the metadata matches modulo the wall-clock timing VALUES
        # (collection is always-on since issue #297, so both carry the keys).
        on_meta = {k: v for k, v in leaves["on"].items() if k != "phase_timings"}
        off_meta = {k: v for k, v in leaves["off"].items() if k != "phase_timings"}
        assert on_meta == off_meta
        assert set(leaves["on"]["phase_timings"]) == set(leaves["off"]["phase_timings"])


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
    the lambda backend dispatches hive runs (issue #199 phase 3). Manifest
    lifecycle (issue #252 hybrid): the write lands at INIT — directly on the
    local path, as a fire-and-forget Event invoke of the setup mode right
    after the ping on the lambda path — and finalize re-ensures it as an
    idempotent backstop."""

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

    def test_local_hive_writes_manifest_before_cells(self, monkeypatch, cfg, tmp_path):
        from zagg import runner
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        calls = []

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            # The manifest lands at init (issue #252 hybrid): by the time any
            # cell runs it is already at the root, so a reader can consume
            # completed leaves while the store builds.
            assert os.path.exists(os.path.join(store_root, hive.MANIFEST_NAME))
            calls.append((int(shard_key), store_root))
            return {"shard_key": int(shard_key), "error": None, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        agg(cfg, catalog=catalog_path, store=root, backend="local")

        assert calls == [(shard, root)]
        # The run wrote ONLY root-level objects at the root — no shared zarr
        # template (D5): the manifest, its aggregation.yaml semantic core
        # (issue #299, D19 — rides the manifest write), the root coverage.moc
        # (issue #200 phase 3, default-on for hive), plus the successful
        # shard's node dir carrying its stats sidecar (issue #297; the mocked
        # worker wrote no leaf, so the node holds only stats.json), plus the
        # end-of-run sweep's own run record (issue #353 — the local dispatcher
        # sweeps in-process, so one lands at the root on every default hive
        # run; part of the contract, hence enumerated rather than filtered).
        listing = sorted(os.listdir(root))
        node = listing[0]
        parquets = [n for n in listing if n.startswith("stats_") and n.endswith(".parquet")]
        assert len(parquets) == 1  # run-level stats parquet (issue #297 phase 3)
        records = [n for n in listing if n.startswith("sweep_stats_") and n.endswith(".json")]
        assert len(records) == 1  # sweep run record (issue #353)
        assert listing == [
            node,
            hive.AGGREGATION_CORE_NAME,
            hive.ROOT_COVERAGE_NAME,
            hive.MANIFEST_NAME,
            parquets[0],
            records[0],
        ]
        from zagg.telemetry import read_sidecar

        leaf = hive.shard_leaf_path(root, shard)
        assert read_sidecar(leaf)["shard_key"] == shard
        assert hive.read_manifest(root)["shard_order"] == 6

    def test_local_hive_finalize_backstop_restores_lost_manifest(self, monkeypatch, cfg, tmp_path):
        # Issue #252 hybrid: the init-time write is primary, but finalize
        # keeps ensure_manifest as an idempotent backstop — a manifest lost
        # mid-run (simulated by deleting it inside the cell) is back at the
        # root by end of run.
        from zagg import runner
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        removed = []

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            # The init write is PRIMARY, so the manifest must already be at the
            # root when a cell runs — assert THEN remove, recording it. A
            # regressed init write (assert fails) or a swallowed
            # FileNotFoundError leaves ``removed`` empty, so the post-run check
            # below fails loudly instead of staying green on finalize alone
            # (``_cell_work`` swallows cell exceptions into an error envelope).
            path = os.path.join(store_root, hive.MANIFEST_NAME)
            assert os.path.exists(path)
            os.remove(path)
            removed.append(True)
            return {"shard_key": int(shard_key), "error": None, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        agg(cfg, catalog=catalog_path, store=root, backend="local")
        # The cell saw the init-written manifest and removed it (init→lost);
        # finalize restored it (lost→restored).
        assert removed == [True]
        assert hive.read_manifest(root)["shard_order"] == 6

    def test_local_hive_rerun_frozen_key_mismatch_fails_before_dispatch(
        self, monkeypatch, cfg, tmp_path
    ):
        # The manifest write runs at init (issue #252 hybrid) and
        # ensure_manifest runs the validate_manifest frozen-key precheck
        # first: a rerun into a root templated for DIFFERENT orders
        # (shard_order 5 vs the catalog's 6) must refuse BEFORE any cell
        # runs — not after fan-out has already mixed new-order leaves into
        # the old-order store (D2).
        from zagg import runner
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        other = HealpixGrid(parent_order=5, child_order=12, layout="fullsphere", config=cfg)
        hive.ensure_manifest(root, hive.build_manifest(other))

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        calls = []

        def fake_hive_write(*args, **kw):
            calls.append(args)
            return {"error": None, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        with pytest.raises(ValueError, match="clear the store root"):
            agg(cfg, catalog=catalog_path, store=root, backend="local")
        # Fail-fast: the precheck raised before dispatch, so no cell ran.
        assert calls == []

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

        # Share the sharded accumulate fake via the module-level helper (its
        # carrier/meta are the sharded class's statics) — no cross-class
        # instantiation.
        fake = _sharded_accumulate_fake(
            grid,
            TestProcessAndWriteHiveSharded._chunk_carrier,
            TestProcessAndWriteHiveSharded._meta,
            {0: {"h": ([np.array([2.5])], [1])}},
        )

        from zagg import runner

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        monkeypatch.setattr(processing, "process_shard", fake)
        agg(cfg, catalog=catalog_path, store=root, backend="local")

        leaf = hive.shard_leaf_path(root, shard)
        for name in ("morton", "h"):
            chunk_dir = os.path.join(leaf, grid.group_path, name, "c")
            n_objects = sum(len(files) for _d, _s, files in os.walk(chunk_dir))
            assert n_objects == 1, name
        assert hive.read_commit(open_store(leaf))["complete"] is True

    def test_local_rerun_skips_current_leaves(self, monkeypatch, cfg, tmp_path):
        """Issue #388 acceptance (local backend): an identical rerun no-ops
        every leaf — the fold never runs, the sidecar is NOT clobbered, and
        the unit counts as ``cells_current``, never ``cells_with_data``."""
        import zagg.processing as processing
        from zagg import runner
        from zagg.grids import from_config
        from zagg.runner import agg
        from zagg.telemetry import read_granule_ids, read_sidecar

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        grid = from_config(cfg, parent_order=6)
        helper = TestProcessAndWriteHive()
        calls = []

        def fake(g, shard_key, urls, **kwargs):
            calls.append(int(shard_key))
            return helper._streaming_fake(grid)(g, shard_key, urls, **kwargs)

        monkeypatch.setattr(processing, "process_shard", fake)
        s1 = agg(cfg, catalog=catalog_path, store=root, backend="local")
        assert calls == [shard]
        assert (s1["cells_current"], s1["cells_refused"], s1["cells_unrecorded"]) == (0, 0, 0)
        assert (s1["objects_touched"], s1["touch_failures"]) == (0, 0)
        leaf = hive.shard_leaf_path(root, shard)
        sidecar = read_sidecar(leaf)
        # The id LIST is the sidecar's sibling, never a record key (issue #388).
        assert "granule_ids" not in sidecar
        # The canonical id space (espg-ruled 2026-08-17): the sibling records
        # the driver-stripped bare id, not the resolved href.
        assert read_granule_ids(leaf)["granule_ids"] == ["granule1.h5"]

        # Age the store-ROOT objects: ensure_manifest early-returns on a
        # frozen-key match, so without the phase-3 root touch they would keep
        # their original LastModified across arbitrarily many skip reruns
        # (review finding) — the D6 manifest expiring is a bricked store.
        roots = [
            os.path.join(root, hive.MANIFEST_NAME),
            os.path.join(root, hive.AGGREGATION_CORE_NAME),
        ]
        for path in roots:
            os.utime(path, (10_000, 10_000))

        s2 = agg(cfg, catalog=catalog_path, store=root, backend="local")
        assert calls == [shard]  # the fold did NOT run again
        assert s2["cells_current"] == 1 and s2["cells_with_data"] == 0
        assert read_sidecar(leaf) == sidecar  # a skip never clobbers the sidecar
        # ...and the lifecycle touch rollup rides the summary (phase 3).
        assert s2["objects_touched"] > 0 and s2["touch_failures"] == 0
        assert all(os.stat(path).st_mtime_ns > 10_000 * 10**9 for path in roots)
        # A skipped unit contributes no run-parquet row: the rerun had no
        # rows at all, so the fail-open write skipped (path None).
        assert s2["run_stats_path"] is None

    def test_local_rerun_overwrite_disables_the_skip(self, monkeypatch, cfg, tmp_path):
        # overwrite=True is the operator's big hammer: today's unconditional
        # rewrite, no identity gate (PR question (2)'s documented posture).
        import zagg.processing as processing
        from zagg import runner
        from zagg.grids import from_config
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        grid = from_config(cfg, parent_order=6)
        helper = TestProcessAndWriteHive()
        calls = []

        def fake(g, shard_key, urls, **kwargs):
            calls.append(int(shard_key))
            return helper._streaming_fake(grid)(g, shard_key, urls, **kwargs)

        monkeypatch.setattr(processing, "process_shard", fake)
        agg(cfg, catalog=catalog_path, store=root, backend="local")
        s2 = agg(cfg, catalog=catalog_path, store=root, backend="local", overwrite=True)
        assert calls == [shard, shard] and s2["cells_current"] == 0

    def test_local_rerun_contraction_refuses_then_flag_rewrites(self, monkeypatch, cfg, tmp_path):
        """Issue #388 acceptance: a contracted shardmap REFUSES per leaf
        (``cells_refused``, never an error; sidecar and leaf intact), and
        ``allow_contraction=True`` proceeds as a normal wholesale rewrite."""
        import zagg.processing as processing
        from zagg import runner
        from zagg.grids import from_config
        from zagg.runner import agg
        from zagg.telemetry import read_granule_ids, read_sidecar

        cfg.output["store_layout"] = "hive"
        shard = _shard_word()

        def _cat(name, granules):
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
                "granules": [granules],
            }
            p = tmp_path / name
            p.write_text(json.dumps(catalog))
            return str(p)

        full = _cat("catalog_full.json", [_rec(1), _rec(2)])
        contracted = _cat("catalog_contracted.json", [_rec(1)])
        root = str(tmp_path / "out")
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        grid = from_config(cfg, parent_order=6)
        helper = TestProcessAndWriteHive()
        calls = []

        def fake(g, shard_key, urls, **kwargs):
            calls.append(int(shard_key))
            return helper._streaming_fake(grid)(g, shard_key, urls, **kwargs)

        monkeypatch.setattr(processing, "process_shard", fake)
        agg(cfg, catalog=full, store=root, backend="local")
        leaf = hive.shard_leaf_path(root, shard)
        sidecar = read_sidecar(leaf)
        recorded_ids = read_granule_ids(leaf)
        assert len(calls) == 1 and len(recorded_ids["granule_ids"]) == 2

        s2 = agg(cfg, catalog=contracted, store=root, backend="local")
        assert len(calls) == 1  # refused: the fold never ran
        assert s2["cells_refused"] == 1 and s2["cells_error"] == 0
        # leaf, sidecar and recorded id list all untouched by the refusal
        assert read_sidecar(leaf) == sidecar
        assert read_granule_ids(leaf) == recorded_ids
        # ...and the refusal's ONLY durable trace: the store-root manifest,
        # carrying the full diff a truncated log line cannot (issue #388).
        manifest = json.loads(open(s2["refusal_manifest_path"]).read())
        assert manifest["cells_refused"] == 1
        unit = manifest["units"][0]
        assert unit["shard_key"] == int(shard) and unit["identity"] == "contraction"
        assert unit["missing_granules"] == ["granule2.h5"]
        assert os.path.basename(s2["refusal_manifest_path"]).startswith("refusals_")

        s3 = agg(cfg, catalog=contracted, store=root, backend="local", allow_contraction=True)
        assert len(calls) == 2  # the flag proceeds as a normal D4 rewrite
        assert s3["cells_refused"] == 0 and s3["cells_with_data"] == 1
        # The canonical id space (espg-ruled 2026-08-17): the sibling records
        # the driver-stripped bare id, not the resolved href.
        assert read_granule_ids(leaf)["granule_ids"] == ["granule1.h5"]
        # Ruled (9)(a): a run with nothing refused writes no manifest.
        assert s3["refusal_manifest_path"] is None

    def test_lambda_hive_fires_async_setup_after_ping(self, monkeypatch, cfg, tmp_path):
        # Issue #252 hybrid: a hive lambda run dispatches NO synchronous
        # setup invoke. The lifecycle is ping (fail-fast, both guards) →
        # async Event setup (the manifest write, before any worker fan-out
        # → progressive reads) → workers → finalize (idempotent backstop,
        # which runs even with consolidate_metadata off — the default) and
        # carries the same manifest inputs. Per-cell events need NO new
        # keys — the worker derives everything from the config dict.
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport
        from zagg.runner import agg

        cfg.output["store_layout"] = "hive"
        catalog_path, shard = self._catalog(tmp_path)
        captured: dict = {}
        order: list = []

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

        def _capture(name):
            def _f(*a, **kw):
                order.append(name)
                captured[name] = kw

            return _f

        monkeypatch.setattr(runner, "_invoke_lambda_setup", _capture("setup"))
        monkeypatch.setattr(runner, "_invoke_lambda_setup_async", _capture("setup_async"))
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", _capture("finalize"))
        monkeypatch.setattr(runner, "_invoke_lambda_ping", _capture("ping"))

        def fake_cell(client, chunk_idx, shard_key, *a, **k):
            order.append("cell")
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

        # NO synchronous setup invoke; the manifest write (async setup) fires
        # AFTER the ping passes and BEFORE any worker — so the manifest lands
        # seconds into the run — and finalize backstops it at the end.
        assert "setup" not in captured
        assert order == ["ping", "setup_async", "cell", "finalize"]
        assert captured["ping"]["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert captured["ping"]["config_dict"]["output"]["store_layout"] == "hive"
        # The async setup carries the same manifest inputs as ping/finalize.
        assert captured["setup_async"]["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert captured["setup_async"]["config_dict"]["output"]["store_layout"] == "hive"
        assert captured["setup_async"]["parent_order"] == 6
        assert captured["finalize"]["dataset"] == {"short_name": "ATL06", "version": "007"}
        # store_layout rides in the config dict already serialized into events.
        assert captured["finalize"]["config_dict"]["output"]["store_layout"] == "hive"
        # The per-cell event schema is unchanged: shard_key stays the packed int.
        assert captured["cell_shard_key"] == shard

    def test_lambda_flat_setup_and_finalize_unchanged(self, monkeypatch, cfg, tmp_path):
        # Flat runs keep the pre-#252 lifecycle: the setup invoke still runs
        # (no dataset threading — that was hive-only and left with the fold)
        # and finalize stays gated on consolidate_metadata (off by default).
        # Flat is explicit now (issue #253: omitted store_layout -> hive).
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport
        from zagg.runner import agg

        cfg.output["store_layout"] = "flat"
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
        monkeypatch.setattr(
            runner, "_invoke_lambda_finalize", lambda *a, **kw: captured.update(finalize=kw)
        )
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
        monkeypatch.setattr(
            runner, "_invoke_lambda_ping", lambda *a, **kw: captured.update(ping=kw)
        )
        monkeypatch.setattr(
            runner, "_invoke_lambda_setup_async", lambda *a, **kw: captured.update(setup_async=kw)
        )
        agg(cfg, catalog=catalog_path, store="s3://out/x.zarr", backend="lambda")
        assert "dataset" not in captured["setup"]
        assert "finalize" not in captured
        assert "ping" not in captured
        # The async manifest write is hive-only (issue #252 hybrid).
        assert "setup_async" not in captured


def _wire_client(body: dict, status_code: int = 200):
    """Mocked boto3 lambda client capturing ``Payload`` on the wire."""
    from unittest.mock import MagicMock

    payload = MagicMock()
    payload.read.return_value = json.dumps(
        {"statusCode": status_code, "body": json.dumps(body)}
    ).encode()
    client = MagicMock()
    client.invoke.return_value = {"Payload": payload, "FunctionError": None}
    return client


class TestInvokeLambdaSetupEvent:
    """Pin the ACTUAL setup events on the wire. The synchronous invoke is
    flat-only now (issue #252 moved the hive manifest write to an async
    Event invoke of the same setup mode, and the PR #205 layout-echo guard
    into the version ping): pin flat byte-identity against pre-phase-3
    deployed functions, and the hive async event + its fire-and-forget
    InvocationType."""

    @staticmethod
    def _invoke(client, config_dict):
        from zagg.runner import _invoke_lambda_setup

        _invoke_lambda_setup(
            client,
            "process-shard",
            "s3://out/product",
            parent_order=6,
            child_order=12,
            overwrite=False,
            config_dict=config_dict,
        )
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_flat_event_matches_baseline(self, cfg):
        # The byte-identity claim, pinned on the wire: no "dataset" key, no
        # raster "times_us" key (issue #264 uses a separate helper), and the
        # event is exactly the pre-phase-3 flat setup event. Flat is now
        # explicit (issue #253: an omitted store_layout defaults to hive).
        cfg.output["store_layout"] = "flat"
        config_dict = asdict(cfg)
        client = _wire_client({"ok": True, "mode": "setup", "layout": "flat"})
        event = self._invoke(client, config_dict)
        assert "dataset" not in event
        assert "times_us" not in event
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
        # keep working against them (explicit flat — issue #253 defaults hive;
        # the defaulted-hive stale-deployment guard is the ping now, issue
        # #252, pinned by the ping tests above).
        cfg.output["store_layout"] = "flat"
        self._invoke(_wire_client({"ok": True, "mode": "setup"}), asdict(cfg))

    def test_hive_async_event_matches_baseline(self, cfg):
        # The async init-time manifest write (issue #252 hybrid): a
        # fire-and-forget InvocationType="Event" invoke of the hive setup
        # branch, carrying exactly the manifest inputs the ping/finalize
        # events pin (config + parent_order + dataset identity + overwrite).
        from zagg.runner import _invoke_lambda_setup_async

        cfg.output["store_layout"] = "hive"
        config_dict = asdict(cfg)
        client = _wire_client({"ok": True, "mode": "setup", "layout": "hive"})
        _invoke_lambda_setup_async(
            client,
            "process-shard",
            "s3://out/product",
            config_dict=config_dict,
            dataset={"short_name": "ATL06", "version": "007"},
            parent_order=6,
            overwrite=False,
        )
        kwargs = client.invoke.call_args.kwargs
        assert kwargs["InvocationType"] == "Event"
        assert json.loads(kwargs["Payload"]) == {
            "mode": "setup",
            "store_path": "s3://out/product",
            "parent_order": 6,
            "overwrite": False,
            "config": config_dict,
            "dataset": {"short_name": "ATL06", "version": "007"},
        }
        # Fire-and-forget: no response is read (an Event invoke returns 202
        # with no function payload), so nothing can block on it.
        client.invoke.return_value["Payload"].read.assert_not_called()

    def test_hive_async_event_threads_creds_and_overwrite(self, cfg):
        # The async setup is the PRIMARY manifest write and fire-and-forget
        # (no response read), so a drifted ``output_credentials`` key or
        # ``overwrite`` flag would fail SILENTLY — the miss only surfacing as
        # the finalize backstop doing the "primary" write. Pin the two
        # conditional branches on the wire: overwrite=True and an
        # output_creds_event both reach the Event payload exactly.
        from zagg.runner import _invoke_lambda_setup_async

        cfg.output["store_layout"] = "hive"
        config_dict = asdict(cfg)
        client = _wire_client({"ok": True, "mode": "setup", "layout": "hive"})
        creds = {"aws_access_key_id": "AK", "aws_secret_access_key": "SK"}
        _invoke_lambda_setup_async(
            client,
            "process-shard",
            "s3://out/product",
            config_dict=config_dict,
            dataset={"short_name": "ATL06", "version": "007"},
            parent_order=6,
            overwrite=True,
            output_creds_event=creds,
        )
        kwargs = client.invoke.call_args.kwargs
        assert kwargs["InvocationType"] == "Event"
        assert json.loads(kwargs["Payload"]) == {
            "mode": "setup",
            "store_path": "s3://out/product",
            "parent_order": 6,
            "overwrite": True,
            "config": config_dict,
            "dataset": {"short_name": "ATL06", "version": "007"},
            "output_credentials": creds,
        }
        client.invoke.return_value["Payload"].read.assert_not_called()


class TestInvokeLambdaFinalizeEvent:
    """Pin the ACTUAL finalize event on the wire (issue #252): hive carries
    the manifest inputs (mirroring the retired hive setup event); flat stays
    byte-identical to the pre-fold finalize event."""

    @staticmethod
    def _invoke(client, **kw):
        from zagg.runner import _invoke_lambda_finalize

        _invoke_lambda_finalize(client, "process-shard", "s3://out/product", **kw)
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_flat_event_matches_baseline(self):
        event = self._invoke(_wire_client({"ok": True, "mode": "finalize"}))
        assert event == {"mode": "finalize", "store_path": "s3://out/product"}

    def test_hive_event_carries_manifest_inputs(self, cfg):
        cfg.output["store_layout"] = "hive"
        config_dict = asdict(cfg)
        event = self._invoke(
            _wire_client({"ok": True, "mode": "finalize", "layout": "hive"}),
            config_dict=config_dict,
            dataset={"short_name": "ATL06", "version": "007"},
            parent_order=6,
            overwrite=False,
        )
        assert event["config"] == config_dict
        assert event["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert event["parent_order"] == 6
        assert event["overwrite"] is False

    def test_non_200_raises(self):
        # The manifest is REQUIRED reader-facing schema (D6): a failed hive
        # finalize must raise, unlike the fail-open root coverage.moc (D9).
        with pytest.raises(RuntimeError, match="Lambda finalize error"):
            self._invoke(
                _wire_client({"error": "boom", "mode": "finalize"}, status_code=500),
                config_dict={"output": {"store_layout": "hive"}},
                parent_order=6,
            )


class TestInvokeLambdaPingEvent:
    """Pin the ACTUAL ping event on the wire and the fail-fast guard (issue
    #252, replacing the PR #205 layout-echo guard): the ping carries the same
    manifest inputs as hive finalize, and any non-200 — a stale function's
    process-handler 400 fall-through, or the handler's validate_manifest
    refusal — raises before a single worker is dispatched. The two modes get
    distinct remedies: a 400 without ``mode: "ping"`` says redeploy, a 500
    that echoes ``mode: "ping"`` says clear the store root."""

    @staticmethod
    def _invoke(client, config_dict, **kw):
        from zagg.runner import _invoke_lambda_ping

        _invoke_lambda_ping(
            client,
            "process-shard",
            "s3://out/product",
            config_dict=config_dict,
            **kw,
        )
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_event_carries_manifest_inputs(self, cfg):
        cfg.output["store_layout"] = "hive"
        config_dict = asdict(cfg)
        event = self._invoke(
            _wire_client({"ok": True, "mode": "ping", "zagg_version": "1.2.3"}),
            config_dict,
            dataset={"short_name": "ATL06", "version": "007"},
            parent_order=6,
            overwrite=False,
        )
        assert event["mode"] == "ping"
        assert event["config"] == config_dict
        assert event["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert event["parent_order"] == 6
        assert event["overwrite"] is False

    def test_stale_function_fall_through_fails_fast(self, cfg):
        # A pre-#252 function doesn't know mode="ping": the event falls
        # through to its process handler, which 400s the key-less event with
        # ZERO writes — the dispatcher turns that into the redeploy message.
        cfg.output["store_layout"] = "hive"
        with pytest.raises(RuntimeError, match="redeploy"):
            self._invoke(
                _wire_client({"error": "shard_key required"}, status_code=400),
                asdict(cfg),
                parent_order=6,
            )

    def test_write_probe_refusal_names_the_grant_not_the_store(self, cfg):
        # Issue #495: a read-only-but-valid grant fails the ping's write probe.
        # The 500 carries check="write_probe", and the dispatcher must NOT
        # reuse the store-contents remedy -- clearing a store root that is not
        # the problem is the wrong instruction, and the run has to stop before
        # the fan-out spends compute it will fail to write.
        cfg.output["store_layout"] = "hive"
        with pytest.raises(RuntimeError, match="could not WRITE") as excinfo:
            self._invoke(
                _wire_client(
                    {"error": "Access Denied", "mode": "ping", "check": "write_probe"},
                    status_code=500,
                ),
                asdict(cfg),
                parent_order=6,
            )
        assert "clear the store root" not in str(excinfo.value)
        assert "grant" in str(excinfo.value)

    def test_probe_delete_failure_warns_the_operator(self, cfg, caplog):
        # Issue #495 fold: the DELETE half is fail-open, but its outcome has to
        # REACH the operator. A Put-but-no-Delete grant returns 200 with
        # probe_delete=false; the dispatcher names the stranded key and the
        # missing action, because s3:DeleteObject is not optional for zagg's
        # real writes (store overwrite, manifest cleanup) and the function's own
        # log group is not where the dispatching operator is looking.
        cfg.output["store_layout"] = "hive"
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            self._invoke(
                _wire_client(
                    {
                        "ok": True,
                        "mode": "ping",
                        "zagg_version": "1.2.3",
                        "write_probe": True,
                        "probe_delete": False,
                        "probe_key": "s3://out/product.status/probe-abc",
                    }
                ),
                asdict(cfg),
                parent_order=6,
            )
        warned = "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)
        assert "s3://out/product.status/probe-abc" in warned
        assert "s3:DeleteObject" in warned

    def test_probe_delete_success_is_silent(self, cfg, caplog):
        # The happy path stays quiet: no warning when the round trip completed.
        cfg.output["store_layout"] = "hive"
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            self._invoke(
                _wire_client(
                    {"ok": True, "mode": "ping", "write_probe": True, "probe_delete": True}
                ),
                asdict(cfg),
                parent_order=6,
            )
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_validate_refusal_fails_fast(self, cfg):
        # The handler's read-only validate_manifest refusal (frozen-key
        # mismatch, D2) surfaces pre-fan-out with the store remedy, not
        # redeploy: the 500 body echoes mode="ping", so the message points at
        # clearing the store root rather than a stale deployment.
        cfg.output["store_layout"] = "hive"
        with pytest.raises(RuntimeError, match="clear the store root"):
            self._invoke(
                _wire_client(
                    {"error": "morton_hive.json ... does not match this run", "mode": "ping"},
                    status_code=500,
                ),
                asdict(cfg),
                parent_order=6,
            )
