"""has_run dedup statuses (issue #299 phase 4, D19).

The acceptance criteria from the issue thread: "hit" only when identity AND
catalog match; a catalog-grown shard reports "stale", never "hit"; debris and
absent leaves are plain misses.
"""

import copy

import zarr

from zagg import hive
from zagg.config import default_config
from zagg.dedup import has_run, shard_status
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.semantics import semantic_hash
from zagg.store import open_store
from zagg.telemetry import build_record, write_sidecar

WORD = morton_word("1121121")  # order-6 shard key
GRANULES = ["s3://b/g1.h5", "s3://b/g2.h5"]


def _cfg():
    return default_config("atl06")


def _grid(cfg):
    return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)


def _write_leaf(root, cfg, *, stamp=True, sidecar=True, sidecar_hash="match", granules=GRANULES):
    """Emit + optionally stamp a leaf, optionally with a stats sidecar."""
    leaf = hive.shard_leaf_path(root, WORD)
    store = open_store(leaf)
    _grid(cfg).emit_shard_template(store, overwrite=True)
    if stamp:
        group = zarr.open_group(store, path="", mode="a", zarr_format=3)
        group.attrs[hive.COMMIT_ATTR] = {"cells_with_data": 1}
    if sidecar:
        recorded = {
            "match": semantic_hash(cfg),
            "other": "0" * 64,
            "absent": None,
        }[sidecar_hash]
        record = build_record(
            shard_key=WORD,
            metadata={"total_obs": 2, "cells_with_data": 1, "duration_s": 0.1},
            granule_ids=granules,
            semantic_hash=recorded,
        )
        write_sidecar(leaf, record)
    return leaf


class TestShardStatus:
    def test_absent_leaf_is_miss(self, tmp_path):
        cfg = _cfg()
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status == {"status": "miss"}

    def test_unstamped_leaf_is_miss(self, tmp_path):
        # Debris (D4): a template without the commit stamp is invisible.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, stamp=False, sidecar=False)
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status["status"] == "miss"

    def test_stamped_without_sidecar_is_stale(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, sidecar=False)
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status["status"] == "stale"
        assert "sidecar" in status["reason"]

    def test_full_match_is_hit(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(cfg), granule_ids=GRANULES
        )
        assert status["status"] == "hit"
        assert status["semantic_hash_match"] is True
        assert status["catalog_match"] is True

    def test_catalog_growth_is_stale_never_hit(self, tmp_path):
        # The headline acceptance criterion: ATL03 is a living collection —
        # a grown catalog must recompute, not skip.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        status = shard_status(
            str(tmp_path),
            WORD,
            semantic_hash=semantic_hash(cfg),
            granule_ids=[*GRANULES, "s3://b/g3.h5"],
        )
        assert status["status"] == "stale"
        assert status["catalog_match"] is False
        assert status["semantic_hash_match"] is True

    def test_semantic_mismatch_is_stale(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        other = copy.deepcopy(cfg)
        other.aggregation["variables"]["count"]["dtype"] = "int64"
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(other), granule_ids=GRANULES
        )
        assert status["status"] == "stale"
        assert status["semantic_hash_match"] is False

    def test_pre299_sidecar_is_stale(self, tmp_path):
        # A pre-#299 sidecar records no semantic_hash: unverifiable identity
        # degrades to recompute, never to a false hit.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, sidecar_hash="absent")
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(cfg), granule_ids=GRANULES
        )
        assert status["status"] == "stale"


class TestHasRun:
    def test_mapping_input_checks_catalog(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        out = has_run(str(tmp_path), cfg, {WORD: GRANULES})
        assert out[WORD]["status"] == "hit"
        grown = has_run(str(tmp_path), cfg, {WORD: [*GRANULES, "s3://b/new.h5"]})
        assert grown[WORD]["status"] == "stale"

    def test_iterable_input_skips_catalog_check(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        out = has_run(str(tmp_path), cfg, [WORD])
        assert out[WORD]["status"] == "hit"
        assert out[WORD]["catalog_match"] is None

    def test_spec_defaults_from_manifest(self, tmp_path):
        # The sidecar key grammar is spec-keyed (#307); has_run reads the
        # manifest once for it. A manifest-less root uses the legacy names.
        cfg = _cfg()
        root = str(tmp_path)
        hive.ensure_manifest(root, hive.build_manifest(_grid(cfg)))
        _write_leaf(root, cfg)
        assert has_run(root, cfg, [WORD])[WORD]["status"] == "hit"

    def test_missing_shards_reported(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        other = morton_word("2431123")
        out = has_run(str(tmp_path), cfg, [WORD, other])
        assert out[WORD]["status"] == "hit"
        assert out[other]["status"] == "miss"
