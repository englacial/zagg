"""The manifest ``multiscales`` discovery mirror (issue #392, spec §4.9)."""

import json

import obstore
import pytest

from zagg.config import PipelineConfig
from zagg.grids.healpix import HealpixGrid
from zagg.hive import MANIFEST_NAME, build_manifest, ensure_manifest, read_manifest
from zagg.multiscales import (
    ARTIFACT_COLUMN,
    ARTIFACT_OVERVIEW,
    MULTISCALES_SPEC,
    manifest_multiscales,
    multiscales_block,
)
from zagg.pyramid import PYRAMID_SPEC_V2, declared_fields, expand_overviews, overview_block_v2
from zagg.store import open_object_store
from zagg.sweep_overview import PYRAMID_SPEC, declare_pyramid

SHARD_ORDER = 2
CELL_ORDER = 4


def _cfg(pyramid=None):
    """A grid-carrying config on the (2, 4) window; ``chunk_inner`` 3."""
    config = PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_mean": {"function": "mean", "dtype": "float32"},
                "h_tdigest": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest",
                    "inner_shape": [2],
                    "dtype": "float32",
                    "fill_value": 0,
                },
            },
        }
    )
    config.output["grid"] = {
        "type": "healpix",
        "parent_order": SHARD_ORDER,
        "child_order": CELL_ORDER,
        "chunk_inner": 3,
        "sharded": True,
    }
    if pyramid is not None:
        config.output["pyramid"] = pyramid
    return config


def _grid(config):
    return HealpixGrid(SHARD_ORDER, CELL_ORDER, config=config, chunk_inner=3, sharded=True)


def _v2_block(cfg):
    """A real ``/2`` pyramid block through the production grammar path."""
    fields, excluded = declared_fields(cfg)
    knob = cfg.output.get("pyramid") or {}
    levels = expand_overviews([3], parent_order=SHARD_ORDER)
    return overview_block_v2(knob, levels, ("cascade", 1), fields, excluded)


class TestMultiscalesBlock:
    def test_mirror_projects_the_v2_block(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        ms = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER, name="T")
        assert isinstance(ms, list) and len(ms) == 1  # ome-style: a LIST of one
        (entry,) = ms
        assert entry["spec"] == MULTISCALES_SPEC
        assert entry["name"] == "T"
        # datasets mirror the recorded overviews verbatim, finest first, with
        # the artifact kind: the shard-order entry is the §4.6 column, every
        # ladder entry an ancestor overview.
        assert entry["datasets"] == [
            {"order": 2, "cells": [3], "artifact": ARTIFACT_COLUMN},
            {"order": 1, "cells": [2], "artifact": ARTIFACT_OVERVIEW},
            {"order": 0, "cells": [1], "artifact": ARTIFACT_OVERVIEW},
        ]
        # the flat per-order lookup: keys are exactly the orders present.
        assert entry["order2res"] == {"2": [3], "1": [2], "0": [1]}
        # native source data is the base, never a dataset entry (§4.5: a
        # member at the base data's own order would BE the base data).
        assert entry["base"] == {"order": SHARD_ORDER, "cells": [CELL_ORDER]}
        assert entry["fields"] == {
            "count": "exact",
            "h_mean": "none",
            "h_tdigest": "approximate",
        }
        assert entry["fold"] == {"fold_source": "cascade", "exact_levels": 1}

    def test_leaves_regime_carries_no_exact_levels(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        block["overview"].pop("exact_levels")
        block["overview"]["fold_source"] = "leaves"
        (entry,) = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER)
        assert entry["fold"] == {"fold_source": "leaves"}

    @pytest.mark.parametrize(
        "pyramid",
        [
            None,
            "off",  # hand-edited non-dict
            {"spec": PYRAMID_SPEC, "overview": {"orders": []}},  # declared-off /1
            {"spec": PYRAMID_SPEC, "overview": {"orders": [1, 0], "spacing": 2}},  # /1
            {"spec": PYRAMID_SPEC_V2, "overview": {}},  # /2 marker, no overviews
            {"spec": PYRAMID_SPEC_V2, "overviews": []},  # never-empty rule
            {"spec": "zagg-pyramid/3", "overviews": [{"node": 1, "cells": [2]}]},
        ],
    )
    def test_nothing_to_mirror(self, pyramid):
        assert multiscales_block(pyramid, shard_order=2, cell_order=4) is None

    def test_mirror_is_canonical_json(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        ms = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER)
        assert json.loads(json.dumps(ms)) == ms


class TestManifestMultiscales:
    def test_derives_from_manifest_orders_and_dataset(self):
        cfg = _cfg(pyramid={"overviews": 3})
        manifest = build_manifest(_grid(cfg), dataset={"short_name": "MS", "version": "1"})
        ms = manifest_multiscales(manifest)
        assert ms == manifest["multiscales"]
        assert ms[0]["name"] == "MS"

    def test_missing_orders_warns_never_raises(self, caplog):
        manifest = {"pyramid": {"spec": PYRAMID_SPEC_V2, "overviews": [{"node": 1, "cells": [2]}]}}
        with caplog.at_level("WARNING"):
            assert manifest_multiscales(manifest) is None
        assert "mirror derivation skipped" in caplog.text


class TestTemplateTime:
    def test_default_v2_manifest_carries_the_mirror(self):
        # No knob at all: the issue #384 default flip declares /2 at the
        # resolved chunk order, and the mirror rides along.
        manifest = build_manifest(_grid(_cfg()), dataset={"short_name": "MS", "version": "1"})
        assert manifest["pyramid"]["spec"] == PYRAMID_SPEC_V2
        (entry,) = manifest["multiscales"]
        assert entry["order2res"] == {"2": [3], "1": [2], "0": [1]}
        assert entry["datasets"][0] == {"order": 2, "cells": [3], "artifact": ARTIFACT_COLUMN}

    @pytest.mark.parametrize("pyramid", [False, {"orders": [1]}])
    def test_v1_and_declared_off_manifests_carry_no_key(self, pyramid):
        manifest = build_manifest(_grid(_cfg(pyramid=pyramid)), dataset={})
        assert manifest["pyramid"]["spec"] == PYRAMID_SPEC
        assert "multiscales" not in manifest


class TestDeclarePyramid:
    def _store(self, root, cfg):
        ensure_manifest(str(root), build_manifest(_grid(cfg), dataset={"short_name": "MS"}))

    def test_retrofit_installs_the_mirror(self, tmp_path):
        # Template under /1 (explicit orders), retrofit to /2: the mirror
        # appears with the declaration. No leaves on purpose — the field
        # probe skips loudly and declaration remains legal (issue #358).
        self._store(tmp_path, _cfg(pyramid={"orders": [1]}))
        assert "multiscales" not in read_manifest(str(tmp_path))
        summary = declare_pyramid(str(tmp_path), _cfg(pyramid={"overviews": 3}))
        assert summary["updated"] is True and summary["multiscales"] is True
        manifest = read_manifest(str(tmp_path))
        assert manifest["multiscales"] == manifest_multiscales(manifest)
        assert manifest["multiscales"][0]["order2res"] == {"2": [3], "1": [2], "0": [1]}

    def test_identical_redeclaration_is_no_put(self, tmp_path, monkeypatch):
        cfg = _cfg(pyramid={"overviews": 3})
        self._store(tmp_path, cfg)
        puts = []
        real_put = obstore.put
        monkeypatch.setattr(obstore, "put", lambda *a, **k: (puts.append(a), real_put(*a, **k))[1])
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is False and summary["previous"] == "identical"
        assert summary["multiscales"] is True
        assert puts == []

    def test_pre_mirror_v2_store_gains_the_key(self, tmp_path):
        # A store declared /2 before issue #392: pyramid block identical,
        # mirror absent — the one "identical" case that still PUTs.
        cfg = _cfg(pyramid={"overviews": 3})
        self._store(tmp_path, cfg)
        manifest = read_manifest(str(tmp_path))
        del manifest["multiscales"]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "identical" and summary["updated"] is True
        manifest = read_manifest(str(tmp_path))
        assert manifest["multiscales"] == manifest_multiscales(manifest)

    def test_declaring_off_removes_the_mirror(self, tmp_path):
        self._store(tmp_path, _cfg(pyramid={"overviews": 3}))
        assert "multiscales" in read_manifest(str(tmp_path))
        summary = declare_pyramid(str(tmp_path), _cfg(pyramid=False))
        assert summary["updated"] is True and summary["multiscales"] is False
        assert "multiscales" not in read_manifest(str(tmp_path))
