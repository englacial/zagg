"""Semantic-core hash canonicalization (issue #299 phase 1, D19 / §8.3).

The §8.3 obligations: syntactic edits (whitespace, key order, comments) never
change the hash; packaging-knob edits (orders, chunking, worker size, read
machinery, carrier) never change it; any semantic edit does.
"""

import copy

import pytest
import yaml

from zagg.config import PipelineConfig, default_config
from zagg.semantics import (
    canonical_semantic_json,
    semantic_core,
    semantic_fingerprint,
    semantic_hash,
)


def _cfg(**overrides) -> PipelineConfig:
    cfg = default_config("atl06")
    for dotted, value in overrides.items():
        parts = dotted.split("__")
        target = getattr(cfg, parts[0])
        for p in parts[1:-1]:
            target = target.setdefault(p, {})
        target[parts[-1]] = value
    return cfg


class TestCanonicalization:
    def test_hash_shape(self):
        h = semantic_hash(_cfg())
        assert len(h) == 64 and int(h, 16) >= 0
        assert semantic_fingerprint(h) == h[:12]

    def test_deterministic(self):
        assert semantic_hash(_cfg()) == semantic_hash(_cfg())

    def test_yaml_syntax_never_changes_hash(self):
        # Same semantics, different comments/whitespace/key order.
        a = yaml.safe_load(
            "data_source:\n"
            "  reader: h5coro\n"
            "  groups: [gt1l]\n"
            "  variables: {h_li: /p}\n"
            "  coordinates: {latitude: /lat, longitude: /lon}\n"
            "aggregation:\n"
            "  variables:\n"
            "    c: {function: len, source: h_li, dtype: int32}\n"
            "output:\n"
            "  grid: {type: healpix, parent_order: 6, child_order: 12}\n"
        )
        b = yaml.safe_load(
            "# a comment\n"
            "output:\n"
            "  grid:\n"
            "    child_order:   12\n"
            "    parent_order:  6\n"
            "    type: healpix   # trailing comment\n"
            "aggregation:\n"
            "  variables:\n"
            "    c:\n"
            "      dtype: int32\n"
            "      source: h_li\n"
            "      function: len\n"
            "data_source:\n"
            "  coordinates: {longitude: /lon, latitude: /lat}\n"
            "  variables: {h_li: /p}\n"
            "  groups: [gt1l]\n"
            "  reader: h5coro\n"
        )
        ca = PipelineConfig(**a)
        cb = PipelineConfig(**b)
        assert canonical_semantic_json(ca) == canonical_semantic_json(cb)
        assert semantic_hash(ca) == semantic_hash(cb)

    def test_packaging_knobs_never_change_hash(self):
        base = semantic_hash(_cfg())
        packaging = [
            _cfg(output__grid__parent_order=9),
            _cfg(output__grid__child_order=19),
            _cfg(output__grid__chunk_inner=11),
            _cfg(output__grid__sharded=False),
            _cfg(output__store_layout="hive"),
            _cfg(output__store="s3://elsewhere/prefix"),
            _cfg(aggregation__handoff="pandas"),
            _cfg(data_source__reader="xarray"),
            _cfg(data_source__driver="https"),
        ]
        for cfg in packaging:
            assert semantic_hash(cfg) == base
        # Worker sizing (issue #235) and read knobs are packaging too.
        cfg = _cfg()
        cfg.worker = {"memory": 8192, "extra_disk": True}
        assert semantic_hash(cfg) == base

    def test_semantic_edits_always_change_hash(self):
        base = semantic_hash(_cfg())
        cfg = _cfg()
        cfg.aggregation["variables"]["h_min"]["function"] = "max"
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.aggregation["variables"]["count"]["dtype"] = "int64"
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.aggregation["variables"]["extra"] = {
            "function": "len",
            "source": "h_li",
            "dtype": "int32",
        }
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["groups"] = ["gt1l"]
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["quality_filter"]["value"] = 1
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["variables"]["h_li"] = "/other/path"
        assert semantic_hash(cfg) != base

    def test_grid_family_is_semantic(self):
        # Grid TYPE (+ indexing scheme) is identity; the orders are not (D24).
        healpix = semantic_core(_cfg())
        assert healpix["grid"] == {"type": "healpix", "indexing_scheme": "nested"}
        rect = default_config("atl06_polar")
        rect = copy.deepcopy(rect)
        rect.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        assert semantic_core(rect)["grid"] == {"type": "rectilinear"}
        assert semantic_hash(rect) != semantic_hash(_cfg())

    def test_null_packaging_values_drop_out(self):
        # A present-but-null read knob (YAML `driver:`) is identical to an
        # absent one — the canonical core omits nulls.
        a, b = _cfg(), _cfg()
        b.data_source["read_plan"] = None
        assert semantic_hash(a) == semantic_hash(b)

    def test_fingerprint_rejects_short_input(self):
        with pytest.raises(ValueError, match="not a semantic hash"):
            semantic_fingerprint("abc")
