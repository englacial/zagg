"""Packaged demo data (zagg.data): the sklearn-style AOI accessors."""

from pathlib import Path

import pytest

from zagg.catalog import load_polygon
from zagg.data import demo_aoi


class TestDemoAoi:
    @pytest.mark.parametrize(
        ("name", "n_parts"),
        [("serc", 1), ("neon_aop", 61), ("california", 8)],
    )
    def test_packaged_aoi_loads(self, name, n_parts):
        path = demo_aoi(name)
        assert Path(path).is_file()
        assert len(load_polygon(path)) == n_parts

    def test_unknown_name_raises(self):
        with pytest.raises(KeyError, match="california"):
            demo_aoi("nowhere")


class TestDemoHiveConfig:
    def test_matches_benchmark_hive_grid(self):
        # The packaged demo template must stay spatially identical to the
        # benchmark hive arm: a shardmap built from one pairs with the other.
        from zagg.config import default_config, load_config
        from zagg.grids import from_config

        packaged = default_config("atl03_tdigest_healpix_hive")
        bench = load_config("tests/data/benchmark/configs/atl03_tdigest_healpix_o9_hive.yaml")
        assert from_config(packaged).spatial_signature() == from_config(bench).spatial_signature()
        assert packaged.output["store_layout"] == "hive"
        # The ladder is hash- and shardmap-invisible (issue #620): the spatial
        # signature above is what pairs the two; the ladder itself is pinned
        # in tests/test_live_store_templates.py.
        assert packaged.output["pyramid"] == {"overviews": [16, 15, 14, 13]}
