"""Tests for zagg.catalog helpers (cycle dates, polygon/bbox, basins).

Fetch is tested via zagg.catalog.sources; shard-map building via
zagg.catalog.shardmap.
"""

import json
from datetime import datetime

import numpy as np
import pytest

from zagg.catalog import (
    cycle_to_dates,
    load_antarctic_basins,
    load_polygon,
    polygon_to_bbox,
)


class TestCycleToDates:
    def test_cycle_1(self):
        start, end = cycle_to_dates(1)
        assert start == datetime(2018, 10, 13)
        assert (end - start).days == 91

    def test_cycle_22(self):
        start, _ = cycle_to_dates(22)
        assert start.year == 2024
        assert start.month == 1

    def test_monotonic(self):
        s1, _ = cycle_to_dates(1)
        s2, _ = cycle_to_dates(2)
        s22, _ = cycle_to_dates(22)
        assert s1 < s2 < s22


class TestLoadPolygon:
    def _write(self, geojson, path):
        with open(path, "w") as f:
            json.dump(geojson, f)

    def test_bare_polygon(self, tmp_path):
        geojson = {"type": "Polygon",
                   "coordinates": [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]]}
        path = tmp_path / "poly.geojson"
        self._write(geojson, path)
        parts = load_polygon(str(path))
        assert len(parts) == 1
        lats, _ = parts[0]
        assert lats.min() == pytest.approx(-80)
        assert lats.max() == pytest.approx(-70)

    def test_feature(self, tmp_path):
        geojson = {"type": "Feature", "properties": {},
                   "geometry": {"type": "Polygon",
                                "coordinates": [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]]}}
        path = tmp_path / "feat.geojson"
        self._write(geojson, path)
        assert len(load_polygon(str(path))) == 1

    def test_feature_collection(self, tmp_path):
        def feat(x):
            ring = [[x, -70], [x + 10, -70], [x + 10, -80], [x, -80], [x, -70]]
            return {"type": "Feature", "properties": {},
                    "geometry": {"type": "Polygon", "coordinates": [ring]}}
        geojson = {"type": "FeatureCollection", "features": [feat(0), feat(20)]}
        path = tmp_path / "fc.geojson"
        self._write(geojson, path)
        assert len(load_polygon(str(path))) == 2

    def test_multipolygon(self, tmp_path):
        geojson = {"type": "MultiPolygon", "coordinates": [
            [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]],
            [[[20, -70], [30, -70], [30, -80], [20, -80], [20, -70]]]]}
        path = tmp_path / "multi.geojson"
        self._write(geojson, path)
        assert len(load_polygon(str(path))) == 2

    def test_lat_lon_order(self, tmp_path):
        geojson = {"type": "Polygon",
                   "coordinates": [[[100, -65], [110, -65], [110, -75], [100, -75], [100, -65]]]}
        path = tmp_path / "order.geojson"
        self._write(geojson, path)
        lats, lons = load_polygon(str(path))[0]
        assert lons.min() == pytest.approx(100)
        assert lats.min() == pytest.approx(-75)


class TestPolygonToBbox:
    def test_single_part(self):
        parts = [(np.array([-70, -80, -75]), np.array([0, 10, 5]))]
        assert polygon_to_bbox(parts) == (0.0, -80.0, 10.0, -70.0)

    def test_multi_part(self):
        parts = [(np.array([-70, -80]), np.array([0, 10])),
                 (np.array([-60, -90]), np.array([20, 30]))]
        assert polygon_to_bbox(parts) == (0.0, -90.0, 30.0, -60.0)


class TestLoadAntarcticBasins:
    def test_loads_basins(self):
        basins = load_antarctic_basins()
        assert len(basins) >= 20

    def test_basin_shape(self):
        lats, lons = load_antarctic_basins()[0]
        assert len(lats) == len(lons) >= 3

    def test_southern_hemisphere(self):
        for lats, _ in load_antarctic_basins():
            assert lats.max() < 0
