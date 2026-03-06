"""Tests for magg.catalog — CMR query, polygon loading, cell discovery, catalog building."""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from magg.catalog import (
    build_catalog,
    cycle_to_dates,
    discover_cells,
    extract_granule_info,
    load_antarctic_basins,
    load_polygon,
    polygon_to_bbox,
    query_cmr,
)

# ---------------------------------------------------------------------------
# cycle_to_dates
# ---------------------------------------------------------------------------


class TestCycleToDates:
    def test_cycle_1(self):
        start, end = cycle_to_dates(1)
        assert start == datetime(2018, 10, 13)
        assert (end - start).days == 91

    def test_cycle_22(self):
        start, end = cycle_to_dates(22)
        assert start.year == 2024
        assert start.month == 1

    def test_monotonic(self):
        """Later cycles have later dates."""
        s1, _ = cycle_to_dates(1)
        s2, _ = cycle_to_dates(2)
        s22, _ = cycle_to_dates(22)
        assert s1 < s2 < s22


# ---------------------------------------------------------------------------
# load_polygon
# ---------------------------------------------------------------------------


class TestLoadPolygon:
    def _write_geojson(self, geojson, path):
        with open(path, "w") as f:
            json.dump(geojson, f)

    def test_bare_polygon(self, tmp_path):
        geojson = {
            "type": "Polygon",
            "coordinates": [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]],
        }
        path = tmp_path / "poly.geojson"
        self._write_geojson(geojson, path)
        parts = load_polygon(str(path))
        assert len(parts) == 1
        lats, lons = parts[0]
        assert lats.min() == pytest.approx(-80)
        assert lats.max() == pytest.approx(-70)

    def test_feature(self, tmp_path):
        geojson = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]],
            },
            "properties": {},
        }
        path = tmp_path / "feat.geojson"
        self._write_geojson(geojson, path)
        parts = load_polygon(str(path))
        assert len(parts) == 1

    def test_feature_collection(self, tmp_path):
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]
                        ],
                    },
                    "properties": {},
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[20, -70], [30, -70], [30, -80], [20, -80], [20, -70]]
                        ],
                    },
                    "properties": {},
                },
            ],
        }
        path = tmp_path / "fc.geojson"
        self._write_geojson(geojson, path)
        parts = load_polygon(str(path))
        assert len(parts) == 2

    def test_multipolygon(self, tmp_path):
        geojson = {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, -70], [10, -70], [10, -80], [0, -80], [0, -70]]],
                [[[20, -70], [30, -70], [30, -80], [20, -80], [20, -70]]],
            ],
        }
        path = tmp_path / "multi.geojson"
        self._write_geojson(geojson, path)
        parts = load_polygon(str(path))
        assert len(parts) == 2

    def test_lat_lon_order(self, tmp_path):
        """GeoJSON is (lon, lat); our output should be (lats, lons)."""
        geojson = {
            "type": "Polygon",
            "coordinates": [[[100, -65], [110, -65], [110, -75], [100, -75], [100, -65]]],
        }
        path = tmp_path / "order.geojson"
        self._write_geojson(geojson, path)
        parts = load_polygon(str(path))
        lats, lons = parts[0]
        assert lons.min() == pytest.approx(100)
        assert lats.min() == pytest.approx(-75)


# ---------------------------------------------------------------------------
# polygon_to_bbox
# ---------------------------------------------------------------------------


class TestPolygonToBbox:
    def test_single_part(self):
        parts = [(np.array([-70, -80, -75]), np.array([0, 10, 5]))]
        bbox = polygon_to_bbox(parts)
        assert bbox == (0.0, -80.0, 10.0, -70.0)

    def test_multi_part(self):
        parts = [
            (np.array([-70, -80]), np.array([0, 10])),
            (np.array([-60, -90]), np.array([20, 30])),
        ]
        bbox = polygon_to_bbox(parts)
        assert bbox == (0.0, -90.0, 30.0, -60.0)


# ---------------------------------------------------------------------------
# load_antarctic_basins
# ---------------------------------------------------------------------------


class TestLoadAntarcticBasins:
    def test_loads_basins(self):
        basins = load_antarctic_basins()
        assert len(basins) > 0
        # Should be roughly 27 Antarctic drainage basins
        assert len(basins) >= 20

    def test_basin_shape(self):
        basins = load_antarctic_basins()
        lats, lons = basins[0]
        assert len(lats) == len(lons)
        assert len(lats) >= 3

    def test_basins_in_southern_hemisphere(self):
        basins = load_antarctic_basins()
        for lats, _ in basins:
            assert lats.max() < 0, "All basin vertices should be in southern hemisphere"


# ---------------------------------------------------------------------------
# discover_cells
# ---------------------------------------------------------------------------


class TestDiscoverCells:
    def test_default_antarctic(self):
        cells = discover_cells(parent_order=6)
        assert len(cells) > 1000
        assert len(cells) < 2000

    def test_custom_polygon(self):
        # Small triangle in Antarctica
        parts = [
            (np.array([-70, -80, -75]), np.array([0, 10, 5])),
        ]
        cells = discover_cells(parent_order=6, polygon_parts=parts)
        assert len(cells) > 0
        assert len(cells) < 100  # small polygon, few cells

    def test_sorted_unique(self):
        cells = discover_cells(parent_order=6)
        assert np.all(np.diff(cells) > 0), "Cells should be sorted and unique"

    def test_higher_order_more_cells(self):
        cells_6 = discover_cells(
            parent_order=6,
            polygon_parts=[(np.array([-70, -80, -75]), np.array([0, 10, 5]))],
        )
        cells_7 = discover_cells(
            parent_order=7,
            polygon_parts=[(np.array([-70, -80, -75]), np.array([0, 10, 5]))],
        )
        assert len(cells_7) > len(cells_6)


# ---------------------------------------------------------------------------
# extract_granule_info
# ---------------------------------------------------------------------------


class TestExtractGranuleInfo:
    def _make_granule(self, s3_url="s3://bucket/file.h5", points=None):
        if points is None:
            points = [
                {"Latitude": -70, "Longitude": 0},
                {"Latitude": -80, "Longitude": 10},
                {"Latitude": -75, "Longitude": 5},
            ]
        return {
            "umm": {
                "GranuleUR": "ATL06_20240106_test",
                "RelatedUrls": [{"URL": s3_url}],
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "GPolygons": [{"Boundary": {"Points": points}}]
                        }
                    }
                },
            }
        }

    def test_extracts_s3_url(self):
        info = extract_granule_info(self._make_granule())
        assert info["s3_url"] == "s3://bucket/file.h5"

    def test_extracts_points(self):
        info = extract_granule_info(self._make_granule())
        assert len(info["points"]) == 3
        assert info["points"][0] == (-70, 0)

    def test_no_s3_url(self):
        g = self._make_granule(s3_url="https://example.com/file.h5")
        info = extract_granule_info(g)
        assert info["s3_url"] is None

    def test_empty_granule(self):
        info = extract_granule_info({})
        assert info["s3_url"] is None
        assert info["points"] == []


# ---------------------------------------------------------------------------
# query_cmr (mocked)
# ---------------------------------------------------------------------------


class TestQueryCmr:
    def _mock_response(self, items, total_hits=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"CMR-Hits": str(total_hits or len(items))}
        resp.json.return_value = {"items": items}
        resp.raise_for_status = MagicMock()
        return resp

    @patch("magg.catalog.requests.get")
    def test_basic_query(self, mock_get):
        mock_get.return_value = self._mock_response([{"id": "1"}, {"id": "2"}])
        result = query_cmr("2024-01-01", "2024-04-01")
        assert len(result) == 2
        # Verify CMR was called with correct params
        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["short_name"] == "ATL06"
        assert "2024-01-01" in params["temporal"]
        assert "2024-04-01" in params["temporal"]

    @patch("magg.catalog.requests.get")
    def test_custom_short_name(self, mock_get):
        mock_get.return_value = self._mock_response([])
        query_cmr("2024-01-01", "2024-04-01", short_name="ATL08")
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args[1]["params"]
        assert params["short_name"] == "ATL08"

    @patch("magg.catalog.requests.get")
    def test_bbox_passed(self, mock_get):
        mock_get.return_value = self._mock_response([])
        query_cmr("2024-01-01", "2024-04-01", bbox=(-180, -90, 180, -60))
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args[1]["params"]
        assert params["bounding_box"] == "-180,-90,180,-60"

    @patch("magg.catalog.requests.get")
    def test_no_bbox(self, mock_get):
        mock_get.return_value = self._mock_response([])
        query_cmr("2024-01-01", "2024-04-01")
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args[1]["params"]
        assert "bounding_box" not in params

    @patch("magg.catalog.requests.get")
    def test_pagination(self, mock_get):
        page1 = self._mock_response([{"id": str(i)} for i in range(2000)], total_hits=3000)
        page2 = self._mock_response([{"id": str(i)} for i in range(1000)], total_hits=3000)
        mock_get.side_effect = [page1, page2]
        result = query_cmr("2024-01-01", "2024-04-01")
        assert len(result) == 3000
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# build_catalog
# ---------------------------------------------------------------------------


class TestBuildCatalog:
    def _make_granules(self, n=5):
        """Create mock CMR granules with polygons in Antarctica."""
        granules = []
        for i in range(n):
            lon_base = i * 20
            granules.append(
                {
                    "umm": {
                        "GranuleUR": f"ATL06_{i:04d}",
                        "RelatedUrls": [{"URL": f"s3://bucket/granule_{i}.h5"}],
                        "SpatialExtent": {
                            "HorizontalSpatialDomain": {
                                "Geometry": {
                                    "GPolygons": [
                                        {
                                            "Boundary": {
                                                "Points": [
                                                    {
                                                        "Latitude": -70,
                                                        "Longitude": lon_base,
                                                    },
                                                    {
                                                        "Latitude": -70,
                                                        "Longitude": lon_base + 15,
                                                    },
                                                    {
                                                        "Latitude": -85,
                                                        "Longitude": lon_base + 15,
                                                    },
                                                    {
                                                        "Latitude": -85,
                                                        "Longitude": lon_base,
                                                    },
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                    }
                }
            )
        return granules

    def test_returns_catalog_and_timings(self):
        granules = self._make_granules()
        polygon_parts = [(np.array([-70, -85, -75]), np.array([0, 50, 25]))]
        catalog, timings = build_catalog(granules, parent_order=6, polygon_parts=polygon_parts)
        assert isinstance(catalog, dict)
        assert isinstance(timings, dict)
        assert "total" in timings

    def test_catalog_has_urls(self):
        granules = self._make_granules()
        polygon_parts = [(np.array([-70, -85, -75]), np.array([0, 50, 25]))]
        catalog, _ = build_catalog(granules, parent_order=6, polygon_parts=polygon_parts)
        if catalog:
            first_urls = next(iter(catalog.values()))
            assert all(url.startswith("s3://") for url in first_urls)

    def test_empty_granules(self):
        catalog, timings = build_catalog(
            [],
            parent_order=6,
            polygon_parts=[(np.array([-70, -85, -75]), np.array([0, 50, 25]))],
        )
        assert len(catalog) == 0

    def test_default_antarctic_basins(self):
        """When no polygon_parts given, should use Antarctic basins."""
        granules = self._make_granules(3)
        catalog, _ = build_catalog(granules, parent_order=6)
        # Should work without error; may or may not find matches
        assert isinstance(catalog, dict)
