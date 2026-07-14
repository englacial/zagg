"""Tests for the metadata fetch layer (``zagg.catalog.sources``).

Covers the generic ``STACSource`` (issue #218), the shared ``rel=next`` pager
(CMR GET flow + POST/merge flow), asset subsetting, and the multi-asset /
datetime extensions to ``granule_records()`` and ShardMap granule entries.
No network: ``requests`` is replaced with a scripted fake.
"""

import numpy as np
import pyarrow as pa
import pytest
import stac_geoparquet.arrow as sga

from zagg.catalog import sources
from zagg.catalog.shardmap import ShardMap, _granule_entry
from zagg.catalog.sources import (
    Catalog,
    STACQuery,
    STACSource,
    _page_search,
    _subset_assets,
)

BBOX = (-76.62, 38.84, -76.50, 38.94)


def _ring():
    return [[-76.6, 38.85], [-76.5, 38.85], [-76.5, 38.93], [-76.6, 38.93], [-76.6, 38.85]]


def _item(gid, assets, dt="2026-07-13T16:02:23Z"):
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [_ring()]},
        "bbox": [-76.6, 38.85, -76.5, 38.93],
        "properties": {"datetime": dt},
        "collection": "sentinel-2-c1-l2a",
        "stac_extensions": [],
        "links": [],
        "assets": assets,
    }


def _s2_assets(gid):
    base = f"https://cogs.example/{gid}"
    return {
        "red": {"href": f"{base}/B04.tif", "roles": ["data"]},
        "nir": {"href": f"{base}/B08.tif", "roles": ["data"]},
        "scl": {"href": f"{base}/SCL.tif", "roles": ["data"]},
        "thumbnail": {"href": f"{base}/thumb.jpg", "roles": ["thumbnail"]},
    }


def _h5_assets(gid):
    return {
        "data": {"href": f"https://h/{gid}.h5", "roles": ["data"]},
        "data_s3": {"href": f"s3://b/{gid}.h5", "roles": ["data"]},
    }


def _page(items, next_link=None):
    doc = {"type": "FeatureCollection", "features": items, "links": []}
    if next_link:
        doc["links"].append({"rel": "next", **next_link})
    return doc


class _FakeResponse:
    def __init__(self, doc):
        self._doc = doc

    def json(self):
        return self._doc

    def raise_for_status(self):
        pass


class _FakeRequests:
    """Scripted page sequence; records every call."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url, json))
        return _FakeResponse(self.pages.pop(0))

    def get(self, url, params=None, timeout=None):
        self.calls.append(("GET", url, params))
        return _FakeResponse(self.pages.pop(0))


@pytest.fixture
def fake_requests(monkeypatch):
    def install(pages):
        fake = _FakeRequests(pages)
        monkeypatch.setattr(sources, "requests", fake)
        return fake

    return install


class TestPageSearch:
    def test_get_flow_follows_get_next(self, fake_requests):
        fake = fake_requests(
            [
                _page([_item("a", _h5_assets("a"))], {"href": "https://cmr/next2"}),
                _page([_item("b", _h5_assets("b"))]),
            ]
        )
        items = _page_search("https://cmr/search", params={"limit": 1})
        assert [it["id"] for it in items] == ["a", "b"]
        assert fake.calls[0] == ("GET", "https://cmr/search", {"limit": 1})
        assert fake.calls[1] == ("GET", "https://cmr/next2", None)

    def test_get_flow_switches_to_post_next(self, fake_requests):
        fake = fake_requests(
            [
                _page(
                    [_item("a", _h5_assets("a"))],
                    {"href": "https://cmr/search", "method": "POST", "body": {"page": 2}},
                ),
                _page([_item("b", _h5_assets("b"))]),
            ]
        )
        items = _page_search("https://cmr/search", params={"limit": 1})
        assert len(items) == 2
        assert fake.calls[1] == ("POST", "https://cmr/search", {"page": 2})

    def test_post_flow_merges_next_body(self, fake_requests):
        fake = fake_requests(
            [
                _page(
                    [_item("a", _s2_assets("a"))],
                    {"method": "POST", "body": {"next": "tok"}, "merge": True},
                ),
                _page([_item("b", _s2_assets("b"))]),
            ]
        )
        body = {"collections": ["sentinel-2-c1-l2a"], "limit": 1}
        items = _page_search("https://es/search", body=body)
        assert len(items) == 2
        merged = fake.calls[1][2]
        assert merged["next"] == "tok"
        assert merged["collections"] == ["sentinel-2-c1-l2a"]

    def test_post_flow_next_without_method_stays_post(self, fake_requests):
        fake = fake_requests(
            [
                _page([_item("a", _s2_assets("a"))], {"body": {"next": "tok"}}),
                _page([], None),
            ]
        )
        _page_search("https://es/search", body={"limit": 1})
        assert fake.calls[1][0] == "POST"

    def test_stops_on_empty_features(self, fake_requests):
        fake = fake_requests(
            [
                _page([], {"href": "https://cmr/never"}),
            ]
        )
        assert _page_search("https://cmr/search", params={}) == []
        assert len(fake.calls) == 1


class TestSTACSource:
    def test_fetch_builds_catalog(self, fake_requests):
        fake = fake_requests([_page([_item("s2a", _s2_assets("s2a"))])])
        src = STACSource("https://es/v1/")
        query = STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        cat = src.fetch(query, limit=500)
        assert len(cat) == 1
        assert cat.metadata["source"] == "STAC"
        assert cat.metadata["root"] == "https://es/v1"
        assert cat.metadata["collections"] == ["sentinel-2-c1-l2a"]
        method, url, body = fake.calls[0]
        assert (method, url) == ("POST", "https://es/v1/search")
        assert body["collections"] == ["sentinel-2-c1-l2a"]
        assert body["bbox"] == list(BBOX)
        assert body["limit"] == 500
        assert "query" not in body

    def test_cloud_cover_filter_in_body(self, fake_requests):
        fake = fake_requests([_page([_item("s2a", _s2_assets("s2a"))])])
        query = STACQuery(
            ["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX, max_cloud_cover=20.0
        )
        STACSource("https://es/v1").fetch(query)
        assert fake.calls[0][2]["query"] == {"eo:cloud_cover": {"lt": 20.0}}

    def test_asset_keep_list_subsets(self, fake_requests):
        fake_requests([_page([_item("s2a", _s2_assets("s2a"))])])
        query = STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        cat = STACSource("https://es/v1", assets=["red", "nir"]).fetch(query)
        rec = cat.granule_records()[0]
        assert set(rec["assets"]) == {"red", "nir"}

    def test_empty_result_raises(self, fake_requests):
        fake_requests([_page([])])
        query = STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        with pytest.raises(ValueError, match="No items"):
            STACSource("https://es/v1").fetch(query)


class TestSubsetAssets:
    def test_keeps_requested(self):
        out = _subset_assets(_item("a", _s2_assets("a")), ["red", "scl"])
        assert set(out["assets"]) == {"red", "scl"}

    def test_no_match_raises(self):
        with pytest.raises(ValueError, match="none of the requested"):
            _subset_assets(_item("a", _s2_assets("a")), ["swir16"])

    def test_original_item_not_mutated(self):
        item = _item("a", _s2_assets("a"))
        _subset_assets(item, ["red"])
        assert set(item["assets"]) == {"red", "nir", "scl", "thumbnail"}


def _catalog(items, meta=None):
    return Catalog(pa.table(sga.parse_stac_items_to_arrow(items)), meta or {})


class TestGranuleRecordsAssets:
    def test_multi_asset_record(self):
        rec = _catalog([_item("s2a", _s2_assets("s2a"))]).granule_records()[0]
        assert rec["https"] is None and rec["s3"] is None
        assert rec["assets"]["red"] == "https://cogs.example/s2a/B04.tif"
        assert rec["assets"]["scl"] == "https://cogs.example/s2a/SCL.tif"
        assert rec["datetime"].startswith("2026-07-13T16:02:23")

    def test_h5_record_shape_unchanged(self):
        rec = _catalog([_item("g1", _h5_assets("g1"))]).granule_records()[0]
        assert rec["https"] == "https://h/g1.h5"
        assert rec["s3"] == "s3://b/g1.h5"
        assert "assets" not in rec

    def test_geoparquet_round_trip_preserves_assets(self, tmp_path):
        cat = _catalog([_item("s2a", _s2_assets("s2a"))], {"source": "STAC"})
        path = str(tmp_path / "cat.parquet")
        cat.to_geoparquet(path)
        rec = Catalog.from_geoparquet(path).granule_records()[0]
        assert rec["assets"]["nir"] == "https://cogs.example/s2a/B08.tif"
        assert rec["datetime"].startswith("2026-07-13T16:02:23")


class TestGranuleEntry:
    def test_h5_entry_shape_unchanged(self):
        rec = {
            "id": "g",
            "s3": "s3://b/g.h5",
            "https": "https://h/g.h5",
            "lats": np.array([0.0]),
            "lons": np.array([0.0]),
        }
        assert _granule_entry(rec) == {"id": "g", "s3": "s3://b/g.h5", "https": "https://h/g.h5"}

    def test_multi_asset_entry_carries_assets_and_datetime(self):
        rec = {
            "id": "s2a",
            "s3": None,
            "https": None,
            "assets": {"red": "https://c/B04.tif"},
            "datetime": "2026-07-13T16:02:23+00:00",
        }
        entry = _granule_entry(rec)
        assert entry["assets"] == {"red": "https://c/B04.tif"}
        assert entry["datetime"] == "2026-07-13T16:02:23+00:00"

    def test_shardmap_json_round_trip(self, tmp_path):
        entry = {
            "id": "s2a",
            "s3": None,
            "https": None,
            "assets": {"red": "https://c/B04.tif", "scl": "https://c/SCL.tif"},
            "datetime": "2026-07-13T16:02:23+00:00",
        }
        sm = ShardMap({"grid": "healpix"}, [7], [[entry]], {"collection": "sentinel-2-c1-l2a"})
        path = str(tmp_path / "sm.json")
        sm.to_json(path)
        assert ShardMap.from_json(path).granules[0][0] == entry

    def test_shardmap_parquet_round_trip(self, tmp_path):
        entry = {
            "id": "s2a",
            "s3": None,
            "https": None,
            "assets": {"red": "https://c/B04.tif"},
            "datetime": "2026-07-13T16:02:23+00:00",
        }
        sm = ShardMap({"grid": "healpix"}, [7], [[entry]], {})
        path = str(tmp_path / "sm.parquet")
        sm.to_parquet(path)
        assert ShardMap.from_parquet(path).granules[0][0] == entry
