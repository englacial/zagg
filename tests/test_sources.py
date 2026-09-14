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
    def __init__(self, doc, status_code=200):
        self._doc = doc
        self.status_code = status_code

    def json(self):
        return self._doc

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")


class _FakeRequests:
    """Scripted page sequence; records every call.

    Pages are FeatureCollection dicts (wrapped in a 200 ``_FakeResponse``) or
    prebuilt ``_FakeResponse`` objects (for non-200 statuses).
    """

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def _next(self):
        page = self.pages.pop(0)
        return page if isinstance(page, _FakeResponse) else _FakeResponse(page)

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url, json))
        return self._next()

    def get(self, url, params=None, timeout=None):
        self.calls.append(("GET", url, params))
        return self._next()


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
                    {
                        "href": "https://es/search",
                        "method": "POST",
                        "body": {"next": "tok"},
                        "merge": True,
                    },
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
                _page(
                    [_item("a", _s2_assets("a"))],
                    {"href": "https://es/search", "body": {"next": "tok"}},
                ),
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

    def test_retries_transient_gateway_error(self, fake_requests, monkeypatch):
        monkeypatch.setattr(sources.time, "sleep", lambda s: None)
        fake = fake_requests(
            [
                _FakeResponse({}, status_code=502),
                _page([_item("a", _h5_assets("a"))]),
            ]
        )
        items = _page_search("https://es/search", body={"limit": 1})
        assert [it["id"] for it in items] == ["a"]
        assert len(fake.calls) == 2

    def test_exhausted_retries_raise(self, fake_requests, monkeypatch):
        monkeypatch.setattr(sources.time, "sleep", lambda s: None)
        fake = fake_requests([_FakeResponse({}, status_code=502)] * 4)
        with pytest.raises(RuntimeError, match="status 502"):
            _page_search("https://es/search", body={"limit": 1})
        assert len(fake.calls) == 4

    def test_non_retryable_status_raises_immediately(self, fake_requests):
        fake = fake_requests([_FakeResponse({}, status_code=404)])
        with pytest.raises(RuntimeError, match="status 404"):
            _page_search("https://es/search", params={})
        assert len(fake.calls) == 1

    def test_default_page_size_stays_under_gateway_ceiling(self, fake_requests):
        # Earth Search 502s (not clamps) above ~300 items/page; the default
        # must stay below that ceiling.
        fake = fake_requests([_page([_item("s2a", _s2_assets("s2a"))])])
        STACSource("https://es/v1").fetch(
            STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        )
        assert fake.calls[0][2]["limit"] == 250


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

    def test_partial_missing_item_skipped_and_recorded(self, fake_requests):
        # One item missing a requested band is skipped, not fatal; the crawl
        # survives and the skip is recorded in catalog metadata (#218).
        full = _item("a", _s2_assets("a"))
        partial = _item("b", {k: v for k, v in _s2_assets("b").items() if k != "scl"})
        fake_requests([_page([full, partial])])
        query = STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        cat = STACSource("https://es/v1", assets=["red", "nir", "scl"]).fetch(query)
        assert len(cat) == 1
        assert cat.metadata["skipped_items"] == {"count": 1, "examples": ["b"]}
        recs = cat.granule_records()
        assert [r["id"] for r in recs] == ["a"]
        assert set(recs[0]["assets"]) == {"red", "nir", "scl"}

    def test_all_skipped_raises_with_keys(self, fake_requests):
        fake_requests([_page([_item("a", _s2_assets("a"))])])
        query = STACQuery(["sentinel-2-c1-l2a"], "2026-06-01", "2026-07-13", BBOX)
        with pytest.raises(ValueError, match="all requested assets.*available keys"):
            STACSource("https://es/v1", assets=["swir16"]).fetch(query)

    def test_heterogeneous_union_no_corruption(self, fake_requests):
        # A collection union with differing asset key sets (item b has an extra
        # band): with no keep-list both are kept and stac-geoparquet unions the
        # asset struct -- each record must carry only its own assets (#218).
        a = _item("a", _s2_assets("a"))
        b_assets = _s2_assets("b")
        b_assets["rededge"] = {"href": "https://cogs.example/b/B05.tif", "roles": ["data"]}
        fake_requests([_page([a, _item("b", b_assets)])])
        query = STACQuery(
            ["sentinel-2-c1-l2a", "sentinel-2-pre-c1-l2a"], "2026-06-01", "2026-07-13", BBOX
        )
        cat = STACSource("https://es/v1").fetch(query)
        assert len(cat) == 2
        assert "skipped_items" not in cat.metadata
        recs = {r["id"]: r for r in cat.granule_records()}
        assert set(recs["a"]["assets"]) == {"red", "nir", "scl", "thumbnail"}
        assert set(recs["b"]["assets"]) == {"red", "nir", "scl", "thumbnail", "rededge"}


class TestSubsetAssets:
    def test_keeps_requested(self):
        out = _subset_assets(_item("a", _s2_assets("a")), ["red", "scl"])
        assert set(out["assets"]) == {"red", "scl"}

    def test_missing_all_returns_none(self):
        assert _subset_assets(_item("a", _s2_assets("a")), ["swir16"]) is None

    def test_missing_some_returns_none(self):
        # Partial keep set -> None (per-item strict); a partial asset map
        # would break the Phase 2 per-band reads (#218).
        assert _subset_assets(_item("a", _s2_assets("a")), ["red", "swir16"]) is None

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

    def test_preserved_thumbnail_keeps_pre218_shape(self):
        # A CMR record grown a preserved thumbnail asset must not sprout
        # ``assets``/``datetime`` -- it still has a canonical data asset (#218).
        assets = _h5_assets("g1")
        assets["thumbnail_0"] = {"href": "https://h/g1_thumb.jpg", "roles": ["thumbnail"]}
        rec = _catalog([_item("g1", assets)]).granule_records()[0]
        assert set(rec) == {"id", "https", "s3", "lats", "lons"}

    def test_geoparquet_round_trip_preserves_assets(self, tmp_path):
        cat = _catalog([_item("s2a", _s2_assets("s2a"))], {"source": "STAC"})
        path = str(tmp_path / "cat.parquet")
        cat.to_geoparquet(path)
        rec = Catalog.from_geoparquet(path).granule_records()[0]
        assert rec["assets"]["nir"] == "https://cogs.example/s2a/B08.tif"
        assert rec["datetime"].startswith("2026-07-13T16:02:23")

    def test_datetime_numeric_offset_round_trip(self, tmp_path):
        # A numeric UTC offset (not "Z") must survive the parse -> geoparquet
        # -> granule_records path as a UTC-aware ISO string.
        item = _item("s2a", _s2_assets("s2a"), dt="2026-07-13T16:02:23+00:00")
        cat = _catalog([item], {"source": "STAC"})
        path = str(tmp_path / "cat.parquet")
        cat.to_geoparquet(path)
        rec = Catalog.from_geoparquet(path).granule_records()[0]
        assert rec["datetime"] == "2026-07-13T16:02:23+00:00"


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


class TestFilterBbox:
    def _catalog(self):
        import stac_geoparquet.arrow as sga

        far = dict(_item("far", _h5_assets("far")))
        far["geometry"] = {
            "type": "Polygon",
            "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
        }
        far["bbox"] = [10, 10, 11, 11]
        items = [_item("near", _h5_assets("near")), far]
        return Catalog(pa.table(sga.parse_stac_items_to_arrow(items)), {"bbox": [0, 0, 1, 1]})

    def test_single_box(self):
        cat = self._catalog()
        sub = cat.filter_bbox((-77.0, 38.0, -76.0, 39.0))
        assert sub.table.column("id").to_pylist() == ["near"]
        assert sub.metadata == cat.metadata

    def test_multi_box_or(self):
        cat = self._catalog()
        sub = cat.filter_bbox([(-77.0, 38.0, -76.0, 39.0), (9.5, 9.5, 10.5, 10.5)])
        assert sorted(sub.table.column("id").to_pylist()) == ["far", "near"]

    def test_no_overlap_empty(self):
        cat = self._catalog()
        assert len(cat.filter_bbox((50.0, 50.0, 51.0, 51.0))) == 0


class TestFootprintCells:
    """The ``footprint_cells`` convention column (issue #396, phase 3).

    A per-granule morton MOC precomputed once per catalog so every later
    ``ShardMap.build`` is set algebra instead of geometry. The column rides in
    the same geoparquet file as the ``geometry`` it was covered from, so it has
    no way to go stale against it — the tests below pin that the column *is*
    that cover, that its order travels with it, and that a subset carries it.
    """

    def _catalog(self, n=3):
        items = []
        for i in range(n):
            it = dict(_item(f"g{i}", _h5_assets(f"g{i}")))
            lon0 = -76.6 + 0.05 * i
            it["geometry"] = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [lon0, 38.85],
                        [lon0 + 0.04, 38.85],
                        [lon0 + 0.04, 38.93],
                        [lon0, 38.93],
                        [lon0, 38.85],
                    ]
                ],
            }
            it["bbox"] = [lon0, 38.85, lon0 + 0.04, 38.93]
            items.append(it)
        return _catalog(items, {"bbox": list(BBOX)})

    def test_absent_column_reads_as_none(self):
        assert self._catalog().footprint_cells() is None

    def test_column_is_the_scalar_cover_of_each_footprint(self):
        # The pin that makes the column trustworthy: each row's stored MOC is
        # exactly what the one-ring cover returns for the ring
        # ``granule_records`` reads, so a build off the column and a build off
        # the geometry are the same computation.
        import mortie

        cat = self._catalog()
        values, offsets, order = cat.index_footprints(9).footprint_cells()
        assert order == 9
        assert offsets[0] == 0 and offsets[-1] == len(values)
        for i, rec in enumerate(cat.granule_records()):
            scalar = np.asarray(
                mortie.polygons_to_morton_mocs(
                    rec["lats"], rec["lons"], np.array([0, len(rec["lats"])]), order=9
                )[0]
            )
            assert np.array_equal(values[offsets[i] : offsets[i + 1]], scalar)

    def test_non_polygonal_rows_get_an_empty_moc(self):
        # mortie's coverage refuses a Point outright, and ``granule_records``
        # skips such a row; the column must stay one entry per table row so the
        # two still line up (a zero-length run), not raise.
        pt = dict(_item("pt", _h5_assets("pt")))
        pt["geometry"] = {"type": "Point", "coordinates": [-76.55, 38.89]}
        cat = _catalog([_item("a", _h5_assets("a")), pt], {"bbox": list(BBOX)})
        values, offsets, _ = cat.index_footprints(9).footprint_cells()
        assert len(offsets) == 3
        assert offsets[1] > 0 and offsets[2] == offsets[1]
        assert [r["id"] for r in cat.granule_records()] == ["a"]

    def test_null_geometry_raises_rather_than_screening(self):
        # A null WKB is not a screened row: ``granule_records``' row-wise
        # ``geom.is_empty`` raises on it, so the vectorised screen must refuse
        # too rather than hand the fast path a quietly shorter alignment than
        # the geometry path would build (issue #439).
        cat = _catalog([_item("a", _h5_assets("a")), _item("b", _h5_assets("b"))])
        field = cat.table.schema.field("geometry")
        wkb = cat.table.column("geometry").to_pylist()
        wkb[1] = None
        table = cat.table.set_column(
            cat.table.column_names.index("geometry"), field, pa.array(wkb, field.type)
        )
        nulled = Catalog(table, dict(cat.metadata or {}))
        with pytest.raises(ValueError, match="1 row\\(s\\) with a null geometry .*first at row 1"):
            nulled.granule_row_mask()
        with pytest.raises(ValueError, match="null geometry"):
            nulled.index_footprints(9)
        with pytest.raises(AttributeError):
            nulled.granule_records()

    def test_all_rows_screened_yields_an_empty_column(self):
        pt = dict(_item("pt", _h5_assets("pt")))
        pt["geometry"] = {"type": "Point", "coordinates": [-76.55, 38.89]}
        values, offsets, _ = _catalog([pt]).index_footprints(9).footprint_cells()
        assert values.size == 0 and offsets.tolist() == [0, 0]

    def test_reindex_replaces_rather_than_appends(self):
        cat = self._catalog().index_footprints(6).index_footprints(8)
        assert cat.table.column_names.count(sources.FOOTPRINT_CELLS) == 1
        assert cat.footprint_cells()[2] == 8
        assert cat.metadata[sources.FOOTPRINT_CELLS_ORDER] == 8

    def test_geoparquet_round_trip_keeps_column_and_order(self, tmp_path):
        cat = self._catalog().index_footprints(9)
        path = str(tmp_path / "indexed.parquet")
        cat.to_geoparquet(path)
        back = Catalog.from_geoparquet(path)
        assert back.metadata[sources.FOOTPRINT_CELLS_ORDER] == 9
        v0, o0, _ = cat.footprint_cells()
        v1, o1, _ = back.footprint_cells()
        assert np.array_equal(v0, v1) and np.array_equal(o0, o1)

    def test_column_is_morton_typed_on_disk(self, tmp_path):
        # Same typed-morton convention the ShardMap parquet manifest uses
        # (docs/morton_arrow.md): the words are a mortie extension type, not
        # anonymous u64, so an Arrow-aware reader sees what they are.
        import pyarrow.parquet as pq

        path = str(tmp_path / "indexed.parquet")
        self._catalog().index_footprints(6).to_geoparquet(path)
        field = pq.read_table(path).schema.field(sources.FOOTPRINT_CELLS)
        assert isinstance(field.type, pa.LargeListType)
        assert field.type.value_type.extension_name == "mortie.morton_index"

    def test_filter_bbox_carries_the_column_aligned(self):
        cat = self._catalog().index_footprints(9)
        values, offsets, _ = cat.footprint_cells()
        kept = cat.filter_bbox((-76.61, 38.8, -76.57, 39.0))
        assert kept.table.column("id").to_pylist() == ["g0"]
        k_values, k_offsets, k_order = kept.footprint_cells()
        assert k_order == 9
        assert np.array_equal(
            k_values[k_offsets[0] : k_offsets[1]], values[offsets[0] : offsets[1]]
        )
