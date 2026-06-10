"""Metadata fetch layer: query a STAC endpoint -> a ``Catalog`` artifact.

This is concern (1) of the #24 split -- *fetch what/when/where*, independent of
any grid. The output is a ``Catalog`` backed by a stac-geoparquet pyarrow table
(STAC Items with intact assets), persistable to a ``.parquet`` file and reusable
across many ShardMap builds at different grids.

``CMRSource`` is the one built-in source; it targets NASA's CMR-STAC endpoint,
which is fully STAC-conformant. Non-CMR sources need no client of their own --
the user exports their own STAC query to stac-geoparquet and loads it via
``Catalog.from_geoparquet``.

Endpoint (S3 vs HTTPS) is **not** chosen here: both ``data`` hrefs are preserved
per granule so the aggregator can pick at run time via ``data_source.driver``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import requests

# stac-geoparquet stores geometry as WKB binary (verified for CMR-STAC items),
# so granule footprints decode with shapely.from_wkb on both fresh and
# round-tripped tables.
_ZAGG_META_KEY = b"zagg:catalog_meta"
_CMR_STAC_ROOT = "https://cmr.earthdata.nasa.gov/stac"


@dataclass
class Query:
    """A spatiotemporal metadata query: *what, when, where*.

    Parameters
    ----------
    short_name : str
        Product short name (e.g. ``"ATL03"``).
    version : str
        Product version (e.g. ``"007"``).
    start_date, end_date : str
        Inclusive date bounds, ``YYYY-MM-DD``.
    region : tuple or str
        Either a ``(lon_min, lat_min, lon_max, lat_max)`` bbox or a path to a
        GeoJSON file (its bounding box is used for the STAC query).
    provider : str
        CMR provider / STAC sub-catalog. Default ``"NSIDC_CPRD"``.
    """

    short_name: str
    version: str
    start_date: str
    end_date: str
    region: tuple | str
    provider: str = "NSIDC_CPRD"

    @property
    def collection(self) -> str:
        """CMR-STAC collection id, ``{short_name}_{version}``."""
        return f"{self.short_name}_{self.version}"


def _resolve_bbox(region) -> tuple[float, float, float, float]:
    """Return a ``(lon_min, lat_min, lon_max, lat_max)`` bbox from a Query region."""
    if isinstance(region, str):
        from zagg.catalog import load_polygon, polygon_to_bbox

        return polygon_to_bbox(load_polygon(region))
    if len(region) != 4:
        raise ValueError("region bbox must be (lon_min, lat_min, lon_max, lat_max)")
    return tuple(float(x) for x in region)


def _normalize_assets(item: dict, *, preserve_thumbnails: bool) -> dict:
    """Collapse CMR's per-granule-keyed assets into canonical keys.

    CMR-STAC names the ``data``-role assets with per-granule-unique keys (the
    full object path), which would explode a geoparquet struct schema. We map
    them to stable keys instead, keeping both endpoints:

    - ``data``     : the HTTPS ``.h5`` data asset,
    - ``data_s3``  : the S3 ``.h5`` data asset,
    - ``metadata`` : the metadata-role asset.

    With ``preserve_thumbnails`` the original ``thumbnail_*``/``browse`` assets
    are kept verbatim (their keys are already stable) for a future
    shardmap-vs-footprint viewer; by default they are dropped.
    """
    out: dict = {}
    for key, asset in item.get("assets", {}).items():
        roles = asset.get("roles") or []
        href = asset.get("href", "")
        if "data" in roles and href.endswith(".h5"):
            out["data" if href.startswith("https") else "data_s3"] = asset
        elif "metadata" in roles:
            out["metadata"] = asset
        elif preserve_thumbnails and ("thumbnail" in roles or "browse" in roles):
            out[key] = asset
    item = dict(item)
    item["assets"] = out
    return item


class CMRSource:
    """Fetch granule metadata from NASA's CMR-STAC endpoint.

    Parameters
    ----------
    provider : str, optional
        Overrides the query provider for the STAC sub-catalog URL.
    timeout : int
        Per-request timeout in seconds.
    """

    def __init__(self, provider: str | None = None, timeout: int = 60):
        self.provider = provider
        self.timeout = timeout

    def fetch(
        self, query: Query, *, preserve_thumbnails: bool = False, limit: int = 2000
    ) -> "Catalog":
        """Run ``query`` against CMR-STAC and return a ``Catalog``.

        Parameters
        ----------
        query : Query
            What/when/where to fetch.
        preserve_thumbnails : bool
            Keep ``thumbnail_*``/``browse`` assets (default drops them).
        limit : int
            Page size hint; CMR clamps it and paging follows ``rel=next``.

        Returns
        -------
        Catalog
        """
        import stac_geoparquet.arrow as sga

        provider = self.provider or query.provider
        bbox = _resolve_bbox(query.region)
        datetime = f"{query.start_date}T00:00:00Z/{query.end_date}T23:59:59Z"

        items = self._search(provider, query.collection, bbox, datetime, limit)
        items = [_normalize_assets(it, preserve_thumbnails=preserve_thumbnails)
                 for it in items]
        if not items:
            raise ValueError(
                f"No granules for {query.collection} over {bbox} in "
                f"{query.start_date}..{query.end_date}"
            )

        table = pa.table(sga.parse_stac_items_to_arrow(items))
        meta = {
            "source": "CMR-STAC",
            "provider": provider,
            "collection": query.collection,
            "short_name": query.short_name,
            "version": query.version,
            "start_date": query.start_date,
            "end_date": query.end_date,
            "bbox": list(bbox),
            "preserve_thumbnails": preserve_thumbnails,
            "total_granules": len(items),
        }
        return Catalog(_attach_meta(table, meta), meta)

    def _search(self, provider, collection, bbox, datetime, limit) -> list[dict]:
        """Page through CMR-STAC item-search, following ``rel=next`` links."""
        url = f"{_CMR_STAC_ROOT}/{provider}/search"
        params: dict | None = {
            "collections": collection,
            "bbox": ",".join(str(x) for x in bbox),
            "datetime": datetime,
            "limit": limit,
        }
        body: dict | None = None
        items: list[dict] = []
        while True:
            if body is not None:
                resp = requests.post(url, json=body, timeout=self.timeout)
            else:
                resp = requests.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            doc = resp.json()
            feats = doc.get("features", [])
            items.extend(feats)
            nxt = next((l for l in doc.get("links", []) if l.get("rel") == "next"), None)
            if not nxt or not feats:
                break
            # A next link is either a GET href or a POST href+body+merge.
            url = nxt["href"]
            if str(nxt.get("method", "GET")).upper() == "POST":
                params, body = None, nxt.get("body", {})
            else:
                params, body = None, None
        return items


def _attach_meta(table: pa.Table, meta: dict) -> pa.Table:
    """Stash zagg catalog metadata in the arrow schema (survives geoparquet I/O)."""
    schema_meta = dict(table.schema.metadata or {})
    schema_meta[_ZAGG_META_KEY] = json.dumps(meta).encode()
    return table.replace_schema_metadata(schema_meta)


@dataclass
class Catalog:
    """Fetched granule metadata: a stac-geoparquet table + provenance.

    Reusable across many ShardMap builds. Endpoint-neutral -- each granule
    carries both its S3 and HTTPS ``.h5`` hrefs.

    Parameters
    ----------
    table : pyarrow.Table
        stac-geoparquet table (one row per granule).
    metadata : dict
        Query provenance (product, version, bbox, dates, ...).
    """

    table: pa.Table
    metadata: dict = field(default_factory=dict)

    def __len__(self) -> int:
        return self.table.num_rows

    def to_geoparquet(self, path: str) -> None:
        """Write the catalog to a stac-geoparquet file.

        ``stac_geoparquet`` rewrites schema metadata with only the GeoParquet
        ``geo`` key, so we reopen and merge zagg provenance back in (keeping
        ``geo`` intact) before the final write.
        """
        import pyarrow.parquet as pq
        import stac_geoparquet.arrow as sga

        sga.to_parquet(self.table, path)
        table = pq.read_table(path)
        schema_meta = dict(table.schema.metadata or {})
        schema_meta[_ZAGG_META_KEY] = json.dumps(self.metadata).encode()
        pq.write_table(table.replace_schema_metadata(schema_meta), path)

    @classmethod
    def from_geoparquet(cls, path: str) -> "Catalog":
        """Load a catalog from a stac-geoparquet file (CMR or user-supplied)."""
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        raw = (table.schema.metadata or {}).get(_ZAGG_META_KEY)
        meta = json.loads(raw) if raw else {}
        return cls(table, meta)

    def granule_records(self) -> list[dict]:
        """Decode the table into per-granule dicts for ShardMap building.

        Returns
        -------
        list of dict
            Each: ``{"id", "s3", "https", "lats", "lons"}`` where ``lats``/
            ``lons`` are the footprint exterior-ring coordinate arrays (WGS84)
            and ``s3``/``https`` are the data-asset hrefs (either may be None).
        """
        import shapely

        ids = self.table.column("id").to_pylist()
        assets = self.table.column("assets").to_pylist()
        geoms = self.table.column("geometry").to_pylist()
        records = []
        for gid, asset_map, wkb in zip(ids, assets, geoms):
            geom = shapely.from_wkb(wkb)
            if geom.is_empty or geom.geom_type not in ("Polygon", "MultiPolygon"):
                continue
            poly = geom if geom.geom_type == "Polygon" else max(geom.geoms, key=lambda g: g.area)
            x, y = poly.exterior.coords.xy
            data = (asset_map or {}).get("data") or {}
            data_s3 = (asset_map or {}).get("data_s3") or {}
            records.append({
                "id": gid,
                "https": data.get("href"),
                "s3": data_s3.get("href"),
                "lats": np.asarray(y),
                "lons": np.asarray(x),
            })
        return records


__all__ = ["Query", "CMRSource", "Catalog"]
