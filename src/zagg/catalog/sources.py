"""Metadata fetch layer: query a STAC endpoint -> a ``Catalog`` artifact.

This is concern (1) of the #24 split -- *fetch what/when/where*, independent of
any grid. The output is a ``Catalog`` backed by a stac-geoparquet pyarrow table
(STAC Items with intact assets), persistable to a ``.parquet`` file and reusable
across many ShardMap builds at different grids.

Two built-in sources:

- ``CMRSource`` targets NASA's CMR-STAC endpoint (per-granule-unique asset keys,
  single ``.h5`` data asset -- normalized to canonical ``data``/``data_s3``).
- ``STACSource`` targets any STAC API root (issue #218), e.g. Earth Search for
  Sentinel-2. Generic APIs use stable per-collection asset keys (``red``,
  ``nir``, ``scl``), so assets are kept under their own keys, optionally
  subset via the ``assets`` keep-list.

Other sources still need no client of their own -- the user exports their own
STAC query to stac-geoparquet and loads it via ``Catalog.from_geoparquet``.

Endpoint (S3 vs HTTPS) is **not** chosen here: both ``data`` hrefs are preserved
per granule so the aggregator can pick at run time via ``data_source.driver``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import requests

# stac-geoparquet stores geometry as WKB binary (verified for CMR-STAC items),
# so granule footprints decode with shapely.from_wkb on both fresh and
# round-tripped tables.
_ZAGG_META_KEY = b"zagg:catalog_meta"
_CMR_STAC_ROOT = "https://cmr.earthdata.nasa.gov/stac"

#: Column name of the per-granule morton MOC footprint index (issue #396).
#: A **zagg-clone convention column**, not a stac-geoparquet upstream field: an
#: indexed catalog is still a valid stac-geoparquet file that any other reader
#: ignores. It cannot go stale against the geometry it describes, because it
#: rides in the same file as that geometry -- there is no second artifact to
#: re-sync, and a catalog subset (``filter_bbox``) takes the column along.
FOOTPRINT_CELLS = "footprint_cells"

#: Catalog-metadata key recording the HEALPix order the column was covered at.
#: The order is the column's whole contract: a MOC answers any shard order
#: **coarser than or equal to** it (``moc_to_order`` coarsens), and nothing
#: finer (that would refine each cell onto all its descendants -- issue #92).
FOOTPRINT_CELLS_ORDER = "footprint_cells_order"


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


@dataclass
class STACQuery:
    """A generic STAC item-search query: *what, when, where* (issue #218).

    Parameters
    ----------
    collections : list of str
        Collection ids to search, e.g. ``["sentinel-2-c1-l2a",
        "sentinel-2-pre-c1-l2a"]`` (query both for a gap-free S2 archive).
    start_date, end_date : str
        Inclusive date bounds, ``YYYY-MM-DD``.
    region : tuple or str
        Either a ``(lon_min, lat_min, lon_max, lat_max)`` bbox or a path to a
        GeoJSON file (its bounding box is used for the STAC query).
    max_cloud_cover : float, optional
        Keep only items with ``eo:cloud_cover`` strictly below this value
        (STAC query extension).
    """

    collections: list[str]
    start_date: str
    end_date: str
    region: tuple | str
    max_cloud_cover: float | None = None


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


def _subset_assets(item: dict, keep: list[str]) -> dict | None:
    """Keep only the ``keep`` asset keys (stable per-collection keys, #218).

    Per-item strict: returns the subset item only when it carries *every*
    requested key, else ``None`` so the caller can skip it. A partial asset
    map would break the Phase 2 reader's per-band reads, surfacing much later
    as an unreadable granule.
    """
    have = item.get("assets", {})
    if set(keep) - set(have):
        return None
    item = dict(item)
    item["assets"] = {k: have[k] for k in keep}
    return item


#: Transient upstream statuses worth retrying: public STAC gateways (Earth
#: Search, CMR-STAC) 502/503/504 sporadically under load, and one bad response
#: must not lose a whole paged crawl.
_RETRY_STATUSES = (502, 503, 504)
_RETRY_ATTEMPTS = 4
_RETRY_BACKOFF_S = 2.0


def _search_request(url, *, params=None, body=None, timeout=60):
    """One item-search request, retrying transient gateway errors.

    Retries ``_RETRY_STATUSES`` with exponential backoff (``_RETRY_ATTEMPTS``
    tries total); any other status falls through to ``raise_for_status`` on
    the first response, an exhausted retry budget on the last.
    """
    for attempt in range(_RETRY_ATTEMPTS):
        if body is not None:
            resp = requests.post(url, json=body, timeout=timeout)
        else:
            resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code not in _RETRY_STATUSES or attempt == _RETRY_ATTEMPTS - 1:
            break
        wait = _RETRY_BACKOFF_S * 2**attempt
        logging.warning(
            "STAC search got %s from %s; retrying in %.0fs (%d/%d)",
            resp.status_code,
            url,
            wait,
            attempt + 1,
            _RETRY_ATTEMPTS - 1,
        )
        time.sleep(wait)
    resp.raise_for_status()
    return resp


def _page_search(url, *, params=None, body=None, timeout=60) -> list[dict]:
    """Page a STAC item-search, following ``rel=next`` links.

    Starts as GET with ``params`` unless ``body`` is given (POST). A next link
    is either a GET href or a POST href+body; per the STAC API spec ``merge``
    folds the link body into the previous request body. When a link omits
    ``method``, the current mode is kept.
    """
    items: list[dict] = []
    while True:
        resp = _search_request(url, params=params, body=body, timeout=timeout)
        doc = resp.json()
        feats = doc.get("features", [])
        items.extend(feats)
        nxt = next((ln for ln in doc.get("links", []) if ln.get("rel") == "next"), None)
        if not nxt or not feats:
            break
        # Require the href -- a next link without one raises KeyError loudly
        # rather than re-requesting the current url forever (#218).
        url = nxt["href"]
        mode = "GET" if body is None else "POST"
        if str(nxt.get("method", mode)).upper() == "POST":
            nxt_body = nxt.get("body", {})
            body = {**body, **nxt_body} if (nxt.get("merge") and body) else nxt_body
            params = None
        else:
            params, body = None, None
    return items


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
        items = [_normalize_assets(it, preserve_thumbnails=preserve_thumbnails) for it in items]
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
        params = {
            "collections": collection,
            "bbox": ",".join(str(x) for x in bbox),
            "datetime": datetime,
            "limit": limit,
        }
        return _page_search(url, params=params, timeout=self.timeout)


class STACSource:
    """Fetch item metadata from any STAC API root (issue #218).

    Searches ``{root}/search`` via POST item-search. Unlike CMR-STAC, generic
    APIs (e.g. Earth Search, ``https://earth-search.aws.element84.com/v1``)
    use stable per-collection asset keys, so assets are kept under their own
    keys -- no canonical-key normalization.

    Parameters
    ----------
    root : str
        STAC API root URL.
    assets : list of str, optional
        Asset-key keep-list (e.g. ``["red", "nir", "scl"]``). ``None`` keeps
        every asset; subsetting keeps the geoparquet struct schema lean.
    time_key : str, optional
        Item property naming the *acquisition group* (issue #218): items of
        one Sentinel-2 datatake land in adjacent MGRS tiles with datetimes
        seconds apart, so the ``(time, cell)`` timestep identity is e.g.
        ``"s2:datatake_id"``, not the item datetime. Recorded in the catalog
        metadata and surfaced per record by :meth:`Catalog.granule_records`.
    timeout : int
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        root: str,
        assets: list[str] | None = None,
        time_key: str | None = None,
        timeout: int = 60,
    ):
        self.root = root.rstrip("/")
        self.assets = assets
        self.time_key = time_key
        self.timeout = timeout

    def fetch(self, query: STACQuery, *, limit: int = 250) -> "Catalog":
        """Run ``query`` against the STAC API and return a ``Catalog``.

        Parameters
        ----------
        query : STACQuery
            What/when/where to fetch.
        limit : int
            Page size hint; paging follows ``rel=next``. Default 250: servers
            that clamp politely allow more, but Earth Search's gateway 502s
            outright when the response page grows too large (observed
            2026-08-03: heavy c1 items 502 above ~300/page instead of
            clamping), and a deterministic 502 is indistinguishable from a
            transient one — so the default stays under the observed ceiling.

        Returns
        -------
        Catalog
        """
        import stac_geoparquet.arrow as sga

        bbox = _resolve_bbox(query.region)
        datetime = f"{query.start_date}T00:00:00Z/{query.end_date}T23:59:59Z"
        body: dict = {
            "collections": list(query.collections),
            "bbox": list(bbox),
            "datetime": datetime,
            "limit": limit,
        }
        if query.max_cloud_cover is not None:
            body["query"] = {"eo:cloud_cover": {"lt": query.max_cloud_cover}}

        crawled = _page_search(f"{self.root}/search", body=body, timeout=self.timeout)
        items, skipped_ids = crawled, []
        if self.assets is not None:
            items = []
            for it in crawled:
                sub = _subset_assets(it, self.assets)
                (items if sub is not None else skipped_ids).append(
                    sub if sub is not None else it.get("id")
                )
            if skipped_ids:
                logging.warning(
                    "STACSource: skipped %d item(s) missing requested assets %s; e.g. %s",
                    len(skipped_ids),
                    sorted(self.assets),
                    skipped_ids[:5],
                )
            if crawled and not items:
                example = sorted((crawled[0].get("assets") or {}))
                raise ValueError(
                    f"No items carry all requested assets {sorted(self.assets)} "
                    f"(e.g. available keys: {example})"
                )
        if not items:
            raise ValueError(
                f"No items for {query.collections} over {bbox} in "
                f"{query.start_date}..{query.end_date}"
            )

        table = pa.table(sga.parse_stac_items_to_arrow(items))
        meta = {
            "source": "STAC",
            "root": self.root,
            "collections": list(query.collections),
            "start_date": query.start_date,
            "end_date": query.end_date,
            "bbox": list(bbox),
            "time_key": self.time_key,
            "max_cloud_cover": query.max_cloud_cover,
            "assets": self.assets,
            "total_granules": len(items),
        }
        if skipped_ids:
            meta["skipped_items"] = {"count": len(skipped_ids), "examples": skipped_ids[:5]}
        return Catalog(_attach_meta(table, meta), meta)


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

    def filter_bbox(self, boxes) -> "Catalog":
        """Subset to granules whose bbox overlaps any of ``boxes`` (superset cut).

        A columnar prefilter over the stac-geoparquet ``bbox`` column — no
        geometry runs here. The exact footprint-vs-shard intersection happens
        in ``ShardMap.build`` (mortie / spherely backends); this cut exists so
        a large catalog (e.g. a full-mission clone) hands the exact backend
        thousands of candidates instead of the whole archive.

        Parameters
        ----------
        boxes : tuple or list of tuple
            One ``(lon_min, lat_min, lon_max, lat_max)`` box, or a list of
            them — one per scattered AOI part, so a multipart AOI is cut
            per part rather than by its (possibly continental) union box.
            Pair with ``grid.coverage_bbox`` for shard-complete cuts.

        Returns
        -------
        Catalog
            New catalog with the subset table; metadata carried verbatim.
        """
        import pyarrow.compute as pc

        if boxes and isinstance(boxes[0], (int, float)):
            boxes = [boxes]
        bb = self.table.column("bbox")
        lon0, lat0, lon1, lat1 = (
            pc.struct_field(bb, f).to_numpy(zero_copy_only=False)
            for f in ("xmin", "ymin", "xmax", "ymax")
        )
        keep = np.zeros(len(lon0), dtype=bool)
        for b_lon0, b_lat0, b_lon1, b_lat1 in boxes:
            keep |= (lon0 <= b_lon1) & (lon1 >= b_lon0) & (lat0 <= b_lat1) & (lat1 >= b_lat0)
        return Catalog(self.table.take(np.flatnonzero(keep)), dict(self.metadata))

    def granule_row_mask(self) -> np.ndarray:
        """Boolean mask over table rows of the rows :meth:`granule_records` emits.

        :meth:`granule_records` skips rows whose geometry is empty or not
        polygonal, so record ``i`` is table row ``np.flatnonzero(mask)[i]`` and
        ``mask.sum()`` is the record count -- both without decoding a single
        record. Same predicate as the row-wise loop, applied with one batched
        ``shapely.from_wkb`` instead of one call per row, and refusing on the one
        input where "same predicate" would otherwise be false (see Raises).

        Two callers need the alignment without the records:
        :meth:`index_footprints`, which gives screened rows an empty MOC so the
        column stays one entry per table row, and ``ShardMap.build``'s
        stored-index fast path, which intersects on the column *before*
        materializing anything (issues #396, #439).

        Returns
        -------
        numpy.ndarray
            ``bool``, length ``table.num_rows``.

        Raises
        ------
        ValueError
            When any row's ``geometry`` is **null**. ``shapely.from_wkb`` maps a
            null to ``None``, whose ``get_type_id`` is ``-1`` -- so screening it
            out here would be silent, while :meth:`granule_records`' row-wise
            ``geom.is_empty`` raises ``AttributeError`` on the same row. A
            granule that vanishes from an indexed build but crashes an
            unindexed one is worse than either, so both paths refuse: this one
            loudly, naming the count and the first offending row.

        Notes
        -----
        Time is small (0.02 s over the 35,639-granule 88S catalog, ~0.3 s over
        the 555,867-row ATL03 clone) but memory is not: the ``to_numpy`` WKB
        copy plus the live shapely objects put the clone's RSS ~1 GB over the
        parquet read. It is a peak, not a leak -- the objects die with the call.
        """
        import shapely

        geoms = shapely.from_wkb(self.table.column("geometry").to_numpy(zero_copy_only=False))
        # geom_type ids 3 and 6 are Polygon and MultiPolygon; -1 is a null WKB.
        type_ids = shapely.get_type_id(geoms)
        null = type_ids == -1
        if null.any():
            rows = np.flatnonzero(null)
            raise ValueError(
                f"catalog has {rows.size} row(s) with a null geometry (first at row "
                f"{int(rows[0])}); a granule with no footprint can be neither covered nor "
                f"assigned. Drop those rows from the catalog before indexing or building."
            )
        return ~shapely.is_empty(geoms) & np.isin(type_ids, (3, 6))

    def cover_footprints(self, order: int) -> tuple:
        """Cover every footprint at ``order`` and return the MOCs -- persisting nothing.

        The shared core of :meth:`index_footprints` and of ``ShardMap.build``'s
        **unindexed** mortie path (issue #445): one ``mortie.arrow.from_wkb``
        call over the geometry column, screened by :meth:`granule_row_mask`, in
        the same row-aligned ragged layout :meth:`footprint_cells` returns. The
        only difference between the two callers is what they do with it --
        ``index_footprints`` writes it into a column, ``build`` intersects it and
        drops it -- so they cannot drift on the cover itself. "Indexed" is
        therefore a statement about persistence, not about how a build assigns.

        Parameters
        ----------
        order : int
            HEALPix order to cover at. Unvalidated here: callers own the bound
            (``index_footprints`` refuses above ``MORTIE_MOC_ORDER_CAP``,
            ``build`` covers at the grid's ``parent_order``).

        Returns
        -------
        tuple
            ``(values, offsets, rows)``. ``values`` is every covered row's
            morton words concatenated (``uint64``); ``offsets`` are arrow list
            offsets **over the whole table** (``int64``, length
            ``table.num_rows + 1``), so row ``i``'s MOC is
            ``values[offsets[i]:offsets[i + 1]]`` and a screened-out row carries
            a zero-length run; ``rows`` is the covered table rows (``int64``,
            ascending) -- ``np.flatnonzero`` of the screen, which is exactly the
            order :meth:`granule_records` emits.

        Notes
        -----
        Row-aligned rather than compact so both callers index it by table row:
        the column ``index_footprints`` writes is one entry per row by contract,
        and ``build``'s intersection is positional against the same table. The
        empty runs cost 8 bytes each.

        Memory is :meth:`index_footprints`' documented posture, because it is
        this method: ``from_wkb`` bounds its own peak at roughly the result
        size, and the screen's WKB copy plus live shapely objects is the term it
        does not bound (~1 GB over the parquet read on the 555,867-row ATL03
        clone -- a peak, not a leak, issue #429).
        """
        from mortie.arrow import from_wkb

        column = self.table.column("geometry")
        # The shapely objects the screen decodes die with the call, so its peak
        # does not stack with the cover below (see ``granule_row_mask``).
        keep = self.granule_row_mask()
        if keep.all():
            # The case that actually occurs (every catalog in the tree); taking
            # the column whole avoids a second WKB copy.
            values, kept_offsets = from_wkb(column, order=int(order))
        elif keep.any():
            values, kept_offsets = from_wkb(column.filter(pa.array(keep)), order=int(order))
        else:
            values, kept_offsets = np.empty(0, dtype=np.uint64), np.zeros(1, dtype=np.int64)
        # Scatter the kept rows' MOC lengths back over every row: screened rows
        # get a zero-length run, so ``values[offsets[i]:offsets[i + 1]]`` stays
        # keyed by table row.
        counts = np.zeros(len(keep), dtype=np.int64)
        counts[keep] = np.diff(kept_offsets)
        offsets = np.zeros(len(keep) + 1, dtype=np.int64)
        np.cumsum(counts, out=offsets[1:])
        rows = np.flatnonzero(keep).astype(np.int64, copy=False)
        return np.asarray(values, dtype=np.uint64), offsets, rows

    def index_footprints(self, order: int) -> "Catalog":
        """Precompute the ``footprint_cells`` morton MOC column (issue #396).

        Covers every row's ``geometry`` WKB once, at ``order``, and returns a new
        catalog carrying the result as a ragged column plus the order in its
        metadata. The per-granule footprint cover is identical for every
        ``ShardMap.build`` against this catalog, so paying it once here turns
        each later build into set intersection with no geometry work at all --
        see ``ShardMap.build``'s fast path.

        Parameters
        ----------
        order : int
            HEALPix order to cover at. **Choose the shard order** (the grid's
            ``parent_order``), not the finer chunk order: coverage words per
            granule roughly double per order, so a full ATL03 clone indexes to
            ~270-420 MB of parquet at order 9 but ~9-13 GB at order 13, and the
            extra resolution is invisible to order-9 shard cells. The column
            serves every grid whose ``parent_order`` is **at most** ``order``;
            a finer grid is refused by ``build`` rather than answered coarsely.
            Bounded above by ``MORTIE_MOC_ORDER_CAP`` (mortie's order-18 coverage
            cap), the same bound ``ShardMap.build`` clamps its own MOC order to,
            so the two paths agree on what "too fine" means. ``build`` clamps
            because its order is derived; this one is the operator's own number,
            so it raises rather than silently indexing at another order than the
            one recorded in ``footprint_cells_order``.

        Raises
        ------
        ValueError
            When ``order`` is above ``MORTIE_MOC_ORDER_CAP``.

        Returns
        -------
        Catalog
            New catalog, same rows and metadata plus ``footprint_cells`` and
            ``footprint_cells_order``. Re-indexing at another order replaces
            the column rather than appending a second one.

        Notes
        -----
        ``mortie.arrow.from_wkb`` (mortie >= 0.9.5, espg/mortie#157/#163) takes
        the geometry column across the Python/Rust boundary once, with the GIL
        released and chunking that bounds peak at roughly the result size. It
        covers the **union of the rings inside each blob**, where
        :meth:`granule_records` reads the largest part's exterior ring only, so
        the two agree exactly on single-part footprints (every CMR ATL03/06
        granule) and the column is a superset for a MultiPolygon -- it keeps the
        smaller parts ``granule_records`` drops.

        Rows :meth:`granule_records` would skip -- empty or non-polygonal
        geometry -- are screened out with the **same** shapely predicate it uses
        and get an empty MOC, so the column stays one entry per table row and a
        catalog carrying a stray ``Point`` indexes rather than raising (mortie's
        coverage refuses a point outright, naming the blob). The screen is
        :meth:`granule_row_mask` -- one vectorised ``shapely.from_wkb``, shared
        with ``ShardMap.build``'s fast path; on the 35,639-granule 88S catalog
        it is 0.02 s against 2.65 s for the order-9 cover, so it costs under 1%
        of a pass that runs once per catalog. A **null** geometry is the one row
        the screen refuses instead of skipping, so this method raises where it
        would once have indexed -- see :meth:`granule_row_mask`'s Raises.

        The screen is cheap in time but it is this pass's **peak in memory**, and
        it is the term ``from_wkb``'s chunking does not bound: on the
        555,867-row ATL03 clone RSS goes 835 MB after the parquet read -> 1,169
        MB after ``to_numpy`` (a full WKB copy) -> 1,794 MB with the shapely
        objects live. They die with ``granule_row_mask``'s frame, so that stays
        a peak rather than stacking with the cover, but a whole-clone index
        wants headroom for it. :meth:`cover_footprints`' ``keep.all()``
        short-circuit avoids a second ~334 MB WKB copy in the case that actually
        occurs (nothing screened out -- every catalog in the tree). Reading the
        geometry-type word straight out of the WKB, or chunking the
        ``from_wkb`` call, would drop the screen's peak entirely; not done here
        because it trades the shared shapely predicate for a hand-rolled one.
        """
        from mortie.arrow import from_morton_index

        from zagg.catalog.shardmap import MORTIE_MOC_ORDER_CAP

        if int(order) > MORTIE_MOC_ORDER_CAP:
            raise ValueError(
                f"footprint_cells order {int(order)} is above mortie's coverage cap "
                f"{MORTIE_MOC_ORDER_CAP}; ShardMap.build clamps its own MOC order there, so a "
                f"finer column could not be the cover any build asks for. Index at "
                f"order <= {MORTIE_MOC_ORDER_CAP} (the grid's parent_order is the right choice)."
            )
        # The cover, the screen and the row-aligned scatter all live in
        # :meth:`cover_footprints` -- shared with ``ShardMap.build``'s unindexed
        # path (issue #445), which runs the identical cover and simply keeps it
        # in memory. This method is that cover plus the column write.
        values, offsets, _ = self.cover_footprints(int(order))
        cells = pa.LargeListArray.from_arrays(
            pa.array(offsets, pa.int64()), from_morton_index(values)
        )
        table = self.table
        if FOOTPRINT_CELLS in table.column_names:
            table = table.set_column(
                table.column_names.index(FOOTPRINT_CELLS), FOOTPRINT_CELLS, cells
            )
        else:
            table = table.append_column(FOOTPRINT_CELLS, cells)
        meta = {**self.metadata, FOOTPRINT_CELLS_ORDER: int(order)}
        return Catalog(_attach_meta(table, meta), meta)

    def footprint_cells(self):
        """The stored footprint index as ``(values, offsets, order)``, or ``None``.

        ``None`` when the catalog was never indexed (:meth:`index_footprints`),
        which is what keeps the ``build`` fast path opt-in: an ordinary catalog
        simply has no column and takes the geometry path.

        Returns
        -------
        tuple or None
            ``(values, offsets, order)`` where ``values`` is the concatenated
            ``uint64`` morton words of every **table row** (not every
            ``granule_records`` entry -- ``build`` aligns the two by row position,
            via ``np.flatnonzero(granule_row_mask())``, since the screened rows
            carry a zero-length run rather than being absent) and ``offsets`` are
            arrow list offsets into it, so row ``i``'s MOC is
            ``values[offsets[i]:offsets[i + 1]]``.
        """
        order = (self.metadata or {}).get(FOOTPRINT_CELLS_ORDER)
        if order is None or FOOTPRINT_CELLS not in self.table.column_names:
            return None
        arr = self.table.column(FOOTPRINT_CELLS).combine_chunks()
        inner = arr.values
        # The column is written with mortie's ``morton_index`` extension type, so
        # the words arrive typed wherever mortie is importable; a reader without
        # it (or a parquet writer that dropped the metadata) sees the uint64
        # storage instead. Accept both -- the words are the same either way.
        inner = inner.storage if isinstance(inner, pa.ExtensionArray) else inner
        values = np.asarray(inner.to_numpy(zero_copy_only=False), dtype=np.uint64)
        offsets = np.asarray(arr.offsets.to_numpy(zero_copy_only=False), dtype=np.int64)
        return values, offsets, int(order)

    def granule_records(self) -> list[dict]:
        """Decode the table into per-granule dicts for ShardMap building.

        Returns
        -------
        list of dict
            Each: ``{"id", "s3", "https", "lats", "lons"}`` where ``lats``/
            ``lons`` are the footprint exterior-ring coordinate arrays (WGS84)
            and ``s3``/``https`` are the canonical data-asset hrefs (either may
            be None). Records with *no* canonical data asset (raster sources,
            #218) additionally carry ``assets`` (``{key: href}`` for every
            non-canonical asset), ``datetime`` (ISO acquisition time), and
            ``time_key`` (the acquisition-group property named by the
            catalog's ``time_key`` metadata, when present); any
            record with a ``data``/``data_s3`` asset -- every CMR record,
            including ``preserve_thumbnails=True`` -- keeps its exact pre-#218
            shape, except that any record whose catalog carries STAC
            ``start_datetime``/``end_datetime`` also gains ``time_start``/
            ``time_end`` (ISO acquisition range, issue #246) for the
            per-window dispatch subsetting.
        """
        import shapely

        ids = self.table.column("id").to_pylist()
        assets = self.table.column("assets").to_pylist()
        geoms = self.table.column("geometry").to_pylist()
        dts = (
            self.table.column("datetime").to_pylist()
            if "datetime" in self.table.column_names
            else [None] * len(ids)
        )

        # Per-granule acquisition range (issue #246): STAC items carry
        # start_datetime/end_datetime properties (every CMR granule does),
        # flattened to columns by stac-geoparquet. Surfaced on EVERY record so
        # the ShardMap can subset granules per time window at dispatch;
        # catalogs without the columns (or null rows) simply omit the keys —
        # the fan-out then degrades to its conservative every-window path.
        def _col(name):
            if name in self.table.column_names:
                return self.table.column(name).to_pylist()
            return [None] * len(ids)

        t_starts, t_ends = _col("start_datetime"), _col("end_datetime")
        # Acquisition-group property (issue #218): the source records its item
        # property name in the catalog metadata; stac-geoparquet flattens item
        # properties to top-level columns, so it reads straight off the table.
        tk_col = (self.metadata or {}).get("time_key")
        tks = (
            self.table.column(tk_col).to_pylist()
            if tk_col and tk_col in self.table.column_names
            else [None] * len(ids)
        )
        records = []
        for gid, asset_map, wkb, dt, tk, ts, te in zip(
            ids, assets, geoms, dts, tks, t_starts, t_ends
        ):
            geom = shapely.from_wkb(wkb)
            if geom.is_empty or geom.geom_type not in ("Polygon", "MultiPolygon"):
                continue
            poly = geom if geom.geom_type == "Polygon" else max(geom.geoms, key=lambda g: g.area)
            x, y = poly.exterior.coords.xy
            asset_map = asset_map or {}
            data = asset_map.get("data") or {}
            data_s3 = asset_map.get("data_s3") or {}
            rec = {
                "id": gid,
                "https": data.get("href"),
                "s3": data_s3.get("href"),
                "lats": np.asarray(y),
                "lons": np.asarray(x),
            }
            # Acquisition range keys (issue #246), on any record whose catalog
            # carries them (unlike the raster-only extras below): newly built
            # ShardMaps gain per-granule time metadata; pre-#246 manifests
            # without the keys keep reading (the dispatcher degrades
            # conservatively).
            if ts is not None:
                rec["time_start"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            if te is not None:
                rec["time_end"] = te.isoformat() if hasattr(te, "isoformat") else str(te)
            # Only records with no canonical data asset (raster sources) grow
            # the extra keys; any record carrying data/data_s3 -- every CMR
            # record, including preserve_thumbnails -- stays byte-identical
            # through granule_records -> ShardMap so existing manifests don't
            # change shape (#218).
            if "data" not in asset_map and "data_s3" not in asset_map:
                extra = {
                    k: (a or {}).get("href")
                    for k, a in asset_map.items()
                    if k not in ("data", "data_s3", "metadata") and (a or {}).get("href")
                }
                if extra:
                    rec["assets"] = extra
                    if dt is not None:
                        rec["datetime"] = dt.isoformat()
                    if tk is not None:
                        rec["time_key"] = str(tk)
            records.append(rec)
        return records


__all__ = [
    "Query",
    "STACQuery",
    "CMRSource",
    "STACSource",
    "Catalog",
    "FOOTPRINT_CELLS",
    "FOOTPRINT_CELLS_ORDER",
]
