"""Rectilinear (regular gridded) output grid, backed by ``odc.geo.GeoBox``.

A 2D grid in a user-specified projected CRS. Cells are squares (or rectangles
when ``resolution`` is a 2-tuple) tiled across ``bounds``. Storage is a 2D
Zarr array with one chunk per ``chunk_shape`` block; each chunk is one shard
(``shard_of`` and ``block_index`` collapse to chunk-arithmetic, no remap).

The grid wraps a ``GeoBox`` (shape + affine + CRS) and a ``GeoboxTiles`` chunk
tiling, which supply coverage, footprint reprojection, and the alignment math
that ``nests_with`` needs. The integer leaf/shard packing is plain row-major
arithmetic on the GeoBox affine.

YAML config form::

    output:
      grid:
        type: rectilinear
        crs: EPSG:3031
        resolution: 5000              # metres (CRS units); scalar or [res_x, res_y]
        bounds: [-3200000, -3200000, 3200000, 3200000]   # xmin, ymin, xmax, ymax
        chunk_shape: [256, 256]

Internal representations
------------------------
- **leaf id** = row-major flat cell index ``r * width + c`` (uint64).
- **shard key** = row-major flat chunk index ``rb * n_col_blocks + cb``
  (int). ``block_index`` unpacks back to ``(rb, cb)``.
- **cell id** = leaf id (no coarsening needed; ``cells_of`` is identity).

Out-of-bounds points get leaf id ``-1`` (signed). The corresponding shard
filter rejects them, so they fall out of the pipeline silently.
"""

from __future__ import annotations

import math

import numpy as np
from affine import Affine
from odc.geo.geobox import GeoBox, GeoboxTiles
from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig
from zarr import config as zarr_config
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    default_config,
    get_agg_fields,
    get_output_signature,
    output_field_signature,
)
from zagg.grids.base import chunk_array_spec, vector_array_spec

OOB_SENTINEL: int = -1


def _normalize_resolution(res) -> tuple[float, float]:
    """Return (res_x, res_y) from a scalar or 2-sequence."""
    if np.isscalar(res):
        return (float(res), float(res))
    rx, ry = res
    return (float(rx), float(ry))


class RectilinearGrid:
    """Rectilinear projected grid backed by ``odc.geo.GeoBox``.

    Parameters
    ----------
    crs : str
        Grid CRS (e.g. ``"EPSG:3031"``).
    resolution : float or (float, float)
        Cell size in CRS units. Scalar means square cells.
    bounds : (float, float, float, float)
        ``(xmin, ymin, xmax, ymax)`` in grid CRS.
    chunk_shape : (int, int)
        ``(chunk_h, chunk_w)`` cells per chunk.
    config : PipelineConfig, optional
        Aggregation schema. Falls back to ``default_config("atl06")``.

    Notes
    -----
    Grid origin is at ``(xmin, ymax)`` with row 0 at the top (north-up).
    """

    def __init__(
        self,
        crs: str,
        resolution,
        bounds,
        chunk_shape=(256, 256),
        config: PipelineConfig | None = None,
    ):
        if len(bounds) != 4:
            raise ValueError("bounds must be (xmin, ymin, xmax, ymax)")
        if len(chunk_shape) != 2:
            raise ValueError("chunk_shape must be (chunk_h, chunk_w)")
        self.crs = str(crs)
        self.res_x, self.res_y = _normalize_resolution(resolution)
        self.xmin, self.ymin, self.xmax, self.ymax = (float(b) for b in bounds)
        self.chunk_h, self.chunk_w = (int(c) for c in chunk_shape)
        self.config = config or default_config("atl06")

        span_x = self.xmax - self.xmin
        span_y = self.ymax - self.ymin
        if span_x <= 0 or span_y <= 0:
            raise ValueError("bounds must have xmax > xmin and ymax > ymin")
        # Cells needed to cover the requested extent (round up; the 1e-9 guards
        # an exactly-divisible span from rounding up on float fuzz).
        raw_w = int(math.ceil(span_x / self.res_x - 1e-9))
        raw_h = int(math.ceil(span_y / self.res_y - 1e-9))
        if raw_w == 0 or raw_h == 0:
            raise ValueError("resolution larger than bounds span")
        # Zero-pad the far edges up to a whole number of chunks so one chunk ==
        # one shard. The origin (xmin, ymax) is preserved, so cell/chunk
        # alignment and `nests_with` stay valid; the extra cells are empty.
        self.width = -(-raw_w // self.chunk_w) * self.chunk_w
        self.height = -(-raw_h // self.chunk_h) * self.chunk_h
        # Extend the far bounds (xmax, ymin) to match the padded grid.
        self.xmax = self.xmin + self.width * self.res_x
        self.ymin = self.ymax - self.height * self.res_y
        self.n_row_blocks = self.height // self.chunk_h
        self.n_col_blocks = self.width // self.chunk_w

        # GeoBox: north-up affine with origin at (xmin, ymax); y resolution
        # negative. GeoboxTiles tiles it into one tile per chunk.
        affine = Affine.translation(self.xmin, self.ymax) * Affine.scale(self.res_x, -self.res_y)
        self._geobox = GeoBox((self.height, self.width), affine, self.crs)
        self._tiles = GeoboxTiles(self._geobox, (self.chunk_h, self.chunk_w))
        self._transformer = None  # lazy WGS84 -> grid CRS, for assign

    # ── shape properties ─────────────────────────────────────────────────

    @property
    def array_shape(self) -> tuple[int, int]:
        return (self.height, self.width)

    @property
    def chunk_shape(self) -> tuple[int, int]:
        return (self.chunk_h, self.chunk_w)

    @property
    def chunk_grid_shape(self) -> tuple[int, int]:
        """Number of chunks per axis (``array_shape // chunk_shape``).

        A ``resolution: chunk`` field (issue #30 item 2) stores one value per
        chunk in a companion array of this shape, indexed by :meth:`block_index`
        (the ``(rb, cb)`` chunk index). Equals ``(n_row_blocks, n_col_blocks)``.
        """
        return (self.n_row_blocks, self.n_col_blocks)

    @property
    def group_path(self) -> str:
        return "rectilinear"

    # ── identity / nesting ───────────────────────────────────────────────

    def signature(self) -> dict:
        """Canonical fingerprint of the grid's defining parameters.

        Recorded in a ShardMap at build time and re-checked at run time so a
        shard map can never be silently paired with a different grid.
        """
        a = self._geobox.affine
        return {
            "type": "rectilinear",
            "crs": str(self._geobox.crs),
            "affine": [a.a, a.b, a.c, a.d, a.e, a.f],
            "shape": [self.height, self.width],
            "chunk_shape": [self.chunk_h, self.chunk_w],
            "output_fields": output_field_signature(self.config),
        }

    def nests_with(self, other) -> bool:
        """Whether ``self`` and ``other`` tile compatibly (align + nest).

        True only for another rectilinear grid in the same CRS whose
        resolutions are whole-number ratios and whose origins align on the
        finer grid. Cross-family (e.g. HEALPix) never nests.
        """
        if not isinstance(other, RectilinearGrid):
            return False
        if self._geobox.crs != other._geobox.crs:
            return False
        if output_field_signature(self.config) != output_field_signature(other.config):
            # Co-aggregated grids must declare the same Option-B output-field
            # set (issue #29): same scalar/vector kinds, trailing shapes, dtypes.
            return False
        if not (_whole_ratio(self.res_x, other.res_x) and _whole_ratio(self.res_y, other.res_y)):
            return False
        fine_x = min(self.res_x, other.res_x)
        fine_y = min(self.res_y, other.res_y)
        return _is_multiple(self.xmin - other.xmin, fine_x) and _is_multiple(
            self.ymax - other.ymax, fine_y
        )

    # ── coverage / coords ────────────────────────────────────────────────

    def coverage(self, polygon_parts) -> np.ndarray:
        """Enumerate shard keys whose chunk intersects any polygon part.

        Parts are ``(lats, lons)`` arrays in WGS84. Reprojection to the grid
        CRS and tile intersection are handled by odc.geo.
        """
        from odc.geo.geom import multipolygon, polygon

        rings = []
        for lats, lons in polygon_parts:
            rings.append([(float(x), float(y)) for x, y in zip(np.asarray(lons), np.asarray(lats))])
        if len(rings) == 1:
            geom = polygon(rings[0], crs="EPSG:4326")
        else:
            geom = multipolygon([[r] for r in rings], crs="EPSG:4326")
        geom = geom.to_crs(self._geobox.crs)

        hits = {self._pack(rb, cb) for rb, cb in self._tiles.tiles(geom)}
        return np.asarray(sorted(hits), dtype=np.int64)

    # ── point assignment ─────────────────────────────────────────────────

    def assign(self, lats, lons) -> np.ndarray:
        """Map (lat, lon) points to row-major flat cell indices.

        Returns ``-1`` for out-of-bounds points.
        """
        lats = np.asarray(lats)
        lons = np.asarray(lons)
        tx = self._transformer_to_grid()
        xs, ys = tx.transform(lons, lats)
        cols = ((xs - self.xmin) // self.res_x).astype(np.int64)
        rows = ((self.ymax - ys) // self.res_y).astype(np.int64)
        in_bounds = (rows >= 0) & (rows < self.height) & (cols >= 0) & (cols < self.width)
        ids = rows * self.width + cols
        return np.where(in_bounds, ids, OOB_SENTINEL).astype(np.int64)

    def cells_of(self, leaf_ids) -> np.ndarray:
        """Identity for rectilinear (leaf_id == cell_id)."""
        return np.asarray(leaf_ids)

    def shards_of(self, leaf_ids) -> np.ndarray:
        """Packed shard key per leaf. OOB leaves get ``-1``."""
        leaf_ids = np.asarray(leaf_ids)
        out = np.full_like(leaf_ids, OOB_SENTINEL, dtype=np.int64)
        valid = leaf_ids != OOB_SENTINEL
        if not np.any(valid):
            return out
        # Work around a signed-int64 miscompute that corrupts the chained index
        # math on arrays >= 2**15 elements (see issue #31; observed on an
        # unsupported numpy/CPython-3.14 pairing). Valid leaf ids are
        # non-negative, so uint64 is exact and uses the unaffected kernel.
        v = leaf_ids[valid].astype(np.uint64)
        rows = v // self.width
        cols = v % self.width
        rb = rows // self.chunk_h
        cb = cols // self.chunk_w
        out[valid] = (rb * self.n_col_blocks + cb).astype(np.int64)
        return out

    def shard_of(self, leaf_ids) -> int:
        """Assert all leaves share a shard; return its packed key."""
        from zagg.grids.base import InconsistentShardError

        shards = self.shards_of(leaf_ids)
        valid = shards != OOB_SENTINEL
        if not np.any(valid):
            raise InconsistentShardError("all leaves are out of bounds")
        first = int(shards[valid].flat[0])
        if not np.all(shards[valid] == first):
            raise InconsistentShardError("leaves span multiple shards")
        return first

    # ── storage / footprint ──────────────────────────────────────────────

    def block_index(self, shard_key) -> tuple[int, int]:
        rb, cb = self._unpack(int(shard_key))
        return (rb, cb)

    def shard_footprint(self, shard_key):
        """Chunk extent reprojected to WGS84, densified along edges.

        Densifying (~32 points per chunk edge) before reprojection keeps
        curved CRS boundaries — and pole-spanning tiles — from collapsing to
        a degenerate polygon.
        """
        rb, cb = self._unpack(int(shard_key))
        densify = max(self.chunk_w * self.res_x, self.chunk_h * self.res_y) / 32
        return self._tiles[(rb, cb)].extent.to_crs("EPSG:4326", resolution=densify).geom

    # ── leaf enumeration ─────────────────────────────────────────────────

    def children(self, shard_key) -> np.ndarray:
        """Cell ids inside this chunk, row-major within the chunk."""
        rb, cb = self._unpack(int(shard_key))
        r0 = rb * self.chunk_h
        c0 = cb * self.chunk_w
        rows = np.arange(self.chunk_h)[:, None] + r0
        cols = np.arange(self.chunk_w)[None, :] + c0
        return (rows * self.width + cols).reshape(-1).astype(np.int64)

    def encode_cell_ids(self, cell_ids) -> np.ndarray:
        """Identity for rectilinear (output coord is the flat cell id)."""
        return np.asarray(cell_ids, dtype=np.int64)

    def chunk_coords(self, shard_key) -> dict:
        """No per-cell coord columns; x/y are 1D dimensional coords on the template."""
        return {}

    # ── template ─────────────────────────────────────────────────────────

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        from zarr import open_array

        spec = self._spec()
        with zarr_config.set({"async.concurrency": 128}):
            spec.to_zarr(store, self.group_path, overwrite=overwrite)
        # Populate the x/y coord arrays with cell-centre coordinates so
        # downstream readers (xarray, rioxarray) get usable spatial axes.
        x_centers = self.xmin + (np.arange(self.width) + 0.5) * self.res_x
        y_centers = self.ymax - (np.arange(self.height) + 0.5) * self.res_y
        x_arr = open_array(store, path=f"{self.group_path}/x", zarr_format=3, consolidated=False)
        x_arr[:] = x_centers
        y_arr = open_array(store, path=f"{self.group_path}/y", zarr_format=3, consolidated=False)
        y_arr[:] = y_centers
        return store

    def _spec(self) -> GroupSpec:
        base = ArraySpec(
            attributes={},
            shape=self.array_shape,
            dimension_names=("y", "x"),
            data_type="float32",
            chunk_grid=NamedConfig(
                name="regular",
                configuration={"chunk_shape": list(self.chunk_shape)},
            ),
            chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
            codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
            storage_transformers=(),
            fill_value="NaN",
        )

        # Coordinate arrays — 1D x and y for CF/GeoZarr compliance.
        coord_x = ArraySpec(
            attributes={"standard_name": "projection_x_coordinate", "units": "m"},
            shape=(self.width,),
            dimension_names=("x",),
            data_type="float64",
            chunk_grid=NamedConfig(name="regular", configuration={"chunk_shape": [self.width]}),
            chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
            codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
            storage_transformers=(),
            fill_value=0.0,
        )
        coord_y = (
            coord_x.with_shape((self.height,))
            .with_dimension_names(("y",))
            .with_attributes({"standard_name": "projection_y_coordinate", "units": "m"})
        )

        members = {"x": coord_x, "y": coord_y}
        for name, meta in self.config.aggregation.get("coordinates", {}).items():
            # DGGS-specific coord names (cell_ids, morton) don't apply here.
            if name in ("cell_ids", "morton"):
                continue
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            members[name] = base.with_data_type(dtype).with_fill_value(fill)
        for name, meta in get_agg_fields(self.config).items():
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            spec = base.with_data_type(dtype).with_fill_value(fill)
            sig = get_output_signature(meta)
            if sig["resolution"] == "chunk":
                # A resolution: chunk field (issue #30 item 2) is stored once per
                # chunk in a companion array shaped at the chunk grid (row-major
                # chunk index), indexed by block_index = (rb, cb).
                members[name] = chunk_array_spec(
                    spec,
                    chunk_grid_shape=self.chunk_grid_shape,
                    chunk_dims=("chunk_y", "chunk_x"),
                )
                continue
            # A vector field (issue #29) gets a trailing payload dim chunked
            # whole; scalars are returned unchanged.
            members[name] = vector_array_spec(
                spec,
                sig,
                base_dims=("y", "x"),
                base_chunk_shape=self.chunk_shape,
            )

        return GroupSpec(members=members, attributes=self._geozarr_attrs())

    def _geozarr_attrs(self) -> dict:
        return {
            "crs": self.crs,
            "resolution": [self.res_x, self.res_y],
            "bounds": [self.xmin, self.ymin, self.xmax, self.ymax],
        }

    # ── internals ────────────────────────────────────────────────────────

    def _pack(self, rb: int, cb: int) -> int:
        return int(rb * self.n_col_blocks + cb)

    def _unpack(self, packed: int) -> tuple[int, int]:
        return (packed // self.n_col_blocks, packed % self.n_col_blocks)

    def _transformer_to_grid(self):
        """WGS84 → grid CRS transformer (lat/lon → x/y)."""
        if self._transformer is None:
            from pyproj import Transformer

            self._transformer = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        return self._transformer


def _whole_ratio(a: float, b: float, tol: float = 1e-9) -> bool:
    """True if the larger of a, b is a whole-number multiple of the smaller."""
    lo, hi = sorted((a, b))
    if lo <= 0:
        return False
    return abs(round(hi / lo) - hi / lo) < tol


def _is_multiple(delta: float, step: float, tol: float = 1e-6) -> bool:
    """True if ``delta`` is an integer multiple of ``step``."""
    q = delta / step
    return abs(round(q) - q) < tol


__all__ = ["RectilinearGrid", "OOB_SENTINEL"]
