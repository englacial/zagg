"""Rectilinear (regular gridded) output grid.

A 2D grid in a user-specified projected CRS. Cells are squares (or rectangles
when ``resolution`` is a 2-tuple) tiled across ``bounds``. Storage is a 2D
Zarr array with one chunk per ``chunk_shape`` block; each chunk is one shard
(``shard_of`` and ``block_index`` collapse to chunk-arithmetic, no remap).

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

import numpy as np
from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig
from zarr import config as zarr_config
from zarr.abc.store import Store

from zagg.config import PipelineConfig, default_config, get_agg_fields

OOB_SENTINEL: int = -1


def _normalize_resolution(res) -> tuple[float, float]:
    """Return (res_x, res_y) from a scalar or 2-sequence."""
    if np.isscalar(res):
        return (float(res), float(res))
    rx, ry = res
    return (float(rx), float(ry))


class RectilinearGrid:
    """Rectilinear projected grid.

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
        # Width / height in cells, rounded down to whole cells.
        self.width = int(span_x // self.res_x)
        self.height = int(span_y // self.res_y)
        if self.width == 0 or self.height == 0:
            raise ValueError("resolution larger than bounds span")
        # Chunk grid dimensions. Edge chunks may not align cleanly; the design
        # constraint is that chunk_shape divides (width, height). Enforce.
        if self.height % self.chunk_h != 0 or self.width % self.chunk_w != 0:
            raise ValueError(
                f"chunk_shape ({self.chunk_h}, {self.chunk_w}) must divide "
                f"grid shape ({self.height}, {self.width}). Adjust bounds, "
                f"resolution, or chunk_shape."
            )
        self.n_row_blocks = self.height // self.chunk_h
        self.n_col_blocks = self.width // self.chunk_w

        self._transformer = None  # lazy

    # ── shape properties ─────────────────────────────────────────────────

    @property
    def array_shape(self) -> tuple[int, int]:
        return (self.height, self.width)

    @property
    def chunk_shape(self) -> tuple[int, int]:
        return (self.chunk_h, self.chunk_w)

    @property
    def group_path(self) -> str:
        return "rectilinear"

    # ── coverage / coords ────────────────────────────────────────────────

    def coverage(self, polygon_parts) -> np.ndarray:
        """Enumerate shard keys whose chunk bbox intersects any polygon part.

        Parts are ``(lats, lons)`` arrays in WGS84.
        """
        from shapely import STRtree
        from shapely.geometry import Polygon

        tx = self._transformer_to_grid()
        polys_grid = []
        for lats, lons in polygon_parts:
            xs, ys = tx.transform(np.asarray(lons), np.asarray(lats))
            polys_grid.append(Polygon(zip(xs, ys)))
        tree = STRtree(polys_grid)

        # Iterate the chunk grid and test intersection.
        hits = []
        for rb in range(self.n_row_blocks):
            for cb in range(self.n_col_blocks):
                bbox = self._chunk_bbox_grid((rb, cb))
                box_poly = Polygon(
                    [
                        (bbox[0], bbox[1]),
                        (bbox[2], bbox[1]),
                        (bbox[2], bbox[3]),
                        (bbox[0], bbox[3]),
                    ]
                )
                if len(tree.query(box_poly, predicate="intersects")) > 0:
                    hits.append(self._pack(rb, cb))
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
        in_bounds = (
            (rows >= 0) & (rows < self.height) & (cols >= 0) & (cols < self.width)
        )
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
        v = leaf_ids[valid]
        rows = v // self.width
        cols = v % self.width
        rb = rows // self.chunk_h
        cb = cols // self.chunk_w
        out[valid] = rb * self.n_col_blocks + cb
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
            raise InconsistentShardError(
                "leaves span multiple shards"
            )
        return first

    # ── storage / footprint ──────────────────────────────────────────────

    def block_index(self, shard_key) -> tuple[int, int]:
        rb, cb = self._unpack(int(shard_key))
        return (rb, cb)

    def shard_footprint(self, shard_key):
        """Chunk bbox reprojected to WGS84, densified along edges."""
        from shapely.geometry import Polygon

        rb, cb = self._unpack(int(shard_key))
        xmin, ymin, xmax, ymax = self._chunk_bbox_grid((rb, cb))
        # Densify each edge with 32 points before reprojection so curved CRS
        # boundaries don't get short-circuited.
        n = 32
        xs = np.concatenate(
            [
                np.linspace(xmin, xmax, n),                  # bottom (y=ymin)
                np.full(n, xmax),                            # right
                np.linspace(xmax, xmin, n),                  # top
                np.full(n, xmin),                            # left
            ]
        )
        ys = np.concatenate(
            [
                np.full(n, ymin),
                np.linspace(ymin, ymax, n),
                np.full(n, ymax),
                np.linspace(ymax, ymin, n),
            ]
        )
        tx = self._transformer_from_grid()
        lons, lats = tx.transform(xs, ys)
        return Polygon(zip(lons, lats))

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
            chunk_key_encoding=NamedConfig(
                name="default", configuration={"separator": "/"}
            ),
            codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
            storage_transformers=(),
            fill_value="NaN",
        )

        # Coordinate arrays — 1D x and y for CF/GeoZarr compliance.
        # Cell-centre values are populated by emit_template after the template
        # is written.
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
        coord_y = coord_x.with_shape((self.height,)).with_dimension_names(
            ("y",)
        ).with_attributes(
            {"standard_name": "projection_y_coordinate", "units": "m"}
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
            members[name] = base.with_data_type(dtype).with_fill_value(fill)

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

    def _chunk_bbox_grid(self, rb_cb) -> tuple[float, float, float, float]:
        """Chunk bbox in grid CRS: (xmin, ymin, xmax, ymax)."""
        rb, cb = rb_cb
        xmin = self.xmin + cb * self.chunk_w * self.res_x
        xmax = xmin + self.chunk_w * self.res_x
        ymax = self.ymax - rb * self.chunk_h * self.res_y
        ymin = ymax - self.chunk_h * self.res_y
        return (xmin, ymin, xmax, ymax)

    def _transformer_to_grid(self):
        """WGS84 → grid CRS transformer (lat/lon → x/y)."""
        if self._transformer is None:
            from pyproj import Transformer

            self._transformer = {
                "to_grid": Transformer.from_crs("EPSG:4326", self.crs, always_xy=True),
                "from_grid": Transformer.from_crs(self.crs, "EPSG:4326", always_xy=True),
            }
        return self._transformer["to_grid"]

    def _transformer_from_grid(self):
        """Grid CRS → WGS84 transformer."""
        self._transformer_to_grid()  # ensure init
        return self._transformer["from_grid"]


__all__ = ["RectilinearGrid", "OOB_SENTINEL"]
