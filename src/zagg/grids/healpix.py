"""HEALPix DGGS output grid via mortie."""
from __future__ import annotations

from typing import Literal

import numpy as np
from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig
from zarr import config as zarr_config
from zarr.abc.store import Store

from zagg.config import PipelineConfig, default_config, get_agg_fields
from zagg.grids.base import InconsistentShardError

HEALPIX_BASE_CELLS: int = 12
HEALPIX_REF_ORDER: int = 18  # mortie's clip2order reference order; do not change


class HealpixGrid:
    """HEALPix DGGS output grid.

    Two layouts:

    - ``"dense"``  shape ``(4^Δ · n_shards,)``. Chunks indexed by their
      position in ``populated_shards`` (catalog order). Requires
      ``populated_shards`` for ``block_index`` / ``emit_template``.
    - ``"fullsphere"``  shape ``(12 · 4^child_order,)``. Chunks indexed
      directly by parent morton ID. Stateless; no shard list needed.

    Parameters
    ----------
    parent_order : int
        Shard (chunk) order.
    child_order : int
        Leaf cell order.
    layout : {"dense", "fullsphere"}, optional
        Storage layout. Defaults to ``"dense"`` (matches pre-refactor behavior).
    config : PipelineConfig, optional
        Aggregation schema. Defaults to the built-in atl06 config.
    populated_shards : iterable of int, optional
        Parent morton IDs that will be written. Required for dense layout.
        Order is preserved (used as the storage-block ordering).
    """

    def __init__(
        self,
        parent_order: int,
        child_order: int,
        layout: Literal["dense", "fullsphere"] = "dense",
        config: PipelineConfig | None = None,
        populated_shards: list[int] | None = None,
    ):
        if child_order < parent_order:
            raise ValueError(
                f"child_order ({child_order}) must be >= parent_order ({parent_order})"
            )
        if layout not in ("dense", "fullsphere"):
            raise ValueError(f"Unknown layout: {layout!r} (expected 'dense' or 'fullsphere')")
        self.parent_order = parent_order
        self.child_order = child_order
        self.level_diff = child_order - parent_order
        self.n_children = 4**self.level_diff
        self.layout = layout
        self.config = config or default_config("atl06")
        self._position_map: dict[int, int] | None = None
        if populated_shards is not None:
            self.set_populated_shards(populated_shards)

    def set_populated_shards(self, shards) -> None:
        """Set the populated-shard list (dense layout only).

        Preserves input order — that order becomes the storage-block order.
        No-op for fullsphere layout.
        """
        if self.layout == "fullsphere":
            return
        self._position_map = {int(s): i for i, s in enumerate(shards)}

    @property
    def n_shards(self) -> int:
        """Number of shards in the storage layout."""
        if self.layout == "fullsphere":
            return HEALPIX_BASE_CELLS * (4**self.parent_order)
        if self._position_map is None:
            raise RuntimeError(
                "HealpixGrid(layout='dense') requires populated_shards before n_shards"
            )
        return len(self._position_map)

    @property
    def array_shape(self) -> tuple[int, ...]:
        if self.layout == "fullsphere":
            return (HEALPIX_BASE_CELLS * (4**self.child_order),)
        return (self.n_children * self.n_shards,)

    @property
    def chunk_shape(self) -> tuple[int, ...]:
        return (self.n_children,)

    @property
    def group_path(self) -> str:
        """Zarr group path emitted by ``emit_template`` (e.g. ``'12'``)."""
        return str(self.child_order)

    # ── OutputGrid protocol ──────────────────────────────────────────────

    def coverage(self, polygon_parts):
        """Enumerate parent morton IDs covering multipart polygons."""
        from mortie import morton_coverage

        lats_parts = [p[0] for p in polygon_parts]
        lons_parts = [p[1] for p in polygon_parts]
        return morton_coverage(lats_parts, lons_parts, order=self.parent_order)

    def assign(self, lats, lons) -> np.ndarray:
        """Map (lat, lon) points to morton IDs at the HEALPix reference order.

        Returns morton at order 18 — mortie's ``clip2order`` requires its
        input at that fixed reference order to clip correctly.
        """
        from mortie import geo2mort

        return geo2mort(lats, lons, order=HEALPIX_REF_ORDER)

    def shards_of(self, leaf_ids) -> np.ndarray:
        """Vectorized parent-morton lookup. ``leaf_ids`` must be at order 18."""
        from mortie import clip2order

        return clip2order(self.parent_order, np.asarray(leaf_ids))

    def cells_of(self, leaf_ids) -> np.ndarray:
        """Coarsen order-18 leaf morton IDs to ``child_order`` cell IDs."""
        from mortie import clip2order

        return clip2order(self.child_order, np.asarray(leaf_ids))

    def shard_of(self, leaf_ids) -> int:
        """Assert all cells share a parent and return that parent morton ID."""
        parents = self.shards_of(leaf_ids)
        first = int(parents.flat[0])
        if not np.all(parents == first):
            raise InconsistentShardError(
                f"cells span multiple shards at parent_order={self.parent_order}"
            )
        return first

    def block_index(self, shard_key) -> tuple[int, ...]:
        """Storage block index for this parent morton ID.

        For fullsphere layout, returns the parent's HEALPix nested cell ID
        (chunks are keyed by parent nested-ID, not by morton — morton is
        sparse/1-4-digit while nested-ID is contiguous in ``[0, 12·4^p)``).
        For dense layout, returns the position in ``populated_shards``.
        """
        if self.layout == "fullsphere":
            from mortie import mort2healpix

            healpix, _ = mort2healpix(np.asarray([int(shard_key)]))
            return (int(healpix[0]),)
        if self._position_map is None:
            raise RuntimeError(
                "block_index requires set_populated_shards() for dense layout"
            )
        return (self._position_map[int(shard_key)],)

    def shard_footprint(self, shard_key):
        """Parent-cell polygon in WGS84 (lon, lat)."""
        from mortie.tools import mort2polygon
        from shapely.geometry import Polygon

        verts = mort2polygon(int(shard_key), step=32)
        lats = np.array([v[0] for v in verts])
        lons = np.array([v[1] for v in verts])
        return Polygon(zip(lons, lats))

    def children(self, shard_key) -> np.ndarray:
        """Child morton IDs under a parent, in canonical order."""
        from mortie import generate_morton_children

        return generate_morton_children(int(shard_key), self.child_order)

    def encode_cell_ids(self, leaf_ids) -> np.ndarray:
        """Convert morton IDs to HEALPix nested cell IDs."""
        from mortie import mort2healpix

        cell_ids, _ = mort2healpix(leaf_ids)
        return cell_ids

    def chunk_coords(self, shard_key) -> dict:
        """Per-cell coord columns for HEALPix: ``morton`` and ``cell_ids``."""
        children = self.children(shard_key)
        return {"morton": children, "cell_ids": self.encode_cell_ids(children)}

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write the Zarr template (group + arrays) to ``store``."""
        spec = self._spec()
        with zarr_config.set({"async.concurrency": 128}):
            spec.to_zarr(store, self.group_path, overwrite=overwrite)
        return store

    def spec(self) -> GroupSpec:
        """Return the pydantic-zarr GroupSpec for this grid's template."""
        return self._spec()

    # ── internals ────────────────────────────────────────────────────────

    def _spec(self) -> GroupSpec:
        if self.layout == "fullsphere":
            n_pixels = HEALPIX_BASE_CELLS * (4**self.child_order)
        else:
            n_pixels = self.n_children * self.n_shards

        base = ArraySpec(
            attributes={},
            shape=(n_pixels,),
            dimension_names=("cells",),
            data_type="float32",
            chunk_grid=NamedConfig(
                name="regular", configuration={"chunk_shape": (self.n_children,)}
            ),
            chunk_key_encoding=NamedConfig(
                name="default", configuration={"separator": "/"}
            ),
            codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
            storage_transformers=(),
            fill_value="NaN",
        )

        members = {}
        for name, meta in self.config.aggregation.get("coordinates", {}).items():
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            members[name] = base.with_data_type(dtype).with_fill_value(fill)
        for name, meta in get_agg_fields(self.config).items():
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            members[name] = base.with_data_type(dtype).with_fill_value(fill)

        return GroupSpec(members=members, attributes=self._dggs_attrs())

    def _dggs_attrs(self) -> dict:
        return {
            "zarr_conventions": [
                {
                    "schema_url": "https://raw.githubusercontent.com/zarr-conventions/dggs/refs/tags/v1/schema.json",
                    "spec_url": "https://github.com/zarr-conventions/dggs/blob/v1/README.md",
                    "uuid": "7b255807-140c-42ca-97f6-7a1cfecdbc38",
                    "name": "dggs",
                    "description": "Discrete Global Grid Systems convention for zarr",
                }
            ],
            "dggs": {
                "name": "healpix",
                "refinement_level": self.child_order,
                "indexing_scheme": "nested",
                "spatial_dimension": "cells",
                "ellipsoid": {
                    "name": "WGS84",
                    "semimajor_axis": 6378137.0,
                    "inverse_flattening": 298.257223563,
                },
                "coordinate": "cell_ids",
                "compression": "none",
            },
        }


__all__ = ["HealpixGrid", "HEALPIX_BASE_CELLS"]
