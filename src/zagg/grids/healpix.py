"""HEALPix DGGS output grid via mortie."""

from __future__ import annotations

from typing import Literal

import numpy as np
from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig
from zarr import config as zarr_config
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    default_config,
    get_agg_fields,
    get_aoi_mask,
    get_cell_ids_encoding,
    get_emit_cell_ids,
    get_output_signature,
    output_field_signature,
)
from zagg.grids.base import (
    InconsistentShardError,
    chunk_array_spec,
    ragged_array_spec,
    ragged_locations_name,
    sharded_array_spec,
    vector_array_spec,
    vlen_dtype_warning_suppressed,
)
from zagg.grids.morton import (
    MORTON_CONVENTION,
    RESOLUTION_EXACT,
    morton_decimal,
    to_morton_array,
)

HEALPIX_BASE_CELLS: int = 12
# Reference order at which ``assign`` resolves points before ``cells_of`` /
# ``shards_of`` coarsen down to ``child_order`` / ``parent_order``. It must be
# >= the finest ``child_order`` any grid uses, or that resolution is silently
# lost (``clip2order`` cannot refine past its input order). mortie 0.8.1 supports
# orders up to 29, so this is the cap. Coarsening from a deeper reference is
# byte-identical for any order <= the old value, so raising it leaves existing
# grids' cell/shard assignments unchanged (verified for the shipped configs).
HEALPIX_REF_ORDER: int = 29


class HealpixGrid:
    """HEALPix DGGS output grid.

    One array layout — ``"fullsphere"``: shape ``(12 · 4^child_order,)``,
    chunks indexed directly by parent nested ID. Stateless; no shard list
    needed. (The deprecated ``"dense"`` pack was removed — issue #88.)

    Parameters
    ----------
    parent_order : int
        Shard (chunk) order.
    child_order : int
        Leaf cell order.
    layout : {"fullsphere"}, optional
        Storage layout. ``"fullsphere"`` is the only (and default) value.
    config : PipelineConfig, optional
        Aggregation schema. Defaults to the built-in atl06 config.
    """

    def __init__(
        self,
        parent_order: int,
        child_order: int,
        layout: Literal["fullsphere"] = "fullsphere",
        config: PipelineConfig | None = None,
        chunk_inner: int | None = None,
        sharded: bool = True,
    ):
        if child_order < parent_order:
            raise ValueError(
                f"child_order ({child_order}) must be >= parent_order ({parent_order})"
            )
        if layout != "fullsphere":
            raise ValueError(f"Unknown layout: {layout!r} (expected 'fullsphere')")
        # chunk_inner (issue #30 item 3): an optional finer ZARR-chunk order between
        # the shard order (parent_order) and the cell order (child_order). One shard
        # (the dispatch unit) then owns K = 4^(chunk_order - parent_order) chunks.
        # HEALPix specs this in its native unit — an order — so orders nest
        # automatically (no extra check beyond the bounds below). Default
        # ``chunk_inner is None`` means chunk_order == parent_order (K == 1), i.e.
        # shard == chunk, byte-identical to the pre-item-3 grid.
        chunk_order = parent_order if chunk_inner is None else int(chunk_inner)
        if not (parent_order <= chunk_order <= child_order):
            raise ValueError(
                f"chunk_inner order ({chunk_order}) must satisfy parent_order "
                f"({parent_order}) <= chunk_inner <= child_order ({child_order})"
            )
        self.parent_order = parent_order
        self.child_order = child_order
        self.chunk_order = chunk_order
        self.chunk_inner = chunk_inner
        self.level_diff = child_order - parent_order
        self.n_children = 4**self.level_diff
        # Cells per ZARR chunk (== n_children when chunk_inner is unset) and the
        # number of chunks one shard owns (K, == 1 when unset).
        self.cells_per_chunk = 4 ** (child_order - chunk_order)
        self.chunks_per_shard = 4 ** (chunk_order - parent_order)
        # Cells per dispatch shard == the whole shard's leaf extent (n_children).
        # When sharded (issue #108), this is the zarr SHARD shape: the K inner
        # chunks (each ``cells_per_chunk``) bundle into one shard object.
        self.cells_per_shard = self.n_children
        # Sharded storage (issue #108): bundle the shard's K inner chunks into one
        # zarr ShardingCodec shard object instead of K independent regular chunk
        # objects. Only meaningful when K > 1 (a finer ``chunk_inner`` gives the
        # shard multiple inner chunks). ``sharded`` defaults True (issue #215 — a
        # missing flag should not silently cost the ~K-fold object blow-up), so a
        # K==1 shard (``chunk_inner`` unset, or no finer than ``parent_order``) has
        # nothing to bundle: sharding is a no-op there and is silently disabled,
        # leaving single-chunk grids byte-identical to a pre-#215 unsharded write.
        # HEALPix lands first (issue #108).
        if self.chunks_per_shard <= 1:
            sharded = False
        self.sharded = bool(sharded)
        self.layout = layout
        self.config = config or default_config("atl06")
        # cell_ids coordinate encoding (issue #135): "nested" (default, the DGGS
        # standard) or "morton" (emit the packed morton words as cell_ids — a
        # test/prototype capability). Default is byte-identical to a pre-flag run.
        # Re-validated here (not only in validate_config) because both coords_of
        # (the cell_ids values) and _dggs_attrs (the recorded indexing_scheme)
        # interpret this string: an unvalidated third value would write NESTED
        # values while recording a different scheme — a mis-decode for consumers.
        self.cell_ids_encoding = get_cell_ids_encoding(self.config)
        if self.cell_ids_encoding not in ("nested", "morton"):
            raise ValueError(
                f"Unknown cell_ids_encoding: {self.cell_ids_encoding!r} "
                "(expected 'nested' or 'morton')"
            )
        # D16 default flip (issue #304 phase 2): the legacy cell_ids array is
        # written only under the explicit transition hatch; morton is the one
        # stored cell coordinate. Both the template member (_group_spec) and
        # the coords column (coords_of) key off this single flag so template
        # and writes can never disagree.
        self.emit_cell_ids = get_emit_cell_ids(self.config)

    @property
    def n_shards(self) -> int:
        """Number of shards in the storage layout."""
        return HEALPIX_BASE_CELLS * (4**self.parent_order)

    @property
    def array_shape(self) -> tuple[int, ...]:
        return (HEALPIX_BASE_CELLS * (4**self.child_order),)

    @property
    def chunk_shape(self) -> tuple[int, ...]:
        """ZARR chunk shape — cells per chunk.

        Equals ``n_children`` (one chunk == one shard) unless ``chunk_inner`` set a
        finer chunk order (issue #30 item 3), in which case it is the smaller
        ``cells_per_chunk = 4^(child_order - chunk_order)``.
        """
        return (self.cells_per_chunk,)

    @property
    def chunk_grid_shape(self) -> tuple[int, ...]:
        """Number of chunks (``array_shape // chunk_shape``) for companion arrays.

        A ``resolution: chunk`` field (issue #30 item 2) stores one value per
        chunk here. Equals ``12·4^chunk_order`` (``== 12·4^parent_order`` when
        ``chunk_inner`` is unset).

        Indexing: at K==1 (``chunk_inner`` unset) the per-chunk index IS
        :meth:`block_index` (one chunk per shard). At K>1 (issue #30 item 3) the
        companion is sized at the finer ``chunk_order`` grid, so the correct
        per-chunk index is the block yielded by :meth:`iter_chunks` — NOT
        ``block_index(shard_key)``, which is the coarser parent-order index of the
        whole shard. The K>1 writer must index by ``iter_chunks``.
        """
        return (self.array_shape[0] // self.cells_per_chunk,)

    def iter_chunks(self, shard_key):
        """Yield ``(chunk_block_index, chunk_children)`` for each chunk in a shard.

        Item 3 (issue #30): one shard (the dispatch unit) owns
        ``K = chunks_per_shard`` finer ZARR chunks. The worker reads the shard's
        granules once, then emits one chunk region + one companion slice per chunk.
        Each yielded ``chunk_block_index`` is the storage block tuple for that chunk
        (as :meth:`block_index` returns for a shard when ``K == 1``) and
        ``chunk_children`` are its cell ids in canonical order.

        When ``chunk_inner`` is unset (``K == 1``) this yields exactly one entry —
        ``(block_index(shard_key), children(shard_key))`` — so the single-chunk
        worker path is byte-identical.

        Order note: at K>1 the union of the per-chunk ``chunk_children`` equals the
        shard's :meth:`children` as a SET, but the concatenation order does NOT match
        ``children(shard_key)`` (each chunk is enumerated in its own canonical order).
        The writer must place each chunk's cells against its own ``block`` region, not
        assume a shard-wide ordering.
        """
        if self.chunks_per_shard == 1:
            yield (self.block_index(shard_key), self.children(shard_key))
            return
        from mortie import generate_morton_children, mort2healpix

        # The shard's K sub-chunks are its morton children at chunk_order; each
        # sub-chunk's block index is its own nested-cell id (fullsphere only).
        sub_chunks = generate_morton_children(int(shard_key), self.chunk_order)
        for sub in np.asarray(sub_chunks):
            healpix, _ = mort2healpix(np.asarray([int(sub)]))
            block = (int(healpix[0]),)
            children = generate_morton_children(int(sub), self.child_order)
            yield (block, children)

    def shard_slab_shape(self) -> tuple[int, ...]:
        """Cell extent of one whole sharded shard (issue #108).

        The shape the sharded worker assembles per dense array before the single
        whole-shard ``set_block_selection`` — ``(cells_per_shard,)`` for HEALPix
        (the K inner chunks laid out contiguously, matching the ShardingCodec's
        inner-chunk tiling of the outer shard).
        """
        return (self.cells_per_shard,)

    def shard_local_region(self, block_index, shard_key) -> tuple:
        """Slice(s) an inner chunk occupies within its shard slab (issue #108).

        ``block_index`` is the inner chunk's global block (from :meth:`iter_chunks`);
        its position inside the shard is ``local = block - block_index(shard)·K``,
        and it occupies the contiguous ``[local·cells_per_chunk, +cells_per_chunk)``
        run of the ``cells_per_shard`` slab (HEALPix nested ids tile the shard in
        ascending order, so this matches the carrier's canonical chunk order).
        """
        (shard_block,) = self.block_index(shard_key)
        (inner_block,) = tuple(int(b) for b in block_index)
        local = inner_block - shard_block * self.chunks_per_shard
        start = local * self.cells_per_chunk
        return (slice(start, start + self.cells_per_chunk),)

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

    # ── strict-AOI cell mask (issue #101, optional) ─────────────────────────

    def aoi_moc(self, aoi) -> np.ndarray:
        """Compact MOC of the AOI at ``child_order`` (native morton; issue #101).

        ``aoi`` is an :class:`~zagg.grids.aoi.AOIGeometry` (WKB/WKT or ``(lats,
        lons)`` ring parts) or, for back-compatibility, a bare parts list. WKB/WKT
        rides mortie's public ``from_wkb`` / ``from_wkt`` cover entry points and
        yields the identical MOC to the equivalent ring. Built once at the shard-map
        stage next to :meth:`coverage`; the per-shard slices (:meth:`aoi_shard_moc`)
        ride the manifest, expanded per worker via :meth:`aoi_mask_for_children`.
        """
        from zagg.grids.aoi import as_aoi_geometry, healpix_aoi_moc_from_geometry

        return healpix_aoi_moc_from_geometry(as_aoi_geometry(aoi), self.child_order)

    def aoi_shard_moc(self, aoi_moc, shard_key) -> np.ndarray:
        """Restrict the AOI MOC to one shard (compact per-shard sub-MOC)."""
        from zagg.grids.aoi import healpix_shard_moc

        return healpix_shard_moc(aoi_moc, int(shard_key))

    def aoi_mask_for_children(self, shard_moc, children) -> np.ndarray:
        """Boolean over ``children`` — ``True`` where the cell is inside the AOI.

        ``shard_moc`` is the per-shard sub-MOC (from :meth:`aoi_shard_moc`),
        ``children`` the chunk's cell morton ids in canonical order; the result is
        already in cell/storage order, ready to ride as the ``aoi_mask`` column.
        """
        from zagg.grids.aoi import healpix_mask_for_children

        return healpix_mask_for_children(shard_moc, children, self.child_order)

    def aoi_mask_from_payload(self, payload, children) -> np.ndarray:
        """Expand a manifest per-shard payload to a per-cell bool over ``children``.

        For HEALPix the payload is the compact sub-MOC (uint64 words as ints), so
        this is :meth:`aoi_mask_for_children` over the chunk's cells. Used by the
        worker, which has only the JSON payload (no recompute) — see
        ``catalog.shardmap._compute_aoi_mask``.
        """
        shard_moc = np.asarray(payload, dtype=np.uint64)
        return self.aoi_mask_for_children(shard_moc, children)

    def assign(self, lats, lons) -> np.ndarray:
        """Map (lat, lon) points to morton IDs at the HEALPix reference order.

        Returns **point-kind** morton words at :data:`HEALPIX_REF_ORDER` (issue
        #87): mortie's ``Kind::Point`` encoding marks the word as a location of
        unknown extent rather than an order-29 area cell, so the per-observation
        ``leaf_id`` can feed a ``location`` channel (``common_ancestor`` preserves
        a lone point, an area cell would misreport its extent). Point and area
        words share the same path prefix — ``clip2order`` coarsening is
        bit-identical — so ``cells_of`` / ``shards_of`` (and every dense output)
        are unchanged. Encoding rides the numpy-level ``geo2mort(...,
        points=True)`` (mortie 0.8.5, espg/mortie#100 — the issue #87 phase-6
        surface, replacing the pandas ``MortonIndexArray`` wrapper + unwrap).
        """
        from mortie import geo2mort

        # Passing the order explicitly is a self-check: point encoding is
        # order-29-only, so mortie raises loudly if HEALPIX_REF_ORDER ever drifts.
        return geo2mort(lats, lons, order=HEALPIX_REF_ORDER, points=True)

    def cell_centers(self, cells) -> tuple[np.ndarray, np.ndarray]:
        """Cell-center ``(lats, lons)`` (WGS84 degrees) for morton cell ids."""
        from mortie import mort2geo

        lats, lons = mort2geo(np.asarray(cells))
        return np.asarray(lats), np.asarray(lons)

    def cell_lonlat(self, cells) -> tuple[np.ndarray, np.ndarray]:
        """Cell-center ``(lons, lats)`` (WGS84, always_xy order) — grid-agnostic
        counterpart to :meth:`cell_centers` for consumers that must not care
        which family the grid is (e.g. the raster ownership rule, #218)."""
        lats, lons = self.cell_centers(cells)
        return lons, lats

    def sample(self, cells, crs, transform, shape):
        """Nearest source-pixel ``(rows, cols, valid)`` for cell centers (#218).

        Pull-NN: order-agnostic (``cells`` may be at any order — ``children()``
        output, or finer/coarser), dense by construction over the raster's
        footprint.
        """
        from zagg.grids.base import sample_nearest

        lats, lons = self.cell_centers(cells)
        return sample_nearest(lons, lats, "EPSG:4326", crs, transform, shape)

    def shards_of(self, leaf_ids) -> np.ndarray:
        """Vectorized parent-morton lookup. ``leaf_ids`` at :data:`HEALPIX_REF_ORDER`."""
        from mortie import clip2order

        return clip2order(self.parent_order, np.asarray(leaf_ids))

    def cells_of(self, leaf_ids) -> np.ndarray:
        """Coarsen reference-order leaf morton IDs to ``child_order`` cell IDs."""
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

        Returns the parent's HEALPix nested cell ID (chunks are keyed by
        parent nested-ID, not by morton — morton is sparse/1-4-digit while
        nested-ID is contiguous in ``[0, 12·4^p)``).
        """
        from mortie import mort2healpix

        healpix, _ = mort2healpix(np.asarray([int(shard_key)]))
        return (int(healpix[0]),)

    def shard_label(self, shard_key) -> str:
        """Decimal morton string for this shard's packed word (issue #199).

        The external form of a HEALPix shard id (D1 in
        ``docs/design/sparse_coverage.md``): hive leaf ids, ``.status``
        object keys, and log lines all carry e.g. ``-31123``, never the raw
        packed-word integer.
        """
        return morton_decimal(shard_key)

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
        """Per-cell coord columns for HEALPix: ``morton`` (plus legacy ``cell_ids``).

        ``morton`` is a mortie ``MortonIndexArray`` (the typed coordinate; #71);
        it is stored as ``uint64`` on disk via :mod:`zagg.grids.morton` and is
        the ONLY cell coordinate written by default (D16, issue #304). The
        ``output.grid.emit_cell_ids: true`` transition hatch adds the legacy
        ``cell_ids`` array — NESTED ``uint64``, or the packed morton words under
        ``cell_ids_encoding: morton`` (issue #135).
        """
        return self.coords_of(self.children(shard_key))

    def coords_of(self, children) -> dict:
        """Per-cell coord columns for an explicit ``children`` array.

        The chunk-resolution variant of :meth:`chunk_coords`: at K>1 (issue #30
        item 3) a worker writes one carrier per finer chunk, whose cells are the
        chunk's own ``children`` (from :meth:`iter_chunks`), not the whole shard's.
        ``chunk_coords`` is just ``coords_of(children(shard_key))``.
        """
        children = np.asarray(children)
        coords: dict = {"morton": to_morton_array(children)}
        if self.emit_cell_ids:
            if self.cell_ids_encoding == "morton":
                coords["cell_ids"] = np.asarray(children, dtype=np.uint64)
            else:
                coords["cell_ids"] = self.encode_cell_ids(children)
        return coords

    # ── identity / nesting ───────────────────────────────────────────────

    def spatial_signature(self) -> dict:
        """Structural (spatial-only) fingerprint of the grid.

        The shard-map reuse guard (``runner._check_signature``, #89) compares
        this — it is purely the spatial layout (no ``output_fields``), so one
        ShardMap is reusable across configs that share the spatial grid but
        declare different aggregation fields.
        """
        return {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": self.parent_order,
            "child_order": self.child_order,
            "layout": self.layout,
        }

    def signature(self) -> dict:
        """Canonical fingerprint of the grid's defining parameters.

        The full fingerprint: the spatial layout (:meth:`spatial_signature`)
        plus the Option-B output-field set and the ``cell_ids_encoding``
        (issue #135 — mixed encodings must never co-aggregate; ``nests_with``
        keys on both). The shard-map reuse guard keys on the spatial part
        only (#89).
        """
        return {
            **self.spatial_signature(),
            "output_fields": output_field_signature(self.config),
            "cell_ids_encoding": self.cell_ids_encoding,
            # The transition hatch (issue #304 phase 2) changes the leaf
            # schema (an extra cell_ids array), so it is part of the full
            # fingerprint — mixed hatch states must never co-aggregate.
            "emit_cell_ids": self.emit_cell_ids,
        }

    def nests_with(self, other) -> bool:
        """Whether ``self`` and ``other`` tile compatibly.

        Any two HEALPix grids nest (the nested hierarchy subdivides 4-for-1 at
        every order), provided they declare the same Option-B output-field set
        (issue #29) — co-aggregated grids must produce the same scalar/vector
        schema — and the same ``cell_ids_encoding`` (issue #135): NESTED ids
        and morton words are different id spaces, so a consumer joining
        co-aggregated products on ``cell_ids`` would silently mismatch.
        Cross-family (e.g. rectilinear) never nests.
        """
        if not isinstance(other, HealpixGrid):
            return False
        if self.cell_ids_encoding != other.cell_ids_encoding:
            return False
        if self.emit_cell_ids != other.emit_cell_ids:
            # The hatch changes the leaf schema (issue #304 phase 2): one
            # store carries a cell_ids array, the other does not.
            return False
        return output_field_signature(self.config) == output_field_signature(other.config)

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write the Zarr template (group + arrays) to ``store``."""
        spec = self._spec()
        # Ragged vlen-array creation warns about the dtype NAME only
        # (zarr-python#3517); message-scoped suppression, see grids.base.
        with zarr_config.set({"async.concurrency": 128}), vlen_dtype_warning_suppressed():
            spec.to_zarr(store, self.group_path, overwrite=overwrite)
        return store

    def emit_shard_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write ONE shard's leaf-zarr template to ``store`` (issue #199 phase 2).

        The hive layout (D3 in ``docs/design/sparse_coverage.md``) gives every
        shard its own self-describing leaf zarr: the same group structure as
        :meth:`emit_template` but with every dense array sized to a single
        shard (``cells_per_shard`` cells, K inner chunks) and a ROOT group so
        the D4 commit stamp is one attrs update on an object that exists
        anyway. Writes go at leaf-LOCAL block indices (0..K-1); when
        ``sharded`` (issue #236) each dense array is instead ONE ShardingCodec
        object spanning the whole leaf, written at leaf block 0 — the
        ShardingCodec is vanilla zarr v3, so the leaf stays self-describing
        (D3), same as the ragged field's sharded vlen array (issue #209).
        """
        spec = GroupSpec(members={self.group_path: self.shard_spec()}, attributes={})
        # Ragged vlen-array creation warns about the dtype NAME only
        # (zarr-python#3517); message-scoped suppression, see grids.base.
        with zarr_config.set({"async.concurrency": 128}), vlen_dtype_warning_suppressed():
            spec.to_zarr(store, "", overwrite=overwrite)
        return store

    def spec(self) -> GroupSpec:
        """Return the pydantic-zarr GroupSpec for this grid's template."""
        return self._spec()

    def shard_spec(self) -> GroupSpec:
        """GroupSpec for ONE shard's hive leaf (issue #199 phase 2).

        Identical member set to :meth:`spec` — same dtypes, fills, chunking —
        with the cells axis sized to one shard and the ``resolution: chunk``
        companions sized to the shard's K inner chunks. The one deliberate
        difference (``leaf=True``): sharded arrays shard across the WHOLE
        leaf — a ragged field's vlen array always (issue #209), the dense
        per-cell arrays when ``sharded`` (issue #236) — so each is ONE object
        per leaf.
        """
        return self._group_spec(self.cells_per_shard, (self.chunks_per_shard,), leaf=True)

    # ── internals ────────────────────────────────────────────────────────

    def _spec(self) -> GroupSpec:
        n_pixels = HEALPIX_BASE_CELLS * (4**self.child_order)
        return self._group_spec(n_pixels, self.chunk_grid_shape)

    def _group_spec(
        self, n_pixels: int, chunk_grid_shape: tuple[int, ...], *, leaf: bool = False
    ) -> GroupSpec:
        base = ArraySpec(
            attributes={},
            shape=(n_pixels,),
            dimension_names=("cells",),
            data_type="float32",
            chunk_grid=NamedConfig(
                name="regular", configuration={"chunk_shape": (self.cells_per_chunk,)}
            ),
            chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
            codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
            storage_transformers=(),
            fill_value="NaN",
        )

        # Sharded storage (issue #108): wrap each dense per-cell array in a
        # ShardingCodec — outer (shard) chunk == ``cells_per_shard``, inner chunk ==
        # ``cells_per_chunk`` (the 64x64 read chunk). Applied to the per-cell
        # coord/data-var arrays only; the ``resolution: chunk`` companions stay
        # regular (they are already one block per chunk on the coarse chunk grid).
        def _shard(arr):
            if not self.sharded:
                return arr
            # The inner chunk is the array's current chunk shape (``cells_per_chunk``
            # on the cells axis; a vector field's trailing payload dim is chunked
            # whole and stays whole). The ShardingCodec OUTER chunk widens only the
            # cells axis to the whole dispatch shard (== the whole leaf on hive,
            # issue #236); the inner read chunk is unchanged.
            cg = arr.chunk_grid
            cfg = cg["configuration"] if isinstance(cg, dict) else cg.configuration
            inner = tuple(int(c) for c in cfg["chunk_shape"])
            shard = (self.cells_per_shard, *inner[1:])
            return sharded_array_spec(arr, shard_shape=shard, inner_chunk_shape=inner)

        members = {}
        for name, meta in self.config.aggregation.get("coordinates", {}).items():
            # D16 default flip (issue #304 phase 2): a declared cell_ids
            # coordinate is honored only under the emit_cell_ids transition
            # hatch — otherwise the member is skipped so the template never
            # carries an array the writer will not fill. Keyed off the same
            # flag as coords_of, so template and writes cannot disagree.
            if name == "cell_ids" and not self.emit_cell_ids:
                continue
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            members[name] = _shard(base.with_data_type(dtype).with_fill_value(fill))
        if self.emit_cell_ids and "cell_ids" not in members:
            # The hatch must not depend on the config still DECLARING the
            # legacy coordinate (phase 3 removes the shipped declarations):
            # synthesize the standard uint64/fill-0 member.
            members["cell_ids"] = _shard(base.with_data_type("uint64").with_fill_value(0))
        # Optional strict-AOI cell mask (issue #101): a bool array aligned to the
        # cell grid, emitted only when ``output.aoi_mask`` is on so off-runs stay
        # byte-identical. fill_value False — cells never written (out-of-AOI shards,
        # or cells the worker leaves untouched) read as not-in-AOI.
        if get_aoi_mask(self.config):
            members["aoi_mask"] = _shard(base.with_data_type("bool").with_fill_value(False))
        for name, meta in get_agg_fields(self.config).items():
            sig = get_output_signature(meta)
            # Ragged fields (issue #48) are ONE vlen-bytes array on the cell
            # grid (issue #209 — the sharded vlen-bytes layout replacing the
            # per-inner-chunk CSR subgroups). At ``resolution: chunk`` the
            # array sits on the chunk grid instead (one payload per chunk, the
            # ragged analogue of the scalar/vector companions). A located
            # field (issue #87) adds a sibling uint64 vlen array.
            if sig["kind"] == "ragged":
                if sig["resolution"] == "chunk":
                    rag_kw: dict = {
                        "shape": chunk_grid_shape,
                        "dims": ("chunks",),
                        "inner_chunk_shape": (1,),
                    }
                else:
                    # A hive LEAF shards the ragged field across the whole
                    # leaf (sharded or not) so a shard's digest is ONE object
                    # (issue #209) — same whole-leaf geometry the dense arrays
                    # take when ``sharded`` (issue #236). On the FLAT layout
                    # the ShardingCodec outer chunk mirrors the dense arrays
                    # when ``sharded``; the unsharded flat layout stays a
                    # regular array (one object per inner chunk — the
                    # streaming per-chunk write must not read-modify-write a
                    # shared shard object).
                    if self.sharded or (leaf and self.chunks_per_shard > 1):
                        rag_shard: tuple | None = (self.cells_per_shard,)
                    else:
                        rag_shard = None
                    rag_kw = {
                        "shape": (n_pixels,),
                        "dims": ("cells",),
                        "inner_chunk_shape": (self.cells_per_chunk,),
                        "shard_shape": rag_shard,
                    }
                located = ragged_locations_name(name) if sig.get("location") else None
                members[name] = ragged_array_spec(
                    element_dtype=sig["dtype"] or "float32",
                    inner_shape=sig["inner_shape"],
                    locations=located,
                    **rag_kw,
                )
                if located:
                    members[located] = ragged_array_spec(element_dtype="uint64", **rag_kw)
                continue
            dtype = meta.get("dtype", "float32")
            fill = meta.get("fill_value", "NaN")
            spec = base.with_data_type(dtype).with_fill_value(fill)
            if sig["resolution"] == "chunk":
                # A resolution: chunk field (issues #30 item 2, #82) is stored once
                # per chunk in a companion array shaped at the chunk grid, indexed by
                # block_index (the parent nested cell id). Compose the two helpers:
                # chunk_array_spec sets the chunk-grid base, then vector_array_spec
                # appends the field's trailing_shape (chunked whole) for a vector
                # companion. A scalar/ragged field has an empty trailing_shape, so
                # vector_array_spec returns the chunk base unchanged.
                members[name] = vector_array_spec(
                    chunk_array_spec(
                        spec,
                        chunk_grid_shape=chunk_grid_shape,
                        chunk_dims=("chunks",),
                    ),
                    sig,
                    base_dims=("chunks",),
                    base_chunk_shape=(1,),
                )
                continue
            # A vector field (issue #29) gets a trailing payload dim chunked
            # whole; scalars are returned unchanged.
            members[name] = _shard(
                vector_array_spec(
                    spec,
                    sig,
                    base_dims=("cells",),
                    base_chunk_shape=(self.cells_per_chunk,),
                )
            )

        return GroupSpec(members=members, attributes=self._dggs_attrs())

    def _dggs_attrs(self) -> dict:
        dggs_entry = {
            "schema_url": "https://raw.githubusercontent.com/zarr-conventions/dggs/refs/tags/v1/schema.json",
            "spec_url": "https://github.com/zarr-conventions/dggs/blob/v1/README.md",
            "uuid": "7b255807-140c-42ca-97f6-7a1cfecdbc38",
            "name": "dggs",
            "description": "Discrete Global Grid Systems convention for zarr",
        }
        common = {
            "refinement_level": self.child_order,
            # O10 resolution discriminator (issue #305, espg-ratified):
            # grid-derived cell coordinates are "exact" by construction —
            # every id is a true cell at its encoded order. "point"
            # (RESOLUTION_POINT) is reserved for location-derived id
            # fields (raw lat/lon cast to order 29 — the temporal event
            # path), which the 29->24 clip rule on the mortie spec page
            # applies to; no zagg aggregation output ever emits it.
            "resolution": RESOLUTION_EXACT,
            "spatial_dimension": "cells",
            "ellipsoid": {
                "name": "WGS84",
                "semimajor_axis": 6378137.0,
                "inverse_flattening": 298.257223563,
            },
            "compression": "none",
        }
        if self.cell_ids_encoding == "morton":
            # D16 / issue #304 phase 1: a morton-declared store uses the
            # DISTINCT grid name "morton" with the typed `morton` coordinate —
            # never name: "healpix" + indexing_scheme: "morton", which a
            # scheme-blind reader silently misreads as NESTED (garbage
            # renders); an unknown grid name makes it hard-reject with a
            # diagnostic instead, and matches moczarr's xdggs registration.
            # The self-declared convention entry (issue #305; permanent UUID)
            # rides the zarr_conventions LIST alongside the generic dggs
            # entry, so a future upstream registry entry can coexist too.
            return {
                "zarr_conventions": [dggs_entry, dict(MORTON_CONVENTION)],
                "dggs": {"name": "morton", **common, "coordinate": "morton"},
            }
        return {
            "zarr_conventions": [dggs_entry],
            "dggs": {
                "name": "healpix",
                "indexing_scheme": "nested",
                **common,
                "coordinate": "cell_ids",
            },
        }


__all__ = ["HealpixGrid", "HEALPIX_BASE_CELLS"]
