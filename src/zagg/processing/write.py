"""Write-stage helpers for :mod:`zagg.processing` (split out of the monolithic
``processing.py`` for the §4 size limit; pure relocation, no behavior change).

Assembles the per-shard output carrier and writes it to the Zarr template
(including the ``resolution: chunk`` companion path). Depends only on
``config``/``grids`` — never on the read or aggregate stages — so the import DAG
stays acyclic.
"""

import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_output_signature,
)
from zagg.grids.base import ragged_locations_name
from zagg.grids.morton import is_morton_array, is_morton_arrow, morton_to_arrow, morton_words


def _arrow_column(block: np.ndarray, sig: dict):
    """Build the Arrow column for one agg field from its per-cell stats block.

    A scalar field's ``(n_cells,)`` block becomes a plain Arrow array (values
    byte-for-byte identical to the pandas carrier). A ``vector`` field's
    ``(n_cells, *trailing_shape)`` block becomes a ``FixedSizeList<C>`` column
    (``C = prod(trailing_shape)``), so every cell carries an identically-sized
    list. Keeping the vector path a list-carrier (rather than a bespoke 2-D
    column) is what lets the future ragged t-digest slot in as a variable-length
    ``List<FixedSizeList<2>>`` through the same seam (issue #29 Tier 2).

    The carrier is ``arro3-core`` (issue #130 path C): ~7 MB, zero required deps,
    importable inside the 250 MB Lambda gate — unlike pyarrow, whose Python
    bindings hard-link a ~100 MB unstrippable C++ core. pyarrow is no longer shipped
    in the layer or a core dep; it survives only in the off-Lambda ``catalog`` extra
    (``zagg.catalog``, via stac-geoparquet).
    """
    from arro3.core import Array, fixed_size_list_array

    if sig["kind"] != "vector":
        return Array.from_numpy(np.ascontiguousarray(block))
    width = int(np.prod(sig["trailing_shape"]))
    flat = np.ascontiguousarray(block).reshape(-1)
    return fixed_size_list_array(Array.from_numpy(flat), width)


def _build_output(
    stats_arrays,
    data_vars,
    agg_fields,
    grid,
    shard_key,
    use_arrow: bool,
    *,
    children=None,
    aoi_mask=None,
):
    """Assemble the per-shard output carrier from the per-cell stats blocks.

    Returns a ``pandas.DataFrame`` for a pure-scalar config (unchanged) or an
    ``arro3.core.Table`` when any ``vector`` field is present, in both cases with
    the data-variable columns followed by the grid's per-cell coord columns.

    On both carriers the ``morton`` coordinate is TYPED (issue #135): the pandas
    carrier holds the mortie ``MortonIndexArray`` (#71), and the arro3 carrier
    holds mortie's ``morton_index`` Arrow extension column
    (:func:`zagg.grids.morton.morton_to_arrow`). The type lives at the
    interchange layer only — ``_iter_carrier_columns`` extracts the packed
    ``uint64`` words at the write boundary, so the on-disk dtype is plain
    ``uint64`` either way.

    ``children`` (issue #30 item 3): when given, the coord columns are computed
    for that explicit chunk's cells via ``grid.coords_of(children)`` instead of the
    whole shard's ``grid.chunk_coords(shard_key)``. This is what lets the K>1 worker
    build one carrier per finer chunk. ``None`` (default) is the K==1 path — the
    coords are the shard's, byte-for-byte unchanged.

    ``aoi_mask`` (issue #101): when given, a per-cell ``bool`` array aligned to this
    chunk's cells is appended as the ``aoi_mask`` column (``True`` where the cell is
    inside the AOI). ``None`` (default, flag off) appends nothing, so the carrier is
    byte-for-byte unchanged.
    """
    coords = dict(
        grid.coords_of(children) if children is not None else grid.chunk_coords(shard_key)
    )
    if aoi_mask is not None:
        coords["aoi_mask"] = np.asarray(aoi_mask, dtype=bool)
    if not use_arrow:
        df_out = pd.DataFrame({var: stats_arrays[var] for var in data_vars})
        for col_name, vals in coords.items():
            df_out[col_name] = vals
        return df_out

    from arro3.core import Array, Table

    columns = {
        var: _arrow_column(stats_arrays[var], get_output_signature(agg_fields[var]))
        for var in data_vars
    }
    for col_name, vals in coords.items():
        # The typed morton coordinate crosses into the arro3 carrier as mortie's
        # ``morton_index`` extension column (issue #135) — zero-copy over the
        # PyCapsule interface, no pyarrow. The extension metadata lives at the
        # interchange layer only: the write boundary (``_iter_carrier_columns``)
        # extracts the packed uint64 words, so on-disk stays plain uint64 (#71).
        if is_morton_array(vals):
            columns[col_name] = morton_to_arrow(vals)
        else:
            columns[col_name] = Array.from_numpy(np.ascontiguousarray(np.asarray(vals)))
    return Table.from_pydict(columns)


def _carrier_empty(carrier) -> bool:
    """Whether a process_shard output carrier (DataFrame or Arrow table) is empty.

    The arro3 carrier is never built with 0 rows in practice — ``_build_output``
    sizes it to ``prod(chunk_shape) > 0`` cells, and a no-data shard returns an
    empty pandas DataFrame before any arro3 table is constructed (arro3 cannot build
    a 0-length array). The ``num_rows == 0`` arm is kept as a defensive parallel to
    the DataFrame ``.empty`` check, not a path the worker exercises.
    """
    if isinstance(carrier, pd.DataFrame):
        return carrier.empty
    return carrier.num_rows == 0


def write_dataframe_to_zarr(
    df_out,
    store: Store,
    *,
    grid,
    chunk_idx: tuple,
) -> Store:
    """Write a per-shard output carrier to an existing Zarr template.

    Parameters
    ----------
    df_out : pandas.DataFrame or arro3.core.Table
        Coordinate + data-variable columns. An ``arro3.core.Table`` is used when the
        config declares any ``vector`` field (issue #29): its ``FixedSizeList``
        columns carry the per-cell ``trailing_shape`` payload, written to a
        Zarr array with a trailing dimension. Cell count must equal
        ``prod(grid.chunk_shape)``; cells are in the grid's canonical chunk order
        (``grid.children(shard_key)``).
    store : Store
        Zarr-compatible store with the template already written.
    grid : OutputGrid
        Grid the data was aggregated against. Provides ``group_path`` and
        ``chunk_shape`` for routing the write.
    chunk_idx : tuple of int
        Storage block index for this shard, as returned by
        ``grid.block_index(shard_key)``.

    Returns
    -------
    Store
        The same store, with data written.
    """
    if _carrier_empty(df_out):
        return store

    expected_count = int(np.prod(grid.chunk_shape))
    n_cells = len(df_out) if isinstance(df_out, pd.DataFrame) else df_out.num_rows
    if n_cells != expected_count:
        raise ValueError(
            f"Expected {expected_count} rows for chunk_shape={grid.chunk_shape}, got {n_cells}"
        )

    # Fields declared ``resolution: chunk`` (issue #30 item 2) are written ONCE per
    # chunk into a companion array shaped at the chunk grid, not the per-cell array.
    # Read the set from the grid's config (every concrete grid carries ``config``).
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))

    chunk_idx = tuple(int(i) for i in chunk_idx)
    for name, values in _iter_carrier_columns(df_out):
        if name in chunk_res_fields:
            # resolution: chunk — the column must be chunk-uniform (every populated
            # cell carries the same chunk value), so collapse the CELL axis to the
            # single value and write it to the companion array's one-block-per-chunk
            # grid at ``chunk_idx`` (which IS ``grid.block_index(shard_key)``). One
            # value per chunk, no per-cell duplication on disk (issues #30 item 2, #82).
            # A vector companion keeps its ``trailing`` shape: the block is
            # (1, ..., 1, *trailing) and the trailing axis is one whole chunk, so the
            # block index appends ``0`` per trailing dim (issue #29's invariant).
            chunk_value = np.asarray(_chunk_uniform_value(name, values))
            trailing = chunk_value.shape
            block = chunk_value.reshape((1,) * len(chunk_idx) + trailing)
            block_idx = chunk_idx + (0,) * len(trailing)
            with config.set({"async.concurrency": 128}):
                array = open_array(
                    store,
                    path=f"{grid.group_path}/{name}",
                    zarr_format=3,
                    consolidated=False,
                )
                array.set_block_selection(block_idx, block)
            continue

        # Scalar columns reshape to the grid's chunk_shape; a vector column keeps
        # its trailing payload dim(s), so the block (and target array) is
        # (*chunk_shape, *trailing_shape). The cell count invariant is unchanged.
        #
        # Single-trailing-chunk invariant (issue #29): the template
        # (``grids.base.vector_array_spec``) chunks the trailing payload dim
        # *whole*, so the trailing block index is always 0 and a shard's payload
        # lands in one Zarr block via ``chunk_idx + (0,) * len(trailing)``.
        trailing = values.shape[1:]
        values = values.reshape((*grid.chunk_shape, *trailing))
        block_idx = chunk_idx + (0,) * len(trailing)
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store,
                path=f"{grid.group_path}/{name}",
                zarr_format=3,
                consolidated=False,
            )
            if trailing:
                # Enforce the single-trailing-chunk invariant: the target array's
                # trailing chunk must span the whole payload, or set_block_selection
                # at block 0 would silently write only part of it (issue #29).
                target_trailing_chunks = array.chunks[len(grid.chunk_shape) :]
                if target_trailing_chunks != trailing:
                    raise ValueError(
                        f"vector field {name!r}: trailing chunk "
                        f"{target_trailing_chunks} must equal trailing shape "
                        f"{trailing} (the payload dim must be one whole chunk)"
                    )
            array.set_block_selection(block_idx, values)

    return store


def write_shard_to_zarr(
    chunk_results: list,
    store: Store,
    *,
    grid,
    shard_key: int,
) -> Store:
    """Write a whole sharded shard in ONE block selection per dense array (issue #108).

    On a ``ShardingCodec`` array (``grid.sharded``) the K inner chunks of one
    dispatch shard live inside a single shard object, and ``set_block_selection``
    is **shard-granular** (one block == one shard). Writing the K inner chunks as K
    separate block selections would each trigger a read-modify-write on the *same*
    shard object — racy and slow. The worker already holds all K chunk carriers
    (it reads the shard's granules once), so this assembles them into one
    shard-wide slab per dense per-cell array and writes it at the shard block in a
    single call. Empty inner chunks are left as fill, so the ShardingCodec omits
    them from the shard index (sub-shard sparsity preserved inside the object).

    ``chunk_results`` is the worker's ``[(block_index, carrier, ragged), ...]`` —
    one entry per inner chunk (from ``grid.iter_chunks``). Each dense per-cell
    column (data vars + coords) is placed into the shard slab by the chunk's own
    region (``grid.shard_local_region(block_index, shard_key)``), reshaped to the
    inner chunk's cell shape (``grid.chunk_shape``) — grid-agnostic, so HEALPix
    (1-D) and rectilinear (2-D) share one path. Ragged fields ride the SAME
    per-object slab pass (issue #209): their vlen-bytes array shares the dense
    ShardingCodec geometry, so a shard's digests collapse to one object instead
    of the old ~K×7 per-inner-chunk CSR objects. Only the ``resolution: chunk``
    companions stay per inner chunk (their arrays are one block per chunk on the
    coarse chunk grid, never sharded).

    Parameters
    ----------
    chunk_results : list of (block_index, carrier, ragged)
        The worker's per-inner-chunk results for one dispatch shard.
    store : Store
        Zarr store with the (sharded) template already written.
    grid : OutputGrid
        Must be ``sharded`` with ``shard_slab_shape`` / ``shard_local_region``.
    shard_key : int
        The dispatch shard's key (HEALPix parent morton id / rect packed tile).

    Returns
    -------
    Store
    """
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))
    inner_shape = tuple(int(s) for s in grid.chunk_shape)

    # Sharding-object split (issue #133 phase 8): a ShardingCodec object normally
    # spans the whole dispatch shard, so the K inner chunks accumulate into ONE slab
    # written in one ``set_block_selection``. ``shard_order`` sizes the object SMALLER
    # than the dispatch shard, so the worker writes its region in per-object passes
    # (accumulate→write→free), bounding peak memory under the 2 GB cap. A grid without
    # the object-split methods (or at the default ``shard_order``) yields ONE object
    # spanning the whole shard — byte-identical to the pre-phase-8 single-object write.
    use_object_split = hasattr(grid, "shard_object_block") and hasattr(
        grid, "shard_object_slab_shape"
    )
    if use_object_split:
        slab_shape = tuple(int(s) for s in grid.shard_object_slab_shape())

        def _object_block(block_index):
            return tuple(int(i) for i in grid.shard_object_block(block_index))

        def _local_region(block_index):
            return grid.shard_object_local_region(block_index)
    else:
        slab_shape = tuple(int(s) for s in grid.shard_slab_shape())
        shard_block = tuple(int(i) for i in grid.block_index(shard_key))

        def _object_block(block_index):
            return shard_block

        def _local_region(block_index):
            return grid.shard_local_region(block_index, shard_key)

    # Group the dispatch shard's inner chunks by sharding object so each object is
    # accumulated and written independently; preserve encounter order for stable,
    # deterministic writes. Companions stay per inner chunk (unsharded); ragged
    # fields ride the SAME per-object slab pass as the dense arrays (issue #209 —
    # their vlen-bytes array shares the dense ShardingCodec geometry), so the
    # per-inner-chunk CSR fan-out (issues #142/#186) is gone.
    objects: dict = {}
    for block_index, carrier, ragged in chunk_results:
        if not _carrier_empty(carrier) or ragged:
            objects.setdefault(_object_block(block_index), []).append(
                (block_index, carrier, ragged)
            )
        # Companions for this inner chunk are not sharded: write them straight
        # through (one block per chunk) — a chunk-resolution ragged companion
        # included (its vlen array is one block per chunk on the chunk grid).
        if chunk_res_fields:
            _write_companion_columns(carrier, store, grid, block_index, chunk_res_fields)
            _write_ragged_companions(ragged, store, grid, block_index, chunk_res_fields)

    # One accumulate→write→free pass per sharding object: each holds at most one
    # object's slab resident at a time (the phase-8 memory bound).
    for obj_block, members in objects.items():
        slabs: dict = {}
        ragged_slabs: dict = {}
        for block_index, carrier, ragged in members:
            region = _local_region(block_index)
            for name, values in _iter_carrier_columns(carrier):
                if name in chunk_res_fields:
                    continue  # companion (resolution: chunk) — handled per chunk above
                values = np.asarray(values)
                trailing = values.shape[1:]
                if name not in slabs:
                    array = open_array(
                        store,
                        path=f"{grid.group_path}/{name}",
                        zarr_format=3,
                        consolidated=False,
                    )
                    fill = array.metadata.fill_value
                    slabs[name] = np.full((*slab_shape, *trailing), fill, dtype=values.dtype)
                slabs[name][region] = values.reshape((*inner_shape, *trailing))
            # Cell-resolution ragged fields: encode this chunk's payloads into
            # the object slab at the same region the dense columns use. Empty
            # cells keep the b"" fill, so an all-empty inner chunk is omitted
            # from the shard index (sub-shard sparsity, issue #209).
            _accumulate_ragged_slabs(
                ragged, ragged_slabs, region, grid, slab_shape, chunk_res_fields
            )

        # One block selection per dense array == one shard object write.
        for name, slab in slabs.items():
            trailing = slab.shape[len(slab_shape) :]
            block_idx = (*obj_block, *((0,) * len(trailing)))
            with config.set({"async.concurrency": 128}):
                array = open_array(
                    store,
                    path=f"{grid.group_path}/{name}",
                    zarr_format=3,
                    consolidated=False,
                )
                if trailing:
                    # Mirror write_dataframe_to_zarr's single-trailing-chunk invariant
                    # (issue #29): a vector field's trailing payload dim must be one whole
                    # (inner) chunk, or set_block_selection at trailing block 0 would
                    # silently write only part of it.
                    target_trailing_chunks = array.chunks[len(slab_shape) :]
                    if target_trailing_chunks != trailing:
                        raise ValueError(
                            f"vector field {name!r}: trailing chunk "
                            f"{target_trailing_chunks} must equal trailing shape "
                            f"{trailing} (the payload dim must be one whole chunk)"
                        )
                array.set_block_selection(block_idx, slab)

        # ONE object write per ragged field too (issue #209): the vlen-bytes
        # array shares the dense arrays' ShardingCodec geometry, so the whole
        # slab lands at the same object block — the ShardingCodec emits a
        # single object with its internal index in place of the old ~K×7
        # per-inner-chunk CSR objects.
        for name, slab in ragged_slabs.items():
            _set_ragged_block(store, grid, name, tuple(obj_block), slab)
    return store


def _write_companion_columns(carrier, store, grid, block_index, chunk_res_fields):
    """Write a chunk's ``resolution: chunk`` companion columns (unsharded path).

    A ``resolution: chunk`` companion array is shaped at the coarse chunk grid
    (one block per chunk), never sharded, so it keeps the per-chunk write even on
    the sharded path. Reuses the same chunk-uniform collapse + block placement as
    :func:`write_dataframe_to_zarr`'s companion branch.
    """
    block_idx = tuple(int(i) for i in block_index)
    for name, values in _iter_carrier_columns(carrier):
        if name not in chunk_res_fields:
            continue
        chunk_value = np.asarray(_chunk_uniform_value(name, values))
        trailing = chunk_value.shape
        block = chunk_value.reshape((1,) * len(block_idx) + trailing)
        target = block_idx + (0,) * len(trailing)
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store,
                path=f"{grid.group_path}/{name}",
                zarr_format=3,
                consolidated=False,
            )
            array.set_block_selection(target, block)


def write_ragged_to_zarr(
    ragged: dict,
    store: Store,
    *,
    grid,
    chunk_idx: tuple,
) -> Store:
    """Write one chunk's ``kind: ragged`` fields to their vlen-bytes arrays (#209).

    The sharded vlen-bytes layout (issue #209) replaces the per-inner-chunk CSR
    subgroups: each ragged field is ONE ``variable_length_bytes`` array on the
    cell grid (template: ``grids.base.ragged_array_spec``), and each populated
    cell's value is the raw little-endian bytes of its ``(n, *inner_shape)``
    payload — the interpretation recorded in the array's attrs
    (``grids.base.RAGGED_ELEMENT_ATTR``). Empty cells keep the ``b""`` fill, so
    a chunk with no ragged data leaves its inner chunk absent on disk.

    This is the per-chunk seam, mirroring :func:`write_dataframe_to_zarr`: it
    writes the chunk's cells at storage block ``chunk_idx``. It is used on the
    UNSHARDED per-chunk write paths (the runner / Lambda ``_write_chunk``
    streaming callback), where the ragged array is regular-chunked — one object
    per inner chunk, so per-chunk writes stay independent (no read-modify-write
    of a shared shard object). The sharded flat path bundles all K chunks in
    :func:`write_shard_to_zarr`; the hive leaf in
    :func:`write_ragged_leaf_to_zarr` — one object per shard on both.

    At **chunk resolution** (``resolution: chunk``, issue #82) a ragged field
    stores ONE payload per chunk: the populated cells collapse under the same
    chunk-uniform contract as scalar/vector companions, and the single payload's
    bytes land at the chunk's block of the chunk-grid companion array.

    Parameters
    ----------
    ragged : dict
        ``{field_name: (values_list, cell_ids)}`` as filled by ``process_shard``;
        ``cell_ids`` are positions in THIS chunk's ``children`` block. A located
        field (issue #87) arrives as ``(values_list, cell_ids, locations_list)``
        and additionally writes the sibling ``{field}_locations`` uint64 vlen
        array, row-aligned with the payload.
    store : Store
        Zarr store with the template (including the ragged arrays) present.
    grid : OutputGrid
        Provides ``group_path``/``chunk_shape`` (and ``config`` for the
        per-field dtype + resolution).
    chunk_idx : tuple of int
        The chunk's storage block index (same value handed to
        :func:`write_dataframe_to_zarr` for the dense columns).

    Returns
    -------
    Store
        The same store, with the chunk's ragged payloads written.
    """
    if not ragged:
        return store
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))
    chunk_idx = tuple(int(i) for i in chunk_idx)
    inner_shape = tuple(int(s) for s in grid.chunk_shape)
    _write_ragged_companions(ragged, store, grid, chunk_idx, chunk_res_fields)
    slabs: dict = {}
    _accumulate_ragged_slabs(
        ragged, slabs, tuple(slice(0, s) for s in inner_shape), grid, inner_shape, chunk_res_fields
    )
    for name, slab in slabs.items():
        _set_ragged_block(store, grid, name, chunk_idx, slab)
    return store


def write_ragged_leaf_to_zarr(ragged_chunks: list, store: Store, *, grid) -> Store:
    """Write a hive leaf's ragged fields in ONE array write each (issue #209).

    The hive counterpart of the sharded path's slab pass: ``ragged_chunks`` is
    ``[(local_block_index, ragged), ...]`` — one entry per streamed chunk, at
    leaf-LOCAL blocks (``hive.leaf_block_index``). The leaf template shards a
    ragged field's vlen array across the whole shard (``shard_spec``), so the K
    chunks accumulate into one shard-wide object slab written in a single call
    — the ShardingCodec emits ONE object per leaf in place of the per-chunk CSR
    subgroups (~7 objects per populated inner chunk). A per-chunk write here
    would read-modify-write that shared object K times, which is why the hive
    write path collects the (small) ragged payloads instead of streaming them.

    ``resolution: chunk`` ragged companions are written per chunk block (their
    array is one block per chunk, unsharded — same as the scalar/vector
    companions).
    """
    if not any(ragged for _block, ragged in ragged_chunks):
        return store
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))
    slab_shape = tuple(int(s) for s in grid.shard_slab_shape())
    inner_shape = tuple(int(s) for s in grid.chunk_shape)
    slabs: dict = {}
    for block, ragged in ragged_chunks:
        block = tuple(int(b) for b in block)
        _write_ragged_companions(ragged, store, grid, block, chunk_res_fields)
        region = tuple(slice(b * c, (b + 1) * c) for b, c in zip(block, inner_shape))
        _accumulate_ragged_slabs(ragged, slabs, region, grid, slab_shape, chunk_res_fields)
    for name, slab in slabs.items():
        _set_ragged_block(store, grid, name, (0,) * len(slab_shape), slab)
    return store


def _ragged_entry(entry) -> tuple:
    """Normalize a ragged sink entry to ``(values_list, cell_ids, locations_list)``.

    Located fields (issue #87) deliver the 3-tuple; unlocated fields keep the
    2-tuple contract (``locations_list`` is ``None``).
    """
    if len(entry) == 3:
        return entry
    values_list, cell_ids = entry
    return values_list, cell_ids, None


def _ragged_sig(name: str, grid) -> dict:
    """Output signature of a ragged field, tolerant of config-less stub grids."""
    agg_fields = get_agg_fields(grid.config) if getattr(grid, "config", None) else {}
    if name in agg_fields:
        return get_output_signature(agg_fields[name])
    return {"kind": "ragged", "inner_shape": (), "dtype": None}


def _ragged_payload_bytes(name: str, value, sig: dict) -> bytes:
    """Raw little-endian bytes of one cell's ragged payload (the vlen wire value).

    The payload must tile the field's declared ``inner_shape`` (the guard
    ``write_csr`` enforced structurally); the byte order is pinned little-endian
    so the stored value matches the ``RAGGED_ELEMENT_ATTR`` interpretation on
    any producer.
    """
    dtype = np.dtype(sig.get("dtype") or "float32").newbyteorder("<")
    arr = np.asarray(value, dtype=dtype)
    inner = sig.get("inner_shape") or ()
    width = int(np.prod(inner)) if inner else 1
    if arr.size % width:
        raise ValueError(
            f"ragged field {name!r}: cell payload of {arr.size} elements does not "
            f"tile inner_shape {tuple(inner)}"
        )
    return np.ascontiguousarray(arr).tobytes()


def _accumulate_ragged_slabs(ragged, slabs, region, grid, slab_shape, chunk_res_fields) -> None:
    """Encode one chunk's cell-resolution ragged payloads into the object slabs.

    ``slabs`` maps array name → object slab of ``slab_shape`` (created lazily,
    ``b""``-filled — the vlen fill, so untouched cells stay absent). The chunk's
    cells land in ``slabs[...][region]`` at their ``cell_ids`` position within
    the chunk (row-major over ``grid.chunk_shape``, grid-agnostic via
    ``np.unravel_index``). A located field (issue #87) fills the sibling
    ``{name}_locations`` slab row-aligned with the payload, with the same
    per-cell length contract ``write_csr`` enforced.
    """
    inner_shape = tuple(int(s) for s in grid.chunk_shape)
    for name, entry in (ragged or {}).items():
        if name in chunk_res_fields:
            continue  # chunk companion — written per chunk block
        values_list, cell_ids, locations_list = _ragged_entry(entry)
        if len(values_list) != len(cell_ids):
            raise ValueError(
                f"values_list (len {len(values_list)}) and cell_ids (len {len(cell_ids)}) "
                "must have the same length"
            )
        sig = _ragged_sig(name, grid)
        if name not in slabs:
            slabs[name] = np.full(slab_shape, b"", dtype=object)
        out = slabs[name][region]
        loc_out = None
        if locations_list is not None:
            if len(locations_list) != len(values_list):
                raise ValueError(
                    f"locations_list (len {len(locations_list)}) and values_list "
                    f"(len {len(values_list)}) must have the same length"
                )
            loc_name = ragged_locations_name(name)
            if loc_name not in slabs:
                slabs[loc_name] = np.full(slab_shape, b"", dtype=object)
            loc_out = slabs[loc_name][region]
        loc_sig = {"kind": "ragged", "inner_shape": (), "dtype": "uint64"}
        width = int(np.prod(sig.get("inner_shape") or ()))  # prod(()) == 1
        locs = locations_list if locations_list is not None else [None] * len(values_list)
        for cid, value, loc in zip(cell_ids, values_list, locs):
            payload = _ragged_payload_bytes(name, value, sig)
            pos = np.unravel_index(int(cid), inner_shape)
            if loc is not None and loc_out is not None:
                n_rows = np.asarray(value).size // width
                loc_arr = np.asarray(loc)
                if loc_arr.shape != (n_rows,):
                    raise ValueError(
                        f"locations for cell {int(cid)} of {name!r} have shape "
                        f"{loc_arr.shape}, expected ({n_rows},) to stay row-aligned "
                        f"with the payload"
                    )
                if n_rows:
                    loc_out[pos] = _ragged_payload_bytes(loc_name, loc_arr, loc_sig)
            if payload:
                out[pos] = payload


def _write_ragged_companions(ragged, store, grid, block_index, chunk_res_fields) -> None:
    """Write a chunk's ``resolution: chunk`` RAGGED companions (issue #82/#209).

    The vlen analogue of :func:`_write_companion_columns`: the populated cells
    collapse to the single chunk payload (chunk-uniform contract,
    :func:`_chunk_uniform_ragged`), whose bytes land at the chunk's block of
    the chunk-grid vlen array. ``location`` + ``resolution: chunk`` is rejected
    at config validation, but a config built without ``validate_config`` could
    still deliver a located triple here — fail loudly rather than silently
    dropping the location channel (issue #87).
    """
    if not chunk_res_fields:
        return
    block_idx = tuple(int(i) for i in block_index)
    for name, entry in (ragged or {}).items():
        if name not in chunk_res_fields:
            continue
        values_list, _cell_ids, locations_list = _ragged_entry(entry)
        if locations_list is not None:
            raise ValueError(
                f"ragged field {name!r} is resolution: chunk but carries a location "
                f"channel; located ragged fields are cell-resolution only"
            )
        chunk_payload = _chunk_uniform_ragged(name, values_list)
        if chunk_payload is None:
            continue  # whole chunk is fill — nothing to record
        block = np.full((1,) * len(block_idx), b"", dtype=object)
        block[(0,) * len(block_idx)] = _ragged_payload_bytes(
            name, chunk_payload, _ragged_sig(name, grid)
        )
        _set_ragged_block(store, grid, name, block_idx, block)


def _set_ragged_block(store, grid, name, block_idx, block) -> None:
    """One block-selection write into a ragged vlen array (issue #209)."""
    with config.set({"async.concurrency": 128}):
        array = open_array(
            store,
            path=f"{grid.group_path}/{name}",
            zarr_format=3,
            consolidated=False,
        )
        array.set_block_selection(tuple(block_idx), block)


def _chunk_uniform_ragged(name: str, values_list: list):
    """Collapse a ``resolution: chunk`` ragged field's per-cell payloads to one.

    A chunk-resolution ragged field stores ONE variable-length payload per chunk
    (issue #82), so every *populated* cell's payload must be identical — the same
    "raise if populated cells disagree" contract the scalar/vector chunk companions
    enforce (:func:`_chunk_uniform_value`), here over whole variable-length arrays.

    Returns the single chunk payload (a numpy array), or ``None`` when no cell is
    populated (the whole chunk is fill — nothing to record).

    Raises a clear error if the populated cells carry differing payloads — that
    means the field genuinely varies per cell and ``resolution: chunk`` is a
    misconfiguration (the per-cell values would otherwise be silently dropped).
    """
    populated = [np.asarray(v) for v in values_list if np.asarray(v).size > 0]
    if not populated:
        return None
    first = populated[0]
    # NaN-aware compare for floats (a NaN-bearing-but-uniform payload is accepted);
    # ``equal_nan`` is only valid for float dtypes, so gate on the dtype.
    use_equal_nan = np.issubdtype(first.dtype, np.floating)
    for other in populated[1:]:
        same_shape = other.shape == first.shape
        equal = same_shape and (
            np.array_equal(other, first, equal_nan=True)
            if use_equal_nan
            else np.array_equal(other, first)
        )
        if not equal:
            n_distinct = len({arr.tobytes() for arr in populated})
            raise ValueError(
                f"resolution: chunk ragged field {name!r} is not chunk-uniform: the "
                f"populated cells carry {n_distinct} distinct payloads; a "
                f"chunk-resolution field must reduce to one payload per chunk (use "
                f"resolution: cell for a per-cell ragged field)"
            )
    return first


def _chunk_resolution_fields(config: PipelineConfig | None) -> set[str]:
    """Names of agg fields declared ``resolution: chunk`` (issue #30 item 2).

    Empty for a config without any such field, so the writer's per-cell path is
    byte-for-byte unchanged for existing (cell-resolution-only) configs.
    """
    if config is None:
        return set()
    return {
        name
        for name, meta in get_agg_fields(config).items()
        if get_output_signature(meta)["resolution"] == "chunk"
    }


def _chunk_uniform_value(name: str, values: np.ndarray):
    """Collapse a ``resolution: chunk`` field's per-cell column to its single value.

    A chunk-resolution field stores ONE value per chunk (issue #30 item 2), so its
    per-cell column must be chunk-uniform: every *populated* cell carries the same
    chunk value. Empty cells (which carry the field's fill sentinel — ``NaN`` for a
    float field, or the bare-chunk-anchor when the expression is a bare precompute
    name) are ignored when selecting the representative value, so an empty cell 0 no
    longer poisons the companion write with ``NaN``.

    The column may be a scalar ``(n_cells,)`` array or a vector
    ``(n_cells, *trailing)`` block (issue #82): the CELL axis (axis 0) is collapsed,
    keeping the per-element ``trailing`` shape. Uniformity is checked per-element over
    the trailing axis — a populated cell is one whose vector is not all-NaN — and the
    returned value is a 0-d scalar (scalar field) or a ``trailing``-shaped array.

    Raises a clear error if the populated cells are NOT uniform — that means the
    field's ``expression`` genuinely varies per cell and ``resolution: chunk`` is a
    misconfiguration (the per-cell values would be silently dropped otherwise).
    """
    arr = np.asarray(values)
    # Reshape to (n_cells, *trailing): a scalar column is (n_cells,) -> trailing ().
    cells = arr.reshape(arr.shape[0], -1) if arr.ndim > 1 else arr.reshape(-1)
    trailing = arr.shape[1:]
    # Treat NaN as the empty/fill sentinel for float columns; integer columns have
    # no NaN, so every cell is "populated" and the uniformity check covers them all.
    # A vector cell is "empty" only when its whole vector is NaN (a partially-NaN
    # vector is a real chunk value, kept as-is).
    if np.issubdtype(arr.dtype, np.floating):
        if arr.ndim > 1:
            populated_mask = ~np.all(np.isnan(cells), axis=1)
        else:
            populated_mask = ~np.isnan(cells)
        populated = cells[populated_mask]
    else:
        populated = cells
    if populated.shape[0] == 0:
        # Whole chunk is fill (e.g. a vector-carrier shard with no chunk anchor):
        # nothing meaningful to record; fall back to the first cell's sentinel.
        return arr[0]
    first = populated[0]
    # Compare per-element over the trailing axis; for float columns NaN positions
    # must match too (so a NaN-bearing-but-uniform vector is accepted), which only
    # ``array_equal(equal_nan=True)`` allows — integer columns have no NaN.
    broadcast_first = np.broadcast_to(first, populated.shape)
    uniform = (
        np.array_equal(populated, broadcast_first, equal_nan=True)
        if np.issubdtype(arr.dtype, np.floating)
        else np.array_equal(populated, broadcast_first)
    )
    if not uniform:
        # Count distinct populated rows for the message. Map NaN to a sentinel
        # first so a uniform-but-NaN-bearing layout is not over-counted (NaN != NaN
        # would otherwise make every such row look distinct).
        keys = populated.reshape(populated.shape[0], -1)
        if np.issubdtype(arr.dtype, np.floating):
            keys = np.where(np.isnan(keys), np.float64(np.inf), keys.astype(np.float64))
        n_distinct = len({tuple(row) for row in keys})
        raise ValueError(
            f"resolution: chunk field {name!r} is not chunk-uniform: the populated "
            f"cells carry {n_distinct} distinct values; a chunk-resolution field must "
            f"reduce to one value per chunk (use resolution: cell for a per-cell field)"
        )
    return first.reshape(trailing) if trailing else first


def _iter_carrier_columns(carrier):
    """Yield ``(name, ndarray)`` for each column of a DataFrame or Arrow table.

    Scalar columns yield a 1-D array; a ``FixedSizeList<C>`` Arrow column yields a
    2-D ``(n_cells, C)`` array (the per-cell vector block), so the writer can map
    it onto the Zarr trailing payload dimension (issue #29). A typed morton
    column — ``MortonIndexArray`` on the pandas carrier (#71), the
    ``morton_index`` Arrow extension column on the arro3 carrier (issue #135) —
    yields its packed ``uint64`` words, the on-disk form.
    """
    if isinstance(carrier, pd.DataFrame):
        for name, series in carrier.items():
            values = series.values
            # The ``morton`` coordinate is carried as a mortie ``MortonIndexArray``
            # (#71); Zarr stores numpy dtypes, so extract its packed ``uint64``
            # words for the on-disk write (no .reshape on the extension array).
            if is_morton_array(values):
                values = morton_words(values)
            yield name, values
        return

    from arro3.core import list_flatten
    from mortie.arrow import import_c_array

    n_rows = carrier.num_rows
    for name in carrier.column_names:
        col = carrier.column(name).combine_chunks()
        # A typed morton column (mortie's ``morton_index`` extension type; issue
        # #135) is the arro3 mirror of the pandas MortonIndexArray branch above:
        # pull the packed uint64 words over the C Data Interface for the on-disk
        # write (Arrow nulls -> the all-zero sentinel), keeping the stored dtype
        # plain uint64 (#71).
        if is_morton_arrow(col):
            yield name, import_c_array(col)
            continue
        # arro3 marks a FixedSizeList by an integer ``list_size`` (None for scalar
        # types), so the per-cell width and the 2-D reshape mirror the pyarrow path
        # exactly — the numpy block written to Zarr is byte-for-byte unchanged.
        width = col.type.list_size
        if width is not None:
            flat = list_flatten(col).to_numpy()
            yield name, flat.reshape(n_rows, width)
        else:
            yield name, col.to_numpy()
