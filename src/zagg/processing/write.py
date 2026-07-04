"""Write-stage helpers for :mod:`zagg.processing` (split out of the monolithic
``processing.py`` for the §4 size limit; pure relocation, no behavior change).

Assembles the per-shard output carrier and writes it to the Zarr template
(including the ``resolution: chunk`` companion path). Depends only on
``config``/``grids`` — never on the read or aggregate stages — so the import DAG
stays acyclic.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.concurrency import fd_safe_max_workers
from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_output_signature,
)
from zagg.csr import write_csr
from zagg.grids.morton import is_morton_array, morton_words

# The sharded path's K ragged (CSR) subgroup writes fan out over a bounded thread
# pool (issue #142): each inner chunk emits an independent CSR group at a disjoint
# prefix, so they are write-independent and latency-bound (~K×3 tiny objects), and
# a serial loop dominated a t-digest shard's write time. 128 mirrors the dense
# path's ``async.concurrency`` budget (issue #108).
_RAGGED_WRITE_CONCURRENCY = 128


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
        # Route the morton coordinate through the same uint64 boundary as the
        # pandas carrier (#71), so both carriers share one on-disk dtype guarantee
        # rather than relying on MortonIndexArray.__array__ returning uint64.
        arr = morton_words(vals) if is_morton_array(vals) else np.asarray(vals)
        columns[col_name] = Array.from_numpy(np.ascontiguousarray(arr))
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
    (1-D) and rectilinear (2-D) share one path. The ``resolution: chunk``
    companions and ragged (CSR) fields are NOT sharded — they are written per inner
    chunk via the existing seams, exactly as on the regular path.

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
    # deterministic writes. Companions + ragged stay per inner chunk (unsharded).
    objects: dict = {}
    ragged_writes: list = []
    for block_index, carrier, ragged in chunk_results:
        if not _carrier_empty(carrier):
            objects.setdefault(_object_block(block_index), []).append((block_index, carrier))
        # Companions for this inner chunk are not sharded: write them straight
        # through (one block per chunk). Ragged (CSR) subgroups are collected and
        # fanned out below (issue #142) -- they target disjoint prefixes, so the
        # per-chunk serial loop needlessly dominated a t-digest shard's write time.
        if chunk_res_fields:
            _write_companion_columns(carrier, store, grid, block_index, chunk_res_fields)
        if ragged:
            ragged_writes.append((ragged, _block_index_key(block_index, grid)))

    _write_ragged_fanout(ragged_writes, store, grid=grid)

    # One accumulate→write→free pass per sharding object: each holds at most one
    # object's slab resident at a time (the phase-8 memory bound).
    for obj_block, members in objects.items():
        slabs: dict = {}
        for block_index, carrier in members:
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
    return store


def _write_ragged_fanout(ragged_writes: list, store: Store, *, grid) -> None:
    """Write the sharded shard's K ragged (CSR) subgroups concurrently (issue #142).

    On the sharded path :func:`write_shard_to_zarr` already holds all K inner-chunk
    carriers resident (it bundles the dense side into one shard object), and each
    inner chunk emits an independent CSR subgroup at a disjoint prefix
    (``{group_path}/{field}/{block_key}/...``). Those writes are therefore
    embarrassingly parallel and latency-bound (~K×3 tiny objects, tens of MB), so a
    serial loop dominated a t-digest shard's write time. Fan them out over a bounded
    ``ThreadPoolExecutor``; the on-disk layout is unchanged (still one CSR group per
    inner chunk -- only the write scheduling differs), and concurrent writes to
    disjoint keys of one store are already zagg's model (the dispatcher fans cells
    over threads onto a shared store).

    The bound is ``min(_RAGGED_WRITE_CONCURRENCY, len(writes), fd_safe_max_workers())``
    so this per-worker inner fan-out respects the same open-file ceiling the Lambda
    dispatcher guards (:func:`zagg.concurrency.fd_safe_max_workers`) -- each in-flight
    write holds a store socket. A failure in any subgroup is surfaced (not swallowed):
    the first exception is re-raised after the pool drains, tagged with how many of
    the K writes failed.

    (The non-sharded/streaming path deliberately stays serial: it writes-then-frees
    each chunk to bound peak memory (issue #91), so it never holds the K carriers at
    once to parallelize.)
    """
    if not ragged_writes:
        return
    workers = min(_RAGGED_WRITE_CONCURRENCY, len(ragged_writes), fd_safe_max_workers())
    if workers <= 1:
        for ragged, ragged_key in ragged_writes:
            write_ragged_to_zarr(ragged, store, grid=grid, shard_key=ragged_key)
        return

    errors: list = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(write_ragged_to_zarr, ragged, store, grid=grid, shard_key=ragged_key): (
                ragged_key
            )
            for ragged, ragged_key in ragged_writes
        }
        for fut in as_completed(futures):
            exc = fut.exception()
            if exc is not None:
                errors.append((futures[fut], exc))
    if errors:
        first_key, first_exc = errors[0]
        raise RuntimeError(
            f"ragged (CSR) write failed for {len(errors)} of {len(ragged_writes)} "
            f"subgroup(s) on the sharded path (first failing shard_key {first_key})"
        ) from first_exc


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
    shard_key: int,
) -> Store:
    """Write a shard's ``kind: ragged`` (CSR) fields to the Zarr store (issue #48).

    Mirrors the :func:`write_dataframe_to_zarr` seam for the dense path: the
    worker collects the per-cell variable-length payloads (a ``kind: ragged``
    field has no fixed per-cell width, so it cannot ride the dense block writer),
    and this function persists them via :func:`zagg.csr.write_csr` — one CSR group
    (``values`` / ``offsets`` / ``cell_ids``) per field per shard.

    Store layout (the contract the ``readers/tdigest_tensor.py`` reader consumes)::

        {group_path}/{field}/{shard_key}/values
        {group_path}/{field}/{shard_key}/offsets
        {group_path}/{field}/{shard_key}/cell_ids

    At **cell resolution** (default) ``cell_ids[k]`` is each populated cell's
    position in the chunk's ``children`` block (the index collected by
    ``process_shard``); the per-shard subgroup name is the ``shard_key`` (the
    coverage cell's morton id for HEALPix), recovered by the reader directly from
    the store.

    At **chunk resolution** (``resolution: chunk``, issue #82) a ragged field
    stores ONE variable-length payload per chunk, not per cell. The populated
    cells are collapsed to that single chunk payload under the same chunk-uniform
    contract as scalar/vector chunk companions (every populated cell must carry an
    identical payload, else raise); it is written as a single-entry CSR with
    ``cell_ids == [0]`` (the lone chunk), so the on-disk layout is the same three
    arrays — a consumer reads the chunk payload as the only populated "cell".

    Parameters
    ----------
    ragged : dict
        ``{field_name: (values_list, cell_ids)}`` as filled by ``process_shard``'s
        ``ragged_out`` sink. Empty (or all-empty payloads) writes empty CSR arrays
        (``write_csr`` skips empties), so a shard with no ragged data is a clean
        no-op rather than a special case. A located field (issue #87) arrives as
        ``(values_list, cell_ids, locations_list)`` and additionally writes a
        ``{field}/{shard_key}/locations`` uint64 array sharing the offsets.
    store : Store
        Zarr-compatible store.
    grid : OutputGrid
        Provides ``group_path`` for routing the write (and ``config`` for the
        per-field dtype + resolution).
    shard_key : int
        Shard identifier; the CSR subgroup name (one chunk per shard at cell
        resolution).

    Returns
    -------
    Store
        The same store, with the ragged CSR arrays written.
    """
    if not ragged:
        return store
    agg_fields = get_agg_fields(grid.config) if getattr(grid, "config", None) else {}
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))
    shard_key = int(shard_key)
    for name, entry in ragged.items():
        # Located fields (issue #87) deliver (values_list, cell_ids, locations_list);
        # unlocated fields keep the 2-tuple.
        if len(entry) == 3:
            values_list, cell_ids, locations_list = entry
        else:
            values_list, cell_ids = entry
            locations_list = None
        sig = get_output_signature(agg_fields[name]) if name in agg_fields else {}
        dtype = sig.get("dtype") or "float32"
        if name in chunk_res_fields:
            # resolution: chunk — collapse the populated cells to the single chunk
            # payload (chunk-uniform, like scalar/vector companions) and store it as
            # a one-entry CSR (the lone chunk at cell_ids == [0]).
            # ``location`` + ``resolution: chunk`` is rejected at config validation,
            # but a config built without validate_config (direct PipelineConfig /
            # Lambda dict payload) could still deliver a located triple here — fail
            # loudly rather than silently dropping the location channel (issue #87).
            if locations_list is not None:
                raise ValueError(
                    f"ragged field {name!r} is resolution: chunk but carries a location "
                    f"channel; located ragged fields are cell-resolution only"
                )
            chunk_payload = _chunk_uniform_ragged(name, values_list)
            if chunk_payload is None:
                continue  # whole chunk is fill — nothing to record
            write_csr(
                store,
                f"{grid.group_path}/{name}/{shard_key}",
                [chunk_payload],
                [0],
                dtype=dtype,
            )
            continue
        write_csr(
            store,
            f"{grid.group_path}/{name}/{shard_key}",
            values_list,
            cell_ids,
            dtype=dtype,
            locations_list=locations_list,
        )
    return store


def _block_index_key(block_index, grid) -> int:
    """Flatten a chunk's block-index tuple to the CSR subgroup key (issue #48, K>1).

    1-D grids (the HEALPix companion grid, the typical case) yield a single-element
    block index used directly; a multi-axis (rectilinear) block index is packed
    row-major against the grid's ``chunk_grid_shape`` so each chunk maps to a
    distinct CSR subgroup name. Deriving the per-axis strides from the chunk grid
    (rather than a fixed shift) keeps the pack injective for any grid size.

    Shared by both the local runner and the Lambda handler (issue #82 phase 7) so
    the K>1 ragged-write key is computed identically off-Lambda and on-Lambda.
    """
    block = tuple(int(b) for b in block_index)
    if len(block) == 1:
        return block[0]
    # Row-major flatten with each axis's true extent as the stride.
    shape = tuple(int(s) for s in getattr(grid, "chunk_grid_shape", ()))
    if len(shape) != len(block):
        # Fall back to a generous fixed stride if the grid does not expose a
        # matching chunk_grid_shape (keeps a unique-enough key without crashing).
        key = 0
        for b in block:
            key = key * (1 << 32) + b
        return key
    key = 0
    for b, extent in zip(block, shape):
        key = key * extent + b
    return key


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
    it onto the Zarr trailing payload dimension (issue #29).
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

    n_rows = carrier.num_rows
    for name in carrier.column_names:
        col = carrier.column(name).combine_chunks()
        # arro3 marks a FixedSizeList by an integer ``list_size`` (None for scalar
        # types), so the per-cell width and the 2-D reshape mirror the pyarrow path
        # exactly — the numpy block written to Zarr is byte-for-byte unchanged.
        width = col.type.list_size
        if width is not None:
            flat = list_flatten(col).to_numpy()
            yield name, flat.reshape(n_rows, width)
        else:
            yield name, col.to_numpy()
