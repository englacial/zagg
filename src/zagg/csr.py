"""CSR (Compressed Sparse Row) writer and reader for ragged Zarr v3 arrays.

A ragged field (issue #48) stores variable-length per-cell payloads using three
co-located Zarr v3 arrays under a common prefix::

    {field_name}/values   -- flat concatenation of all per-cell arrays
    {field_name}/offsets  -- per-populated-cell start index into values (length
                             n_populated + 1; offsets[-1] == len(values))
    {field_name}/cell_ids -- which cells (by position in the chunk's children
                             array) have data (length n_populated)

This mirrors the standard CSR layout: ``values[offsets[k]:offsets[k+1]]`` is the
payload for cell ``cell_ids[k]``.  Cells absent from ``cell_ids`` have no data.

Profiling note (IO vs compute)
-------------------------------
For a typical ATL06 shard (4096 cells, ~100–1000 t-digest centroids per cell),
the flat ``values`` array is small relative to the scalar arrays written by the
dense path, so CSR IO overhead is dominated by the additional per-shard Zarr
metadata writes (3 arrays instead of 1).  The per-shard compute for the CSR
pack/unpack (``np.concatenate`` + ``np.cumsum``) is O(total_elements) and
negligible vs t-digest construction.  At δ=512, each cell stores up to 2048
floats (1024 centroids × 2 columns); at the shard scale the flat values array
is ≲8 MB, fitting comfortably in a single Zarr chunk with ``chunks=None``
(store as one object). For production the chunk size should be tuned to the
expected number of populated cells × centroids_per_cell × item_size.
"""

from __future__ import annotations

import numpy as np
import zarr
from zarr.abc.store import Store


def write_csr(
    store: Store,
    field_name: str,
    values_list: list[np.ndarray],
    cell_ids: list[int],
    *,
    dtype: str | np.dtype = "float32",
    zarr_format: int = 3,
    locations_list: list[np.ndarray] | None = None,
) -> None:
    """Write a ragged field to a Zarr store as three CSR arrays.

    Parameters
    ----------
    store : Store
        Zarr-compatible store (memory, local, S3, …).
    field_name : str
        Prefix under which the three arrays are written:
        ``{field_name}/values``, ``{field_name}/offsets``,
        ``{field_name}/cell_ids``.
    values_list : list of ndarray
        Per-populated-cell payloads.  Each element is a numpy array of shape
        ``(n_elements, *inner_shape)`` (or 1-D ``(n_elements,)`` for a scalar
        inner type).  Empty arrays are silently skipped.
    cell_ids : list of int
        Cell index (position in the chunk's ``children`` array) for each entry
        in ``values_list``.  Must have the same length as ``values_list``.
    dtype : str or numpy dtype, optional
        Dtype for the ``values`` array.  Default ``"float32"``.
    zarr_format : int, optional
        Zarr format version.  Default 3.
    locations_list : list of ndarray, optional
        Per-cell ``uint64`` morton location vectors (issue #87), index-aligned
        with ``values_list``; each element has one word per payload element.
        Written as a fourth array ``{field_name}/locations`` **sharing**
        ``offsets``/``cell_ids`` with ``values`` (the shared offsets are valid
        because the per-cell lengths are enforced equal).  ``None`` (default)
        writes exactly the pre-#87 three arrays, byte-identical.

    Raises
    ------
    ValueError
        If ``values_list`` and ``cell_ids`` have different lengths, if any
        element of ``values_list`` has an inconsistent inner shape, or if a
        ``locations_list`` entry's length disagrees with its payload.
    """
    if len(values_list) != len(cell_ids):
        raise ValueError(
            f"values_list (len {len(values_list)}) and cell_ids (len {len(cell_ids)}) "
            "must have the same length"
        )
    if locations_list is not None and len(locations_list) != len(values_list):
        raise ValueError(
            f"locations_list (len {len(locations_list)}) and values_list "
            f"(len {len(values_list)}) must have the same length"
        )

    dtype = np.dtype(dtype)

    # Filter out empty payloads (cells with no data contribute nothing to CSR).
    # Locations ride the same filter so they stay index-aligned.
    locs_or_none = locations_list if locations_list is not None else [None] * len(values_list)
    non_empty = [
        (arr, cid, loc)
        for arr, cid, loc in zip(values_list, cell_ids, locs_or_none)
        if np.asarray(arr).size > 0
    ]

    flat_locs: np.ndarray | None = None
    if not non_empty:
        flat = np.empty(0, dtype=dtype)
        offsets = np.zeros(1, dtype=np.int64)
        ids_arr = np.empty(0, dtype=np.int64)
        if locations_list is not None:
            flat_locs = np.empty(0, dtype=np.uint64)
    else:
        arrays, ids, locs = zip(*non_empty)
        arrays = [np.asarray(a, dtype=dtype) for a in arrays]
        # Validate consistent inner shape.
        inner = arrays[0].shape[1:] if arrays[0].ndim > 1 else ()
        for i, a in enumerate(arrays):
            a_inner = a.shape[1:] if a.ndim > 1 else ()
            if a_inner != inner:
                raise ValueError(
                    f"Inconsistent inner shape at index {i}: expected {inner}, got {a_inner}"
                )
        flat = np.concatenate([a.reshape(a.shape[0], -1) if a.ndim > 1 else a for a in arrays])
        lengths = np.array([a.shape[0] for a in arrays], dtype=np.int64)
        offsets = np.concatenate([[0], np.cumsum(lengths)])
        ids_arr = np.array(ids, dtype=np.int64)
        if locations_list is not None:
            locs = [np.asarray(loc, dtype=np.uint64) for loc in locs]
            for i, (a, loc) in enumerate(zip(arrays, locs)):
                if loc.shape != (a.shape[0],):
                    raise ValueError(
                        f"locations at index {i} have shape {loc.shape}, expected "
                        f"({a.shape[0]},) to share the payload's offsets"
                    )
            flat_locs = np.concatenate(locs) if locs else np.empty(0, dtype=np.uint64)

    _write_array(store, f"{field_name}/values", flat, zarr_format=zarr_format)
    _write_array(store, f"{field_name}/offsets", offsets, zarr_format=zarr_format)
    _write_array(store, f"{field_name}/cell_ids", ids_arr, zarr_format=zarr_format)
    if flat_locs is not None:
        _write_array(store, f"{field_name}/locations", flat_locs, zarr_format=zarr_format)


def _write_array(store: Store, path: str, data: np.ndarray, *, zarr_format: int = 3) -> None:
    """Write a 1-D or 2-D numpy array to a Zarr store at ``path``."""
    arr = zarr.open_array(
        store,
        path=path,
        mode="w",
        shape=data.shape,
        chunks=data.shape
        if data.size > 0
        else tuple(max(1, d) for d in data.shape)
        if data.ndim > 1
        else (1,),
        dtype=data.dtype,
        zarr_format=zarr_format,
    )
    if data.size > 0:
        arr[...] = data


def read_csr(
    store: Store,
    field_name: str,
    *,
    zarr_format: int = 3,
    locations: bool = False,
) -> dict[str, np.ndarray]:
    """Read CSR arrays written by :func:`write_csr`.

    Parameters
    ----------
    store : Store
        Zarr-compatible store.
    field_name : str
        Prefix used when writing (``{field_name}/values`` etc.).
    zarr_format : int, optional
        Zarr format version.  Must match the version used when writing.
        Default 3.
    locations : bool, optional
        Also read the ``{field_name}/locations`` companion (issue #87) into a
        ``"locations"`` key.  Opt-in so value-only reads stay free of the extra
        store probe.  Raises a clear error when the field carries no location
        channel.

    Returns
    -------
    dict with keys ``"values"``, ``"offsets"``, ``"cell_ids"``.
        - ``values`` : flat concatenation of all per-cell payloads.
        - ``offsets`` : length ``n_populated + 1``; ``values[offsets[k]:offsets[k+1]]``
          is the payload for cell ``cell_ids[k]``.
        - ``cell_ids`` : which cells (by chunk position) have data.
        With ``locations=True``, also ``"locations"`` — flat uint64 morton words
        sharing ``offsets`` with ``values``.
    """
    values = zarr.open_array(store, path=f"{field_name}/values", mode="r", zarr_format=zarr_format)[
        ...
    ]
    offsets = zarr.open_array(
        store, path=f"{field_name}/offsets", mode="r", zarr_format=zarr_format
    )[...]
    cell_ids = zarr.open_array(
        store, path=f"{field_name}/cell_ids", mode="r", zarr_format=zarr_format
    )[...]
    out = {"values": values, "offsets": offsets, "cell_ids": cell_ids}
    if locations:
        try:
            out["locations"] = zarr.open_array(
                store, path=f"{field_name}/locations", mode="r", zarr_format=zarr_format
            )[...]
        except (FileNotFoundError, KeyError) as e:
            raise ValueError(
                f"{field_name!r} has no locations array; it was not written as a "
                f"located ragged field (declare 'location:' on the field — issue #87)"
            ) from e
    return out


def iter_csr_cells(
    csr: dict[str, np.ndarray],
) -> list[tuple[int, np.ndarray]]:
    """Decode a CSR dict into a list of ``(cell_id, payload)`` pairs.

    Parameters
    ----------
    csr : dict
        As returned by :func:`read_csr`.

    Returns
    -------
    list of (int, ndarray)
        One entry per populated cell, in the order they were written.
    """
    values = csr["values"]
    offsets = csr["offsets"]
    cell_ids = csr["cell_ids"]
    result = []
    for k, cid in enumerate(cell_ids):
        payload = values[offsets[k] : offsets[k + 1]]
        result.append((int(cid), payload))
    return result
