"""OutputGrid protocol — pluggable spatial grid interface.

Each implementation owns the grid-specific operations the pipeline needs:
shard enumeration, point-to-cell assignment, write-partition identity,
storage-block mapping, footprint geometry, and Zarr template emission.

Terminology
-----------
- **leaf id** — the high-precision spatial identifier returned by ``assign``.
  Grid-specific (HEALPix: morton at the grid's reference order; rectilinear:
  flat row-major cell index). Pipeline code treats it as an opaque integer.
- **cell id** — the aggregation-grid identifier returned by ``cells_of`` and
  ``children``. For some grids (rectilinear) leaf id and cell id coincide.
- **shard key** — the write-partition identifier returned by ``shard_of``.
  ``block_index`` maps a shard key to a storage block tuple.

The shard_of / block_index split (see bench/REPORT.md §2) keeps spatial
identity separate from storage position. For grids where chunks tile space
algorithmically (rectilinear, fullsphere DGGS) the two collapse; for dense-
packed sparse DGGS arrays they differ and block_index needs the populated-
shard list to translate.
"""

from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import Any, Protocol, runtime_checkable

import numpy as np
from zarr.abc.store import Store

ShardKey = Any  # int for HEALPix, tuple[int,int] for rectilinear, etc.

#: Array-attrs key on a ragged vlen-bytes array (issue #209) recording the
#: per-cell element interpretation. The value is
#: ``{"element": {"dtype": "<numpy dtype>", "shape": [-1, *inner_shape]}}``:
#: each populated cell's value is the raw little-endian bytes of an
#: ``(n, *inner_shape)`` array (``-1`` marks the per-cell varying count), so a
#: reader reconstructs cell ``i`` as
#: ``np.frombuffer(a[i], dtype).reshape(-1, *inner_shape)``. A LOCATED field's
#: payload array additionally carries ``{"locations": "<sibling array name>"}``
#: (issue #87) — the reader binds the uint64 channel by that declaration, not
#: by reconstructing the naming convention (review, PR #211). The block is
#: versioned (``{"spec": RAGGED_SPEC}``, the coverage-envelope discipline) so
#: readers fail loudly on a future revision instead of half-parsing it.
RAGGED_ELEMENT_ATTR = "ragged"

#: Convention version stamped into the :data:`RAGGED_ELEMENT_ATTR` block and
#: strict-checked by the readers (``readers/tdigest_tensor._open_ragged``).
#: This attrs seam is the INTERIM contract: the issue #210 typed
#: ``vlen-array<T>`` dtype migration moves the element declaration into the
#: zarr data type itself and supersedes (bumps or removes) this marker.
RAGGED_SPEC = "zagg-ragged/1"

#: zstd level of the ragged inner codec chain — 3, matching the coverage
#: sidecar precedent (``zagg.hive._ZSTD_LEVEL``), fixed so identical payloads
#: produce identical objects across workers.
RAGGED_ZSTD_LEVEL = 3

#: Array-attrs key declaring a digest payload's weight-column semantics
#: (spec §2.0, issue #424): a SIBLING of :data:`RAGGED_ELEMENT_ATTR` on the
#: payload array — never a key inside the versioned ``ragged`` block, which
#: is retired wholesale under ``zagg-ragged/2`` (a sibling key survives that
#: metadata-only migration untouched; espg ruling on issue #422). Spec-owned:
#: stamped at template time from the field's ``weights:`` declaration,
#: reserved against config-declared attrs.
WEIGHTS_ATTR = "weights"

#: The defined §2.0 declarations. ``counts`` — integer weights ≥ 1 summing to
#: the exact observation count (the default: an ABSENT key reads as counts,
#: keeping every pre-#424 store conformant verbatim). ``flux`` — positive
#: reals whose sum estimates detected photoelectrons (calibration provenance
#: required in attrs). Merges are legal only between matching declarations.
WEIGHTS_KINDS = ("counts", "flux")

#: Array-attrs key binding a ragged payload array to its §8.3 per-centroid
#: temporal sibling — the sibling's array NAME. It mirrors the ``locations``
#: binding but sits OUTSIDE the versioned ``ragged`` block, exactly as
#: :data:`WEIGHTS_ATTR` does and for the same reason (the block is retired
#: wholesale under ``zagg-ragged/2``), so `/1`'s block grammar does not move
#: for this revision. Spec-owned: writer-stamped from the field's
#: ``temporal:`` declaration, reserved against config-declared attrs.
TIMES_ATTR = "times"

#: Array-attrs key of the spec §9 spatial declaration, stamped on a located
#: field's ``{field}_locations`` sibling — the array that HOLDS the morton
#: words. §8's word-typed declaration pattern, keyed by its domain.
LOCATED_ATTR = "located"
#: The §9 convention revision, strict-checked on read.
LOCATED_SPEC = "zagg-located/1"
#: The §9 word-grammar revision token. It names mortie's morton conventions
#: — specification §1 (bit layout) and §4 (encoding-carried kind), both
#: enumerated as frozen for 1.x by that document's §10 — as a
#: ``{name}/{major}`` token, never a URL or an installed-version stamp.
LOCATED_GRAMMAR = "mortie-morton/1"
#: The only §9 ``shape``: one word per centroid row of the payload the
#: sibling accompanies.
LOCATED_SHAPE_PER_CENTROID = "per-centroid"


def located_declaration_block() -> dict:
    """The §9 declaration a writer stamps on a located sibling array."""
    return {
        "spec": LOCATED_SPEC,
        "shape": LOCATED_SHAPE_PER_CENTROID,
        "grammar": LOCATED_GRAMMAR,
    }


def located_declaration(attrs) -> dict | None:
    """The §9 ``located`` block from a sibling array's attrs, strict-checked.

    Returns ``None`` when the key is absent — which §9 defines as §2.2
    verbatim (kind-keyed words decoded per word, and the deepest common
    ancestor of the members' words after a merge), never a refusal, so every
    located store written before the declaration stays conformant. Raises on a block this reader cannot
    decode: an unknown ``spec``, an unimplemented ``shape``, or an uncited
    word ``grammar``. Unrecognized keys are informative and ignored.
    """
    block = dict(attrs or {}).get(LOCATED_ATTR)
    if block is None:
        return None
    if not isinstance(block, dict):
        raise ValueError(f"{LOCATED_ATTR!r} attrs must be a mapping (got {block!r}) — spec §9")
    if block.get("spec") != LOCATED_SPEC:
        raise ValueError(
            f"unknown located declaration spec {block.get('spec')!r} (spec §9 defines "
            f"{LOCATED_SPEC!r}); refusing to guess a future revision's word encoding"
        )
    if block.get("shape") != LOCATED_SHAPE_PER_CENTROID:
        raise ValueError(
            f"located declaration shape {block.get('shape')!r} is not implemented "
            f"(spec §9 defines {LOCATED_SHAPE_PER_CENTROID!r})"
        )
    if block.get("grammar") != LOCATED_GRAMMAR:
        raise ValueError(
            f"located declaration cites word grammar {block.get('grammar')!r}, not "
            f"{LOCATED_GRAMMAR!r} (spec §9); refusing to decode words under a grammar "
            f"this reader does not implement"
        )
    return block


def weights_declaration(attrs) -> str:
    """The §2.0 weights declaration recorded in a payload array's attrs.

    Absent key reads as ``"counts"`` (spec §2.0 — existing stores are
    conformant verbatim); an unknown value is a future spec revision and
    raises rather than half-parsing (the strict-check discipline every
    versioned convention on the spec page gets).
    """
    value = dict(attrs or {}).get(WEIGHTS_ATTR, "counts")
    if value not in WEIGHTS_KINDS:
        raise ValueError(
            f"unknown weights declaration {value!r} (spec §2.0 defines {WEIGHTS_KINDS}); "
            f"refusing to guess weight semantics for a future spec revision"
        )
    return value


def apply_field_attrs(spec, meta: dict):
    """Merge a variable's config-declared ``attrs`` onto its array spec.

    ``aggregation.variables.{name}.attrs`` (issue #321) records field-level
    provenance in the store — e.g. the composition field's lane order and the
    signal threshold the split was committed at — so readers bind to metadata
    rather than reconstructing config conventions.

    Attrs land on the **payload array only**. A located ragged field's sibling
    uint64 channel (:func:`ragged_locations_name`) is addressed *through* the
    payload array, so it deliberately carries no user attrs — the provenance of
    the pair lives on the field the config named (review finding, PR #334).

    Declared keys are merged **over** the spec's own attributes, so a config
    could in principle shadow spec-owned metadata. Two keys are protected from
    that: ``ragged`` is reserved outright (enforced at config validation), and
    the ``composition`` block's spec-owned halves (``spec``/``lanes``) are
    **stamped here** from the module constants
    (:func:`zagg.stats.composition.composition_attrs_block`), with config
    validation rejecting a declaration that disagrees — so neither convention
    marker in tree is an author transcription (review finding, issue #340).

    A field declaring ``temporal: per-cell`` (spec §8.2, issue #410) gets the
    spec-owned ``temporal`` block stamped here from the module constants for
    the same reason: the dense uint64 array IS the array holding the words,
    and the declaration must be the writer's, never an author's transcription.
    """
    from zagg.time_axis import TOC_SHAPE_PER_CELL, temporal_attrs

    attrs = meta.get("attrs")
    declared = meta.get("temporal")
    if declared is not None and declared != TOC_SHAPE_PER_CELL:
        # per-centroid rides the ragged sibling (§8.3), stamped by
        # ``ragged_array_spec``; nothing else is a dense-array shape.
        declared = None
    if not attrs and declared is None:
        return spec
    if declared is not None:
        spec = spec.with_attributes({**dict(spec.attributes or {}), **temporal_attrs(declared)})
    if not attrs:
        return spec
    from zagg.stats.composition import COMPOSITION_ATTR, composition_attrs_block

    merged = {**dict(spec.attributes or {}), **attrs}
    block = merged.get(COMPOSITION_ATTR)
    if isinstance(block, dict):
        merged[COMPOSITION_ATTR] = composition_attrs_block(block)
    return spec.with_attributes(merged)


def ragged_locations_name(field_name: str) -> str:
    """On-disk array name of a located ragged field's uint64 channel (issue #87).

    Under the vlen-bytes layout (issue #209) the location words are a SIBLING
    vlen array (``{field}_locations``) row-aligned with the digest payload —
    the CSR layout's fourth in-group array cannot nest under what is now an
    array node.
    """
    return f"{field_name}_locations"


def ragged_times_name(field_name: str) -> str:
    """On-disk array name of a temporal ragged field's uint64 channel (§8.3).

    The per-centroid temporal sibling (``{field}_times``), row-aligned with
    the digest payload exactly as the located sibling is. Stored under the
    payload array's :data:`TIMES_ATTR` binding — a reader binds by that
    declaration, never by reconstructing this convention (spec §8.3).
    """
    return f"{field_name}_times"


@contextmanager
def vlen_dtype_warning_suppressed():
    """Suppress zarr's vlen-bytes dtype-naming warning at array CREATION only.

    zarr-python names the dtype ``variable_length_bytes`` in metadata while
    the v3 registry name is ``bytes`` (zarr-python#3517, accepted both ways on
    read), so creating a ragged vlen array emits an
    ``UnstableSpecificationWarning`` about that naming. Scoped to the exact
    message (the ``coverage.moc`` suppression precedent) so nothing else is
    silenced; reads/writes/opens do not warn.
    """
    from zarr.errors import UnstableSpecificationWarning

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"The data type \(VariableLengthBytes\(\)\)",
            category=UnstableSpecificationWarning,
        )
        yield


def ragged_array_spec(
    *,
    shape,
    dims,
    inner_chunk_shape,
    shard_shape=None,
    element_dtype,
    inner_shape=(),
    locations=None,
    weights=None,
    times=None,
    temporal=None,
    located=False,
):
    """Vlen-bytes ``ArraySpec`` for a ``kind: ragged`` field (issue #209).

    The sharded vlen-bytes layout: ONE zarr array with the
    ``variable_length_bytes`` data type replaces the per-inner-chunk CSR
    subgroups (~7 objects per populated inner chunk). Each populated cell
    holds the raw little-endian bytes of its ``(n, *inner_shape)`` payload
    (``n`` varies per cell); empty cells keep the ``b""`` fill, and an
    all-empty inner chunk is omitted from the shard index — the same
    sub-shard sparsity the dense arrays get. The element interpretation is
    self-describing via :data:`RAGGED_ELEMENT_ATTR` in the array attrs.

    Codec chain: ``[vlen-bytes, zstd(level=3)]``. The zstd deviates from the
    dense arrays' bytes-only/uncompressed policy deliberately: a vlen payload
    has no fixed-width raw layout to preserve, and level 3 matches the
    coverage-sidecar precedent. With ``shard_shape`` the chain rides INSIDE a
    ``ShardingCodec`` (outer chunk == ``shard_shape``), collapsing a shard's K
    inner chunks to one object with an internal index — single-cell reads stay
    2 GETs (index suffix + one ranged inner chunk). ``None`` keeps a regular
    array chunked at ``inner_chunk_shape`` (one object per inner chunk — the
    unsharded per-chunk-write layout).

    Parameters
    ----------
    shape : tuple of int
        Array shape — the grid's cell axes (or the chunk grid for a
        ``resolution: chunk`` companion).
    dims : tuple of str
        Dimension names matching ``shape``.
    inner_chunk_shape : tuple of int
        Read-chunk shape (``grid.chunk_shape``; ``(1,)*ndim`` for a chunk
        companion).
    shard_shape : tuple of int, optional
        ShardingCodec outer chunk. ``None`` (default) emits a regular array.
    element_dtype : str
        Numpy dtype of one payload element (recorded in attrs; the bytes are
        little-endian).
    inner_shape : tuple of int, optional
        Per-element trailing shape (``sig["inner_shape"]``, e.g. ``(2,)`` for
        a centroid pair). Empty for a flat per-cell vector.
    locations : str, optional
        Name of the located field's uint64 sibling array (issue #87),
        declared in the payload array's attrs so a reader binds the channel
        by METADATA, not by reconstructing the naming convention (review,
        PR #211). ``None`` (unlocated) records nothing.
    weights : str, optional
        The field's §2.0 weights declaration (issue #424), stamped as the
        :data:`WEIGHTS_ATTR` SIBLING key beside the ``ragged`` block (never
        inside it — the block is retired under ``/2``). ``None`` (undeclared)
        records nothing: the spec reads an absent key as ``"counts"``, so
        pre-#424 configs emit byte-identical templates. Pass it for the
        payload array only — a companion sibling carries no user attrs.
    times : str, optional
        Name of the field's per-centroid TEMPORAL sibling (spec §8.3, issue
        #410), stamped as the :data:`TIMES_ATTR` SIBLING key beside the
        ``ragged`` block on the PAYLOAD array — a binding, not a
        declaration. ``None`` records nothing (no temporal companion).
    temporal : str, optional
        Spec §8 ``shape`` of the words THIS array holds — passed for a
        per-centroid sibling, never for a payload array. Stamps the
        spec-owned ``temporal`` declaration block.
    located : bool, optional
        Stamp the spec §9 ``located`` declaration on THIS array — passed for
        a ``{field}_locations`` sibling, the array that holds the morton
        words. Its absence on an older store is §2.2 verbatim, never a
        refusal.

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import ArraySpec, NamedConfig

    inner_codecs = [
        {"name": "vlen-bytes", "configuration": {}},
        {"name": "zstd", "configuration": {"level": RAGGED_ZSTD_LEVEL, "checksum": False}},
    ]
    if shard_shape is not None:
        # Index codecs mirror ``sharded_array_spec`` (zarr's create_array default).
        codecs: tuple = (
            NamedConfig(
                name="sharding_indexed",
                configuration={
                    "chunk_shape": [int(c) for c in inner_chunk_shape],
                    "codecs": inner_codecs,
                    "index_codecs": [
                        {"name": "bytes", "configuration": {"endian": "little"}},
                        {"name": "crc32c"},
                    ],
                    "index_location": "end",
                },
            ),
        )
        chunk_shape = tuple(int(c) for c in shard_shape)
    else:
        codecs = tuple(NamedConfig(**c) for c in inner_codecs)
        chunk_shape = tuple(int(c) for c in inner_chunk_shape)
    element = {"dtype": str(element_dtype), "shape": [-1, *(int(s) for s in inner_shape)]}
    ragged_meta: dict = {"spec": RAGGED_SPEC, "element": element}
    if locations is not None:
        ragged_meta["locations"] = str(locations)
    attributes: dict = {RAGGED_ELEMENT_ATTR: ragged_meta}
    if weights is not None:
        if weights not in WEIGHTS_KINDS:
            raise ValueError(
                f"weights declaration {weights!r} is not one of {WEIGHTS_KINDS} (spec §2.0)"
            )
        attributes[WEIGHTS_ATTR] = str(weights)
    if times is not None:
        attributes[TIMES_ATTR] = str(times)
    if temporal is not None:
        from zagg.time_axis import TOC_SHAPE_PER_CENTROID, temporal_attrs

        if temporal != TOC_SHAPE_PER_CENTROID:
            raise ValueError(
                f"a ragged sibling declares temporal shape {TOC_SHAPE_PER_CENTROID!r}, "
                f"not {temporal!r} (spec §8.3)"
            )
        attributes.update(temporal_attrs(temporal))
    if located:
        attributes[LOCATED_ATTR] = located_declaration_block()
    return ArraySpec(
        attributes=attributes,
        shape=tuple(int(s) for s in shape),
        dimension_names=tuple(dims),
        data_type="variable_length_bytes",
        chunk_grid=NamedConfig(name="regular", configuration={"chunk_shape": list(chunk_shape)}),
        chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
        codecs=codecs,
        storage_transformers=(),
        fill_value="",
    )


def vector_array_spec(base, sig, *, base_dims, base_chunk_shape):
    """Extend a scalar data-var ``ArraySpec`` with a trailing payload dim.

    Issue #29 phase 5: a ``kind: vector`` field is stored as a dense array of
    shape ``(*spatial_shape, *trailing_shape)`` — the per-cell vector rides on
    one or more trailing dimensions appended to the grid's spatial axes.

    **Single-trailing-chunk invariant.** The trailing payload dimension(s) are
    chunked *whole* (one chunk spans the full ``trailing_shape``). This is what
    lets :func:`zagg.processing.write_dataframe_to_zarr` address a shard's
    payload with ``block_idx = chunk_idx + (0,) * len(trailing_shape)`` — the
    trailing block index is always ``0``. Do not chunk the trailing dim; the
    writer assumes block 0.

    Parameters
    ----------
    base : ArraySpec
        The scalar data-var spec for this grid (already carries the field's
        ``dtype``/``fill_value``); its ``shape`` is the spatial shape and its
        chunk grid the spatial chunk shape.
    sig : dict
        Output signature from :func:`zagg.config.get_output_signature`
        (``kind``/``trailing_shape``/``dtype``). For a scalar field (empty
        ``trailing_shape``) ``base`` is returned unchanged.
    base_dims : tuple of str
        Spatial dimension names (e.g. ``("cells",)`` or ``("y", "x")``).
    base_chunk_shape : tuple of int
        Spatial chunk shape (the grid's ``chunk_shape``).

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    trailing = tuple(int(t) for t in sig["trailing_shape"])
    if not trailing:
        return base
    shape = (*base.shape, *trailing)
    # Name trailing payload axes ``vector`` (one dim) or ``vector_0``/``vector_1``
    # (multi-dim, e.g. a t-digest ``(k, 2)``) — distinct from the spatial axes.
    if len(trailing) == 1:
        trailing_names: tuple[str, ...] = ("vector",)
    else:
        trailing_names = tuple(f"vector_{i}" for i in range(len(trailing)))
    dim_names = (*base_dims, *trailing_names)
    chunk_shape = (*base_chunk_shape, *trailing)
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(chunk_shape)})
    # Set shape + dimension_names together: ArraySpec validates their ranks
    # match on construction, so a chained ``with_shape`` then
    # ``with_dimension_names`` would transiently mismatch and raise.
    return type(base)(
        **{
            **base.model_dump(),
            "shape": shape,
            "dimension_names": dim_names,
            "chunk_grid": chunk_grid,
        }
    )


def chunk_array_spec(base, *, chunk_grid_shape, chunk_dims):
    """Build a chunk-resolution companion ``ArraySpec`` from a scalar data-var spec.

    Issue #30 item 2: a ``resolution: chunk`` field stores ONE value per chunk
    rather than one per aggregation cell. Its array is shaped at the *chunk grid*
    (``grid.chunk_grid_shape`` = number of chunks along each axis), with one Zarr
    block per chunk so :func:`zagg.processing.write_dataframe_to_zarr` writes a
    chunk's single value at its chunk block index.

    At K==1 (``chunk_inner`` unset, one chunk per shard) that index is
    ``grid.block_index(shard_key)``. At K>1 (issue #30 item 3) the chunk grid is
    finer than the shard grid, so the per-chunk index is the block yielded by
    ``grid.iter_chunks(shard_key)``, not ``block_index``.

    The companion carries the field's ``dtype``/``fill_value`` (inherited from
    ``base``); only its shape, dimension names and chunk grid are re-set to the
    chunk grid. Each axis is chunked ``1`` (one chunk == one block).

    Parameters
    ----------
    base : ArraySpec
        The scalar data-var spec for this field (already carries
        ``dtype``/``fill_value``).
    chunk_grid_shape : tuple of int
        Number of chunks along each spatial axis (the companion array shape).
    chunk_dims : tuple of str
        Dimension names for the chunk-grid axes (e.g. ``("chunks",)`` for HEALPix,
        ``("chunk_y", "chunk_x")`` for rectilinear).

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    shape = tuple(int(s) for s in chunk_grid_shape)
    chunk_shape = tuple(1 for _ in shape)
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(chunk_shape)})
    return type(base)(
        **{
            **base.model_dump(),
            "shape": shape,
            "dimension_names": tuple(chunk_dims),
            "chunk_grid": chunk_grid,
        }
    )


def sharded_array_spec(base, *, shard_shape, inner_chunk_shape):
    """Wrap a dense data-var ``ArraySpec`` in a zarr ``ShardingCodec`` (issue #108).

    Decouples the **write/dispatch** granularity (one shard object per dispatch
    shard) from the **read** granularity (the inner 64×64 chunk). The K inner
    chunks of one shard become ONE shard object (empties omitted from the shard
    index, so sub-shard sparsity is preserved *inside* the object) instead of K
    independent regular chunk objects.

    The transform: the array's outer chunk grid is re-set to ``shard_shape`` (the
    whole dispatch shard) and a ``sharding_indexed`` codec replaces the array's
    top-level codecs, carrying the inner ``chunk_shape`` (the 64×64 read chunk)
    plus the array's existing (bytes-only) codecs as its INNER codecs. Building
    the codec config by hand — rather than ``ArraySpec.from_array`` (which drops
    the sharding codec in pydantic-zarr 0.10.0) or ``zarr.create_array`` (which
    silently injects a zstd level-0 compressor) — is what preserves zagg's
    bytes-only/uncompressed on-disk policy (the two prototype caveats on
    issue #108).

    Parameters
    ----------
    base : ArraySpec
        The dense data-var spec whose ``chunk_grid`` currently chunks at the
        inner chunk shape; its ``codecs`` are the bytes-only inner codecs.
    shard_shape : tuple of int
        Outer chunk (== shard) shape: the whole dispatch shard's cell extent.
    inner_chunk_shape : tuple of int
        Inner chunk shape (the 64×64 read chunk); ``prod(shard_shape) //
        prod(inner_chunk_shape) == K`` inner chunks per shard.

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    inner_codecs = [c.model_dump() if hasattr(c, "model_dump") else dict(c) for c in base.codecs]
    # crc32c on the shard index matches zarr's create_array default; the index
    # payload is bytes-only (no compression), keeping the policy bytes-only.
    index_codecs = [
        {"name": "bytes", "configuration": {"endian": "little"}},
        {"name": "crc32c"},
    ]
    sharding = NamedConfig(
        name="sharding_indexed",
        configuration={
            "chunk_shape": list(inner_chunk_shape),
            "codecs": inner_codecs,
            "index_codecs": index_codecs,
            "index_location": "end",
        },
    )
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(shard_shape)})
    return type(base)(
        **{
            **base.model_dump(),
            "chunk_grid": chunk_grid,
            "codecs": (sharding,),
        }
    )


def sample_nearest(xs, ys, src_crs, crs, transform, shape):
    """Nearest source-pixel indices for points, under a raster's affine (#218).

    The pull-NN primitive shared by the grid ``sample`` implementations: map
    point coordinates into the raster CRS, invert the affine, and take the
    pixel whose footprint contains each point.

    Parameters
    ----------
    xs, ys : array-like
        Point coordinates in ``src_crs`` (x/lon first — ``always_xy``).
    src_crs, crs : any pyproj CRS input
        Source CRS of the points and CRS of the raster.
    transform : sequence of float
        The raster's affine in STAC ``proj:transform`` / rasterio order
        ``(a, b, c, d, e, f)``: ``x = a*col + b*row + c``,
        ``y = d*col + e*row + f`` with (col, row) at the pixel's upper-left
        corner. A 9-element row-major 3x3 form is accepted (trailing row
        ignored).
    shape : (height, width)
        Raster shape, for the bounds mask.

    Returns
    -------
    (rows, cols, valid)
        ``int64`` pixel indices per point and a bool mask, ``False`` where the
        point falls outside the raster (indices are then meaningless).
    """
    from pyproj import CRS, Transformer

    tx = Transformer.from_crs(
        CRS.from_user_input(src_crs), CRS.from_user_input(crs), always_xy=True
    )
    x, y = tx.transform(np.asarray(xs, dtype=float), np.asarray(ys, dtype=float))
    a, b, c, d, e, f = (float(t) for t in transform[:6])
    det = a * e - b * d
    if det == 0.0:
        raise ValueError(f"degenerate raster transform (zero determinant): {transform!r}")
    dx, dy = x - c, y - f
    cols = np.floor((e * dx - b * dy) / det).astype(np.int64)
    rows = np.floor((a * dy - d * dx) / det).astype(np.int64)
    h, w = shape
    valid = (rows >= 0) & (rows < h) & (cols >= 0) & (cols < w)
    return rows, cols, valid


class InconsistentShardError(ValueError):
    """Raised by shard_of when input cells don't all share a shard."""


@runtime_checkable
class OutputGrid(Protocol):
    """Pluggable output grid.

    Implementations are stateless (fullsphere DGGS, rectilinear):
    ``block_index`` resolves from the shard key alone.
    """

    @property
    def array_shape(self) -> tuple[int, ...]:
        """Full output-array shape (1D for DGGS, 2D for rectilinear)."""
        ...

    @property
    def chunk_shape(self) -> tuple[int, ...]:
        """Per-chunk shape (rank matches ``array_shape``)."""
        ...

    def coverage(self, polygon_parts: list[tuple[np.ndarray, np.ndarray]]) -> np.ndarray:
        """Enumerate shard keys covering multipart polygons."""
        ...

    def assign(self, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
        """Map (lat, lon) points to leaf ids.

        Elementwise: N-D input returns the input shape. The a-priori chunk
        planner feeds a 2-D ``(n_chunks, samples_per_chunk)`` lat/lon pair
        straight through ``assign`` -> :meth:`shards_of` and reduces with
        ``.any(axis=1)`` (``processing.apriori._chunk_shard_mask``, issue
        #543), so a backend that ravels would mis-plan silently.
        """
        ...

    def cells_of(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Coarsen leaf ids to aggregation cell ids."""
        ...

    def sample(self, cells, crs, transform, shape) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Nearest source-pixel ``(rows, cols, valid)`` for cell centers.

        The pull-NN raster->grid primitive (issue #218): the writer samples a
        source raster at every cell center, guaranteeing a dense grid at any
        cell order with no collision or fill logic. ``crs``/``transform``/
        ``shape`` describe the source raster (see
        :func:`zagg.grids.base.sample_nearest`).
        """
        ...

    def shards_of(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Vectorized: shard key for each leaf id.

        Elementwise, like :meth:`assign`: N-D input returns the input shape.
        """
        ...

    def shard_of(self, leaf_ids: np.ndarray) -> ShardKey:
        """All leaves must share a shard; return it.

        Raises
        ------
        InconsistentShardError
            If the input cells span more than one shard.
        """
        ...

    def block_index(self, shard_key: ShardKey) -> tuple[int, ...]:
        """Storage block index for this shard in the Zarr array.

        Pure arithmetic on ``shard_key`` (the parent nested id for HEALPix,
        the row/col block for rectilinear).
        """
        ...

    def shard_label(self, shard_key: ShardKey) -> str:
        """External string form of a shard key (issue #199).

        Used wherever a shard id surfaces outside the process — hive leaf
        ids, async ``.status`` object keys, log lines. HEALPix renders the
        packed word as its decimal morton string (D1 in
        ``docs/design/sparse_coverage.md``); rectilinear keeps the packed tile
        int's decimal digits.
        """
        ...

    def shard_footprint(self, shard_key: ShardKey):
        """Shard polygon in WGS84 (shapely.Geometry)."""
        ...

    def children(self, shard_key: ShardKey) -> np.ndarray:
        """Enumerate cell ids under a shard, in canonical chunk order."""
        ...

    def encode_cell_ids(self, cell_ids: np.ndarray) -> np.ndarray:
        """Encode cell ids to the output coord array (e.g., morton → healpix)."""
        ...

    def chunk_coords(self, shard_key: ShardKey) -> dict:
        """Per-cell coord column values for a chunk (HEALPix: morton, cell_ids).

        Empty for grids that store coords as 1D dimensional arrays on the
        template (e.g., rectilinear's ``x``/``y``).
        """
        ...

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write a Zarr template (group + arrays) for this grid to ``store``."""
        ...

    def signature(self) -> dict:
        """Canonical fingerprint of the grid's defining parameters.

        Recorded in a ShardMap at build time and compared at run time so a
        shard map can never be silently paired with a mismatched grid.
        """
        ...

    def nests_with(self, other: "OutputGrid") -> bool:
        """Whether this grid and ``other`` tile compatibly (align + nest).

        The primitive for cross-aggregator compatibility validation: same
        family, aligned, whole-number resolution ratios, and the same Option-B
        output-field set (issue #29 — same scalar/vector kinds, trailing
        shapes, dtypes). Cross-family (HEALPix vs rectilinear) always returns
        False.
        """
        ...


def shard_label(grid, shard_key) -> str:
    """External string form of ``shard_key`` under ``grid`` (issue #199).

    Dispatches to ``grid.shard_label`` (decimal morton string for HEALPix,
    plain int digits for rectilinear) and falls back to ``str(int(...))`` for
    minimal grid stand-ins that don't implement the method (the same tolerance
    the worker extends to ``iter_chunks``).
    """
    fn = getattr(grid, "shard_label", None)
    return fn(shard_key) if fn is not None else str(int(shard_key))


__all__ = [
    "OutputGrid",
    "RAGGED_ELEMENT_ATTR",
    "RAGGED_SPEC",
    "RAGGED_ZSTD_LEVEL",
    "WEIGHTS_ATTR",
    "WEIGHTS_KINDS",
    "weights_declaration",
    "TIMES_ATTR",
    "LOCATED_ATTR",
    "LOCATED_GRAMMAR",
    "LOCATED_SHAPE_PER_CENTROID",
    "LOCATED_SPEC",
    "located_declaration",
    "located_declaration_block",
    "ShardKey",
    "InconsistentShardError",
    "shard_label",
    "ragged_array_spec",
    "ragged_locations_name",
    "ragged_times_name",
    "vector_array_spec",
    "vlen_dtype_warning_suppressed",
    "chunk_array_spec",
    "sharded_array_spec",
]
