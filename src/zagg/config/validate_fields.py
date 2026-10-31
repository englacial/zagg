"""Per-field ``aggregation.variables`` validators (issue #677 phase 0)."""

import logging

import numpy as np

from zagg.config.expressions import _is_numeric, _validate_expression_columns, resolve_function

logger = logging.getLogger(__name__)


def _validate_composition_attrs(name: str, meta: dict, attrs: dict, agg_vars: dict) -> None:
    """Cross-check a ``composition`` attrs block against the field it describes.

    The whole field rests on one invariant: the ``N`` a reader divides the lanes
    by is the total weight of the digest named by ``of``, and the lanes were
    packed at the ``threshold`` recorded here (issue #321). Both halves are
    silent-wrong-answer failures — lanes stay in range, only the recovered
    counts are wrong — so they are checked at load:

    * ``of`` must name another ``aggregation.variables`` entry of
      ``kind: ragged`` (a typo or a later rename of the digest field would
      otherwise leave readers dereferencing a field that does not exist);
    * ``threshold`` must equal the field's own ``params.threshold`` (the value
      the reducer actually packs at);
    * ``fill_value`` must be ``0`` — an empty signal stratum packs to ``0`` and
      readers key presence off ``lane > 0``, so a nonzero fill would make every
      *unwritten* cell report spurious flag presence (spec §3).

    The block's other two keys are **spec-owned**, not author config: the
    template stamps ``spec``/``lanes`` from
    :data:`~zagg.stats.composition.COMPOSITION_SPEC` /
    :data:`~zagg.stats.composition.LANES`
    (:func:`~zagg.stats.composition.composition_attrs_block`), and a config
    that *declares* either one must agree with the constant. Lane order in
    particular is not a knob — :func:`~zagg.stats.composition.pack_composition`
    packs in fixed ``LANES`` order — so a permuted or truncated declaration
    would mislabel every lane for a reader binding to it per spec §3.3 and is
    rejected here (review finding, issue #340).

    The third coupling — the digest fields' ``where`` predicates committing the
    same cut as ``threshold`` — is *not* validated here: ``where`` is an
    arbitrary expression, so any check would be a regex over Python source that
    both misses real drift (``> 1`` is the same cut as ``>= 2``) and rejects
    valid predicates. It is stated in :func:`zagg.stats.composition.pack_composition`'s
    docstring instead, where it travels with the function rather than with one
    config template.
    """
    block = attrs.get("composition")
    if not isinstance(block, dict):
        return
    from zagg.stats.composition import COMPOSITION_SPEC, LANES

    spec = block.get("spec")
    if spec is not None and spec != COMPOSITION_SPEC:
        raise ValueError(
            f"Variable '{name}': attrs.composition.spec {spec!r} is not the convention "
            f"this writer packs ({COMPOSITION_SPEC!r}) — the marker is stamped from "
            f"zagg.stats.composition, not declared (issue #340)"
        )
    lanes = block.get("lanes")
    if lanes is not None and list(lanes) != list(LANES):
        raise ValueError(
            f"Variable '{name}': attrs.composition.lanes must be the {COMPOSITION_SPEC} "
            f"lane order {list(LANES)} — pack_composition packs lane i at bits 8i..8i+7 "
            f"in that fixed order, so a permuted declaration would mislabel every lane"
        )
    fill = meta.get("fill_value")
    if fill != 0:
        raise ValueError(
            f"Variable '{name}': a composition field must declare fill_value: 0 (got "
            f"{fill!r}) — an empty signal stratum packs to 0, and readers key presence "
            f"off 'lane > 0', so a nonzero fill makes every unwritten cell report "
            f"spurious flag presence (spec §3)"
        )
    of = block.get("of")
    if of is not None:
        if of == name:
            raise ValueError(
                f"Variable '{name}': attrs.composition.of names the composition field "
                f"itself; it must name the signal digest whose total weight is N_signal"
            )
        target = agg_vars.get(of)
        if target is None:
            raise ValueError(
                f"Variable '{name}': attrs.composition.of '{of}' is not a declared "
                f"aggregation.variables field (readers resolve N_signal through it)"
            )
        if target.get("kind") != "ragged":
            raise ValueError(
                f"Variable '{name}': attrs.composition.of '{of}' must be a "
                f"'kind: ragged' digest field (got kind "
                f"{target.get('kind', 'scalar')!r}) — N_signal is its total weight"
            )
    threshold = block.get("threshold")
    if threshold is not None:
        packed = (meta.get("params") or {}).get("threshold")
        if packed is not None and packed != threshold:
            raise ValueError(
                f"Variable '{name}': attrs.composition.threshold {threshold!r} disagrees "
                f"with params.threshold {packed!r} — the recorded threshold must be the "
                f"one the reducer packs at (issue #321)"
            )


# Required per-variable keys for a temporal/event aggregation spec. ``mask``
# is optional (``specs_from_config`` defaults it to ``"ais"``); capability
# *names* (spatial_func / temporal_reducer / mask) are resolved by the registry
# at run time, not here -- a typo surfaces as a clean ``UnknownCapability`` from
# ``process_event`` rather than a load-time guess about which plugins are
# installed.
_TEMPORAL_SPEC_KEYS = ("variable", "collection", "spatial_func", "temporal_reducer")


def _validate_chunk_precompute(aggregation: dict, ds_vars: set[str]) -> None:
    """Validate the ``aggregation.chunk_precompute`` block (issue #30, item 1).

    Each named entry is a chunk-level scalar computed ONCE per chunk (shard) over
    the shard's pooled column data, before the per-cell loop; its name then enters
    the per-cell expression namespace. Validation mirrors ``aggregation.variables``:
    each entry must declare exactly one of ``function``/``expression``, and any
    ``source`` / expression / param column references must exist in
    ``data_source.variables``. ``dtype`` (optional) must be a valid numpy dtype.

    A precompute name must not collide with a ``data_source.variables`` column or
    a reserved namespace name (``leaf_id``): the per-cell namespace is built as
    ``{**cell_data, **chunk_scalars}`` in :func:`zagg.processing.process_shard`, so
    a colliding name would shadow the real column *array* with a 0-d *scalar* and
    corrupt every cell. Such names are rejected here.

    Inter-precompute references are NOT supported: an entry's expression is
    evaluated only over the pooled columns (:func:`zagg.processing._eval_chunk_precompute`
    iterates the entries independently, with no defined order), so one entry cannot
    reference another (e.g. ``chunk_gain`` cannot read ``chunk_offset``). A name
    that references another precompute entry is rejected as an unknown column.

    The block is optional; a config without it is unchanged.

    Parameters
    ----------
    aggregation : dict
        The config's ``aggregation`` mapping.
    ds_vars : set[str]
        Available ``data_source.variables`` column names.

    Raises
    ------
    ValueError
        On any invalid ``chunk_precompute`` declaration.
    """
    precompute = aggregation.get("chunk_precompute")
    if precompute is None:
        return
    if not isinstance(precompute, dict):
        raise ValueError("aggregation.chunk_precompute must be a mapping of name -> entry")
    from zagg.time_axis import TOC_WORD_COLUMN

    reserved = ds_vars | {"leaf_id", TOC_WORD_COLUMN}
    for name, meta in precompute.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("chunk_precompute entry names must be non-empty strings")
        if name in reserved:
            raise ValueError(
                f"chunk_precompute '{name}': name collides with a "
                f"data_source.variables column or a reserved derived column "
                f"('leaf_id', {TOC_WORD_COLUMN!r}); the per-cell namespace merge "
                f"would shadow the real column with a chunk scalar. Rename the "
                f"precompute entry."
            )
        if not isinstance(meta, dict):
            raise ValueError(f"chunk_precompute '{name}': entry must be a mapping")

        has_func = "function" in meta
        has_expr = "expression" in meta
        if has_func and has_expr:
            raise ValueError(
                f"chunk_precompute '{name}': 'function' and 'expression' are mutually exclusive"
            )
        if not has_func and not has_expr:
            raise ValueError(f"chunk_precompute '{name}': must specify 'function' or 'expression'")

        source = meta.get("source")
        if source is not None and source not in ds_vars:
            raise ValueError(
                f"chunk_precompute '{name}': source '{source}' not in data_source.variables"
            )

        if has_func:
            resolve_function(meta["function"])  # raises ValueError on failure

        for pval in meta.get("params", {}).values():
            if not isinstance(pval, str):
                continue  # numeric literal
            if pval in ds_vars or _is_numeric(pval):
                continue
            if any(v in pval for v in ds_vars):
                continue
            raise ValueError(
                f"chunk_precompute '{name}': param value '{pval}' references "
                f"unknown column (available: {ds_vars})"
            )

        if has_expr:
            _validate_expression_columns(name, meta["expression"], ds_vars)

        if "dtype" in meta:
            try:
                np.dtype(meta["dtype"])
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"chunk_precompute '{name}': dtype {meta['dtype']!r} is not a valid "
                    f"numpy dtype ({e})"
                ) from e


# Recognized per-field output kinds. ``ragged`` is the Tier-2 carrier
# for variable-length per-cell outputs; see issue #48.
OUTPUT_KINDS = ("scalar", "vector", "ragged")

# Recognized per-field output resolutions (issue #30 item 2). ``cell`` (default)
# stores one value per aggregation cell; ``chunk`` stores one value per chunk in a
# companion array shaped at the chunk grid, indexed by ``grid.block_index``.
OUTPUT_RESOLUTIONS = ("cell", "chunk")


def _validate_temporal_shape(name: str, meta: dict, kind: str, config=None) -> None:
    """Validate a field's ``temporal:`` declaration (spec §8.2/§8.3, #410).

    The declared value is a §8 **shape**, and each shape pins the array that
    will hold the words:

    - ``per-cell`` — the field's own DENSE array is the companion, so it must
      be a ``uint64`` scalar field at cell resolution, declaring the §8.2
      reserved ``fill_value: 0`` that marks an unobserved cell EXPLICITLY (the
      template's dense default is ``"NaN"``, so an assumed 0 here would only
      defer the failure to zarr);
    - ``per-centroid`` — a uint64 ragged SIBLING row-aligned with the field's
      digest, so the field must be ``kind: ragged`` at cell resolution; the
      sibling shares the payload's per-cell row counts by construction, which
      a chunk-uniform collapse would destroy.

    ``coordinate`` is deliberately not accepted here: a time axis is declared
    by ``output.time_encoding`` (§8.1), not by an aggregation variable.

    A well-formed declaration is then gated on a PRODUCER and on the store's
    per-observation clock — see :func:`_validate_temporal_producer`. The shape
    checks run first, and deliberately: they describe the field itself, so they
    are the more useful message when both are wrong.
    """
    from zagg.time_axis import (
        TOC_FIELD_SHAPES,
        TOC_SHAPE_COORDINATE,
        TOC_SHAPE_PER_CELL,
        TOC_SHAPE_PER_CENTROID,
        TOC_UNOBSERVED,
    )

    shape = meta.get("temporal")
    if shape is None:
        return
    if shape == TOC_SHAPE_COORDINATE:
        raise ValueError(
            f"Variable '{name}': temporal shape {TOC_SHAPE_COORDINATE!r} is the time "
            f"axis' own declaration — set 'output.time_encoding: toc' instead (spec §8.1)"
        )
    if shape not in TOC_FIELD_SHAPES:
        raise ValueError(
            f"Variable '{name}': temporal {shape!r} is not one of {TOC_FIELD_SHAPES} (spec §8)"
        )
    if meta.get("resolution", "cell") != "cell":
        raise ValueError(
            f"Variable '{name}': 'temporal' is not supported with 'resolution: chunk' "
            f"— a temporal companion is per cell or per centroid (spec §8.2/§8.3)"
        )
    if shape == TOC_SHAPE_PER_CELL:
        if kind != "scalar":
            raise ValueError(
                f"Variable '{name}': temporal {TOC_SHAPE_PER_CELL!r} is a dense per-cell "
                f"companion, so kind must be 'scalar', not {kind!r} (spec §8.2)"
            )
        if str(meta.get("dtype")) != "uint64":
            raise ValueError(
                f"Variable '{name}': temporal {TOC_SHAPE_PER_CELL!r} requires "
                f"dtype 'uint64' (got {meta.get('dtype')!r}) — the words are packed "
                f"uint64 (spec §8.2)"
            )
        # No default here. The dense template's own default is ``"NaN"``
        # (``grids/healpix.py``, ``grids/rectilinear.py``), so assuming 0 for
        # an absent key would pass validation and then die in the template
        # with a bare zarr ``TypeError: Invalid type: NaN`` — the §8.2
        # message below is what the author must see instead. A spec-owned
        # reservation is declared, never inferred, exactly as §2.0's
        # ``weights`` is.
        if "fill_value" not in meta:
            raise ValueError(
                f"Variable '{name}': temporal {TOC_SHAPE_PER_CELL!r} requires an explicit "
                f"fill_value {TOC_UNOBSERVED} — §8.2 reserves it as the unobserved-cell "
                f"marker, and the dense default ('NaN') is not a uint64 word"
            )
        fill = meta["fill_value"]
        if isinstance(fill, bool) or fill != TOC_UNOBSERVED:
            raise ValueError(
                f"Variable '{name}': temporal {TOC_SHAPE_PER_CELL!r} requires "
                f"fill_value {TOC_UNOBSERVED} (got {fill!r}) — §8.2 reserves it as the "
                f"unobserved-cell marker"
            )
        _validate_temporal_producer(name, meta, config)
        return
    if kind != "ragged":
        raise ValueError(
            f"Variable '{name}': temporal {TOC_SHAPE_PER_CENTROID!r} is a sibling of a "
            f"ragged payload, so kind must be 'ragged', not {kind!r} (spec §8.3)"
        )
    _validate_temporal_producer(name, meta, config)


def _validate_temporal_producer(name: str, meta: dict, config=None) -> None:
    """Refuse a ``temporal:`` companion nothing in this release would produce.

    A declaration whose reducer emits no toc words would validate, run, stamp
    the §8 declaration, and write a store violating the section it declares —
    an all-empty ``{field}_times`` sibling beside a populated payload (§8.3's
    row-alignment MUST), or a dense array holding the field's own reducer output
    cast to ``uint64`` under a ``zagg-toc/1`` stamp (§8.2's envelope claim). The
    gate is therefore an allowlist,
    :data:`zagg.time_axis.TOC_PRODUCING_FUNCTIONS`, split by the shape each
    reducer can serve: :data:`~zagg.time_axis.TOC_PER_CELL_FUNCTIONS` produce a
    whole-cell word, the rest carry the channel beside a ragged payload.

    A producing reducer is necessary but not sufficient: the words have to come
    from somewhere, so a companion also requires the store's per-observation
    clock (``output.time_source``, :func:`zagg.time_axis.toc_source` — which
    falls back to a continuous-scale ``output.windowing``). ``config`` is
    optional only so the per-field checks stay callable in isolation; every
    ``validate_config`` path passes it.
    """
    from zagg.time_axis import (
        TOC_NO_CLOCK_ERROR,
        TOC_PER_CELL_FUNCTIONS,
        TOC_PRODUCING_FUNCTIONS,
        TOC_SHAPE_PER_CELL,
        toc_source,
    )

    shape = meta.get("temporal")
    func = meta.get("function")
    if func not in TOC_PRODUCING_FUNCTIONS:
        raise ValueError(
            f"Variable '{name}': 'temporal' declares a companion that function {func!r} "
            f"does not produce — only {sorted(TOC_PRODUCING_FUNCTIONS)} emit toc words "
            f"(spec §8.2/§8.3, issue #410)"
        )
    per_cell_reducer = func in TOC_PER_CELL_FUNCTIONS
    if per_cell_reducer != (shape == TOC_SHAPE_PER_CELL):
        wants = TOC_SHAPE_PER_CELL if per_cell_reducer else "per-centroid"
        raise ValueError(
            f"Variable '{name}': function {func!r} produces the {wants!r} temporal "
            f"shape, not {shape!r} — a whole-cell reducer cannot fill a per-centroid "
            f"sibling, and a digest kernel's channel is not a dense per-cell array "
            f"(spec §8.2/§8.3)"
        )
    # The clock cross-check runs through the same resolver the worker encodes
    # with (``toc_source``: ``output.time_source``, falling back to a
    # continuous-scale ``output.windowing`` block), and raises the worker's
    # exact message so the two seams read identically (issue #472) — the
    # worker's copy stays as defense in depth. Two limits on that parity, both
    # named here because this is where a reader will look (fold review):
    #   * this seam is reachable only from ``validate_config``'s spatial,
    #     non-raster branch (the aggregation-variable loop, after both early
    #     returns), while ``_toc_word_column`` is ungated. Benign today —
    #     ``calculate_cell_statistics`` is only reached from the spatial point
    #     path, so no other branch can produce toc words — but a raster or
    #     event toc path would inherit the gap silently.
    #   * the resolver is shared, but the two clock declarations validate their
    #     COLUMN against different sets: ``_validate_time_source`` accepts a
    #     broadcast/segment-level column (GEDI's shot-rate clock) where
    #     ``_validate_windowing`` refuses one (segment-rate window membership
    #     is unsupported), so the fallback is not interchangeable with the
    #     explicit block.
    if config is not None and toc_source(config) is None:
        raise ValueError(f"Variable '{name}': {TOC_NO_CLOCK_ERROR}")


def _validate_output_kind(name: str, meta: dict, config=None) -> None:
    """Validate a variable's non-scalar output declaration.

    A field may declare ``kind`` (``scalar`` default, ``vector``, or ``ragged``)
    and a shape key (``trailing_shape`` for ``vector``, ``inner_shape`` for
    ``ragged``). ``scalar`` fields need neither and stay the default path.
    ``vector`` and ``ragged`` fields may be driven by either ``function`` or
    ``expression``; ``len``/``count`` are rejected for both (they short-circuit
    to a scalar count). See issue #29 (vector) and issue #48 (ragged). A ragged
    field may additionally declare ``weights`` (the spec §2.0 counts/flux
    payload declaration; flux requires ``gain`` provenance attrs) and
    ``overview_delta`` (the split pyramid-fold budget) — both issue #424,
    both rejected on other kinds.

    A field may also declare ``resolution`` (``cell`` default, or ``chunk``).
    A ``resolution: chunk`` field (issue #30 item 2) is written ONCE per chunk
    into a companion array shaped at the chunk grid (``main.shape //
    chunk_shape``), indexed by ``grid.block_index(shard_key)`` — the compact
    storage for a chunk-uniform value (e.g. a ``chunk_precompute`` anchor).
    ``scalar``, ``vector``, and ``ragged`` kinds may all be ``resolution: chunk``
    (issue #82): a ``scalar`` companion is a plain chunk-grid array, a ``vector``
    companion appends the field's ``trailing_shape`` to the chunk grid (chunked
    whole), and a ``ragged`` companion holds one
    variable-length payload per chunk, written by ``write_ragged_to_zarr`` (phase
    4c). The shape keys are validated below exactly as for cell resolution — the
    chunk axis just replaces the cell axis.

    Parameters
    ----------
    name : str
        Variable name (for error messages).
    meta : dict
        The variable's aggregation metadata.

    Raises
    ------
    ValueError
        On any invalid output-kind declaration.
    """
    kind = meta.get("kind", "scalar")
    if kind not in OUTPUT_KINDS:
        allowed = ", ".join(OUTPUT_KINDS)
        raise ValueError(
            f"Variable '{name}': output kind '{kind}' is not supported (allowed: {allowed})"
        )

    # ``location`` (issue #87) is the ragged location channel; other kinds have
    # no companion ragged array to carry it.
    if "location" in meta and kind != "ragged":
        raise ValueError(
            f"Variable '{name}': 'location' is only valid for kind 'ragged', not '{kind}'"
        )

    # ``weights`` (spec §2.0, issue #424) declares a digest payload's
    # weight-column semantics; ``overview_delta`` (issue #424) is the split
    # pyramid-fold compression budget. Both describe a ragged digest payload —
    # nothing else has a weight column or an overview digest fold.
    for key in ("weights", "overview_delta"):
        if key in meta and kind != "ragged":
            raise ValueError(
                f"Variable '{name}': '{key}' is only valid for kind 'ragged', not '{kind}'"
            )

    _validate_temporal_shape(name, meta, kind, config)

    # resolution (cell default, or chunk). A chunk-resolution field stores one
    # value per chunk in a companion array (issue #30 item 2). ``scalar`` and
    # ``vector`` chunk companions are wired (issue #82): a scalar companion is a
    # plain chunk-grid array, a vector companion appends the field's
    # ``trailing_shape`` to the chunk grid (chunked whole). The kind-specific shape
    # keys are validated by the per-kind branches below regardless of resolution.
    resolution = meta.get("resolution", "cell")
    if resolution not in OUTPUT_RESOLUTIONS:
        allowed = ", ".join(OUTPUT_RESOLUTIONS)
        raise ValueError(
            f"Variable '{name}': resolution '{resolution}' is not supported (allowed: {allowed})"
        )
    # ``ragged`` at chunk resolution (issue #82) stores one variable-length
    # payload per chunk instead of per cell. It rides the same vlen writer as
    # cell-resolution ragged (``write_ragged_to_zarr``), which collapses
    # the populated cells to the single chunk payload under the same chunk-uniform
    # contract as scalar/vector chunk companions (raise if populated cells
    # disagree). No special shape key is needed beyond ``inner_shape``.

    # dtype, when declared, must name a real numpy dtype (applies to all kinds).
    if "dtype" in meta:
        try:
            np.dtype(meta["dtype"])
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Variable '{name}': dtype {meta['dtype']!r} is not a valid numpy dtype ({e})"
            ) from e

    has_trailing = "trailing_shape" in meta

    if kind == "scalar":
        if has_trailing:
            raise ValueError(
                f"Variable '{name}': 'trailing_shape' is only valid for kind 'vector', not 'scalar'"
            )
        return

    if kind == "vector":
        # trailing_shape is required and must be positive ints.
        if not has_trailing:
            raise ValueError(f"Variable '{name}': kind 'vector' requires 'trailing_shape'")
        _validate_trailing_shape(name, meta["trailing_shape"])

        # ``len``/``count`` short-circuit to a scalar obs count in
        # ``calculate_cell_statistics``; pairing them with kind 'vector' would
        # silently emit a scalar, so reject the nonsensical combination.
        if meta.get("function") in ("len", "count"):
            raise ValueError(
                f"Variable '{name}': function {meta['function']!r} produces a scalar "
                f"count and cannot be combined with kind 'vector'"
            )
        return

    # kind == "ragged": inner_shape is required; trailing_shape is rejected.
    if has_trailing:
        raise ValueError(
            f"Variable '{name}': 'trailing_shape' is only valid for 'vector', not 'ragged'"
        )
    if "inner_shape" not in meta:
        raise ValueError(f"Variable '{name}': kind 'ragged' requires 'inner_shape'")
    _validate_trailing_shape(name, meta["inner_shape"], key_name="inner_shape")

    # The §2.0 weights declaration (issue #424): "counts" (integer weights,
    # sum exact — the absent-key default) or "flux" (positive reals, sum an
    # estimate of detected photoelectrons). A flux field MUST carry its
    # calibration provenance — the spec requires a ``gain`` attrs mapping
    # naming at minimum the gain constant's name and version — so a store
    # never holds calibrated weights whose calibration is unrecoverable.
    weights = meta.get("weights")
    if weights is not None:
        from zagg.grids.base import WEIGHTS_KINDS

        if weights not in WEIGHTS_KINDS:
            raise ValueError(
                f"Variable '{name}': weights {weights!r} is not one of {WEIGHTS_KINDS} (spec §2.0)"
            )
        if weights == "flux":
            gain = (meta.get("attrs") or {}).get("gain")
            if not isinstance(gain, dict) or not {"name", "version"} <= set(gain):
                raise ValueError(
                    f"Variable '{name}': weights 'flux' requires calibration provenance "
                    f"in attrs — a 'gain' mapping with at least 'name' and 'version' "
                    f"(spec §2.0, issue #424)"
                )

    # ``overview_delta`` (issue #424): the pyramid/overview fold budget, split
    # from the leaf ``params.delta`` (leaf δ is the loss-free bound, overview δ
    # the ~1/δ accuracy bound). A top-level key, NOT a params entry — params
    # values are forwarded to the reducer as kwargs, which never take it.
    overview_delta = meta.get("overview_delta")
    if overview_delta is not None:
        if not isinstance(overview_delta, int) or isinstance(overview_delta, bool):
            raise ValueError(
                f"Variable '{name}': overview_delta must be a positive int (got {overview_delta!r})"
            )
        if overview_delta < 1:
            raise ValueError(
                f"Variable '{name}': overview_delta must be a positive int (got {overview_delta!r})"
            )

    # Same restriction as vector: ``len``/``count`` produce a scalar count.
    if meta.get("function") in ("len", "count"):
        raise ValueError(
            f"Variable '{name}': function {meta['function']!r} produces a scalar "
            f"count and cannot be combined with kind 'ragged'"
        )

    # Optional location channel (issue #87): the reducer also receives the named
    # per-observation morton column (``locations=`` kwarg) and returns a
    # ``(payload, locations)`` pair, stored as a uint64 companion vlen array. Only
    # a ``function`` reducer can accept the kwarg, and the chunk-uniform collapse
    # of ``resolution: chunk`` has no location fold — reject both combinations.
    location = meta.get("location")
    if location is not None:
        if not isinstance(location, str):
            raise ValueError(
                f"Variable '{name}': 'location' must be a column name string (got {location!r})"
            )
        if "function" not in meta:
            raise ValueError(
                f"Variable '{name}': 'location' requires a 'function' reducer "
                f"(an expression cannot receive the locations column)"
            )
        if resolution == "chunk":
            raise ValueError(
                f"Variable '{name}': 'location' is not supported with "
                f"'resolution: chunk' (locations are folded per cell)"
            )


def _validate_trailing_shape(name: str, trailing_shape, key_name: str = "trailing_shape") -> None:
    """Check a shape field (trailing_shape or inner_shape) is a tuple of positive ints."""
    if isinstance(trailing_shape, int):
        dims: tuple = (trailing_shape,)
    elif isinstance(trailing_shape, (list, tuple)):
        dims = tuple(trailing_shape)
    else:
        raise ValueError(
            f"Variable '{name}': '{key_name}' must be an int or a "
            f"sequence of ints (got {trailing_shape!r})"
        )
    if not dims:
        raise ValueError(f"Variable '{name}': '{key_name}' must have at least one dimension")
    for dim in dims:
        if not isinstance(dim, int) or isinstance(dim, bool) or dim < 1:
            raise ValueError(
                f"Variable '{name}': '{key_name}' entries must be positive integers (got {dim!r})"
            )
