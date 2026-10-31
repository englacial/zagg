"""Per-field ``aggregation.variables`` validators (issue #677 phase 0)."""

from zagg.config.expressions import _is_numeric, _validate_expression_columns, resolve_function

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
    # Shape (a mapping of non-blank names, exactly one of function/expression,
    # a valid dtype) is the model's (``models_aggregation.PrecomputeSpec``);
    # the column cross-references below need ``ds_vars``.
    from zagg.time_axis import TOC_WORD_COLUMN

    reserved = ds_vars | {"leaf_id", TOC_WORD_COLUMN}
    for name, meta in precompute.items():
        if name in reserved:
            raise ValueError(
                f"chunk_precompute '{name}': name collides with a "
                f"data_source.variables column or a reserved derived column "
                f"('leaf_id', {TOC_WORD_COLUMN!r}); the per-cell namespace merge "
                f"would shadow the real column with a chunk scalar. Rename the "
                f"precompute entry."
            )
        has_func = "function" in meta
        has_expr = "expression" in meta
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


# Recognized per-field output kinds. ``ragged`` is the Tier-2 carrier
# for variable-length per-cell outputs; see issue #48.
OUTPUT_KINDS = ("scalar", "vector", "ragged")

# Recognized per-field output resolutions (issue #30 item 2). ``cell`` (default)
# stores one value per aggregation cell; ``chunk`` stores one value per chunk in a
# companion array shaped at the chunk grid, indexed by ``grid.block_index``.
OUTPUT_RESOLUTIONS = ("cell", "chunk")


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
    """Validate one field's output declaration through :class:`FieldSpec`.

    The per-field grammar (kind/shape keys, ``resolution``, ``location``,
    ``temporal`` shape, ``weights``, ``overview_delta``, ``attrs``) is the
    model's (issue #677 phase 2); this is the single-field entry point the
    tests exercise, with the message prefixed by the variable name as before.
    A well-formed ``temporal`` declaration is then gated on a producer and the
    store's clock — :func:`_validate_temporal_producer`.
    """
    from pydantic import ValidationError

    from zagg.config.models import format_validation_error
    from zagg.config.models_aggregation import FieldSpec

    try:
        FieldSpec.model_validate(meta)
    except ValidationError as exc:
        raise ValueError(f"Variable '{name}': {format_validation_error(exc)}") from exc
    if meta.get("temporal") is not None:
        _validate_temporal_producer(name, meta, config)
