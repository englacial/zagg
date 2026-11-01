"""``output`` block validators: store layout, clocks, pyramid (issue #677 phase 0)."""

import logging

import numpy as np

from zagg.config.accessors import (
    get_agg_fields,
    get_pyramid,
)
from zagg.config.core import PipelineConfig
from zagg.config.source import _segment_variable_names
from zagg.config.windowing import get_windowing

logger = logging.getLogger(__name__)


def _validate_time_source(config: PipelineConfig) -> None:
    """Validate ``output.time_source`` — the per-observation clock (issue #410).

    The block a temporal companion's words are encoded from:
    ``{field, epoch, scale, units}`` (see :func:`zagg.time_axis.toc_source`).
    Three rules beyond shape:

    - ``field`` must be a BASE-RATE ``data_source.variables`` column, the same
      constraint ``output.windowing.time_field`` carries — a companion is one
      word per observation, so a coordinate or a segment-level column would
      encode at the wrong rate;
    - ``scale`` must be continuous (:data:`zagg.time_axis.TOC_SOURCE_SCALES`).
      ``utc`` is refused by name rather than silently tolerated: §8.3 requires
      an instant exact to the nanosecond and a nominal-UTC column is only good
      to the leap seconds since its epoch;
    - the declared column must not be :data:`zagg.time_axis.TOC_WORD_COLUMN`
      itself, which is the DERIVED word column, not a source.

    Declaring the block without any temporal companion is legal (it materializes
    the derived column, which an expression may read); the converse is not — see
    :func:`_validate_temporal_producer`.
    """
    from zagg.time_axis import TOC_SOURCE_SCALES
    from zagg.windows import parse_utc

    block = (config.output or {}).get("time_source")
    if block is None:
        return
    # Shape, the derived-column rule, ``scale``/``units`` and the epoch parse
    # are the model's (``models_output.TimeSource``); what remains needs the
    # data_source (the declared column) and the windowing block (the two clocks).
    field = block["field"]
    scale = block.get("scale") or TOC_SOURCE_SCALES[0]
    units = block.get("units") or "seconds"
    # Base-rate means "one value per observation AFTER the read", which includes
    # a coarser-level variable the reader broadcasts down (issue #30): GEDI's
    # clock is shot-rate and every sample of a waveform carries its shot's
    # instant, which is exactly what makes a single-shot cell's centroids exact
    # timestamps. So the accepted set is the same one every other column
    # reference validates against — declared variables plus broadcast level
    # variables — and NOT ``data_source.variables`` alone.
    declared = set((config.data_source or {}).get("variables") or {}) | _segment_variable_names(
        config.data_source or {}
    )
    if field not in declared:
        raise ValueError(
            f"output.time_source.field {field!r} is not a declared data_source variable "
            f"(available, including broadcast level variables: {sorted(declared)}) — a "
            f"temporal companion is one word per observation, so the column must reach "
            f"base rate"
        )
    # Cross-check the two clocks (issue #410 review). ``toc_source``'s fallback
    # single-sources the clock only when THIS block is absent, so when both are
    # declared nothing else stops a store from routing windows on one clock and
    # encoding its §8.3 words from another — unbounded disagreement, not a
    # one-quantum edge effect. A windowing block on ``scale: utc`` is the
    # deliberate exception: it gets no fallback (a nominal-UTC column cannot
    # carry a nanosecond-exact word), so an explicit continuous declaration is
    # the only way to carry words there, and the residual routing gap is
    # windows.py's own documented tolerance.
    try:
        windowing = get_windowing(config)
    except (KeyError, TypeError, ValueError):
        return  # malformed windowing block: _validate_windowing names it
    if windowing is None or windowing["scale"] not in TOC_SOURCE_SCALES:
        return
    disagree = [
        (key, value, windowing[wkey])
        for key, wkey, value in (
            ("field", "time_field", field),
            ("scale", "scale", scale),
            ("units", "units", units),
        )
        if value != windowing[wkey]
    ]
    # The epoch is compared against the DECLARED windowing value, not the
    # normalized dict's: get_windowing renders through ``iso_utc``, so comparing
    # against that would report a config declaring both blocks with the SAME
    # sub-second epoch as two disagreeing clocks, when the only difference is
    # that one side has been rendered. Sub-second epochs are refused on their
    # own terms by _validate_windowing's #390 guard, which runs after this
    # validator. Reaching this line means get_windowing took its point branch
    # (the raster branch fixes ``scale`` to ``utc``, which returned above) and
    # parsed a declared epoch off the block, so the key is present and parses.
    declared_epoch = config.output["windowing"]["epoch"]
    if parse_utc(block["epoch"]) != parse_utc(declared_epoch):
        disagree.append(("epoch", block["epoch"], declared_epoch))
    if disagree:
        detail = ", ".join(f"{k}: {a!r} vs {b!r}" for k, a, b in disagree)
        raise ValueError(
            f"output.time_source disagrees with output.windowing ({detail}) — window "
            f"routing and the spec §8.3 toc words would run on two different clocks, so "
            f"an observation could be routed into a window its own stored word reads as "
            f"outside of. Make them agree, or drop output.time_source and let it derive "
            f"from output.windowing (issue #410)"
        )


def _validate_pyramid(config: PipelineConfig) -> None:
    """Validate the ``output.pyramid`` knob's grammar (issue #201).

    Only an EXPLICIT block is checked (absent/false are always legal); an
    explicit block requires the hive store layout, like ``sweep`` — the
    pyramid materializes through the hive manifest.
    """
    raw = config.output.get("pyramid")
    if raw is None or raw is False:
        return
    # Grammar, the hive requirement, ``orders``/``overviews`` against the grid
    # and the fold knobs are the model's (``models_output.Pyramid`` and
    # ``Output._pyramid_rules``); ``summarize`` names aggregation fields, so
    # that cross-check stays here.
    knob = get_pyramid(config) or {}
    summarize = knob.get("summarize")
    if summarize is None:
        return
    fields = get_agg_fields(config)
    for source, decl in summarize.items():
        if source not in fields:
            raise ValueError(
                f"output.pyramid.summarize names unknown field {source!r} "
                f"(declared: {sorted(fields)})"
            )
        target = decl["as"]
        if target in fields:
            raise ValueError(
                f"output.pyramid.summarize.{source}.as = {target!r} collides with a "
                f"declared field: derived summaries must use a DIFFERENT name so overview "
                f"schema never silently differs from source (D24)"
            )


_NAN_AMBIGUOUS_REDUCTIONS = ("min", "max", "sum")


def _warn_nan_ambiguous_reductions(config: PipelineConfig) -> None:
    """Warn when a plain ``min``/``max``/``sum`` pairs with a NaN fill (#201).

    Warning, not error — the pairing is legal (the shipped atl06 config uses
    it by design), but the store encoding has a consequence the author should
    have chosen knowingly: a NaN datum and the fill sentinel are the same
    bytes, so downstream the reduction behaves as its nan-skipping form (the
    pyramid block declares this as ``nan_policy: "skip"``) and a
    NaN-propagating result is unrecoverable. Declaring ``nanmin``/``nanmax``/
    ``nansum`` states the same semantics explicitly and silences this.
    """
    from zagg.semantics import _fold_function_name, field_composability

    for name, meta in get_agg_fields(config).items():
        fn = _fold_function_name(meta.get("function"))
        if fn not in _NAN_AMBIGUOUS_REDUCTIONS or not _is_nan_fill(meta):
            continue
        try:
            if field_composability(meta) != "exact":
                continue
        except Exception:
            continue  # malformed meta: the kind/shape validators own the error
        logger.warning(
            f"aggregation.variables.{name}: {meta.get('function')!r} with a NaN fill_value — "
            f"NaN data is indistinguishable from fill at read; the reduction behaves as "
            f'nan{fn} in overviews (declared in the pyramid block as nan_policy: "skip") '
            f"and any NaN-carrying result is unrecoverable — use nan{fn} explicitly to "
            f"silence this (issue #201)"
        )


def _is_nan_fill(meta: dict) -> bool:
    """Whether a field's fill sentinel is NaN (explicitly, or by float default)."""
    fill = meta.get("fill_value")
    if fill is None:
        try:
            return np.issubdtype(np.dtype(meta.get("dtype", "float32")), np.floating)
        except TypeError:
            return False
    if isinstance(fill, str):
        return fill.strip().lower() == "nan"
    return isinstance(fill, float) and np.isnan(fill)
