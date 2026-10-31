"""``output`` block validators: store layout, clocks, pyramid (issue #677 phase 0)."""

import logging

import numpy as np

from zagg.config.accessors import (
    SWEEP_MODES,
    SWEEP_NONE,
    TOUCH_POLICIES,
    _declares_windowing,
    get_agg_fields,
    get_child_order,
    get_pyramid,
    get_store_layout,
    get_sweep_mode,
)
from zagg.config.core import PipelineConfig
from zagg.config.source import _segment_variable_names
from zagg.config.windowing import get_windowing

logger = logging.getLogger(__name__)


def _validate_time_encoding(config: PipelineConfig) -> None:
    """Validate ``output.time_encoding`` (spec §8, issue #443).

    The knob declares how the ``(time, cells)`` time coordinate is encoded —
    legacy int64 microseconds (absent/default) or mortie toc words. It is
    raster-only: the spatial and temporal pipelines write no time coordinate,
    so a declaration there would be inert.
    """
    from zagg.time_axis import DEFAULT_TIME_ENCODING, time_encoding

    encoding = time_encoding(config)
    if encoding != DEFAULT_TIME_ENCODING and (config.data_source or {}).get("reader") != "raster":
        raise ValueError(
            f"output.time_encoding: {encoding!r} applies to raster (time, cells) "
            f"products only — this pipeline writes no time coordinate (spec §8)"
        )


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
    from zagg.time_axis import TOC_SOURCE_SCALES, TOC_WORD_COLUMN
    from zagg.windows import UNIT_SECONDS, parse_utc

    block = (config.output or {}).get("time_source")
    if block is None:
        return
    if not isinstance(block, dict):
        raise ValueError(
            f"output.time_source must be a mapping {{field, epoch, scale, units}} "
            f"(got {block!r}) — spec §8.3, issue #410"
        )
    unknown = set(block) - {"field", "epoch", "scale", "units"}
    if unknown:
        raise ValueError(
            f"output.time_source has unknown key(s) {sorted(unknown)} "
            f"(known: epoch, field, scale, units)"
        )
    field = block.get("field")
    if not isinstance(field, str) or not field:
        raise ValueError(
            "output.time_source.field is required: the per-observation time column "
            "the temporal companion's words are encoded from (issue #410)"
        )
    if field == TOC_WORD_COLUMN:
        raise ValueError(
            f"output.time_source.field {field!r} is the DERIVED toc word column, not a "
            f"source — name the dataset-time column it is encoded from (issue #410)"
        )
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
    if "epoch" not in block:
        raise ValueError(
            "output.time_source.epoch is required: the UTC instant the column counts from"
        )
    parse_utc(block["epoch"])  # raises with the offending value named
    scale = block.get("scale") or TOC_SOURCE_SCALES[0]
    if scale not in TOC_SOURCE_SCALES:
        raise ValueError(
            f"output.time_source.scale must be a continuous timescale {TOC_SOURCE_SCALES} "
            f"(got {scale!r}); a nominal-UTC column cannot carry the nanosecond-exact "
            f"instant spec §8.3 requires of a timestamp word"
        )
    units = block.get("units") or "seconds"
    if units not in UNIT_SECONDS:
        raise ValueError(
            f"output.time_source.units must be one of {sorted(UNIT_SECONDS)} (got {units!r})"
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


def _validate_store_layout_keys(config: PipelineConfig) -> None:
    """Shared ``output.store_layout`` / ``coverage_moc`` cross-checks.

    Optional store layout (issue #199 phase 2): "hive" (one leaf zarr per
    shard under a morton digit tree — ``docs/design/sparse_coverage.md``
    D1-D6) or "flat" (the single shared store). The default is grid-aware
    (issue #253, ``get_store_layout``): HEALPix -> hive, rectilinear -> flat —
    so the hive gates below resolve through the default, not the raw key.
    Hive ids are morton decimal strings, so the layout is HEALPix-only;
    metadata consolidation assumes the single shared store, so it is rejected
    with hive (explicit or defaulted) rather than silently mis-writing. Called
    from both the point-pipeline and raster validation branches (issue #247:
    raster + hive is legal, so the checks live in one place; the issue #239
    stopgap rejections are gone).

    The end-of-run root coverage MOC (issue #200 phase 3) is boolean when
    present, default ON for the hive layout (O9); it writes
    ``{store}/coverage.moc``, a hive-root object, so an EXPLICIT true on a
    non-healpix grid or a flat-layout store is a config mistake, not a no-op —
    rejected pointedly. Absent simply means off there (``get_coverage_moc``
    resolves the default).
    """
    grid = config.output.get("grid")
    store_layout = config.output.get("store_layout")
    if store_layout is not None and store_layout not in ("flat", "hive"):
        raise ValueError(f"output.store_layout must be 'flat' or 'hive' (got {store_layout!r})")
    # Lifecycle-touch policy (issue #501): validated at load so a typo fails at
    # submission rather than silently resolving to the default deep inside a
    # worker's skip path, where the wrong answer is either version churn on a
    # published store or a collaborator's data expiring.
    touch = config.output.get("touch")
    if touch is not None and touch not in TOUCH_POLICIES:
        raise ValueError(
            f"output.touch must be one of {', '.join(repr(t) for t in TOUCH_POLICIES)} "
            f"(got {touch!r})"
        )
    # D19 product name (issue #299): validated at load so a bad name fails
    # before any store I/O. The grammar lives in hive.py (one source).
    name = config.output.get("product_name")
    if name is not None:
        from zagg.hive import validate_product_name

        validate_product_name(name)
    if get_store_layout(config) == "hive":
        if (grid or {}).get("type", "healpix") != "healpix":
            raise ValueError(
                "output.store_layout: hive requires a healpix grid (hive node names "
                f"are morton decimal digits; grid type is {(grid or {}).get('type')!r})"
            )
        if config.output.get("consolidate_metadata"):
            raise ValueError(
                "output.store_layout: hive has no store-root zarr hierarchy to "
                "consolidate (D5/D12) — drop output.consolidate_metadata"
            )
    coverage_moc = config.output.get("coverage_moc")
    if coverage_moc is not None and not isinstance(coverage_moc, bool):
        raise ValueError(f"output.coverage_moc must be a boolean (got {coverage_moc!r})")
    if coverage_moc:
        if (grid or {}).get("type", "healpix") != "healpix":
            raise ValueError(
                "output.coverage_moc requires a healpix grid (the root coverage.moc "
                "is a morton MOC; drop the flag for rectilinear output)"
            )
        if get_store_layout(config) != "hive":
            raise ValueError(
                "output.coverage_moc requires output.store_layout: hive (the root "
                "coverage.moc lives at the hive store root; flat stores have no "
                "hive root to bootstrap from)"
            )
    # End-of-run rollup sweep trigger (issue #300), same posture as
    # coverage_moc: boolean when present, default ON for hive (get_sweep
    # resolves it), explicit true on a non-hive store is a config mistake.
    # "stages" (issue #384) additionally chains the STAGED pyramid sweep
    # after the families pass — the DEFAULT for a hive store that declares a
    # ladder since issue #620 ("the sweep belongs to the append"). The
    # booleans and the mode words are the same two answers spelled twice:
    # get_sweep_mode owns the mapping.
    sweep = config.output.get("sweep")
    if sweep is not None and not isinstance(sweep, bool) and sweep not in SWEEP_MODES:
        raise ValueError(
            f"output.sweep must be a boolean or one of {list(SWEEP_MODES)} (got {sweep!r})"
        )
    # Resolved, not truthy: ``"none"`` is a truthy string that means OFF, so
    # spelling the opt-out in words on a flat store must not be refused.
    if (
        sweep is not None
        and get_sweep_mode(config) != SWEEP_NONE
        and get_store_layout(config) != "hive"
    ):
        raise ValueError(
            "output.sweep requires output.store_layout: hive (the rollup sweep "
            "folds hive-tree leaf artifacts; flat stores have no digit tree)"
        )
    # Versioned leaves (issue #582, spec §1.5): a boolean kill-switch, default
    # ON for hive (get_leaf_versions resolves it); ``false`` keeps the legacy
    # in-place leaf for readers that do not yet follow ``current``.
    leaf_versions = config.output.get("leaf_versions")
    if leaf_versions is not None and not isinstance(leaf_versions, bool):
        raise ValueError(f"output.leaf_versions must be a boolean (got {leaf_versions!r})")
    if leaf_versions and get_store_layout(config) != "hive":
        raise ValueError(
            "output.leaf_versions requires output.store_layout: hive (a version subgroup "
            "lives under a hive leaf's stable root)"
        )
    # Icechunk companion repo (issue #580, spec §11): same posture as sweep —
    # boolean when present, default ON for hive (get_icechunk resolves it),
    # explicit true on a non-hive store is a config mistake (the repo indexes
    # hive leaves by shard rank).
    icechunk = config.output.get("icechunk")
    if icechunk is not None and not isinstance(icechunk, (bool, dict)):
        raise ValueError(
            f"output.icechunk must be a boolean or an options block (got {icechunk!r})"
        )
    if isinstance(icechunk, dict):
        # The ladder's knobs (issue #580 phase 6, spec §11.4/§11.5): the
        # shard-order-relative checks run at init (icechunk_refs.resolve_options,
        # where the shard order is known); the shape and the one invariant
        # that needs no geometry — a commit must write whole manifests — here.
        unknown = set(icechunk) - {"commit", "commit_order", "split_order", "retain_runs"}
        if unknown:
            raise ValueError(
                f"output.icechunk has unknown key(s) {sorted(unknown)} (accepts commit, "
                f"commit_order, split_order, retain_runs)"
            )
        # Run-tag retention (issue #582): how many run tags finalize keeps;
        # 0 (the default) keeps every run and never expires or collects.
        retain = icechunk.get("retain_runs")
        if retain is not None and (
            isinstance(retain, bool) or not isinstance(retain, int) or retain < 0
        ):
            raise ValueError(
                f"output.icechunk.retain_runs must be a non-negative integer (got {retain!r})"
            )
        commit = icechunk.get("commit")
        if commit is not None and commit not in ("ladder", "leaf"):
            raise ValueError(f"output.icechunk.commit must be 'ladder' or 'leaf' (got {commit!r})")
        orders = {k: icechunk.get(k) for k in ("commit_order", "split_order")}
        for key, value in orders.items():
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(
                    f"output.icechunk.{key} must be a non-negative integer (got {value!r})"
                )
        if (
            orders["commit_order"] is not None
            and orders["split_order"] is not None
            and orders["split_order"] < orders["commit_order"]
        ):
            raise ValueError(
                f"output.icechunk.split_order {orders['split_order']} is finer than commit_order "
                f"{orders['commit_order']}: a manifest (one per split_order cell) would be written "
                f"by more than one commit (spec §11.5)"
            )
    # The RESOLVED opt-in, not the literal's truthiness: an options block is
    # an opt-in whatever it holds, and the empty one -- the spelling for "the
    # ladder defaults" -- is falsy, so the three scope guards below skipped it
    # and stood up a repo §11.6 exists to prevent (review finding).
    enabled = icechunk is True or isinstance(icechunk, dict)
    if enabled and get_store_layout(config) != "hive":
        raise ValueError(
            "output.icechunk requires output.store_layout: hive (the companion repo "
            "references hive leaves by shard rank; flat stores have no leaves)"
        )
    if enabled and (config.data_source or {}).get("reader") == "raster":
        raise ValueError(
            "output.icechunk is out of scope for raster products (spec §11.6): "
            "raster leaves are never sharded, so stage 1 records no refs for them"
        )
    # Overview pyramid declaration (issue #201): explicit blocks are grammar-
    # checked here; the D24 none-field warning fires at manifest build time
    # (template time for the store), not per config validation. The NaN-fill
    # reduction warning below IS per config validation (espg ruling on the
    # PR #344 nan-policy finding): it concerns the store encoding itself.
    _validate_pyramid(config)
    _warn_nan_ambiguous_reductions(config)


def _validate_pyramid(config: PipelineConfig) -> None:
    """Validate the ``output.pyramid`` knob's grammar (issue #201).

    Only an EXPLICIT block is checked (absent/false are always legal); an
    explicit block requires the hive store layout, like ``sweep`` — the
    pyramid materializes through the hive manifest.
    """
    raw = config.output.get("pyramid")
    if raw is None or raw is False:
        return
    knob = get_pyramid(config) or {}  # raises on a non-mapping
    if get_store_layout(config) != "hive":
        raise ValueError(
            "output.pyramid requires output.store_layout: hive (overviews live at "
            "hive-tree ancestor nodes; flat stores have no digit tree)"
        )
    unknown = set(knob) - {
        "spacing",
        "orders",
        "overviews",
        "all_time",
        "fold_source",
        "exact_levels",
        "summarize",
    }
    if unknown:
        raise ValueError(f"output.pyramid has unknown keys {sorted(unknown)}")
    _validate_pyramid_fold(knob)
    spacing = knob.get("spacing")
    if spacing is not None and (not isinstance(spacing, int) or spacing < 1):
        raise ValueError(f"output.pyramid.spacing must be an int >= 1 (got {spacing!r})")
    parent_order = int((config.output.get("grid") or {}).get("parent_order", 0))
    if knob.get("overviews") is not None:
        # The issue #382 collapsed grammar (leaf cell resolutions; the fixed
        # ladder above the shard is law, not declaration): an overviews knob
        # REPLACES the /1 schedule knobs wholesale, so mixing the two
        # grammars is refused rather than silently resolved either way.
        if knob.get("orders") is not None or knob.get("spacing") is not None:
            raise ValueError(
                "output.pyramid.overviews is the zagg-pyramid/2 grammar and replaces "
                "orders/spacing wholesale — declare overviews OR orders/spacing, never both"
            )
        from zagg.pyramid import normalize_overviews, validate_overviews

        # The module's own accessor, not a raw .get with a 0 fallback: a
        # missing key must refuse by NAME ("output.grid.child_order is
        # required"), not validate its way to a level-range error that points
        # at neither the problem nor the key.
        validate_overviews(
            normalize_overviews(knob["overviews"]),
            parent_order=parent_order,
            child_order=get_child_order(config),
        )
    orders = knob.get("orders")
    if orders is not None:
        ok = isinstance(orders, list) and all(isinstance(k, int) for k in orders)
        if not ok or any(not (0 <= k < parent_order) for k in orders):
            raise ValueError(
                f"output.pyramid.orders must be a list of ancestor orders in "
                f"[0, parent_order) = [0, {parent_order}) (got {orders!r})"
            )
    all_time = knob.get("all_time")
    if all_time is not None and not isinstance(all_time, bool):
        raise ValueError(f"output.pyramid.all_time must be a boolean (got {all_time!r})")
    if all_time is False and _declares_windowing(config):
        # espg's ruling on PR #623 question (16): a windowed store must
        # declare the all-time fold. The rollup families and the §4.10
        # companion ride each node's all-time CLOSE, so a windowed store
        # without one has nowhere to fold them; the absent key resolves to
        # true (get_pyramid), and only the explicit refusal is an error.
        raise ValueError(
            "output.windowing requires the all-time fold: output.pyramid.all_time: false "
            "is refused on a windowed store (the rollup families and the multiscales "
            "companion fold in each node's all-time close, issue #610 phase 4) — omit the "
            "key, which resolves to true on a windowed store, or set it true"
        )
    summarize = knob.get("summarize")
    if summarize is None:
        return
    if not isinstance(summarize, dict):
        raise ValueError(f"output.pyramid.summarize must be a mapping (got {summarize!r})")
    fields = get_agg_fields(config)
    for source, decl in summarize.items():
        if source not in fields:
            raise ValueError(
                f"output.pyramid.summarize names unknown field {source!r} "
                f"(declared: {sorted(fields)})"
            )
        target = (decl or {}).get("as")
        if not isinstance(target, str) or not target:
            raise ValueError(f"output.pyramid.summarize.{source} requires 'as': <new field name>")
        if target in fields:
            raise ValueError(
                f"output.pyramid.summarize.{source}.as = {target!r} collides with a "
                f"declared field: derived summaries must use a DIFFERENT name so overview "
                f"schema never silently differs from source (D24)"
            )


def _validate_pyramid_fold(knob: dict) -> None:
    """Validate the ``fold_source``/``exact_levels`` fold knobs (issue #376).

    ``fold_source: "leaves"`` — the pre-#376 exact-from-leaves fold — is legal
    but deprecated, so it warns here (config validation, the one place a
    misspelled knob still refuses before any store exists) rather than
    refusing. ``exact_levels`` is the cascade boundary: how many of the finest
    declared levels keep the exact-from-leaves fold. It is meaningful only
    under ``cascade`` (under ``leaves`` every level is exact already), so
    declaring both is a contradiction worth naming.
    """
    from zagg.sweep_overview import FOLD_SOURCES

    fold_source = knob.get("fold_source")
    if fold_source is not None and fold_source not in FOLD_SOURCES:
        raise ValueError(
            f"output.pyramid.fold_source must be one of {list(FOLD_SOURCES)} (got {fold_source!r})"
        )
    exact_levels = knob.get("exact_levels")
    if exact_levels is not None and (not isinstance(exact_levels, int) or exact_levels < 1):
        raise ValueError(
            f"output.pyramid.exact_levels must be an int >= 1 — the finest declared "
            f"level has no finer overview to cascade from, so it is always folded from "
            f"the leaves (got {exact_levels!r})"
        )
    if fold_source == "leaves":
        if exact_levels is not None:
            raise ValueError(
                "output.pyramid.exact_levels is meaningless under fold_source: leaves "
                "(every level folds from the leaves there); drop one of the two"
            )
        logger.warning(
            "output.pyramid.fold_source: leaves is DEPRECATED (issue #376) — it re-folds "
            "every declared level from the raw leaves, so per-node memory grows with the "
            "subtree; the default 'cascade' folds each coarse level from the level below it"
        )


#: Exact-law reductions whose plain (NaN-propagating) forms cannot round-trip
#: through a NaN fill: at read, a NaN datum and the fill are the same bytes.
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
