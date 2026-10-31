"""``aggregation`` and ``output`` accessors (issue #677 phase 0).

Every function here is a one-line read over the raw config dicts; the
pydantic models of later phases keep these as shims (issue #677, accessor
ruling 2026-10-31) so no call site churns in the schema PR.
"""

import logging

from zagg.config.core import PipelineConfig

logger = logging.getLogger(__name__)


def get_agg_fields(config: PipelineConfig) -> dict:
    """Return aggregation variable metadata keyed by variable name.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict
        ``{name: {function/expression, source, params, dtype, fill_value, ...}}``.
        A field may also declare a non-scalar output (issue #29) via ``kind``
        (``scalar`` default, or ``vector``) and ``trailing_shape``; use
        :func:`get_output_signature` to read the normalized declaration.
    """
    return dict(config.aggregation.get("variables", {}))


def get_chunk_precompute(config: PipelineConfig) -> dict:
    """Return the ``aggregation.chunk_precompute`` entries keyed by name (issue #30).

    Each entry is a chunk-level scalar evaluated ONCE per chunk (shard) over the
    shard's pooled column data, before the per-cell loop; the resulting scalar is
    injected into the per-cell expression namespace. Returns an empty dict when no
    ``chunk_precompute`` block is present, so the existing per-cell path is a no-op.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict
        ``{name: {function/expression, source, params, dtype}}``.
    """
    return dict(config.aggregation.get("chunk_precompute", {}))


def get_output_signature(meta: dict) -> dict:
    """Return the normalized non-scalar output signature for one agg field.

    This is the single read point for a field's output declaration (issues
    #29 and #48): its output ``kind``, the per-cell ``trailing_shape``,
    ``inner_shape``, and ``dtype``. Later phases (statistic eval, the
    per-shard container, and the grid ``signature()``) consume this rather
    than re-parsing the raw metadata.

    Parameters
    ----------
    meta : dict
        A single variable's aggregation metadata (a value of
        :func:`get_agg_fields`).

    Returns
    -------
    dict
        ``{"kind": str, "trailing_shape": tuple, "inner_shape": tuple, "dtype":
        str, "resolution": str}``.
        ``trailing_shape`` is ``()`` for scalar and ragged fields.
        ``inner_shape`` is ``()`` for scalar and vector fields; for ragged it
        holds the per-element shape (e.g. ``(2,)`` for a centroid pair).
        ``dtype`` is the declared dtype string, or ``None`` if unset.
        ``resolution`` is ``"cell"`` (default — one value per aggregation cell) or
        ``"chunk"`` (issue #30 item 2 — one value per chunk, stored in a companion
        array shaped at the chunk grid and indexed by ``grid.block_index``).
        ``location`` is the ragged location channel's column name (issue #87), or
        ``None`` when the field carries no location.
    """
    kind = meta.get("kind", "scalar")
    if kind == "vector":
        ts = meta["trailing_shape"]
        trailing_shape = (ts,) if isinstance(ts, int) else tuple(ts)
        inner_shape: tuple = ()
    elif kind == "ragged":
        trailing_shape = ()
        rs = meta["inner_shape"]
        inner_shape = (rs,) if isinstance(rs, int) else tuple(rs)
    else:
        trailing_shape = ()
        inner_shape = ()
    return {
        "kind": kind,
        "trailing_shape": trailing_shape,
        "inner_shape": inner_shape,
        "dtype": meta.get("dtype"),
        "resolution": meta.get("resolution", "cell"),
        # Ragged location channel (issue #87): the per-observation morton column
        # the reducer folds per centroid; ``None`` for unlocated fields.
        "location": meta.get("location"),
        # Ragged weights declaration (spec §2.0, issue #424): "counts"/"flux",
        # or ``None`` when undeclared (the spec reads absence as counts, so
        # ``None`` keeps pre-#424 templates and signatures byte-identical).
        "weights": meta.get("weights"),
        # Temporal companion shape (spec §8.2/§8.3, issue #410):
        # "per-cell"/"per-centroid", or ``None`` when the field carries no
        # temporal companion — absence declares nothing, so pre-#410
        # templates and signatures stay byte-identical.
        "temporal": meta.get("temporal"),
    }


def output_field_signature(config: PipelineConfig) -> list[dict]:
    """Return the Option-B output-field signature for a config (issue #29).

    A canonical, JSON-serializable list of ``{"name", "kind", "trailing_shape",
    "dtype"}`` for every aggregation variable, sorted by ``name``. Recorded in a
    grid's :meth:`signature` so a shard map can never be silently paired with a
    grid whose output schema (scalar vs vector, trailing shape, dtype) differs,
    and compared in ``nests_with`` so co-aggregated grids must share a field set.

    ``trailing_shape`` is rendered as a ``list`` (``()`` for scalar fields) so
    the structure round-trips through JSON unchanged.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list of dict
    """
    fields = []
    for name, meta in get_agg_fields(config).items():
        sig = get_output_signature(meta)
        entry = {
            "name": name,
            "kind": sig["kind"],
            "trailing_shape": list(sig["trailing_shape"]),
            "inner_shape": list(sig["inner_shape"]),
            "dtype": sig["dtype"],
        }
        # A location channel changes the store schema (a uint64 companion vlen
        # array — issue #87), so it belongs in the signature; keyed only when set
        # so existing shard-map signatures are byte-identical.
        if sig["location"] is not None:
            entry["location"] = sig["location"]
        # A weights declaration changes the payload semantics (spec §2.0,
        # issue #424) — same keyed-only-when-set discipline as location.
        if sig["weights"] is not None:
            entry["weights"] = sig["weights"]
        # A temporal declaration changes the store schema too (a dense uint64
        # companion, or a uint64 sibling vlen array — spec §8.2/§8.3, issue
        # #410), under the same keyed-only-when-set discipline.
        if sig["temporal"] is not None:
            entry["temporal"] = sig["temporal"]
        fields.append(entry)
    return sorted(fields, key=lambda f: f["name"])


def get_coords(config: PipelineConfig) -> list[str]:
    """Return coordinate column names from the aggregation config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[str]
    """
    return list(config.aggregation.get("coordinates", {}).keys())


def get_data_vars(config: PipelineConfig) -> list[str]:
    """Return data variable column names from the aggregation config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[str]
    """
    return list(config.aggregation.get("variables", {}).keys())


def get_driver(config: PipelineConfig) -> str:
    """Return the data access driver from the config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"s3"`` or ``"https"``. Defaults to ``"s3"``.
    """
    return config.data_source.get("driver", "s3")


def get_handoff(config: PipelineConfig) -> str:
    """Return the per-cell aggregation carrier from the aggregation config (issue #132).

    The ``handoff`` knob lives on the ``aggregation`` block, default ``"arrow"``,
    and selects the per-cell read->concat->extract carrier: ``"arrow"`` (an
    ``arro3.core`` Table, faster + lighter on dense shards) or ``"pandas"`` (a
    DataFrame, which tolerates nullable columns natively). Both feed identical
    numpy arrays into the same reductions, so scalar outputs are byte-for-byte
    identical (issues #130/#131). A pipeline declares its carrier next to the rest
    of its aggregation settings rather than relying on a global default; the
    explicit ``handoff=`` kwarg still overrides this value.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"arrow"`` (default) or ``"pandas"``.
    """
    return config.aggregation.get("handoff", "arrow")


def get_child_order(config: PipelineConfig) -> int:
    """Return child_order from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    int

    Raises
    ------
    ValueError
        If child_order is not set in the config.
    """
    grid = config.output.get("grid", {})
    child_order = grid.get("child_order")
    if child_order is None:
        raise ValueError("output.grid.child_order is required")
    return int(child_order)


def get_parent_order(config: PipelineConfig) -> int:
    """Return parent_order (shard order) from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    int

    Raises
    ------
    ValueError
        If parent_order is not set in the config.
    """
    grid = config.output.get("grid", {})
    parent_order = grid.get("parent_order")
    if parent_order is None:
        raise ValueError("output.grid.parent_order is required")
    return int(parent_order)


def get_layout(config: PipelineConfig) -> str:
    """Return the HEALPix storage layout from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"fullsphere"`` (the only layout; the deprecated ``"dense"`` pack
        was removed — issue #88).
    """
    return config.output.get("grid", {}).get("layout", "fullsphere")


def get_sharded(config: PipelineConfig, default: bool = False) -> bool:
    """Return whether the output grid uses ShardingCodec storage (issue #108).

    The ``sharded`` knob lives on the grid/chunk block next to ``chunk_inner``
    (mirroring its accessor). When ``True`` the grid bundles a dispatch shard's K
    inner chunks into one zarr shard object instead of K independent regular chunk
    objects; a K==1 grid has nothing to bundle, so the grid silently no-ops it
    (issue #215). ``default`` is the value returned when the flag is omitted —
    ``False`` here, but ``from_config`` passes ``True`` for HEALPix output on
    both store layouts (issue #215 flat, issue #236 hive: a missing flag should
    not cost the ~K-fold object blow-up).
    """
    return bool(config.output.get("grid", {}).get("sharded", default))


def get_emit_cell_ids(config: PipelineConfig) -> bool:
    """Whether to keep writing the legacy ``cell_ids`` array (issue #304).

    Default ``False`` (the D16 flip): ``morton`` is the only stored cell
    coordinate; NESTED ids are derived at read (moczarr fabrication). ``True``
    is the transition escape hatch — the ``cell_ids`` array is written in
    ADDITION to ``morton`` (holding the NESTED encode) so legacy
    browser-direct consumers keep working until the gridlook morton decode
    lands; the dggs attrs are unaffected by the hatch.
    """
    return bool(config.output.get("grid", {}).get("emit_cell_ids", False))


def get_product_name(config: PipelineConfig) -> str | None:
    """The D19 product name (issue #299), or ``None`` for a bare store.

    When set, the run's effective store root becomes
    ``{output.store}/{product_name}/`` (``zagg.hive.effective_store_root``) —
    a NAMED product root in a multi-product store. Absent (the default),
    stores keep today's bare single-product layout byte-identically.
    """
    return config.output.get("product_name")


def get_store_path(config: PipelineConfig) -> str | None:
    """Return the store path from the output config, or None.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("store")


#: Legal ``output.touch`` values (issue #501). ``auto`` is the issue #495
#: phase 4 inference; the other two are the operator's override of it.
TOUCH_POLICIES = ("auto", "always", "never")


def get_touch_policy(config: PipelineConfig) -> str:
    """Return the skip-run lifecycle-touch policy (issue #501).

    The touch (issue #388) defeats a bucket EXPIRATION rule, so whether to run
    it is a property of the destination — one we cannot read cross-account
    (``s3:GetLifecycleConfiguration`` is bucket-owner only) and that no bucket
    name reliably encodes. The operator knows, so it is declarable:

    ``auto`` (default)
        The issue #495 phase 4 inference, verbatim: touch unless the
        destination is in :data:`zagg.store._PUBLISHED_BUCKETS`. A pure no-op
        for every config that predates this knob.
    ``always``
        Touch regardless of destination — an un-negotiated external target
        whose expiry rule we know about but whose name says nothing.
    ``never``
        Never touch, local paths included — an archival destination, or one
        where version churn outweighs the protection.

    An override layered ON TOP of the inference, not a replacement.

    An unrecognized value falls through to ``auto`` (:func:`zagg.lifecycle.
    _touch_applies`) but WARNS first: :func:`validate_config` rejects a typo at
    submission, but the worker's own funnel — :func:`load_config_from_dict` on
    a hand-built invoke event — does not validate, so ``touch: "nevr"`` would
    otherwise touch an archival destination against the operator's stated
    intent with nothing in the log. One greppable line instead (review finding
    on PR #496).
    """
    touch = config.output.get("touch", "auto")
    if touch not in TOUCH_POLICIES:
        logger.warning(
            f"output.touch={touch!r} is not one of "
            f"{', '.join(repr(t) for t in TOUCH_POLICIES)} — falling through to 'auto' "
            "(the issue #495 phase 4 inference); this config was not validated at "
            "submission (issue #501)"
        )
    return touch


def get_store_layout(config: PipelineConfig) -> str:
    """Return the STORE layout — morton hive vs flat single store (issue #199).

    Distinct from ``output.grid.layout`` (the HEALPix *array* layout inside one
    store): ``output.store_layout`` selects how shard output is arranged under
    ``output.store``. ``"hive"`` writes one self-describing leaf zarr per shard
    at ``{store}/{sign+base}/{d1}/.../{full_id}.zarr`` with a root
    ``morton_hive.json`` manifest and a per-leaf commit stamp — see
    ``docs/design/sparse_coverage.md`` (D1-D6) and :mod:`zagg.hive`. ``"flat"``
    is the single shared zarr store.

    The default is grid-aware and pipeline-agnostic (issue #253): HEALPix
    grids default to ``"hive"`` (the ratified profile — for point aggregation
    AND the raster path, which writes hive leaves per issue #247); rectilinear
    grids default to ``"flat"`` (hive ids are morton digits — HEALPix-only).
    An explicit HEALPix ``"flat"`` remains valid for interop/debug but is
    deprecated — removal is gated on the sparse-DGGS read path (issue #251
    phase 3). A present-but-null key falls back to the default, like
    ``layout``.

    Returns
    -------
    str
        ``"flat"`` or ``"hive"``.
    """
    layout = config.output.get("store_layout")
    if layout is not None:
        return layout
    grid_type = (config.output.get("grid") or {}).get("type", "healpix")
    return "hive" if grid_type == "healpix" else "flat"


def get_coverage_moc(config: PipelineConfig) -> bool:
    """Whether the end-of-run root ``coverage.moc`` write is on (issue #200).

    Default ON for hive-layout stores (O9/espg: coverage MOCs are the default
    for healpix templates — and hive is healpix-only by validation);
    ``output.coverage_moc: false`` opts out. Flat-layout / non-healpix
    configs default off, and an explicit ``true`` there is rejected by
    ``validate_config``. A present-but-null key falls back to the default,
    like ``store_layout``.
    """
    flag = config.output.get("coverage_moc")
    if flag is None:
        return get_store_layout(config) == "hive"
    return bool(flag)


#: The three ``output.sweep`` modes, resolved by :func:`get_sweep_mode`.
#: ``"none"`` is no tail sweep at all, ``"families"`` the D22 families pass
#: alone, ``"stages"`` that pass plus the staged ladder chained after it.
SWEEP_NONE = "none"
SWEEP_FAMILIES = "families"
SWEEP_STAGES = "stages"
SWEEP_MODES = (SWEEP_NONE, SWEEP_FAMILIES, SWEEP_STAGES)


def _declares_ladder(config: PipelineConfig, grid) -> bool:
    """Whether this config declares a ``/2`` ladder a staged sweep can walk.

    :func:`zagg.column.leaf_column_plan`'s gate PLUS
    ``build_pyramid_block``'s raster exemption, because the gate alone is not
    that function's manifest default: the issue #384 flip condition carries
    ``(config.data_source or {}).get("reader") != "raster"``
    (``sweep_overview.py``) and ``column._leaf_levels`` does not, so a
    ``reader: raster`` hive config with a strictly-interior ``chunk_inner``
    has a column gate of True against a ``zagg-pyramid/1`` manifest. Raster
    hive products have NO ladder (espg's inventory,
    https://github.com/englacial/zagg/issues/620#issuecomment-6068583904;
    ``declare_pyramid`` refuses ``reader: raster``), so the sweep default
    must not claim one — it would refuse at ``ladder_entries``' ``/2`` gate
    once per invoke. Unreachable today (``runner.agg`` re-routes raster to
    ``RasterStrategy`` and ``client.Run`` refuses it outright), pinned so the
    next tail that chains cannot reach it either.

    It needs the GRID: the issue #384 default flip is ``/2`` only where the
    grid's resolved chunk order is strictly interior, and a ``K == 1`` grid
    (no ``chunk_inner``) stays ``/1`` with no column to gather. Without a
    grid the ladder cannot be confirmed, so the answer is no and the default
    stays on the families pass rather than chaining a pass that would refuse
    at the ``/2`` gate once per run.

    One narrower divergence remains and cannot be reached from here:
    ``build_pyramid_block`` bounds the flip by ``output.grid.child_order``
    only WHEN PRESENT, where ``_leaf_levels`` always bounds by the grid's
    resolved ``child_order``. A config spelling no ``child_order`` is the
    grid-less ``declare_pyramid`` retrofit shape — ``grids.from_config``
    refuses it ("output.grid.child_order is required") — so this predicate
    has no grid for it and already answers no.
    """
    if grid is None:
        return False
    if (config.data_source or {}).get("reader") == "raster":
        return False  # build_pyramid_block's raster exemption, same flip condition
    from zagg.column import leaf_column_plan

    try:
        return leaf_column_plan(config, grid) is not None
    except ValueError:
        return False  # a declaration the sweep would refuse walks no ladder either


def get_sweep_mode(config: PipelineConfig, grid=None) -> str:
    """Which end-of-run sweep the tail chains: one of :data:`SWEEP_MODES`.

    THE resolver for ``output.sweep`` (issue #620 section 2, espg's ruling
    "the sweep belongs to the append"). Every call site asks this rather than
    reading the raw key, so the default and its spellings live in one place.

    The resolution, with the raw key's legacy spellings mapped onto the modes:

    =========================  =========================================
    ``output.sweep``           mode
    =========================  =========================================
    absent / null, hive        ``"stages"`` when ``grid`` is given and the
                               config declares a ``/2`` ladder, else
                               ``"families"``
    absent / null, non-hive    ``"none"``
    ``true`` / ``"families"``  ``"families"`` — the pass alone, no ladder
    ``false`` / ``"none"``     ``"none"``
    ``"stages"``               ``"stages"``
    =========================  =========================================

    **What changed, and the way out.** The ladder used to be opt-in: a hive
    store defaulted to the families pass and a ``/2`` store's ladder was
    walked by hand or by ``sweep: "stages"``. It is now the default, because
    the sweep is the append's — a run that leaves its ladder for an operator
    call is how the unscoped full-store sweep got run twice (issue #610's
    cost comment,
    https://github.com/englacial/zagg/issues/610#issuecomment-6067171538).
    An existing caller that wants the old behaviour spells it: ``sweep:
    true`` (or ``"families"``) keeps the families pass alone, ``false`` (or
    ``"none"``) keeps nothing. ``true`` deliberately does NOT mean the
    ladder — it is the spelling existing configs already carry, and promoting
    it would change what they do without anyone writing it down.

    "Declares a ladder" is :func:`_declares_ladder` — the leaf column gate,
    which needs the ``grid``. A caller that holds one (every chaining site
    does) passes it; one that does not gets ``"families"``, never a chained
    pass the ``/2`` gate would refuse.
    """
    raw = config.output.get("sweep")
    if raw is None:
        if get_store_layout(config) != "hive":
            return SWEEP_NONE
        return SWEEP_STAGES if _declares_ladder(config, grid) else SWEEP_FAMILIES
    if isinstance(raw, bool):
        return SWEEP_FAMILIES if raw else SWEEP_NONE
    mode = str(raw)
    if mode not in SWEEP_MODES:
        # The declared return type, enforced at the only place it CAN be for a
        # worker-built config: the Lambda worker builds its config with
        # ``load_config_from_dict``, which never calls ``validate_config``
        # (``build_pyramid_block``'s docstring records the same gap), so
        # ``sweep: "Stages"`` or ``"ladder"`` would otherwise resolve to that
        # literal, compare False against every mode, and silently give the run
        # the families-only tail with no refusal anywhere.
        raise ValueError(
            f"output.sweep must be a boolean or one of {list(SWEEP_MODES)} (got {raw!r})"
        )
    return mode


def chains_stages(config: PipelineConfig, grid=None) -> bool:
    """Whether the run tail chains the STAGED ladder — fail-open (D9).

    The chaining gate :func:`get_sweep_mode` ``== SWEEP_STAGES``, wrapped,
    because every tail it sits in is deliberately fail-open: the gate replaced
    a dict lookup (``config.output.get("sweep") == "stages"``) that could not
    fail, and the resolver reaches ``leaf_column_plan`` →
    ``declared_fields`` → ``composability_classes`` + ``agg[name]``, which a
    hand-built or worker-built aggregation block can raise ``KeyError`` or
    ``TypeError`` from (and the resolver itself refuses an unknown mode word).
    Unwrapped it would land AFTER every leaf is written and before the
    Icechunk finalize, failing a run whose data is complete — where its
    neighbours (``sweep_after_run``, ``stage_sweep_after_run``,
    ``_finalize_icechunk_local``) all swallow and warn. An unconfirmable
    declaration is NO chained pass, never a guessed one; the loud refusal is
    :func:`validate_config`'s, up front.
    """
    try:
        return get_sweep_mode(config, grid) == SWEEP_STAGES
    except Exception as e:
        logger.warning(f"staged sweep not chained, sweep mode unresolved (fail-open, D9): {e}")
        return False


def get_sweep(config: PipelineConfig) -> bool:
    """Whether the end-of-run rollup sweep trigger is on (issue #300).

    Default ON for hive-layout stores (the D22 sweep folds hive-tree leaf
    artifacts into interior rollups; every artifact is a fail-open regenerable
    cache, D9); ``output.sweep: false`` opts out. Non-hive configs default
    off, and an explicit ``true`` there is rejected by ``validate_config``,
    mirroring ``coverage_moc``. A present-but-null key falls back to the
    default. The trigger transport is backend-shaped (D8): the local
    dispatchers run the sweep in-process; the Lambda dispatchers post a
    fire-and-forget ``mode="sweep"`` worker Event invoke.

    The families pass's own gate: true for every mode but ``"none"``
    (:func:`get_sweep_mode`, which says whether the ladder chains too).
    """
    return get_sweep_mode(config) != SWEEP_NONE


def get_leaf_versions(config: PipelineConfig) -> bool:
    """Whether hive leaves are written VERSIONED (issue #582, spec §1.5).

    Default ON for hive-layout stores: each unit's arrays go to a fresh
    ``run-{run_id}-{attempt}`` subgroup under the stable leaf root, whose
    stamp names it as ``current``, so a replacement never rewrites bytes an
    earlier run tag references. ``output.leaf_versions: false`` keeps the
    legacy in-place leaf — the switch for a store whose readers do not yet
    follow ``current``. Default ON carries spec §1.5's operator
    preconditions: no bucket expiration rule over the hive tree (the
    collector owns version lifetime) and readers that follow ``current``.
    Layout, not semantics: outside the D19 core.
    """
    flag = config.output.get("leaf_versions")
    if flag is None:
        return get_store_layout(config) == "hive"
    return bool(flag)


def get_icechunk(config: PipelineConfig) -> bool:
    """Whether the Icechunk companion repo is written (issue #580, spec §11).

    Default ON for hive-layout stores: the once-per-run ``icechunk_init`` step
    and the per-leaf virtual-ref commit at leaf commit, both fail-open (the
    leaves stay normative; the repo is a regenerable index). ``output.icechunk:
    false`` opts out.

    The default follows the WRITER's scope, not the layout alone (spec
    §11.6). **Windowed** hive stores are in scope as of issue #584 phase 2:
    a window's leaves share a shard rank, and the repo's row dimension
    (§11.2) is what separates them, so the knob defaults ON for them too.
    Raster hive products (never sharded) still record no refs, so the knob
    resolves OFF there rather than standing up a repo no leaf can fill. A
    present-but-null key falls back to that default; an explicit ``true`` on
    either out-of-scope shape — non-hive, raster — is rejected by
    ``validate_config``, mirroring ``sweep``. Excluded from the D19 semantic
    core like the other run triggers (:mod:`zagg.semantics`).
    """
    flag = config.output.get("icechunk")
    if flag is None:
        return (
            get_store_layout(config) == "hive"
            and (config.data_source or {}).get("reader") != "raster"
        )
    if isinstance(flag, dict):
        return True  # an options block (phase 6) is an opt-in with knobs
    return bool(flag)


def get_icechunk_options(config: PipelineConfig) -> dict:
    """The raw ``output.icechunk`` ladder knobs, absent keys ``None`` (issue #580 phase 6).

    ``{"commit": "ladder" | "leaf" | None, "commit_order": int | None,
    "split_order": int | None, "rows_per_manifest": int | None,
    "retain_runs": int | None}`` — a boolean or
    absent knob yields all-``None``; :func:`zagg.icechunk_refs.resolve_options`
    applies the shard-order defaults and the §11.5 invariants, and
    :func:`zagg.icechunk_finalize.resolve_retain_runs` the retention default.
    """
    flag = config.output.get("icechunk")
    block = flag if isinstance(flag, dict) else {}
    return {
        k: block.get(k)
        for k in ("commit", "commit_order", "split_order", "rows_per_manifest", "retain_runs")
    }


def get_pyramid(config: PipelineConfig) -> dict | None:
    """The overview pyramid declaration knob, or ``None`` when disabled (#201).

    ``output.pyramid`` shapes the manifest's template-time pyramid block
    (``zagg.sweep_overview.build_pyramid_block``): absent/null returns ``{}``
    — the defaults apply (an every-2-orders overview schedule below the shard
    order, the espg-ratified display default; the all-time fold only on a
    windowed store, where it is required and resolved true) — and
    ``false`` returns ``None``, declaring the overview family OFF for the
    store. An explicit mapping may carry ``spacing`` (int >= 1), ``orders``
    (explicit ancestor orders, winning over ``spacing``), ``overviews`` (the
    issue #382 ``zagg-pyramid/2`` grammar: leaf cell resolutions, strictly
    descending and strictly inside the shard's resolution window, replacing
    ``orders``/``spacing`` wholesale — the fixed every-order ladder above
    the shard is law, see :mod:`zagg.pyramid`),
    ``all_time`` (bool: also maintain the ``all.zarr`` cross-window fold on
    windowed stores — **resolved to** ``True`` **on a windowed store that
    omits it**, issue #610 phase 4: a windowed store must declare the fold,
    and :func:`_validate_pyramid` refuses an explicit ``false`` there),
    ``fold_source`` (``"cascade"`` — the default — or the
    deprecated ``"leaves"``, issue #376) with ``exact_levels`` (how many of
    the finest declared levels fold exactly from the leaves), and
    ``summarize`` (the D24 opt-in derived-summary declarations,
    ``{source_field: {"as": name, ...}}`` — recorded in the pyramid block,
    never the semantic core).
    """
    raw = config.output.get("pyramid")
    if raw is False:
        return None
    if raw is None:
        knob: dict = {}
    elif isinstance(raw, dict):
        knob = dict(raw)
    else:
        raise ValueError(f"output.pyramid must be a mapping or false (got {raw!r})")
    if _declares_windowing(config):
        knob.setdefault("all_time", True)
    return knob


def _declares_windowing(config: PipelineConfig) -> bool:
    """Whether ``output.windowing`` declares a known window schedule, read off the raw block.

    The same test :func:`get_windowing` opens with, without the normalization
    — so it is safe before :func:`_validate_windowing` has run. A non-mapping
    block, or a schedule that is not a known one (a typo, a reserved name,
    ``null``), answers ``False`` here and is refused by name there, so the
    windowing refusal is never masked by the all-time one.
    """
    from zagg import windows as _windows

    block = config.output.get("windowing")
    if not isinstance(block, dict):
        return False
    try:
        return _windows.check_schedule(block.get("schedule", "none")) != "none"
    except (TypeError, ValueError):
        return False


def get_aoi_mask(config: PipelineConfig) -> bool:
    """Whether the optional strict-AOI cell mask is enabled (issue #101).

    ``output.aoi_mask: true`` packages a per-cell boolean ``aoi_mask`` array
    aligned to the output cell grid, ``True`` where the cell is inside the AOI.
    Defaults to ``False`` — when off, no array is emitted and outputs are
    byte-identical to a run without the feature.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    bool
    """
    return bool(config.output.get("aoi_mask", False))


def get_consolidate_metadata(config: PipelineConfig) -> bool:
    """Whether the finalize step consolidates zarr metadata (issue #191).

    ``output.consolidate_metadata: true`` runs ``zarr.consolidate_metadata`` as a
    finalize step after every cell completes, producing a consolidated-metadata
    blob. Defaults to ``False``: no zagg reader opens with ``use_consolidated`` —
    readers navigate to specific paths in a few GETs — and consolidation is an
    optional zarr-v3 extension that costs ~70 s of serial metadata GETs per run.
    When off, the finalize invoke is skipped entirely and stores read fine (v3
    readers do lazy metadata reads natively).

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    bool
    """
    return bool(config.output.get("consolidate_metadata", False))


def get_output_endpoint_url(config: PipelineConfig) -> str | None:
    """Return the output S3 endpoint URL from the output config, or None.

    Non-secret S3-compatible endpoint (e.g. R2, MinIO). Credentials are never
    stored in config; they are supplied at runtime.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("endpoint_url")


def get_output_region(config: PipelineConfig) -> str | None:
    """Return the output S3 region from the output config, or None.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("region")
