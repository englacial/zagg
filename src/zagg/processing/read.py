"""Read-stage helpers for :mod:`zagg.processing` (split out of the monolithic
``processing.py`` for the §4 size limit; pure relocation, no behavior change).

Reads and spatially filters HDF5 groups for one shard. Depends only on
``config``/``read_plan``/``grids``/``schema`` — never on the aggregate or write
stages — so the import DAG stays acyclic.
"""

import numpy as np
import pandas as pd

from zagg.config import (
    evaluate_filter_expression,
    filters_from_data_source,
)
from zagg.read_plan import execute_read_plan, plan_read


def _make_url_rewriter(driver: str | None):
    """Return a function that converts a granule URL for the active h5coro driver.

    The ShardMap carries the driver-appropriate href already (S3 vs HTTPS is
    chosen at dispatch), so this only strips the ``s3://`` scheme for the S3
    driver (h5coro's S3Driver expects ``bucket/key``); HTTPS is used as-is.
    """
    if driver == "https":
        return lambda url: url
    return lambda url: url.replace("s3://", "", 1)


_COMPARE = {
    "eq": np.equal,
    "ne": np.not_equal,
    "ge": np.greater_equal,
    "le": np.less_equal,
    "lt": np.less,
    "gt": np.greater,
}


def _expand_mask_to_base(
    coarse_mask: np.ndarray,
    index_beg_arr: np.ndarray,
    count_arr: np.ndarray,
    index_base: int,
    total_base_size: int,
) -> np.ndarray:
    """Expand a coarse-rate boolean mask to a base-rate boolean mask (issue #43, Phase B).

    Each coarse parent ``p`` covers base-rate rows
    ``index_beg_arr[p] - index_base, ..., index_beg_arr[p] - index_base + count_arr[p] - 1``.
    The contiguity assumption: ranges do not overlap and together tile the full base array.

    Parameters
    ----------
    coarse_mask : np.ndarray
        1-D boolean array of length ``n_parents``.
    index_beg_arr : np.ndarray
        Per-parent start index into the base array (before ``index_base`` shift).
    count_arr : np.ndarray
        Per-parent child count (number of base-rate rows this parent covers).
    index_base : int
        Subtracted from ``index_beg_arr`` to get 0-based base indices.
    total_base_size : int
        Length of the output base-rate array.

    Returns
    -------
    np.ndarray
        1-D boolean array of length ``total_base_size``.
    """
    out = np.zeros(total_base_size, dtype=bool)
    for p, keep in enumerate(coarse_mask):
        if not keep:
            continue
        cnt = int(count_arr[p])
        # Empty parents cover no base rows. Real ATL03 marks them with
        # ``count == 0`` AND ``ph_index_beg == 0`` (issue #116), so under
        # ``index_base=1`` they would otherwise give ``beg = 0 - 1 = -1`` and
        # raise below; skip them, mirroring the non-empty-only contract
        # ``read_plan.plan_read`` already uses (its ``cnt > 0`` skip).
        if cnt == 0:
            continue
        beg = int(index_beg_arr[p]) - index_base
        if beg < 0:
            raise ValueError(
                f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
            )
        out[beg : beg + cnt] = True
    return out


def _broadcast_segment_to_base(
    seg_values: np.ndarray,
    index_beg_arr: np.ndarray,
    count_arr: np.ndarray,
    index_base: int,
    total_base_size: int,
) -> np.ndarray:
    """Broadcast a per-segment variable to a base-rate (per-photon) array (issue #30).

    Each coarse parent ``p`` covers base-rate rows ``index_beg_arr[p] - index_base``
    through ``... + count_arr[p] - 1`` (the same contiguous parent->child tiling
    :func:`_expand_mask_to_base` expands a mask over). Under #43's contiguity
    assumption (ranges do not overlap and together tile the full base array) this
    equals ``np.repeat(seg_values, count_arr)``; placing by ``index_beg`` keeps the
    per-parent value correctly positioned even when ``index_beg`` is shifted
    (``index_base``). Any base row left untiled (a gap, if the contiguity assumption
    is violated) is filled with ``NaN`` for float dtypes so it surfaces as a missing
    value rather than uninitialized garbage; non-float dtypes are zero-filled. The
    returned array carries each photon's segment value (e.g. ``dem_h``, one value per
    ~100 photons) so it can ride alongside the base-rate variables through the read
    plan's spatial/keep masks.

    Parameters
    ----------
    seg_values : np.ndarray
        1-D per-parent values of length ``n_parents``.
    index_beg_arr, count_arr, index_base, total_base_size
        As in :func:`_expand_mask_to_base`.

    Returns
    -------
    np.ndarray
        1-D array of length ``total_base_size`` and ``seg_values``' dtype.

    Raises
    ------
    ValueError
        If a parent's range starts before 0 or extends past ``total_base_size``
        (a tiling that does not fit the declared base size — e.g. a segment-level
        variable on a level whose link does not match the read's base extent).
    """
    # NaN-fill floats so an untiled gap reads as missing, not garbage (the mask path
    # is safe-by-construction with np.zeros; a value array has no such safe default).
    if np.issubdtype(seg_values.dtype, np.floating):
        out = np.full(total_base_size, np.nan, dtype=seg_values.dtype)
    else:
        out = np.zeros(total_base_size, dtype=seg_values.dtype)
    for p in range(len(seg_values)):
        cnt = int(count_arr[p])
        # Empty segments cover no photons. Real ATL03 marks them with
        # ``count == 0`` AND ``ph_index_beg == 0`` (issue #116, see
        # ``read_plan.plan_read``'s ``cnt > 0`` skip); under ``index_base=1``
        # that gives ``beg = 0 - 1 = -1`` and would raise below, which is what
        # made the gain_bias dem_h broadcast drop every photon. Skip them.
        if cnt == 0:
            continue
        beg = int(index_beg_arr[p]) - index_base
        if beg < 0:
            raise ValueError(
                f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
            )
        if beg + cnt > total_base_size:
            raise ValueError(
                f"segment {p} range [{beg}:{beg + cnt}] exceeds base size {total_base_size}; "
                f"the segment-level variable's link does not tile the read's base extent"
            )
        out[beg : beg + cnt] = seg_values[p]
    return out


def _segment_level_variables(data_source: dict) -> dict[str, dict[str, str]]:
    """Collect declared segment-level (non-base) readable variables (issue #30).

    A non-base level may declare ``variables`` as a ``{name: path-template}``
    mapping (the readable form, distinct from the documentation-only ``list[str]``
    form). Each such variable is read at coarse rate and broadcast to the base
    (photon) rows via the level's ``link`` (``_broadcast_segment_to_base``), so a
    per-segment field like ``dem_h`` becomes a per-photon column the aggregation /
    ``chunk_precompute`` can reduce. Returns ``{level_key: {name: template}}`` for
    every non-base level carrying a dict ``variables``; empty when none do, so the
    read path is unchanged for configs without it.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or base_level is None:
        return {}
    out: dict[str, dict[str, str]] = {}
    for name, lvl in levels.items():
        if name == base_level or not isinstance(lvl, dict):
            continue
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict) and lvl_vars:
            out[name] = dict(lvl_vars)
    return out


def _read_segment_broadcasts(
    h5obj, group: str, data_source: dict, levels: dict, n_base: int
) -> dict[str, np.ndarray]:
    """Read each segment-level variable and broadcast it to a base-rate column (issue #30).

    For every non-base level carrying a ``{name: path}`` ``variables`` mapping, read
    the variable and the level's link arrays at coarse rate, then broadcast to the
    base (photon) rows via :func:`_broadcast_segment_to_base`. Returns
    ``{name: base_rate_array}`` (length ``n_base``), ready to be sliced through the
    same spatial / keep masks the base-rate variables are. A variable name colliding
    with a ``data_source.variables`` column is rejected (it would shadow the read).
    """
    seg_vars = _segment_level_variables(data_source)
    if not seg_vars:
        return {}
    base_cols = set(data_source.get("variables", {}))
    out: dict[str, np.ndarray] = {}
    for level_key, mapping in seg_vars.items():
        lvl = levels[level_key]
        link = lvl["link"]
        index_base = int(link.get("index_base", 0))
        ibeg_path = link["index_beg"].format(group=group)
        cnt_path = link["count"].format(group=group)
        link_data = h5obj.readDatasets([ibeg_path, cnt_path])
        ibeg_arr = link_data[ibeg_path]
        cnt_arr = link_data[cnt_path]
        for col_name, tmpl in mapping.items():
            if col_name in base_cols:
                raise ValueError(
                    f"segment-level variable '{col_name}' on level '{level_key}' "
                    f"collides with a data_source.variables column"
                )
            seg_path = tmpl.format(group=group)
            seg_values = np.asarray(h5obj.readDatasets([seg_path])[seg_path])
            out[col_name] = _broadcast_segment_to_base(
                seg_values, ibeg_arr, cnt_arr, index_base, n_base
            )
    return out


def _predicate_mask(arr: np.ndarray, f: dict) -> np.ndarray:
    """Build a 1-D boolean keep-mask for one structured predicate (issue #43).

    ``f`` is a normalized structured filter (see :func:`zagg.config.get_filters`):
    ``{op, column, value|values, keep}``. An integer ``column`` selects a column
    from a 2-D flag array before comparing; it is required for N-D arrays and
    rejected for 1-D arrays. ``keep: false`` inverts the result (drop matches).
    """
    column = f.get("column")
    if arr.ndim > 1:
        if column is None:
            raise ValueError(f"filter on '{f['dataset']}': N-D array requires an integer 'column'")
        arr = arr[:, column]
    elif column is not None:
        raise ValueError(f"filter on '{f['dataset']}': 'column' set but array is 1-D")

    op = f["op"]
    if op == "in":
        mask = np.isin(arr, f["values"])
    elif op == "not_in":
        mask = ~np.isin(arr, f["values"])
    else:
        mask = _COMPARE[op](arr, f["value"])
    if not f.get("keep", True):
        mask = ~mask
    return mask


def _level_coord_paths(level: dict, group: str) -> tuple[str, str]:
    """Resolve ``(latitude, longitude)`` HDF5 paths for a coarse-level spatial index.

    The level's ``coordinates`` field is a ``{latitude, longitude}`` dict of names
    relative to the level's ``path`` template (matching the schema in #43's issue
    body). Both halves are required for the ``read_plan`` to compute an AOI box.
    """
    coords = level.get("coordinates")
    if not isinstance(coords, dict) or "latitude" not in coords or "longitude" not in coords:
        raise ValueError(
            "read_plan.spatial_index level requires "
            "'coordinates: {latitude: <name>, longitude: <name>}'"
        )
    base = level["path"].format(group=group).rstrip("/")
    lat_name = coords["latitude"]
    lon_name = coords["longitude"]
    # Allow either a relative name (joined to the level path) or an absolute path
    # template (already group-substituted on .format above? no -- coords names
    # don't carry templates; keep them simple). Absolute paths win as-is.
    lat_path = lat_name if lat_name.startswith("/") else f"{base}/{lat_name}"
    lon_path = lon_name if lon_name.startswith("/") else f"{base}/{lon_name}"
    return lat_path, lon_path


def _planned_read_group(
    h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
):
    """Planned (AOI-bounded) read of one HDF5 group via the coarse spatial index.

    Issue #43 Phase C: when ``data_source.read_plan.spatial_index`` names a coarse
    level whose ``link`` points at the base level, we read the coarse coordinates
    + link arrays once (small), call :func:`zagg.read_plan.plan_read` with the
    mortie segment->shard mask (``grid.shards_of(grid.assign(...)) == shard_key``,
    the same exact test the photon path applies) to compute which base-rate
    slices the shard actually touches, and read base-rate
    coords + variables + filter datasets only over those slices via
    :func:`zagg.read_plan.execute_read_plan`. This avoids the
    ``lat_ph`` + ``lon_ph`` full-coord read (up to ~245 MB per ATL03 beam) that
    drives Lambda OOMs (issue #43 motivation).

    Falls back transparently to :func:`_read_group` when:
    - the empty-AOI short-circuit fires (no parents match) → return ``None``;
    - ``plan_read`` flags ``full_read=True`` (selectivity above threshold);
    - the cell ``signal_conf_ph``-style 2-D structured filter would be re-read
      via the planned slices either way (the helper handles that uniformly).

    Returns the same ``pandas.DataFrame`` / ``pyarrow.Table`` / ``None`` contract
    as :func:`_read_group`. Output rows are in plan-slice / spatial-mask /
    filter order — which matches the full-read path's row ordering because the
    plan's runs are emitted in increasing parent index.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    levels = data_source["levels"]
    base_level_key = data_source["base_level"]
    rp = data_source["read_plan"]
    spatial_index_level = rp["spatial_index"]
    pad = int(rp.get("pad", 1))
    full_read_threshold = float(rp.get("full_read_threshold", 0.9))

    si_lvl = levels[spatial_index_level]
    link = si_lvl.get("link")
    if not isinstance(link, dict):
        raise ValueError(f"read_plan.spatial_index level {spatial_index_level!r} requires a 'link'")
    if link["to"] != base_level_key:
        raise ValueError(
            f"read_plan.spatial_index level {spatial_index_level!r} must link "
            f"directly to base level {base_level_key!r} (got link.to={link['to']!r})"
        )
    index_base = int(link.get("index_base", 0))

    # Read coarse-level coordinates + link arrays in one go (small — geolocation
    # rate is ~30x lighter than photon rate on ATL03).
    si_lat_path, si_lon_path = _level_coord_paths(si_lvl, group)
    ibeg_path = link["index_beg"].format(group=group)
    cnt_path = link["count"].format(group=group)
    coarse_data = h5obj.readDatasets([si_lat_path, si_lon_path, ibeg_path, cnt_path])
    coarse_lats = coarse_data[si_lat_path]
    coarse_lons = coarse_data[si_lon_path]
    ibeg_arr = coarse_data[ibeg_path]
    cnt_arr = coarse_data[cnt_path]

    if len(coarse_lats) == 0:
        return None

    # ``n_base`` under #43's contiguity assumption ("ranges do not overlap and
    # together tile the full base array" -- :func:`_expand_mask_to_base`).
    # ``int(cnt_arr.sum())`` makes the assumption explicit and is identical to
    # ``ibeg_arr[-1] - index_base + cnt_arr[-1]`` when contiguity holds. If a
    # future granule format drops trailing photons or gaps between parents,
    # either form under- or over-estimates -- track via a follow-up to #43.
    n_base = int(np.asarray(cnt_arr).sum())
    if n_base <= 0:
        return None

    # Match segments to this shard with the SAME mortie test the photon path
    # applies below (``grid.shards_of(grid.assign(...)) == shard_key``), not a
    # loose bbox + per-segment shapely scan (issue #95). It is exact to the leaf
    # cell, vectorized (~280x faster than the shapely loop on a 181k-segment
    # ATL03 beam), and antimeridian/polar-correct -- so the wide-bbox bail the
    # old bbox path needed is gone; a shard that genuinely spans most segments is
    # still caught by ``plan_read``'s selectivity ``full_read`` fallback. The
    # mask is rep-point based, so a boundary segment whose photons straddle the
    # shard edge is recovered by ``pad`` (and the photon-level filter below never
    # over-includes); residual omission is bounded to a few edge photons (#95).
    coarse_leaf = grid.assign(np.asarray(coarse_lats), np.asarray(coarse_lons))
    coarse_mask = grid.shards_of(coarse_leaf) == shard_key

    plan = plan_read(
        np.asarray(coarse_lats),
        np.asarray(coarse_lons),
        np.asarray(ibeg_arr),
        np.asarray(cnt_arr),
        n_base,
        index_base=index_base,
        pad=pad,
        full_read_threshold=full_read_threshold,
        coarse_mask=coarse_mask,
    )

    if not plan.parent_runs:
        return None  # empty AOI -- no parent intersects, skip the group entirely

    if plan.full_read:
        # Selectivity above threshold: many small reads would still sum to most
        # of the file. Defer to the full-coord-read path; semantics identical.
        return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)

    # h5coro-compatible reader callback for execute_read_plan.
    def _read_fn(path, hyperslice=None):
        if hyperslice is None:
            return h5obj.readDatasets([path])[path]
        return h5obj.readDatasets([{"dataset": path, "hyperslice": hyperslice}])[path]

    # ---- Read base coords + variables + filter datasets over the planned slices.
    filters = filters_from_data_source(data_source)
    base_structured = [
        f
        for f in filters
        if "expression" not in f and (f.get("level") is None or f.get("level") == base_level_key)
    ]
    coarse_structured = [
        f
        for f in filters
        if "expression" not in f and f.get("level") is not None and f.get("level") != base_level_key
    ]
    expressions = [f for f in filters if "expression" in f]

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = execute_read_plan(plan, _read_fn, lat_path, np.float64)
    lons = execute_read_plan(plan, _read_fn, lon_path, np.float64)

    if len(lats) == 0:
        return None

    # Apply spatial / shard mask over the concatenated planned reads.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == shard_key
    if np.sum(mask_spatial) == 0:
        return None

    # Read the variables and base-level filter datasets via the same plan. Read
    # each distinct path once (the variable and filter dataset paths can coincide).
    var_paths = {col: tmpl.format(group=group) for col, tmpl in variables.items()}
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    paths_seen: set[str] = set()
    arrays_by_path: dict[str, np.ndarray] = {}
    for path in list(var_paths.values()) + list(filter_paths.values()):
        if path in paths_seen:
            continue
        paths_seen.add(path)
        # dtype hint isn't load-bearing -- execute_read_plan dtype-casts via
        # np.asarray, which is a no-op when the source dtype already matches.
        arrays_by_path[path] = execute_read_plan(plan, _read_fn, path, None)

    # Base-level structured filters: ANDed keep-masks over the concatenated reads.
    keep_mask: np.ndarray | None = None
    for f in base_structured:
        flag = arrays_by_path[filter_paths[id(f)]][mask_spatial]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    # Cross-level (Phase B) filters: read coarse flags fully, expand to base
    # rate (length n_base), then subset to the planned indices.
    if coarse_structured:
        # Build the global base-index array once: which original-base positions
        # are present in the concatenated planned read.
        global_idx = np.concatenate([np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices])
        cross_full: np.ndarray | None = None
        for f in coarse_structured:
            level_key = f["level"]
            cf_lvl = levels[level_key]
            cf_link = cf_lvl["link"]
            cf_index_base = int(cf_link.get("index_base", 0))
            cf_flag_path = f["dataset"].format(group=group)
            cf_ibeg_path = cf_link["index_beg"].format(group=group)
            cf_cnt_path = cf_link["count"].format(group=group)
            cf_data = h5obj.readDatasets([cf_flag_path, cf_ibeg_path, cf_cnt_path])
            cf_flag = cf_data[cf_flag_path]
            cf_ibeg = cf_data[cf_ibeg_path]
            cf_cnt = cf_data[cf_cnt_path]
            coarse_fmask = _predicate_mask(cf_flag, f)
            expanded = _expand_mask_to_base(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, n_base)
            cross_full = expanded if cross_full is None else (cross_full & expanded)
        # Subset the full-length mask to the concatenated planned indices, then
        # to the spatial keep window so it lines up with keep_mask above.
        cross_planned = cross_full[global_idx][mask_spatial]
        keep_mask = cross_planned if keep_mask is None else (keep_mask & cross_planned)

    if keep_mask is not None and np.sum(keep_mask) == 0:
        return None

    # Segment-level variables (issue #30): read each declared non-base-level
    # variable and broadcast it to a base-rate per-photon column (length n_base),
    # then subset to the concatenated planned indices so it lines up with the
    # base-rate variables before the spatial / keep masks below.
    seg_broadcasts = _read_segment_broadcasts(h5obj, group, data_source, levels, n_base)
    if seg_broadcasts:
        seg_global_idx = np.concatenate(
            [np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices]
        )

    # Build the data dict (variables sliced to mask_spatial, then to keep_mask).
    leaf_after_spatial = leaf_ids[mask_spatial]
    data_dict: dict[str, np.ndarray] = {}
    for col_name, path in var_paths.items():
        values = arrays_by_path[path][mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, base_values in seg_broadcasts.items():
        values = base_values[seg_global_idx][mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    data_dict["leaf_id"] = (
        leaf_after_spatial[keep_mask] if keep_mask is not None else leaf_after_spatial
    )

    # Base-level expression filters (aggregation-time escape hatch, no pushdown).
    # The namespace carries both base-rate ``variables`` and any segment-level
    # broadcast columns (issue #30), which are already materialized into
    # ``data_dict`` above, so an expression filter may reference e.g. ``dem_h``.
    expr_names = list(variables) + list(seg_broadcasts)
    for f in expressions:
        cols = {c: data_dict[c] for c in expr_names if c in data_dict}
        try:
            emask = evaluate_filter_expression(f["expression"], cols)
        except NameError as e:
            raise NameError(
                f"expression filter {f['expression']!r} references an undefined name: {e}"
            ) from e
        if emask.shape != data_dict["leaf_id"].shape:
            raise ValueError(
                f"expression filter {f['expression']!r} must yield a per-row "
                f"boolean mask (got shape {emask.shape})"
            )
        if np.sum(emask) == 0:
            return None
        data_dict = {k: v[emask] for k, v in data_dict.items()}

    if arrow:
        import pyarrow as pa

        return pa.table(data_dict)
    return pd.DataFrame(data_dict)


def _read_group(h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False):
    """Read and spatially filter one HDF5 group.

    Returns a ``pandas.DataFrame`` (default) or, when ``arrow=True``, a
    ``pyarrow.Table`` carrying the identical columns. Returns ``None`` when the
    group has no observations in this shard.

    Supports three modes (issues #43 Phase A/B/C):

    *Flat* (no ``levels``/``base_level`` in ``data_source``): unchanged from Phase A —
    all structured filters are applied directly to base-rate data.

    *Hierarchical filtering* (``levels`` + ``base_level`` present): structured
    filters whose normalized ``level`` key names a non-base level are applied at
    coarse rate, then expanded to base-rate via the level's ``link`` arrays
    (``_expand_mask_to_base``). Base-level structured filters and expression
    filters are unchanged.

    *Hierarchical (planned) read* (``read_plan.spatial_index`` set, in addition
    to ``levels``/``base_level``): the coarse-level spatial-index coordinates
    are read fully (cheap), matched to the shard with the mortie segment->shard
    mask (``grid.shards_of(grid.assign(...)) == shard_key``), and base-rate
    coords + variables + filter datasets are read only
    over the planned hyperslices via :func:`zagg.read_plan.execute_read_plan`.
    Empty-AOI groups short-circuit to ``None``. Selectivity above the configured
    threshold falls back to the full-read path; the planned and full paths
    produce row-for-row identical output (#43 Phase C parity).
    """
    rp = data_source.get("read_plan")
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    # Truthy-checking ``levels``/``base_level`` would route an empty ``{}`` (a
    # config typo, easy to do) back to the full-read path silently. Reject
    # incomplete configurations explicitly instead -- the planned path is
    # gated only when ``spatial_index`` is set, and *then* requires a real
    # multi-level structure to operate on.
    if isinstance(rp, dict) and rp.get("spatial_index"):
        if not isinstance(levels, dict) or not levels:
            raise ValueError(
                "data_source.read_plan.spatial_index requires a non-empty 'levels' mapping"
            )
        if not base_level:
            raise ValueError("data_source.read_plan.spatial_index requires 'base_level'")
        return _planned_read_group(h5obj, group, data_source, shard_key, grid, arrow=arrow)
    return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)


def _read_group_full(
    h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
):
    """Full-coord-read variant of :func:`_read_group` (the pre-#49-Phase-C path).

    Reads the base-rate coordinate arrays in full, computes the spatial mask,
    then hyperslices variables + base-level filter datasets to the matched
    ``[min_idx, max_idx]`` range. Cross-level structured filters are read fully
    at coarse rate and expanded to base-rate via ``_expand_mask_to_base``.
    Expression filters apply over already-read variable columns.

    Kept as the explicit fallback for: groups whose ``data_source`` declares no
    ``read_plan.spatial_index``; ``plan_read``'s selectivity fallback
    (``full_read=True``); and the legacy flat (no-levels) form.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    filters = filters_from_data_source(data_source)
    base_level_key = data_source.get("base_level")
    levels = data_source.get("levels")
    # Partition filters: base-level structured, coarse-level structured, expressions.
    base_structured = [
        f
        for f in filters
        if "expression" not in f and (f.get("level") is None or f.get("level") == base_level_key)
    ]
    coarse_structured = [
        f
        for f in filters
        if "expression" not in f and f.get("level") is not None and f.get("level") != base_level_key
    ]
    expressions = [f for f in filters if "expression" in f]

    # Resolve coordinate paths
    coord_paths = [path.format(group=group) for path in coordinates.values()]
    coord_data = h5obj.readDatasets(coord_paths)

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = coord_data[lat_path]
    lons = coord_data[lon_path]

    if len(lats) == 0:
        return None

    # Assign points to leaf cells, then filter to the current shard.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == shard_key

    if np.sum(mask_spatial) == 0:
        return None

    # Bounding indices for hyperslice read
    indices = np.where(mask_spatial)[0]
    min_idx = int(indices[0])
    max_idx = int(indices[-1]) + 1

    # --- Coarse-level filter expansion (Phase B) ---
    # For each filter whose level is not the base level, read the coarse-rate
    # flag array from the declared level path, build a coarse mask, then expand
    # to base-rate via the level link arrays.  AND the results into ``cross_mask``.
    cross_mask: np.ndarray | None = None
    if coarse_structured and levels is not None:
        for f in coarse_structured:
            level_key = f["level"]
            lvl = levels[level_key]
            flag_path = f["dataset"].format(group=group)
            # Read the coarse flag array (full level, no hyperslice — we need all parents
            # to align with link arrays which are also full-length).
            coarse_data = h5obj.readDatasets([{"dataset": flag_path}])
            coarse_arr = coarse_data[flag_path]
            coarse_fmask = _predicate_mask(coarse_arr, f)
            # Read the link arrays from this level.
            link = lvl["link"]
            index_base = int(link.get("index_base", 0))
            ibeg_path = link["index_beg"].format(group=group)
            cnt_path = link["count"].format(group=group)
            link_data = h5obj.readDatasets(
                [
                    {"dataset": ibeg_path},
                    {"dataset": cnt_path},
                ]
            )
            ibeg_arr = link_data[ibeg_path]
            cnt_arr = link_data[cnt_path]
            expanded = _expand_mask_to_base(coarse_fmask, ibeg_arr, cnt_arr, index_base, len(lats))
            cross_mask = expanded if cross_mask is None else (cross_mask & expanded)
        if cross_mask is not None and np.sum(cross_mask[min_idx:max_idx]) == 0:
            return None

    # Build hyperslice dataset list: variables + any base-level structured-filter arrays.
    # Read each distinct path once; flag datasets may coincide with a variable.
    datasets = []
    paths_seen = set()
    var_paths = {col: tmpl.format(group=group) for col, tmpl in variables.items()}
    for path in var_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    for path in filter_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)

    data = h5obj.readDatasets(datasets)

    # Apply spatial mask to sliced data
    mask_sliced = mask_spatial[min_idx:max_idx]

    # Combine base-level structured predicates as ANDed keep-masks (issue #43).
    keep_mask = None
    for f in base_structured:
        flag = data[filter_paths[id(f)]][mask_sliced]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    # AND in the cross-level expanded mask, aligned to the sliced window.
    if cross_mask is not None:
        cross_sliced = cross_mask[min_idx:max_idx][mask_sliced]
        keep_mask = cross_sliced if keep_mask is None else (keep_mask & cross_sliced)

    if keep_mask is not None and np.sum(keep_mask) == 0:
        return None

    # Segment-level variables (issue #30): read each declared non-base-level
    # variable and broadcast it to a base-rate per-photon column (length len(lats))
    # so it can be sliced through the same masks as the base-rate variables below.
    seg_broadcasts = _read_segment_broadcasts(h5obj, group, data_source, levels or {}, len(lats))

    # Build dataframe (variables sliced to spatial mask, then to the keep-mask)
    leaf_sliced = leaf_ids[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path in var_paths.items():
        values = data[path][mask_sliced]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, base_values in seg_broadcasts.items():
        values = base_values[min_idx:max_idx][mask_sliced]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values

    if keep_mask is not None:
        data_dict["leaf_id"] = leaf_sliced[keep_mask]
    else:
        data_dict["leaf_id"] = leaf_sliced

    # Base-level ``expression`` filters: aggregation-time escape hatch, evaluated
    # over the already-read variable columns (forfeits pushdown, issue #43). The
    # namespace also carries any segment-level broadcast columns (issue #30),
    # materialized into ``data_dict`` above, so a filter may reference e.g.
    # ``dem_h``.
    expr_names = list(variables) + list(seg_broadcasts)
    for f in expressions:
        cols = {c: data_dict[c] for c in expr_names if c in data_dict}
        try:
            emask = evaluate_filter_expression(f["expression"], cols)
        except NameError as e:
            raise NameError(
                f"expression filter {f['expression']!r} references an undefined name: {e}"
            ) from e
        if emask.shape != data_dict["leaf_id"].shape:
            raise ValueError(
                f"expression filter {f['expression']!r} must yield a per-row "
                f"boolean mask (got shape {emask.shape})"
            )
        if np.sum(emask) == 0:
            return None
        data_dict = {k: v[emask] for k, v in data_dict.items()}

    if arrow:
        import pyarrow as pa

        return pa.table(data_dict)
    return pd.DataFrame(data_dict)
