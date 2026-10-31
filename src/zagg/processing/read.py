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
        # ``read_plan.plan_read`` already uses (its ``cnt > 0`` skip). Skipping
        # intentionally bypasses the ``beg < 0`` validation for these parents --
        # correct, since they map to zero base rows (a non-empty parent with
        # ``beg < 0`` still raises).
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
        # made the gain_bias dem_h broadcast drop every photon. Skip them; this
        # intentionally bypasses the ``beg < 0`` / ``beg + cnt > base`` checks
        # for empties (they map to zero base rows). A non-empty segment with
        # ``beg < 0`` or an over-extending range still raises below.
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


def link_base_extent(index_beg_arr, count_arr, index_base: int) -> int:
    """Length of the base array a record link tiles (issue #452).

    ``max(index_beg - index_base + count)`` over the non-empty records: the last
    base row any record reaches, which is the extent every consumer of a link
    needs — :func:`zagg.read_plan.plan_read`'s ``base_end`` clamp, the gather
    maps, the broadcasts.

    On a **contiguous** product (records tile the base array end to end — #43's
    assumption, ATL03's shape) this IS ``Σcount``, so substituting it is a
    no-op. On a **strided** one it is not: GEDI L1B allocates a fixed
    1,420-sample window per shot in ``rxwaveform`` while ``rx_sample_count`` is
    the valid-sample count (61–1,420, typically ~700), so ``Σcount``
    understates the flat array by ~50%, every planned run clamps to
    ``base_end <= base_start``, and the whole read silently returns zero rows
    (issue #452).

    Empty records contribute nothing: ``count == 0`` marks them and their
    origin-1 sentinel start (0) is not a real position (issue #116) — the same
    skip :func:`_expand_mask_to_base` and :func:`_broadcast_segment_to_base`
    apply. An all-empty link has extent 0.
    """
    beg = np.asarray(index_beg_arr).astype(np.int64) - index_base
    cnt = np.asarray(count_arr).astype(np.int64)
    nonempty = cnt > 0
    if not nonempty.any():
        return 0
    return int((beg[nonempty] + cnt[nonempty]).max())


def _validate_link_disjoint(
    index_beg_arr, count_arr, index_base: int, level: str | None = None
) -> None:
    """Refuse a link whose non-empty records overlap (issue #452).

    Disjointness is the link grammar's standing precondition (#43's contiguity
    assumption, minus the requirement that the ranges also *tile* — a strided
    product leaves gaps). Until now it was assumed everywhere and checked
    nowhere: the paint maps (:func:`zagg.processing.read_vlen.expand_link_indices`,
    :func:`zagg.processing.read_vlen._planned_gather_map`,
    :func:`_broadcast_segment_to_base`) validate only ``beg >= 0`` and
    ``beg + cnt <= n_base``, and *resolve* an overlap by paint order — a later
    record silently shadows an earlier one.

    That mattered because the route carries two independently-derived owner
    maps: paint order above, and last-start-wins in
    :func:`_link_parent_at_rows`. On disjoint records they agree exactly (3,000
    randomized links, 0 divergences); on overlapping ones they do not, and the
    disagreement is per-row and silent — coordinates and the synthesized column
    would come from one record while a companion variable and any record-level
    filter verdict came from another. Validating here, once per level where the
    link is first assembled, turns the precondition into enforced truth: a
    malformed product now fails loudly instead of writing mismatched columns.

    Empty records (``count == 0``) own no rows and are skipped (issue #116).
    """
    beg = np.asarray(index_beg_arr).astype(np.int64) - index_base
    cnt = np.asarray(count_arr).astype(np.int64)
    nonempty = np.flatnonzero(cnt > 0)
    if nonempty.size < 2:
        return
    order = nonempty[np.argsort(beg[nonempty], kind="stable")]
    beg_sorted = beg[order]
    end_sorted = beg_sorted + cnt[order]
    bad = np.flatnonzero(beg_sorted[1:] < end_sorted[:-1])
    if bad.size:
        i = int(bad[0])
        where = f" on level {level!r}" if level is not None else ""
        raise ValueError(
            f"link records{where} overlap: record {int(order[i])} covers base rows "
            f"[{int(beg_sorted[i])}:{int(end_sorted[i])}] but record {int(order[i + 1])} "
            f"starts at {int(beg_sorted[i + 1])}; the link grammar requires disjoint "
            f"record ranges"
        )


def _link_parent_at_rows(
    index_beg_arr, count_arr, index_base: int, base_rows: np.ndarray
) -> np.ndarray:
    """Owning record index per given base row, ``-1`` where none (issue #452).

    The planned-rate twin of the paint-and-slice loops above: instead of
    building a length-``n_base`` array and selecting the read's rows out of it,
    each requested row's record is located directly, with one ``searchsorted``
    over the link's starts. On the vlen route ``n_base`` is sample rate (~2·10^8
    per GEDI beam group) while ``base_rows`` is the plan's rows (~3·10^5), so
    the difference is a 2 GB worker surviving or not (the issue #43 OOM
    posture, applied to the expansion side).

    Records must not overlap — the link grammar's contract, enforced by
    :func:`_validate_link_disjoint` where each level's link is assembled, since
    this derivation (last start at or before the row wins) and the paint maps'
    (later record shadows earlier) agree only on disjoint records. Ties on an
    identical start resolve to the later record either way, matching the paint
    order of :func:`_broadcast_segment_to_base`. Empty records (``count == 0``)
    own nothing (issue #116).
    """
    beg = np.asarray(index_beg_arr).astype(np.int64) - index_base
    cnt = np.asarray(count_arr).astype(np.int64)
    rows = np.asarray(base_rows, dtype=np.int64)
    nonempty = np.flatnonzero(cnt > 0)
    if nonempty.size == 0 or rows.size == 0:
        return np.full(rows.shape, -1, dtype=np.int64)
    order = nonempty[np.argsort(beg[nonempty], kind="stable")]
    pos = np.searchsorted(beg[order], rows, side="right") - 1
    inside = pos >= 0
    owner = order[np.where(inside, pos, 0)]
    inside &= rows < beg[owner] + cnt[owner]
    return np.where(inside, owner, -1)


def _expand_mask_at_rows(
    coarse_mask: np.ndarray,
    index_beg_arr,
    count_arr,
    index_base: int,
    base_rows: np.ndarray,
) -> np.ndarray:
    """:func:`_expand_mask_to_base` restricted to ``base_rows`` (issue #452).

    Identical to ``_expand_mask_to_base(...)[base_rows]`` — a row owned by no
    record is ``False`` there and here — without the length-``n_base``
    intermediate (196 MB per cross-level filter on a GEDI beam group).

    Equivalence includes the *scope* of the ``beg < 0`` refusal: the base-rate
    form checks it inside a loop the predicate guard has already ``continue``\\ d
    past, so a record the predicate rejects is never validated. Widening that to
    every non-empty record would fail-hard on a granule the production route
    reads fine (a malformed start on a record that is filtered out anyway), so
    the keep-mask is part of the test here too.
    """
    keep = np.asarray(coarse_mask, dtype=bool)
    beg = np.asarray(index_beg_arr).astype(np.int64) - index_base
    cnt = np.asarray(count_arr).astype(np.int64)
    bad = keep & (cnt > 0) & (beg < 0)
    if bad.any():
        p = int(np.argmax(bad))
        raise ValueError(
            f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
        )
    parent = _link_parent_at_rows(index_beg_arr, count_arr, index_base, base_rows)
    valid = parent >= 0
    return keep[np.where(valid, parent, 0)] & valid


def _gather_segment_at_rows(
    seg_values: np.ndarray,
    index_beg_arr,
    count_arr,
    index_base: int,
    total_base_size: int,
    base_rows: np.ndarray,
) -> np.ndarray:
    """:func:`_broadcast_segment_to_base` restricted to ``base_rows`` (issue #452).

    Identical to ``_broadcast_segment_to_base(...)[base_rows]``, fill semantics
    included (``NaN`` for float dtypes on a row no record owns, zero otherwise),
    without the length-``n_base`` intermediate — one per segment-level variable,
    which on GEDI's six per-shot companions is ~7 GB against a 2 GB worker.
    """
    beg = np.asarray(index_beg_arr).astype(np.int64) - index_base
    cnt = np.asarray(count_arr).astype(np.int64)
    nonempty = cnt > 0
    bad = nonempty & (beg < 0)
    if bad.any():
        p = int(np.argmax(bad))
        raise ValueError(
            f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
        )
    over = nonempty & (beg + cnt > total_base_size)
    if over.any():
        p = int(np.argmax(over))
        raise ValueError(
            f"segment {p} range [{beg[p]}:{beg[p] + cnt[p]}] exceeds base size "
            f"{total_base_size}; the segment-level variable's link does not tile "
            f"the read's base extent"
        )
    parent = _link_parent_at_rows(index_beg_arr, count_arr, index_base, base_rows)
    valid = parent >= 0
    return _gather_segment_at_parents(seg_values, np.where(valid, parent, 0), valid)


def _gather_segment_at_parents(
    seg_values: np.ndarray, parent_safe: np.ndarray, valid: np.ndarray
) -> np.ndarray:
    """The gather half of :func:`_gather_segment_at_rows`, owner map supplied.

    Split out so a caller that is ALREADY holding the per-row owner map does not
    derive a second one (issue #452 review). On the vlen route the coordinates
    level's gather map is exactly that map, and the shipped GEDI template hangs
    six companion variables off that same level: without this the route ran six
    searchsorted derivations of a map it had in hand, by a different rule (last
    start wins) than the one that produced it (paint order) — two owner maps
    that have to agree, where one will do.

    ``parent_safe`` is the owner index per row with unowned rows clamped to 0
    and flagged in ``valid``; unowned rows take the same fill
    :func:`_broadcast_segment_to_base` gives an untiled row (``NaN`` for float
    dtypes, zero otherwise).
    """
    seg_values = np.asarray(seg_values)
    out = seg_values[parent_safe]
    if not valid.all():
        out[~valid] = np.nan if np.issubdtype(seg_values.dtype, np.floating) else 0
    return out


def _segment_level_variables(data_source: dict) -> dict[str, dict[str, str | dict]]:
    """Collect declared segment-level (non-base) readable variables (issue #30).

    A non-base level may declare ``variables`` as a ``{name: path-template}``
    mapping (the readable form, distinct from the documentation-only ``list[str]``
    form). Each such variable is read at coarse rate and broadcast to the base
    (photon) rows via the level's ``link`` (``_broadcast_segment_to_base``), so a
    per-segment field like ``dem_h`` becomes a per-photon column the aggregation /
    ``chunk_precompute`` can reduce. Returns ``{level_key: {name: template}}`` for
    every non-base level carrying a dict ``variables``; empty when none do, so the
    read path is unchanged for configs without it.

    Asset-sourced entries (the ``{asset, path}`` mapping form, issue #464) are
    excluded: they read the PAIRED granule via the sibling join, never this
    handle — the vlen route collects them separately
    (:func:`zagg.processing.read_vlen.asset_variable_specs`). A ``{path,
    column}`` entry (issue #678: a record-level table such as GLAH01's
    ``i_wt_fact_filt [n, 6]``) is kept as its mapping, and the broadcast
    applies the column selector after the coarse read.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or base_level is None:
        return {}
    out: dict[str, dict[str, str | dict]] = {}
    for name, lvl in levels.items():
        if name == base_level or not isinstance(lvl, dict):
            continue
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict) and lvl_vars:
            plain = {
                k: v for k, v in lvl_vars.items() if not isinstance(v, dict) or "asset" not in v
            }
            if plain:
                out[name] = plain
    return out


def _read_segment_broadcasts(
    h5obj,
    group: str,
    data_source: dict,
    levels: dict,
    n_base: int,
    read_fn=None,
    base_rows: np.ndarray | None = None,
    parents_by_level: dict[str, tuple[np.ndarray, np.ndarray]] | None = None,
) -> dict[str, np.ndarray]:
    """Read each segment-level variable and broadcast it to a base-rate column (issue #30).

    For every non-base level carrying a ``{name: path}`` ``variables`` mapping, read
    the variable and the level's link arrays at coarse rate, then broadcast to the
    base (photon) rows via :func:`_broadcast_segment_to_base`. Returns
    ``{name: base_rate_array}`` (length ``n_base``), ready to be sliced through the
    same spatial / keep masks the base-rate variables are. A variable name colliding
    with a ``data_source.variables`` column is rejected (it would shadow the read).

    ``base_rows`` (issue #452) hands back the same values at the read's rows only
    — length ``len(base_rows)``, gathered via :func:`_gather_segment_at_rows`
    instead of built at length ``n_base`` and sliced. The vlen route passes its
    plan's rows: ``n_base`` is sample rate there, so the base-rate form costs
    ~1.2 GB per companion variable on a GEDI beam group.

    ``parents_by_level`` maps a level key to a ``(parent_safe, valid)`` owner map
    the caller already holds, for levels whose link the caller has already
    resolved per row. Those levels skip both the link read and the owner
    derivation and gather straight off the supplied map — one derivation, one
    truth (issue #452 review). Levels absent from it are resolved as before.
    """
    seg_vars = _segment_level_variables(data_source)
    if not seg_vars:
        return {}
    base_cols = set(data_source.get("variables", {}))
    out: dict[str, np.ndarray] = {}
    for level_key, mapping in seg_vars.items():
        held = (parents_by_level or {}).get(level_key)
        if held is None:
            lvl = levels[level_key]
            link = lvl["link"]
            index_base = int(link.get("index_base", 0))
            ibeg_path = link["index_beg"].format(group=group)
            cnt_path = link["count"].format(group=group)
            if read_fn is None:
                link_data = h5obj.readDatasets([ibeg_path, cnt_path])
                ibeg_arr = link_data[ibeg_path]
                cnt_arr = link_data[cnt_path]
            else:
                ibeg_arr = read_fn(ibeg_path)
                cnt_arr = read_fn(cnt_path)
            # First assembly of this level's link on this route: refuse
            # overlapping records once here rather than let the two owner
            # derivations disagree per row further down (issue #452). A level
            # the caller already resolved was validated where it was assembled.
            _validate_link_disjoint(ibeg_arr, cnt_arr, index_base, level=level_key)
        for col_name, tmpl in mapping.items():
            if col_name in base_cols:
                raise ValueError(
                    f"segment-level variable '{col_name}' on level '{level_key}' "
                    f"collides with a data_source.variables column"
                )
            column = tmpl.get("column") if isinstance(tmpl, dict) else None
            seg_path = (tmpl["path"] if isinstance(tmpl, dict) else tmpl).format(group=group)
            if read_fn is None:
                seg_values = np.asarray(h5obj.readDatasets([seg_path])[seg_path])
            else:
                seg_values = np.asarray(read_fn(seg_path))
            seg_values = _select_column(seg_values, column, col_name)
            if held is not None:
                out[col_name] = _gather_segment_at_parents(seg_values, held[0], held[1])
            elif base_rows is None:
                out[col_name] = _broadcast_segment_to_base(
                    seg_values, ibeg_arr, cnt_arr, index_base, n_base
                )
            else:
                out[col_name] = _gather_segment_at_rows(
                    seg_values, ibeg_arr, cnt_arr, index_base, n_base, base_rows
                )
    return out


def _variable_specs(variables: dict) -> dict[str, tuple[str, int | None]]:
    """Normalize ``data_source.variables`` entries to ``(path_template, column)``.

    A plain-string entry reads the whole dataset. The mapping form
    ``{path: ..., column: k}`` selects one column of a 2-D dataset (issue
    #321) — the same semantics as ``column`` on structured filters — so
    several scalar columns can be pulled from one multi-column dataset (e.g.
    the five ``signal_conf_ph`` surfaces) while the shared path is still read
    once via the existing path dedup.
    """
    specs: dict[str, tuple[str, int | None]] = {}
    for col, entry in variables.items():
        if isinstance(entry, str):
            specs[col] = (entry, None)
        else:
            # An ``axes`` entry (issue #678) carries no column: the fixed-width
            # route flattens it itself, and a path-only consumer (the inline
            # backend's chunk-map prebuild) wants just the path.
            column = entry.get("column")
            specs[col] = (entry["path"], None if column is None else int(column))
    return specs


def _select_column(values: np.ndarray, column: int | None, col_name: str) -> np.ndarray:
    """Apply a variable's ``column`` selector; shape-check both directions."""
    if column is not None:
        if values.ndim < 2:
            raise ValueError(f"variable '{col_name}': 'column' set but array is 1-D")
        return values[:, column]
    if values.ndim > 1:
        raise ValueError(f"variable '{col_name}': N-D dataset requires an integer 'column'")
    return values


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


def _record_obs_read(io_stats: dict | None, n: int) -> None:
    """Accumulate base-rate rows DECODED (pre-filter) for the read counter.

    The point-path counterpart of the raster ``io_stats`` counters (issue #374
    / issue #297): rows fetched, before the shard mask, structured/expression
    filters, and segment padding are applied — so ``n_obs_read / n_obs`` is the
    read-vs-keep ratio, derived at read time and never stored.

    PRESENCE of the key is the "measured" signal, not its value: a read seam
    that never calls this (a stubbed ``_read_group``, or a worker predating the
    field) leaves it absent, which the worker reports as ``None`` — unmeasured,
    per the #297 nullable-column convention — rather than a real zero.
    """
    if io_stats is not None:
        io_stats["obs_read"] = io_stats.get("obs_read", 0) + int(n)


def _planned_read_group(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    arrow: bool = False,
    read_fn=None,
    io_stats: dict | None = None,
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

    Returns the same ``pandas.DataFrame`` / ``arro3.core.Table`` / ``None`` contract
    as :func:`_read_group`. Output rows are in plan-slice / spatial-mask /
    filter order — which matches the full-read path's row ordering because the
    plan's runs are emitted in increasing parent index.

    ``read_fn`` is the addressing seam (issue #160): an index backend may
    supply its own reader (e.g. ``inline``'s boundary-safe chunk-map reads).
    It serves the coarse selection reads here (issue #179 — compiled + pooled)
    and is forwarded to :func:`_execute_plan_group` for the base-rate data
    reads; ``None`` (default) uses the plain h5coro bridge — the hierarchical
    baseline. Selection semantics (the plan) and everything downstream of the
    returned columns are identical regardless.

    ``io_stats`` (issue #374) is the optional read-counter sink, forwarded to
    whichever arm actually decodes base-rate rows — the full-read fallback or
    :func:`_execute_plan_group` — so a group is counted exactly once. The
    pre-decode short-circuits stamp a measured zero themselves, so this route
    never reports "unmeasured" for a group it did look at.
    """
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

    # Read coarse-level coordinates + link arrays (small — geolocation rate is
    # ~30x lighter than photon rate on ATL03). With a backend-supplied
    # ``read_fn`` these selection reads take the same compiled, pooled route as
    # the data reads (issue #179): four independent full-dataset reads per
    # group is exactly the fan-out the ``read_workers`` pool wants, and until
    # now they were serial uncompiled h5coro on every backend — roughly half
    # the read wall on dense shards. ``read_fn`` degrades per dataset inside
    # the backend (inline) or falls the group back via ``on_miss`` (sidecar),
    # so behavior on misses matches the data reads. ``None`` keeps the batched
    # h5coro read byte-identical (the pinned hierarchical baseline).
    si_lat_path, si_lon_path = _level_coord_paths(si_lvl, group)
    ibeg_path = link["index_beg"].format(group=group)
    cnt_path = link["count"].format(group=group)
    selection_paths = [si_lat_path, si_lon_path, ibeg_path, cnt_path]
    if read_fn is None:
        coarse_data = h5obj.readDatasets(selection_paths)
    else:
        coarse_data = _read_paths_pooled(
            [(p, None) for p in dict.fromkeys(selection_paths)],
            lambda p, dt: read_fn(p),
            _read_workers(data_source),
        )
    coarse_lats = coarse_data[si_lat_path]
    coarse_lons = coarse_data[si_lon_path]
    ibeg_arr = coarse_data[ibeg_path]
    cnt_arr = coarse_data[cnt_path]

    # The three short-circuits below return before any base-rate decode, so
    # each stamps a measured zero (issue #374): without it the planned route --
    # the production route for ATL03, where a granule assigned for one beam
    # short-circuits on the other five -- reports ``n_obs_read = None``,
    # indistinguishable from an uninstrumented read seam. That discrimination
    # is the whole point of the nullable column (review finding).
    if len(coarse_lats) == 0:
        _record_obs_read(io_stats, 0)
        return None

    # ``n_base`` is the extent the coarse link tiles: the last base row any
    # non-empty parent reaches (:func:`link_base_extent`, issue #452). On this
    # route's contiguous products (#43's assumption: ranges neither overlap nor
    # leave holes) that is exactly ``Σcount``, so the plan below is unchanged;
    # deriving it from the link instead makes the route correct for a strided
    # product too, where ``Σcount`` understates the array and clamps every run
    # to an empty slice.
    n_base = link_base_extent(ibeg_arr, cnt_arr, index_base)
    if n_base <= 0:
        _record_obs_read(io_stats, 0)
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
        _record_obs_read(io_stats, 0)
        return None  # empty AOI -- no parent intersects, skip the group entirely

    if plan.full_read:
        # Selectivity above threshold: many small reads would still sum to most
        # of the file. Defer to the full-coord-read path; semantics identical.
        # ``read_fn`` rides along (issue #179): the fallback keeps the compiled
        # + pooled addressing instead of dropping to serial h5coro.
        return _read_group_full(
            h5obj,
            group,
            data_source,
            shard_key,
            grid,
            arrow=arrow,
            read_fn=read_fn,
            io_stats=io_stats,
        )

    return _execute_plan_group(
        h5obj,
        group,
        data_source,
        shard_key,
        grid,
        plan,
        n_base,
        arrow,
        read_fn=read_fn,
        io_stats=io_stats,
    )


def _execute_plan_group(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    plan,
    n_base,
    arrow=False,
    read_fn=None,
    io_stats: dict | None = None,
):
    """Execute a computed :class:`~zagg.read_plan.ReadPlan` over one group.

    The shared back half of the planned read: base coords + variables + filter
    datasets are read only over ``plan``'s slices, then the exact photon-level
    shard mask, structured/cross-level/expression filters, and segment-level
    broadcasts are applied — identical semantics regardless of how the plan was
    computed (:func:`_planned_read_group`'s geolocation-rate mortie mask, or the
    a-priori chunk-boundary plan of issue #148 arm 2a). ``read_fn`` overrides
    the h5coro hyperslice callback (the a-priori path substitutes one that works
    around h5coro's chunk-aligned-start B-tree bug) and also serves the
    cross-level coarse-filter reads and segment-level broadcasts (issue #179 —
    the whole planned read is one addressing seam); ``None`` uses the plain
    ``readDatasets`` bridge throughout.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    levels = data_source.get("levels") or {}
    base_level_key = data_source.get("base_level")

    _read_fn = read_fn
    if _read_fn is None:
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

    # Read fan-out (issue #170 phase 4): only when an index backend supplied
    # a compiled read_fn -- the hierarchical h5coro path (read_fn None) keeps
    # its serial reads (see _read_workers).
    workers = _read_workers(data_source) if read_fn is not None else 1

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    coord_arrays = _read_paths_pooled(
        [(lat_path, np.float64), (lon_path, np.float64)],
        lambda p, dt: execute_read_plan(plan, _read_fn, p, dt),
        workers,
    )
    lats, lons = coord_arrays[lat_path], coord_arrays[lon_path]

    # Rows DECODED by this group's plan (issue #374): the padded, boundary-
    # straddling reads counted BEFORE the shard mask and filters below, so the
    # segment→photon indexed-IO efficiency (issue #43) is derivable from the
    # run parquet. Recorded ahead of the empty short-circuit so an instrumented
    # route that decodes nothing still reads as measured-zero, not unmeasured
    # -- as do the pre-decode short-circuits in ``_planned_read_group`` and
    # ``_apriori_read_group``, which never reach this line.
    _record_obs_read(io_stats, len(lats))

    if len(lats) == 0:
        return None

    # Apply spatial / shard mask over the concatenated planned reads.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == shard_key
    if np.sum(mask_spatial) == 0:
        return None

    # Read the variables and base-level filter datasets via the same plan. Read
    # each distinct path once (the variable and filter dataset paths can coincide).
    var_specs = _variable_specs(variables)
    var_paths = {col: tmpl.format(group=group) for col, (tmpl, _) in var_specs.items()}
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    # dtype hint isn't load-bearing -- execute_read_plan dtype-casts via
    # np.asarray, which is a no-op when the source dtype already matches.
    # dict.fromkeys: read each distinct path once, in first-seen order.
    arrays_by_path: dict[str, np.ndarray] = _read_paths_pooled(
        [(p, None) for p in dict.fromkeys(list(var_paths.values()) + list(filter_paths.values()))],
        lambda p, dt: execute_read_plan(plan, _read_fn, p, dt),
        workers,
    )

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
            # Coarse flag + link arrays through the same compiled, pooled
            # seam as the selection reads (issue #179); ``read_fn is None``
            # keeps the batched h5coro read byte-identical.
            cf_paths = [cf_flag_path, cf_ibeg_path, cf_cnt_path]
            if read_fn is None:
                cf_data = h5obj.readDatasets(cf_paths)
            else:
                cf_data = _read_paths_pooled(
                    [(p, None) for p in dict.fromkeys(cf_paths)],
                    lambda p, dt: read_fn(p),
                    workers,
                )
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
    # base-rate variables before the spatial / keep masks below. ``read_fn``
    # rides along (issue #179) so the coarse variable + link reads keep the
    # compiled addressing; ``None`` keeps the serial h5coro path.
    seg_broadcasts = _read_segment_broadcasts(
        h5obj, group, data_source, levels, n_base, read_fn=read_fn
    )
    if seg_broadcasts:
        seg_global_idx = np.concatenate(
            [np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices]
        )

    # Build the data dict (variables sliced to mask_spatial, then to keep_mask).
    leaf_after_spatial = leaf_ids[mask_spatial]
    data_dict: dict[str, np.ndarray] = {}
    for col_name, path in var_paths.items():
        values = _select_column(arrays_by_path[path], var_specs[col_name][1], col_name)
        values = values[mask_spatial]
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
        from arro3.core import Table

        # arro3-core carrier (issue #130): no pyarrow on the worker. ``from_pydict``
        # accepts the raw numpy column arrays directly (zero-copy for the contiguous
        # dense reads here), matching the pandas carrier's columns.
        return Table.from_pydict(data_dict)
    return pd.DataFrame(data_dict)


def _read_group(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    arrow: bool = False,
    granule_url: str | None = None,
    io_stats: dict | None = None,
    siblings: dict | None = None,
):
    """Read and spatially filter one HDF5 group.

    Returns a ``pandas.DataFrame`` (default) or, when ``arrow=True``, an
    ``arro3.core.Table`` carrying the identical columns. Returns ``None`` when the
    group has no observations in this shard.

    When ``data_source.coordinates.level`` names a record level (issue #425),
    the group is a vlen-packed product (GEDI-style): coordinates expand from
    the record level and the flat base datasets are gathered by the record
    link — see :mod:`zagg.processing.read_vlen`. ``siblings`` (open h5coro
    handles per sibling-asset name, from a paired-asset shard map) is consumed
    only by that route's asset filters and ignored elsewhere.

    When ``data_source.read_plan.chunk_boundaries`` is set (issue #148 arm 2a),
    the read is planned a priori from the granule's chunk-boundary parquet —
    no geolocation-rate coordinate read — via
    :func:`zagg.processing.apriori._apriori_read_group`; ``granule_url``
    (passed by the worker only when the feature is on) locates the parquet.
    Takes precedence over ``spatial_index`` so the benchmark configs can keep
    both keys and select the arm with ``chunk_boundaries`` alone.

    Otherwise supports three modes (issues #43 Phase A/B/C):

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

    ``io_stats`` (issue #374) is an optional mutable dict the read routes
    accumulate ``obs_read`` (rows decoded, pre-filter) into. The worker owns
    one per granule, so it is written only by that granule's thread and read
    by the dispatcher after the read completes — no lock (the point-path
    analogue of the raster ``io_stats`` sink). ``None`` counts nothing.
    """
    # Vlen-packed sources (issue #425) dispatch on the coordinates.level key
    # before the plan-shape branches below — the vlen route owns its own
    # planned/full arms (read_plan.spatial_index must name the record level).
    coords = data_source.get("coordinates")
    # A fixed-width record level (issue #678) routes to its own module when the
    # coordinates level links with ``width``; a width link without
    # ``coordinates.level`` is the swath form, whose read path is a follow-up.
    from zagg.processing.read_fixed import fixed_width_level

    width_level = fixed_width_level(data_source)
    if isinstance(coords, dict) and coords.get("level") is not None:
        if width_level is not None and width_level[0] == coords["level"]:
            from zagg.processing.read_fixed import _fixed_read_group

            return _fixed_read_group(
                h5obj,
                group,
                data_source,
                shard_key,
                grid,
                arrow=arrow,
                io_stats=io_stats,
                siblings=siblings,
            )
        from zagg.processing.read_vlen import _vlen_read_group

        return _vlen_read_group(
            h5obj,
            group,
            data_source,
            shard_key,
            grid,
            arrow=arrow,
            io_stats=io_stats,
            siblings=siblings,
        )
    if width_level is not None:
        raise ValueError(
            f"levels.{width_level[0]}.link.width without coordinates.level is the swath "
            "form, whose read path is not implemented yet (issue #678 follow-up)"
        )
    rp = data_source.get("read_plan")
    # Presence check, not truthiness: an empty/misconfigured ``chunk_boundaries``
    # block must fail loudly inside the a-priori path (missing ``prefix``), not
    # silently run another arm and corrupt the benchmark comparison.
    if isinstance(rp, dict) and "chunk_boundaries" in rp:
        from zagg.processing.apriori import _apriori_read_group

        return _apriori_read_group(
            h5obj,
            group,
            data_source,
            shard_key,
            grid,
            arrow=arrow,
            granule_url=granule_url,
            io_stats=io_stats,
        )
    # Truthy-checking ``levels``/``base_level`` would route an empty ``{}`` (a
    # config typo, easy to do) back to the full-read path silently. Reject
    # incomplete configurations explicitly instead -- the planned path is
    # gated only when ``spatial_index`` is set, and *then* requires a real
    # multi-level structure to operate on.
    if isinstance(rp, dict) and rp.get("spatial_index"):
        _validate_planned_config(data_source)
        return _planned_read_group(
            h5obj, group, data_source, shard_key, grid, arrow=arrow, io_stats=io_stats
        )
    return _read_group_full(
        h5obj, group, data_source, shard_key, grid, arrow=arrow, io_stats=io_stats
    )


def _validate_planned_config(data_source: dict) -> None:
    """Completeness gate for the planned (spatial-index) route.

    Shared by :func:`_read_group`'s dispatch and any index backend that
    plugs into :func:`_planned_read_group` directly (issue #160 —
    ``inline``), so the accepted-config surface cannot drift between them.
    """
    levels = data_source.get("levels")
    if not isinstance(levels, dict) or not levels:
        raise ValueError(
            "data_source.read_plan.spatial_index requires a non-empty 'levels' mapping"
        )
    if not data_source.get("base_level"):
        raise ValueError("data_source.read_plan.spatial_index requires 'base_level'")


def _read_workers(data_source: dict) -> int:
    """``data_source.read_workers``: per-worker read concurrency (issue #170).

    Applies only to reads routed through an index backend's compiled
    ``read_fn`` (each is one blocking ranged fetch + a GIL-released decode,
    so threads overlap S3 latency and use both Lambda vCPUs); the
    hierarchical h5coro path keeps its serial batched reads regardless — it
    is the pinned uncached benchmark baseline. Default 8. Peak RSS grows
    with width (each in-flight read holds its compressed buffers + decoded
    output), so dense-shard configs can dial it down; ``1`` is serial.
    """
    w = data_source.get("read_workers", 8)
    if isinstance(w, bool) or not isinstance(w, int) or w < 1:
        raise ValueError(f"data_source.read_workers must be an integer >= 1 (got {w!r})")
    return w


def _read_paths_pooled(entries, read_one, workers: int) -> dict:
    """Run ``read_one(path, dtype)`` per (path, dtype) entry, fanned across a
    bounded thread pool when ``workers > 1`` (issue #170 phase 4). Results
    are keyed by path, so completion order cannot affect output; a failed
    read re-raises at collection exactly as the serial loop would. Serial
    (entry order) when ``workers <= 1`` or there is a single entry.
    """
    if workers <= 1 or len(entries) <= 1:
        return {p: read_one(p, dt) for p, dt in entries}
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=min(workers, len(entries))) as pool:
        futures = {p: pool.submit(read_one, p, dt) for p, dt in entries}
        return {p: f.result() for p, f in futures.items()}


def _read_group_full(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    arrow: bool = False,
    read_fn=None,
    io_stats: dict | None = None,
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

    ``read_fn`` (issue #170 phase 2) is the same addressing seam
    :func:`_planned_read_group` takes — ``(path, hyperslice|None) -> array``
    — so index backends (``inline``) can route this path's decode through the
    compiled reader too, giving read-plan-less (flat) data sources the fast
    decode. ``None`` keeps the batched h5coro reads byte-identical.

    ``io_stats`` (issue #374) is the optional read-counter sink; this path
    decodes the base-rate coordinates in FULL, so what it records is the whole
    group's row count regardless of how few rows survive the shard mask.
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

    # Read fan-out (issue #170 phase 4): only when an index backend supplied
    # a compiled read_fn -- see _read_workers.
    workers = _read_workers(data_source) if read_fn is not None else 1

    # Resolve coordinate paths
    coord_paths = [path.format(group=group) for path in coordinates.values()]
    if read_fn is None:
        coord_data = h5obj.readDatasets(coord_paths)
    else:
        coord_data = _read_paths_pooled(
            [(p, None) for p in dict.fromkeys(coord_paths)],
            lambda p, dt: read_fn(p),
            workers,
        )

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = coord_data[lat_path]
    lons = coord_data[lon_path]

    # Rows DECODED by this group (issue #374) — the full base-rate coordinate
    # read, counted before the shard mask and filters below.
    _record_obs_read(io_stats, len(lats))

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
            # Read the coarse flag array in full ("hyperslice": [] is h5coro's
            # full-read form; the key is required on dict entries — issue #157).
            # We need all parents to align with the full-length link arrays.
            if read_fn is None:
                coarse_data = h5obj.readDatasets([{"dataset": flag_path, "hyperslice": []}])
                coarse_arr = coarse_data[flag_path]
            else:
                coarse_arr = read_fn(flag_path)
            coarse_fmask = _predicate_mask(coarse_arr, f)
            # Read the link arrays from this level.
            link = lvl["link"]
            index_base = int(link.get("index_base", 0))
            ibeg_path = link["index_beg"].format(group=group)
            cnt_path = link["count"].format(group=group)
            if read_fn is None:
                link_data = h5obj.readDatasets(
                    [
                        {"dataset": ibeg_path, "hyperslice": []},
                        {"dataset": cnt_path, "hyperslice": []},
                    ]
                )
                ibeg_arr = link_data[ibeg_path]
                cnt_arr = link_data[cnt_path]
            else:
                ibeg_arr = read_fn(ibeg_path)
                cnt_arr = read_fn(cnt_path)
            expanded = _expand_mask_to_base(coarse_fmask, ibeg_arr, cnt_arr, index_base, len(lats))
            cross_mask = expanded if cross_mask is None else (cross_mask & expanded)
        if cross_mask is not None and np.sum(cross_mask[min_idx:max_idx]) == 0:
            return None

    # Build hyperslice dataset list: variables + any base-level structured-filter arrays.
    # Read each distinct path once; flag datasets may coincide with a variable.
    datasets = []
    paths_seen = set()
    var_specs = _variable_specs(variables)
    var_paths = {col: tmpl.format(group=group) for col, (tmpl, _) in var_specs.items()}
    for path in var_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    for path in filter_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)

    if read_fn is None:
        data = h5obj.readDatasets(datasets)
    else:
        hyperslices = {d["dataset"]: d["hyperslice"] for d in datasets}
        data = _read_paths_pooled(
            [(p, None) for p in hyperslices],
            lambda p, dt: read_fn(p, hyperslices[p]),
            workers,
        )

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
    seg_broadcasts = _read_segment_broadcasts(
        h5obj, group, data_source, levels or {}, len(lats), read_fn=read_fn
    )

    # Build dataframe (variables sliced to spatial mask, then to the keep-mask)
    leaf_sliced = leaf_ids[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path in var_paths.items():
        values = _select_column(data[path], var_specs[col_name][1], col_name)
        values = values[mask_sliced]
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
        from arro3.core import Table

        # arro3-core carrier (issue #130): no pyarrow on the worker. ``from_pydict``
        # accepts the raw numpy column arrays directly (zero-copy for the contiguous
        # dense reads here), matching the pandas carrier's columns.
        return Table.from_pydict(data_dict)
    return pd.DataFrame(data_dict)
