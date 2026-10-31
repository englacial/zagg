"""Generic vlen (variable-length record) reader primitives (issue #425).

Three sensor-agnostic declarative primitives extend the reader grammar so
vlen-packed products (GEDI L1B ``rxwaveform``, LVIS, any future waveform
sensor) are pure config — no sensor module (ruling on issue #422):

- **ragged gather** — the base level is a flat sample array tiled by a coarse
  (record) level's origin-aware ``link`` (``index_beg``/``count``/
  ``index_base``). GEDI's ``rx_sample_start_index`` is origin-1, the exact
  ``index_base: 1`` precedent of ATL03 ``ph_index_beg``. Planned reads slice
  the flat array by the record link, so only the shard's records are decoded.
- **expand** — per-record scalars repeat by sample count to base rate.
  Coordinates declare it with ``data_source.coordinates.level: <record
  level>`` (the base level carries no native coordinates); per-record
  variables reuse the existing segment-level ``variables`` broadcast
  (:func:`zagg.processing.read._read_segment_broadcasts`).
- **coordinate synthesis** — a per-sample column computed as
  ``linspace(start, stop, count)`` within each record: a
  ``data_source.variables`` entry of the mapping form
  ``{synthesize: linspace, level: <record level>, start: <path>, stop:
  <path>}`` (GEDI per-sample elevation from the per-shot
  ``elevation_bin0``/``elevation_lastbin`` endpoints).

After expansion each sample is an ordinary point observation; everything
downstream of the returned columns (filters, aggregation, write) is untouched.
Split out of ``read.py`` per the §4 module cap (the route would push it past
1,200 lines); the shared helpers are imported from there.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from zagg.config import evaluate_filter_expression, filters_from_data_source
from zagg.processing.read import (
    _expand_mask_at_rows,
    _expand_mask_to_base,
    _gather_segment_at_parents,
    _predicate_mask,
    _read_paths_pooled,
    _read_segment_broadcasts,
    _read_workers,
    _record_obs_read,
    _segment_level_variables,
    _select_column,
    _validate_link_disjoint,
    _variable_specs,
    link_base_extent,
)
from zagg.read_plan import execute_read_plan, plan_read

logger = logging.getLogger(__name__)


def vlen_coordinates_level(data_source: dict) -> str | None:
    """The record level named by ``coordinates.level``, or ``None`` when flat.

    Presence of the key is the vlen-route dispatch predicate
    (:func:`zagg.processing.read._read_group`): the base level has no native
    coordinate datasets, so lat/lon expand from this coarse level via its
    ``link`` counts.
    """
    coords = data_source.get("coordinates")
    if not isinstance(coords, dict):
        return None
    return coords.get("level")


def synthesized_specs(variables: dict) -> dict[str, dict]:
    """The ``synthesize:``-form entries of ``data_source.variables``.

    Returns ``{name: entry}`` for every mapping entry carrying ``synthesize``;
    the plain path / ``{path, column}`` entries are read by the ordinary
    variable machinery (:func:`zagg.processing.read._variable_specs`).
    """
    return {
        name: entry
        for name, entry in variables.items()
        if isinstance(entry, dict) and "synthesize" in entry
    }


def path_form_variables(variables: dict) -> dict:
    """``data_source.variables`` minus the ``synthesize:`` entries.

    The subset the ordinary path machinery understands: a synthesized entry
    carries no ``path``, so :func:`zagg.processing.read._variable_specs`
    raises ``KeyError`` on one. Every consumer of that normalizer on a
    possibly-vlen source (the read below, the inline backend's write-back
    prebuild) filters through here first.
    """
    synth = synthesized_specs(variables)
    return {name: entry for name, entry in variables.items() if name not in synth}


def asset_variable_specs(data_source: dict) -> dict[str, dict]:
    """Asset-sourced record-level variables: ``{name: {asset, path}}`` (issue #464).

    A ``levels.<coordinates.level>.variables`` entry may be an ``{asset, path}``
    mapping: the dataset is read from the named sibling asset's handle, joined
    per record on the declared ``assets.<name>.join`` key, and broadcast to
    base rate exactly like a plain record-level companion — so it is an
    ordinary column downstream (expression filters included). Validation
    (:func:`zagg.config._validate_vlen_source`) pins these to the coordinates
    level: the join is keyed to that level's records.
    """
    coord_level = vlen_coordinates_level(data_source)
    if coord_level is None:
        return {}
    lvl_vars = ((data_source.get("levels") or {}).get(coord_level) or {}).get("variables")
    if not isinstance(lvl_vars, dict):
        return {}
    return {
        name: entry
        for name, entry in lvl_vars.items()
        if isinstance(entry, dict) and "asset" in entry
    }


def expand_link_indices(
    ibeg_arr: np.ndarray, cnt_arr: np.ndarray, index_base: int, n_base: int
) -> tuple[np.ndarray, np.ndarray]:
    """Per-base-row ``(record, within-record)`` index arrays from a link.

    The gather map of the vlen grammar: ``parent_idx[j]`` is the record owning
    base row ``j`` (``-1`` for a row no record tiles — a gap surfaces as an
    unassignable row, never as data borrowed from a neighbor), and
    ``within_idx[j]`` is the row's 0-based offset inside its record (what the
    linspace synthesis interpolates over). Placement is by ``index_beg`` under
    the same origin (``index_base``) and empty-record discipline as
    :func:`zagg.processing.read._broadcast_segment_to_base`.

    ``n_base`` is the caller's: :func:`_vlen_read_group` derives it from the
    link itself (:func:`zagg.processing.read.link_base_extent`, issue #452). On
    a strided product the ``-1`` rows are ROUTINE — the slack between a
    record's valid samples and the next record's window start (GEDI L1B: ~50%
    of the rows a plan decodes) — and are dropped downstream by the
    ``valid & mask_spatial`` test, never assigned a neighbor's record. That
    handling is load-bearing, not defensive: GEDI's slack is **zero-filled in
    the product** (``rx_sample_start_index`` steps by exactly 1,420 with
    ``rx_sample_count`` ~701, and ``rxwaveform[701:1420]`` of each window reads
    as literal zeros), so a gap row that leaked through would aggregate as a
    real zero-amplitude sample. On a contiguous product these rows do not
    occur. A record reaching past ``n_base`` still raises rather than wrapping.
    """
    parent_idx = np.full(n_base, -1, dtype=np.int64)
    within_idx = np.zeros(n_base, dtype=np.int64)
    for p in range(len(cnt_arr)):
        cnt = int(cnt_arr[p])
        # Empty records cover no samples; under index_base=1 their sentinel
        # start (0) would read as beg=-1 and raise below, so skip them (the
        # issue #116 convention the segment broadcast follows).
        if cnt == 0:
            continue
        beg = int(ibeg_arr[p]) - index_base
        if beg < 0:
            raise ValueError(
                f"index_beg_arr[{p}]={ibeg_arr[p]} is less than index_base={index_base}"
            )
        if beg + cnt > n_base:
            raise ValueError(
                f"record {p} range [{beg}:{beg + cnt}] exceeds base size {n_base}; "
                f"the record link does not tile the declared base extent"
            )
        parent_idx[beg : beg + cnt] = p
        within_idx[beg : beg + cnt] = np.arange(cnt)
    return parent_idx, within_idx


def _planned_gather_map(
    ibeg_arr: np.ndarray,
    cnt_arr: np.ndarray,
    index_base: int,
    n_base: int,
    base_slices: list[tuple[int, int]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """:func:`expand_link_indices` restricted to a plan's rows.

    Same placement, validation and gap semantics, but the arrays are sized to
    the planned rows (``sum(e - s)``) instead of the whole base extent — the
    planned arm exists so a shard touching a handful of records never
    materializes anything at sample rate (the issue #43 OOM posture), and at
    GEDI's ~10^8 samples per beam group a full-rate ``int64`` pair is ~1.6 GB
    against a 2 GB worker. Validation still runs over every record, so a
    granule whose link does not tile the base extent raises here exactly as it
    does on the full read.

    Returns ``(global_idx, parent_idx, within_idx)``: the planned base rows and
    their per-row record / within-record indices.
    """
    beg_all = np.asarray(ibeg_arr).astype(np.int64) - index_base
    cnt_all = np.asarray(cnt_arr).astype(np.int64)
    # Empty records cover no samples; their origin-1 sentinel start would read
    # as beg=-1, so they are skipped rather than validated (the issue #116
    # convention expand_link_indices follows).
    nonempty = cnt_all > 0
    bad = nonempty & (beg_all < 0)
    if bad.any():
        p = int(np.argmax(bad))
        raise ValueError(f"index_beg_arr[{p}]={ibeg_arr[p]} is less than index_base={index_base}")
    over = nonempty & (beg_all + cnt_all > n_base)
    if over.any():
        p = int(np.argmax(over))
        raise ValueError(
            f"record {p} range [{beg_all[p]}:{beg_all[p] + cnt_all[p]}] exceeds base size "
            f"{n_base}; the record link does not tile the declared base extent"
        )
    if not base_slices:
        empty = np.empty(0, dtype=np.int64)
        return empty, empty.copy(), empty.copy()
    global_idx = np.concatenate([np.arange(s, e, dtype=np.int64) for s, e in base_slices])
    parent_idx = np.full(len(global_idx), -1, dtype=np.int64)
    within_idx = np.zeros(len(global_idx), dtype=np.int64)
    off = 0
    for s, e in base_slices:
        # Records overlapping this slice, walked in index order so a later
        # record shadows an earlier one exactly as the full-rate map does.
        for p in np.nonzero(nonempty & (beg_all < e) & (beg_all + cnt_all > s))[0]:
            beg = int(beg_all[p])
            lo = max(beg, s)
            hi = min(beg + int(cnt_all[p]), e)
            parent_idx[off + lo - s : off + hi - s] = p
            within_idx[off + lo - s : off + hi - s] = np.arange(lo - beg, hi - beg)
        off += e - s
    return global_idx, parent_idx, within_idx


def synthesize_linspace(
    start_vals: np.ndarray,
    stop_vals: np.ndarray,
    cnt_arr: np.ndarray,
    parent_idx: np.ndarray,
    within_idx: np.ndarray,
) -> np.ndarray:
    """Per-sample ``linspace(start, stop, count)`` values within each record.

    Row ``j`` of record ``p`` (count ``n_p``) gets
    ``start[p] + (stop[p] - start[p]) * j / (n_p - 1)`` — endpoints exact, a
    single-sample record gets ``start[p]``. Rows with no record
    (``parent_idx == -1``) get NaN; they carry no coordinates either and are
    dropped by the spatial mask.
    """
    parent_idx = np.asarray(parent_idx)
    valid = parent_idx >= 0
    out = np.full(len(parent_idx), np.nan, dtype=np.float64)
    p = parent_idx[valid]
    start = np.asarray(start_vals, dtype=np.float64)[p]
    stop = np.asarray(stop_vals, dtype=np.float64)[p]
    denom = np.maximum(np.asarray(cnt_arr, dtype=np.float64)[p] - 1.0, 1.0)
    out[valid] = start + (stop - start) * (np.asarray(within_idx)[valid] / denom)
    return out


def _validate_vlen_config(data_source: dict) -> tuple[str, dict]:
    """Completeness gate for the vlen route; returns ``(coord_level_key, level)``.

    The coordinates level must be a declared non-base level whose ``link``
    points directly at the base level — the same shape
    :func:`zagg.processing.read._planned_read_group` requires of
    ``read_plan.spatial_index``.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or not levels or not base_level:
        raise ValueError("coordinates.level requires 'levels' and 'base_level'")
    key = data_source["coordinates"]["level"]
    lvl = levels.get(key)
    if lvl is None:
        raise ValueError(f"coordinates.level {key!r} is not a key in levels")
    link = lvl.get("link")
    if not isinstance(link, dict) or link.get("to") != base_level:
        raise ValueError(
            f"coordinates.level {key!r} must link directly to base level {base_level!r}"
        )
    return key, lvl


def _vlen_read_group(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    arrow: bool = False,
    read_fn=None,
    io_stats: dict | None = None,
    siblings: dict | None = None,
):
    """Read and spatially filter one vlen-packed HDF5 group.

    Same ``pandas.DataFrame`` / ``arro3.core.Table`` / ``None`` contract as
    :func:`zagg.processing.read._read_group`. The record level's coordinates
    and link arrays are read fully (record rate is ~3 orders lighter than
    sample rate on GEDI); when ``read_plan.spatial_index`` names the
    coordinates level the flat base datasets are sliced to the shard's records
    via :func:`zagg.read_plan.plan_read` (the ragged gather), otherwise they
    are read in full. Coordinates never read at base rate — they expand from
    the record level — and synthesized columns are computed, not read.

    ``siblings`` maps sibling-asset names to open h5coro handles (issue #425,
    the paired-asset L2A join); ``None`` when the granule carries no sibling
    assets. ``read_fn`` is the index-backend addressing seam and ``io_stats``
    the read counter, both as in ``_read_group``.
    """
    coord_key, coord_lvl = _validate_vlen_config(data_source)
    levels = data_source["levels"]
    base_level_key = data_source["base_level"]
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    link = coord_lvl["link"]
    index_base = int(link.get("index_base", 0))

    workers = _read_workers(data_source) if read_fn is not None else 1

    def _read_full(paths: list[str]) -> dict:
        """Full-dataset reads through the active addressing seam."""
        if read_fn is None:
            return h5obj.readDatasets(list(dict.fromkeys(paths)))
        return _read_paths_pooled(
            [(p, None) for p in dict.fromkeys(paths)], lambda p, dt: read_fn(p), workers
        )

    # ---- Record-level coordinates + link arrays (small, full read).
    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    ibeg_path = link["index_beg"].format(group=group)
    cnt_path = link["count"].format(group=group)
    coarse = _read_full([lat_path, lon_path, ibeg_path, cnt_path])
    coarse_lats = np.asarray(coarse[lat_path])
    coarse_lons = np.asarray(coarse[lon_path])
    ibeg_arr = np.asarray(coarse[ibeg_path])
    cnt_arr = np.asarray(coarse[cnt_path])

    if len(coarse_lats) == 0:
        _record_obs_read(io_stats, 0)
        return None
    # The base extent the record link tiles (issue #452): the last base row any
    # non-empty record reaches. Contiguous products make that ``Σcount`` (#43's
    # assumption, ATL03's shape); STRIDED ones do not — GEDI L1B gives every
    # shot a fixed 1,420-sample window in ``rxwaveform`` and counts only the
    # valid samples, so ``Σcount`` understates the array by ~50%, ``plan_read``
    # clamps every run to ``base_end <= base_start``, and the group returns
    # zero rows with no error raised. Under striding the slack rows between
    # records are ROUTINE (143k of 310k planned rows on the reference shard):
    # they belong to no record, so the gather map marks them ``parent_idx ==
    # -1``, ``synthesize_linspace`` NaNs them, and ``valid & mask_spatial``
    # drops them before a single column is assembled. That drop is the
    # correctness guarantee, not a nicety: GEDI zero-fills the slack in the
    # product (the tail of each 1,420-sample window past ``rx_sample_count``
    # reads as literal zeros), so any row that leaked through would land in a
    # cell as a real zero-amplitude sample — ~46% of the fetched span.
    # First assembly of the coordinates link: the whole route hangs off it, and
    # both owner derivations (the paint maps below, the searchsorted twins in
    # read.py) are only equivalent on disjoint records — so refuse an
    # overlapping link here, once, rather than emit silently mismatched columns.
    _validate_link_disjoint(ibeg_arr, cnt_arr, index_base, level=coord_key)
    n_base = link_base_extent(ibeg_arr, cnt_arr, index_base)
    if n_base <= 0:
        _record_obs_read(io_stats, 0)
        return None

    # Match records to this shard with the same exact mortie test the sample
    # rows get below — expanded sample coordinates ARE the record coordinates,
    # so the record mask is exact, not a rep-point approximation.
    coarse_mask = grid.shards_of(grid.assign(coarse_lats, coarse_lons)) == shard_key

    rp = data_source.get("read_plan")
    plan = None
    if isinstance(rp, dict) and rp.get("spatial_index"):
        if rp["spatial_index"] != coord_key:
            raise ValueError(
                f"read_plan.spatial_index {rp['spatial_index']!r} must name the "
                f"coordinates level {coord_key!r} on a vlen data source"
            )
        plan = plan_read(
            coarse_lats,
            coarse_lons,
            ibeg_arr,
            cnt_arr,
            n_base,
            index_base=index_base,
            pad=int(rp.get("pad", 1)),
            full_read_threshold=float(rp.get("full_read_threshold", 0.9)),
            coarse_mask=coarse_mask,
        )
        if not plan.parent_runs:
            _record_obs_read(io_stats, 0)
            return None
        if plan.full_read:
            plan = None  # selectivity above threshold: read the flat datasets fully
    elif not coarse_mask.any():
        _record_obs_read(io_stats, 0)
        return None

    # ---- The gather map: (record, within-record) index per DECODED base row.
    # The planned arm builds it at plan rate, never at sample rate — the map is
    # the read's dominant allocation on a real granule. The full arm is
    # O(n_base) by necessity and needs no row selection at all, so ``global_idx``
    # stays ``None`` (the identity) rather than a full-rate ``arange``.
    if plan is not None:
        global_idx, parent_planned, within_planned = _planned_gather_map(
            ibeg_arr, cnt_arr, index_base, n_base, plan.base_slices
        )
    else:
        global_idx = None
        parent_planned, within_planned = expand_link_indices(ibeg_arr, cnt_arr, index_base, n_base)

    # ``global_idx`` is the arm switch for every expansion below: the plan's base
    # rows on the planned arm, ``None`` on the full arm. On the full arm the rows
    # ARE ``arange(n_base)``, but materializing that identity would be a
    # regression, not a convenience — it costs 8 B/row (1.57 GB on a GEDI beam
    # group) and turns each base-rate paint into a 33 B/row searchsorted gather.
    # So the full arm keeps the base-rate forms it always used and never builds
    # a row vector at all.

    # Rows DECODED by this group (issue #374): the planned slices (or the full
    # base extent), counted before the shard mask and filters below.
    _record_obs_read(io_stats, len(parent_planned))
    if len(parent_planned) == 0:
        return None

    # ---- Expand coordinates; exact per-sample shard mask. A gap row (no
    # record) has no coordinates: clamp its gather index so the fancy index
    # cannot wrap to the last record, and force it out via the valid mask.
    valid = parent_planned >= 0
    parent_safe = np.where(valid, parent_planned, 0)
    lats = coarse_lats[parent_safe]
    lons = coarse_lons[parent_safe]
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = valid & (grid.shards_of(leaf_ids) == shard_key)
    if not mask_spatial.any():
        return None

    # ---- Read base-rate variables + base-level filter datasets (the ragged
    # gather: planned slices when a plan is active, full reads otherwise).
    filters = filters_from_data_source(data_source)
    base_structured = [
        f
        for f in filters
        if "expression" not in f
        and f.get("asset") is None
        and (f.get("level") is None or f.get("level") == base_level_key)
    ]
    coarse_structured = [
        f
        for f in filters
        if "expression" not in f
        and f.get("asset") is None
        and f.get("level") is not None
        and f.get("level") != base_level_key
    ]
    asset_filters = [f for f in filters if "expression" not in f and f.get("asset") is not None]
    expressions = [f for f in filters if "expression" in f]

    path_specs = _variable_specs(path_form_variables(variables))
    var_paths = {col: tmpl.format(group=group) for col, (tmpl, _) in path_specs.items()}
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    base_paths = list(dict.fromkeys(list(var_paths.values()) + list(filter_paths.values())))
    if plan is not None:
        _base_read_fn = read_fn
        if _base_read_fn is None:

            def _base_read_fn(path, hyperslice=None):
                if hyperslice is None:
                    return h5obj.readDatasets([path])[path]
                return h5obj.readDatasets([{"dataset": path, "hyperslice": hyperslice}])[path]

        arrays_by_path = _read_paths_pooled(
            [(p, None) for p in base_paths],
            lambda p, dt: execute_read_plan(plan, _base_read_fn, p, dt),
            workers,
        )
        # The planned arm cannot see the dataset's declared length, but it can
        # check the shape it does control: a hyperslice that runs off the end of
        # a truncated dataset comes back short, and every base-rate array must
        # be exactly the gather map's length. Without this the mismatch surfaces
        # downstream as an opaque boolean-index ``IndexError`` in a worker log —
        # the same corruption the full arm below refuses in domain words, and
        # the class of failure issue #452 spent a day isolating.
        for path, arr in arrays_by_path.items():
            if len(arr) != len(global_idx):
                raise ValueError(
                    f"dataset {path!r} returned {len(arr)} of {len(global_idx)} planned "
                    f"rows; the record link does not tile the declared base extent"
                )
    else:
        # Full arm: the flat datasets may be LONGER than the link's extent — a
        # strided product pads the tail of its last record's window (GEDI:
        # 1,420 samples allocated, ``count`` valid). Those rows belong to no
        # record, so trim them here and keep every base-rate array the same
        # length as the gather map (issue #452). A dataset SHORTER than the
        # extent is a real inconsistency (the link over-runs the array) and is
        # reported with the gather map's wording.
        arrays_by_path = {}
        for path, arr in _read_full(base_paths).items():
            arr = np.asarray(arr)
            if len(arr) < n_base:
                raise ValueError(
                    f"dataset {path!r} has {len(arr)} rows, fewer than the record "
                    f"link's base extent {n_base}; the record link does not tile "
                    f"the declared base extent"
                )
            arrays_by_path[path] = arr[:n_base]

    # ---- Filters. Base-level structured predicates over the gathered rows;
    # record-level predicates expand to base rate via each level's link (the
    # existing cross-level machinery); sibling-asset predicates join per
    # record on the declared key, then expand the same way.
    keep_mask: np.ndarray | None = None
    for f in base_structured:
        flag = np.asarray(arrays_by_path[filter_paths[id(f)]])[mask_spatial]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    # The PLANNED arm expands at planned rate (issue #452): each record-rate
    # verdict is gathered onto the rows this read decoded, never painted across
    # the whole base extent and sliced afterwards (196 MB per filter on a GEDI
    # beam group). The FULL arm decodes every base row anyway, so it keeps the
    # base-rate paint — cheaper there by 33x, since the at-rows form's
    # searchsorted temporaries are 33 B/row against the paint's 1 B/row. Either
    # way a row owned by no record is False.
    cross_planned: np.ndarray | None = None
    for f in coarse_structured:
        cf_flag_path = f["dataset"].format(group=group)
        if f["level"] == coord_key:
            # The predicate rides the coordinates level's own link, whose per-row
            # owner the gather map above already carries — so reuse it, exactly
            # as the sibling-asset arm below does. Deriving it a second time (by
            # last-start-wins, where the map was painted) would put two owner
            # maps on the route that have to agree, for no gain.
            coarse_fmask = _predicate_mask(np.asarray(_read_full([cf_flag_path])[cf_flag_path]), f)
            expanded = coarse_fmask[parent_safe] & valid
        else:
            cf_link = levels[f["level"]]["link"]
            cf_paths = [
                cf_flag_path,
                cf_link["index_beg"].format(group=group),
                cf_link["count"].format(group=group),
            ]
            cf_data = _read_full(cf_paths)
            cf_ibeg = np.asarray(cf_data[cf_paths[1]])
            cf_cnt = np.asarray(cf_data[cf_paths[2]])
            cf_index_base = int(cf_link.get("index_base", 0))
            _validate_link_disjoint(cf_ibeg, cf_cnt, cf_index_base, level=f["level"])
            coarse_fmask = _predicate_mask(np.asarray(cf_data[cf_flag_path]), f)
            # A genuinely different link owns its own placement, so its per-row
            # owner does have to be derived — by searchsorted over that link's
            # starts on the planned arm, by the base-rate paint on the full one.
            expanded = (
                _expand_mask_at_rows(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, global_idx)
                if global_idx is not None
                else _expand_mask_to_base(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, n_base)
            )
        cross_planned = expanded if cross_planned is None else (cross_planned & expanded)

    # One record-key join per sibling asset, shared by the predicate arm here and
    # the variable arm below (issue #464 review): the shipped GEDI template hangs
    # both a saturation filter and the DEM variable off its ``l2a`` asset, and a
    # join per consumer would pay two primary key reads, two sibling
    # ``readDatasets`` and two argsorts per group per granule — ``_read_full`` has
    # no cache, so that is real I/O.
    asset_var_paths: dict[str, dict[str, str]] = {}
    for name, entry in asset_variable_specs(data_source).items():
        asset_var_paths.setdefault(entry["asset"], {})[name] = entry["path"].format(group=group)
    asset_joins = (
        _sibling_joins(
            h5obj,
            group,
            data_source,
            asset_filters,
            asset_var_paths,
            len(coarse_lats),
            siblings,
            _read_full,
        )
        if asset_filters or asset_var_paths
        else {}
    )

    if asset_filters:
        record_mask = _sibling_record_mask(group, asset_filters, len(coarse_lats), asset_joins)
        # The sibling join is keyed to the coordinates level, whose per-row owner
        # the gather map already carries.
        expanded = record_mask[parent_safe] & valid
        cross_planned = expanded if cross_planned is None else (cross_planned & expanded)

    if cross_planned is not None:
        cross_masked = cross_planned[mask_spatial]
        keep_mask = cross_masked if keep_mask is None else (keep_mask & cross_masked)
    if keep_mask is not None and not keep_mask.any():
        return None

    # ---- Synthesized columns (computed, never read) + record-level variable
    # broadcasts (the existing expand primitive), aligned to the gathered rows.
    synth_cols: dict[str, np.ndarray] = {}
    for name, entry in synthesized_specs(variables).items():
        sp = _read_full([entry["start"].format(group=group), entry["stop"].format(group=group)])
        synth_cols[name] = synthesize_linspace(
            np.asarray(sp[entry["start"].format(group=group)]),
            np.asarray(sp[entry["stop"].format(group=group)]),
            cnt_arr,
            parent_planned,
            within_planned,
        )

    # Record-level variable broadcasts. On the PLANNED arm they gather straight
    # onto this read's rows (issue #452): the base-rate form is one sample-rate
    # array per companion variable — ~7 GB for GEDI's six against a 2 GB worker.
    # On the FULL arm ``base_rows`` stays ``None`` and the original base-rate
    # broadcast runs: every base row is decoded there anyway, and the at-rows
    # gather would cost 8x the paint (33 B/row vs 4) for the same answer.
    # Companions declared on the COORDINATES level — the common case, and all six
    # of the shipped GEDI template's — reuse the gather map instead of each
    # re-deriving it (issue #452 review).
    seg_broadcasts = (
        _read_segment_broadcasts(
            h5obj,
            group,
            data_source,
            levels,
            n_base,
            read_fn=read_fn,
            base_rows=global_idx,
            parents_by_level={coord_key: (parent_safe, valid)},
        )
        if _segment_level_variables(data_source)
        else {}
    )

    # Asset-sourced record-level variables (issue #464): read from the sibling
    # handle via the record-key join, NaN'd where the record has no sibling
    # row, then broadcast through the same coordinates-level gather map the
    # companions above reuse — identical on both arms, since ``parent_safe`` /
    # ``valid`` are already at this read's rate.
    #
    # A granule with no open sibling handle degrades a VARIABLE-ONLY asset to
    # all-NaN columns with one warning per granule; asset FILTERS keep their
    # fail-closed semantics, and since the raise happens up in
    # ``_sibling_joins`` it wins whenever the SAME asset carries both. That is
    # the shipped gedi01b template's shape — ``l2a`` carries the
    # ``rx_clipbin_count`` filter alongside the DEM variable — so a pairless
    # granule there fails the group outright and the NaN arm below never fires
    # (pinned: ``TestGediTemplate.test_pairless_granule_fails_the_group``). The
    # NaN arm serves a config whose asset has variables only.
    asset_cols: dict[str, np.ndarray] = {}
    for asset, name_paths in asset_var_paths.items():
        join = asset_joins.get(asset)
        if join is None:
            # Absent from the joins means no open sibling handle for this
            # granule, and no filter rides this asset (one that did would have
            # raised); the one-per-granule warning was logged there.
            record_vals = {
                name: np.full(len(coarse_lats), np.nan, dtype=np.float64) for name in name_paths
            }
        else:
            values_by_path, matched = join
            record_vals = {}
            for name, path in name_paths.items():
                # Upgrade only as far as the NaN needs (issue #464 review): the
                # unmatched-row fill demands a float dtype, so promote against
                # float32 — a float32 DEM stays float32 instead of doubling the
                # column's paint, and an integer flag lands in a float type able
                # to hold it. ``astype`` copies either way, so the NaN write
                # never reaches the join's shared ``values_by_path`` (one join
                # per asset, several consumers).
                vals = np.asarray(values_by_path[path])
                if vals.ndim > 1:
                    # The asset form is scoped to record-rate scalars: it has no
                    # 'column' selector, so say that rather than borrow the
                    # plain-variable message ("requires an integer 'column'"),
                    # which points at a key this form rejects (issue #464
                    # review). Richer L2A companions ride issue #465.
                    raise ValueError(
                        f"asset variable {name!r}: dataset {path!r} is "
                        f"{vals.ndim}-D; the asset form takes no 'column' "
                        f"selector yet (1-D datasets only; refs issue #465)"
                    )
                vals = vals.astype(np.promote_types(vals.dtype, np.float32))
                vals[~matched] = np.nan
                record_vals[name] = vals
        for name, vals in record_vals.items():
            asset_cols[name] = _gather_segment_at_parents(vals, parent_safe, valid)

    # ---- Assemble columns: gathered variables, synthesized, broadcasts, leaf.
    data_dict: dict[str, np.ndarray] = {}
    for col_name, path in var_paths.items():
        values = _select_column(np.asarray(arrays_by_path[path]), path_specs[col_name][1], col_name)
        values = values[mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, values in synth_cols.items():
        values = values[mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, planned_values in seg_broadcasts.items():
        values = planned_values[mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, planned_values in asset_cols.items():
        values = planned_values[mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    leaf_after = leaf_ids[mask_spatial]
    data_dict["leaf_id"] = leaf_after[keep_mask] if keep_mask is not None else leaf_after

    # Base-level expression filters over the assembled columns, as in the
    # dense routes (aggregation-time escape hatch, no pushdown).
    expr_names = list(variables) + list(seg_broadcasts) + list(asset_cols)
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
        if not emask.any():
            return None
        data_dict = {k: v[emask] for k, v in data_dict.items()}

    if arrow:
        from arro3.core import Table

        return Table.from_pydict(data_dict)
    return pd.DataFrame(data_dict)


def _sibling_joins(
    h5obj,
    group: str,
    data_source: dict,
    asset_filters: list[dict],
    asset_var_paths: dict[str, dict[str, str]],
    n_records: int,
    siblings: dict | None,
    read_full,
) -> dict[str, tuple[dict[str, np.ndarray], np.ndarray]]:
    """One record-key join per sibling asset, covering every consumer's datasets.

    Each declared sibling asset (``data_source.assets``) carries a ``join``
    block naming the record-level key dataset on both sides (GEDI:
    ``shot_number`` on L1B and L2A). This collects the UNION of the datasets
    that asset's consumers need — the predicates' ``dataset`` paths plus the
    asset variables' ``path`` values — and joins once
    (:func:`_sibling_joined_values`), so an asset used by both arms pays one key
    read, one sibling read and one argsort instead of two (issue #464 review).

    Returns ``{asset: (values_by_path, matched)}``. An asset the granule carries
    no open handle for is ABSENT from the result: a VARIABLE-only asset degrades
    to NaN columns with one warning per granule
    (:func:`_warn_missing_sibling_once`), while an asset carrying a filter raises
    instead — an unfilterable record must not pass a quality gate silently (the
    pairless-granule posture of the shardmap build, applied per record). So every
    filter asset is guaranteed present for :func:`_sibling_record_mask`.
    """
    assets_cfg = data_source.get("assets")
    if not isinstance(assets_cfg, dict):
        raise ValueError("asset filters and asset variables require a data_source.assets block")
    paths_by_asset: dict[str, list[str]] = {}
    for f in asset_filters:
        paths_by_asset.setdefault(f["asset"], []).append(f["dataset"].format(group=group))
    for asset, name_paths in asset_var_paths.items():
        paths_by_asset.setdefault(asset, []).extend(name_paths.values())
    filter_assets = {f["asset"] for f in asset_filters}
    joins: dict[str, tuple[dict[str, np.ndarray], np.ndarray]] = {}
    for asset, paths in paths_by_asset.items():
        cfg = assets_cfg.get(asset)
        if cfg is None:
            raise ValueError(f"asset {asset!r} is not declared in data_source.assets")
        sibling = (siblings or {}).get(asset)
        if sibling is None:
            if asset in filter_assets:
                raise ValueError(
                    f"granule carries no open sibling handle for asset {asset!r}; "
                    f"was the shard map built with the paired-asset join?"
                )
            _warn_missing_sibling_once(h5obj, asset, sorted(asset_var_paths.get(asset, {})))
            continue
        joins[asset] = _sibling_joined_values(
            sibling, group, asset, cfg, n_records, paths, read_full
        )
    return joins


def _sibling_record_mask(
    group: str,
    asset_filters: list[dict],
    n_records: int,
    joins: dict[str, tuple[dict[str, np.ndarray], np.ndarray]],
) -> np.ndarray:
    """Per-record keep-mask from sibling-asset predicates (the L2A join).

    Evaluates each predicate on the joined sibling values ``_sibling_joins``
    already gathered for this asset (the join is shared with the asset-variable
    arm, issue #464 review). A record with no sibling match fails every
    predicate on that asset — an unfilterable record must not pass a quality
    gate silently — and an asset carrying a filter is guaranteed to be in
    ``joins``: :func:`_sibling_joins` raises rather than omit one.
    """
    mask = np.ones(n_records, dtype=bool)
    by_asset: dict[str, list[dict]] = {}
    for f in asset_filters:
        by_asset.setdefault(f["asset"], []).append(f)
    for asset, filters in by_asset.items():
        values_by_path, matched = joins[asset]
        asset_mask = matched.copy()
        for f in filters:
            asset_mask &= _predicate_mask(values_by_path[f["dataset"].format(group=group)], f)
        mask &= asset_mask
    return mask


def _sibling_joined_values(
    sibling,
    group: str,
    asset: str,
    cfg: dict,
    n_records: int,
    dataset_paths: list[str],
    read_full,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    """Sibling-asset datasets joined to the primary's record order (the key join).

    The record-rate join one :func:`_sibling_joins` call runs per asset (issue
    #464): reads the ``join`` block's key dataset on both sides plus
    ``dataset_paths`` from the sibling handle (all per-record rate, small),
    matches records by key value, and returns ``(values_by_path, matched)`` —
    row ``i`` of each gathered array is the sibling's value for primary record
    ``i``, and ``matched[i]`` says whether that record has a sibling row at
    all. An unmatched row's value is a placeholder (the clamped gather's
    neighbor, or a zero when the sibling has no rows to clamp to), so every
    consumer must apply its own missing-row policy against ``matched``: asset
    filters fail the record closed (:func:`_sibling_record_mask`); asset
    variables NaN it. An EMPTY sibling (zero rows) is that contract's degenerate
    case, not an error — every record is unmatched, and each path's placeholder
    is a zeros array of length ``n_records`` shaped like the sibling dataset
    beyond axis 0 (issue #464 review). A DUPLICATED right key raises: it would
    make "the sibling's value" one of N candidates chosen by sort order, which
    is the silent-junk shape this reader exists to refuse. So does a gathered
    dataset at a different RATE than the join key — the gather would answer with
    another record's row, or raise an ``IndexError`` naming nothing.
    """
    join = cfg["join"]
    left_paths = _join_key_paths(join["left"], group)
    right_paths = _join_key_paths(join["right"], group)
    right_path = right_paths[0] if len(right_paths) == 1 else tuple(right_paths)
    # Index by path: h5coro's ``H5Promise`` (what ``readDatasets`` returns on a
    # real handle) has ``__getitem__`` and ``keys`` but no ``.values()``.
    left_data = read_full(left_paths)
    left_keys = _composite_key([np.asarray(left_data[p]) for p in left_paths])
    sib_paths = list(right_paths) + list(dataset_paths)
    sib_data = sibling.readDatasets(list(dict.fromkeys(sib_paths)))
    right_keys = _composite_key([np.asarray(sib_data[p]) for p in right_paths])
    if len(left_keys) != n_records:
        raise ValueError(
            f"assets.{asset}.join.left has {len(left_keys)} records; the "
            f"coordinates level has {n_records}"
        )
    # Every gathered dataset must be at the join key's rate, or ``[sib_idx]``
    # silently answers with the wrong row — or raises a bare ``IndexError``
    # naming neither asset nor dataset (issue #464 review). Checked here, before
    # the empty-sibling placeholder too: a zero-row key beside a populated
    # dataset is the same malformed product.
    for p in dict.fromkeys(dataset_paths):
        n_rows = len(np.asarray(sib_data[p]))
        if n_rows != len(right_keys):
            raise ValueError(
                f"assets.{asset} dataset {p!r} has {n_rows} rows; the join key "
                f"{right_path!r} has {len(right_keys)}"
            )
    if len(right_keys) == 0:
        # No sibling row to gather: hand back the documented placeholder rather
        # than let the clamped gather below index an empty array (the guards it
        # used to carry kept ``matched``/``sib_idx`` alive but the gather still
        # raised ``IndexError``, naming neither asset nor dataset).
        return {
            p: np.zeros(
                (n_records, *np.asarray(sib_data[p]).shape[1:]),
                dtype=np.asarray(sib_data[p]).dtype,
            )
            for p in dict.fromkeys(dataset_paths)
        }, np.zeros(n_records, dtype=bool)
    # Key-value join: position of each left key in the sibling's rows.
    order = np.argsort(right_keys, kind="stable")
    sorted_keys = right_keys[order]
    # The join is only a join if the right key identifies a row: a duplicated
    # key would resolve to whichever row sorts first, so a filter verdict — and
    # since issue #464 a STORED value — would be one of N candidates with
    # nothing recording the ambiguity. A duplicate-key sibling is a malformed
    # product; refuse it here (one vectorized pass at record rate).
    dup = sorted_keys[1:] == sorted_keys[:-1]
    if dup.any():
        i = int(np.argmax(dup))
        raise ValueError(
            f"assets.{asset}.join.right {right_path!r} has duplicate key "
            f"{sorted_keys[i]}; the record join requires unique sibling keys"
        )
    pos = np.searchsorted(sorted_keys, left_keys, side="left")
    pos_clip = np.minimum(pos, len(sorted_keys) - 1)
    matched = sorted_keys[pos_clip] == left_keys
    sib_idx = order[pos_clip]
    values_by_path = {p: np.asarray(sib_data[p])[sib_idx] for p in dict.fromkeys(dataset_paths)}
    return values_by_path, matched


def _join_key_paths(side, group: str) -> list[str]:
    """The group-substituted key path(s) of one side of a ``join`` block."""
    paths = side if isinstance(side, list) else [side]
    return [p.format(group=group) for p in paths]


def _composite_key(arrays: list[np.ndarray]) -> np.ndarray:
    """One sortable key array from one or more key columns (issue #678).

    A single column is the key itself (the GEDI ``shot_number`` form,
    unchanged). Several columns pack into a structured array whose fields
    compare lexicographically, so the argsort / searchsorted / equality join
    in :func:`_sibling_joined_values` runs on tuples without a second code
    path: GLAH01 ↔ GLAH05 match on (``i_rec_ndx``, ``i_shot_count``).
    """
    if len(arrays) == 1:
        return arrays[0]
    n = len(arrays[0])
    if any(len(a) != n for a in arrays):
        raise ValueError(f"composite join key columns differ in length: {[len(a) for a in arrays]}")
    names = ",".join(f"k{i}" for i in range(len(arrays)))
    return np.rec.fromarrays(arrays, names=names).view(np.ndarray)


def _warn_missing_sibling_once(h5obj, asset: str, names: list[str]) -> None:
    """One warning per granule when an asset VARIABLE's sibling is absent (#464).

    Asset variables degrade to NaN columns — unlike asset filters, which fail
    closed (:func:`_sibling_record_mask`): a quality gate must never pass
    unverified records silently, but a missing companion column is recoverable
    downstream (and an expression filter over it drops the NaN rows by plain
    comparison semantics). The read runs once per beam group; the guard rides
    the granule's open primary handle so eight groups warn once, not eight
    times.

    Only a VARIABLE-ONLY asset reaches here: :func:`_sibling_joins` raises for
    an asset carrying a filter before this is called, so a config declaring
    both on one asset (the shipped gedi01b template's ``l2a``) never warns —
    it fails the group.
    """
    warned = getattr(h5obj, "_zagg_asset_nan_warned", None)
    if warned is None:
        warned = set()
        try:
            h5obj._zagg_asset_nan_warned = warned
        except AttributeError:
            pass  # a handle with __slots__: degrade to one warning per group
    if asset in warned:
        return
    warned.add(asset)
    logger.warning(
        f"granule carries no open sibling handle for asset {asset!r}; asset "
        f"variables {names} filled with NaN"
    )
