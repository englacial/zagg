"""Fixed-width 2-D record-array read route (issue #678).

The second record-structured modality beside the ragged vlen route
(:mod:`zagg.processing.read_vlen`): a record level whose ``link`` declares a
constant ``width`` instead of ``index_beg``/``count`` datasets. Every record
owns exactly ``width`` consecutive base rows, so the link is synthesized —
``index_beg = i * width``, ``count = width`` — at record rate (O(n_records),
never O(n_samples)) and every downstream primitive of the vlen grammar
(planned record selection, expand-by-count, record-level filters and
companions, the sibling-asset join) runs unchanged over it. The route owns
what the ragged one cannot express:

- **axes** — an N-D base-level dataset is flattened by an explicit
  per-dimension declaration (``[record, sample]``, ``[record, sample, 0]``,
  ``[sample, record]``): integer picks first, then a transpose to
  ``(record, sample)``, then ``reshape(-1)`` — row-major, so the flat row
  order is exactly the ragged gather order with a constant count. A
  record-first layout slices per record on the planned read (rows along
  dim 0, trailing dims whole — the h5coro hyperslice contract); any other
  layout reads whole with a logged notice.
- **offsets synthesis** — ``start[record] + offsets[sample] * scale`` with
  ``offsets`` a 1-D dataset of length ``width`` (a dimension scale such as
  GLAS's ``DS_Gate_Index_544``) or a constant step.
- **index synthesis** — the within-record sample index as a column, so the
  ordinary expression filter drops a padded row's tail.
- **composite join keys** — ``assets.<name>.join.left/right`` as lists:
  GLAH01 ↔ GLAH05/06 match on (``i_rec_ndx``, ``i_shot_count``).
- **asset-sourced coordinates** — ``coordinates.latitude/longitude`` as
  ``{asset, path}``: GLAH01 carries no 40 Hz geolocation, so the record
  coordinates come from the sibling through the same join; a record with no
  sibling row has no position and is dropped before the shard mask.

Width or rank mismatches against the dataset's actual shape raise before any
aggregation (the issue #452 lesson: a strided or mismatched product must not
silently read zero). GEDI and ATL03 never enter here — dispatch is on
``link.width``, and the vlen route is untouched when it is absent.
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
from zagg.processing.read_vlen import (
    _sibling_joins,
    _sibling_record_mask,
    asset_variable_specs,
    path_form_variables,
    synthesize_linspace,
    synthesized_specs,
)
from zagg.read_plan import execute_read_plan, plan_read

logger = logging.getLogger(__name__)


def fixed_width_level(data_source: dict) -> tuple[str, dict] | None:
    """``(level key, link)`` of the level whose link declares ``width``, else ``None``.

    The dispatch predicate for this route (:func:`zagg.processing.read._read_group`),
    tested after ``coordinates.level``: a width link under the coordinates level
    routes here, a dataset link to the vlen route. Validation
    (``zagg.config._width_level``) already guarantees at most one width level
    against the base level.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict):
        return None
    for key, lvl in levels.items():
        link = lvl.get("link") if isinstance(lvl, dict) else None
        if isinstance(link, dict) and "width" in link and link.get("to") == base_level:
            return key, link
    return None


def fixed_link_arrays(n_records: int, width: int) -> tuple[np.ndarray, np.ndarray]:
    """The synthesized origin-0 link of a fixed-width level: ``(index_beg, count)``.

    Record ``i`` owns base rows ``[i * width, (i + 1) * width)``. Two
    record-rate ``int64`` arrays, so :func:`zagg.read_plan.plan_read`,
    :func:`zagg.processing.read.link_base_extent` and the cross-level
    expansions consume it exactly like a dataset link; it is contiguous and
    disjoint by construction, so its extent is ``n_records * width``.
    """
    ibeg = np.arange(n_records, dtype=np.int64) * int(width)
    cnt = np.full(n_records, int(width), dtype=np.int64)
    return ibeg, cnt


def flatten_axes(values: np.ndarray, axes: list, width: int, name: str) -> np.ndarray:
    """Apply an ``axes`` declaration: integer picks, transpose, ``reshape(-1)``.

    ``values`` is the dataset (or a record-row slice of it). Rank must equal
    ``len(axes)`` and the ``sample`` axis must be exactly ``width`` long —
    either mismatch is the loud error issue #452 asks for, raised on the
    first read, before any aggregation. Returns a 1-D array of
    ``n_records_in_values * width`` rows in ``(record, sample)`` row-major
    order.
    """
    values = np.asarray(values)
    if values.ndim != len(axes):
        raise ValueError(
            f"variable {name!r}: dataset is {values.ndim}-D but axes {axes!r} declares "
            f"{len(axes)} dimensions"
        )
    sample_axis = axes.index("sample")
    if values.shape[sample_axis] != width:
        raise ValueError(
            f"variable {name!r}: the 'sample' axis has {values.shape[sample_axis]} entries "
            f"but link.width is {width}; the fixed-width link does not tile the dataset"
        )
    picks = tuple(slice(None) if a in ("record", "sample") else int(a) for a in axes)
    picked = values[picks]
    kept = [a for a in axes if a in ("record", "sample")]
    if kept != ["record", "sample"]:
        picked = picked.T
    return np.ascontiguousarray(picked).reshape(-1)


def synthesize_offsets(
    start_vals: np.ndarray,
    offsets: np.ndarray | float,
    scale: float,
    width: int,
    parent_idx: np.ndarray,
    within_idx: np.ndarray,
) -> np.ndarray:
    """Per-sample ``start[record] + offsets[sample] * scale``.

    ``offsets`` is a 1-D array of length ``width`` (one entry per within-record
    sample; a different length is the same mismatch :func:`flatten_axes`
    refuses) or a constant step, in which case ``offsets[sample] = step *
    sample``. ``start`` is per record. Rows with no record (``parent_idx ==
    -1``) get NaN, as the linspace synthesis does.
    """
    parent_idx = np.asarray(parent_idx)
    within = np.asarray(within_idx)
    if isinstance(offsets, (int, float)):
        per_sample = within.astype(np.float64) * float(offsets)
    else:
        table = np.asarray(offsets, dtype=np.float64)
        if table.ndim != 1 or len(table) != width:
            raise ValueError(
                f"synthesize offsets: the offsets dataset has shape {table.shape}; "
                f"expected a 1-D table of length link.width {width}"
            )
        per_sample = table[within]
    valid = parent_idx >= 0
    out = np.full(len(parent_idx), np.nan, dtype=np.float64)
    start = np.asarray(start_vals, dtype=np.float64)[parent_idx[valid]]
    out[valid] = start + per_sample[valid] * float(scale)
    return out


def _coordinate_sources(coordinates: dict) -> dict[str, dict] | None:
    """``{latitude: {asset, path}, longitude: ...}`` when the record coordinates
    are asset-sourced (issue #678, GLAH01 ↔ GLAH05), else ``None``."""
    entries = {k: coordinates.get(k) for k in ("latitude", "longitude")}
    if all(isinstance(e, dict) and "asset" in e for e in entries.values()):
        return {k: dict(e) for k, e in entries.items()}
    return None


def _validate_fixed_config(data_source: dict) -> tuple[str, dict, int]:
    """Completeness gate for the route; returns ``(coord level, level, width)``."""
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or not levels or not base_level:
        raise ValueError("coordinates.level requires 'levels' and 'base_level'")
    key = data_source["coordinates"]["level"]
    lvl = levels.get(key)
    if lvl is None:
        raise ValueError(f"coordinates.level {key!r} is not a key in levels")
    link = lvl.get("link")
    if not isinstance(link, dict) or link.get("to") != base_level or "width" not in link:
        raise ValueError(
            f"coordinates.level {key!r} must link directly to base level {base_level!r} "
            "with 'width' (the fixed-width route)"
        )
    width = link["width"]
    if width == "auto":
        # Resolving the width needs the dataset's shape before the planned
        # read, and the index-backend ``read_fn`` seam has no metadata-only
        # query yet; the swath follow-up (issue #678 phase 5) adds it.
        raise ValueError(
            f"levels.{key}.link.width: 'auto' is not readable yet (declare the "
            "integer width; swath support is the issue #678 follow-up)"
        )
    return key, lvl, int(width)


def _fixed_read_group(
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
    """Read and spatially filter one fixed-width HDF5 group.

    Same ``pandas.DataFrame`` / ``arro3.core.Table`` / ``None`` contract as
    :func:`zagg.processing.read._read_group`, and the same shape as
    :func:`zagg.processing.read_vlen._vlen_read_group`: record-rate arrays are
    read fully, the shard's records are selected with the exact mortie test,
    and when ``read_plan.spatial_index`` names the coordinates level the base
    datasets are sliced to the planned records — record-first ``axes``
    datasets by row range, flat base-rate datasets by the synthesized link.
    ``read_fn`` / ``io_stats`` / ``siblings`` are as in ``_read_group``.
    """
    coord_key, coord_lvl, width = _validate_fixed_config(data_source)
    levels = data_source["levels"]
    base_level_key = data_source["base_level"]
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    workers = _read_workers(data_source) if read_fn is not None else 1

    def _read_full(paths: list[str]) -> dict:
        if read_fn is None:
            return h5obj.readDatasets(list(dict.fromkeys(paths)))
        return _read_paths_pooled(
            [(p, None) for p in dict.fromkeys(paths)], lambda p, dt: read_fn(p), workers
        )

    def _read_rows(path: str, lo: int, hi: int):
        """Rows ``[lo, hi)`` along dim 0, trailing dims whole."""
        if read_fn is not None:
            return read_fn(path, hyperslice=[(lo, hi)])
        return h5obj.readDatasets([{"dataset": path, "hyperslice": [(lo, hi)]}])[path]

    filters = filters_from_data_source(data_source)
    asset_filters = [f for f in filters if "expression" not in f and f.get("asset") is not None]
    asset_var_paths: dict[str, dict[str, str]] = {}
    for name, entry in asset_variable_specs(data_source).items():
        asset_var_paths.setdefault(entry["asset"], {})[name] = entry["path"].format(group=group)

    # ---- Record-level coordinates: on the primary, or joined from a sibling.
    coord_assets = _coordinate_sources(coordinates)
    if coord_assets is None:
        lat_path = coordinates["latitude"].format(group=group)
        lon_path = coordinates["longitude"].format(group=group)
        coarse = _read_full([lat_path, lon_path])
        coarse_lats = np.asarray(coarse[lat_path], dtype=np.float64)
        coarse_lons = np.asarray(coarse[lon_path], dtype=np.float64)
        n_records = len(coarse_lats)
        matched = np.ones(n_records, dtype=bool)
    else:
        n_records = _primary_record_count(data_source, coord_assets, group, _read_full)
        for key, entry in coord_assets.items():
            asset_var_paths.setdefault(entry["asset"], {})[f"__coord_{key}"] = entry["path"].format(
                group=group
            )
    asset_joins = (
        _sibling_joins(
            h5obj,
            group,
            data_source,
            asset_filters,
            asset_var_paths,
            n_records,
            siblings,
            _read_full,
        )
        if asset_filters or asset_var_paths
        else {}
    )
    if coord_assets is not None:
        coarse_lats, coarse_lons, matched = _joined_coordinates(
            coord_assets, group, asset_joins, n_records
        )
        for key, entry in coord_assets.items():
            del asset_var_paths[entry["asset"]][f"__coord_{key}"]

    if n_records == 0:
        _record_obs_read(io_stats, 0)
        return None
    ibeg_arr, cnt_arr = fixed_link_arrays(n_records, width)
    n_base = link_base_extent(ibeg_arr, cnt_arr, 0)

    # Records with no sibling row have no position: out of the mask, never
    # assigned (a NaN coordinate must not reach the grid).
    coarse_mask = np.zeros(n_records, dtype=bool)
    if matched.any():
        coarse_mask[matched] = (
            grid.shards_of(grid.assign(coarse_lats[matched], coarse_lons[matched])) == shard_key
        )

    rp = data_source.get("read_plan")
    plan = None
    if isinstance(rp, dict) and rp.get("spatial_index"):
        if rp["spatial_index"] != coord_key:
            raise ValueError(
                f"read_plan.spatial_index {rp['spatial_index']!r} must name the "
                f"coordinates level {coord_key!r} on a fixed-width data source"
            )
        plan = plan_read(
            coarse_lats,
            coarse_lons,
            ibeg_arr,
            cnt_arr,
            n_base,
            index_base=0,
            pad=int(rp.get("pad", 1)),
            full_read_threshold=float(rp.get("full_read_threshold", 0.9)),
            coarse_mask=coarse_mask,
        )
        if not plan.parent_runs:
            _record_obs_read(io_stats, 0)
            return None
        if plan.full_read:
            plan = None
    elif not coarse_mask.any():
        _record_obs_read(io_stats, 0)
        return None

    # ---- The gather map. Contiguous constant-width records make it
    # arithmetic: owner = row // width, within = row % width. Planned arm at
    # plan rate, full arm at base rate (every base row is decoded there).
    if plan is not None:
        global_idx = np.concatenate([np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices])
        parent_idx = global_idx // width
        within_idx = global_idx % width
    else:
        global_idx = None
        parent_idx = np.repeat(np.arange(n_records, dtype=np.int64), width)
        within_idx = np.tile(np.arange(width, dtype=np.int64), n_records)
    _record_obs_read(io_stats, len(parent_idx))
    if len(parent_idx) == 0:
        return None

    valid = matched[parent_idx]
    parent_safe = parent_idx  # every row has an owner; unmatched rows leave via ``valid``
    lats = np.where(valid, coarse_lats[parent_safe], 0.0)
    lons = np.where(valid, coarse_lons[parent_safe], 0.0)
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = valid & (grid.shards_of(leaf_ids) == shard_key)
    if not mask_spatial.any():
        return None

    # ---- Base-rate reads: flat datasets by the synthesized link, axes
    # datasets by record rows.
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
    expressions = [f for f in filters if "expression" in f]

    plain = {n: e for n, e in path_form_variables(variables).items() if not _has_axes(e)}
    path_specs = _variable_specs(plain)
    var_paths = {col: tmpl.format(group=group) for col, (tmpl, _) in path_specs.items()}
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    flat_paths = list(dict.fromkeys(list(var_paths.values()) + list(filter_paths.values())))
    arrays_by_path = _read_flat(
        flat_paths, plan, n_base, width, global_idx, h5obj, read_fn, workers, _read_full
    )
    axes_cols = _read_axes_variables(
        {n: e for n, e in variables.items() if _has_axes(e)},
        group,
        width,
        plan,
        n_base,
        global_idx,
        _read_rows,
        _read_full,
    )

    # ---- Filters: base-level predicates over the gathered rows; record-level
    # predicates expand per link; sibling predicates join per record.
    keep_mask: np.ndarray | None = None
    for f in base_structured:
        flag = np.asarray(arrays_by_path[filter_paths[id(f)]])[mask_spatial]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    cross_planned: np.ndarray | None = None
    for f in coarse_structured:
        cf_flag_path = f["dataset"].format(group=group)
        if f["level"] == coord_key:
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
            # The planned arm's searchsorted owners and the full arm's paint
            # agree only on disjoint records (the vlen route's check, issue #452).
            _validate_link_disjoint(cf_ibeg, cf_cnt, cf_index_base, level=f["level"])
            coarse_fmask = _predicate_mask(np.asarray(cf_data[cf_flag_path]), f)
            expanded = (
                _expand_mask_at_rows(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, global_idx)
                if global_idx is not None
                else _expand_mask_to_base(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, n_base)
            )
        cross_planned = expanded if cross_planned is None else (cross_planned & expanded)

    if asset_filters:
        record_mask = _sibling_record_mask(group, asset_filters, n_records, asset_joins)
        expanded = record_mask[parent_safe] & valid
        cross_planned = expanded if cross_planned is None else (cross_planned & expanded)

    if cross_planned is not None:
        cross_masked = cross_planned[mask_spatial]
        keep_mask = cross_masked if keep_mask is None else (keep_mask & cross_masked)
    if keep_mask is not None and not keep_mask.any():
        return None

    # ---- Synthesized columns + record-level broadcasts + asset variables.
    synth_cols = _synthesize(
        synthesized_specs(variables), group, width, cnt_arr, parent_idx, within_idx, _read_full
    )
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
    asset_cols: dict[str, np.ndarray] = {}
    for asset, name_paths in asset_var_paths.items():
        join = asset_joins.get(asset)
        for name, path in name_paths.items():
            if join is None:
                vals = np.full(n_records, np.nan, dtype=np.float64)
            else:
                values_by_path, asset_matched = join
                vals = np.asarray(values_by_path[path])
                if vals.ndim > 1:
                    raise ValueError(
                        f"asset variable {name!r}: dataset {path!r} is {vals.ndim}-D; "
                        "the asset form takes no 'column' selector yet (1-D datasets only)"
                    )
                vals = vals.astype(np.promote_types(vals.dtype, np.float32))
                vals[~asset_matched] = np.nan
            asset_cols[name] = _gather_segment_at_parents(vals, parent_safe, valid)

    # ---- Assemble.
    def _take(values):
        values = values[mask_spatial]
        return values[keep_mask] if keep_mask is not None else values

    data_dict: dict[str, np.ndarray] = {}
    for col_name, path in var_paths.items():
        data_dict[col_name] = _take(
            _select_column(np.asarray(arrays_by_path[path]), path_specs[col_name][1], col_name)
        )
    for col_name, values in axes_cols.items():
        data_dict[col_name] = _take(values)
    for col_name, values in synth_cols.items():
        data_dict[col_name] = _take(values)
    for col_name, values in seg_broadcasts.items():
        data_dict[col_name] = _take(values)
    for col_name, values in asset_cols.items():
        data_dict[col_name] = _take(values)
    data_dict["leaf_id"] = _take(leaf_ids)

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


def _has_axes(entry) -> bool:
    return isinstance(entry, dict) and "axes" in entry


def _primary_record_count(data_source: dict, coord_assets: dict, group: str, read_full) -> int:
    """Record count of the coordinates level when its coordinates are joined
    from a sibling: the length of the join's primary (``left``) key."""
    asset = coord_assets["latitude"]["asset"]
    left = data_source["assets"][asset]["join"]["left"]
    first = (left[0] if isinstance(left, list) else left).format(group=group)
    return len(np.asarray(read_full([first])[first]))


def _joined_coordinates(
    coord_assets: dict, group: str, joins: dict, n_records: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Record lat/lon gathered through the sibling join; ``matched`` per record.

    A granule with no open handle for the coordinate asset fails the group:
    there is nothing to place. Unmatched records get a 0.0 placeholder that
    ``matched`` keeps out of every mask.
    """
    out = []
    matched = None
    for key in ("latitude", "longitude"):
        entry = coord_assets[key]
        join = joins.get(entry["asset"])
        if join is None:
            raise ValueError(
                f"granule carries no open sibling handle for asset {entry['asset']!r}, "
                f"which sources coordinates.{key}; was the shard map built with the "
                "paired-asset join?"
            )
        values_by_path, m = join
        vals = np.asarray(values_by_path[entry["path"].format(group=group)], dtype=np.float64)
        if vals.ndim != 1:
            raise ValueError(f"coordinates.{key}: asset dataset must be 1-D (got {vals.shape})")
        out.append(np.where(m, vals, 0.0))
        matched = m if matched is None else (matched & m)
    assert matched is not None and len(matched) == n_records
    return out[0], out[1], matched


def _read_flat(
    flat_paths, plan, n_base, width, global_idx, h5obj, read_fn, workers, read_full
) -> dict:
    """Flat base-rate datasets (plain / ``column`` variables, base filters):
    planned slices by the synthesized link, else full reads — the vlen route's
    two arms. Unlike a strided ragged link, a fixed-width link leaves no slack
    past its last record, so a full read of any other length than
    ``n_records * width`` means the declared width is wrong: it raises rather
    than trims (issue #452), as the ``axes`` arm does. The planned arm cannot
    see the dataset's length (the ``read_fn`` seam has no metadata query yet —
    the ``width: auto`` follow-up), so it checks the rows it gets back."""
    if not flat_paths:
        return {}
    if plan is None:
        out = {}
        got = read_full(flat_paths)  # an ``H5Promise`` has no ``.items()``: index by path
        for path in flat_paths:
            arr = np.asarray(got[path])
            if len(arr) != n_base:
                raise ValueError(
                    f"dataset {path!r} has {len(arr)} rows but the fixed-width link "
                    f"covers {n_base // width} records x width {width} = {n_base}; "
                    "the record link does not tile the dataset"
                )
            out[path] = arr
        return out
    _base_read_fn = read_fn
    if _base_read_fn is None:

        def _base_read_fn(path, hyperslice=None):
            if hyperslice is None:
                return h5obj.readDatasets([path])[path]
            return h5obj.readDatasets([{"dataset": path, "hyperslice": hyperslice}])[path]

    out = _read_paths_pooled(
        [(p, None) for p in flat_paths],
        lambda p, dt: execute_read_plan(plan, _base_read_fn, p, dt),
        workers,
    )
    for path, arr in out.items():
        if len(arr) != len(global_idx):
            raise ValueError(
                f"dataset {path!r} returned {len(arr)} of {len(global_idx)} planned rows; "
                "the fixed-width link does not tile the dataset"
            )
    return out


def _read_axes_variables(
    entries: dict, group: str, width: int, plan, n_base: int, global_idx, read_rows, read_full
) -> dict[str, np.ndarray]:
    """Every ``axes`` variable as a 1-D column at this read's rows.

    A record-first dataset (``axes[0] == "record"``) is sliced per planned
    run — rows ``[s // width, e // width)`` for the base slice ``[s, e)``,
    which is always record-aligned under the synthesized link — and never
    read whole on the planned arm. Any other layout cannot be row-sliced per
    record (h5coro slices dim 0), so it reads whole, with a notice, and the
    planned rows are gathered afterwards. One read per distinct
    ``(path, axes)``: two band picks on one cube share nothing but the path.
    """
    out: dict[str, np.ndarray] = {}
    cache: dict[tuple, np.ndarray] = {}
    for name, entry in entries.items():
        path = entry["path"].format(group=group)
        axes = list(entry["axes"])
        key = (path, tuple(axes))
        if key in cache:
            out[name] = cache[key]
            continue
        if axes[0] == "record" and plan is not None:
            parts = [
                flatten_axes(read_rows(path, s // width, e // width), axes, width, name)
                for s, e in plan.base_slices
            ]
            flat = np.concatenate(parts)
            if len(flat) != len(global_idx):
                raise ValueError(
                    f"variable {name!r}: dataset {path!r} returned {len(flat)} of "
                    f"{len(global_idx)} planned rows; the fixed-width link does not tile it"
                )
        else:
            if axes[0] != "record":
                logger.info(
                    f"variable {name!r}: axes {axes!r} is not record-first, so {path!r} "
                    "is read whole (per-record row slicing needs the record axis at dim 0)"
                )
            flat = flatten_axes(read_full([path])[path], axes, width, name)
            if len(flat) != n_base:
                raise ValueError(
                    f"variable {name!r}: dataset {path!r} flattens to {len(flat)} rows but "
                    f"the coordinates level has {n_base // width} records of width {width}"
                )
            if global_idx is not None:
                flat = flat[global_idx]
        cache[key] = flat
        out[name] = flat
    return out


def _synthesize(
    specs: dict, group: str, width: int, cnt_arr, parent_idx, within_idx, read_full
) -> dict[str, np.ndarray]:
    """The three synthesis kinds over this read's gather map."""
    out: dict[str, np.ndarray] = {}
    for name, entry in specs.items():
        kind = entry["synthesize"]
        if kind == "index":
            out[name] = np.asarray(within_idx, dtype=np.int64)
            continue
        start_path = entry["start"].format(group=group)
        if kind == "linspace":
            stop_path = entry["stop"].format(group=group)
            sp = read_full([start_path, stop_path])
            out[name] = synthesize_linspace(
                np.asarray(sp[start_path]),
                np.asarray(sp[stop_path]),
                cnt_arr,
                parent_idx,
                within_idx,
            )
            continue
        offsets = entry["offsets"]
        if isinstance(offsets, str):
            off_path = offsets.format(group=group)
            data = read_full([start_path, off_path])
            table = np.asarray(data[off_path])
        else:
            data = read_full([start_path])
            table = offsets
        out[name] = synthesize_offsets(
            np.asarray(data[start_path]),
            table,
            float(entry.get("scale", 1)),
            width,
            parent_idx,
            within_idx,
        )
    return out
