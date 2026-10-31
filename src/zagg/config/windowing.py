"""``output.windowing``: validation, normalization, and the per-window filters (issue #677 phase 0)."""

from datetime import datetime, timedelta, timezone

from zagg.config.accessors import get_store_layout
from zagg.config.core import PipelineConfig
from zagg.config.source import _segment_variable_names, filters_from_data_source


def _validate_windowing(config: PipelineConfig) -> None:
    """Validate the ``output.windowing`` block (issue #246, morton-hive/2).

    ``{schedule, time_field, epoch, scale?, units?, windows?}`` — the nested
    form ratified on the #246 thread. Absent/null is schedule ``none``
    (today's behavior). A present block requires the hive store layout on a
    healpix grid (windowed leaf names are a morton-hive convention, like
    ``coverage_moc``). ``quarterly`` is grammar-reserved on the mortie spec
    page but NOT implemented — rejected with a pointed message. The explicit
    windows list must be well-formed: frozen label grammar, half-open
    ``start < end``, unique labels, non-overlapping ranges. ``time_field``
    must name a declared BASE-RATE ``data_source`` variable (a flat
    ``variables`` entry or the base level's ``variables``) — a coordinate or
    a non-base (segment-rate) level column is rejected, since the stamp
    ``time_range`` is pooled from read variable columns and window membership
    is decided per observation. On the RASTER path (issue #247) membership is
    the acquisition's STAC ``datetime`` instead: ``time_field`` is optional
    (fixed to ``datetime``) and the ``epoch``/``scale``/``units`` conversion
    knobs are rejected. ``unit`` (issue #586 phase 2) picks the dispatch
    unit — ``shard`` (default: one invoke per shard emits every window its
    granules span) or ``window`` (one invoke per ``(shard, window)``); the
    raster path always dispatches per window and rejects the key.
    """
    from zagg import windows as _windows

    block = config.output.get("windowing")
    if block is None:
        return
    if not isinstance(block, dict):
        raise ValueError(
            f"output.windowing must be a mapping "
            f"{{schedule, time_field, epoch, ...}} (got {block!r})"
        )
    try:
        schedule = _windows.check_schedule(block.get("schedule", "none"))
    except ValueError as e:
        raise ValueError(f"output.windowing.schedule: {e}") from e
    if schedule == "none":
        # An inert ``schedule: none`` block is equivalent to an absent block
        # (``get_windowing`` returns ``None``, no windowed output), so it must
        # NOT require the hive/healpix layout — this early return precedes the
        # layout guards. A stray ``windows`` key is still rejected, mirroring
        # the generative-schedule check below (validation symmetry).
        if block.get("windows") is not None:
            raise ValueError(
                "output.windowing.windows only applies to schedule: explicit "
                "(schedule: none produces no windowed output)"
            )
        return
    grid = config.output.get("grid") or {}
    if (grid.get("type", "healpix")) != "healpix":
        raise ValueError(
            "output.windowing requires a healpix grid (windowed leaves are a "
            f"morton-hive convention; grid type is {grid.get('type')!r})"
        )
    if get_store_layout(config) != "hive":
        raise ValueError(
            "output.windowing requires output.store_layout: hive (window leaves "
            "are hive leaf zarrs; the flat shared store has no leaves to window)"
        )
    unit = block.get("unit")
    if unit is not None and unit not in WINDOWING_UNITS:
        raise ValueError(
            f"output.windowing.unit must be one of {WINDOWING_UNITS} (got {unit!r}): "
            f"'shard' emits every window from one invoke per shard, 'window' "
            f"dispatches one invoke per (shard, window)"
        )
    time_field = block.get("time_field")
    if (config.data_source or {}).get("reader") == "raster":
        if unit is not None:
            raise ValueError(
                "output.windowing.unit does not apply to raster pipelines: window "
                "membership is decided per acquisition at dispatch, one unit per "
                "(shard, window) (drop the key)"
            )
        # Raster window membership is the acquisition's STAC ``datetime``,
        # decided at dispatch (issue #247, ratified): there is no
        # per-observation timestamp column, so ``time_field`` is optional and
        # fixed — the manifest records the resolved field, and a property-name
        # knob (e.g. ``start_datetime`` for interval-typed items) can be added
        # later without breaking any existing manifest.
        if time_field is not None and time_field != "datetime":
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is not configurable "
                "on the raster path: window membership is the acquisition's STAC "
                "datetime (drop the key, or set it to 'datetime')"
            )
        # The epoch/scale/units knobs describe a dataset-unit time_field
        # conversion; STAC datetimes are already ISO-8601 UTC instants, so
        # there is nothing to configure — reject rather than record a
        # misleading manifest temporal block (get_windowing resolves the
        # fixed encoding).
        knobs = [k for k in ("epoch", "scale", "units") if block.get(k) is not None]
        if knobs:
            raise ValueError(
                f"output.windowing.{knobs[0]} does not apply to raster "
                "pipelines: STAC datetimes are ISO-8601 UTC instants (drop the "
                "key)"
            )
        _validate_windowing_windows(block, schedule)
        return
    if not isinstance(time_field, str) or not time_field:
        raise ValueError(
            "output.windowing.time_field is required: the per-observation "
            "timestamp column that decides window membership"
        )
    # ``time_field`` must be a BASE-RATE variable column, i.e. a
    # ``data_source.variables`` entry (the base level reads its columns from
    # there too — ``_validate_overviews`` forbids a base-level ``variables``
    # mapping). The stamp ``time_range`` (a windowed leaf's headline truth) is
    # pooled from the read VARIABLE columns (``col_arrays`` in the worker),
    # never from ``coordinates`` — so a coordinate ``time_field`` would filter
    # yet silently drop the stamp, and a lat/lon coordinate is not a timestamp
    # anyway. A non-base (segment-rate) level column is rejected too: window
    # membership would be decided per whole segment, not per observation,
    # which is not supported this round (see ``window_time_filters``).
    ds = config.data_source or {}
    declared = set(ds.get("variables") or {})
    if time_field not in declared:
        if time_field in set(ds.get("coordinates") or {}):
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is a data_source "
                f"coordinate; the stamp time_range is pooled from read variable "
                f"columns and a coordinate is not read as a timestamp. Declare it "
                f"under data_source.variables (or the base level's variables)."
            )
        if time_field in _segment_variable_names(ds):
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is declared on a "
                f"non-base (segment-rate) level; segment-rate window membership is "
                f"not supported yet (whole segments would be kept or dropped). "
                f"Declare it on the base level or data_source.variables."
            )
        raise ValueError(
            f"output.windowing.time_field {time_field!r} is not a declared "
            f"base-rate data_source column (one of {sorted(declared)}); the "
            f"worker filters per observation on variable columns it reads"
        )
    entry = (ds.get("variables") or {}).get(time_field)
    if isinstance(entry, dict) and ("axes" in entry or "synthesize" in entry):
        # The window predicate is a base-rate structured filter on the
        # field's dataset (``window_time_filters``); an N-D ``axes`` dataset
        # or a synthesized column has no such 1-D dataset to filter (issue
        # #678). A fixed-width product's clock is record-rate, which the
        # segment-rate rule above already declines.
        raise ValueError(
            f"output.windowing.time_field {time_field!r} is an 'axes' or 'synthesize' "
            f"variable; the window filter needs a flat base-rate dataset path"
        )
    epoch = block.get("epoch")
    if epoch is None:
        raise ValueError(
            "output.windowing.epoch is required: the dataset's zero time as an "
            "ISO-8601 UTC instant (e.g. '2018-01-01T00:00:00Z' for ICESat-2 "
            "delta_time)"
        )
    try:
        parsed_epoch = _windows.parse_utc(epoch)
    except ValueError as e:
        raise ValueError(f"output.windowing.epoch: {e}") from e
    # Validate the RENDERED epoch, not the parsed one (issue #390). get_windowing
    # canonicalizes through windows.iso_utc (timespec="seconds"), so a sub-second
    # epoch parses clean here and is then silently truncated at the one place it
    # is consumed — shifting EVERY window conversion by the dropped fraction. The
    # predicate calls the renderer and round-trips it rather than repeating the
    # truncation rule, so the two cannot drift (the PR #367 pattern).
    # The message renders parsed_epoch rather than repr-ing the declared value:
    # yaml.safe_load types an ISO timestamp scalar as a datetime, which parse_utc
    # passes through, so repr would hand the author a stdlib constructor call
    # instead of the value they wrote. The drop is reported in microseconds
    # because that is exactly what iso_utc discards and nothing else: an integer,
    # so no scientific notation for the 1 µs case, and never negative.
    rendered_epoch = _windows.iso_utc(parsed_epoch)
    if _windows.parse_utc(rendered_epoch) != parsed_epoch:
        raise ValueError(
            f"output.windowing.epoch {parsed_epoch.isoformat()!r} carries sub-second "
            f"precision that the canonical whole-second rendering drops: it is "
            f"recorded as {rendered_epoch!r}, which would shift every window "
            f"conversion by {parsed_epoch.microsecond} µs — declare the epoch at "
            f"second precision"
        )
    scale = block.get("scale") or "utc"
    if scale not in _windows.EPOCH_SCALES:
        raise ValueError(
            f"output.windowing.scale must be one of {_windows.EPOCH_SCALES} (got {scale!r})"
        )
    units = block.get("units") or "seconds"
    if units not in _windows.UNIT_SECONDS:
        raise ValueError(
            f"output.windowing.units must be one of {tuple(_windows.UNIT_SECONDS)} (got {units!r})"
        )
    _validate_windowing_windows(block, schedule)


#: Width of the window a ``{label, timestamp}`` point entry desugars to (#355).
#: One second is the grammar's native resolution — boundaries render at
#: ``timespec="seconds"`` throughout — and keeps membership off float equality.
_POINT_WINDOW_WIDTH = timedelta(seconds=1)


def _explicit_window_bounds(entry: dict) -> tuple[datetime, datetime]:
    """The half-open UTC ``[start, end)`` of one explicit ``windows`` entry.

    Two forms, discriminated on keys and never mixed (issue #355): the range
    ``{label, start, end}``, and the point sugar ``{label, timestamp}`` ->
    ``[t, t + 1s)``. Either form refuses keys beyond its own — a dead or
    typo'd key has no semantic effect, so it fails loud rather than riding
    along. Bounds come back at declared precision; :func:`get_windowing`
    renders them through ``windows.iso_utc`` (``timespec="seconds"``), so a
    sub-second ``timestamp`` normalizes to the whole second containing it —
    truncation is monotone, so it can neither move ``t`` out of its own window
    nor manufacture an overlap. *Range* bounds truncate identically, each to
    the second containing it, so a fractional ``start``/``end`` silently
    retimes that edge of the window; a range is never widened to recover the
    truncated fraction. The one refusal is total collapse — bounds that render
    to the same second would dispatch as an empty window, so it is rejected
    here, on the rendered values. See ``docs/hive_layout.md`` for the
    bound-truncation edge, the same-second collision and the silent-miss edge.
    """
    from zagg import windows as _windows

    ranged = sorted({"start", "end"} & set(entry))
    if "timestamp" in entry:
        if ranged:
            raise ValueError(
                f"explicit window entry carries both timestamp and {', '.join(ranged)}; each "
                f"entry declares exactly one of {{timestamp}} or {{start, end}} "
                f"(got {entry!r})"
            )
        # `width` is documented as the future escape hatch but does not exist
        # yet: accepting it silently would hand back a 1 s window and a clean
        # validate_config.
        extra = sorted(set(entry) - {"label", "timestamp"}, key=repr)
        if extra:
            raise ValueError(
                f"explicit point window entry carries unsupported key(s) "
                f"{', '.join(map(repr, extra))}; the point form is "
                f"{{label, timestamp}} (got {entry!r})"
            )
        point = _windows.parse_utc(entry["timestamp"])
        return point, point + _POINT_WINDOW_WIDTH
    # repr, not str: YAML 1.1 resolves bare `on`/`no`/numerals to bool/int
    # keys, which are neither sortable against strings nor joinable — the
    # TypeError would escape _validate_windowing_windows' ValueError wrapper
    # and lose the `output.windowing.windows:` prefix. repr also shows the
    # reader that the key came back coerced.
    extra = sorted(set(entry) - {"label", "start", "end"}, key=repr)
    if extra:
        raise ValueError(
            f"explicit range window entry carries unsupported key(s) "
            f"{', '.join(map(repr, extra))}; the range form is "
            f"{{label, start, end}} (got {entry!r})"
        )
    start = _windows.parse_utc(entry["start"])
    end = _windows.parse_utc(entry["end"])
    if start < end and _windows.iso_utc(start) == _windows.iso_utc(end):
        # Each bound truncates to the second containing it (windows.iso_utc,
        # timespec="seconds"), so a range with both bounds inside one second
        # renders to a `ge x` / `lt x` pair that silently matches nothing. The
        # predicate above calls the renderer rather than repeating its
        # truncation, so the two cannot drift.
        # Refuse rather than widen — the point form is the spelling for
        # one-second intent. Only this total collapse is refused: a fractional
        # bound in a *different* second merely retimes that edge (documented in
        # docs/hive_layout.md).
        raise ValueError(
            f"explicit window range is empty after truncation to whole-second "
            f"granularity: start {entry['start']!r} and end {entry['end']!r} "
            f"both render as {_windows.iso_utc(start)!r}; use the point form "
            f"{{label, timestamp}} for a one-second window"
        )
    return start, end


def _validate_windowing_windows(block: dict, schedule: str) -> None:
    """Validate the ``windows`` list half of ``output.windowing`` (issue #246).

    Shared by the point-pipeline and raster branches of
    :func:`_validate_windowing`: frozen label grammar, half-open
    ``start < end``, unique labels, non-overlapping ranges — required for
    ``schedule: explicit``, rejected otherwise. Point entries
    (``{label, timestamp}``, issue #355) are desugared by
    :func:`_explicit_window_bounds` first, so every check below runs on ordinary
    bounds and composes unchanged.
    """
    from zagg import windows as _windows

    declared_windows = block.get("windows")
    if schedule != "explicit":
        if declared_windows is not None:
            raise ValueError(
                f"output.windowing.windows only applies to schedule: explicit "
                f"(the {schedule} schedule derives its windows from labels)"
            )
        return
    if not isinstance(declared_windows, list) or not declared_windows:
        raise ValueError(
            "output.windowing.windows is required for schedule: explicit — a "
            "non-empty list of {label, start, end} (or {label, timestamp}) entries"
        )
    seen: dict = {}
    for entry in declared_windows:
        if not isinstance(entry, dict) or not (
            "label" in entry and ({"start", "end"} <= set(entry) or "timestamp" in entry)
        ):
            raise ValueError(
                f"each explicit window must be a {{label, start, end}} or "
                f"{{label, timestamp}} mapping (got {entry!r})"
            )
        try:
            label = _windows.validate_label(entry["label"], "explicit")
            start, end = _explicit_window_bounds(entry)
        except ValueError as e:
            raise ValueError(f"output.windowing.windows: {e}") from e
        if label == _windows.SCHEDULE_NONE_TOKEN:
            # The opaque grammar admits it, but the token names the unwindowed
            # / all-time artifact everywhere (leaf_name_v3, the D23 overview
            # basename and its envelope key), so an explicit window by this
            # name would ALIAS the all-time fold — the per-window overview gets
            # overwritten and never regenerates (review finding, issue #201).
            raise ValueError(
                f"explicit window label {label!r} is the reserved schedule:none token "
                f"(SCHEDULE_NONE_TOKEN, D23) — it would alias the all-time artifact; "
                f"declare a different explicit label"
            )
        if not start < end:
            # Range form only — a desugared point is half-open by construction.
            raise ValueError(
                f"explicit window {label!r} is not half-open: start "
                f"{entry['start']!r} must precede end {entry['end']!r}"
            )
        if label in seen:
            raise ValueError(f"explicit window label {label!r} is declared twice")
        seen[label] = (start, end)
    ordered = sorted(seen.items(), key=lambda kv: kv[1][0])
    for (a, (_sa, ea)), (b, (sb, _eb)) in zip(ordered, ordered[1:]):
        if ea > sb:
            raise ValueError(
                f"explicit windows {a!r} and {b!r} overlap; windows must be "
                f"disjoint half-open ranges"
            )


#: The raster branch's fixed windowing epoch (issue #247, ratified): raster
#: window membership is the acquisition's STAC ``datetime``, already an ISO-8601
#: UTC instant, so the encoding is the identity one and is never author-declared.
#: Held as an *instant*, not a spelling — ``get_windowing`` renders it through
#: ``windows.iso_utc``, the same renderer its point branch canonicalizes the
#: declared epoch with, so the two branches cannot write different spellings of
#: the same instant if that rendering ever changes (issue #390).
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def get_windowing(config: PipelineConfig) -> dict | None:
    """The normalized temporal windowing declaration, or ``None`` (issue #246).

    ``None`` — absent block, null block, or an explicit ``schedule: none`` —
    is today's unwindowed behavior (bare leaf names, ``morton-hive/1``).
    Otherwise a normalized dict with defaults resolved::

        {"schedule", "time_field", "epoch", "scale", "units", "windows"}

    ``epoch`` and explicit-window boundaries are canonicalized to ISO-8601
    UTC strings. The ``epoch`` canonicalization is lossless on any config that
    passed ``validate_config`` — ``timespec="seconds"`` would otherwise truncate
    a sub-second epoch and shift every window conversion, so
    :func:`_validate_windowing` refuses one outright (issue #390). A config
    built without that check (``load_config_from_dict``, a hand-rolled worker
    payload) still truncates here; explicit bounds truncate either way,
    documented on :func:`_explicit_window_bounds`. ``windows`` is ``None``
    except for ``schedule: explicit``, where a ``{label, timestamp}`` point
    entry is desugared to its second-wide range (issue #355) so the normalized
    list is uniformly ``{label, start, end}``.
    On the raster branch (``reader: raster``) ``time_field`` is the fixed STAC
    ``datetime`` and ``epoch``/``scale``/``units`` are fixed to the Unix-epoch
    UTC-seconds encoding any ISO instant normalizes to, rather than
    canonicalizing a declared ``epoch`` off the block (``_validate_windowing``
    rejects those conversion knobs there); the fixed epoch is still rendered
    through the same ``windows.iso_utc`` as the declared one, so the branches
    cannot spell one instant two ways (issue #390). The same dict feeds the
    manifest temporal block (:func:`zagg.hive.build_manifest`) and the dispatch
    fan-out, so the two can never disagree.
    """
    from zagg import windows as _windows

    block = config.output.get("windowing")
    if not block or block.get("schedule", "none") == "none":
        return None
    declared = None
    if block["schedule"] == "explicit":
        declared = []
        for w in block["windows"]:
            start, end = _explicit_window_bounds(w)
            declared.append(
                {
                    "label": w["label"],
                    "start": _windows.iso_utc(start),
                    "end": _windows.iso_utc(end),
                }
            )
    if (config.data_source or {}).get("reader") == "raster":
        # Raster membership is the acquisition's STAC ``datetime`` (issue
        # #247, ratified): the manifest records the resolved field plus the
        # fixed encoding any ISO-8601 UTC instant normalizes to (UTC seconds
        # since the Unix epoch). _validate_windowing rejects the conversion
        # knobs on raster configs, so nothing here can disagree with it. The
        # epoch is *rendered* through iso_utc rather than spelled out, for the
        # reason _validate_windowing's #390 guard calls the renderer instead of
        # restating its truncation rule: a literal here would be a copy of
        # iso_utc's output format, free to drift from the spelling the point
        # branch below gives the very same instant (see _UNIX_EPOCH).
        return {
            "schedule": block["schedule"],
            "time_field": "datetime",
            "epoch": _windows.iso_utc(_UNIX_EPOCH),
            "scale": "utc",
            "units": "seconds",
            "windows": declared,
        }
    return {
        "schedule": block["schedule"],
        "time_field": block["time_field"],
        "epoch": _windows.iso_utc(_windows.parse_utc(block["epoch"])),
        "scale": block.get("scale") or "utc",
        "units": block.get("units") or "seconds",
        "windows": declared,
    }


#: ``output.windowing.unit`` values (issue #586 phase 2): ``shard`` is one
#: invoke per shard emitting every window its granules span (the default);
#: ``window`` keeps the per-``(shard, window)`` fan-out.
WINDOWING_UNITS = ("shard", "window")


def get_windowing_unit(config: PipelineConfig) -> str:
    """The windowed dispatch unit, ``"shard"`` (default) or ``"window"`` (issue #586).

    Read apart from :func:`get_windowing` on purpose: the unit is how a run
    is DISPATCHED, not what the store holds — the leaves, the manifest's
    temporal block and the D19 semantic hash (which folds the normalized
    windowing declaration, :mod:`zagg.semantics`) are byte-identical either
    way, so the knob must move none of them. Raster configs always read
    ``"window"`` (membership is per acquisition at dispatch).
    """
    if (config.data_source or {}).get("reader") == "raster":
        return "window"
    block = config.output.get("windowing") or {}
    return block.get("unit") or "shard"


def window_time_filters(config: PipelineConfig, start: float, end: float) -> list[dict]:
    """Structured filters implementing one window's ``[start, end)`` (issue #246).

    The observation-level window filter is exactly a pair of structured
    predicates on the declared ``time_field`` — ``ge start`` / ``lt end`` in
    DATASET units (converted once at dispatch) — so it rides the existing,
    pushdown-eligible filter machinery (issue #43) on every backend instead
    of a bespoke row filter. ``_validate_windowing`` restricts ``time_field``
    to a base-rate ``data_source.variables`` entry (the base level reads its
    columns from there too), so the predicate always filters at base rate
    (``level=None``, per observation); a coordinate or non-base/segment-rate
    ``time_field`` is rejected at validation and never reaches here. Appended
    to :func:`filters_from_data_source`'s normalized output by the hive write
    path, preserving any declared filters.

    A ``time_field`` declared in the ``{path, column}`` mapping form (issue
    #321) carries its column selector into both predicates — a structured
    filter takes the same ``column`` key, so a time column packed inside a 2-D
    dataset filters at base rate like any other.
    """
    windowing = get_windowing(config)
    if windowing is None:
        raise ValueError("window_time_filters requires a windowed config (output.windowing)")
    field = windowing["time_field"]
    entry = ((config.data_source or {}).get("variables") or {}).get(field)
    path, column = _variable_entry(entry)
    if not (isinstance(path, str) and path):
        raise ValueError(
            f"windowing time_field {field!r} has no base-rate dataset path in "
            f"data_source.variables — validate_config should have rejected "
            f"this configuration"
        )
    # ``column`` is emitted only for the mapping form, so the string form's
    # filter pair stays byte-identical to the pre-#321 shape.
    col = {"column": column} if column is not None else {}
    return [
        {"level": None, "dataset": path, **col, "op": "ge", "value": float(start)},
        {"level": None, "dataset": path, **col, "op": "lt", "value": float(end)},
    ]


def _variable_entry(entry) -> tuple[str | None, int | None]:
    """``(path, column)`` of one ``data_source.variables`` entry, mapping form or not.

    The read path's :func:`zagg.processing.read._variable_specs` normalizes the
    whole block; this is the single-entry form for config-side consumers that
    hold one name (currently the windowing time_field).
    """
    if isinstance(entry, str):
        return entry, None
    if isinstance(entry, dict):
        col = entry.get("column")
        return entry.get("path"), None if col is None else int(col)
    return None, None


def windowed_cell_config(config: PipelineConfig, window: dict) -> tuple[PipelineConfig, dict]:
    """One work unit's config with its window filter injected (issue #246).

    Returns ``(config, windowing)``: a per-unit config copy whose normalized
    filter list gains the :func:`window_time_filters` pair for ``window``'s
    dataset-unit ``[start, end)`` (declared filters preserved — the explicit
    list wins over ``quality_filter`` sugar, so normalize-then-append), plus
    the normalized windowing declaration. Raises if the config declares no
    ``output.windowing`` — a dispatched window without one is dispatcher/
    config drift, never guessed around.
    """
    from dataclasses import replace

    windowing = get_windowing(config)
    if windowing is None:
        raise ValueError(
            "a window was dispatched but the config declares no output.windowing "
            "block — dispatcher/config drift, refusing to guess the time_field"
        )
    ds = dict(config.data_source)
    ds["filters"] = filters_from_data_source(ds) + window_time_filters(
        config, window["start"], window["end"]
    )
    return replace(config, data_source=ds), windowing
