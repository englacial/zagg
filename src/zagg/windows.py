"""Temporal window primitives for ``morton-hive/2`` (issue #246, D13/D15).

Pure functions implementing the FROZEN window-label grammar and boundary
semantics recorded on the mortie spec page
(https://github.com/espg/mortie/issues/62#issuecomment-4986809092):

- Windowed leaf names are ``{full_id}_{window}.zarr`` — underscore separator,
  **parse rule: split on the first** ``_``. The underscore never appears in
  morton decimal ids (sign + digits) nor in window labels (charset below), so
  the split is unambiguous.
- Generative schedules have fixed ISO-derived, hyphen-free labels —
  ``yearly`` = ``YYYY``, ``monthly`` = ``YYYYMM``, ``daily`` = ``YYYYMMDD`` —
  so **lexicographic order = chronological order** within a store.
  ``quarterly`` (``YYYYQ[1-4]``) is grammar-reserved, NOT implemented.
- Explicit-list labels are opaque, ``[0-9A-Za-z-]{1,32}`` (no ``_`` by
  construction); their ranges are declared per label in the store manifest and
  never parsed out of the label itself.
- **Window boundaries are UTC calendar terms, half-open** ``[start, end)``,
  regardless of the store's native time encoding.

UTC <-> dataset-time conversion (ratified on the #246 thread): stdlib
``datetime`` has no leap-second support (it cannot even represent ``:60``),
and an exact conversion needs a maintained leap-second table (astropy-sized —
rejected as a dependency). Instead :func:`utc_to_offset` /
:func:`offset_to_utc` use the fixed current scale offsets
(:data:`GPS_MINUS_UTC` / :data:`TAI_MINUS_UTC`, correct since 2017-01-01):

- ``scale: "utc"`` — the field counts (nominal) UTC seconds since the epoch;
  the naive ``datetime`` difference is the value.
- ``scale: "gps"`` / ``"tai"`` — the field counts CONTINUOUS scale seconds
  since a UTC-labeled epoch instant. For an epoch **on/after 2017-01-01**
  (e.g. the ICESat-2 ATLAS SDP epoch, 2018-01-01T00:00:00Z) the scale-UTC
  offset is identical at both ends and cancels — the naive difference is
  exact until a future leap second is declared. For an epoch **before**
  2017-01-01 the scale offset at the epoch is taken as 0 — exact for the
  scales' own epochs (GPS: 1980-01-06, TAI: 1958-01-01), where the offset is
  0 by definition; an arbitrary pre-2017 epoch inherits up to the full
  current offset as error.

**Documented tolerance**: window boundaries are accurate to <= 1 leap second
(none declared since 2017-01-01; a future one shifts a boundary by 1 s —
negligible against daily/monthly/yearly windows). If exact second-scale
boundaries are ever needed, these two functions are the contained swap point
for an astropy-backed conversion.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

#: Generative window schedules: labels derive from the boundary instant and
#: the manifest stays static as data accrues (D15).
GENERATIVE_SCHEDULES = ("yearly", "monthly", "daily")
#: All implemented schedules (``none`` = no temporal partitioning, bare leaf
#: names — a ``morton-hive/1`` store *is* a ``/2`` store with this schedule).
SCHEDULES = ("none", *GENERATIVE_SCHEDULES, "explicit")
#: Grammar-reserved on the mortie spec page (``YYYYQ[1-4]``), NOT implemented:
#: validation rejects it with a pointed message rather than half-supporting it.
RESERVED_SCHEDULES = ("quarterly",)

#: GPS - UTC in seconds — constant since the 2017-01-01 leap second.
GPS_MINUS_UTC = 18
#: TAI - UTC in seconds (TAI = GPS + 19 s, a fixed relation).
TAI_MINUS_UTC = GPS_MINUS_UTC + 19
#: Supported dataset time scales for the epoch conversion.
EPOCH_SCALES = ("utc", "gps", "tai")
#: Supported dataset time units -> seconds multiplier.
UNIT_SECONDS = {"seconds": 1.0, "days": 86400.0}

#: First instant of the current constant-offset era (the 2017-01-01 leap
#: second is the most recent one declared).
_OFFSET_ERA = datetime(2017, 1, 1, tzinfo=timezone.utc)

#: Explicit-list label charset (frozen): opaque, no ``_`` by construction —
#: the superset grammar every window label (generative included) satisfies.
_LABEL_RE = re.compile(r"^[0-9A-Za-z-]{1,32}$")
_GENERATIVE_RE = {
    "yearly": re.compile(r"^\d{4}$"),
    "monthly": re.compile(r"^\d{6}$"),
    "daily": re.compile(r"^\d{8}$"),
}


def check_schedule(schedule: str) -> str:
    """Validate a schedule name; returns it. Reserved names get a pointed reject."""
    if schedule in RESERVED_SCHEDULES:
        raise ValueError(
            f"schedule {schedule!r} is grammar-reserved on the morton-hive/2 spec "
            f"but not implemented; use one of {GENERATIVE_SCHEDULES} or an explicit "
            f"windows list"
        )
    if schedule not in SCHEDULES:
        raise ValueError(f"unknown window schedule {schedule!r} (one of {SCHEDULES})")
    return schedule


def parse_utc(value) -> datetime:
    """Parse an ISO-8601 string (or pass through a ``datetime``) as aware UTC.

    A naive input is taken AS UTC (window boundaries are UTC-defined); an
    aware input is converted. ``Z`` suffixes are accepted.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"{value!r} is not an ISO-8601 datetime: {e}") from e
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """Canonical ISO-8601 UTC rendering (seconds precision, ``+00:00`` offset)."""
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def window_label(dt: datetime, schedule: str) -> str:
    """The generative-schedule label of the window containing ``dt`` (UTC)."""
    check_schedule(schedule)
    dt = parse_utc(dt)
    if schedule == "yearly":
        return f"{dt.year:04d}"
    if schedule == "monthly":
        return f"{dt.year:04d}{dt.month:02d}"
    if schedule == "daily":
        return f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"
    raise ValueError(f"schedule {schedule!r} has no generative labels")


def validate_label(label: str, schedule: str = "explicit") -> str:
    """Validate a window label against its schedule's grammar; returns it.

    Generative labels must match their fixed-width digit form (and decode to a
    real calendar instant — checked by :func:`window_range`); explicit labels
    the frozen opaque charset ``[0-9A-Za-z-]{1,32}`` (``_`` excluded by
    construction, so the leaf-name split stays unambiguous).
    """
    check_schedule(schedule)
    pattern = _GENERATIVE_RE.get(schedule, _LABEL_RE)
    if not isinstance(label, str) or not pattern.match(label):
        raise ValueError(
            f"window label {label!r} does not match the {schedule} schedule's "
            f"grammar ({pattern.pattern}; frozen on the morton-hive/2 spec)"
        )
    return label


def window_range(label: str, schedule: str, windows: list[dict] | None = None):
    """The half-open UTC ``[start, end)`` of one window.

    Generative labels decode arithmetically. Explicit labels are OPAQUE: they
    resolve only through ``windows`` — the manifest/config-declared
    ``[{label, start, end}, ...]`` list — never by parsing the label.
    """
    validate_label(label, schedule)
    if schedule == "yearly":
        start = datetime(int(label), 1, 1, tzinfo=timezone.utc)
        return start, datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
    if schedule == "monthly":
        y, m = int(label[:4]), int(label[4:6])
        start = datetime(y, m, 1, tzinfo=timezone.utc)  # raises on month 0/13+
        y2, m2 = (y + 1, 1) if m == 12 else (y, m + 1)
        return start, datetime(y2, m2, 1, tzinfo=timezone.utc)
    if schedule == "daily":
        start = datetime(int(label[:4]), int(label[4:6]), int(label[6:8]), tzinfo=timezone.utc)
        return start, start + timedelta(days=1)
    if schedule == "explicit":
        for w in windows or []:
            if w.get("label") == label:
                return parse_utc(w["start"]), parse_utc(w["end"])
        raise ValueError(
            f"explicit window label {label!r} is not declared (labels are opaque; "
            f"their ranges come only from the declared windows list)"
        )
    raise ValueError(f"schedule {schedule!r} has no windows")


def windows_intersecting(
    start: datetime, end: datetime, schedule: str, windows: list[dict] | None = None
) -> list[str]:
    """Labels of every window intersecting the CLOSED instant range [start, end].

    ``start``/``end`` are observation-range endpoints (instants, both
    included); windows are half-open ``[w_start, w_end)``, so an instant
    sitting exactly on a boundary belongs to the LATER window only.
    Generative results are chronological (= lexicographic); explicit results
    keep the declared order.
    """
    check_schedule(schedule)
    start, end = parse_utc(start), parse_utc(end)
    if end < start:
        raise ValueError(f"end {iso_utc(end)} precedes start {iso_utc(start)}")
    if schedule == "explicit":
        return [
            w["label"]
            for w in windows or []
            if parse_utc(w["start"]) <= end and start < parse_utc(w["end"])
        ]
    if schedule not in GENERATIVE_SCHEDULES:
        raise ValueError(f"schedule {schedule!r} has no windows to intersect")
    labels = []
    label = window_label(start, schedule)
    last = window_label(end, schedule)
    while True:
        labels.append(label)
        if label == last:
            return labels
        _lo, hi = window_range(label, schedule)
        label = window_label(hi, schedule)


def leaf_name(full_id: str, window: str | None = None) -> str:
    """The leaf zarr basename: ``{full_id}_{window}.zarr``, or bare (D13).

    ``window`` is validated against the frozen superset charset (no ``_``),
    so the name always splits back unambiguously.
    """
    if window is None:
        return f"{full_id}.zarr"
    validate_label(window)
    return f"{full_id}_{window}.zarr"


def split_leaf_name(name: str) -> tuple[str, str | None]:
    """``(full_id, window-or-None)`` from a leaf basename — split on the FIRST ``_``.

    The frozen parse rule: morton decimal ids never contain ``_`` and window
    labels cannot (charset), so the first underscore is the one separator.
    Raises on a non-``.zarr`` name or a malformed window label.
    """
    if not name.endswith(".zarr"):
        raise ValueError(f"{name!r} is not a leaf zarr name")
    stem = name.removesuffix(".zarr")
    if "_" not in stem:
        return stem, None
    full_id, window = stem.split("_", 1)
    validate_label(window)
    return full_id, window


def epoch_offset(scale: str) -> int:
    """The CURRENT ``scale - UTC`` offset in seconds (0 / 18 / 37).

    Frozen constants, correct since 2017-01-01 (the most recent leap second);
    see the module docstring for the conversion semantics and the <= 1
    leap-second tolerance. A new scale (the "TAI hook") is one more entry.
    """
    offsets = {"utc": 0, "gps": GPS_MINUS_UTC, "tai": TAI_MINUS_UTC}
    if scale not in offsets:
        raise ValueError(f"unknown time scale {scale!r} (one of {EPOCH_SCALES})")
    return offsets[scale]


def _unit_seconds(units: str) -> float:
    if units not in UNIT_SECONDS:
        raise ValueError(f"unknown time units {units!r} (one of {tuple(UNIT_SECONDS)})")
    return UNIT_SECONDS[units]


def utc_to_offset(dt: datetime, *, epoch, scale: str = "utc", units: str = "seconds") -> float:
    """A UTC instant as dataset time: ``units`` on ``scale`` since ``epoch``.

    ``epoch`` is the dataset's zero as a UTC instant (ISO string or
    ``datetime``). See the module docstring for the fixed-offset semantics
    and tolerance; e.g. ICESat-2 ``delta_time`` is
    ``scale="gps", epoch="2018-01-01T00:00:00Z"`` (the ATLAS SDP epoch).
    """
    epoch_dt = parse_utc(epoch)
    offset = epoch_offset(scale)  # validates the scale even when it cancels
    seconds = (parse_utc(dt) - epoch_dt).total_seconds()
    if scale != "utc" and epoch_dt < _OFFSET_ERA:
        seconds += offset
    return seconds / _unit_seconds(units)


def offset_to_utc(value: float, *, epoch, scale: str = "utc", units: str = "seconds") -> datetime:
    """Inverse of :func:`utc_to_offset`: dataset time -> aware UTC instant."""
    epoch_dt = parse_utc(epoch)
    offset = epoch_offset(scale)  # validates the scale even when it cancels
    seconds = float(value) * _unit_seconds(units)
    if scale != "utc" and epoch_dt < _OFFSET_ERA:
        seconds -= offset
    return epoch_dt + timedelta(seconds=seconds)


__all__ = [
    "EPOCH_SCALES",
    "GENERATIVE_SCHEDULES",
    "GPS_MINUS_UTC",
    "RESERVED_SCHEDULES",
    "SCHEDULES",
    "TAI_MINUS_UTC",
    "UNIT_SECONDS",
    "check_schedule",
    "epoch_offset",
    "iso_utc",
    "leaf_name",
    "offset_to_utc",
    "parse_utc",
    "split_leaf_name",
    "utc_to_offset",
    "validate_label",
    "window_label",
    "window_range",
    "windows_intersecting",
]
