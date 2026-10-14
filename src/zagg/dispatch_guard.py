"""Dispatch circuit breaker for the Lambda fan-out (issue #652).

The 2026-10-12 v4 build released 1,526 leaf invokes at once into a cold
prefix; S3 write contention stretched every window's write phase until 1,522
of them hit the 900 s wall, and the tail then swept a run that had mostly
failed (post-mortem: issue #651). The breaker reads every completed result
and STOPS DISPATCHING on either rule:

- ``timeout-ratio`` -- more than ``timeout_ratio`` of the last ``window``
  results timed out;
- ``store-fatal`` -- at least ``store_fatal`` store-fatal results among the
  run's first ``store_fatal_first``.

The guard paces at the run's full worker width (the concurrency preflight's
clamp), as the fixed window did before it; a dispatch ramp was measured and
set aside (issue #652, ruled 2026-10-14).

A trip is sticky. Units not yet started are then *refused* -- they resolve
``not-dispatched`` instead of invoking -- and the dispatchers skip the tail
legs (staged sweep, families pass, Icechunk finalize). In-flight invokes
cannot be recalled; they finish and their results still land.

One guard watches every Lambda transport (issue #652 phase 6), fed at the
seam each already has: the Event transport's status poller records each
status and asks :meth:`DispatchGuard.room` for the next batch; every pool
fan-out -- the async and sync transports and the temporal fan-out through
:func:`zagg.dispatch.dispatch`, the raster fan-out and the facade's sync pool
directly -- wraps its work in :meth:`DispatchGuard.gate`, which records each
unit on its pool thread under the guard's lock and refuses the rest after a
trip. No I/O, no clock.
The store-fatal rule reads ``store_error_class`` when a status carries it
(issue #651, PR #658) and otherwise classifies the error string with
:func:`zagg.dispatch.classify_store_error`, the worker's own classifier.
``error_class`` is read only as a fallback for status objects written before
the #658 rename, and only when it holds one of the three store classes -- in
zagg that name also carries telemetry's exception type (``"ValueError"``),
which must not short-circuit the classifier.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, fields
from typing import Any, Callable, Iterable

from zagg.dispatch import (
    BENIGN_ERRORS,
    STORE_BENIGN,
    STORE_FATAL,
    STORE_TRANSIENT,
    classify_store_error,
)

#: The values ``store_error_class`` (or a pre-#658 ``error_class``) may hold.
_STORE_CLASSES = frozenset({STORE_FATAL, STORE_TRANSIENT, STORE_BENIGN})

#: Breaker rule names, as they land in ``summary["breaker"]["rule"]``.
RULE_TIMEOUT_RATIO = "timeout-ratio"
RULE_STORE_FATAL = "store-fatal"

#: The ``outcome`` a shard the breaker refused to dispatch resolves with.
NOT_DISPATCHED = "not-dispatched"

#: How the async / raster result channels (issue #151) report a unit whose
#: result object never landed by the poll deadline: the worker timed out, was
#: OOM-killed or crashed -- the async twin of the Event ``failed-unknown`` drop,
#: so the ``timeout-ratio`` rule counts it. ``runner`` builds the message off it.
NO_RESULT_PREFIX = "no worker result within"


#: How a trip's ``tail_sweep_short`` reason starts. A sweep folds leaves that
#: exist and never writes the refused ones, so the "regenerate with
#: ``python -m zagg.sweep``" hint is printed only beside a reason that does
#: not start with this: a trip's remedy is a re-dispatch, carried in its text.
BREAKER_SHORT_PREFIX = "the dispatch circuit breaker tripped"


def guard_pacing(where: str) -> str:
    """``summary["dispatch_pacing"]`` / ``handle.dispatch_pacing`` (issue #652).

    Every Lambda fan-out -- each transport, raster and temporal included --
    runs at its full worker width under the circuit breaker; ``where`` names
    the path (``"invocation='async'"``, ``"raster fan-out"``, ...) so a
    summary says which seam watched it.
    """
    return f"full worker width, circuit breaker on ({where}) (issue #652)"


@dataclass(frozen=True)
class GuardConfig:
    """The breaker knobs (issue #652 defaults; every one a knob).

    Built from the ``dispatch`` block -- ``{"breaker": {...}}`` with the field
    names below -- by :meth:`from_dict`, which rejects unknown keys, wrong
    types and out-of-range values at submission.
    """

    window: int = 50
    timeout_ratio: float = 0.20
    store_fatal: int = 3
    store_fatal_first: int = 20

    _BREAKER = ("window", "timeout_ratio", "store_fatal", "store_fatal_first")
    _INTS = frozenset({"window", "store_fatal", "store_fatal_first"})

    @classmethod
    def from_dict(cls, block: dict | None) -> GuardConfig:
        """Validate a ``dispatch`` block; ``None`` / ``{}`` is the defaults."""
        if block is not None and not isinstance(block, dict):
            raise ValueError(f"dispatch must be a mapping (got {block!r})")
        block = dict(block or {})
        if "ramp" in block:
            raise ValueError(
                "dispatch.ramp was removed (issue #652, 2026-10-14 ruling): the guard paces "
                "at the full worker width; the rolling window is breaker.window"
            )
        sub = block.pop("breaker", None)
        sub = {} if sub is None else sub
        if not isinstance(sub, dict):
            raise ValueError(f"dispatch.breaker must be a mapping (got {sub!r})")
        if block:
            raise ValueError(f"dispatch: unknown key(s) {sorted(block)} (expected breaker)")
        if "wall_fraction" in sub:
            raise ValueError("breaker.wall_fraction was removed with the ramp (issue #652)")
        unknown = set(sub) - set(cls._BREAKER)
        if unknown:
            raise ValueError(f"dispatch.breaker: unknown key(s) {sorted(unknown)}")
        cfg = cls(**{k: v for k, v in sub.items() if v is not None})
        for f in fields(cls):
            v = getattr(cfg, f.name)
            integer = f.name in cls._INTS
            kinds = int if integer else (int, float)
            # nan / inf pass every comparison below and switch the rule off.
            if isinstance(v, bool) or not isinstance(v, kinds) or not math.isfinite(v) or v <= 0:
                kind = "an integer" if integer else "a number"
                raise ValueError(f"dispatch: {f.name} must be {kind} > 0 (got {v!r})")
        if cfg.timeout_ratio >= 1.0:  # at 1.0 the rule can never fire
            raise ValueError(
                f"dispatch: breaker.timeout_ratio must be < 1.0 (got {cfg.timeout_ratio!r})"
            )
        if cfg.store_fatal > cfg.store_fatal_first:
            raise ValueError(
                f"dispatch: breaker.store_fatal {cfg.store_fatal} exceeds "
                f"breaker.store_fatal_first {cfg.store_fatal_first}, so it can never trip"
            )
        return cfg

    def as_dict(self) -> dict:
        """The nested ``dispatch`` block this config round-trips to."""
        return {"breaker": {k: getattr(self, k) for k in self._BREAKER}}


def parse_knobs(pairs: Iterable[str] | None) -> dict | None:
    """``["breaker.window=20", "breaker.timeout_ratio=0.3"]`` -> the nested ``dispatch`` block.

    The CLI's ``--dispatch group.key=value`` spelling (repeatable); values are
    numbers (``int`` when they parse as one, else ``float``). Shape only --
    :meth:`GuardConfig.from_dict` owns the key and range validation. ``None``
    / empty means "no knobs".
    """
    if not pairs:
        return None
    block: dict = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        group, dot, name = key.partition(".")
        if not (sep and dot and group and name):
            raise ValueError(f"--dispatch expects group.key=value (got {pair!r})")
        try:
            value: int | float = int(raw)
        except ValueError:
            try:
                value = float(raw)
            except ValueError:
                raise ValueError(f"--dispatch {key}: not a number (got {raw!r})") from None
        block.setdefault(group, {})[name] = value
    return block


@dataclass(frozen=True)
class ResultRecord:
    """One completed result, reduced to what the rules read."""

    timeout: bool
    store_fatal: bool


def classify_result(result: dict) -> ResultRecord:
    """Reduce a completed result dict to a :class:`ResultRecord`.

    A timeout is the sync transport's ``timeout`` flag, the Event transport's
    ``failed-unknown`` drop outcome (no status object by the deadline -- a
    worker killed at the wall writes none), or Lambda's own ``Task timed
    out`` text. Store-fatal reads ``store_error_class`` off the result, then
    its body (PR #658), then a pre-#658 ``error_class`` holding a store class,
    and falls back to classifying the error string.
    """
    body = result.get("body") or {}
    error = result.get("error")
    timeout = bool(
        result.get("timeout")
        or result.get("outcome") == "failed-unknown"
        or (error and "Task timed out" in str(error))
        or (error and str(error).startswith(NO_RESULT_PREFIX))
    )
    # The no-data channel's text starts with its benign entry and may carry a
    # source read's 403/404 exemplar after it: benign, never store-fatal.
    failed = bool(error) and not str(error).startswith(BENIGN_ERRORS) and not timeout
    # PR #658 stamps the leaf's class as ``store_error_class`` (the ruled name).
    # ``error_class`` is the pre-#658 spelling, honored only when it holds a
    # store class: telemetry fills the same name with the exception type.
    store_class = result.get("store_error_class") or body.get("store_error_class")
    if store_class is None:
        legacy = result.get("error_class") or body.get("error_class")
        store_class = legacy if legacy in _STORE_CLASSES else None
    if store_class is None and failed:
        store_class = classify_store_error(error)
    return ResultRecord(timeout=timeout, store_fatal=failed and store_class == STORE_FATAL)


class FixedWindow:
    """A fixed pacing window with no breaker: the poller's default.

    ``max_in_flight`` shards dispatched-unresolved at once, or unbounded on
    ``None`` (``Run.attach`` registers observe-only entries). Mirrors the
    :class:`DispatchGuard` reads the poller and the summaries make --
    ``record``, ``room``, ``tripped``, ``breaker`` -- and never trips.
    """

    tripped: dict | None = None
    breaker: dict | None = None

    def __init__(self, max_in_flight: int | None):
        self._max = max_in_flight

    def record(self, result: dict) -> None:
        pass

    def room(self, queued: int, in_flight: int) -> int:
        return queued if self._max is None else max(self._max - in_flight, 0)


class DispatchGuard:
    """The circuit breaker over one run's completed results (see the module doc).

    ``workers`` is the run's fan-out width (the concurrency preflight's
    clamp), the in-flight window it paces by; ``None`` is unbounded.
    """

    def __init__(self, config: GuardConfig | None = None, *, workers: int | None = None):
        self.config = config or GuardConfig()
        self.width = workers
        self._records: list[ResultRecord] = []
        self._timed_out: set = set()  # unit keys already recorded as a timeout
        self.tripped: dict | None = None
        self._lock = threading.Lock()  # serializes :meth:`gate` records (pool threads)

    def record(self, result: dict) -> None:
        """Fold one completed result in and re-evaluate both rules."""
        cfg = self.config
        rec = classify_result(result)
        key = result.get("unit", result.get("shard_key"))
        if rec.timeout and key is not None:
            # A dropped unit re-fires once: it counts one timeout, recorded at
            # its first drop; a later ok/failed completion still counts. The
            # identity is the poller's ``unit`` (its status key, one per
            # (shard, window)), else ``shard_key``.
            if key in self._timed_out:
                return
            self._timed_out.add(key)
        self._records.append(rec)
        n = len(self._records)
        if self.tripped is not None:
            return
        if n <= cfg.store_fatal_first:
            if sum(r.store_fatal for r in self._records) >= cfg.store_fatal:
                self.tripped = self._verdict(RULE_STORE_FATAL)
                return
        if n >= cfg.window:
            last = self._records[-cfg.window :]
            if sum(r.timeout for r in last) / cfg.window > cfg.timeout_ratio:
                self.tripped = self._verdict(RULE_TIMEOUT_RATIO)

    def room(self, queued: int, in_flight: int) -> int:
        """Slots to fire now. Tripped: every queued shard, so the poller refuses them."""
        if self.tripped is not None or self.width is None:
            return queued
        return max(self.width - in_flight, 0)

    def refusal(self) -> dict:
        """``_result`` kwargs for a shard the tripped breaker refused to dispatch."""
        t = self.tripped or {}
        return {
            "status_code": None,
            "body": {},
            "error": (
                f"not dispatched: circuit breaker tripped ({t.get('rule')}) at result "
                f"{t.get('at_result')}, width {t.get('width')} (issue #652)"
            ),
            "outcome": NOT_DISPATCHED,
        }

    def refused(self, **ident) -> dict:
        """A refused unit's full result dict (``ident``: its ``shard_key`` / ``event_key``).

        The shape a pool fan-out's accumulator and run record read: no billed
        duration, never attempted (``retries: -1``, as on the Event poller).
        """
        return {**ident, **self.refusal(), "lambda_duration": 0, "retries": -1, "timeout": False}

    def gate(self, work: Callable[..., Any], refuse: Callable[..., Any]) -> Callable[..., Any]:
        """``work`` wrapped for a pool: refused after a trip, recorded as it completes.

        A unit whose pool slot opens after a trip calls ``refuse`` instead
        (both get the unit's arguments; ``refuse`` returns or raises what the
        caller reads for a refused unit). A unit that runs is recorded on its
        own pool thread the moment ``work`` returns -- or raises an exception
        carrying a result ``payload`` (``ShardError``) -- under the guard's
        lock, BEFORE that thread takes its next unit: recording later, on a
        collecting loop, would let a freed thread start the next unit against
        a ``tripped`` that is one result stale.
        """

        def run(*args, **kwargs):
            if self.tripped is not None:
                return refuse(*args, **kwargs)
            try:
                result = work(*args, **kwargs)
            except Exception as exc:
                self._record_locked(getattr(exc, "payload", None))
                raise
            self._record_locked(result)
            return result

        return run

    def _record_locked(self, result) -> None:
        if isinstance(result, dict):
            with self._lock:
                self.record(result)

    @property
    def breaker(self) -> dict | None:
        """``summary["breaker"]``: the trip, else ``None``."""
        return self.tripped

    def _verdict(self, rule: str) -> dict:
        return {"tripped": True, "rule": rule, "at_result": len(self._records), "width": self.width}


def describe(breaker: dict | None, pacing: str | None = None) -> list[str]:
    """Human lines for ``summary["breaker"]`` / ``["dispatch_pacing"]``.

    Shared by the CLI, the notebook view and the run log. ``pacing`` (see
    :func:`guard_pacing`) leads when given, so a clean run still says the
    breaker watched it.
    """
    lines = [f"Dispatch pacing: {pacing}"] if pacing else []
    if breaker:
        lines.append(
            f"Circuit breaker TRIPPED, dispatch stopped: rule {breaker.get('rule')} at result "
            f"{breaker.get('at_result')}, width {breaker.get('width')} (issue #652)"
        )
    return lines
