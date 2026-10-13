"""Dispatch ramp + circuit breaker for the Event fan-out (issue #652).

The 2026-10-12 v4 build released 1,526 leaf invokes at once into a cold
prefix; S3 write contention stretched every window's write phase until 1,522
of them hit the 900 s wall, and the tail then swept a run that had mostly
failed (post-mortem: issue #651). Two mechanisms, one object, consulted at
the one seam that decides what fires next
(:meth:`zagg.client_transport.StatusPoller._dispatch_up_to_window`):

- **Ramp** -- the in-flight width starts at ``start`` and multiplies by
  ``factor`` up to ``cap`` (never above the run's ``workers``) once the last
  ``window`` results dispatched at the current width (a result's
  ``dispatch_width`` stamp; unstamped, those completed since the width took
  effect) have a median write phase (status ``timings``: ``write + column + icechunk``) within
  ``tolerance`` x the baseline, the median of the run's first ``window``
  write phases. The first wave IS the canary: it is just the ramp's first
  step. With the ruled defaults (500 -> 1,000, one step) the first step's
  samples are the baseline, so that step is gated by the breaker alone.
- **Breaker** -- over the same rolling window of completed results:
  ``timeout-ratio`` (more than ``timeout_ratio`` of the last ``window``
  timed out) and ``store-fatal`` (at least ``store_fatal`` store-fatal
  results among the first ``store_fatal_first``) STOP DISPATCHING; a
  ``wall-margin`` hold (median wall over ``wall_fraction`` x the function
  timeout) stops widening while it lasts and keeps dispatching.

A trip is sticky. Queued shards are then released for *refusal* -- the
poller resolves them ``not-dispatched`` instead of invoking -- and the
dispatchers skip the tail legs (staged sweep, families pass, Icechunk
finalize). In-flight Event invokes cannot be recalled; they finish and their
results still land.

Pure: no I/O, no clock, no threads. The poller feeds completed result dicts
(its own v1-shaped ``_result``) through :meth:`DispatchGuard.record` and asks
:meth:`DispatchGuard.room` for the next batch; one thread owns an instance.
The store-fatal rule reads ``error_class`` when a status carries it (issue
#651, PR #658) and otherwise classifies the error string with the same
classifier.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, fields

from zagg import dispatch as _dispatch
from zagg.dispatch import BENIGN_ERRORS

#: The worker's own store-error classifier and its fatal class (issue #651,
#: PR #658) once they are on main; the fallback below until then.
#: TODO(#658): drop ``_classify_fallback`` when ``zagg.dispatch`` carries
#: ``classify_store_error`` -- it mirrors that classifier's string path
#: (benign, credentials, S3 code, status, refusal tokens) verbatim, dropping
#: only the exception-type / ``from``-chain checks a status string has no
#: object for.
STORE_FATAL: str = getattr(_dispatch, "STORE_FATAL", "store-fatal")
_STATUS_RE = re.compile(r"(?:status(?: code)?|http|error)\W{0,3}([45]\d\d)\b", re.I)
_TRANSIENT_TOKENS = (
    "requesttimeout",
    "slowdown",
    "internalerror",
    "serviceunavailable",
    "too many requests",
)
_FATAL_TOKENS = (
    "got invalid",
    "malformed",
    "error parsing",
    "failed to parse",
    "bad request",
    "invalidargument",
    "invalid argument",
    "nosuchbucket",
    "no such bucket",
    "nosuchkey",
    "permission denied",
    "unauthenticated",
    "forbidden",
    "refusing to",
    "gate refused",
    "spec §",
)


def _classify_fallback(error) -> str:
    text = str(error) if error is not None else ""
    if text in BENIGN_ERRORS:
        return "benign"
    from zagg.processing.worker import is_auth_failure  # lazy: keep this module light

    if is_auth_failure(Exception(text)):
        return STORE_FATAL
    low = text.lower()
    if any(t in low for t in _TRANSIENT_TOKENS):
        return "transient"
    match = _STATUS_RE.search(text)
    if match:
        code = int(match.group(1))
        return "transient" if code in (408, 429) or code >= 500 else STORE_FATAL
    return STORE_FATAL if any(t in low for t in _FATAL_TOKENS) else "transient"


classify_store_error = getattr(_dispatch, "classify_store_error", _classify_fallback)


#: The write-phase keys summed per result (the worker's ``phase_timings``).
WRITE_PHASES = ("write", "column", "icechunk")

#: Breaker rule names, as they land in ``summary["breaker"]["rule"]``.
RULE_TIMEOUT_RATIO = "timeout-ratio"
RULE_WALL_MARGIN = "wall-margin"
RULE_STORE_FATAL = "store-fatal"

#: The ``outcome`` a shard the breaker refused to dispatch resolves with.
NOT_DISPATCHED = "not-dispatched"


@dataclass(frozen=True)
class GuardConfig:
    """The ramp + breaker knobs (issue #652 defaults; every one a knob).

    Built from the ``dispatch`` block -- ``{"ramp": {...}, "breaker": {...}}``
    with the field names below -- by :meth:`from_dict`, which rejects unknown
    keys, wrong types and out-of-range values at submission.
    """

    start: int = 500
    factor: int = 2
    cap: int = 1000
    window: int = 50
    tolerance: float = 1.2
    timeout_ratio: float = 0.20
    wall_fraction: float = 0.85
    store_fatal: int = 3
    store_fatal_first: int = 20

    _RAMP = ("start", "factor", "cap", "window", "tolerance")
    _BREAKER = ("timeout_ratio", "wall_fraction", "store_fatal", "store_fatal_first")
    _INTS = frozenset({"start", "factor", "cap", "window", "store_fatal", "store_fatal_first"})

    @classmethod
    def from_dict(cls, block: dict | None) -> GuardConfig:
        """Validate a ``dispatch`` block; ``None`` / ``{}`` is the defaults."""
        if block is not None and not isinstance(block, dict):
            raise ValueError(f"dispatch must be a mapping (got {block!r})")
        block = dict(block or {})
        values: dict = {}
        for group, names in (("ramp", cls._RAMP), ("breaker", cls._BREAKER)):
            sub = block.pop(group, None)
            sub = {} if sub is None else sub
            if not isinstance(sub, dict):
                raise ValueError(f"dispatch.{group} must be a mapping (got {sub!r})")
            unknown = set(sub) - set(names)
            if unknown:
                raise ValueError(f"dispatch.{group}: unknown key(s) {sorted(unknown)}")
            values.update({k: v for k, v in sub.items() if v is not None})
        if block:
            raise ValueError(f"dispatch: unknown key(s) {sorted(block)} (expected ramp, breaker)")
        cfg = cls(**values)
        for f in fields(cls):
            v = getattr(cfg, f.name)
            integer = f.name in cls._INTS
            kinds = int if integer else (int, float)
            if isinstance(v, bool) or not isinstance(v, kinds) or v <= 0:
                kind = "an integer" if integer else "a number"
                raise ValueError(f"dispatch: {f.name} must be {kind} > 0 (got {v!r})")
        if cfg.cap < cfg.start:
            raise ValueError(f"dispatch: ramp.cap {cfg.cap} is below ramp.start {cfg.start}")
        if cfg.tolerance < 1.0:
            raise ValueError(f"dispatch: ramp.tolerance must be >= 1.0 (got {cfg.tolerance!r})")
        for name in ("timeout_ratio", "wall_fraction"):  # at 1.0 the rule can never fire
            if getattr(cfg, name) >= 1.0:
                raise ValueError(
                    f"dispatch: breaker.{name} must be < 1.0 (got {getattr(cfg, name)!r})"
                )
        if cfg.store_fatal > cfg.store_fatal_first:
            raise ValueError(
                f"dispatch: breaker.store_fatal {cfg.store_fatal} exceeds "
                f"breaker.store_fatal_first {cfg.store_fatal_first}, so it can never trip"
            )
        return cfg

    def as_dict(self) -> dict:
        """The nested ``dispatch`` block this config round-trips to."""
        return {
            "ramp": {k: getattr(self, k) for k in self._RAMP},
            "breaker": {k: getattr(self, k) for k in self._BREAKER},
        }


@dataclass(frozen=True)
class ResultRecord:
    """One completed result, reduced to what the rules read."""

    wall_s: float | None
    write_s: float | None
    timeout: bool
    store_fatal: bool
    dispatch_width: int | None = None


def _median(values: list) -> float | None:
    return statistics.median(values) if values else None


def classify_result(result: dict, *, function_timeout_s: float) -> ResultRecord:
    """Reduce a poller result dict to a :class:`ResultRecord`.

    A timeout is the sync transport's ``timeout`` flag, the Event transport's
    ``failed-unknown`` drop outcome (no status object by the deadline -- a
    worker killed at the wall writes none), or Lambda's own ``Task timed
    out`` text; its wall is the function timeout when the body carries no
    billed duration. Store-fatal reads ``error_class`` off the result or its
    body (PR #658) and falls back to classifying the error string.
    """
    body = result.get("body") or {}
    error = result.get("error")
    timeout = bool(
        result.get("timeout")
        or result.get("outcome") == "failed-unknown"
        or (error and "Task timed out" in str(error))
    )
    wall = result.get("lambda_duration") or None
    if wall is None and timeout:
        wall = float(function_timeout_s)
    timings = body.get("phase_timings") or {}
    parts = [timings[k] for k in WRITE_PHASES if isinstance(timings.get(k), (int, float))]
    # The no-data channel's text starts with its benign entry and may carry a
    # source read's 403/404 exemplar after it: benign, never store-fatal.
    failed = bool(error) and not str(error).startswith(BENIGN_ERRORS) and not timeout
    error_class = result.get("error_class") or body.get("error_class")
    if error_class is None and failed:
        error_class = classify_store_error(error)
    return ResultRecord(
        wall_s=wall,
        write_s=sum(parts) if parts else None,
        timeout=timeout,
        store_fatal=failed and error_class == STORE_FATAL,
        dispatch_width=result.get("dispatch_width"),
    )


class FixedWindow:
    """A fixed pacing window: what the poller paces by without a ramp.

    ``max_in_flight`` shards dispatched-unresolved at once, or unbounded on
    ``None`` (``Run.attach`` registers observe-only entries). Mirrors the
    :class:`DispatchGuard` reads the poller and the summaries make --
    ``record``, ``room``, ``tripped``, ``breaker``, ``ramp`` -- and never
    records or trips, so ``breaker`` is ``None`` and ``ramp`` is ``[]``.
    """

    tripped: dict | None = None
    breaker: dict | None = None

    @property
    def ramp(self) -> list[dict]:
        return []  # a fresh list: no shared mutable class state

    def __init__(self, max_in_flight: int | None):
        self._max = max_in_flight
        self.width = max_in_flight  # what the poller stamps on each result

    def record(self, result: dict) -> None:
        pass

    def room(self, queued: int, in_flight: int) -> int:
        return queued if self._max is None else max(self._max - in_flight, 0)


class DispatchGuard:
    """The ramp + breaker over one run's completed results (see the module doc).

    ``workers`` is the run's fan-out width (the concurrency preflight's
    clamp): the ramp never exceeds it. ``function_timeout_s`` prices a
    timeout's wall and anchors the ``wall-margin`` rule.
    """

    def __init__(
        self,
        config: GuardConfig | None = None,
        *,
        workers: int | None = None,
        function_timeout_s: float,
    ):
        self.config = config or GuardConfig()
        self._timeout_s = float(function_timeout_s)
        self._cap = self.config.cap if workers is None else min(self.config.cap, int(workers))
        self.width = min(self.config.start, self._cap)
        self._records: list[ResultRecord] = []
        self._timed_out: set = set()  # shard keys already recorded as a timeout
        self._widened_at = 0  # result count when the current width took effect
        self._baseline: float | None = None
        self.ramp: list[dict] = [{"width": self.width, "at_result": 0}]
        self.tripped: dict | None = None
        self._hold: dict | None = None

    # -- the poller's two calls ----------------------------------------------

    def record(self, result: dict) -> None:
        """Fold one completed result in; re-evaluate the breaker, then the ramp."""
        cfg = self.config
        rec = classify_result(result, function_timeout_s=self._timeout_s)
        key = result.get("shard_key")
        if rec.timeout and key is not None:
            # A dropped shard re-fires once: it counts one timeout, recorded at
            # its first drop; a later ok/failed completion still counts.
            if key in self._timed_out:
                return
            self._timed_out.add(key)
        self._records.append(rec)
        n = len(self._records)
        if self.tripped is not None:
            return
        if n <= cfg.store_fatal_first:
            if sum(r.store_fatal for r in self._records) >= cfg.store_fatal:
                self.tripped = self._verdict(RULE_STORE_FATAL, True)
                return
        if n < cfg.window:
            return
        last = self._records[-cfg.window :]
        if sum(r.timeout for r in last) / cfg.window > cfg.timeout_ratio:
            self.tripped = self._verdict(RULE_TIMEOUT_RATIO, True)
            return
        wall = _median([r.wall_s for r in last if r.wall_s is not None])
        holding = wall is not None and wall > cfg.wall_fraction * self._timeout_s
        if holding and self._hold is None:
            self._hold = self._verdict(RULE_WALL_MARGIN, False)
        if holding or self.width >= self._cap:
            return
        samples = [r.write_s for r in self._records if r.write_s is not None]
        if self._baseline is None and len(samples) >= cfg.window:
            self._baseline = _median(samples[: cfg.window])
        at_width = [
            r.write_s
            for r in self._records[self._widened_at :]
            if r.write_s is not None and r.dispatch_width in (None, self.width)
        ]
        baseline, current = self._baseline, _median(at_width[-cfg.window :])
        if baseline is None or current is None or len(at_width) < cfg.window:
            return
        if current <= cfg.tolerance * baseline:
            self.width = min(self.width * cfg.factor, self._cap)
            self._widened_at = n
            self.ramp.append({"width": self.width, "at_result": n})

    def room(self, queued: int, in_flight: int) -> int:
        """Slots to fire now. Tripped: every queued shard, so the poller refuses them."""
        if self.tripped is not None:
            return queued
        return max(self.width - in_flight, 0)

    # -- what the poller and the summaries read --------------------------------

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

    @property
    def breaker(self) -> dict | None:
        """``summary["breaker"]``: the trip, else the first wall-margin hold, else ``None``."""
        return self.tripped or self._hold

    def _verdict(self, rule: str, tripped: bool) -> dict:
        return {
            "tripped": tripped,
            "rule": rule,
            "at_result": len(self._records),
            "width": self.width,
        }
