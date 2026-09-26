"""Per-shard stats record: standardized schema + mergeable fold (issue #297).

One versioned record per processed shard, built from the worker's existing
``metadata`` dict. Two consumers, one source: the JSON **sidecar** written as a
SIBLING object next to a hive leaf ``.zarr`` (``stats.json``; the
``{hash}.stats.json`` naming arrives with issue #299 — ``semantic_hash`` is a
nullable placeholder until then), and the **run-level parquet** the dispatcher
writes at the store root (one row per shard, failure rows included).

The schema is mergeable by construction: only associative stats (counts, sums,
min/max — no stored means/medians), so the up-tree rollup is
:func:`merge` — a fold that is associative and commutative up to float
summation order. Identity-like fields (``shard_key``, ``granules_sha256``,
``invoked_by``, ...) merge as equal-or-``None``: a mismatch collapses to
``None`` (absorbing), which keeps the fold associative.

The sidecar has one SIBLING of its own (issue #388): ``granules.json``, the
recorded granule-id list behind the record's ``granules_sha256``, on the same
spec-keyed naming grammar (:func:`granule_ids_key`). Both are recorded in the
CANONICAL granule-id space — the driver-stripped bare id
(:func:`canonical_granule_id`, espg-ruled at the D19 hash epoch) — so the
identity a leaf carries names the granules it read and not the driver that
fetched them. It is deliberately not a record key — identity equality is the
hash compare every fan-out reader already makes, and the list is fetched only
to name what a contraction dropped.

``build_record``/``merge`` are pure (no I/O); the sidecar/parquet helpers below
them do object-store I/O and import their backends lazily.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
from datetime import datetime, timezone
from typing import Any, Iterable

from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC, LAMBDA_PRICE_PER_GB_SEC_BY_ARCH

#: Version stamped into every record (issue #297). The bump-on-key-change rule
#: applies once the schema is released; while unreleased, pre-release key changes
#: (e.g. the D19 rev 2 / D20 record) rev in place and this stays 1.
SCHEMA_VERSION = 1

#: Leaf sidecar object name (sibling of the leaf ``.zarr``, not inside it).
#: Windowed leaves (issue #246) suffix the window label — ``stats_{window}.json``
#: — mirroring the ``{full_id}_{window}.zarr`` leaf naming so two windows of one
#: shard cannot clobber each other's sidecar.
SIDECAR_NAME = "stats.json"

#: Recorded granule-id list object (issue #388) — a SIBLING of the stats
#: sidecar, on the same naming grammar (:func:`granule_ids_key`), holding the
#: id list behind the sidecar's ``granules_sha256``. Split out of the record
#: on the espg ruling (question (6)(c)): identity EQUALITY is the small
#: sidecar's hash compare, which every fan-out reader already pays
#: (``dedup.shard_status`` per shard, ``rows_from_status`` per envelope), so
#: the list — ~4,600 ids ≈ 550 KB on a pole shard — must never ride the
#: record, the response envelope, or those GETs. It is read exactly once, and
#: only when the hash MISMATCHES: to NAME the granules a contraction dropped.
GRANULE_IDS_NAME = "granules.json"

#: The granule-id sibling's OWN version marker (issue #388), on the refusal
#: manifest's precedent. Deliberately not :data:`SCHEMA_VERSION`: that number
#: versions the D20 run RECORD, which this object exists to not be — welding
#: them would bump the sibling on every record rev and make a sibling format
#: change unversionable without revving the record. Readers must treat it
#: leniently (:func:`zagg.dedup.leaf_recorded_ids`): the hash pairing is what
#: decides, and an unknown marker degrades to ``unrecorded-ids``, never to an
#: error.
GRANULE_IDS_SPEC = "zagg-granule-ids/1"

#: ``platform.machine()`` spellings -> the #298 price-table arch keys, so the
#: worker-side record prices with the same table the dispatcher's cost block
#: uses. An unmapped/absent arch falls back to the flat default rate.
_ARCH_ALIASES = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "x86_64", "amd64": "x86_64"}

#: D19 digest shape (:func:`zagg.semantics.semantic_hash`): a full sha256 hex
#: digest. Only the ``metadata`` FALLBACK in :func:`build_record` is checked
#: against it — that dict is not always locally built: the dispatcher's
#: stale-worker path (``zagg.runner._lambda_result_rows``) passes the JSON body
#: the remote worker returned, so an unchecked value would let a version-skewed
#: worker plant an identity that ``dedup.shard_status`` and
#: ``dedup.classify_leaf_identity`` later trust to skip. Malformed reads as no
#: recorded identity (``None`` — never provably current), never a wrong one.
_SEMANTIC_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

# Merge dispositions (associative + commutative by construction). Floats sum,
# so equality across fold orders holds up to FP summation order.
_SUM_KEYS = ("n_shards", "n_granules", "n_obs", "cells_with_data", "duration_s")
_SUM_OR_NONE_KEYS = (
    "gb_seconds",
    "est_cost_usd",
    "n_obs_read",
    "spill_bytes",
    "spill_blocks_closed",
    "raster_bytes_read",
    "raster_px_decoded",
    "raster_px_sampled",
)
_MAX_OR_NONE_KEYS = ("max_memory_mb", "container_hwm_mb")
_EQ_OR_NONE_KEYS = (
    "window",
    # Bulk multi-window unit width (issue #586): every leaf of one shard
    # invoke carries the same N; a rollup across invokes collapses it.
    "unit_windows",
    "shard_key",
    "run_id",
    "semantic_hash",
    "granules_sha256",
    # O11 (issue #342): per-leaf by definition, so a multi-leaf rollup
    # collapses it to None (absence = unverifiable, §5.3) while merge([r])
    # stays r.
    "content_hashes",
    # Leaf pyramid column basename (issue #383's deferred run-record key,
    # landed with #388): D22 discovery is run-record-driven, so column
    # existence must be discoverable without a tree listing. Named
    # ``leaf_column``, not ``column`` — see _ROW_SCALARS below.
    "leaf_column",
    # Icechunk companion refs record (issue #580): per-leaf by definition —
    # one commit per leaf — so a rollup collapses it to None like the O11
    # record above.
    "icechunk",
    "zagg_version",
    "lambda",
    "invoked_by",
    "error",
)


def canonical_granule_id(entry) -> str:
    """The canonical identity of one granule: its **driver-stripped bare id**.

    espg-ruled at the D19 hash epoch, 2026-08-17 (PR #420 question (1)(b)):
    *"we want the granule to trigger the hash, not how that granule is
    fetched."* One physical granule is named three ways across the code paths
    that record it — a resolved ``s3://bucket/key/FILE`` href, an
    ``https://host/path/FILE`` href (``zagg.runner._resolve_urls`` picks one by
    ``data_source.driver``), or the bare catalog id (``rec["id"]``, which for
    every CMR/STAC catalog zagg reads IS the href's basename). Pre-epoch each
    spelling hashed to a different :func:`granules_sha256`, so switching driver
    — pure fetch mechanism, already packaging in the D19 core
    (:data:`zagg.semantics.DATA_SOURCE_PACKAGING_KEYS`) — made every leaf's
    recorded catalog identity un-reproducible and sent the skip gate down the
    ``expansion`` arm for a rerun over exactly the same inputs.

    The canonical form is therefore the basename: the scheme, host, bucket and
    key prefix are all *where the bytes live*, not *which granule it is*, and
    the s3 and https hrefs for one granule agree on nothing else. Ids that are
    already bare pass through unchanged and idempotently, which covers the
    raster id space (:func:`raster_granule_ids` — STAC item ids or ISO
    datetimes, neither of which carries a path separator).

    Paired-asset worker payloads (issue #425) arrive as ``{"url", "assets"}``
    mappings; the record's granule identity is the **primary alone** — issue
    #425's invariant, unchanged by this epoch — which is what keeps a caller
    passing resolved strings (the local backend) and one passing the worker
    payload verbatim (the Lambda handler) on one hash. The ``assets`` are
    dropped, so a sibling href never reaches a digest at all: swap the sibling
    for a genuinely different granule and :func:`granules_sha256` does not move.
    Whether the recorded catalog identity *should* cover the sibling a paired
    read consumes is a separate ruling, not implied here.

    Accepted cost of the ruling: two granules whose hrefs differ only in
    prefix collapse to one identity. Every catalog zagg reads names granules
    globally uniquely (that is *why* ``rec["id"]`` equals the basename), and
    the alternative — keeping any part of the fetch path — is exactly what the
    ruling excludes. The cost is not only an identity collision: it also
    degrades the **contraction guard's** per-granule resolution inside a
    collapsed group, and in the unsafe direction — dropping one member of a
    collided pair leaves ``zagg.dedup.classify_leaf_identity``'s set diff
    unchanged, so the leaf reads ``id-multiset-drift`` and rewrites where the
    pre-epoch href-space diff refused and named the dropped href. Such a leaf
    is logged loudly (``zagg.dedup._warn_on_collapsed_recorded_ids``); making
    it refuse instead is a ruling on the contraction predicate, standing for
    espg (PR #420 review finding (2)).

    A malformed entry RAISES rather than minting an identity: a mapping with no
    ``url`` primary, or an empty/``None`` id, has no canonical form, and
    stringifying it would give every such entry the SAME id — collapsing N of
    them onto one recorded id, silently shrinking the recorded set the same way
    the collision above degrades the contraction guard. Nothing in the
    dispatchers produces one (``zagg.runner._resolve_granule_entries`` always
    sets a non-empty ``url``), and the pre-epoch ``build_record`` raised
    ``KeyError`` here; the whole point of the D19/D20 identity pair is that an
    identity is never silently wrong.
    """
    if isinstance(entry, dict):
        if not entry.get("url"):
            raise ValueError(
                f"granule entry has no 'url' primary, so it has no canonical "
                f"identity (issue #425's paired-asset shape): {entry!r}"
            )
        entry = entry["url"]
    if entry is None or entry == "":
        raise ValueError("granule entry is empty, so it has no canonical identity")
    text = str(entry).rstrip("/")
    return text.rpartition("/")[2] or text


def canonical_granule_ids(granule_ids: Iterable[Any] | None) -> list[str] | None:
    """:func:`canonical_granule_id` over an id list, preserving order/length.

    ``None`` in, ``None`` out — the callers distinguish "no planned set" from
    "the empty set" (:func:`zagg.dedup.classify_leaf_identity`), so this must
    not collapse the two.
    """
    if granule_ids is None:
        return None
    return [canonical_granule_id(g) for g in granule_ids]


def granules_sha256(granule_ids: Iterable[Any] | None) -> str | None:
    """Catalog identity of a shard: sha256 over its sorted granule ids.

    Ids are whatever uniquely names the shard's inputs (granule URLs for the
    aggregation path, item ids/datetimes for raster), reduced to their
    canonical form first (:func:`canonical_granule_id`) so the digest names
    the GRANULES and not the driver that fetched them. Any href form is
    accepted, and so are the paired-asset mappings (issue #425) — hence
    ``Iterable[Any]``, matching :func:`canonical_granule_ids` and the callers,
    not the ``Iterable[str]`` this annotation carried pre-epoch. Sorted so the
    hash is order-independent; ``None``/empty -> ``None`` (no catalog
    identity).
    """
    ids = sorted(canonical_granule_ids(granule_ids) or [])
    if not ids:
        return None
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def raster_granule_ids(granules: Iterable[dict]) -> list:
    """Catalog-identity inputs for a raster unit's stats record.

    Raster ShardMap entries carry no granule URL; the stable per-acquisition
    identity is the STAC item id when present, else the acquisition datetime.
    """
    return [e.get("id") or e.get("datetime") for e in granules if e.get("id") or e.get("datetime")]


def lambda_env() -> dict | None:
    """The executing Lambda's config block, or ``None`` off-Lambda.

    Read from the standard runtime env vars — the worker needs no event key
    for this. ``function_variant`` is the deployed function name (the ``-disk``
    / benchmark twins are distinct names); request ids / function ARNs are
    deliberately omitted (account-identifying, add nothing — issue #297).
    """
    memory = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    if not memory:
        return None
    return {
        "memory_mb": int(memory),
        "arch": platform.machine(),
        "function_variant": os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
    }


def build_record(
    *,
    shard_key,
    metadata: dict,
    granule_ids: Iterable[Any] | None = None,
    invoked_by: dict | None = None,
    run_id: str | None = None,
    window: str | None = None,
    semantic_hash: str | None = None,
    lambda_config: dict | None = None,
) -> dict:
    """Build one shard's stats record from the worker's ``metadata`` dict.

    ``metadata`` is the existing worker result (``process_shard`` /
    ``process_and_write_hive`` / the raster metas): ``total_obs``,
    ``total_obs_read`` (when the read path measured it — issue #374),
    ``cells_with_data``, ``duration_s``, ``phase_timings``, and the memory
    telemetry keys when the caller stamped them. ``invoked_by`` is copied
    VERBATIM from the invoke payload — the dispatcher resolves it via
    ``sts get-caller-identity`` once per run; workers cannot see the invoker.
    ``run_id`` is threaded the same way (dispatcher-generated, stamped through
    the invoke payload, copied verbatim) so a leaf sidecar joins back to its
    run-level parquet.
    ``window`` (issue #300) is the unit's time-window label (``None``
    unwindowed) — recorded so run records name the exact leaf a row describes
    (the sweep's run-record discovery computes sidecar names from it) and
    merged equal-or-``None`` like the other identity fields, so a cross-window
    rollup reads ``None``. ``semantic_hash`` (issue #299, D19) is the run
    config's semantic-core hash — the identity half the ``has_run`` dedup
    check compares; ``granules_sha256`` below is the catalog half. When the
    caller passes none, ``metadata["semantic_hash"]`` fills in (issue #388):
    the shared hive seams stamp it there, so a caller that never resolved
    the hash itself (the Lambda handler) still records the identity a later
    skip-if-current comparison needs. That fallback is VALIDATED against the
    D19 digest shape (:data:`_SEMANTIC_HASH_RE`) because ``metadata`` is not
    always locally built — see the constant; a caller-passed ``semantic_hash``
    is the caller's own value and is trusted as given.
    ``lambda_config`` is :func:`lambda_env` on Lambda, ``None`` locally;
    when present it prices ``gb_seconds`` / ``est_cost_usd`` from
    ``duration_s`` (the billed-duration approximation the dispatcher's cost
    estimate already uses).
    """
    error = metadata.get("error")
    if semantic_hash is None:
        fallback = metadata.get("semantic_hash")
        if isinstance(fallback, str) and _SEMANTIC_HASH_RE.match(fallback):
            semantic_hash = fallback
    duration_s = float(metadata.get("duration_s") or 0.0)
    gb_seconds = est_cost = None
    if lambda_config and lambda_config.get("memory_mb"):
        gb_seconds = duration_s * lambda_config["memory_mb"] / 1024.0
        # Arch-keyed rate (issue #298's price table, folded in here): the
        # record prices with the same table as the dispatcher's cost block.
        arch = _ARCH_ALIASES.get(str(lambda_config.get("arch") or "").lower())
        est_cost = gb_seconds * LAMBDA_PRICE_PER_GB_SEC_BY_ARCH.get(arch, LAMBDA_PRICE_PER_GB_SEC)
    phase_entries = {
        k: float(v)
        for k, v in (metadata.get("phase_timings") or {}).items()
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    }
    # Byte-volume metrics (the spill instrumentation, issue #217) must not ride
    # in the seconds-only phase block: the run parquet flattens phase_timings to
    # seconds-typed columns, where a byte count would mislead cost/latency
    # queries. Split any ``*_bytes`` entries out; surface spill volume on its own
    # summed field (issue #297). ``spill_blocks_closed`` (issue #370) is split
    # the same way: the fold-regime marker (0/absent = exact single-block leaf)
    # is a count, not seconds.
    spill_bytes = phase_entries.get("spill_bytes")
    spill_blocks_closed = _opt_int(phase_entries.get("spill_blocks_closed"))
    phase_timings = {
        k: v
        for k, v in phase_entries.items()
        if not k.endswith("_bytes") and k != "spill_blocks_closed"
    }
    # Canonical granule identity (espg-ruled 2026-08-17, the D19 hash epoch):
    # the driver-stripped bare id, so an s3 href, an https href and a bare
    # catalog id of one granule record identically. Paired-asset worker
    # payloads (issue #425) carry ``{"url", "assets"}`` entries and identify by
    # their primary, so the catalog hash stays byte-identical between a caller
    # passing resolved strings (the local backend) and one passing the worker
    # payload verbatim (the Lambda handler) -- see
    # :func:`canonical_granule_id`.
    granule_ids = canonical_granule_ids(granule_ids)
    n_granules = metadata.get("granule_count")
    if n_granules is None:
        n_granules = len(granule_ids or [])
    return {
        "schema_version": SCHEMA_VERSION,
        "shard_key": int(shard_key),
        "window": str(window) if window is not None else None,
        # Bulk multi-window emit (issue #586 phase 2): the number of leaves
        # the shard invoke that wrote this leaf emitted, when it emitted more
        # than its own — one record per leaf, so ``duration_s`` /
        # ``max_memory_mb`` / ``gb_seconds`` / the read phase repeat across
        # the N rows of one invoke (per-invoke quantiles stay the fleet-safety
        # numbers); a per-invoke SUM de-duplicates on ``(run_id, shard_key)``
        # where this is set. ``None`` on a ``(shard, window)`` fan-out unit
        # and on unwindowed runs.
        "unit_windows": _opt_int(metadata.get("unit_windows")),
        "run_id": run_id,
        # The semantic-core hash (D19 rev 2, issue #299): hashes the config's
        # semantic core, NOT the whole template. Nullable for callers without
        # a config in scope.
        "semantic_hash": semantic_hash,
        "zagg_version": _zagg_version(),
        "n_shards": 1,
        "n_granules": int(n_granules),
        # The catalog identity half, and the ONLY id-derived value the record
        # carries: the id LIST behind it lives in its own sibling object
        # (:data:`GRANULE_IDS_NAME`, issue #388) so it never rides this record,
        # the response envelope, or any fan-out identity GET.
        "granules_sha256": granules_sha256(granule_ids),
        # O11 content hashes (issue #342, spec §5.3): the verification half of
        # the D19 identity split, computed by the hive leaf writer from the
        # staged arrays and ridden here off ``metadata``. None wherever no
        # writer recorded them (flat layouts, raster, failures) — absence
        # reads as unverifiable, not tampered.
        "content_hashes": metadata.get("content_hashes"),
        "n_obs": int(metadata.get("total_obs") or 0),
        # Point-path read volume (issue #374): observations DECODED before the
        # shard mask / filters / read-plan padding cut them to ``n_obs``, so
        # ``n_obs_read >= n_obs`` and the read-vs-keep ratio is derivable at
        # read time (never stored — the raster ``px_decoded`` convention).
        # Nullable: ``None`` on the raster path and on records from workers
        # predating the field, where absence means unmeasured, not zero.
        "n_obs_read": _opt_int(metadata.get("total_obs_read")),
        "cells_with_data": int(metadata.get("cells_with_data") or 0),
        "phase_timings": phase_timings,
        "duration_s": duration_s,
        "spill_bytes": spill_bytes,
        # Fold-regime marker (issue #370): blocks closed at the spill threshold.
        # 0/absent = exact single-block leaf; > 0 = the leaf's outputs were
        # folded across blocks (digest merge semantics, coarsened locations,
        # one composition re-quantization).
        "spill_blocks_closed": spill_blocks_closed,
        # Raster read-volume counters (issue #297): compressed bytes fetched,
        # pixels decoded (whole tiles), cell samples gathered. Stored raw — the
        # px_decoded / px_sampled ratio is derived at read, never stored
        # (mergeable-by-construction). That ratio reads as read-time
        # over-provision only when the output grid is coarser than the source; a
        # finer grid can push it below 1. None off-raster.
        "raster_bytes_read": _opt_int(metadata.get("raster_bytes_read")),
        "raster_px_decoded": _opt_int(metadata.get("raster_px_decoded")),
        "raster_px_sampled": _opt_int(metadata.get("raster_px_sampled")),
        # Leaf pyramid column basename (issue #383, recorded per its PR #391
        # deferral): rides the worker metadata when the unit wrote a column,
        # so run-record-driven discovery (D22) sees columns without a tree
        # listing. The column's resolution set lives in the artifact's own
        # ``zagg_column`` attrs — read the column, not this row, for it.
        #
        # Named ``leaf_column`` (espg ruling on issue #388, question (5)(c);
        # renamed before the schema released) for two reasons. (1) ``column``
        # is SQL-reserved in both engines this record's parquet targets
        # (DuckDB, Trino/Athena): the unquoted ``WHERE column IS NOT NULL``
        # is a parse error, so every filter would have to quote it. (2) It
        # reads correctly on the pyramid column's OWN record: "the column
        # this LEAF carries" — NOT "the column this record describes".
        # ``None`` means no column was recorded by this unit; it is never a
        # denial that the described artifact is one. The pyramid column's own
        # stats sidecar records ``None`` here (``zagg.column`` builds it from
        # a hand-made metadata dict with no ``leaf_column`` key — only the
        # leaf record carries the basename, via ``hive.py``), and so does any
        # cross-leaf rollup, where the equal-or-None merge collapses it. So a
        # sidecar scan for column-bearing units must key on the LEAF records,
        # not on the column artifacts' own.
        "leaf_column": metadata.get("leaf_column"),
        # Icechunk companion refs (issue #580, spec §11.4): the leaf's refs
        # commit — ``{path, snapshot, arrays, refs, rebases, commit_s,
        # checksum}`` on success (``commit: "leaf"``), or in ladder mode the
        # ref sidecar it wrote instead — ``{sidecar, bytes, refs, arrays,
        # checksum}`` — ``{"skipped": reason}`` for a unit stage 1
        # does not index (windowed, empty), ``{"error": ...}`` when the
        # fail-open write did not land. ``rebases`` and ``commit_s`` are the
        # fleet's first measurement of commit contention (the issue's open
        # question (1)). None wherever no writer recorded it (the knob off,
        # flat layouts, raster, failures).
        "icechunk": metadata.get("icechunk"),
        "gb_seconds": gb_seconds,
        "est_cost_usd": est_cost,
        "max_memory_mb": _opt_float(metadata.get("max_memory_mb")),
        "container_hwm_mb": _opt_float(metadata.get("container_hwm_mb")),
        "lambda": dict(lambda_config) if lambda_config else None,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "success": not error,
        "error": str(error) if error else None,
        "invoked_by": dict(invoked_by) if invoked_by else None,
    }


def merge(records: Iterable[dict]) -> dict:
    """Fold stats records into one (associative + commutative; issue #297).

    Counts/sums sum, memory high-waters max, ``timestamp`` takes the latest,
    ``success`` ANDs, ``phase_timings`` sums per key over the key union, and
    identity fields keep their common value or collapse to ``None`` on any
    mismatch (``None`` is absorbing, which is what keeps the fold
    associative). ``merge([r]) == r`` up to key order. Raises ``ValueError``
    on an empty iterable or a ``schema_version`` mismatch.
    """
    records = list(records)
    if not records:
        raise ValueError("merge requires at least one record")
    versions = {r.get("schema_version") for r in records}
    if versions != {SCHEMA_VERSION}:
        raise ValueError(f"cannot merge stats records with schema_version(s) {sorted(versions)}")
    out: dict[str, Any] = {"schema_version": SCHEMA_VERSION}
    for key in _EQ_OR_NONE_KEYS:
        first = records[0].get(key)
        if all(r.get(key) == first for r in records):
            # Defensively copy dict values so a rolled-up record never
            # aliases a leaf's nested dict, mirroring build_record (issue
            # #297). One level deeper than a plain ``dict()``: flat values
            # (``lambda``/``invoked_by``) are unaffected, but
            # ``content_hashes`` nests an ``arrays`` map (issue #342) that a
            # shallow copy would still alias.
            if isinstance(first, dict):
                out[key] = {k: dict(v) if isinstance(v, dict) else v for k, v in first.items()}
            else:
                out[key] = first
        else:
            out[key] = None
    for key in _SUM_KEYS:
        out[key] = sum(r.get(key) or 0 for r in records)
    phase_timings: dict[str, float] = {}
    for r in records:
        for name, secs in (r.get("phase_timings") or {}).items():
            phase_timings[name] = phase_timings.get(name, 0.0) + secs
    out["phase_timings"] = phase_timings
    for key in _SUM_OR_NONE_KEYS:
        vals = [r.get(key) for r in records if r.get(key) is not None]
        out[key] = sum(vals) if vals else None
    for key in _MAX_OR_NONE_KEYS:
        vals = [r.get(key) for r in records if r.get(key) is not None]
        out[key] = max(vals) if vals else None
    stamps = [r.get("timestamp") for r in records if r.get("timestamp") is not None]
    out["timestamp"] = max(stamps) if stamps else None
    out["success"] = all(bool(r.get("success")) for r in records)
    return out


def _opt_float(value) -> float | None:
    return float(value) if value is not None else None


def _opt_int(value) -> int | None:
    return int(value) if value is not None else None


def _zagg_version() -> str:
    import zagg

    return zagg.__version__


def failure_record(*, shard_key=None, error, duration_s=None, run_id=None) -> dict:
    """Skeleton record for a shard with no worker record (issue #297 phase 3).

    Timed-out / OOM / dropped shards write no sidecar and return no envelope
    record; the dispatcher still owes the run parquet a row (error, duration
    until failure). Built through :func:`build_record` so the row shape and
    schema version cannot drift from real records; ``shard_key`` may be
    unknown (``None``) when the failure predates key resolution.
    """
    record = build_record(
        shard_key=shard_key if shard_key is not None else -1,
        metadata={"error": str(error) or "unknown failure", "duration_s": duration_s},
        run_id=run_id,
    )
    if shard_key is None:
        record["shard_key"] = None
    return record


#: Scalar record fields copied straight into a parquet row (flatten order).
_ROW_SCALARS = (
    "schema_version",
    "shard_key",
    "window",
    "unit_windows",
    "run_id",
    "semantic_hash",
    "zagg_version",
    "n_shards",
    "n_granules",
    "granules_sha256",
    "n_obs",
    "n_obs_read",
    "cells_with_data",
    "duration_s",
    "gb_seconds",
    "est_cost_usd",
    "spill_bytes",
    "spill_blocks_closed",
    "raster_bytes_read",
    "raster_px_decoded",
    "raster_px_sampled",
    # Leaf column basename (issue #383's deferred run-record key): a scalar
    # string, so D22 run-record discovery can find column-bearing leaves
    # from the run parquet alone, without a tree listing. The name is
    # ``leaf_column`` and not the bare ``column`` precisely because this is
    # the form the query engines see: ``column`` is SQL-reserved in DuckDB
    # and Trino/Athena, where ``WHERE column IS NOT NULL`` is a parse error
    # and every filter would need ``WHERE "column" IS NOT NULL``. It also
    # reads right on the pyramid column's own row ("the column this LEAF
    # carries"). Renamed pre-release under the issue #388 ruling; nothing
    # published this schema under the old spelling.
    # The granule-id LIST has no column here and never had: the parquet join
    # key for catalog identity is granules_sha256, and since issue #388 the
    # list is not even on the record (:data:`GRANULE_IDS_NAME`).
    "leaf_column",
    "max_memory_mb",
    "container_hwm_mb",
    "timestamp",
    "success",
    "error",
)


def flatten_record(record: dict, *, retries=None, error_class=None) -> dict:
    """One run-parquet row from a stats record (issue #297 phase 3).

    Nested blocks flatten to columns duckdb/Athena can query directly:
    ``phase_timings`` -> ``phase_{name}``, ``lambda`` -> ``lambda_memory_mb``
    / ``lambda_arch`` / ``lambda_function_variant``, ``invoked_by`` ->
    ``invoked_by`` (the ARN) + ``invoked_by_userid``. ``retries`` is the
    dispatcher's attempt count for the shard; ``error_class`` defaults to the
    error string's leading token (callers with the real exception type pass
    it explicitly).
    """
    row = {key: record.get(key) for key in _ROW_SCALARS}
    error = record.get("error")
    if error_class is None and error:
        error_class = str(error).split(":", 1)[0]
    row["error_class"] = error_class
    row["retries"] = retries
    for name, secs in (record.get("phase_timings") or {}).items():
        row[f"phase_{name}"] = secs
    lam = record.get("lambda") or {}
    row["lambda_memory_mb"] = lam.get("memory_mb")
    row["lambda_arch"] = lam.get("arch")
    row["lambda_function_variant"] = lam.get("function_variant")
    ident = record.get("invoked_by") or {}
    row["invoked_by"] = ident.get("arn")
    row["invoked_by_userid"] = ident.get("userid")
    # Icechunk refs commit (issue #580): the scalars a fleet run's contention
    # question reads straight off the parquet — ``rebases`` and ``commit_s``
    # per leaf — plus the snapshot for joining a leaf to the repo's history,
    # and ``checksum``, the form the leaf's refs carry (``"etag"`` on an object
    # store, ``"last_modified"`` on a local one, §11.3). ``checksum`` is here
    # because it varies by STORE SCHEME rather than by run, so "did this run's
    # refs land with staleness detection, and in which form?" is not derivable
    # from any other column (review finding).
    # Coerced, never dereferenced blind: ``metadata`` is not always locally
    # built — the dispatcher's stale-worker path (``runner._lambda_result_rows``)
    # passes the JSON body a remote worker returned — so a version-skewed body
    # carrying a non-dict here must not take down the WHOLE run-parquet write
    # on a path that is otherwise fail-open (review finding).
    ice = record.get("icechunk")
    ice = ice if isinstance(ice, dict) else {}
    row["icechunk_snapshot"] = ice.get("snapshot")
    row["icechunk_refs"] = ice.get("refs")
    row["icechunk_rebases"] = ice.get("rebases")
    row["icechunk_commit_s"] = ice.get("commit_s")
    row["icechunk_checksum"] = ice.get("checksum")
    # Ladder mode (phase 6): the leaf wrote a ref sidecar instead of committing.
    row["icechunk_sidecar"] = ice.get("sidecar")
    row["icechunk_bytes"] = ice.get("bytes")
    # The levels the leaf's refs cover, by CELL order — comma-joined so the
    # column stays a parquet scalar. Both record shapes carry it (the ladder's
    # sidecar and the ``commit: "leaf"`` twin), and it is the only place
    # "did this leaf's column level get indexed, or only its base?" is
    # answerable from the run parquet (review finding).
    levels = ice.get("levels")
    row["icechunk_levels"] = ",".join(str(o) for o in levels) if levels else None
    row["icechunk_skipped"] = ice.get("skipped")
    row["icechunk_error"] = ice.get("error")
    return row


#: ``run_id`` charset for the parquet key: opaque, no ``/`` or ``.`` (mirrors
#: the frozen window-label grammar in :mod:`zagg.windows`).
_RUN_ID_RE = re.compile(r"^[0-9A-Za-z-]{1,64}$")
#: The D20 ``%Y%m%dT%H%M%SZ`` timestamp grammar — the only shape ``run_parquet_key``
#: emits itself, pinned so a caller-supplied value can't smuggle a path escape.
_RUN_TS_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")


def run_parquet_key(run_id: str, timestamp: str | None = None) -> str:
    """Store-root object name of a run's stats parquet: timestamp, then run id.

    Timestamp-first (D20, docs/design/sparse_coverage.md) so a lexicographic
    listing of ``stats_*.parquet`` is chronological and time-range queries can
    prune on the key alone.

    Both components flow into the object KEY, so both are validated against
    their frozen grammar first — the D8 worker-invoke transport (issue #313)
    makes ``timestamp`` a caller-supplied input, and an embedded ``/`` or
    ``..`` would escape the store root. Like :func:`sidecar_key`'s
    ``validate_label``, a malformed value RAISES rather than composing a
    traversing key.
    """
    return _run_scoped_key("stats", "parquet", run_id, timestamp, what="run parquet")


#: Refusal manifest object name (issue #388, ruled question (9)(c)): the
#: store-ROOT record of a run's contraction refusals. Timestamp-first like the
#: run parquet and the sweep's own record, and deliberately outside the
#: ``stats_*.parquet`` glob the sweep's run-record discovery scans.
REFUSAL_SPEC = "zagg-refusals/1"


def refusal_manifest_key(run_id: str, timestamp: str | None = None) -> str:
    """Store-root object name of a run's refusal manifest (issue #388).

    ``refusals_{ts}_{run_id}.json`` — the run parquet's grammar with its own
    stem, so a listing of a store root sorts a run's artifacts together and
    neither name can be mistaken for the other's.
    """
    return _run_scoped_key("refusals", "json", run_id, timestamp, what="refusal manifest")


def _run_scoped_key(stem: str, ext: str, run_id: str, timestamp: str | None, *, what: str) -> str:
    """``{stem}_{ts}_{run_id}.{ext}`` with both components validated.

    Both flow into the object KEY, so both are checked against their frozen
    grammar first — the D8 worker-invoke transport (issue #313) makes
    ``timestamp`` a caller-supplied input, and an embedded ``/`` or ``..``
    would escape the store root. Like :func:`sidecar_key`'s ``validate_label``,
    a malformed value RAISES rather than composing a traversing key.
    """
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not _RUN_TS_RE.match(ts):
        raise ValueError(
            f"{what} timestamp {ts!r} does not match the D20 grammar ({_RUN_TS_RE.pattern})"
        )
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError(
            f"run_id {run_id!r} does not match the opaque key charset ({_RUN_ID_RE.pattern})"
        )
    return f"{stem}_{ts}_{run_id}.{ext}"


def write_refusal_manifest(
    store_root: str,
    refusals,
    *,
    run_id: str,
    timestamp: str | None = None,
    semantic_hash: str | None = None,
    store_kwargs: dict | None = None,
) -> str | None:
    """PUT the run's refusal manifest at the store root; its path or ``None``.

    A refused unit (the issue #388 contraction guard) writes NOTHING — no
    leaf, no sidecar, no run-parquet row — so before this the only trace of
    which granules a rerun would have dropped was a worker log line truncated
    to five ids. This is the durable full list, ruled (question (9)(c)) as
    ONE small root object per refusing run rather than a synthesized D20 row:
    a refusal has no ``n_obs``, no ``content_hashes`` and no committed leaf to
    describe, so a run-record row for it would change what a row IS.

    ``refusals`` is the run's refused unit metadata dicts (the seams'
    ``{"refused": True, "missing_granules": [...]}`` early returns). Each
    contributes its unit identity, its classification (``contraction`` /
    ``mixed``) and the FULL missing-id diff — which is exactly the id list the
    guard read from the leaf's granule-id sibling to name the drop, composed
    here into one place an operator can act from. Units sort by (shard,
    window) so two runs over the same refusal set produce comparable objects.
    Nothing is written when nothing refused: a pure-skip run stays row-less
    and object-less at the root (ruled (9)(a)).

    Fail-open (D9 telemetry class, the sweep run record's posture): a failed
    write logs and returns ``None`` — the run's exit status and its
    ``cells_refused`` count are unaffected. The COMPOSITION is inside the
    same guard as the PUT (``write_granule_ids``'s posture), because this runs
    before the summary and the run-stats parquet: a ``MemoryError`` on an
    unbounded refusal set, or a malformed ``missing_granules``, must cost the
    manifest, never the run record of a run that already did all its work.
    Callers must be store-writers in their own right (D8): the local
    dispatcher is also the worker, which is why this rides the same wrap-up
    seam as the root ``coverage.moc``.
    """
    import logging

    try:
        units = []
        for meta in refusals:
            if not isinstance(meta, dict):
                continue
            missing = [str(g) for g in (meta.get("missing_granules") or [])]
            units.append(
                {
                    "shard_key": meta.get("shard_key"),
                    "window": meta.get("window"),
                    "identity": meta.get("identity"),
                    "n_missing": len(missing),
                    "missing_granules": missing,
                }
            )
        if not units:
            return None
        units.sort(key=lambda u: (str(u["shard_key"]), str(u["window"])))
        body = {
            "spec": REFUSAL_SPEC,
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            # The run context needed to act on it: WHEN, and WHICH product
            # (the D19 semantic core the refused units were dispatched under).
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "semantic_hash": semantic_hash,
            "zagg_version": _zagg_version(),
            "cells_refused": len(units),
            "units": units,
        }

        from zagg.store import open_object_store, put_object

        key = refusal_manifest_key(run_id, timestamp)
        put_object(
            open_object_store(store_root, **(store_kwargs or {})),
            key,
            json.dumps(body, indent=1).encode(),
        )
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"refusal manifest write failed (fail-open, issue #388): {e}"
        )
        return None
    return f"{store_root.rstrip('/')}/{key}"


def write_run_parquet(
    store_root: str,
    rows: list,
    *,
    run_id: str,
    timestamp: str | None = None,
    store_kwargs: dict | None = None,
    finalize_error: str | None = None,
    icechunk_init: dict | None = None,
) -> str:
    """PUT the run-level stats parquet at the store root (issue #297 phase 3).

    One row per dispatched shard — successes from the workers' records
    (envelope-ridden, no second S3 listing), failures from the dispatcher's
    :class:`~zagg.dispatch.RunReport` via :func:`failure_record`. Written with
    the core ``fastparquet`` engine (the :mod:`zagg.catalog.extract`
    precedent — pyarrow stays off the worker path, issue #130);
    ``object_encoding="utf8"`` pins string columns that may be all-null in a
    given run (e.g. ``invoked_by`` locally). Returns the object's full path.

    ``timestamp`` pins the D20 key instead of stamping write time — the D8
    worker-invoke transport (issue #313): the dispatcher names the object at
    dispatch so ``summary["run_stats_path"]`` is knowable without reading the
    fire-and-forget worker's response.

    ``finalize_error`` is the one RUN-level column (issue #335): the guarded
    finalize's failure string (``None`` on a clean run), broadcast constant
    across the rows like ``run_id``/``n_shards`` already are, so a postmortem
    can tell "finalize failed" from "the run never happened" off the parquet
    alone. Always written, so the column set is the same every run.

    ``icechunk_init`` (issue #580) is the run's companion-repo init record
    (``summary["icechunk"]``): three more run-level columns, split the way the
    row-level ``icechunk_*`` flattener splits its record rather than collapsed
    into one — ``icechunk_init_repo`` (the record's ``path``),
    ``icechunk_init_snapshot`` (the init commit), ``icechunk_init_error``
    and ``icechunk_split_ratchet`` (the §11.5 ratchet this run applied, or null)
    (the fail-open failure string, on the ``finalize_error`` precedent). So a
    run's leaves join to the repo history they were committed into, and
    "init failed" is never something a reader infers from whether a value
    looks like a hex id. ``None`` (knob off, non-hive) writes all three null;
    all three are always written, so the column set is the same every run.
    """
    import tempfile

    import pandas as pd

    from zagg.store import open_object_store, put_object

    if not rows:
        raise ValueError("write_run_parquet requires at least one row")
    key = run_parquet_key(run_id, timestamp)
    df = pd.DataFrame(rows)
    # Run-level (issue #335): constant down the column, None on a clean run.
    df["finalize_error"] = finalize_error
    init = icechunk_init or {}
    df["icechunk_init_repo"] = init.get("path")
    df["icechunk_init_snapshot"] = init.get("snapshot")
    df["icechunk_init_error"] = init.get("error")
    # The §11.5 split ratchet (phase 6): ``"{from}->{to}"`` when this run
    # moved the store's split_order coarser — the flag a later
    # rewrite_manifests pass reads — else null.
    ratchet = init.get("split_ratchet")
    df["icechunk_split_ratchet"] = (
        f"{ratchet['from']}->{ratchet['to']}" if isinstance(ratchet, dict) else None
    )
    # Packed morton shard keys exceed 2^53 (and int64 for high base cells), so
    # the DataFrame's float64 inference on a column that mixes ints with
    # failure-row ``None``s silently corrupts them (issue #300 — the sweep's
    # run-record discovery needs exact keys back). Rebuild the column as
    # nullable UInt64 straight from the row values, bypassing the float
    # intermediate; key spaces it cannot hold (a negative key) fall through
    # untouched.
    if "shard_key" in df.columns:
        try:
            df["shard_key"] = pd.array([r.get("shard_key") for r in rows], dtype="UInt64")
        except (TypeError, ValueError, OverflowError):
            pass
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, key)
        df.to_parquet(local, engine="fastparquet", index=False, object_encoding="utf8")
        with open(local, "rb") as fh:
            data = fh.read()
    put_object(open_object_store(store_root, **(store_kwargs or {})), key, data)
    return f"{store_root.rstrip('/')}/{key}"


def rows_from_status(status_prefix: str, *, store_kwargs: dict | None = None) -> list:
    """Run-parquet rows assembled worker-side from the async status prefix.

    The pointer transport of the D8 worker-invoke run-record write (issue
    #313): when a run's rows exceed the async-invoke payload budget, the
    dispatcher sends the status-prefix LOCATION instead and the worker — whose
    role can read the store side — LISTs the per-shard result envelopes the
    workers mirrored there (issue #151) and flattens each envelope's
    ``body["stats"]`` record into a row. Envelopes without a record (stale
    worker) and unparsable objects are skipped with a warning — the run
    record is best-effort telemetry (fail-open, D9-style), never load-bearing.
    Failure rows cannot be assembled here (a timed-out shard mirrored no
    envelope); the dispatcher sends those few rows inline alongside the
    pointer. Dispatcher-side ``retries`` are likewise not recoverable from
    envelopes, so pointer-assembled success rows carry ``retries: None``.

    The per-envelope GETs fan out over a bounded thread pool (this branch is
    the large-run one — hundreds to thousands of shards — where thousands of
    serial S3 round-trips would eat a real slice of the worker's 900 s budget;
    the run record is the thing you most want kept for exactly those runs).
    """
    import json as _json
    import logging
    from concurrent.futures import ThreadPoolExecutor

    import obstore

    from zagg.store import open_object_store

    logger = logging.getLogger(__name__)
    store = open_object_store(status_prefix, **(store_kwargs or {}))
    # Immediate children only — the layout is flat (``{run_id}/{shard}.json``),
    # so ``list_with_delimiter`` (the coverage.py/hive.py prefix-walk precedent)
    # matches the semantics and won't parse any future nested key as an envelope.
    listing = obstore.list_with_delimiter(store)
    keys = [meta["path"] for meta in listing["objects"] if meta["path"].endswith(".json")]

    def _record(key: str) -> list:
        try:
            envelope = _json.loads(bytes(obstore.get(store, key).bytes()))
            # Two object shapes share the prefix layout: the #151 result
            # envelope carries the response body as a JSON STRING; the v2
            # per-shard status object (issue #327) embeds it as a dict. The
            # run prefix's non-shard objects (dispatch manifest, tail marker)
            # parse fine and simply carry no ``stats`` record.
            body = envelope.get("body", "{}")
            if isinstance(body, str):
                body = _json.loads(body or "{}")
            record = (body or {}).get("stats") if isinstance(body, dict) else None
        except Exception as e:
            logger.warning(f"skipping unparsable status envelope {key}: {e}")
            return []
        return stats_records(record)

    if not keys:
        return []
    with ThreadPoolExecutor(max_workers=min(16, len(keys))) as pool:
        return [flatten_record(rec) for recs in pool.map(_record, keys) for rec in recs]


def stats_records(stats) -> list:
    """The D20 records a unit's ``stats`` carries: one, or one per emitted leaf.

    A ``(shard, window)`` unit and an unwindowed shard ride ONE record; a
    bulk multi-window shard invoke (issue #586 phase 2) rides a LIST, one per
    leaf it emitted. Anything else (absent, a stale worker's body) is none.
    """
    if isinstance(stats, dict):
        return [stats]
    if isinstance(stats, list):
        return [r for r in stats if isinstance(r, dict)]
    return []


def window_metas(meta) -> list:
    """The per-leaf metadata dicts one unit result stands for.

    A bulk multi-window shard invoke (issue #586 phase 2) returns the shard's
    metadata with ``windows``, one per-window dict in dispatch order — each
    the shape a ``(shard, window)`` unit returns — so every consumer that
    counts, classifies or sweeps per leaf reads through this; any other unit
    result is its own single leaf.
    """
    if not isinstance(meta, dict):
        return []
    windows = meta.get("windows")
    if isinstance(windows, list):
        return [m for m in windows if isinstance(m, dict)]
    return [meta]


# ---------------------------------------------------------------------------
# Leaf sidecar I/O (phase 2) — one small JSON object per hive leaf, written by
# the worker on success only, SIBLING to the leaf ``.zarr`` (never inside it:
# the leaf stays vanilla zarr v3 and the D4 commit stamp stays its final write).
# ---------------------------------------------------------------------------


#: Manifest ``spec`` string selecting the D23 window-only naming grammar.
#: ``/1``/``/2`` (and an absent spec) keep the frozen legacy sidecar names.
SPEC_V3 = "morton-hive/3"

#: Specs that key the frozen legacy sidecar names (bare / ``stats_{window}.json``).
#: An absent spec (``None``) is a ``morton-hive/1`` store by definition.
_LEGACY_SPECS = (None, "morton-hive/1", "morton-hive/2")


def sidecar_key(leaf_name: str, spec: str | None = None) -> str:
    """Sidecar object name for a leaf zarr basename, keyed by store spec.

    Legacy (``spec`` absent / ``morton-hive/1`` / ``/2``): bare leaves get
    :data:`SIDECAR_NAME`; windowed leaves (issue #246) get
    ``stats_{window}.json`` — a hive node directory holds every window's leaf
    of its one shard, so a bare ``stats.json`` would self-clobber across
    windows. Mirrors the ``{full_id}_{window}.zarr`` leaf naming. Frozen: what
    every current writer emits, unchanged.

    ``morton-hive/3`` (:data:`SPEC_V3`, D23 — window-only leaf naming, no
    writer yet): the sidecar is the leaf stem + ``.stats.json`` —
    ``{window}.stats.json``, and ``all.stats.json`` for the ``schedule:
    none`` :data:`~zagg.windows.SCHEDULE_NONE_TOKEN` leaf. Derived from the
    leaf basename itself, so the token has ONE source
    (:func:`zagg.windows.leaf_name_v3`) and the issue #299 writer flip is a
    spec switch here, not a rename.

    An unrecognized spec RAISES rather than silently defaulting to the legacy
    grammar: a versioned naming bump must be a loud, deliberate change here, or
    a writer/reader spec mismatch would key the wrong sidecar name and read as
    absent instead of failing.
    """
    return _sibling_key(leaf_name, SIDECAR_NAME, spec)


def granule_ids_key(leaf_name: str, spec: str | None = None) -> str:
    """Object name of a leaf's recorded granule-id list (issue #388).

    The stats sidecar's grammar with :data:`GRANULE_IDS_NAME` as the base
    name, so the two siblings are named by ONE rule and cannot drift:
    ``granules.json`` / ``granules_{window}.json`` on the legacy specs,
    ``{stem}.granules.json`` under :data:`SPEC_V3`. Unrecognized specs raise,
    exactly as in :func:`sidecar_key`.
    """
    return _sibling_key(leaf_name, GRANULE_IDS_NAME, spec)


def _sibling_key(leaf_name: str, base: str, spec: str | None) -> str:
    """The spec-keyed name of a leaf SIBLING object; see :func:`sidecar_key`.

    One grammar for the whole D20 sibling family — the sidecar and the issue
    #388 granule-id list differ only in ``base``. Kept private so the family
    stays closed: every sibling name has a named ``*_key`` owner above it.
    """
    if spec == SPEC_V3:
        from zagg.windows import validate_label

        stem = leaf_name.removesuffix(".zarr")
        if not stem or stem == leaf_name:
            raise ValueError(f"{leaf_name!r} is not a leaf zarr name")
        # Match the legacy branch's strictness: a malformed stem (embedded
        # ``/`` path escape, forbidden ``_``) must raise, not pass through as a
        # malformed key. The ``all`` schedule-none token satisfies the explicit
        # grammar, so it keeps passing.
        validate_label(stem)
        return f"{stem}.{base}"
    if spec not in _LEGACY_SPECS:
        raise ValueError(
            f"unknown store spec {spec!r} (one of {_LEGACY_SPECS} for legacy names "
            f"or {SPEC_V3!r} for D23 window-only naming)"
        )
    from zagg.windows import split_leaf_name

    _full_id, window = split_leaf_name(leaf_name)
    if window is None:
        return base
    stem, ext = base.rsplit(".", 1)
    return f"{stem}_{window}.{ext}"


def sidecar_path(leaf_path: str, spec: str | None = None) -> str:
    """Absolute path of a leaf's stats sidecar (sibling of the ``.zarr``)."""
    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    return f"{prefix}/{sidecar_key(name, spec)}"


def write_sidecar(leaf_path: str, record: dict, spec: str | None = None, **store_kwargs) -> None:
    """PUT ``record`` as the leaf's stats sidecar (success path only, #297)."""

    from zagg.store import open_object_store, put_object

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    put_object(
        open_object_store(prefix, **store_kwargs),
        sidecar_key(name, spec),
        json.dumps(record).encode(),
    )


def granule_ids_path(leaf_path: str, spec: str | None = None) -> str:
    """Absolute path of a leaf's granule-id list object (issue #388)."""
    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    return f"{prefix}/{granule_ids_key(name, spec)}"


def write_granule_ids(leaf_path: str, granule_ids, spec: str | None = None, **store_kwargs) -> bool:
    """PUT the leaf's recorded granule-id list beside its sidecar (issue #388).

    Written by the leaf SEAMS (``hive.process_and_write_hive`` /
    ``processing.raster.process_and_write_raster_hive``) right after the D4
    commit stamp, not by the dispatcher that writes the sidecar: the seam
    holds the very list the identity gate compares as ``planned_ids``, so the
    recorded and planned id SPACES have one source, and a worker-side write
    is the D8-sanctioned one — which is also what carries this to the fleet
    with no Lambda-handler change (the sidecar's own ``semantic_hash``
    precedent).

    The object is self-describing and self-pairing: it carries its own
    :data:`GRANULE_IDS_SPEC` marker (not the D20 record's ``schema_version``
    — this is deliberately not that schema) and the ``granules_sha256`` of the
    list it holds, and a reader must accept it only when that hash matches the
    sidecar's (:func:`zagg.dedup.leaf_recorded_ids`).
    A torn rewrite — new sidecar, lost sibling PUT, or the reverse — then
    reads as "no recorded set" rather than as a stale set that could refuse
    or excuse the wrong granules.

    Fail-open INSIDE (the ``zagg.column`` sidecar precedent, D9 telemetry
    class): the leaf is already committed when this runs, so a failed PUT
    must never fail the unit. Returns whether the object landed; the cost of
    it not landing is one wholesale rewrite (``unrecorded-ids``) on a later
    mismatching rerun, never a wrong skip.
    """
    import logging

    try:
        from zagg.store import open_object_store, put_object

        prefix, _, name = leaf_path.rstrip("/").rpartition("/")
        # Recorded in the CANONICAL id space (espg-ruled 2026-08-17): the same
        # driver-stripped bare ids the hash beside them is taken over, and the
        # same space ``dedup.classify_leaf_identity`` diffs the planned set in.
        ids = sorted(canonical_granule_ids(granule_ids) or [])
        put_object(
            open_object_store(prefix, **store_kwargs),
            granule_ids_key(name, spec),
            json.dumps(
                {
                    "spec": GRANULE_IDS_SPEC,
                    "granules_sha256": granules_sha256(ids),
                    "granule_ids": ids,
                }
            ).encode(),
        )
        return True
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"granule-id sibling write failed (fail-open, issue #388): {e}"
        )
        return False


def read_granule_ids(leaf_path: str, spec: str | None = None, **store_kwargs) -> dict | None:
    """The leaf's granule-id list object, or ``None`` when absent (issue #388)."""
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    try:
        data = obstore.get(
            open_object_store(prefix, **store_kwargs), granule_ids_key(name, spec)
        ).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))


def read_sidecar(leaf_path: str, spec: str | None = None, **store_kwargs) -> dict | None:
    """The leaf's stats sidecar record, or ``None`` when absent."""
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    try:
        data = obstore.get(
            open_object_store(prefix, **store_kwargs), sidecar_key(name, spec)
        ).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))
