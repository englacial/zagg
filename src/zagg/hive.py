"""Morton-hive store layout: leaf paths, manifest, and commit stamp (issue #199).

Phase 2 of the layout migration (``docs/design/sparse_coverage.md`` §2-§3).
Under ``output.store_layout: hive`` each shard is its own **self-describing
leaf zarr** under a morton digit tree::

    {store_root}/
      morton_hive.json               <- static manifest (§3); root-only exception
      {sign+base}/{d1}/.../{d_n}/    <- one decimal digit per level (D2)
        {full_id}.zarr/              <- vanilla zarr v3 leaf (D3)

- Ids are morton decimal strings (D1); the leaf path is computed by mortie's
  ``hive_path`` (the convention is owned by the mortie spec) and re-checked
  here against the node invariant.
- **Node invariant (D5)**: below the root a node contains only digit children
  (``[1-4]/``, or the ``{sign+base}`` component at the first level) and
  ``*.zarr`` objects — zero zarr metadata above the leaf, so 2,000 workers
  share no mutable state and a delimiter-LIST with no digit children is a
  definitive "nothing finer exists".
- **Manifest (D6)**: ``morton_hive.json`` is written once at template time and
  never touched during a run; with it every shard path is computable with zero
  requests. The convention is versioned (``morton-hive/1``) from day one.
- **Commit stamp (D4)**: the shard's FINAL write is a root
  ``group.attrs.update(...)`` marking completion (plus cell count, timestamp,
  granule count). A ``.zarr/`` prefix whose root metadata lacks the stamp is
  debris — incomplete, ignorable, safe to overwrite on retry. This is NOT
  consolidated metadata: one small PUT on an object that must exist anyway.

- **Coverage (§4, issue #200)**: the stamp carries a ``coverage`` payload —
  tier 0 is the shard's morton box, the canonical <= 4-member cover of its
  occupied cells (:func:`zagg.grids.morton.morton_box`), padded to exactly
  four decimal-string slots with JSON-null sentinels. Zero extra requests
  (it rides the stamp PUT) and debris semantics are inherited: a torn
  worker's coverage never becomes visible. Exact cell-order occupancy is a
  zstd-compressed bitmap SIDECAR inside the leaf (``coverage.moc`` — the O8
  resolution; the one recorded exception to the vanilla-v3 leaf: data reads
  are unaffected, but member enumeration warns and skips it), written
  before the stamp and pointed to from the envelope; attrs stay lean and the
  extra GET is paid only by readers that pass the box test. The optional end-of-run
  root ``coverage.moc`` (issue #200 phase 3, default-on for hive) is a
  shard-order ranges MOC at the store root — the second root-only object,
  written fail-open by the dispatcher (locally) or a fire-and-forget worker
  invoke (Lambda), and a regenerable cache under D9. The pyramid sweep (§7)
  is a follow-on; the manifest's ``pyramid`` block is declared-only in round
  one (D11/D12).
"""

from __future__ import annotations

import json
import logging
import time
import warnings
from datetime import datetime, timezone

import numpy as np
import zarr
from zarr.errors import GroupNotFoundError

from zagg.store import open_object_store
from zagg.windows import leaf_name, split_leaf_name, union_time_range

logger = logging.getLogger(__name__)

#: Convention version recorded in the manifest and the commit stamp (D6).
HIVE_SPEC = "morton-hive/1"
#: The temporal superset convention (issue #246, D13/D15): declared iff the
#: manifest carries a temporal block; a ``/1`` store *is* a ``/2`` store with
#: ``schedule: none``, so ``/1`` stays the spec written for unwindowed stores.
HIVE_SPEC_V2 = "morton-hive/2"
#: Root manifest object name (the root-only exception to the node invariant).
MANIFEST_NAME = "morton_hive.json"
#: Root-group attrs key carrying the commit stamp (D4).
COMMIT_ATTR = "morton_hive_commit"
#: Convention version of the stamp's coverage payload (§4 tier 0, issue #200).
COVERAGE_SPEC = "morton-moc/1"
#: Fixed slot count of the tier-0 morton box (2-4 members, null-padded).
COVERAGE_BOX_SLOTS = 4
#: In-leaf occupancy-bitmap sidecar object name (issue #200 phase 2, O8) —
#: the one recorded exception to the "vanilla zarr v3 leaf" claim: a foreign
#: key inside ``{full_id}.zarr/`` that zarr readers ignore (data reads are
#: unaffected; ``members()``/``tree()`` emit a ``ZarrUserWarning`` and skip
#: it — review finding, PR #208 round 2).
COVERAGE_SIDECAR = "coverage.moc"
#: zstd level for the sidecar bitmap — fixed so identical occupancy produces
#: byte-identical sidecars across workers and backends.
_ZSTD_LEVEL = 3
#: Store-ROOT coverage object name (issue #200 phase 3): the shard-order MOC
#: for the one-GET bootstrap — the second root-only exception to the node
#: invariant, next to the manifest. Same name as the in-leaf sidecar
#: (:data:`COVERAGE_SIDECAR`), different location and encoding.
ROOT_COVERAGE_NAME = "coverage.moc"


def shard_leaf_path(store_root: str, shard_key, window: str | None = None) -> str:
    """Absolute path of a shard's leaf zarr under ``store_root`` (D2/D3).

    Computed by mortie's ``hive_path`` — the layout convention is owned by the
    mortie spec — and re-checked against the node invariant (D5) so a future
    drift in either side fails loudly instead of writing a stray prefix.
    ``window`` (issue #246, D13) selects the shard's time-windowed leaf,
    ``{full_id}_{window}.zarr``, at the same node; ``None`` is the bare
    schedule-``none`` leaf, byte-identical to pre-windowing paths. Raises
    ``ValueError`` on an invalid shard key or window label.
    """
    from mortie import MortonIndexArray

    word = int(shard_key)
    if word < 0:
        raise ValueError(
            f"shard key must be a packed morton word (got {word}); parse a decimal "
            f"id with zagg.grids.morton.morton_word first"
        )
    rel = MortonIndexArray.from_words(np.asarray([word], dtype=np.uint64)).hive_path()[0]
    if window is not None:
        node, _sep, bare = rel.rpartition("/")
        rel = f"{node}/{leaf_name(bare.removesuffix('.zarr'), window)}"
    check_node_invariant(rel)
    return f"{store_root.rstrip('/')}/{rel}"


def check_node_invariant(rel_path: str) -> None:
    """Raise unless ``rel_path`` is a legal hive leaf path (D5).

    Below the root only digit components are allowed — ``{sign+base}``
    (optional ``-``, one digit ``1..6``) at the first level, one ``1..4`` digit
    per level after — terminating in ``{full_id}.zarr`` (or the windowed
    ``{full_id}_{window}.zarr``, issue #246: split on the first ``_``, window
    label per the frozen grammar) whose id equals the concatenated components.
    This is the walker's contract: any other name under the root (bar the
    manifest and the root ``coverage.moc``) breaks child classification.
    """
    parts = rel_path.strip("/").split("/")
    leaf = parts[-1]
    ok = len(parts) >= 2 and leaf.endswith(".zarr")
    if ok:
        head, digits = parts[0], parts[1:-1]
        try:
            full_id, _window = split_leaf_name(leaf)
        except ValueError:
            full_id = None  # malformed window label -> not a legal leaf
        ok = _is_base_component(head)
        ok = ok and all(len(d) == 1 and d in "1234" for d in digits)
        ok = ok and full_id == head + "".join(digits)
    if not ok:
        raise ValueError(f"path {rel_path!r} violates the hive node invariant (D5)")


def build_manifest(grid, dataset: dict | None = None, windowing: dict | None = None) -> dict:
    """Build the static ``morton_hive.json`` payload for one store (§3, D6).

    ``grid`` supplies the orders; ``dataset`` (typically the ShardMap's
    ``metadata``) supplies identity — only ``short_name`` and ``version`` are
    recorded. The split schedule is implicit under D2 (one digit per level down
    to the shard order) but recorded explicitly for forward compatibility; the
    ``pyramid`` block is declared-only in round one (D11: overviews are a
    second-pass sweep, never written at fan-out time).

    ``windowing`` (issue #246) is the normalized declaration from
    :func:`zagg.config.get_windowing`; when given, the manifest declares
    ``spec: "morton-hive/2"`` and carries the D15 **temporal block** — the
    STATIC schema half of the temporal split (schedule, time encoding, the
    membership ``time_field``, the explicit windows list, and the append
    policy). Temporal EXTENT deliberately never lives here: actual ranges are
    leaf-stamp truth and root-summary cache. ``None`` writes the ``/1``
    manifest byte-identical to pre-windowing runs.
    """
    dataset = dataset or {}
    manifest = {
        "spec": HIVE_SPEC_V2 if windowing else HIVE_SPEC,
        "dataset": {
            "short_name": dataset.get("short_name"),
            "version": dataset.get("version"),
        },
        "cell_order": int(grid.child_order),
        "shard_order": int(grid.parent_order),
        "split_schedule": [1] * int(grid.parent_order),
        "pyramid": {"orders": [], "aggregation": {}},
        "generated_at": _utcnow(),
    }
    if windowing:
        temporal = {
            "schedule": windowing["schedule"],
            "time_field": windowing["time_field"],
            "epoch": windowing["epoch"],
            "scale": windowing["scale"],
            "units": windowing["units"],
            # D15 records calendar alongside encoding/units/epoch. Only
            # proleptic_gregorian is supported this round: the three scales
            # (utc/gps/tai) all derive from stdlib datetime, which is proleptic
            # Gregorian.
            "calendar": "proleptic_gregorian",
            # Generative schedules append by adding leaves the schedule already
            # describes (manifest untouched); the explicit list is the noted
            # D15 exception — appending outside it re-templates the manifest.
            "append_policy": (
                "re-template" if windowing["schedule"] == "explicit" else "new-window"
            ),
        }
        if windowing.get("windows"):
            temporal["windows"] = windowing["windows"]
        manifest["temporal"] = temporal
    return manifest


def ensure_manifest(store_root: str, manifest: dict, *, overwrite: bool = False, **store_kwargs):
    """Write the root manifest once at template time; verify it on reruns.

    A retry into an existing hive store must be able to proceed (that is the
    D4 debris/retry model), so an existing manifest is accepted — but only if
    its FROZEN keys match the run's own (:data:`_FROZEN_MANIFEST_KEYS`: orders
    + identity + schedule — the flat path's ``_check_signature`` analogue).
    ``generated_at`` and ``pyramid`` are excluded: the pyramid block is
    populated/updated by the §7 sweep by design (D11), so comparing it would
    brick every resume after the first sweep.

    ``overwrite=True`` replaces the MANIFEST ONLY — it never clears data. To
    guard against the silent-corruption footgun (committed leaves from the old
    orders would survive a "re-template" and be indistinguishable from legal
    mixed-order data, D2), an overwrite that CHANGES the frozen keys refuses
    when the digit tree already has children (one delimiter-LIST); clear the
    store root first. Returns the manifest now in effect.
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    existing = _read_json(store, MANIFEST_NAME)
    frozen_matches = existing is not None and _frozen(existing) == _frozen(manifest)
    if existing is not None and not overwrite:
        if not frozen_matches:
            raise ValueError(
                f"{MANIFEST_NAME} at {store_root} does not match this run "
                f"(existing {existing!r} vs {manifest!r}); this store was templated "
                f"for different orders/identity — clear the store root (or pick a "
                f"new one) before writing with this configuration"
            )
        return existing
    if overwrite and existing is not None and not frozen_matches:
        # One delimiter-LIST: a {sign+base}-shaped child means shards were
        # already written under the OLD configuration. Their leaves are
        # stamped and walker-discoverable, so replacing just the manifest
        # would leave them masquerading as legal mixed-order data (D2).
        listing = obstore.list_with_delimiter(store)
        children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
        if any(_is_base_component(c) for c in children):
            raise ValueError(
                f"refusing to overwrite {MANIFEST_NAME} at {store_root} with "
                f"different orders/identity: the digit tree already has shard "
                f"data (e.g. {children[0]!r}/), and overwrite replaces the "
                f"manifest only — clear the store root first"
            )
    obstore.put(store, MANIFEST_NAME, json.dumps(manifest, indent=1).encode())
    return manifest


#: Manifest keys the resume match-check compares (orders + identity + split
#: and temporal schedules — a windowing change re-partitions the leaf names,
#: so it must refuse resume exactly like an orders change). ``generated_at``
#: (a timestamp) and ``pyramid`` (populated by the §7 sweep, D11) are mutable
#: by design and excluded. ``temporal`` projects to ``None`` on both sides for
#: pre-#246 manifests, so existing stores resume unchanged.
_FROZEN_MANIFEST_KEYS = (
    "spec",
    "dataset",
    "cell_order",
    "shard_order",
    "split_schedule",
    "temporal",
)


def _frozen(manifest: dict) -> dict:
    """The frozen-key projection of a manifest (resume/overwrite match-check)."""
    return {k: manifest.get(k) for k in _FROZEN_MANIFEST_KEYS}


def _is_base_component(name: str) -> bool:
    """Whether ``name`` is a ``{sign+base}``-shaped hive root child (D5)."""
    base = name[1:] if name.startswith("-") else name
    return len(base) == 1 and base in "123456"


def read_manifest(store_root: str, **store_kwargs) -> dict | None:
    """Read ``morton_hive.json`` from a store root; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), MANIFEST_NAME)


def build_coverage(
    shard_key, occupied, cell_order: int, *, bitmap: bytes | None = None, full: bool = False
) -> dict:
    """Coverage payload for one shard's commit stamp (§4, issue #200).

    ``occupied`` is the shard's occupied cell words (mixed order allowed —
    the cells ``cells_with_data`` counts); the box is their canonical
    <= 4-member cover (:func:`zagg.grids.morton.morton_box`). ``None``/empty
    falls back to the trivial 1-member cover, the shard id itself — always a
    valid ancestor of its own coverage. Members are serialized as decimal
    morton strings (D1), padded to exactly :data:`COVERAGE_BOX_SLOTS` slots
    with trailing ``None`` (JSON null) sentinels — the recorded pad lean.
    ``cell_order`` records the order occupancy was measured at; ``source``
    the producer (``"worker"`` at the leaf tier — phase-3 root and
    sweep-composed payloads record theirs). ``generated_at`` is DELIBERATELY
    omitted at the leaf (review finding, PR #208): the payload rides the
    commit stamp, whose ``written_at`` is the one clock and one writer;
    root/ancestor carriers add their own timestamp fields under this same
    spec (per-carrier-optional).

    ``bitmap`` (phase 2, the O8 resolution) is the encoded sidecar payload
    from :func:`encode_coverage_bitmap`; when given the envelope grows the
    ``encoding``/``sidecar`` pointer plus compressed/raw byte sizes. A
    box-only envelope (``None``, the phase-1 shape) omits those keys — a
    reader treats their absence as "box only". Raises ``ValueError`` if the
    box escapes the shard's subtree (occupied cells from another shard are
    an upstream bug, never stamped).

    ``full`` (issue #246, D14) marks whole-subtree coverage: the ``encoding``
    discriminator becomes ``"full"`` and NO sidecar is written or pointed to
    — the shard id itself is the exact MOC, so readers skip the sidecar GET
    entirely. Decided by one popcount at stamp time (the caller's job);
    mutually exclusive with ``bitmap``.
    """
    if full and bitmap is not None:
        raise ValueError(
            "full=True and a bitmap payload are mutually exclusive: a fully "
            "occupied subtree writes no sidecar (D14)"
        )
    from zagg.grids.morton import morton_box, morton_decimal

    shard = morton_decimal(shard_key)
    if occupied is None or len(occupied) == 0:
        labels = [shard]
    else:
        labels = [morton_decimal(w) for w in morton_box(occupied)]
    if len(labels) > COVERAGE_BOX_SLOTS or any(not s.startswith(shard) for s in labels):
        raise ValueError(
            f"coverage box {labels} escapes shard {shard}'s subtree — occupied "
            f"cells must be the shard's own (the shard id is always a valid "
            f"trivial cover, so this is an upstream cell-assignment bug)"
        )
    coverage = {
        "spec": COVERAGE_SPEC,
        "box": labels + [None] * (COVERAGE_BOX_SLOTS - len(labels)),
        "cell_order": int(cell_order),
        "source": "worker",
    }
    if full:
        coverage["encoding"] = "full"
    elif bitmap is not None:
        n_bits = 4 ** (int(cell_order) - _decimal_order(shard))
        coverage.update(
            encoding="bitmap",
            sidecar=COVERAGE_SIDECAR,
            nbytes=len(bitmap),
            raw_nbytes=-(-n_bits // 8),
        )
    return coverage


def _decimal_order(decimal: str) -> int:
    """HEALPix order of a D1 decimal id (one digit per level past the base)."""
    return len(decimal) - (2 if decimal.startswith("-") else 1)


def _cell_ranks(shard: str, cells, cell_order: int) -> np.ndarray:
    """Bit index of each cell in the shard-subtree bitmap (frozen convention).

    Bit ``i`` is the i-th cell of the shard subtree at ``cell_order`` in
    ascending packed-word (Z-)order — equivalently the base-4 value of the
    cell's D1 digit tail with digits ``1..4`` mapped to ``0..3``. Raises
    ``ValueError`` for a cell outside the subtree or not at ``cell_order``
    (the bitmap is exact-order by construction; there is nothing conservative
    to fall back to).
    """
    from zagg.grids.morton import to_morton_array

    depth = int(cell_order) - _decimal_order(shard)
    ranks = np.empty(len(cells), dtype=np.int64)
    for i, dec in enumerate(to_morton_array(cells).decimal_repr()):
        tail = dec[len(shard) :]
        if not dec.startswith(shard) or len(tail) != depth:
            raise ValueError(
                f"cell {dec} is not an order-{cell_order} cell of shard {shard}; "
                f"the coverage bitmap encodes exact cell-order occupancy only"
            )
        rank = 0
        for ch in tail:
            rank = rank * 4 + (int(ch) - 1)
        ranks[i] = rank
    return ranks


def encode_coverage_bitmap(shard_key, occupied, cell_order: int) -> bytes:
    """zstd-compressed exact occupancy bitmap for one shard (issue #200 phase 2).

    The O8-resolved leaf encoding: a bit field over the shard subtree at
    ``cell_order`` — ``4^(cell_order - shard_order)`` bits, bit ``i`` per the
    :func:`_cell_ranks` convention (ascending packed-word order; base-4 digit
    tail), packed MSB-first within each byte (``np.packbits``), zstd-
    compressed at a fixed level. Raw size is deterministic
    (``ceil(4^depth / 8)`` bytes) regardless of fragmentation — the property
    that beat coarsen-to-fit ranges in the #202 item (6) measurement; the
    bit-order convention freezes with the mortie-side spec. zstd rides
    numcodecs, already in the tree via zarr's codec stack — no new
    dependency.
    """
    from numcodecs import Zstd

    from zagg.grids.morton import morton_decimal

    shard = morton_decimal(shard_key)
    depth = int(cell_order) - _decimal_order(shard)
    if depth <= 0:
        raise ValueError(f"cell_order {cell_order} is not below shard {shard}'s order")
    # Staging is one uint8 per BIT — 8x the raw bitmap (1 MB at the design
    # point: order-9 shards, order-19 cells). It is bounded by the shard's
    # cell count, which the worker already materializes for the leaf
    # template, so no extra guard here; coarse-shard + deep-cell configs
    # beyond that envelope are out of scope (review note, PR #208 round 2).
    bits = np.zeros(4**depth, dtype=np.uint8)
    bits[_cell_ranks(shard, occupied, cell_order)] = 1
    return bytes(Zstd(level=_ZSTD_LEVEL).encode(np.packbits(bits).tobytes()))


def decode_coverage_bitmap(payload: bytes, shard_key, cell_order: int) -> np.ndarray:
    """Occupied cell words from a sidecar bitmap payload (issue #200 phase 2).

    The inverse of :func:`encode_coverage_bitmap`: returns the sorted packed
    ``uint64`` cell words at ``cell_order`` whose bits are set — exact
    occupancy, no over-coverage. Posture (review finding, PR #208 round 2):
    a CORRUPT payload — zstd garbage, or a decompressed size that is not the
    exact raw bitmap size for the depth — raises loudly rather than
    zero-padding/truncating to a plausible partial cell set (a false
    negative, the one thing D9 forbids; the exact truth is intact in the
    leaf, so surfacing beats under-reporting). A MISSING sidecar degrades to
    ``None`` in :func:`read_coverage_bitmap`.
    """
    from numcodecs import Zstd

    from zagg.grids.morton import morton_decimal, morton_word

    shard = morton_decimal(shard_key)
    depth = int(cell_order) - _decimal_order(shard)
    raw = np.frombuffer(bytes(Zstd().decode(payload)), dtype=np.uint8)
    expected = -(-(4**depth) // 8)
    if raw.size != expected:
        raise ValueError(
            f"coverage sidecar decompressed to {raw.size} B; an order-{cell_order} bitmap "
            f"for shard {shard} is exactly {expected} B — refusing to zero-pad or truncate "
            f"(a partial cell set would be a false negative)"
        )
    bits = np.unpackbits(raw, count=4**depth)
    words = np.empty(int(bits.sum()), dtype=np.uint64)
    for i, rank in enumerate(np.flatnonzero(bits)):
        digits, rank = [], int(rank)
        for _ in range(depth):
            digits.append(str(rank % 4 + 1))
            rank //= 4
        words[i] = morton_word(shard + "".join(reversed(digits)))
    return np.sort(words)


def write_coverage_sidecar(leaf_root: str, payload: bytes, **store_kwargs) -> None:
    """PUT the occupancy bitmap sidecar into a leaf (issue #200 phase 2).

    One object at ``{leaf}/coverage.moc`` — the recorded exception to the
    vanilla-v3 leaf, ignored by zarr readers (member enumeration warns and
    skips it; data reads are unaffected). Written BEFORE the commit
    stamp so the stamp stays the leaf's FINAL write (D4): in an unstamped
    prefix the sidecar is debris like everything else, and the wholesale
    retry re-template clears it.
    """
    import obstore

    obstore.put(open_object_store(leaf_root, **store_kwargs), COVERAGE_SIDECAR, payload)


def read_coverage_bitmap(
    leaf_root: str, *, coverage: dict | None = None, **store_kwargs
) -> np.ndarray | None:
    """A leaf's exact occupied cell words from its sidecar, or ``None``.

    Gates on the committed stamp's envelope (:func:`read_coverage`): no
    stamp, a box-only phase-1 payload (no ``encoding``/``sidecar`` keys), an
    unknown encoding, or a missing sidecar object all read ``None`` — the
    box is then the only index and readers degrade per D9, never to wrong
    answers. An ``encoding: "full"`` envelope (issue #246, D14) also reads
    ``None`` here — there IS no sidecar; the shard id itself is the exact
    MOC and :func:`zagg.coverage.bitmap_and` short-circuits on it. Pass an
    already-read ``coverage`` envelope to skip the stamp GET. A PRESENT-but-corrupt sidecar raises instead (see
    :func:`decode_coverage_bitmap` — degrading a corrupt payload would be
    indistinguishable from healthy box-only coverage). The shard id comes
    from the leaf basename — ``{full_id}.zarr``, or the windowed
    ``{full_id}_{window}.zarr`` (issue #246) — via the frozen first-``_``
    split; ``cell_order`` from the envelope. One GET, paid only by readers
    that want cell-level filtering.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.grids.morton import morton_word
    from zagg.store import open_store

    if coverage is None:
        coverage = read_coverage(open_store(leaf_root, **store_kwargs))
    if not coverage or coverage.get("encoding") != "bitmap" or not coverage.get("sidecar"):
        return None
    # Windowed leaves (issue #246) carry `{full_id}_{window}.zarr` basenames;
    # the shard id is the part before the first `_` (the frozen parse rule).
    shard = morton_word(split_leaf_name(leaf_root.rstrip("/").rsplit("/", 1)[-1])[0])
    store = open_object_store(leaf_root, **store_kwargs)
    try:
        data = obstore.get(store, str(coverage["sidecar"])).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return decode_coverage_bitmap(bytes(data), shard, int(coverage["cell_order"]))


def stamp_commit(
    leaf_store,
    *,
    cells_with_data: int,
    granule_count: int,
    coverage: dict | None = None,
    window: str | None = None,
    time_range: tuple | list | None = None,
) -> None:
    """Stamp a shard leaf complete — the shard's FINAL write (D4).

    One small PUT rewriting the leaf's root ``zarr.json`` (which the template
    already created), not consolidation. Until this lands, the leaf prefix is
    debris: a worker that dies mid-shard leaves no stamp, and a retry may
    overwrite the prefix wholesale. ``coverage`` (issue #200) attaches the
    tier-0 payload from :func:`build_coverage`; ``None`` writes the
    pre-coverage stamp unchanged.

    ``window``/``time_range`` (issue #246, D15): a windowed leaf's stamp is
    the TRUTH half of the temporal split — the window label plus the actual
    ``[t_min, t_max]`` written, as ISO-8601 UTC strings (ratified #246 Q2;
    the manifest keeps only the static schedule). A windowed stamp declares
    ``spec: "morton-hive/2"``; unwindowed stamps stay ``/1`` byte-identical.
    ``time_range`` without ``window`` is rejected (no unwindowed extent claim).
    """
    if window is None and time_range is not None:
        raise ValueError(
            "time_range rides windowed stamps only (D15: unwindowed leaves make "
            "no extent claim in the stamp); pass window= as well"
        )
    group = zarr.open_group(leaf_store, path="", mode="r+", zarr_format=3)
    stamp: dict = {
        "spec": HIVE_SPEC if window is None else HIVE_SPEC_V2,
        "complete": True,
        "cells_with_data": int(cells_with_data),
        "granule_count": int(granule_count),
        "written_at": _utcnow(),
    }
    if window is not None:
        stamp["window"] = str(window)
        if time_range is not None:
            # The stamp is the D15 TRUTH half — it fails CLOSED on a bad range
            # (unlike the fail-open cache union). Validate a 2-sequence of
            # parseable UTC instants with t_min <= t_max before it becomes
            # durable truth; the production path already builds this via
            # windows.iso_time_range (worker min/max, ordered), so this guards
            # direct callers. (review finding, PR #248)
            from zagg.windows import parse_utc

            try:
                lo, hi = time_range
                lo_dt, hi_dt = parse_utc(lo), parse_utc(hi)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"time_range must be a 2-sequence of parseable UTC instants "
                    f"[t_min, t_max]; got {time_range!r} ({e})"
                ) from e
            if lo_dt > hi_dt:
                raise ValueError(
                    f"time_range is reversed: t_min {lo!r} follows t_max {hi!r} "
                    f"(the D15 stamp is truth and must fail closed on a bad range)"
                )
            stamp["time_range"] = [str(t) for t in time_range]
    if coverage is not None:
        stamp["coverage"] = coverage
    group.attrs[COMMIT_ATTR] = stamp


def read_commit(leaf_store) -> dict | None:
    """The leaf's commit stamp, or ``None`` for debris / absent leaves (D4).

    Absence (no root group at all) and an unstamped root are the same answer:
    the shard is not complete. Presence requires the stamp — never infer
    completeness from the ``.zarr/`` prefix existing.
    """
    try:
        group = zarr.open_group(leaf_store, path="", mode="r", zarr_format=3)
    except (FileNotFoundError, GroupNotFoundError):
        return None
    stamp = group.attrs.get(COMMIT_ATTR)
    # A malformed (non-mapping) stamp is debris too — never half-trusted.
    return dict(stamp) if isinstance(stamp, dict) else None


def read_coverage(leaf_store) -> dict | None:
    """The leaf's tier-0 coverage payload, or ``None`` when absent (issue #200).

    Rides :func:`read_commit`: debris and absent leaves read ``None``, and so
    does a committed pre-coverage stamp (issue #199 stores carry no
    ``coverage`` key) — older stores keep reading fine. STRICT on the spec
    (review finding, PR #208): only ``spec == "morton-moc/1"`` payloads are
    returned; a malformed dict or an unknown/future spec reads as absent
    rather than half-parsed, so a new envelope version must be adopted here
    deliberately instead of leaking through to box consumers. Box members are
    decimal morton strings; parse one back with
    :func:`zagg.grids.morton.morton_word`.
    """
    stamp = read_commit(leaf_store)
    if stamp is None:
        return None
    coverage = stamp.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("spec") != COVERAGE_SPEC:
        return None
    return dict(coverage)


def _decimal_base(decimal: str) -> str:
    """The ``{sign+base}`` component of a D1 decimal id."""
    return decimal[:2] if decimal.startswith("-") else decimal[:1]


def _decimal_rank(decimal: str) -> int:
    """Base-4 value of a D1 digit tail (digits ``1..4`` -> ``0..3``)."""
    rank = 0
    for ch in decimal[len(_decimal_base(decimal)) :]:
        rank = rank * 4 + (int(ch) - 1)
    return rank


def _rank_tail(rank: int, depth: int) -> str:
    """Inverse of :func:`_decimal_rank`: the width-``depth`` digit tail."""
    digits = []
    for _ in range(depth):
        digits.append(str(rank % 4 + 1))
        rank //= 4
    return "".join(reversed(digits))


def build_root_coverage(
    shard_keys, order: int, *, source: str = "dispatcher", time_range: tuple | list | None = None
) -> dict:
    """Store-root coverage envelope from completed shard keys (issue #200 phase 3).

    The O1 serialization: JSON ranges under the ``morton-moc/1`` envelope,
    with ``encoding: "ranges"`` (vs the leaf sidecar's ``"bitmap"``), the
    shard ``order``, ``source`` and ``generated_at`` — the root carrier's
    staleness discriminators (per-carrier fields under the same spec; the
    leaf payload deliberately omits them, see :func:`build_coverage`). A
    range is an inclusive ``[first, last]`` run of same-order cells within
    ONE base cell, consecutive in base-4 digit-tail rank (ascending
    packed-word order — the bitmap's rank convention at the root). Endpoints
    are D1 decimal STRINGS: packed u64 words exceed 2^53, so raw JSON
    numbers would be silently mangled by any float-based parser (O1).

    ``time_range`` (issue #246, D15): the root summary optionally carries the
    ``[min, max]`` ISO-8601 UTC union of the run's leaf-stamp time ranges —
    CACHE, never truth (the per-leaf stamps are the truth; the walk and the
    sweep regenerate this). Omitted for unwindowed stores, keeping their root
    object byte-identical to pre-#246 runs.
    """
    from zagg.grids.morton import to_morton_array

    words = np.unique(np.asarray(shard_keys, dtype=np.uint64))
    if words.size == 0:
        raise ValueError("build_root_coverage requires at least one shard key")
    decs = list(to_morton_array(words).decimal_repr())
    bad = [d for d in decs if _decimal_order(d) != int(order)]
    if bad:
        raise ValueError(f"shard keys {bad[:3]} are not at shard order {order}")
    # np.unique sorts by packed word; at a fixed order the words of one base
    # cell are contiguous and rank-ascending, so one linear pass finds runs.
    ranges = []
    start = prev = decs[0]
    for dec in decs[1:]:
        same_run = (
            _decimal_base(dec) == _decimal_base(prev)
            and _decimal_rank(dec) == _decimal_rank(prev) + 1
        )
        if same_run:
            prev = dec
            continue
        ranges.append([start, prev])
        start = prev = dec
    ranges.append([start, prev])
    envelope = {
        "spec": COVERAGE_SPEC,
        "encoding": "ranges",
        "order": int(order),
        "source": source,
        "generated_at": _utcnow(),
        "ranges": ranges,
    }
    if time_range is not None:
        envelope["time_range"] = [str(t) for t in time_range]
    return envelope


def root_coverage_words(envelope: dict) -> np.ndarray:
    """Shard words from a root envelope's ranges (inverse of the builder).

    Raises ``ValueError`` on malformed ranges (base-crossing, wrong order,
    reversed endpoints) — same loud posture as the bitmap decoder: a corrupt
    cache must never yield a plausible partial answer.

    Scale note (review, PR #208 round 3): expansion is O(covered shards) in
    a Python loop — milliseconds at coherent-run scale (the design point,
    shard order <= 11 regional products), but a full-sphere accumulated root
    (~3M order-9 / ~50M order-11 shards) would take minutes worker-side. An
    interval-space union on ``[base, lo_rank, hi_rank]`` triples (O(ranges),
    no word materialization) is the upgrade path if root objects ever reach
    continental-accumulation scale; out of scope here.
    """
    from zagg.grids.morton import morton_word

    order = int(envelope["order"])
    words = []
    for lo, hi in envelope["ranges"]:
        base = _decimal_base(lo)
        lo_rank, hi_rank = _decimal_rank(lo), _decimal_rank(hi)
        ok = _decimal_base(hi) == base and lo_rank <= hi_rank
        ok = ok and _decimal_order(lo) == order and _decimal_order(hi) == order
        if not ok:
            raise ValueError(f"malformed coverage range [{lo}, {hi}] at order {order}")
        words.extend(morton_word(base + _rank_tail(r, order)) for r in range(lo_rank, hi_rank + 1))
    return np.unique(np.asarray(words, dtype=np.uint64))


def write_root_coverage(store_root: str, envelope: dict, **store_kwargs) -> dict:
    """GET-union-PUT the store-root ``coverage.moc`` (issue #200 phase 3).

    Incremental runs accumulate: a parsable existing object with the same
    spec/encoding/order is UNIONED with ``envelope`` before the PUT. An
    unparsable or incompatible existing object is logged and OVERWRITTEN —
    the root MOC is a regenerable cache (D9): the leaf stamps are the
    durable truth and the §7 sweep is the authoritative rebuilder, so
    merging with garbage would be worse than replacing it. CONCURRENT runs
    race benignly (review finding, PR #208 round 3): GET-union-PUT is not
    atomic and S3 has no compare-and-swap, so the last writer wins and its
    union may miss the loser's shards until the sweep or the next run
    re-unions — accepted under D9/O7 (a missing listing degrades to "reader
    doesn't see the newest run", never a wrong answer; do NOT add a lock).
    Returns the payload actually written.
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    try:
        existing = _read_json(store, ROOT_COVERAGE_NAME)
    except ValueError:
        logger.warning(
            f"existing {ROOT_COVERAGE_NAME} at {store_root} is not JSON; overwriting "
            f"(regenerable cache — the sweep is the authoritative rebuilder)"
        )
        existing = None
    merged = envelope
    if isinstance(existing, dict):
        compatible = (
            existing.get("spec") == envelope.get("spec")
            and existing.get("encoding") == envelope.get("encoding")
            and existing.get("order") == envelope.get("order")
        )
        if compatible:
            try:
                union = np.union1d(root_coverage_words(existing), root_coverage_words(envelope))
                merged = build_root_coverage(
                    union,
                    int(envelope["order"]),
                    source=envelope.get("source", "dispatcher"),
                    # D15: incremental runs accumulate the time union too —
                    # cache semantics identical to the spatial ranges.
                    time_range=union_time_range(
                        existing.get("time_range"), envelope.get("time_range")
                    ),
                )
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"existing {ROOT_COVERAGE_NAME} at {store_root} failed to parse ({e}); "
                    f"overwriting (regenerable cache — the sweep rebuilds authoritatively)"
                )
        else:
            logger.warning(
                f"existing {ROOT_COVERAGE_NAME} at {store_root} has an incompatible "
                f"envelope; overwriting (regenerable cache)"
            )
    obstore.put(store, ROOT_COVERAGE_NAME, json.dumps(merged, indent=1).encode())
    return merged


def read_root_coverage(store_root: str, **store_kwargs) -> dict | None:
    """Read the store-root ``coverage.moc``; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), ROOT_COVERAGE_NAME)


def leaf_block_index(grid, block_index, shard_key) -> tuple:
    """Leaf-LOCAL storage block for a chunk in a hive leaf (issue #199 phase 2).

    The hive leaf's arrays are sized to one shard, so a chunk's block index is
    its position WITHIN the shard, not the global block ``iter_chunks`` yields.
    Derived from the existing ``shard_local_region`` seam (the sharded path's
    within-shard placement): the region's start divided by the chunk extent.
    At K==1 this is always ``(0,)``.
    """
    region = grid.shard_local_region(block_index, shard_key)
    return tuple(int(s.start) // int(c) for s, c in zip(region, grid.chunk_shape))


def process_and_write_hive(
    shard_key,
    granule_urls,
    grid,
    s3_creds,
    store_root,
    config,
    *,
    store_kwargs,
    driver=None,
    handoff="arrow",
    aoi_payload=None,
    profile=False,
    window=None,
):
    """Process one shard into its own hive leaf store (issue #199 phase 2).

    The SHARED per-shard write path for both backends (phase 3): the local
    runner's ``_cell_work`` and the Lambda handler's hive branch both call
    this, so leaf templating, chunk placement, ragged layout, and stamp
    ordering cannot drift between dispatchers. The shard's output is a
    self-describing leaf zarr at :func:`shard_leaf_path` ``(store_root,
    shard_key)`` (D3), with dense chunks written at leaf-LOCAL block indices
    and — as the shard's FINAL write — the D4 commit stamp on the leaf's root
    group. The leaf template is emitted lazily on the first chunk write
    (mirroring the Lambda handler's lazy store open), so a no-data shard never
    creates the ``.zarr/`` prefix; a worker that dies mid-shard leaves an
    UNSTAMPED prefix — debris, overwritten wholesale on retry
    (``overwrite=True`` on the leaf template makes the retry idempotent).
    When ``grid.sharded`` (issue #236) the dense chunks are not streamed:
    the K carriers accumulate and the whole leaf is written once
    (``write_leaf_to_zarr`` — one ShardingCodec object per array), mirroring
    the flat sharded switch in ``runner._process_and_write``.
    ``profile`` forwards to ``process_shard`` (issue #100), which fills
    ``metadata["phase_timings"]`` with read/index/aggregate; the leaf write
    work — interleaved with the stream (or a single post-stream pass when
    sharded), plus the ragged/coverage/stamp finalize — accumulates into an
    additive ``write`` phase alongside them (issue #249). Unlike the flat
    path, the lazy leaf template emission counts as write: in hive the worker
    owns its leaf's template PUTs (there is no dispatcher-side template), so
    excluding them would hide real write-out cost. Default ``False`` makes
    zero timing calls and leaves ``metadata`` unchanged — no probe tax.

    ``window`` (issue #246, D13/D15) is one dispatch unit's time window:
    ``{"label", "start", "end"}``, bounds half-open in DATASET units
    (converted once at dispatch). It selects the windowed leaf name, injects
    the observation-level ``time_field`` filter (the temporal analog of
    ``aoi_mask`` — see :func:`zagg.config.windowed_cell_config`), stamps the
    window label + the ACTUAL written ISO-UTC time range (also returned as
    ``metadata["time_range"]`` for the root-summary union), and arms the D14
    popcount (``encoding: "full"``). ``None`` is byte-identical to
    pre-windowing behavior.
    """
    from zagg.processing import (
        process_shard,
        write_dataframe_to_zarr,
        write_leaf_to_zarr,
        write_ragged_leaf_to_zarr,
    )
    from zagg.store import open_store

    windowing = None
    time_range_of = None
    if window is not None:
        from zagg.config import windowed_cell_config

        # Inject the window's observation filter into a per-unit config copy
        # (the issue #43 machinery — see windowed_cell_config).
        config, windowing = windowed_cell_config(config, window)
        time_range_of = windowing["time_field"]

    leaf_path = shard_leaf_path(store_root, shard_key, window=window["label"] if window else None)
    box: dict = {}
    _write_elapsed = 0.0

    def _leaf():
        if "store" not in box:
            store = open_store(leaf_path, **store_kwargs)
            # overwrite=True: any existing prefix here is either debris from a
            # torn run (D4) or a prior committed write being redone — both are
            # replaced wholesale; per-leaf state never blocks a retry. The
            # overwrite enumeration warns about the prior attempt's coverage
            # sidecar — the ONE foreign key we put there ourselves — so that
            # specific warning is expected and suppressed; anything else in
            # the prefix stays loud (review finding, PR #208 round 2).
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=f"Object at {COVERAGE_SIDECAR}")
                grid.emit_shard_template(store, overwrite=True)
            box["store"] = store
        return box["store"]

    # Sharded leaf output (issue #236): the sharded leaf template bundles each
    # dense array's K inner chunks into ONE ShardingCodec object, so the
    # per-chunk streaming write below would read-modify-write that object K
    # times (the same failure the flat sharded path warns about in
    # ``runner._process_and_write``). Mirror the flat switch: accumulate the K
    # carriers via ``chunk_results`` and write the whole leaf once after the
    # stream (``write_leaf_to_zarr`` — dense + ragged, one object each). The
    # re-added O(shard) dense term is ~1.3 MB at production geometry
    # (parent 11 / child 19: 65,536 cells x ~20 B across the dense arrays) —
    # trivial next to the ragged accumulation below, which this path folds
    # into the same single-write pass.
    sharded = getattr(grid, "sharded", False)
    chunk_results: list | None = [] if sharded else None

    # Ragged fields accumulate across the streamed chunks (leaf-LOCAL blocks)
    # and are written ONCE after the stream (issue #209): the leaf's ragged
    # vlen array is a single ShardingCodec object spanning the shard, so a
    # per-chunk write here would read-modify-write that object K times.
    # Memory bound (review, PR #211): this re-adds an O(shard-payload) term to
    # the otherwise O(chunk) streaming path (issue #91) — at the o8 t-digest
    # scale this fix exists to unlock (sparse NEON o8: 17.6 M centroids × 8 B
    # ≈ 141 MB of held payload; ~200 MB peak through the single write, once
    # per-cell ``bytes``-object overhead and the assembled ~60 MB shard object
    # are counted). Accepted deliberately: workers run 4 GB (issue #193), the
    # dense side keeps its O(chunk) stream-and-free bound, and the
    # accumulation is what deletes the ~K×7-object PUT storm that was ~1/3 of
    # shard wall at CONUS scale (issue #209). If 88S-scale shards or the #148
    # streaming budget ever say otherwise, the escape valve is spilling the
    # ragged field back to per-inner-chunk writes against a regular-chunked
    # vlen array (the unsharded flat layout) — named here, not built.
    ragged_chunks: list = []

    def _write_chunk(block_index, carrier, ragged):
        nonlocal _write_elapsed
        _t0 = time.time() if profile else None
        store = _leaf()
        local = leaf_block_index(grid, block_index, shard_key)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=local)
        if ragged:
            ragged_chunks.append((local, ragged))
        if profile:
            _write_elapsed += time.time() - _t0

    # Occupied-cell sink (issue #200): the worker already holds the shard's
    # populated cell words; collect them here to derive the stamp's coverage.
    occupied: list = []
    _df_out, metadata = process_shard(
        grid,
        int(shard_key),
        granule_urls,
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        handoff=handoff,
        aoi_payload=aoi_payload,
        chunk_results=chunk_results,
        write_chunk=None if sharded else _write_chunk,
        occupied_out=occupied,
        time_range_of=time_range_of,
        profile=profile,
    )
    # Windowed stamp truth (D15): convert the worker's dataset-unit extent to
    # ISO-8601 UTC once, here — the same strings feed the stamp below and the
    # dispatcher's root-summary union (via the returned metadata).
    time_range = None
    if window is not None and metadata.get("time_range") is not None:
        from zagg.windows import iso_time_range

        time_range = iso_time_range(metadata["time_range"], windowing)
        metadata["time_range"] = time_range
    # Sharded leaf: ONE whole-leaf write per array (dense + ragged together,
    # issue #236), after the stream. The leaf template is emitted here (still
    # lazily, via ``_leaf``), so a shard that produced no chunks never creates
    # the ``.zarr/`` prefix — same contract as the streaming path's first
    # ``_write_chunk``.
    if sharded and chunk_results and not metadata.get("error"):
        _t0 = time.time() if profile else None
        write_leaf_to_zarr(chunk_results, _leaf(), grid=grid, shard_key=int(shard_key))
        if profile:
            _write_elapsed += time.time() - _t0
    # Stamp ONLY a fully-written leaf: an errored shard (or one that streamed
    # no chunks) stays unstamped — debris by definition (D4). The stamp is the
    # last write, so its presence certifies everything before it landed — the
    # box payload rides it (zero extra requests), the exact-occupancy bitmap
    # sidecar is PUT just before it (issue #200 phase 2), and both inherit
    # its debris semantics: a torn worker's coverage never becomes visible.
    # The leaf write order is pinned: dense (streamed, or one object each when
    # sharded) -> ragged (one object, issue #209) -> coverage sidecar -> stamp.
    if "store" in box and not metadata.get("error"):
        _t0 = time.time() if profile else None
        if not sharded:
            write_ragged_leaf_to_zarr(ragged_chunks, box["store"], grid=grid)
        words = np.concatenate(occupied) if occupied else None
        if words is not None and words.size == 0:
            words = None
        bitmap = None
        # D14 popcount (issue #246): a fully-occupied subtree stamps
        # ``encoding: "full"``, no sidecar. Gated on windowing (/2 stores
        # only) so schedule-none output stays object-for-object identical to
        # pre-#246 runs (the mortie spec files "full" under /2).
        depth = int(grid.child_order) - int(grid.parent_order)
        full = window is not None and words is not None and np.unique(words).size == 4**depth
        # Depth 0 (child_order == parent_order, a legal one-cell-per-shard
        # config) skips the sidecar: a 1-bit bitmap says nothing the stamp
        # itself doesn't, and encode would raise AFTER the chunk writes,
        # leaving the shard permanently unstampable debris (review finding,
        # PR #208 round 2). The envelope simply omits the pointer — box only.
        if words is not None and not full and depth > 0:
            bitmap = encode_coverage_bitmap(shard_key, words, grid.child_order)
            write_coverage_sidecar(leaf_path, bitmap, **store_kwargs)
        stamp_commit(
            box["store"],
            cells_with_data=metadata.get("cells_with_data", 0),
            granule_count=metadata.get("granule_count", 0),
            coverage=build_coverage(shard_key, words, grid.child_order, bitmap=bitmap, full=full),
            window=window["label"] if window else None,
            time_range=time_range,
        )
        if profile:
            _write_elapsed += time.time() - _t0
    # Write-phase split (issue #249): read/index/aggregate come from
    # ``process_shard``; ``write`` is the leaf write-out above (template +
    # dense chunks + ragged + coverage sidecar + stamp). Same gate as the flat
    # Lambda handler's issue #100 write bracket: only a clean, actually-written
    # shard carries it, so a time-to-failure never lands as a write duration
    # and a no-data shard (no leaf) stays write-less.
    if profile and not metadata.get("error") and "phase_timings" in metadata and "store" in box:
        metadata["phase_timings"]["write"] = _write_elapsed
    return metadata


def _read_json(obj_store, key: str) -> dict | None:
    """GET+parse one small JSON object; ``None`` when it does not exist."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(obj_store, key).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "COMMIT_ATTR",
    "COVERAGE_BOX_SLOTS",
    "COVERAGE_SIDECAR",
    "COVERAGE_SPEC",
    "HIVE_SPEC",
    "HIVE_SPEC_V2",
    "MANIFEST_NAME",
    "ROOT_COVERAGE_NAME",
    "build_coverage",
    "build_manifest",
    "build_root_coverage",
    "check_node_invariant",
    "decode_coverage_bitmap",
    "encode_coverage_bitmap",
    "ensure_manifest",
    "leaf_block_index",
    "process_and_write_hive",
    "read_commit",
    "read_coverage",
    "read_coverage_bitmap",
    "read_manifest",
    "read_root_coverage",
    "root_coverage_words",
    "shard_leaf_path",
    "stamp_commit",
    "write_coverage_sidecar",
    "write_root_coverage",
]
