"""O11 content hashes: the writer-side port of the spec §5 recipe (issue #342).

The logical content hash of a hive leaf is **per-array sha256 over decoded
values** — never stored object bytes, so codec/packaging changes are invisible
by construction while any value change flips the hash (exact bytes, no float
tolerance). The recipe is NOT this module's to adjust: the normative text is
``docs/specification.md`` §5 and the reference implementation is
``moczarr.stats.hash_arrays`` / ``combined_hash`` (espg/moczarr PR #23),
adopted verbatim here and pinned byte-for-byte by the committed conformance
fixtures (``tests/data/spec/*.expected.json``, spec §7).

Recipe summary (§5.2/§5.3):

- fixed-width arrays hash as their full decoded contents, raw C-order
  **little-endian** bytes at the declared dtype;
- vlen (object-dtype) arrays hash per cell in flat C order as
  ``u64le(len(payload)) || payload`` — the length prefix makes the digest
  injective and covers the cell grid, not just the payloads;
- an element with no byte recipe RAISES (a silently wrong digest is worse
  than none);
- ``combined`` = sha256 over the SORTED per-array hex digests joined by
  ``"\\n"``, hashed as ASCII (array names deliberately excluded).

Hashes are recorded in the leaf's D20 stats sidecar under ``content_hashes``
in the §5.3 structured shape (:func:`content_hashes_record`); absence reads
as *unverifiable, not tampered*.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any, Mapping

import numpy as np

logger = logging.getLogger(__name__)

#: Width of the §5.2 vlen recipe's per-cell length prefix (uint64, LE).
VLEN_LENGTH_PREFIX = 8

#: Byte budget for out-of-order rows ONE :class:`StreamingArrayHash` may hold
#: (issue #342 phase 5). The raster sink receives timesteps in *completion*
#: order (``asyncio.as_completed``), so a row arriving before its predecessors
#: must be parked until its turn. The budget bounds that parking — at typical
#: raster leaf geometry (tens of acquisitions x a shard's cells) the whole
#: array is well under it, while a pathological leaf degrades to "no recorded
#: hash" (§5.3 unverifiable, not tampered) instead of eating the streaming
#: write path's memory bound (issues #231/#232).
STREAM_PENDING_MAX_BYTES = 64 * 1024 * 1024


def _element_bytes(element: object) -> bytes:
    """One vlen cell's decoded payload as raw little-endian bytes (§5.2).

    ``variable_length_bytes`` cells decode to :class:`bytes` (zagg's ragged
    payloads and their ``{field}_locations`` siblings). ``None`` — an
    unwritten cell may decode as ``None``, not ``b""`` — is zero-length;
    ``str`` covers a vlen-utf8 future; an ndarray (a typed ``zagg-ragged/2``
    cell, spec §6) normalizes to C-contiguous little-endian bytes at its
    element dtype. Anything else RAISES rather than hashing a ``repr`` — a
    digest that is silently wrong is worse than no digest at all.
    """
    if element is None:
        return b""
    if isinstance(element, (bytes, bytearray, memoryview)):
        return bytes(element)
    if isinstance(element, str):
        return element.encode()
    if isinstance(element, np.ndarray):
        values = np.ascontiguousarray(element)
        if values.dtype.byteorder == ">":
            values = values.astype(values.dtype.newbyteorder("<"))
        return values.tobytes()
    raise ValueError(
        f"vlen element of type {type(element).__name__} has no O11 byte recipe "
        f"(expected bytes, str, ndarray, or None)"
    )


def hash_array(values: np.ndarray) -> str:
    """The §5.2 hash of ONE decoded array (fixed-width or vlen/object dtype)."""
    values = np.ascontiguousarray(values)
    if values.dtype.kind == "O":  # vlen: length-prefixed payloads, flat C order
        digest = hashlib.sha256()
        for element in values.ravel(order="C"):
            payload = _element_bytes(element)
            digest.update(len(payload).to_bytes(VLEN_LENGTH_PREFIX, "little"))
            digest.update(payload)
        return digest.hexdigest()
    if values.dtype.byteorder == ">":  # canonical form is little-endian
        values = values.astype(values.dtype.newbyteorder("<"))
    return hashlib.sha256(values.tobytes()).hexdigest()


def hash_arrays(group: Any, *, staged: Mapping[str, np.ndarray] | None = None) -> dict[str, str]:
    """O11 per-array hashes of every named zarr array beneath ``group``.

    ``group`` is an open zarr v3 group at the leaf ROOT; the scope is
    discovery-based (§5.1): ``members(max_depth=None)``, so the key set is
    whatever named arrays exist beneath the root — data fields, ragged vlen
    payloads and their locations siblings, ``morton``, every coordinate —
    keyed by the array's path relative to the leaf root (e.g. ``"8/morton"``).

    ``staged`` maps array keys to the IN-MEMORY values the writer just wrote
    (the ratified issue #342 hash source: the leaf write path already holds
    each array's full slab, so hashing is a memory-bandwidth pass, no data
    GETs). A staged value is hashed in place of a store read-back; enumerated
    arrays with no staged value (e.g. ``resolution: chunk`` companions, which
    are written per chunk-block rather than as one slab) fall back to reading
    that array back. The two sources are pinned equal by the write-read
    parity test in ``tests/test_content_hash.py`` — the CI guard for the
    dtype-cast/fill drift class §5 exists to catch.

    A staged key matching NO enumerated array warns: the digests stay correct
    (that array was read back) but the staged seam is broken — silent
    key-composition drift between writer and enumeration would otherwise turn
    every hash into a read-back with no observable symptom.
    """
    import zarr

    staged = staged or {}
    hashes: dict[str, str] = {}
    for key, node in group.members(max_depth=None):
        if not isinstance(node, zarr.Array):
            continue
        values = staged[key] if key in staged else node[...]
        hashes[key] = hash_array(np.asarray(values))
    unused = set(staged) - set(hashes)
    if unused:
        logger.warning(
            "staged values for %s match no array beneath the leaf root; those arrays "
            "were hashed from a store read-back instead (staged keys must be array "
            "paths relative to the leaf root, e.g. '8/morton')",
            sorted(unused),
        )
    return hashes


#: Sentinel for "the fill row has not been materialized yet".
_UNSET = object()


class StreamingArrayHash:
    """§5.2 digest of an array assembled from leading-axis rows as they are written.

    The incremental hash source for write paths that never hold the whole
    array (issue #342 phase 5, the raster hive leaf): a ``(time, cells)``
    band is written one timestep at a time, and the concatenation of those
    rows' decoded C-order little-endian bytes IS the array's full decoded
    byte sequence — so a live ``sha256`` fed row by row finalizes to exactly
    the digest :func:`hash_array` would compute over the assembled array. No
    read-back, no whole-array staging.

    Two things the identity depends on, both ENFORCED here rather than
    assumed — a digest that is silently wrong is worse than no digest:

    - **Unwritten rows still count.** The recipe covers the array's full
      logical contents, so a row that never gets a slab contributes its
      **fill** bytes. Rows missing at :meth:`finalize` are filled from
      ``fill_value``; an array with a gap and no reconstructable fill is
      dropped instead of hashed.
    - **Order.** Rows may arrive in any order (the raster sink runs on
      ``asyncio.as_completed``), so out-of-order rows are parked until their
      turn — bounded by :data:`STREAM_PENDING_MAX_BYTES`. A row written
      twice, a row outside the declared extent, a shape/dtype mismatch, or a
      parking overflow all **invalidate** the digest: :meth:`finalize`
      returns ``None`` and :attr:`invalid_reason` says why.

    Thread-safe: the raster sink hands slabs off to worker threads when
    ``write_buffer > 1``.
    """

    def __init__(self, shape, dtype, fill_value=None):
        self.shape = tuple(int(s) for s in shape)
        self.dtype = np.dtype(dtype)
        self.fill_value = fill_value
        self.invalid_reason: str | None = None
        self._row_shape = self.shape[1:]
        self._n_rows = self.shape[0] if self.shape else 0
        self._digest = hashlib.sha256()
        self._next = 0
        self._pending: dict[int, np.ndarray] = {}
        self._pending_bytes = 0
        self._fill_row: Any = _UNSET
        self._lock = threading.Lock()

    def _invalidate(self, reason: str) -> None:
        if self.invalid_reason is None:
            self.invalid_reason = reason
        self._pending.clear()
        self._pending_bytes = 0

    def _row_bytes(self, values: np.ndarray) -> bytes:
        values = np.ascontiguousarray(values)
        if values.dtype.byteorder == ">":  # canonical form is little-endian
            values = values.astype(values.dtype.newbyteorder("<"))
        return values.tobytes()

    def update(self, index: int, values) -> None:
        """Record the row written at leading-axis ``index`` (any order)."""
        with self._lock:
            if self.invalid_reason is not None:
                return
            index = int(index)
            values = np.asarray(values)
            if values.shape != self._row_shape or values.dtype != self.dtype:
                self._invalidate(
                    f"row {index} is {values.shape}/{values.dtype}, expected "
                    f"{self._row_shape}/{self.dtype}"
                )
                return
            if not 0 <= index < self._n_rows:
                self._invalidate(f"row {index} is outside the array's {self._n_rows} rows")
                return
            if index < self._next or index in self._pending:
                self._invalidate(f"row {index} was written more than once")
                return
            if index > self._next:
                self._pending[index] = values
                self._pending_bytes += values.nbytes
                if self._pending_bytes > STREAM_PENDING_MAX_BYTES:
                    self._invalidate(
                        f"out-of-order rows exceeded the {STREAM_PENDING_MAX_BYTES}-byte "
                        f"parking budget"
                    )
                return
            self._digest.update(self._row_bytes(values))
            self._next += 1
            while self._next in self._pending:  # drain rows this one unblocked
                row = self._pending.pop(self._next)
                self._pending_bytes -= row.nbytes
                self._digest.update(self._row_bytes(row))
                self._next += 1

    def finalize(self) -> str | None:
        """The array's §5.2 digest, or ``None`` when it cannot be stood behind."""
        with self._lock:
            if self.invalid_reason is not None:
                return None
            for index in range(self._next, self._n_rows):
                row = self._pending.pop(index, None)
                if row is not None:
                    self._pending_bytes -= row.nbytes
                    self._digest.update(self._row_bytes(row))
                    continue
                if self._fill_row is _UNSET:
                    self._fill_row = (
                        None
                        if self.fill_value is None
                        else np.full(self._row_shape, self.fill_value, dtype=self.dtype).tobytes()
                    )
                if self._fill_row is None:
                    self._invalidate(
                        f"row {index} was never written and the array declares no "
                        f"fill_value to reconstruct it"
                    )
                    return None
                self._digest.update(self._fill_row)
            self._next = self._n_rows
            return self._digest.hexdigest()


def combined_hash(hashes: Mapping[str, str]) -> str:
    """The O11 combined hash: sha256 of the sorted per-array hex digests.

    Serialization pinned by §5.3 (and the committed fixtures): the digests
    sorted lexically and joined with ``"\\n"``, hashed as ASCII — array
    *names* deliberately excluded ("hash of the sorted per-array hashes"),
    so ``combined`` is immune to enumeration order.
    """
    return hashlib.sha256("\n".join(sorted(hashes.values())).encode()).hexdigest()


def content_hashes_record(hashes: Mapping[str, str]) -> dict:
    """The §5.3 structured ``content_hashes`` sidecar record.

    The ``arrays`` map is key-sorted so a regenerated record diffs cleanly
    (``combined`` is order-immune either way — it sorts the digests).
    """
    return {"arrays": dict(sorted(hashes.items())), "combined": combined_hash(hashes)}


def staged_record(store: Any, staged: Mapping[str, np.ndarray], what: str) -> dict | None:
    """The §5.3 record over ``staged`` (read-back for the rest), or ``None``.

    The one entry point for every stamp writer that already holds its slabs
    (hive leaf, overview, stage overview, leaf column, stage column):
    computed before the stamp so the record rides the stamp itself (issue
    #580). Fail-open (D9 telemetry posture) — a failure logs and returns
    ``None``, and the caller stamps without the key, which §5.3 reads as
    unverifiable, never tampered.

    ``what`` is a log label only (the failing artifact, in whatever grammar
    the call site names it); nothing parses it.
    """
    try:
        import zarr

        group = zarr.open_group(store, path="", mode="r", zarr_format=3)
        return content_hashes_record(hash_arrays(group, staged=staged))
    except Exception as e:
        logger.warning(f"{what}: O11 content hashing failed (fail-open, issue #342): {e}")
        return None
