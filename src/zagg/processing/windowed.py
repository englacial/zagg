"""Per-window read sinks for the bulk multi-window worker (issue #586 phase 2).

One invoke per SHARD reads its granules once and emits one leaf per time
window. The read loop in :func:`zagg.processing.worker.process_shard` hands
each group read to :class:`WindowBins`, which splits it on the declared
``time_field`` into the run's windows — the same half-open ``[start, end)``
predicate the per-window fan-out injects as a filter pair
(:func:`zagg.config.window_time_filters`), applied to the chunk the read
returned instead of inside the read — and parks each part in that window's
sink: a plain list on the pooled path, or that window's own
``StreamingAggregator`` / ``SpillAggregator`` under ``aggregation.streaming``.
The worker then aggregates the sinks one window at a time, so the pooled
slab, the outputs and the spill read-back are bounded by ONE window, while
the read itself is the shard's single pass.

Byte-identity with the ``(shard, window)`` fan-out: a window's sink receives
exactly the rows the fan-out unit's read would have kept — same granules
(``granules``, the window's membership, decides which granules' rows it
takes and which count toward its buffer cadence, so a streaming flush lands on the same granule boundary),
same group order, same row order — so the pooled aggregation and the
single-block spill regime reproduce the fan-out's leaf bit for bit. The one
bulk-only departure is the shared ``/tmp`` cap below.
"""

from __future__ import annotations

import os
import tempfile

import numpy as np

from zagg.processing.spill import SPILL_TMP_FRACTION, SpillAggregator


def _column(chunk, name: str) -> np.ndarray:
    """One column of a group read as numpy, whichever carrier holds it."""
    if hasattr(chunk, "column_names"):
        return chunk.column(name).combine_chunks().to_numpy()
    return chunk[name].to_numpy()


def _take(chunk, mask: np.ndarray):
    """The rows of ``chunk`` where ``mask`` is true, same carrier, same order."""
    if hasattr(chunk, "column_names"):
        from arro3.core import Table

        return Table.from_pydict({n: _column(chunk, n)[mask] for n in chunk.column_names})
    return chunk[mask].reset_index(drop=True)


def bin_chunk(chunk, time_field: str, windows: list[dict]):
    """Yield ``(label, rows)`` for each window ``chunk`` has rows in.

    ``windows`` entries are the dispatch payloads ``{"label", "start",
    "end"}`` with bounds in dataset units; membership is ``start <= t < end``
    on the ``time_field`` column (:func:`zagg.processing.read._predicate_mask`
    semantics: ``ge`` then ``lt``). A chunk wholly inside one window is
    yielded as-is, uncopied; a straddler is split into one part per window it
    touches; a window with no rows is skipped, as the fan-out's filtered read
    drops an empty group (``_read_group`` returns ``None`` there).
    """
    t = _column(chunk, time_field)
    for w in windows:
        mask = (t >= w["start"]) & (t < w["end"])
        n = int(mask.sum())
        if n == 0:
            continue
        yield w["label"], (chunk if n == t.size else _take(chunk, mask))


class WindowBins:
    """The per-window sinks one bulk shard read feeds.

    ``make_buffered`` builds one streaming aggregator per window (``None`` on
    the pooled path, where each window is a list of reads). ``granules`` on a
    window payload names the granules (indices into the unit's granule list)
    that belong to it; ``granule_done`` advances only those windows' buffer
    cadence, reproducing the fan-out unit's flush boundaries. A payload
    without ``granules`` counts every granule (the legacy-shardmap rule: no
    spans, every granule rides every window). A window with membership is
    flushed once the read passes its last member, so the resident tail
    buffers are those of the windows still being read, not all N; under
    ``mode: merge`` each window's running state stays resident until its
    leaf is written.

    Under spill, N aggregators fill N blocks side by side on one ``/tmp``.
    Each keeps the fan-out unit's own threshold (so its fold regime matches
    the fan-out's), and on top of that the bins enforce ONE shared cap on the
    open blocks' total — :data:`~zagg.processing.spill.SPILL_TMP_FRACTION` of
    the free space at construction, the same fraction a single aggregator's
    default threshold caps itself at — by closing the largest open block when
    the total crosses it. That close is the one bulk-only fold: it happens
    only where the windows' combined spill outgrows what one unit's block may
    hold, and it folds under the same law-equivalent block merge the
    threshold crossing does.
    """

    def __init__(self, windows: list[dict], time_field: str, make_buffered=None):
        self.windows = list(windows)
        self.time_field = time_field
        self.reads: dict[str, list] = {w["label"]: [] for w in self.windows}
        self.buffered: dict[str, object] = {}
        if make_buffered is not None:
            self.buffered = {w["label"]: make_buffered() for w in self.windows}
        self._members: dict[str, set | None] = {
            w["label"]: (set(w["granules"]) if w.get("granules") is not None else None)
            for w in self.windows
        }
        # A window's last member granule: once the (index-ordered) read passes
        # it, no more of the window's rows can come, so its tail buffer is
        # flushed there — the fan-out unit's end-of-read flush, at the same
        # granule boundary — instead of parking until the shard's read ends.
        self._last = {label: (max(m) if m else None) for label, m in self._members.items()}
        self._drained: set = set()
        spills = [a for a in self.buffered.values() if isinstance(a, SpillAggregator)]
        self._tmp_cap = None
        if spills:
            st = os.statvfs(spills[0].tmp_dir or tempfile.gettempdir())
            self._tmp_cap = int(SPILL_TMP_FRACTION * st.f_bavail * st.f_frsize)

    def add_reads(self, reads, index: int) -> None:
        """Bin every group read of granule ``index`` into its windows' sinks.

        Only the windows the granule is a MEMBER of take its rows — the
        fan-out reads a granule for exactly those windows — so a loose or
        instant catalog span never lands rows in a leaf whose recorded
        granule set does not name the granule.
        """
        windows = [w for w in self.windows if self._member(w["label"], index)]
        for chunk in reads:
            for label, part in bin_chunk(chunk, self.time_field, windows):
                if self.buffered:
                    self.buffered[label].add_read(part)
                else:
                    self.reads[label].append(part)

    def _member(self, label: str, index: int) -> bool:
        members = self._members[label]
        return members is None or index in members

    def granule_done(self, index: int) -> None:
        """Mark one granule read for the windows it belongs to (streaming cadence)."""
        for label, agg in self.buffered.items():
            if self._member(label, index):
                agg.granule_done()
            last = self._last[label]
            if last is not None and index >= last and label not in self._drained:
                agg.flush()
                self._drained.add(label)
        self._enforce_tmp_cap()

    def flush(self) -> None:
        """Drain every window's tail buffer (the end-of-read flush)."""
        for agg in self.buffered.values():
            agg.flush()

    def _enforce_tmp_cap(self) -> None:
        if self._tmp_cap is None:
            return
        spills = [a for a in self.buffered.values() if isinstance(a, SpillAggregator)]
        if sum(a.open_block_bytes for a in spills) >= self._tmp_cap:
            largest = max(spills, key=lambda a: a.open_block_bytes)
            if largest.open_block_bytes:
                largest.close_block()

    def sink(self, label: str) -> tuple[list, object | None]:
        """``(reads, buffered)`` for one window — the pair the aggregate tail takes."""
        return self.reads.get(label, []), self.buffered.get(label)

    def release(self, label: str) -> None:
        """Free one window's reads (and spill files) once its leaf is written."""
        self.reads[label] = []
        agg = self.buffered.pop(label, None)
        if agg is not None and hasattr(agg, "close"):
            agg.close()

    def close(self) -> None:
        """Release every window still held (cleanup on any exit path)."""
        for label in list(self.buffered):
            self.release(label)
        self.reads = {label: [] for label in self.reads}
