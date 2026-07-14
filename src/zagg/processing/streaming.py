"""Streaming buffered aggregation for mergeable reducers (issue #148, phase 4).

The pooled worker path holds every granule's filtered reads until the shard is
fully read (``_concat_and_group`` in ``worker.py``), then digests once. At 88S
scale (a 5,620-granule stress shard, tens of millions of photons) that pool
alone blows the 2 GB Lambda cap, so ``aggregation.streaming`` trades one-shot
pooling for **bounded buffers + incremental merges**: reads accumulate for
``buffer_granules`` granules, are grouped and reduced into per-cell running
state, and released — peak memory is one buffer plus the running digest state,
independent of the shard's granule count.

Only *mergeable* reducers can stream, so the mode is deliberately narrow and
validated up front (:func:`validate_streaming`):

- dense scalar fields with ``function: len`` — merged by summation (exact,
  byte-identical to pooled);
- ragged fields with ``function: zagg.stats.tdigest.build_tdigest`` — merged by
  :func:`zagg.stats.tdigest.merge_tdigests` under the field's own ``delta``
  (approximate within t-digest guarantees; **exactly** the pooled result when
  the shard fits in a single buffer, since one flush == one pooled build).

Everything else (expressions, vector fields, ``resolution: chunk`` companions,
``chunk_precompute``) has no incremental-merge story and raises. A fixed-size
buffer is used rather than pure granule-by-granule updates because each flush
costs one merge round over the touched cells (~10 µs/cell, see
``zagg/stats/tdigest.py``): near 88S a tangent-running granule touches most of
the shard's cells, so N/B rounds instead of N keeps merge CPU in seconds.
"""

from __future__ import annotations

import numpy as np

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_chunk_precompute,
    get_output_signature,
)
from zagg.stats.tdigest import _DEFAULT_DELTA, build_tdigest, merge_tdigests

#: The one ragged reducer with a merge law wired up.
_TDIGEST_FUNCTION = "zagg.stats.tdigest.build_tdigest"


def get_streaming(config: PipelineConfig) -> dict | None:
    """Return the ``aggregation.streaming`` block, or ``None`` (pooled path).

    The block is ``{"buffer_granules": int}``; ``buffer_granules`` must be a
    positive int. Absent block -> ``None`` so existing configs are untouched.
    """
    block = config.aggregation.get("streaming")
    if block is None:
        return None
    if not isinstance(block, dict):
        raise ValueError("aggregation.streaming must be a mapping, e.g. {buffer_granules: 50}")
    buffer_granules = block.get("buffer_granules", 50)
    if not isinstance(buffer_granules, int) or buffer_granules < 1:
        raise ValueError(
            f"aggregation.streaming.buffer_granules must be a positive int "
            f"(got {buffer_granules!r})"
        )
    return {"buffer_granules": buffer_granules}


def validate_streaming(config: PipelineConfig) -> None:
    """Reject configs whose reducers have no incremental-merge law.

    Raises ``ValueError`` naming every offending field so a config error reads
    as one message, not a peel-the-onion loop.
    """
    problems: list[str] = []
    if get_chunk_precompute(config):
        problems.append("chunk_precompute is chunk-scoped and cannot stream")
    for name, meta in get_agg_fields(config).items():
        sig = get_output_signature(meta)
        if "expression" in meta:
            problems.append(f"field '{name}': expression fields cannot stream")
        elif sig["resolution"] != "cell":
            problems.append(f"field '{name}': resolution '{sig['resolution']}' cannot stream")
        elif sig["kind"] == "ragged":
            if sig["location"] is not None:
                # The located channel (issue #87) has a merge law (located
                # merge_tdigests), but the streaming state does not thread
                # per-cell locations yet — reject rather than silently dropping
                # the channel from the store.
                problems.append(
                    f"field '{name}': located ragged fields (location: "
                    f"{sig['location']!r}) cannot stream yet"
                )
            elif meta.get("function") != _TDIGEST_FUNCTION:
                problems.append(
                    f"field '{name}': ragged function {meta.get('function')!r} has no "
                    f"merge law (only {_TDIGEST_FUNCTION})"
                )
            elif tuple(sig["inner_shape"]) != (2,):
                # The pooled path validates payload shape per cell via
                # _coerce_ragged_value; the buffered path stores merged digests
                # directly, so a mis-declared inner_shape must fail HERE, not
                # silently disagree with the store schema readers key on.
                problems.append(
                    f"field '{name}': tdigest payloads are (k, 2) centroids; "
                    f"declared inner_shape {list(sig['inner_shape'])} cannot stream"
                )
        elif sig["kind"] == "scalar":
            # ``count`` is the pooled path's alias of ``len`` (aggregate.py);
            # both merge by summation.
            if meta.get("function") not in ("len", "count"):
                problems.append(
                    f"field '{name}': scalar function {meta.get('function')!r} is not "
                    "mergeable (only 'len'/'count')"
                )
        else:
            problems.append(f"field '{name}': kind '{sig['kind']}' cannot stream")
    if problems:
        raise ValueError(
            "aggregation.streaming is on but the config is not streamable: " + "; ".join(problems)
        )


class StreamingAggregator:
    """Bounded-buffer worker state: accumulate B granules, merge, release.

    Drives the same ``_concat_and_group`` grouping as the pooled path per
    flush, then folds each populated cell into running per-cell state:
    ``len`` fields by summation, tdigest fields via ``merge_tdigests``.
    ``chunk_outputs`` then emits the exact ``(stats_arrays, ragged_payloads,
    ragged_cell_indices, cells_with_data)`` shape ``_aggregate_chunk_cells``
    returns, so the worker's carrier/ragged construction is shared verbatim.
    """

    def __init__(self, config: PipelineConfig, grid, handoff: str, buffer_granules: int):
        validate_streaming(config)
        self.grid = grid
        self.handoff = handoff
        self.buffer_granules = buffer_granules
        agg_fields = get_agg_fields(config)
        self._count_fields: list[str] = []
        self._digest_fields: dict[str, tuple[str, int]] = {}  # name -> (source, delta)
        for name, meta in agg_fields.items():
            if get_output_signature(meta)["kind"] == "ragged":
                delta = int((meta.get("params") or {}).get("delta", _DEFAULT_DELTA))
                # Mirror the pooled path's source default (aggregate.py:
                # ``meta.get("source") or value_col`` with the worker's
                # ``value_col="h_li"``) so a config that runs pooled doesn't die
                # here with a bare KeyError when streaming is turned on.
                self._digest_fields[name] = (meta.get("source") or "h_li", delta)
            else:
                self._count_fields.append(name)
        self.counts: dict[int, int] = {}
        self.digests: dict[str, dict[int, np.ndarray]] = {n: {} for n in self._digest_fields}
        self.n_obs_total = 0
        self.flushes = 0
        self._buffer: list = []
        self._buffered_granules = 0

    def add_read(self, chunk) -> None:
        """Buffer one group read (the carrier ``_read_group`` returned)."""
        self._buffer.append(chunk)

    def granule_done(self) -> None:
        """Mark one granule fully read; flush when the buffer is full."""
        self._buffered_granules += 1
        if self._buffered_granules >= self.buffer_granules:
            self.flush()

    def flush(self) -> None:
        """Group the buffered reads and merge them into the running state."""
        if not self._buffer:
            self._buffered_granules = 0
            return
        from zagg.processing.aggregate import _concat_and_group

        col_arrays, cell_to_slice, n_obs = _concat_and_group(self._buffer, self.grid, self.handoff)
        self.n_obs_total += n_obs
        for cell, (start, end) in cell_to_slice.items():
            self.counts[cell] = self.counts.get(cell, 0) + (end - start)
            for name, (source, delta) in self._digest_fields.items():
                fresh = build_tdigest(col_arrays[source][start:end], delta=delta)
                held = self.digests[name].get(cell)
                self.digests[name][cell] = (
                    fresh if held is None else merge_tdigests(held, fresh, delta=delta)
                )
        self.flushes += 1
        self._buffer = []
        self._buffered_granules = 0

    @property
    def empty(self) -> bool:
        """True when no observation ever survived filtering (mirror of no reads)."""
        return not self.counts and not self._buffer

    def chunk_outputs(self, children, agg_fields: dict):
        """Emit one chunk's aggregation outputs from the running state.

        Same return contract as ``_aggregate_chunk_cells``: dense arrays are
        preallocated to the field's dtype/fill and filled per populated cell;
        ragged payloads are collected with their chunk-local cell indices.
        """
        children = np.asarray(children)
        n_cells = len(children)
        stats_arrays: dict[str, np.ndarray] = {}
        for name in self._count_fields:
            meta = agg_fields[name]
            dtype = np.dtype(meta.get("dtype", "float32"))
            if meta.get("fill_value", "NaN") == "NaN":
                stats_arrays[name] = np.full(n_cells, np.nan, dtype=dtype)
            else:
                stats_arrays[name] = np.zeros(n_cells, dtype=dtype)
        ragged_payloads: dict[str, list] = {n: [] for n in self._digest_fields}
        ragged_cell_indices: dict[str, list[int]] = {n: [] for n in self._digest_fields}

        cells_with_data = 0
        for i, child in enumerate(children):
            cell = int(child)
            count = self.counts.get(cell)
            if count is None:
                # Empty cell: dense fields keep their fill; ``len`` over an empty
                # slice is 0 on the pooled path, so mirror it explicitly.
                for name in self._count_fields:
                    stats_arrays[name][i] = 0
                continue
            cells_with_data += 1
            for name in self._count_fields:
                stats_arrays[name][i] = count
            for name in self._digest_fields:
                digest = self.digests[name].get(cell)
                if digest is not None and digest.size > 0:
                    ragged_payloads[name].append(digest)
                    ragged_cell_indices[name].append(i)
        return stats_arrays, ragged_payloads, ragged_cell_indices, cells_with_data
