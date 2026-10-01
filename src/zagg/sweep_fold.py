"""The chunk-streamed stage fold (issue #586 phase 4; kernels from issue #384).

:mod:`zagg.sweep_stage` owns one stage worker — the planner, the column
reader, the writers; this module owns the two fold kernels that worker runs,
in the form the 2026-09-26 amendment on issue #586 ruled: **block by block
over the output range**. Every output cell's fold is independent — a gather
assigns a child's span, a merge k-way merges the children's partials for that
cell — so a level is folded one block of output cells at a time: read only
the child members covering the block, fold, hand the block on, free. Nothing
is spilled to disk: the inputs already live in the store and are re-read per
block, so a spill would only add billed ephemeral storage.

What that bounds. Before this module a fold densified a whole level: one slab
per field for all ``4^(r - k)`` output cells, each child member read whole
(``group[name][:]``) before it was assigned or merged. The 0.55 fleet's
order-6 stage died at 4,094 MB on 64 unwindowed leaves that way. Resident
inputs are now one block's: :data:`STAGE_BLOCK_ORDER` output cells of a
gather, or the source cells of the output cells one block covers in a merge
(never fewer than one output cell's ``factor`` sources — the flat k-way law
of the merge-source ruling folds a cell in ONE call, so that is a floor, not
a choice).

The block is aligned to the **stage column's own zarr chunking**: a column
group wider than one block is written on regular inner chunks of exactly one
block (:func:`zagg.sweep_stage.write_stage_column`), so the block a writer
emits is one chunk object and the block a parent reads is one chunk object.
A ladder overview stays the single chunk it always was (its extent is the
ladder depth, 256 cells at the reference geometry; the Icechunk level arrays
of spec §11 reference it whole), so its fold assembles one output chunk from
streamed inputs.

Values are unchanged by the block size: a block is a set of whole output
cells, each folded from exactly the sources the whole-level fold gave it, in
the same order. ``content_hash``, ``generation`` and ``source_children``
accumulate across blocks and are computed once at the end.
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)

#: One fold block, as a HEALPix order difference: ``4 ** STAGE_BLOCK_ORDER``
#: cells (1,024). It is also the inner-chunk extent of a streamed stage
#: column group, so writer and reader blocks are whole chunk objects. At the
#: reference geometry (shard 9, relay 11) an order-6 column's relay group is
#: exactly one block and an order-3 column's is 64. A power of four so blocks
#: nest in every child span and merge factor.
STAGE_BLOCK_ORDER = 5


def block_cells() -> int:
    """Cells per fold block (read at call time — the tests narrow it)."""
    return 4 ** int(STAGE_BLOCK_ORDER)


class FoldMeter:
    """Resident-input accounting for one unit's folds (issue #586 phase 4).

    Counts the source cells a fold block has read and not yet released; the
    block's inputs are released together when its outputs are handed on, so
    :attr:`peak_cells` is the largest single block's inputs — the number the
    streamed fold exists to bound, and what the stage record reports
    (``fold_peak_cells``) so a fleet run can be read against it.
    """

    def __init__(self):
        self.blocks = 0
        self.cells_read = 0
        self.peak_cells = 0
        self._held = 0

    def hold(self, values) -> None:
        if values is None:
            return
        n = int(len(values))
        self._held += n
        self.cells_read += n
        self.peak_cells = max(self.peak_cells, self._held)

    def release(self) -> None:
        """A block's outputs were handed on: its inputs are no longer held."""
        self.blocks += 1
        self._held = 0


def _fetch(reader, res: int, name: str, rng, meter):
    """One member read: whole (``rng is None``) or the ``[a, b)`` cell range."""
    values = reader.read(res, name) if rng is None else reader.read_range(res, name, *rng)
    if meter is not None:
        meter.hold(values)
    return values


def _companion_group(reader, res: int, name: str, declared: list, rng=None, meter=None):
    """One contributor's ``{array: values}`` at ``res``, or ``None`` if refused.

    A field and EVERY companion it declares are read together and validated
    before any of them is used (issue #410). ``_ColumnReader.read`` returns
    ``None`` per ARRAY, not per member — its documented schema-evolution
    contract — so a column written before ``location:``/``temporal:`` joined the
    declaration reads its payload fine and its siblings as ``None``. That is not
    under-coverage: the words are exact only *given* the centroid partition the
    payload describes (spec §9.1/§8.3/§2.3), so a payload gathered or folded
    without its words is **corruption**, not a missing member. All absent
    together is the honest schema-evolution case and reads as fill.

    Returns ``None`` for a partial group; the caller counts the contributor
    unreadable and skips it, which is the posture
    ``sweep_overview._fold_node`` takes at a leaf missing a sibling (skip
    loudly, ``failed += 1``) rather than aborting the level — a single stale
    child column must not take a whole stage down.

    ``rng`` is the ``[a, b)`` cell range of a block read (``None`` reads the
    member whole). Presence is a property of the ARRAY, not of the range, so
    every block of one contributor reaches the same verdict.
    """
    arrays = {name: _fetch(reader, res, name, rng, meter)}
    for _kwarg, sibling in declared:
        arrays[sibling] = _fetch(reader, res, sibling, rng, meter)
    present = [k for k, v in arrays.items() if v is not None]
    if not present or len(present) == len(arrays):
        return arrays
    logger.warning(
        f"stage sweep: column {reader.path} carries {sorted(present)} at resolution {res} "
        f"but not {sorted(set(arrays) - set(present))}; counting the contributor "
        f"unreadable rather than writing a payload and channels that describe different "
        f"partitions (spec §9.1/§8.3, §1.1)"
    )
    return None


def _source_counts(rows: list, broken=()) -> tuple:
    """``(folded, missing, unreadable)`` over the dense child range.

    A child counts **folded** when at least one of its windows delivered a
    usable column, **missing** when every window's column is cleanly absent
    (never generated, or a fleet still in flight), and **unreadable**
    otherwise — the distinction the two counters must not launder into one.

    ``broken`` is the set of ``(child index, window index)`` contributors a
    fold refused MID-READ: a located payload present without its §9 channel
    (:func:`_companion_group`). Such a contributor is not usable, exactly as an
    unreadable column is not, so it is classified here rather than counted as
    a clean fold — the counters are what ``source_children`` records, and an
    artifact folded without a contributor must say so (spec §4.5).
    """
    from zagg.sweep_stage import _is_reader

    folded = missing = unreadable = 0
    for i, row in enumerate(rows):
        if row is None:
            continue
        if any(_is_reader(r) and (i, w) not in broken for w, r in enumerate(row)):
            folded += 1
        elif all(r is None for r in row):
            missing += 1
        else:
            unreadable += 1
    return folded, missing, unreadable


def _companions(fields: dict) -> dict:
    """``{field: [(kernel kwarg, sibling array), ...]}`` for the fields that declare any."""
    from zagg.sweep_overview import field_companions

    return {
        name: field_companions(name, meta)
        for name, meta in fields.items()
        if field_companions(name, meta)
    }


def broken_groups(rows: list, fields: dict, *, res: int) -> set:
    """The gather contributors :func:`_companion_group` will refuse at ``res``.

    Presence only — one metadata open per array, which the block reads then
    reuse — so a writer can know a member's ``source_children`` BEFORE it
    streams a single block (:func:`zagg.sweep_stage.write_stage_column` must
    not clear an existing column for a relay member that folds nothing).
    """
    from zagg.sweep_stage import _is_reader

    broken: set = set()
    for i, row in enumerate(rows):
        reader = row[0] if row is not None else None
        if not _is_reader(reader):
            continue
        for name, declared in _companions(fields).items():
            present = [reader.has(res, n) for n in (name, *(sib for _kw, sib in declared))]
            if any(present) and not all(present):
                broken.add((i, 0))
    return broken


# ---------------------------------------------------------------------------
# The gather: concatenation of child members, one block of output cells at a
# time.
# ---------------------------------------------------------------------------


def _gather_block(rows, fields, companions, *, res, span, lo, hi, broken, meter) -> dict:
    """Output cells ``[lo, hi)`` of a gather: each covering child's own range."""
    from zagg.sweep_overview import _empty_slab
    from zagg.sweep_stage import _is_reader

    slabs = {name: _empty_slab(meta, hi - lo) for name, meta in fields.items()}
    for declared in companions.values():
        for _kwarg, sibling in declared:
            slabs[sibling] = np.full(hi - lo, b"", dtype=object)
    for i in range(lo // span, min(len(rows), -(-hi // span))):
        row = rows[i]
        if row is None:
            continue
        reader = row[0]
        if not _is_reader(reader):
            continue
        a, b = max(lo, i * span), min(hi, (i + 1) * span)
        # A block that covers the child whole reads it whole — the one read a
        # single-chunk member has, and the shape the pre-streaming fold used.
        rng = None if (a, b) == (i * span, (i + 1) * span) else (a - i * span, b - i * span)
        grouped: dict = {}
        skip: set = set()
        for name, declared in companions.items():
            group = _companion_group(reader, res, name, declared, rng, meter)
            if group is None:
                # EVERY half stays fill: the child's span for this field reads
                # as under-covered, never as a payload with an absent channel.
                broken.add((i, 0))
                skip |= {name, *(sib for _kw, sib in declared)}
                continue
            grouped.update(group)
        for name in list(slabs):
            if name in skip:
                continue
            values = grouped[name] if name in grouped else _fetch(reader, res, name, rng, meter)
            if values is not None:
                slabs[name][a - lo : b - lo] = values
    return slabs


def iter_gather(rows, fields, *, res, span, n_out, broken, block=None, meter=None):
    """Yield ``(lo, hi, slabs)`` for a gather, one block of output cells each.

    The streamed form the stage column writer drives: it writes each block as
    one chunk and drops it. ``broken`` is the caller's accumulator — the
    ``(child, window)`` contributors refused mid-read, which feed
    :func:`_source_counts` once the last block is in.
    """
    companions = _companions(fields)
    step = block_cells() if block is None else int(block)
    for lo in range(0, n_out, step):
        hi = min(lo + step, n_out)
        slabs = _gather_block(
            rows, fields, companions, res=res, span=span, lo=lo, hi=hi, broken=broken, meter=meter
        )
        yield lo, hi, slabs
        if meter is not None:
            meter.release()


def _joined(parts: list) -> dict:
    """Per-block slabs joined along the cells axis (one block: returned as is)."""
    if len(parts) == 1:
        return parts[0]
    return {name: np.concatenate([p[name] for p in parts]) for name in parts[0]}


def _gather_slabs(
    rows: list, fields: dict, *, res: int, span: int, n_out: int, block=None, meter=None
) -> tuple:
    """Concatenate child members at ``res`` — gen-1 content, untouched.

    ``rows`` is the DENSE rank-ordered child range: ``None`` for a child no
    candidate leaf inhabits (genuinely empty — fill, uncounted), else the
    child's reader row (gathers are per-window by construction, one reader,
    itself ``None`` when the candidate's column is missing — under-coverage,
    counted). Payloads are ASSIGNED, never decoded or re-folded — the
    acceptance contract that gather levels carry gen-1 bytes untouched.
    Returns ``(slabs, folded, missing, unreadable, demotions)`` — the last
    always empty here because a gather FOLDS nothing, so the packed rail has
    no site to fire in; NOT because a gather cannot relay the half-paired
    shape the rail exists for. It can, and the residual is the laundering
    case named at the return.

    A located field's sibling is gathered under the same rule as its payload
    (ruling 4 on issue #410): a gather ASSIGNS gen-1 bytes, so the pair stays
    row-aligned by construction — **given that both arrays are present**, which
    is the one thing the read does not establish. The pair is therefore read
    and validated (:func:`_companion_group`) BEFORE anything is assigned, so a
    child carrying one half contributes neither and is counted unreadable
    rather than half-applied into the span.

    The whole level, assembled from :func:`iter_gather`'s blocks: what a
    ladder overview (one zarr chunk) is written from. The inputs are held one
    block at a time; the output is the level.
    """
    broken: set = set()
    parts = [
        slabs
        for _lo, _hi, slabs in iter_gather(
            rows, fields, res=res, span=span, n_out=n_out, broken=broken, block=block, meter=meter
        )
    ]
    # A gather ASSIGNS gen-1 bytes, so the packed guard rail — a fold-site
    # check — has no site to fire in and the demotions slot is empty (issue
    # #518), kept so every fold arm returns one shape. Empty because nothing
    # folds here, NOT because a gather cannot produce the half-paired shape
    # the rail exists for: a source column carrying the ``of`` digest without
    # the word relays a PRESENT, all-fill word array beside a populated
    # divisor (the packed pair is not validated the way ``_companion_group``
    # validates a located one), which the next rung's ``_merge_slabs`` then
    # reads as legitimate ``(0, n)`` parts — diluting the lane fractions
    # instead of blanking them, with no rail fired and nothing in any attrs.
    # That relay laundering predates issue #518 and stands as a question on
    # its PR; the fix belongs beside ``_companion_group``'s
    # pairing logic, not in this observability path (review finding).
    return (_joined(parts), *_source_counts(rows, broken), [])


# ---------------------------------------------------------------------------
# The merge: a flat k-way fold of the gen-1 tier, one block of output cells
# at a time.
# ---------------------------------------------------------------------------


def _pieces(rows, *, src_per_child, s_lo, s_hi, step):
    """``(child, row, a, b)`` — child-local source ranges covering ``[s_lo, s_hi)``.

    In rank order, each at most ``step`` cells: the reads one merge block
    makes. ``step``, ``src_per_child`` and every merge factor are powers of
    four, so a piece starts on a chunk boundary of the child's own member and
    a chunk is never read twice.
    """
    for i in range(s_lo // src_per_child, min(len(rows), -(-s_hi // src_per_child))):
        row = rows[i]
        if row is None:
            continue
        c_lo = max(s_lo, i * src_per_child) - i * src_per_child
        c_hi = min(s_hi, (i + 1) * src_per_child) - i * src_per_child
        for a in range(c_lo, c_hi, step):
            yield i, row, a, min(a + step, c_hi)


def _range(a: int, b: int, src_per_child: int):
    """The read range of a piece: ``None`` (whole member) when it is the child."""
    return None if (a, b) == (0, src_per_child) else (a, b)


def _merge_block(
    rows, fields, *, res_src, src_per_child, factor, lo, hi, windows, step, state, meter
) -> dict:
    """Output cells ``[lo, hi)`` of a merge, from the sources that cover them."""
    from zagg.stats.composition import merge_composition_kway
    from zagg.sweep_overview import (
        DEMOTION_DIVISOR_MISSING,
        DEMOTION_WORD_MISSING,
        _empty_slab,
        combine_dense,
        decode_digest,
        field_companions,
        fold_dense,
        fold_digests,
        note_demotion,
        overview_fold_delta,
        payload_weight,
    )
    from zagg.sweep_stage import _is_reader

    broken, demoted, noted = state["broken"], state["demoted"], state["noted"]
    s_lo, s_hi = lo * factor, hi * factor
    span = dict(src_per_child=src_per_child, s_lo=s_lo, s_hi=s_hi, step=step)
    slabs: dict = {}
    for name, meta in fields.items():
        if meta["class"] != "exact":
            continue
        law, fill = meta.get("method"), meta.get("fill_value", "NaN")
        out = None
        for w in range(windows):
            acc = _empty_slab(meta, s_hi - s_lo)
            for i, row, a, b in _pieces(rows, **span):
                reader = row[w]
                if not _is_reader(reader):
                    continue
                values = _fetch(reader, res_src, name, _range(a, b, src_per_child), meter)
                if values is not None:
                    base = i * src_per_child - s_lo
                    acc[base + a : base + b] = values
            part = fold_dense(acc, factor, law, fill)
            out = part if out is None else combine_dense(out, part, law, fill)
        slabs[name] = out
    for name, meta in fields.items():
        if meta["class"] != "approximate":
            continue
        dtype = meta.get("dtype") or "float32"
        inner = tuple(meta.get("inner_shape") or (2,))
        delta = overview_fold_delta(meta)
        # A located field folds its sibling in the SAME k-way call as its
        # payload (ruling 4 on issue #410): the merged words are keyed on the
        # centroid partition that merge produces (spec §9.1), so the two are
        # accumulated together per open cell and folded together. A contributor
        # carrying one half of the pair is SKIPPED loudly and counted
        # unreadable — the gather's posture, and ``_fold_node``'s at a leaf
        # missing the sibling — never folded payload-only, and never a raise
        # that takes the whole level down over one stale child column.
        declared = field_companions(name, meta)
        out = np.full(hi - lo, b"", dtype=object)
        sibling_slabs = {kwarg: np.full(hi - lo, b"", dtype=object) for kwarg, _ in declared}
        pending: dict[int, list] = {}
        # {kernel kwarg: {open cell: [word vectors]}} — accumulated in lockstep
        # with ``pending`` so a cell closes with its payload and every channel.
        words: dict[str, dict[int, list]] = {kwarg: {} for kwarg, _ in declared}

        def _close(
            j,
            out=out,
            delta=delta,
            dtype=dtype,
            declared=declared,
            pending=pending,
            words=words,
            sibling_slabs=sibling_slabs,
        ):
            cell = pending.pop(j)
            if not declared:
                out[j - lo] = fold_digests(cell, delta=delta, dtype=dtype)
                return
            payload, *encoded = fold_digests(
                cell,
                delta=delta,
                dtype=dtype,
                channels={kwarg: words[kwarg].pop(j) for kwarg, _ in declared},
            )
            out[j - lo] = payload
            for (kwarg, _), value in zip(declared, encoded, strict=True):
                sibling_slabs[kwarg][j - lo] = value

        for i, row, a, b in _pieces(rows, **span):
            base = i * src_per_child
            for w, reader in enumerate(row):
                if not _is_reader(reader):
                    continue
                group = _companion_group(
                    reader, res_src, name, declared, _range(a, b, src_per_child), meter
                )
                if group is None:
                    broken.add((i, w))
                    continue
                slab = group[name]
                if slab is None:
                    continue
                for pos, payload in enumerate(slab):
                    if payload is None or not len(payload):
                        continue
                    key = (base + a + pos) // factor
                    pending.setdefault(key, []).append(decode_digest(payload, dtype, inner))
                    for kwarg, sibling in declared:
                        words[kwarg].setdefault(key, []).append(
                            decode_digest(group[sibling][pos], "uint64", ())
                        )
            # Cells wholly covered by the sources read so far are complete:
            # fold and free them, so resident state never exceeds the open
            # boundary.
            done = (base + b) // factor
            for j in [j for j in pending if j < done]:
                _close(j)
        for j in list(pending):
            _close(j)
        slabs[name] = out
        for kwarg, sibling in declared:
            slabs[sibling] = sibling_slabs[kwarg]
    for name, meta in fields.items():
        if meta["class"] != "packed":
            continue
        # The packed composition fold (issue #515, spec §3.4): each source
        # cell contributes its ``(word, n)`` pair, ``n`` being the ``of``
        # digest's weight at the same cell, and every output cell collapses in
        # ONE k-way call (single quantization). A contributor carrying one
        # half of the pair is SKIPPED and counted unreadable — the word is
        # uninterpretable without its divisor digest, and a divisor without
        # its word says nothing.
        #
        # Skipping alone does NOT keep the pair consistent in ONE of the two
        # directions, and that is the difference from the located pair above:
        # there both halves are the same field's, so refusing one refuses both.
        # Here the divisor is a DIFFERENT declared field, folded by the digest
        # loop above, which knows nothing about this one — so a contributor
        # carrying the DIVISOR but not the word lands in the level's
        # ``N_signal`` while its word is excluded, and a reader doing the §3.3
        # recovery divides by a denominator the word never covered (a ~29%
        # skew, reproduced on review). Absence over wrongness: every output
        # cell that contributor's span covers keeps the fill word ``0``, which
        # makes no §3.2 presence/fraction claim, while the digest itself stays
        # correct on its own and ``source_children.unreadable`` records that
        # the level folded short (spec §4.5).
        #
        # The REVERSE direction poisons nothing: when the divisor is the
        # missing half, the digest loop above reads that same array for the
        # ``of`` field itself and drops the contributor too (``slab is None``),
        # so word and ``N_signal`` already exclude the same rows. It is still
        # counted ``broken`` — the level did fold short — but the other
        # children's correct words stand (review finding).
        #
        # In the skew direction the blanking IS wider than the offending
        # contributor: at every level with ``r < child_order`` one output cell
        # is shared by ``factor / src_per_child`` children (this test's own
        # shape), so poisoning it drops SIBLING contributions as well. Left by
        # design — a shared cell whose folded ``N_signal`` counts rows no
        # surviving word describes cannot carry an honest word, and blanking
        # beats skewing.
        #
        # Blocks: a contributor's verdict is a property of its ARRAYS, so
        # every block its span reaches re-derives the same poisoned cells; the
        # demotion record counts it once (``noted``), however many blocks it
        # crossed.
        of_name = meta.get("of")
        of_dtype = (fields.get(of_name) or {}).get("dtype") or "float32"
        out = _empty_slab(meta, hi - lo)
        parts_by_cell: dict[int, list] = {}
        poisoned: set[int] = set()
        for i, row, a, b in _pieces(rows, **span):
            base = i * src_per_child
            for w, reader in enumerate(row):
                if not _is_reader(reader):
                    continue
                rng = _range(a, b, src_per_child)
                word_slab = _fetch(reader, res_src, name, rng, meter)
                of_values = _fetch(reader, res_src, of_name, rng, meter)
                if word_slab is None or of_values is None:
                    if (word_slab is None) != (of_values is None):
                        first = (name, i, w) not in noted
                        noted.add((name, i, w))
                        if first:
                            logger.warning(
                                f"stage sweep: column {reader.path} carries only one of "
                                f"{name!r}/{of_name!r} at resolution {res_src}; counting the "
                                f"contributor unreadable (spec §3.3, §1.1)"
                            )
                        broken.add((i, w))
                        # Either direction is a demotion the artifact must
                        # record (issue #518, spec §4.3): the level's word
                        # coverage folded short, and the bytes alone cannot
                        # say so (the fill word makes no §3.2 claim).
                        if of_values is not None:
                            blanked = range(
                                base // factor,
                                (base + src_per_child + factor - 1) // factor,
                            )
                            poisoned.update(blanked)
                            if first:
                                note_demotion(
                                    demoted, name, DEMOTION_WORD_MISSING, of_name, cells=blanked
                                )
                        elif first:
                            note_demotion(demoted, name, DEMOTION_DIVISOR_MISSING, of_name)
                    continue
                for pos in range(len(word_slab)):
                    n = payload_weight(of_values[pos], of_dtype)
                    if n > 0:
                        parts_by_cell.setdefault((base + a + pos) // factor, []).append(
                            (int(word_slab[pos]), n)
                        )
        # Poisoned cells drop out HERE, after accumulation: every surviving
        # cell still folds its parts in one k-way call (single quantization).
        for j, parts in parts_by_cell.items():
            if j not in poisoned:
                out[j - lo] = merge_composition_kway(parts)
        slabs[name] = out
    return slabs


def merge_level(
    rows: list,
    fields: dict,
    *,
    res_src: int,
    src_per_child: int,
    factor: int,
    n_out: int,
    block=None,
    meter=None,
) -> tuple:
    """One merge level, block by block: ``(slabs, broken, demotions)``.

    :func:`_merge_slabs` without the per-child roll-up — ``broken`` is the raw
    ``(child, window)`` set the fold refused, for a caller whose coverage
    counters are per WINDOW (the all-time fold, where the one "child" is the
    node itself — :func:`zagg.sweep_units.close_node`).
    """
    from zagg.sweep_overview import demotion_records

    windows = next((len(row) for row in rows if row is not None), 1)
    step = block_cells() if block is None else int(block)
    out_step = max(1, step // int(factor))
    state: dict = {"broken": set(), "demoted": {}, "noted": set()}
    parts = []
    for lo in range(0, n_out, out_step):
        parts.append(
            _merge_block(
                rows,
                fields,
                res_src=res_src,
                src_per_child=src_per_child,
                factor=factor,
                lo=lo,
                hi=min(lo + out_step, n_out),
                windows=windows,
                step=step,
                state=state,
                meter=meter,
            )
        )
        if meter is not None:
            meter.release()
    return _joined(parts), state["broken"], demotion_records(state["demoted"])


def _merge_slabs(
    rows: list,
    fields: dict,
    *,
    res_src: int,
    src_per_child: int,
    factor: int,
    n_out: int,
    block=None,
    meter=None,
) -> tuple:
    """K-way fold of the gen-1 tier, ``factor``-to-one — the ruled merge.

    ``rows`` is the DENSE rank-ordered child range (``None`` for uninhabited
    children, else ``[reader-or-None per window]``). The sources are the
    relayed partials (``res_src`` the relay member —
    :func:`zagg.column.relay_resolution`, the leaf columns' res-``shard_order
    + 2`` member) or, for the all-time fold on a windowed store, the node's
    own per-window overviews at the level's resolution (``factor == 1``,
    windows folding across — :func:`zagg.sweep_units.close_node`). Each
    output cell folds in ONE flat k-way call — what makes the merge tree
    independent of ``tuple_width`` (the merge-source law;
    ``merge_tdigests_kway`` is order-independent by its sort, #370).

    Memory bound: one block of output cells at a time (:func:`_merge_block`),
    sized so its sources are one fold block (:func:`block_cells`) or one
    output cell's ``factor`` of them, whichever is larger. Inside a block the
    exact classes hold one dense source vector per window (scalars); the
    digest classes stream piece by piece, holding one piece plus the open
    output cells' decoded digests (~``factor`` digests per cell — the envelope
    the ruling priced). Since issue #538 ``factor`` is ``4 ** (relay - r)``,
    not the one-order ``4``: a one-order merge off the res-``shard_order + 2``
    relay k-ways 64 δ-bounded digests per output cell where it k-wayed 4, and
    each child contributes ``src_per_child`` 16 rather than 1. Missing
    candidates contribute fill and are counted (``source_children.missing``).

    A located field's pair is read together (:func:`_companion_group`) and a
    contributor carrying one half is **skipped for that field and counted
    unreadable** — the same posture as the gather, and as
    ``sweep_overview._fold_node``'s at a leaf missing the sibling. It is not a
    raise: one stale child column must not take a whole stage level down, and
    the fold is a fold — dropping a contributor is under-coverage the artifact
    records, where writing a payload without its words would be corruption
    (spec §9.1). Fields whose reads are sound still fold that contributor: the
    read succeeded, so the loss is known per field, and the per-child
    ``unreadable`` count is what says the artifact folded short.

    Returns ``(slabs, folded, missing, unreadable, demotions)`` — the last a
    :func:`zagg.sweep_overview.demotion_records` list naming every packed
    field the half-pair rail demoted here, per direction (issue #518).

    The rail marks a fired contributor ``broken``, which is a WHOLE-contributor
    verdict, not a per-field one: where it fires on every contributor (the
    mis-declared-divisor shape) ``folded == 0`` and
    :func:`zagg.sweep_stage._stage_fold` drops the level, records included —
    see the note at its ``folded == 0`` guard.

    The order a cell's sources reach its k-way call is the whole-level
    fold's: child-major, and within one child's piece window-major. The two
    orders coincide because no fold has ``factor > 1`` AND more than one
    window — a relay merge is one window's, and the cross-window fold is
    cell-for-cell.
    """
    slabs, broken, demotions = merge_level(
        rows,
        fields,
        res_src=res_src,
        src_per_child=src_per_child,
        factor=factor,
        n_out=n_out,
        block=block,
        meter=meter,
    )
    # The coverage counters are computed at the END, from the same
    # ``(child, window)`` classification the gather uses: a contributor whose
    # located pair this fold refuses (:func:`_companion_group`) is unreadable,
    # not a clean fold, and that is only known once the sources have been read.
    return (slabs, *_source_counts(rows, broken), demotions)
