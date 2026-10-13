"""Native chunks for the overview levels of the companion repo (spec §11.3, issue #584).

The §4 overview objects are the one artifact a later run **rewrites in
place**: a store's leaves are versioned (§1.5 — a replacement writes a new
version subgroup and the old objects stand), but ``{window}.zarr`` and
``all.zarr`` are re-folded under their own keys every time a node's subtree
gains a leaf. A virtual ref into a rewritten object is stale: the checksum
the ref carries (§11.3) is the object's ETag or ``last_modified`` as it was,
so the read FAILS rather than returning the wrong bytes — which is the
honest outcome on the branch tip (the next ladder pass re-plans it) and the
wrong one on a **tag**, whose whole purpose is to read the store as that run
left it (§11.4). The cross-window fold's row (``all``) is rewritten by every
append, so on a windowed store that is every tag ever cut.

So an overview level's chunks are the repo's **own bytes**, copied in by the
commit that would otherwise have referenced them (the ruling of issue #582,
carried by this issue's plan as its phase 3). The store's objects stay
exactly as they are — authoritative for the ladder's own gather
(``sweep_overview`` reads and re-folds them) and for a path reader — and the
repo simply stops pointing at them. The cost is the overview bytes a second
time, in the repo, plus a GET of every overview object in the committing
node's **whole subtree** — not one array's: the node's units carry its
children's columns recursively, so at the global setting (``commit_order``
3, ``shard_order`` 9) that is of order 10⁴ objects inside one session, where
the virtual arm did no IO at commit time at all. They are read in gathers of
:data:`READ_BATCH` to keep that off the wall clock (issue #584 review); the
per-invoke figure is unmeasured and wants a fleet run before the v4 build.
What it buys is a tag that reads every level it was cut with, the ``all`` row
included.

The decision is the LEVEL's, read off the repo's own ``zagg_icechunk`` block
(:func:`native_level`): an ``overview`` level is native, a ``leaf`` or
``column`` level virtual. Nothing in the §11.4 carrier changes — a ref plan
entry already names the object, the offsets and the lengths, which is all
the copy needs — so a stage node's column travels exactly as before and the
committing node decides.
"""

from __future__ import annotations

import math

import numpy as np

#: The repo level artifact whose chunks are the repo's own bytes (§11.3).
NATIVE_ARTIFACT = "overview"
#: Objects read concurrently inside one commit (§11.3). The reads are this
#: commit's whole IO cost and a unit names the committing node's whole
#: subtree at one order, so they are gathered the way ``_write`` gathers its
#: writes — but in windows, so the memory peak stays this many objects
#: rather than the subtree's (issue #584 review).
READ_BATCH = 16


def native_level(level: dict | None) -> bool:
    """Whether this repo level's chunks are copied in rather than referenced (§11.3)."""
    return level is not None and level.get("artifact") == NATIVE_ARTIFACT


def _vet_meta(key: str, meta, checksum, *, etags: bool) -> None:
    """Refuse an object that moved since the plan read it (§11.3).

    The staleness guard a virtual ref gets for free: a native chunk carries
    no checksum (the bytes are the repo's own), so the object is vetted HERE
    against the form the plan recorded — the ETag on an object store, the
    ceiled ``last_modified`` on ``file://``. A mismatch means another writer
    re-folded this overview between the plan and the copy, so the gather
    describes a state that no longer exists: it raises, the ladder's hook
    counts the node failed (D9), and the next pass re-plans.
    """
    from zagg.icechunk_refs import _ceil_second

    have = meta["e_tag"] if etags else _ceil_second(meta["last_modified"])
    if checksum is not None and have != checksum:
        raise ValueError(
            f"{key} moved between the ref plan and the native copy (plan {checksum!r}, "
            f"now {have!r}): another writer re-folded this overview (spec §11.3)"
        )


def _read(store, tasks: list[tuple], *, etags: bool) -> list[bytes]:
    """Every task's object bytes, in ONE gather, each vetted as it lands (§11.3).

    One GET per object, which is also the read that serves every chunk of
    it — and the committing node pays one for every overview object in its
    whole subtree (:func:`stage_unit`), so they are issued concurrently
    rather than serially. The first object that moved raises out of the
    gather, which costs the node its whole gather by design.
    """
    import asyncio

    import obstore
    from zarr.core.sync import sync

    async def one(key: str, checksum) -> bytes:
        result = await obstore.get_async(store, key)
        _vet_meta(key, result.meta, checksum, etags=etags)
        return bytes(await result.bytes_async())

    async def read():
        return await asyncio.gather(*(one(key, checksum) for _e, key, checksum, _c in tasks))

    return list(sync(read()))


def _write(session, pairs: list[tuple[str, bytes]]) -> None:
    """Write ``(chunk key, bytes)`` payloads into the session, one gather."""
    import asyncio

    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    buffer = default_buffer_prototype().buffer
    store = session.store

    async def write():
        await asyncio.gather(*(store.set(key, buffer.from_bytes(data)) for key, data in pairs))

    sync(write())


def _vet_shard_index(data: bytes, entry: dict, location: str) -> None:
    """Refuse a shard object whose own index no longer matches the plan's (§11.3).

    The byte-exact staleness guard, and the one the virtual path structurally
    cannot have: a checksum form can collide — an ETag is whatever the
    container mints, and a ``file://`` ``last_modified`` is ceiled to the
    whole second, so a re-fold inside that second is invisible to it — but a
    re-fold that moved or resized ANY inner chunk shows up in the index. The
    copy has the whole object in hand and ``_INDEX_LOCATION`` is ``end``, so
    the index is simply read again from the bytes about to be sliced and
    compared against the pairs :func:`~zagg.icechunk_refs.object_ref_plan`
    recorded. Without it ``data[start : start + length]`` cannot raise, and a
    shrinking rewrite inside that window is committed as a SHORT native
    chunk — silently, and permanently in a tag (issue #584 review).
    """
    from zagg.icechunk_refs import _ABSENT, _INDEX_CRC_BYTES, _INDEX_ENTRY_BYTES

    n = math.prod(int(c) for c in entry["chunk_grid"])
    index_bytes = _INDEX_ENTRY_BYTES * n + _INDEX_CRC_BYTES
    if len(data) < index_bytes:
        raise ValueError(
            f"{location} is {len(data)} bytes, too short to hold its own {n}-chunk shard index "
            f"({index_bytes}), so it no longer matches the ref plan (spec §11.3)"
        )
    offsets = np.asarray(entry["offsets"], dtype="<u8")
    lengths = np.asarray(entry["lengths"], dtype="<u8")
    payload = len(data) - index_bytes
    extent = int((offsets + lengths).max()) if n else 0
    if extent > payload:
        raise ValueError(
            f"{location}: its {payload} payload bytes no longer hold the ref plan's chunks, "
            f"which end at {extent} — a slice of it would be SHORT (spec §11.3)"
        )
    index = np.frombuffer(data[payload : len(data) - _INDEX_CRC_BYTES], dtype="<u8").reshape(n, 2)
    present = index[:, 0] != _ABSENT
    if not (
        np.array_equal(np.where(present, index[:, 0], 0), offsets)
        and np.array_equal(np.where(present, index[:, 1], 0), lengths)
        and np.array_equal(present, [bool(loc) for loc in entry["locations"]])
    ):
        raise ValueError(
            f"{location}: its shard index no longer matches the ref plan "
            f"({int(present.sum())} chunks in the object against "
            f"{sum(1 for loc in entry['locations'] if loc)} planned): another writer re-folded "
            f"this overview (spec §11.3)"
        )


def _tasks(entries: list[dict], prefix: str) -> list[tuple]:
    """``(entry, object key, checksum, chunk)`` per OBJECT a unit's entries need.

    Flattening both plan shapes to the object is what makes the reads
    batchable, and the reads are the commit's whole cost. A sharded entry is
    ONE object — a sharded hive array is one shard object, which is what its
    single ``checksum`` belongs to and what ``object_ref_plan`` refuses
    anything else for — whose index yields every chunk of the array, so its
    ``chunk`` is ``None``; a regular entry is one object per chunk.

    The prefix strip is the house helper, ``icechunk_ladder._strip``, which
    refuses a location ``outside the container`` rather than silently
    removing the wrong bytes. No carried entry can be outside it —
    ``pack_units`` stores locations relative to the prefix and
    ``unpack_units`` re-expands them with the same ``container_prefix``
    this strips, and ``open_vetted`` refuses a root disagreeing with the
    block's ``url_prefix`` — but a plain slice would report a mismatch as
    ``_vet_meta``'s "another writer re-folded this overview", which is the
    wrong cause (issue #584 review).
    """
    from zagg.icechunk_ladder import _strip

    tasks: list[tuple] = []
    for entry in entries:
        if entry["sharded"]:
            objects = {location for location in entry["locations"] if location}
            if len(objects) > 1:
                raise ValueError(
                    f"{entry['path']}: a sharded entry names {len(objects)} objects but carries "
                    f"one checksum; a hive leaf array is one shard object (spec §11.3)"
                )
            for location in objects:
                tasks.append((entry, _strip(location, prefix), entry.get("checksum"), None))
        else:
            for chunk in entry["chunks"]:
                tasks.append((entry, _strip(chunk[1], prefix), chunk[3], chunk))
    return tasks


def _sharded_pairs(entry: dict, order: int, row: int, data: bytes, location: str) -> list:
    """``(chunk key, bytes)`` for every present chunk of a sharded entry's one object.

    The object is vetted against its own index (:func:`_vet_shard_index`) and
    sliced by the index the plan already read: the chunk at grid position
    ``j`` is ``[offset, offset + length)`` of that object, byte-for-byte the
    ``(1, C)`` chunk the repo array holds (``icechunk_rows.reroot``), so the
    copy re-encodes nothing.
    """
    _vet_shard_index(data, entry, location)
    grid = tuple(int(n) for n in entry["chunk_grid"])
    offsets = np.asarray(entry["offsets"], dtype="<u8")
    lengths = np.asarray(entry["lengths"], dtype="<u8")
    pairs = []
    for flat, index in enumerate(np.ndindex(grid)):
        if not entry["locations"][flat]:
            continue  # absent from the shard index: that chunk was never written
        start, length = int(offsets[flat]), int(lengths[flat])
        cell = (int(o) + int(i) for o, i in zip(entry["arr_offset"], index))
        key = "/".join(str(c) for c in (row, *cell))
        pairs.append((f"{order}/{entry['path']}/c/{key}", data[start : start + length]))
    return pairs


def _pairs(entry: dict, order: int, row: int, data: bytes, chunk) -> list[tuple[str, bytes]]:
    """``(chunk key, bytes)`` one read object yields: a shard's slices, or one chunk."""
    from zagg.icechunk_rows import row_key

    if chunk is None:
        return _sharded_pairs(entry, order, row, data, next(c for c in entry["locations"] if c))
    key, location, length, _checksum = chunk
    if len(data) != int(length):
        raise ValueError(
            f"{location} is {len(data)} bytes, the plan read {int(length)} (spec §11.3)"
        )
    return [(f"{order}/{row_key(key, row)}", data)]


def stage_unit(
    session, order: int, row: int, entries: list[dict], prefix: str, store, *, etags: bool
) -> int:
    """Copy a ref-plan unit's ``entries`` into ``session`` natively; the bytes written.

    ``order`` is the level's cell order (its group in the repo), ``row`` the
    resolved row index, ``prefix`` the container's ``url_prefix`` (a plan
    location carries it, §11.3) and ``store`` an obstore handle on the store
    root. Both plan shapes are copied: a sharded array's one object sliced
    by its index, and a regular array's one object per chunk.

    A UNIT at a time, not an entry at a time, because the reads batch and
    they are the only IO the commit does. One overview unit names every node
    of the committing node's subtree at that order — at the global setting
    (``commit_order`` 3, ``shard_order`` 9) the subtree's overview nodes
    number 1,365 across the orders, times the composable arrays per node — so
    the objects are read :data:`READ_BATCH` at a time, concurrently, and
    written before the next window is read: the round trips come off the wall
    clock while the peak stays that many objects rather than the subtree's
    (issue #584 review). Each slice is a **copy** of its bytes, so that peak
    is about twice a window's object bytes, not one window's.
    """
    tasks = _tasks(entries, prefix)
    written = 0
    for start in range(0, len(tasks), READ_BATCH):
        batch = tasks[start : start + READ_BATCH]
        pairs: list[tuple[str, bytes]] = []
        for (entry, _key, _checksum, chunk), data in zip(batch, _read(store, batch, etags=etags)):
            pairs.extend(_pairs(entry, order, row, data, chunk))
        _write(session, pairs)
        written += sum(len(data) for _key, data in pairs)
    return written


__all__ = ["NATIVE_ARTIFACT", "READ_BATCH", "native_level", "stage_unit"]
