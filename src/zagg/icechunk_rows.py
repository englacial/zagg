"""The row dimension of the Icechunk companion repo (spec §11.2, issue #584).

``zagg-icechunk/2`` gives every level array a leading **row** dimension:
shape ``(n_rows, n_cells)``, chunk ``(1, C)``, chunk index ``(w, r·C + j)``.
A row is one window of the store's schedule, or the reserved ``all`` row —
the single row of an unwindowed (``schedule: none``) store. This module owns
what is the same for every level:

- **the row law** — a label's row is allocated once, in order of first
  appearance, and never moves; the ``zagg_icechunk`` block's ``rows`` list is
  the authority (:func:`row_index`), and readers look a row up or sort by the
  coordinate, never assume an order;
- **the row coordinate** — two ``int64`` arrays at the repo root,
  ``window_start`` / ``window_end``, each window's half-open ``[start, end)``
  in the manifest's temporal epoch, scale and units (:func:`row_bounds`); the
  ``all`` row carries no bound and reads the fill value;
- **the re-rooted array** (:func:`reroot`) — a leaf array's spec on the whole
  sphere under the row dimension;
- **allocation** (:func:`grow_rows`) — the once-per-run init grows every
  array by the run's new labels inside its ``init {run_id}`` commit;
- **the array-model identity check** (:func:`check_array_model`) — what a
  §11.4 operation may move of the model: rows, appended, and nothing else.

Workers never open the repo to learn a row index: a ref unit names its row
by LABEL and the committing session resolves it (``icechunk_refs.commit_units``).
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, Iterable, Mapping, cast

import numpy as np

from zagg.windows import SCHEDULE_NONE_TOKEN

logger = logging.getLogger(__name__)

#: The reserved row label: an unwindowed store's one row, and a windowed
#: store's all-time fold (never both in one store, issue #584).
ALL_ROW = SCHEDULE_NONE_TOKEN
#: The leading dimension's name on every level array and on the coordinate.
ROW_DIM = "window"
#: The root coordinate arrays: each row's half-open ``[start, end)``.
ROW_START, ROW_END = "window_start", "window_end"
ROW_COORDS = (ROW_START, ROW_END)
#: The coordinate's fill — "no bound", what the ``all`` row reads at both ends.
ROW_FILL = int(np.iinfo(np.int64).min)
#: Rows per coordinate chunk: one small chunk holds every window of a mission.
ROW_COORD_CHUNK = 1024
#: The manifest-split run length on the row axis (§11.5). Icechunk splits an
#: axis the config does not name at ONE chunk, which would cut a manifest per
#: row; naming it at a length no row count reaches is what makes the split
#: "the cell axis alone" (issue #584 phase 0).
ROW_SPLIT = 2**31 - 1
#: Fresh-session retries of an init whose row allocation lost to another run's.
ROW_ALLOC_TRIES = 5


def cell_axis_split(chunks: int) -> dict:
    """A level's manifest-split sizes: ``chunks`` on the cell axis, every row in one (§11.5)."""
    from icechunk import ManifestSplitDimCondition as D

    return {D.Axis(0): ROW_SPLIT, D.Axis(1): int(chunks)}


def check_revision(have, want: str, path: str) -> None:
    """Refuse a repo whose ``zagg_icechunk.spec`` is not this writer's (§11.2).

    A ``zagg-icechunk/1`` repo has no row dimension, so every ref this writer
    plans would land at an index its arrays do not have. There is no upgrade
    path — no published store carries a ``/1`` repo — so the remedy is to
    clear the repo and let the next init re-create it.
    """
    if have != want:
        raise ValueError(
            f"icechunk repo {path} is {have!r}, this writer is {want!r}: the array model "
            f"gained the row dimension and a repo is not upgraded in place. Clear {path} "
            f"and re-run — the init re-creates it (spec §11.2, issue #584)."
        )


def run_rows(windowing: dict | None, labels: Iterable[str] = ()) -> list[str]:
    """The row labels one run writes, in order of first appearance (§11.4 init).

    An unwindowed run writes the single ``all`` row; a windowed run the
    labels of its units, deduplicated in the order given.
    """
    if windowing is None:
        return [ALL_ROW]
    return list(dict.fromkeys(str(label) for label in labels))


def store_rows(temporal: dict | None, rows: Iterable[str] | None) -> list[str]:
    """The labels an init allocates on a store with this ``temporal`` block, vetted.

    An unwindowed store always has its one ``all`` row, whatever the run
    names (and may name no other); a windowed store gets exactly the run's
    labels — its ``all`` row only when the run names it — and an init naming
    none is refused (it would make a zero-row repo, §11.2). Order of first
    appearance, duplicates dropped.
    """
    labels = list(dict.fromkeys([*([] if temporal else [ALL_ROW]), *(rows or ())]))
    if not labels:
        raise ValueError(
            "a windowed store's init names no row label: the run's windows (and 'all' for "
            "its all-time fold) are the dispatcher's to name (spec §11.2)"
        )
    for label in labels:
        row_bounds(label, temporal)
    return labels


def row_bounds(label: str, temporal: dict | None) -> tuple[int, int] | None:
    """One row's ``[start, end)`` in the manifest's epoch/scale/units, or ``None``.

    ``None`` is the ``all`` row: no bound, so both coordinates keep the fill.
    Any other label is a window of the manifest's schedule; an unwindowed
    store has none, and a boundary that is not a whole number of the declared
    units is refused rather than rounded (the coordinate is ``int64``).
    """
    from zagg.windows import utc_to_offset, window_range

    if label == ALL_ROW:
        return None
    if not temporal:
        raise ValueError(f"row {label!r}: an unwindowed store has the one row {ALL_ROW!r}")
    start, end = window_range(label, temporal["schedule"], temporal.get("windows"))
    encoding = {k: temporal[k] for k in ("epoch", "scale", "units")}
    bounds = []
    for instant in (start, end):
        value = utc_to_offset(instant, **encoding)
        if value != round(value):
            raise ValueError(
                f"row {label!r}: window boundary {instant.isoformat()} is not a whole number "
                f"of {temporal['units']} since {temporal['epoch']} (the row coordinate is int64)"
            )
        bounds.append(int(round(value)))
    return bounds[0], bounds[1]


def coordinate_specs(n_rows: int, temporal: dict | None) -> dict:
    """``{name: ArraySpec}`` for the root row coordinate at ``n_rows`` rows.

    The attrs carry the manifest's time encoding, CF-shaped (``units`` as
    ``"{units} since {epoch}"``, ``calendar``) plus its ``scale``; an
    unwindowed store's coordinate has none — its one row is unbounded.
    """
    from pydantic_zarr.experimental.v3 import ArraySpec, NamedConfig

    attrs = {}
    if temporal:
        attrs = {
            "units": f"{temporal['units']} since {temporal['epoch']}",
            "calendar": temporal.get("calendar", "proleptic_gregorian"),
            "scale": temporal["scale"],
        }
    return {
        name: ArraySpec(
            attributes=attrs,
            shape=(int(n_rows),),
            dimension_names=(ROW_DIM,),
            data_type="int64",
            chunk_grid=NamedConfig(
                name="regular", configuration={"chunk_shape": (ROW_COORD_CHUNK,)}
            ),
            chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
            codecs=(
                NamedConfig(name="bytes", configuration={"endian": "little"}),
                NamedConfig(name="zstd", configuration={"level": 3, "checksum": False}),
            ),
            storage_transformers=(),
            fill_value=ROW_FILL,
        )
        for name in ROW_COORDS
    }


def reroot(spec, n_shards: int, n_rows: int):
    """One leaf array spec re-rooted on the whole order, under the row dimension (§11.2).

    Shape ``(n_rows, n_shards · L₀, *L[1:])`` and chunk ``(1, *inner)``; when
    the leaf array is sharded the chunk is the INNER chunk shape and the
    codecs the INNER chain (the ``sharding_indexed`` wrapper is gone); dtype,
    fill and attrs pass through verbatim, and the dims gain the leading
    ``window``. A ``(1, C)`` chunk holds the bytes the leaf's ``(C,)`` inner
    chunk does, so a virtual ref decodes unchanged.
    """
    data = spec.model_dump()
    codecs = list(data["codecs"])
    inner_shape = data["chunk_grid"]["configuration"]["chunk_shape"]
    if codecs and codecs[0]["name"] == "sharding_indexed":
        inner = codecs[0]["configuration"]
        inner_shape = inner["chunk_shape"]
        data["codecs"] = list(inner["codecs"])
    data["chunk_grid"] = {"name": "regular", "configuration": {"chunk_shape": [1, *inner_shape]}}
    shape = list(data["shape"])
    shape[0] *= int(n_shards)
    data["shape"] = (int(n_rows), *shape)
    data["dimension_names"] = (ROW_DIM, *(data["dimension_names"] or [None] * len(shape)))
    return type(spec)(**data)


def level_group_spec(grid, n_rows: int = 1):
    """One order's group: the leaf's resolution-group attrs, every array re-rooted."""
    from pydantic_zarr.experimental.v3 import GroupSpec

    leaf = grid.shard_spec()
    members = {name: reroot(spec, grid.n_shards, n_rows) for name, spec in leaf.members.items()}
    return GroupSpec(members=members, attributes=leaf.attributes)


def row_index(rows: Iterable[str], label: str) -> int:
    """The row ``label`` was allocated at; raises when the repo has none (§11.4)."""
    rows = list(rows)
    if label not in rows:
        raise ValueError(
            f"row {label!r} is not allocated in the icechunk repo (rows {rows}): "
            f"a run's init allocates its rows (spec §11.4)"
        )
    return rows.index(label)


def row_key(key: str, row: int) -> str:
    """A cell-axis chunk key ``{array}/c/{i…}`` placed at ``row``: ``{array}/c/{row}/{i…}``."""
    name, _, index = key.partition("/c/")
    return f"{name}/c/{int(row)}/{index}"


def write_bounds(session, rows: list, labels: Iterable[str], temporal: dict | None) -> None:
    """Write ``labels``' ``[start, end)`` at their rows of the root coordinate."""
    import zarr

    root = zarr.open_group(session.store, mode="r+")
    for label in labels:
        bounds = row_bounds(label, temporal)
        if bounds is None:
            continue
        row = row_index(rows, label)
        for name, value in zip(ROW_COORDS, bounds):
            cast("Any", root[name])[row] = value


def grow_rows(session, rows: list, labels: Iterable[str], temporal: dict | None) -> list[str]:
    """Allocate ``labels`` not yet in ``rows`` on ``session``; the full row list after.

    Every array of the repo with the row dimension — each level's, listed or
    retired, and the root coordinate — grows by the new labels, appended in
    the order given: no existing row moves and no chunk is touched (a resize
    rewrites no manifest, issue #584 phase 0). The caller records the
    returned list as the block's ``rows`` in the same commit.
    """
    import zarr

    new = [label for label in dict.fromkeys(labels) if label not in rows]
    if not new:
        return list(rows)
    grown = [*rows, *new]
    root = zarr.open_group(session.store, mode="r+")
    for _path, node in root.members(max_depth=None):
        dims = getattr(node.metadata, "dimension_names", None) or (None,)
        if isinstance(node, zarr.Array) and dims[0] == ROW_DIM:
            node.resize((len(grown), *node.shape[1:]))
    write_bounds(session, grown, new, temporal)
    return grown


def commit_rows(
    repo,
    existing: dict,
    updates: dict,
    labels: list,
    temporal: dict | None,
    commit,
    root_attrs: Mapping | None = None,
) -> tuple:
    """A reopened repo's init commit: block ``updates`` + the run's new rows; ``(snapshot, rows)``.

    ``commit(session)`` commits and returns the snapshot id — the caller's,
    so the message, metadata and rebase loop stay its own. Two runs
    allocating rows at once both resize every array, which no rebase
    reconciles (``RebaseFailedError``, issue #584 phase 0): the loser retries
    in a FRESH session, which re-reads the rows the winner recorded and
    appends after them. ``updates`` were computed from ``existing``, so a
    session whose block changed in anything but ``rows`` — to anything but
    ``updates`` themselves (the winner made the same ones) — raises instead:
    re-applying them would overwrite the winner's ratchet or knobs, and the
    caller fails open as it did when such a race raised before the rows.

    ``root_attrs`` are other root keys this init refreshes (the §11.1
    convention keys; ``None`` removes one), each written only where it
    differs — an up-to-date repo's init stays the empty commit — with the
    base layout entry's ``dggs`` read from the live group, so the init never
    reverts a ``set-attrs`` change (:func:`zagg.multiscales.live_conventions`).
    """
    import icechunk
    import zarr

    from zagg.icechunk_refs import BRANCH, ICECHUNK_ATTR
    from zagg.multiscales import live_conventions

    based_on = ({**existing, "rows": None}, {**existing, **updates, "rows": None})
    tries = 0
    while True:
        session = repo.writable_session(BRANCH)
        root = zarr.open_group(session.store, mode="r+")
        block = dict(cast("Any", root.attrs[ICECHUNK_ATTR]))
        if {**block, "rows": None} not in based_on:
            keys = {*block, *existing} - {"rows"}
            changed = sorted(k for k in keys if block.get(k) != existing.get(k))
            raise ValueError(
                f"icechunk block changed under this init ({changed}): another run's init "
                f"landed a different ratchet or knobs; not re-applying stale updates (§11.4)"
            )
        rows = grow_rows(session, list(block["rows"]), labels, temporal)
        if {**block, **updates, "rows": rows} != block:
            root.attrs[ICECHUNK_ATTR] = {**block, **updates, "rows": rows}
        for key, value in live_conventions(root, dict(root_attrs or {})).items():
            if root.attrs.get(key) != value:
                if value is None:
                    del root.attrs[key]
                else:
                    root.attrs[key] = value
        try:
            return commit(session), rows
        except icechunk.RebaseFailedError:
            tries += 1
            if tries >= ROW_ALLOC_TRIES:
                raise


@contextlib.contextmanager
def splits_follow_block(repo, store_root: str, store_kwargs: dict, saved: bool):
    """Around a reopened repo's init commit: a raising init re-saves the block's splits.

    A ratcheting init saves its splitting config BEFORE its commit (§11.5),
    and the save is not part of the commit. ``saved`` says this init made
    one: if the commit then raises — the lost race :func:`commit_rows`
    refuses, an exhausted rebase, anything — the repo's saved config would
    stay at a cut the block never recorded, and every later commit would cut
    its manifests there (issue #597). So before the error propagates the
    splits are re-saved from main's block as it stands NOW
    (``icechunk_refs.block_splits``): the winner's ratchet, or the block this
    init found. The init's own error always raises; a re-save that fails is
    logged. Residual, as in ``declare-pyramid``: a ratchet saving between this
    read and this save is lost (last writer wins). Only a raised ``Exception``
    is covered: an init killed between its save and its commit (a Lambda
    timeout, an OOM kill) runs no handler, and its saved config stays ahead of
    the block until the splits are saved again (a further ratchet, or a
    ``declare-pyramid`` that adds a level).
    """
    from zagg.icechunk_refs import BRANCH, _save_splits, _session_block, block_splits

    try:
        yield
    except Exception:
        if saved:
            try:
                block = _session_block(repo.readonly_session(BRANCH))
                _save_splits(repo, store_root, block_splits(block), store_kwargs)
            except Exception as exc:
                logger.warning(
                    f"icechunk init failed after saving a split ratchet, and re-saving the "
                    f"block's splits failed too ({type(exc).__name__}: {exc}): the repo's saved "
                    f"splitting config may be ahead of its block (spec §11.5, issue #597)"
                )
        raise


# ── the array-model identity check ──────────────────────────────────────────


def array_model(session) -> dict[str, dict]:
    """``{array path: metadata minus attrs}`` for every array in the session."""
    import zarr

    root = zarr.open_group(session.store, mode="r")
    model = {}
    for path, node in root.members(max_depth=None):
        if isinstance(node, zarr.Array):
            meta = node.metadata.to_dict()
            meta.pop("attributes", None)
            model[path] = meta
    return model


def _has_rows(meta: Mapping) -> bool:
    return (meta.get("dimension_names") or (None,))[0] == ROW_DIM


def _rows_grew(before: Mapping, after: Mapping) -> bool:
    """Whether ``after`` is ``before`` with a longer row extent and nothing else moved."""
    old, new = tuple(before["shape"]), tuple(after["shape"])
    if not _has_rows(before) or new[0] < old[0] or new[1:] != old[1:]:
        return False
    return {**after, "shape": None} == {**before, "shape": None}


def check_array_model(
    before: dict[str, dict],
    after: dict[str, dict],
    rows_before: Iterable[str],
    rows_after: Iterable[str] | None,
    allow_new: tuple[str, ...] = (),
) -> None:
    """Raise unless the array model moved by row growth and nothing else (spec §11.4).

    The one thing an existing array's model may do is gain rows: its leading
    ``window`` extent may grow — never shrink — and nothing else of its
    metadata (the cell extent, dtype, chunk grid, codecs, fill value,
    dimension names) may differ. Rows grow for the whole repo or not at all:
    the block's ``rows`` may only gain labels at its END, each label once
    (the row law, §11.2), and every array with the row dimension must hold exactly that
    many rows. No array may disappear; a new one must sit under an
    ``allow_new`` path prefix.
    """
    for array, meta in before.items():
        if array not in after:
            raise ValueError(f"operation would remove array {array!r}")
        if after[array] != meta and not _rows_grew(meta, after[array]):
            raise ValueError(f"operation would change the array model of {array!r} (spec §11.2)")
    for array in after:
        if array not in before and not array.startswith(allow_new):
            raise ValueError(f"operation would add array {array!r}")
    old, new = list(rows_before), list(rows_after or [])
    if new[: len(old)] != old:
        raise ValueError(f"operation would reorder or drop rows: {old} -> {new} (spec §11.2)")
    if len(set(new)) != len(new):
        raise ValueError(f"operation would allocate a row twice: {new} (spec §11.2)")
    off = sorted(a for a, m in after.items() if _has_rows(m) and m["shape"][0] != len(new))
    if off:
        raise ValueError(f"operation would leave {off} off the block's {len(new)} rows (§11.2)")


__all__ = [
    "ALL_ROW",
    "ROW_ALLOC_TRIES",
    "ROW_COORDS",
    "ROW_COORD_CHUNK",
    "ROW_DIM",
    "ROW_END",
    "ROW_FILL",
    "ROW_SPLIT",
    "ROW_START",
    "array_model",
    "cell_axis_split",
    "check_array_model",
    "check_revision",
    "commit_rows",
    "coordinate_specs",
    "grow_rows",
    "level_group_spec",
    "reroot",
    "row_bounds",
    "row_index",
    "row_key",
    "run_rows",
    "splits_follow_block",
    "store_rows",
    "write_bounds",
]
