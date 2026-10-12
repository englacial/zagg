"""The leaf column's shard layout: fine groups on inner chunks (issue #631).

A leaf column (:mod:`zagg.column`, spec §4.6) was one chunk per member array
per resolution group, whatever the group's width. That is right for the
19/13/9 reference geometry's default tier — {13, 12, 11, 10, 9}, 256 cells
at the widest — and wrong once the tier reaches below the leaf chunk order:
the v4 ATL03 rebuild declares ``overviews: [16, 15, 14, 13]``, and at order
16 a group is 16,384 cells, ~3 MB of signal digest per leaf object at the
128-centroid ``overview_delta`` cap (~470 B per saturated signal cell,
measured), where the viewer's windows and the cascade's block reads both
want pieces of ~0.5 MB. Splitting such a group into regular chunk objects
would multiply the fleet's PUTs (and the Icechunk manifest) 16-fold at
order 16; the leaves already solve the same problem with the zarr v3
``sharding_indexed`` codec, so the fine column groups take it too: still
ONE object per member array per leaf, an index at the end, inner chunks a
reader can fetch by byte range.

**The layout rule** (normative, spec §4.6) is a function of the group's own
geometry — ``(node_order, res)`` — and nothing else, so every consumer
(the writer, the backfill's skip gate, ``pyramid_check``, the Icechunk ref
planner, an external reader with only the manifest) derives it the same way:

- a group of fewer than ``4 ** COLUMN_INNER_ORDER`` cells (``res - node <
  5``: ≤ 256 cells) is one chunk per array, unsharded — byte-identical to
  every column written before this rule, so the default tier never moves;
- a wider group is sharded, its inner chunk ``min(4 ** 5, n_cells / 4)``
  cells: at most 1,024 cells (the largest power of four whose saturated
  signal digest stays under 0.5 MB at the 128 cap), and at least four inner
  chunks per object (an object that is a single inner chunk has no piece to
  fetch; four is one HEALPix refinement, the smallest split that is one).

On the 19/13/9 geometry: order 16 → 16 inner chunks of 1,024 cells, 15 → 4 of
1,024, 14 → 4 of 256, 13 → one whole 256-cell chunk as today. The inner
chunk is always a power of four, so a viewer's window block never straddles
one (gridlook derives its block depth from the chunk shape). The inner
codec chain is the member's own — ``vlen-bytes`` + ``zstd`` for the ragged
digests and their companions, the leaves' bytes-only policy for dense
fields — with ``index_location: end`` and a ``bytes`` + ``crc32c`` index,
the leaf shard layout (:func:`zagg.grids.base.sharded_array_spec`,
:func:`zagg.grids.base.ragged_array_spec`), so a reader of leaf shards
reads column shards unchanged.

The constant equals the staged sweep's fold block
(:data:`zagg.sweep_fold.STAGE_BLOCK_ORDER`): a stage gather reads a child
column's member one block at a time, so over a fine leaf column a block read
is at most one inner chunk's worth of cells — one 1,024-cell inner chunk
above ``4^5`` cells, the whole four-inner-chunk object at exactly ``4^5``.
(A STAGE column keeps a ``4^5``-cell group one chunk, ``sweep_stage``'s
``res - node <= STAGE_BLOCK_ORDER``: its chunking is orchestration, §4.6,
where a leaf column's is the wire contract.)
"""

from __future__ import annotations

from zagg.multiscales import ARTIFACT_COLUMN

#: One inner chunk of a sharded column group, as a HEALPix order difference:
#: ``4 ** 5 = 1,024`` cells, ~0.5 MB of saturated signal digest at the
#: 128-centroid pyramid cap (~470 B per cell, measured on the v4 ATL03
#: rebuild). A constant, not a knob: the layout is a wire-format rule.
COLUMN_INNER_ORDER = 5


def inner_chunk_order(node_order: int, res: int) -> int | None:
    """The ``chunk_inner`` order of a column group's arrays, or ``None``.

    ``None`` is the unsharded single chunk (a group narrower than
    ``4 ** COLUMN_INNER_ORDER`` cells); otherwise the order whose cells one
    inner chunk spans ``4 ** (res - order)`` of — the deeper of
    ``res - COLUMN_INNER_ORDER`` (1,024-cell chunks) and ``node_order + 1``
    (four chunks per object), see the module docstring.
    """
    node_order, res = int(node_order), int(res)
    if res - node_order < COLUMN_INNER_ORDER:
        return None
    return max(res - COLUMN_INNER_ORDER, node_order + 1)


def inner_chunk_cells(node_order: int, res: int) -> int:
    """Cells per chunk of a column group's arrays: the inner chunk, or the whole group."""
    order = inner_chunk_order(node_order, res)
    return 4 ** (int(res) - (int(node_order) if order is None else order))


def column_grid(node_order: int, res: int, config):
    """The ``HealpixGrid`` whose ``shard_spec`` is one column group's layout.

    The ONE construction every column consumer shares — the writer
    (:func:`zagg.column.write_column`), the backfill's structure gate
    (:func:`zagg.column_backfill.column_structure`), the Icechunk ref planner
    (:func:`zagg.icechunk_refs.level_grids`, :func:`zagg.icechunk_refs.leaf_units`)
    — so the layout cannot drift between them. ``config`` is the
    :func:`zagg.sweep_overview._overview_config` of the column's fields. An
    unsharded group passes no ``chunk_inner``: ``HealpixGrid`` then holds one
    chunk per shard and turns sharding off itself, the pre-#631 spec byte
    for byte.
    """
    from zagg.grids.healpix import HealpixGrid

    inner = inner_chunk_order(node_order, res)
    return HealpixGrid(
        int(node_order), int(res), config=config, chunk_inner=inner, sharded=inner is not None
    )


def expected_layout(node_order: int, res: int) -> dict:
    """``{"sharded", "chunk_cells"}`` the rule prescribes for one group's arrays."""
    return {
        "sharded": inner_chunk_order(node_order, res) is not None,
        "chunk_cells": inner_chunk_cells(node_order, res),
    }


def layout_mismatch(node_order: int, res: int, metadata: dict) -> str | None:
    """How one stored column array departs from the rule, or ``None``.

    The ``pyramid_check`` layout leg (spec §4.6 is normative on the layout,
    so a fine group written whole — a pre-#631 worker, or a hand-built
    column — fails read-back by name rather than passing on its values).
    """
    want, got = expected_layout(node_order, res), stored_layout(metadata)
    if want == got:
        return None

    def _say(layout):
        kind = "sharded" if layout["sharded"] else "unsharded"
        return f"{kind}, {layout['chunk_cells']} cells per chunk"

    return f"layout is {_say(got)}; §4.6 requires {_say(want)} for a group at order {int(res)}"


def stored_layout(metadata: dict) -> dict:
    """:func:`expected_layout`'s twin, read off one array's zarr v3 metadata.

    ``chunk_cells`` is the cells axis of the inner chunk when the array's
    codec chain opens with ``sharding_indexed``, else of the chunk grid — the
    §1.5 reading every zagg reader applies to a leaf array.
    """
    codecs = list(metadata.get("codecs") or [])
    first = codecs[0] if codecs else {}
    if isinstance(first, dict) and first.get("name") == "sharding_indexed":
        return {"sharded": True, "chunk_cells": int(first["configuration"]["chunk_shape"][0])}
    return {
        "sharded": False,
        "chunk_cells": int(metadata["chunk_grid"]["configuration"]["chunk_shape"][0]),
    }


def stale_column_levels(levels: dict | None) -> list[str]:
    """The Icechunk ``levels`` (spec §11.1) of a column chunked against the rule.

    A companion repo initialized before the rule records a fine column level
    at one chunk per node; a ref planned under the rule lands there at chunk
    indices that mean something else (low ranks overwrite another node's
    slots, high ranks fall out of bounds). Its vets refuse it: clear and
    re-init (issue #631).
    """
    return sorted(
        (
            order
            for order, level in (levels or {}).items()
            if level.get("artifact") == ARTIFACT_COLUMN
            and int(level["chunk_order"]) != _level_chunk_order(level["node_order"], order)
        ),
        key=int,
    )


def check_column_entry(level: dict | None, entry: dict, array_path: str) -> None:
    """Refuse a whole-object ref (a pre-rule leaf-refs sidecar) for a sharded column level."""
    if entry["sharded"] or not level or level.get("artifact") != ARTIFACT_COLUMN:
        return
    if inner_chunk_order(level["node_order"], level["cell_order"]) is not None:
        raise ValueError(
            f"{array_path}: a whole-object ref for a column level §4.6 shards (a leaf-refs "
            f"sidecar written before issue #631): re-plan the leaf under the rule"
        )


def _level_chunk_order(node_order, res) -> int:
    inner = inner_chunk_order(node_order, res)
    return int(node_order) if inner is None else inner
