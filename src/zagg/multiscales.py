"""zagg-native ``multiscales`` convention metadata (issue #392).

A ``zagg-pyramid/2`` store is inherently a multiresolution grid (spec §4):
the fixed every-order ladder from ``shard_order - 1`` down to 0, plus the
leaf entry and the native resolution. Today that identity is legible only
through the ``pyramid`` block's own grammar. This module derives the
**discovery mirror**: a machine-readable ``multiscales`` declaration —
the OME/zarr-style multiscales attr grammar adapted to the node-tree
reality — written into the manifest (``morton_hive.json``) beside the
``pyramid`` block by the same writers (``hive.build_manifest`` at template
time, ``sweep_overview.declare_pyramid`` on retrofit), so a reader
discovers the whole ladder — orders present, per-order cell resolution,
per-field composability class, and the declared fold provenance — from
ONE metadata read.

The mirror is exactly that: **derived, never authoritative**. The
``pyramid`` block stays the normative declaration (spec §4.5); on any
disagreement the ``pyramid`` block wins, and this module's derivation is
the only writer. The key is present exactly when the manifest declares
``zagg-pyramid/2`` — a ``/1`` or declared-off store carries no
``multiscales`` key (absent = pre-convention store; no marker bump).
Nothing here writes a store: :mod:`zagg.hive` and
:mod:`zagg.sweep_overview` own the PUTs.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Version marker of the manifest ``multiscales`` mirror (issue #392).
#: Follows the conformance rule: strict-check, fail loudly on an unknown
#: revision.
MULTISCALES_SPEC = "zagg-multiscales/1"

#: Artifact-kind tokens the mirror's dataset entries carry: the leaf entry's
#: artifact is the §4.6 per-leaf column (``{window}.pyramid.zarr``), every
#: ladder entry's is the §4.1 ancestor-node overview (``{window}.zarr``).
ARTIFACT_COLUMN = "column"
ARTIFACT_OVERVIEW = "overview"


def multiscales_block(pyramid: dict, *, shard_order: int, cell_order: int, name=None):
    """The manifest ``multiscales`` list for a ``/2`` pyramid block.

    Returns the mirror — a **list** with one multiscale object, the
    OME/zarr-style shape (one image = one list entry; zagg stores declare
    exactly one) — or ``None`` when there is nothing to mirror: a ``/1``
    block, the declared-off shape, or anything that is not a ``/2``
    declaration (defensive: the manifest is hand-editable, and a malformed
    block must not crash template time — the pyramid grammar's own
    validation is the loud path).

    The derivation is a projection of §4.5 truth, never new information:

    - ``datasets`` — one entry per §4.4 level entry, finest first, in the
      recorded ``pyramid.overviews`` order: ``{order, cells, artifact}``
      with ``order``/``cells`` copied verbatim (``cells`` keeps the
      normative block's vocabulary — cell resolutions, one per member) and
      ``artifact`` the kind that carries the level (:data:`ARTIFACT_COLUMN`
      at the shard order, :data:`ARTIFACT_OVERVIEW` above it);
    - ``order2res`` — the flat per-order lookup ``{str(order): cells}``
      (JSON has no int keys): the cell resolutions of the **pyramid
      levels** at order k, keys exactly the orders present. It does NOT
      carry the base's native ``cells``, which ride in ``base`` alone and
      share the shard order's key — the resolutions readable at order k are
      ``order2res[str(k)]`` UNIONed with ``base["cells"]`` when
      ``k == base["order"]``, never the base overwriting the entry;
    - ``base`` — the native source data (``{order, cells: [cell_order]}``):
      part of the resolution ladder a reader picks from, never a pyramid
      level (it IS the data, §4.5), so it is not a dataset entry;
    - ``fields`` — ``{name: class}`` projected from the block's D24 map;
    - ``fold`` — the declared fold provenance (``fold_source``, plus
      ``exact_levels`` exactly when the block declares it).
    """
    if not isinstance(pyramid, dict) or not isinstance(pyramid.get("overviews"), list):
        return None
    from zagg.pyramid import PYRAMID_SPEC_V2

    if pyramid.get("spec") != PYRAMID_SPEC_V2 or not pyramid["overviews"]:
        return None
    shard_order, cell_order = int(shard_order), int(cell_order)
    datasets: list[dict] = []
    for entry in pyramid["overviews"]:
        order = int(entry["node"])
        datasets.append(
            {
                "order": order,
                "cells": [int(r) for r in entry["cells"]],
                "artifact": ARTIFACT_COLUMN if order == shard_order else ARTIFACT_OVERVIEW,
            }
        )
    overview = pyramid.get("overview") or {}
    fold = {"fold_source": overview.get("fold_source")}
    if overview.get("exact_levels") is not None:
        fold["exact_levels"] = int(overview["exact_levels"])
    block = {
        "spec": MULTISCALES_SPEC,
        "name": name,
        "base": {"order": shard_order, "cells": [cell_order]},
        "datasets": datasets,
        "order2res": {str(e["order"]): list(e["cells"]) for e in datasets},
        "fields": {n: m.get("class") for n, m in (overview.get("fields") or {}).items()},
        "fold": fold,
    }
    return [block]


def manifest_multiscales(manifest: dict):
    """Derive the mirror from a manifest's own ``pyramid`` block and orders.

    The one derivation both writers call (:func:`zagg.hive.build_manifest`,
    :func:`zagg.sweep_overview.declare_pyramid`), so the mirror can never be
    computed two ways. Returns :func:`multiscales_block`'s list, or ``None``
    when the manifest carries no ``/2`` declaration — in which case the
    caller must not write (and on retrofit must remove) the key.
    """
    if not isinstance(manifest, dict):
        return None
    try:
        return multiscales_block(
            manifest.get("pyramid") or {},
            shard_order=int(manifest["shard_order"]),
            cell_order=int(manifest["cell_order"]),
            name=(manifest.get("dataset") or {}).get("short_name"),
        )
    except (AttributeError, KeyError, TypeError, ValueError) as e:
        # Derived convenience, never load-bearing: a manifest the pyramid
        # grammar itself would refuse must not crash the mirror projection.
        # ``AttributeError`` is the hand-edit leg: every ``.get`` chain above
        # (``dataset``, ``pyramid.overview``, a field meta) hits a scalar and
        # raises it when the key was flattened by hand.
        logger.warning(f"multiscales: mirror derivation skipped ({e!r})")
        return None


__all__ = [
    "ARTIFACT_COLUMN",
    "ARTIFACT_OVERVIEW",
    "MULTISCALES_SPEC",
    "manifest_multiscales",
    "multiscales_block",
]
