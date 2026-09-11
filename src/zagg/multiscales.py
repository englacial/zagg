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
The manifest PUTs belong to :mod:`zagg.hive` and
:mod:`zagg.sweep_overview`; the one thing this module writes itself is the
issue #394 **companion group** (:func:`write_multiscales_group`, spec
§4.10): the stock-zarr-legible tree at the reserved root path
``multiscales/`` that mirrors the coarse ladder as metadata-only group
documents referencing (never copying) the node artifacts.
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


#: Reserved store-root child carrying the companion group (issue #394, spec
#: §4.10) — excluded from the D19 product-name grammar so a multi-product
#: root walker can never classify it as a product.
GROUP_NAME = "multiscales"
#: Companion root-group attrs key: the provenance stamp (spec §4.10).
GROUP_ATTR = "zagg_multiscales"
#: Companion per-order group attrs key: the level declaration + member refs.
LEVEL_ATTR = "zagg_multiscales_level"


def write_multiscales_group(store_root: str, *, store_kwargs=None) -> dict:
    """Write/refresh the stock-tool-legible companion group (issue #394).

    Metadata only, references only (spec §4.10): one zarr v3 group at the
    reserved root path ``multiscales/``, one child group per coarse ladder
    order (``shard_order - 1`` down to 0), each mapping the node decimals
    the ladder owns at that order — the ancestors of the store's occupied
    shards — to the store-root-relative path of that node's resolution
    group (``{hive_path(node)}/all.zarr/{cells[0]}``). No arrays and no
    data bytes are ever written: a member names where the node's overview
    lives WHEN materialized (overviews are regenerable caches, §4.1, and
    declared-but-unswept is legal, §4.5). The root document carries the
    §4.9 mirror (derived fresh from the ``pyramid`` block — the block wins
    over any recorded copy) plus the provenance stamp, inlines every child
    via zarr v3 consolidated metadata (one GET walks the tree), and is PUT
    LAST — the commit marker; a prefix without it is debris.

    Member discovery is the root ``coverage.moc`` when usable (one GET),
    else the D22 run-record discovery; the stamp records which
    (``members_source``). Raises on a store with no ``/2`` declaration
    (the caller's explicit operation, same posture as ``declare_pyramid``);
    returns a ``{"written": False, "reason": ...}`` summary on the
    windowed-without-``all_time`` gate — a legal store this revision's
    all-time-only companion does not apply to. The sweep finisher calls
    this fail-open (D9 cache class: the companion is a recorded mirror,
    regenerable at any time and self-healing on the ratchet).
    """
    import json

    from zagg.coverage import load_coverage
    from zagg.grids.morton import morton_decimal
    from zagg.hive import MANIFEST_NAME, _utcnow, read_manifest, root_coverage_words
    from zagg.store import open_object_store, put_object
    from zagg.sweep_overview import _node_at, _node_rel

    store_kwargs = dict(store_kwargs or {})
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(
            f"no {MANIFEST_NAME} at {store_root} — not a hive store root; the "
            f"multiscales companion mirrors an existing hive store only"
        )
    mirror = manifest_multiscales(manifest)
    if mirror is None:
        raise ValueError(
            f"the {MANIFEST_NAME} at {store_root} declares no zagg-pyramid/2 pyramid — "
            f"there is no ladder to mirror (declare_pyramid retrofits the declaration)"
        )
    overview = (manifest.get("pyramid") or {}).get("overview") or {}
    if manifest.get("temporal") is not None and not overview.get("all_time"):
        # INFO, not WARNING: §4.10 calls this store shape legal ("a gated
        # windowed store"), and the finisher calls this writer on EVERY
        # admitted sweep — warning about the ordinary case would train
        # operators to ignore the one logger where the fail-open warnings
        # live. The reason rides back in the summary for the CLI caller.
        logger.info(
            f"multiscales: {store_root} is windowed and declares no all_time fold — "
            f"the /1 companion mirrors the all-time fold only; nothing written"
        )
        return {"written": False, "reason": "windowed store without all_time"}
    envelope = load_coverage(store_root, **store_kwargs)
    if envelope is not None:
        decimals = [morton_decimal(int(w)) for w in root_coverage_words(envelope)]
        source = "coverage.moc"
    else:
        from zagg.sweep import discover_leaves

        refs = discover_leaves(store_root, store_kwargs=store_kwargs)
        decimals = sorted({morton_decimal(int(key)) for key, _ in refs})
        source = "run-records"
    (entry,) = mirror
    children: dict[str, dict] = {}
    for ds in entry["datasets"]:
        if ds["artifact"] != ARTIFACT_OVERVIEW:
            continue  # leaf entry and native data stay hive-only (§4.10)
        order, r = int(ds["order"]), int(ds["cells"][0])
        nodes = sorted({_node_at(d, order) for d in decimals})
        children[str(order)] = {
            "zarr_format": 3,
            "node_type": "group",
            "attributes": {
                LEVEL_ATTR: {
                    "spec": MULTISCALES_SPEC,
                    "order": order,
                    "cells": list(ds["cells"]),
                    "artifact": ARTIFACT_OVERVIEW,
                    "window": "all",
                    "members": {n: f"{_node_rel(n)}/all.zarr/{r}" for n in nodes},
                }
            },
        }
    root_doc = {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "multiscales": mirror,
            GROUP_ATTR: {
                "spec": MULTISCALES_SPEC,
                "window": "all",
                "members_source": source,
                "generated_at": _utcnow(),
            },
        },
        "consolidated_metadata": {
            "kind": "inline",
            "must_understand": False,
            "metadata": children,
        },
    }
    if not decimals:
        logger.warning(
            f"multiscales: no occupied shards discoverable at {store_root} ({source}) — "
            f"companion written with empty member sets"
        )
    store = open_object_store(store_root, **store_kwargs)
    for name, doc in children.items():
        put_object(store, f"{GROUP_NAME}/{name}/zarr.json", json.dumps(doc, indent=1).encode())
    put_object(store, f"{GROUP_NAME}/zarr.json", json.dumps(root_doc, indent=1).encode())
    return {
        "written": True,
        "orders": sorted((int(k) for k in children), reverse=True),
        "members": {k: len(v["attributes"][LEVEL_ATTR]["members"]) for k, v in children.items()},
        "members_source": source,
        "window": "all",
    }


__all__ = [
    "ARTIFACT_COLUMN",
    "ARTIFACT_OVERVIEW",
    "GROUP_ATTR",
    "GROUP_NAME",
    "LEVEL_ATTR",
    "MULTISCALES_SPEC",
    "manifest_multiscales",
    "multiscales_block",
    "write_multiscales_group",
]
