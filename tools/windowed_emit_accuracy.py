"""The all-time accuracy leg of the schedule measurement (issue #602; PR #587
question (16)).

At one node per ladder level, compare the **all-time overview** the staged
sweep wrote (``all.zarr`` — on a windowed store the per-node close that merges
the node's per-window overviews, themselves a cascade of their children's
artifacts, so the artifact at order ``k`` sits ``shard_order - k + 1`` merges
from raw) against a **flat fold of the same leaves**: one k-way merge, per
output cell, of every ``(leaf, window)`` column member under the node at the
level's own resolution (clamped to the column's coarsest member, the shard
order, below it). The flat fold is two or three merges from raw whatever the
level, so the gap between the two is what the cascade's depth costs.

Per level: the exact leg (``count`` at every populated cell equals the flat
sum — an inequality is a defect, not a tolerance) and, per digest field, the
**rank error** of the artifact's quantiles under the flat reference's CDF —
``cdf_ref(quantile_art(q)) - cdf_ref(quantile_ref(q))`` over the probe quantiles
and every cell both hold — reported in units of ``1/δ`` (δ the fold budget, ``overview_fold_delta``):
the k-way merge's own bound is a few of those. A level whose member resolution
equals its cells (orders 6..8 on the o9/d=4 geometry) merges exactly the digests
the close merged, so its error is identically zero; a non-zero there is a bug.

Read-only; one zarr open per source column (``2 + fields`` GETs). Imported by
``windowed_emit_measure.py`` (``--skip-accuracy`` to leave it out).
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import numpy as np

logger = logging.getLogger(__name__)

#: Probe quantiles for the rank-error leg.
QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)


def _open_group(path: str, store_kwargs: dict):
    import zarr

    from zagg.store import open_store

    return zarr.open_group(
        open_store(path, read_only=True, **store_kwargs), mode="r", zarr_format=3
    )


def probe_nodes(shards: list[str], shard_order: int) -> list[tuple[int, str]]:
    """One node per ladder level, ``(order, decimal)`` finest first: the first
    shard's ancestor at every order above the shard."""
    from zagg.hive import _decimal_base

    if not shards:
        return []
    first = sorted(shards)[0]
    base = len(_decimal_base(first))
    return [(k, first[: base + k]) for k in range(shard_order - 1, -1, -1)]


def _digest_fields(fields: dict) -> dict:
    return {
        n: m
        for n, m in fields.items()
        if m.get("class") == "approximate" and str(m.get("method", "")).startswith("tdigest")
    }


def _exact_fields(fields: dict) -> list[str]:
    return [n for n, m in fields.items() if m.get("class") == "exact"]


def _member_for(res: int, groups: list[int]) -> int:
    """The coarsest-but-not-coarser-than-``res`` column member: ``res`` itself at
    or above the shard order, else the shard-order member."""
    finer = [g for g in groups if g >= res]
    return min(finer) if finer else max(groups)


def _parents(words, order: int) -> np.ndarray:
    """Each source word's ancestor word at ``order`` (via the decimal grammar)."""
    from zagg.grids.morton import morton_decimal, morton_word
    from zagg.hive import _decimal_base

    out = np.empty(len(words), dtype=np.uint64)
    for i, w in enumerate(words):
        dec = morton_decimal(int(w))
        out[i] = morton_word(dec[: len(_decimal_base(dec)) + order])
    return out


def _read_source(path: str, res: int, names: list[str], store_kwargs: dict) -> dict | None:
    try:
        g = _open_group(path, store_kwargs)[str(res)]
        return {n: g[n][:] for n in ("morton", *names)}
    except Exception as e:  # a missing / unreadable column is reported, not fatal
        logger.warning(f"accuracy: source {path} @{res} unreadable ({e})")
        return None


def _rank_errors(art_payload, ref_payload, dtype, inner_shape) -> list[float]:
    """Per probe quantile, the rank gap between the two digests UNDER THE
    REFERENCE's CDF: ``cdf_ref(q_art(q)) - cdf_ref(q_ref(q))`` over the total
    weight. Both quantiles go through the same CDF, so identical digests read
    exactly 0 and the quantile->CDF round-trip bias cancels."""
    from zagg.stats.tdigest import cdf_from_tdigest, quantile_from_tdigest
    from zagg.sweep_overview import decode_digest

    art = decode_digest(art_payload, dtype, inner_shape)[:, :2]
    ref = decode_digest(ref_payload, dtype, inner_shape)[:, :2]
    total = float(ref[:, 1].sum())
    if not len(art) or not len(ref) or total <= 0:
        return []
    return [
        (
            float(cdf_from_tdigest(ref, quantile_from_tdigest(art, q)))
            - float(cdf_from_tdigest(ref, quantile_from_tdigest(ref, q)))
        )
        / total
        for q in QUANTILES
    ]


def level_accuracy(
    store_root: str,
    node: str,
    order: int,
    cells: int,
    shards: list[str],
    windows: list,
    fields: dict,
    groups: list[int],
    *,
    store_kwargs: dict,
    workers: int = 16,
) -> dict:
    """One level's comparison: the all-time artifact at ``node`` vs the flat fold."""
    from zagg.column import column_name
    from zagg.stats.tdigest import merge_tdigests_kway
    from zagg.sweep import _node_rel
    from zagg.sweep_overview import decode_digest, encode_digest, overview_fold_delta

    digests, exact = _digest_fields(fields), _exact_fields(fields)
    names = [*exact, *digests]
    member = _member_for(cells, groups)
    out: dict = {
        "order": order,
        "cells": cells,
        "node": node,
        "member": member,
        "shards": len(shards),
        "windows": len(windows),
        "sources": 0,
    }
    try:
        art = _open_group(f"{store_root}/{_node_rel(node)}/all.zarr", store_kwargs)
        prov = dict(art.attrs).get("zagg_overview") or {}
        level = art[str(cells)]
        art_words = np.asarray(level["morton"][:], dtype=np.uint64)
        art_arrays = {n: level[n][:] for n in names}
    except Exception as e:
        out["status"] = f"artifact unreadable: {type(e).__name__}: {e}"
        return out
    out["merges_from_raw"] = prov.get("merges_from_raw")
    out["source_windows"] = prov.get("source_windows")
    paths = [
        f"{store_root}/{_node_rel(s)}/{column_name(w)}"
        for s in shards
        if s.startswith(node)
        for w in windows
    ]
    with ThreadPoolExecutor(max(1, workers)) as ex:
        sources = [
            s
            for s in ex.map(lambda p: _read_source(p, member, names, store_kwargs), paths)
            if s is not None
        ]
    out["sources"], out["sources_unreadable"] = len(sources), len(paths) - len(sources)
    if not sources:
        out["status"] = "no sources"
        return out
    # Flat reference per output cell: exact sums, one k-way merge per digest.
    index = {int(w): i for i, w in enumerate(art_words)}
    ref_exact = {n: np.zeros(len(art_words), dtype=np.float64) for n in exact}
    ref_parts: dict = {n: [[] for _ in art_words] for n in digests}
    for src in sources:
        rows = _parents(src["morton"], order + (cells - order))  # ancestor at ``cells``
        for j, parent in enumerate(rows):
            i = index.get(int(parent))
            if i is None:
                continue
            for n in exact:
                ref_exact[n][i] += float(src[n][j])
            for n, meta in digests.items():
                p = src[n][j]
                if p is not None and len(p):
                    ref_parts[n][i].append(
                        decode_digest(
                            p, meta.get("dtype", "float32"), meta.get("inner_shape", [2])
                        )[:, :2]
                    )
    out["exact"] = {
        n: bool(np.array_equal(np.asarray(art_arrays[n], dtype=np.float64), ref_exact[n]))
        for n in exact
    }
    out["exact_cells_off"] = {
        n: int((np.asarray(art_arrays[n], dtype=np.float64) != ref_exact[n]).sum()) for n in exact
    }
    out["fields"] = {}
    for n, meta in digests.items():
        delta = overview_fold_delta(meta)
        dtype, inner = meta.get("dtype", "float32"), tuple(meta.get("inner_shape", [2]))
        errors, compared, identical = [], 0, 0
        for i, parts in enumerate(ref_parts[n]):
            art_payload = art_arrays[n][i]
            if not parts or art_payload is None or not len(art_payload):
                continue
            ref = encode_digest(merge_tdigests_kway(parts, delta), dtype)
            errs = _rank_errors(art_payload, ref, dtype, inner)
            if errs:
                compared += 1
                identical += bytes(art_payload) == ref
                errors.extend(abs(e) for e in errs)
        arr = np.asarray(errors, dtype=float)
        out["fields"][n] = {
            "delta": delta,
            "cells_compared": compared,
            "cells_identical": identical,  # payload bytes equal to the flat fold's
            "rank_err_max": float(arr.max()) if arr.size else None,
            "rank_err_p50": float(np.median(arr)) if arr.size else None,
            # the same, in units of the fold budget's own bound 1/δ
            "rank_err_max_x_delta": float(arr.max() * delta) if arr.size else None,
        }
    return out


def accuracy_numbers(
    store_root: str,
    manifest: dict,
    shards: list[str],
    windows: list,
    *,
    store_kwargs: dict,
    levels=None,
    workers: int = 16,
) -> dict:
    """Every ladder level's :func:`level_accuracy` at one probe node each.

    ``shards`` are the store's shard decimals, ``windows`` its window labels
    (``[None]`` on an unwindowed store, whose ``all.zarr`` is its only
    artifact). ``levels`` restricts the orders probed.
    """
    from zagg.column import column_resolutions
    from zagg.sweep_stage import ladder_entries

    shard_order = int(manifest["shard_order"])
    pyramid = manifest.get("pyramid") or {}
    fields = (pyramid.get("overview") or {}).get("fields") or {}
    try:
        entries = {e["node"]: e["cells"][0] for e in ladder_entries(pyramid, shard_order)}
    except ValueError as e:
        return {"status": f"no ladder: {e}", "levels": []}
    groups = column_resolutions(pyramid.get("overviews") or [], shard_order)
    rows = []
    for order, node in probe_nodes(shards, shard_order):
        if order not in entries or (levels is not None and order not in levels):
            continue
        rows.append(
            level_accuracy(
                store_root,
                node,
                order,
                entries[order],
                shards,
                windows,
                fields,
                groups,
                store_kwargs=store_kwargs,
                workers=workers,
            )
        )
    return {"quantiles": list(QUANTILES), "levels": rows}
