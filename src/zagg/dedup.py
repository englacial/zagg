"""Per-shard dedup status: ``has_run`` (issue #299 phase 4, D19).

Answers "has this exact product already produced this shard?" from durable
store state, for the estimate/skip flows (#298) to consume. Three signals, in
strictly increasing cost:

1. **The commit stamp** (D4): a leaf whose root attrs lack the stamp — or no
   leaf at all — is a plain ``"miss"`` (debris is invisible).
2. **The D20 stats sidecar**: the recorded ``semantic_hash`` (intended
   identity, D19) and ``granules_sha256`` (catalog identity — the output is
   ``f(template, shard, catalog snapshot)``, and ATL03 is a living
   collection). A stamped leaf whose sidecar is missing, records a different
   or absent ``semantic_hash``, or hashes a different granule set is
   ``"stale"`` — present but not provably this product over this catalog;
   a catalog-grown shard is stale, **never** a hit.
3. **The O11 content hashes**, surfaced (not recomputed) when the sidecar
   carries them: the *verifier* — "intended identical" (semantic hash) vs
   "actually byte-identical" (per-array decoded-value hashes) — for callers
   that go on to compare two stores.

Statuses are conservative by construction: every ambiguity degrades toward
recompute (``stale``/``miss``), never toward a false ``hit`` — a wrong "skip"
silently ships wrong data; a wrong "recompute" costs one shard's work.
"""

from __future__ import annotations

import logging

from zagg.hive import read_commit, read_manifest, shard_leaf_path
from zagg.store import open_store
from zagg.telemetry import granules_sha256, read_sidecar

logger = logging.getLogger(__name__)

#: ``has_run`` statuses: complete + verified for THIS product and catalog
#: snapshot / present but unverifiable-or-outdated / not (completely) there.
STATUSES = ("hit", "stale", "miss")


def shard_status(
    store_root: str,
    shard_key,
    *,
    semantic_hash: str,
    granule_ids=None,
    window: str | None = None,
    spec: str | None = None,
    **store_kwargs,
) -> dict:
    """Dedup status of ONE (shard, window) leaf; see :func:`has_run`.

    ``granule_ids`` is the shard's CURRENT catalog snapshot in the same id
    space the sidecars record (resolved granule URLs on the aggregation
    path, STAC item ids/datetimes for raster — cf.
    :func:`zagg.telemetry.granules_sha256`); ``None`` skips the catalog
    check (identity match alone then gates the hit).
    """
    leaf = shard_leaf_path(store_root, int(shard_key), window=window)
    stamp = read_commit(open_store(leaf, **store_kwargs))
    if stamp is None:
        return {"status": "miss"}
    sidecar = read_sidecar(leaf, spec=spec, **store_kwargs)
    if sidecar is None:
        # Stamped but unverifiable (pre-#297 leaf, or a lost fail-open PUT):
        # never a hit — the leaf may be any product/catalog vintage.
        return {"status": "stale", "reason": "no stats sidecar"}
    detail: dict = {
        "semantic_hash_match": sidecar.get("semantic_hash") == semantic_hash,
        "catalog_match": None,
    }
    if content := sidecar.get("content_hashes"):
        # O11 verifier, surfaced when recorded (never recomputed here).
        detail["content_hashes"] = content
    if granule_ids is not None:
        detail["catalog_match"] = sidecar.get("granules_sha256") == granules_sha256(granule_ids)
    if not detail["semantic_hash_match"]:
        return {"status": "stale", "reason": "semantic_hash mismatch or unrecorded", **detail}
    if detail["catalog_match"] is False:
        return {"status": "stale", "reason": "catalog grown/changed", **detail}
    return {"status": "hit", **detail}


def has_run(
    store_root: str,
    config,
    shards,
    *,
    window: str | None = None,
    spec: str | None = None,
    **store_kwargs,
) -> dict:
    """Per-shard dedup status for a prospective run (issue #299 phase 4).

    Parameters
    ----------
    store_root : str
        The PRODUCT root (apply :func:`zagg.hive.effective_store_root` /
        :func:`zagg.hive.product_root` first for multi-product stores).
    config : PipelineConfig
        The prospective run's config; its ``semantic_hash`` is the identity
        compared against each sidecar.
    shards : mapping or iterable
        ``{shard_key: granule_ids}`` (current catalog snapshot per shard —
        the same id space the sidecars record) or a bare iterable of shard
        keys (catalog check skipped).
    window : str, optional
        Window label for windowed stores (one status per (shard, window)).
    spec : str, optional
        The store's naming spec for sidecar keys; default: read once from
        the manifest (``None`` on a manifest-less root = the ``/1`` legacy
        names).

    Returns
    -------
    dict
        ``{int(shard_key): {"status": "hit"|"stale"|"miss", ...detail}}``.
    """
    from zagg.semantics import semantic_hash as _semantic_hash

    want = _semantic_hash(config)
    if spec is None:
        spec = (read_manifest(store_root, **store_kwargs) or {}).get("spec")
    items = shards.items() if hasattr(shards, "items") else ((k, None) for k in shards)
    return {
        int(key): shard_status(
            store_root,
            key,
            semantic_hash=want,
            granule_ids=ids,
            window=window,
            spec=spec,
            **store_kwargs,
        )
        for key, ids in items
    }


__all__ = ["STATUSES", "has_run", "shard_status"]
