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

:func:`classify_leaf_identity` (issue #388) is the worker-side flavor of the
same comparison: pure (the caller supplies the recorded sidecar), with the
id-set reading — equal / expansion / contraction / mixed — that the leaf
skip-if-current and contraction-guard decisions key on. The recorded id LIST
it diffs against is NOT in the sidecar: it sits in a sibling object read
lazily, on hash mismatch only, through :func:`leaf_recorded_ids` — so both
comparisons above keep their one small GET per shard.
"""

from __future__ import annotations

import logging

from zagg.hive import read_commit, read_manifest, shard_leaf_path
from zagg.store import open_store
from zagg.telemetry import canonical_granule_ids, granules_sha256, read_sidecar

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

    ``granule_ids`` is the shard's CURRENT catalog snapshot — resolved granule
    URLs (in any href form) on the aggregation path, STAC item ids/datetimes
    for raster; both sides reduce to the canonical bare granule id before the
    compare (:func:`zagg.telemetry.canonical_granule_id`). ``None`` skips the
    catalog check (identity match alone then gates the hit).

    .. warning::
       The catalog check is an EXACT ``granules_sha256`` comparison. Since the
       D19 hash epoch the href FORM no longer counts (an ``s3://`` href, an
       ``https://`` href and a bare catalog id of one granule compare equal),
       but a caller supplying genuinely different names — product short-names,
       say — still makes EVERY stamped shard report ``stale`` on
       ``catalog_match`` rather than a genuine catalog change.
       A ``"catalog grown/changed"`` stale carries both
       ``granules_sha256_recorded`` and ``granules_sha256_current`` so a
       mis-spaced caller can see the mismatch is systematic (every shard
       differs), not a real snapshot drift.
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
    if content := (stamp.get("content_hashes") or sidecar.get("content_hashes")):
        # O11 verifier, surfaced when recorded (never recomputed here). The
        # §5.3 preference is the STAMP's copy — sealed with the bytes it
        # certifies (issue #580) — with the sidecar as the fallback for a
        # leaf written before the stamp carried it.
        detail["content_hashes"] = content
    if granule_ids is not None:
        recorded = sidecar.get("granules_sha256")
        current = granules_sha256(granule_ids)
        detail["catalog_match"] = recorded == current
    if not detail["semantic_hash_match"]:
        return {"status": "stale", "reason": "semantic_hash mismatch or unrecorded", **detail}
    if detail["catalog_match"] is False:
        # Surface both digests so a systematic id-space mismatch (every shard
        # differs) is distinguishable from a genuine per-shard catalog change.
        return {
            "status": "stale",
            "reason": "catalog grown/changed",
            "granules_sha256_recorded": recorded,
            "granules_sha256_current": current,
            **detail,
        }
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
        ``{shard_key: granule_ids}`` (current catalog snapshot per shard, in
        any href form — see :func:`shard_status`) or a bare iterable of shard
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


#: Skip-if-current actions (issue #388): what the worker does with one unit.
IDENTITY_ACTIONS = ("skip", "rewrite", "refuse")


def leaf_recorded_ids(leaf_path, recorded, *, spec=None, store_kwargs=None):
    """The leaf's recorded granule-id list, or ``None`` (issue #388).

    Reads the sidecar's ``granules.json`` SIBLING
    (:func:`zagg.telemetry.read_granule_ids`) — the object the id list moved
    to so identity EQUALITY stays a ``granules_sha256`` compare against the
    small sidecar. Call it only on a hash mismatch: this is the one read the
    split exists to avoid paying per shard.

    ``recorded`` is the sidecar record it must PAIR with: the sibling is
    accepted only when its own ``granules_sha256`` equals the sidecar's, so a
    torn rewrite (one of the two PUTs lost, or a stale sibling beside a fresh
    sidecar) reads as no recorded set rather than as ids that could name the
    wrong granules as dropped — or, worse, hide real ones. Fail-open on every
    other fault (absent, unreadable, malformed): the RECORD's own
    ``granule_ids`` if it carries one (the back-compat window below), else
    ``None``, which the caller classifies ``unrecorded-ids`` and rewrites.

    The sibling is also read leniently on its ``spec`` marker — an absent or
    unknown one is not a rejection. Only the hash pairing decides, so a future
    ``zagg-granule-ids/N`` that keeps these two keys stays readable, and an
    unrecognized one degrades to the ruled ``unrecorded-ids`` fallback rather
    than to an error.
    """
    ids = _sibling_ids(leaf_path, recorded, spec, store_kwargs)
    if ids is not None:
        return ids
    # Back-compat (issue #388): leaves written in the window between PR #397's
    # merge (``3f13af2``) and the commit that moved the list out carry it ON
    # the record with no sibling at all. Without this they classify
    # ``unrecorded-ids`` forever; with it those stores self-heal, and the list
    # pairs by construction (``build_record`` hashed the very ids it embedded).
    legacy = (recorded or {}).get("granule_ids")
    return legacy if isinstance(legacy, list) else None


def _sibling_ids(leaf_path, recorded, spec, store_kwargs):
    """The paired granule-id sibling's list, or ``None``; see above."""
    from zagg.telemetry import read_granule_ids

    try:
        sibling = read_granule_ids(str(leaf_path), spec, **(store_kwargs or {}))
    except Exception as e:
        logger.warning(f"granule-id sibling read failed (degrading to rewrite, issue #388): {e}")
        return None
    if not isinstance(sibling, dict):
        return None
    ids = sibling.get("granule_ids")
    if not isinstance(ids, list):
        return None
    if sibling.get("granules_sha256") != (recorded or {}).get("granules_sha256"):
        logger.warning(
            f"granule-id sibling at {leaf_path} does not pair with the stats sidecar "
            f"(hash mismatch) — treating the recorded id set as unrecorded (issue #388)"
        )
        return None
    return ids


def _warn_on_collapsed_recorded_ids(rec_ids, canonical) -> None:
    """Say so when canonicalization collapsed DISTINCT recorded ids.

    The canonical form is a basename (:func:`zagg.telemetry.canonical_granule_id`,
    espg-ruled 2026-08-17), so two recorded hrefs differing only in prefix
    become one id. The diff in :func:`classify_leaf_identity` is over SETS, so
    for such a leaf the contraction guard loses its per-granule resolution
    *inside* the collapsed group: dropping one member of a collided pair leaves
    the set unchanged, and the leaf reads ``id-multiset-drift`` and REWRITES
    where the pre-epoch href-space diff refused and named the dropped href.

    Resolving that either way is a ruling on the ruled predicate itself
    (``recorded − planned ≠ ∅``) rather than an implementation choice: after
    collapse a recorded duplicate and two distinct collapsed granules are
    byte-identical data, so a multiset diff would recover the refusal here at
    the price of refusing the duplicate-drift case that
    ``test_duplicate_drift_with_equal_sets_rewrites`` pins as a rewrite. Left
    standing for espg (PR #420 review finding (2)) — but never silent.
    """
    groups: dict[str, set[str]] = {}
    for original, canon in zip(rec_ids, canonical):
        groups.setdefault(canon, set()).add(str(original))
    collided = sorted(c for c, originals in groups.items() if len(originals) > 1)
    if collided:
        logger.warning(
            f"canonicalization collapsed distinct recorded granule ids onto "
            f"{collided} — the contraction guard cannot resolve a drop WITHIN a "
            f"collapsed group on this leaf (it would read as id-multiset-drift "
            f"and rewrite); ids that collide only in basename are outside what "
            f"the canonical identity ruling can distinguish (issue #415)"
        )


def classify_leaf_identity(recorded, *, semantic_hash, planned_ids, load_recorded_ids=None) -> dict:
    """Classify one planned unit against its leaf's recorded identity (#388).

    Identity is the ruled PAIR: the run's ``semantic_hash`` (what/how, D19)
    x the unit's planned granule-id set (over what). The recorded side is
    the leaf's D20 stats sidecar (``semantic_hash``, ``granules_sha256``) —
    and, only when those disagree, the recorded id LIST, which lives in the
    sidecar's own sibling object and is fetched through
    ``load_recorded_ids`` (:func:`leaf_recorded_ids`).

    Fast path: ``granules_sha256`` equality (one compare — the common rerun
    case). The id-set difference runs ONLY on a mismatch, classifying:

    - ``skip`` / ``"equal"`` — both identity halves match: the leaf is
      current; the caller no-ops the fold and touches the leaf.
    - ``refuse`` / ``"contraction"`` or ``"mixed"`` — the recorded set has
      ids the planned set no longer lists (``recorded - planned != {}``,
      the ruled predicate — deliberately NOT strict-subset, so the mixed
      add-and-drop signature of an upstream purge behind a fresh catalog
      query trips it even when the planned set grew). ``missing`` names the
      dropped ids; the caller proceeds only under ``allow_contraction``.
      Judged on the id sets alone — a semantic mismatch does not excuse
      dropping inputs.
    - ``rewrite`` / everything else — expansion (``planned >= recorded``),
      a semantic change over covered inputs, or an unverifiable leaf
      (``no-sidecar``; ``unknown-planned``, no planned set to diff; or
      ``unrecorded-ids``, a pre-#388 sidecar with no recorded set to diff):
      wholesale D4 rewrite exactly as today.

    Conservatism here is ONE-SIDED, not the symmetric rule
    :func:`shard_status` follows. For the *skip* decision rewrite is the
    safe direction — a false skip strands a stale leaf, so every ambiguity
    degrades toward recompute. For the *contraction guard* it is not: a
    rewrite over a contracted input set destroys aggregated data the
    operator was never told about, which is the loss issue #388 exists to
    prevent. ``unrecorded-ids`` sits exactly on that seam — the recorded
    hash already says something changed, while the direction is unknowable
    — so it carries its OWN classification rather than sharing
    ``no-sidecar``/``expansion``: callers must count and surface it apart
    from the ordinary rewrites (the phase-2 run stats, the phase-4 operator
    docs), because the guard cannot protect any leaf written before this
    release — no leaf written before issue #388 has the granule-id sibling
    that records its input set. The canonical-identity epoch opened one more
    seam on that same side: two recorded hrefs differing only in prefix
    collapse to ONE canonical id, and this set diff then cannot see a drop
    *within* that group — the leaf rewrites where the pre-epoch diff refused.
    Never silent (:func:`_warn_on_collapsed_recorded_ids`), and left standing
    as a question on the predicate rather than resolved here.

    Parameters
    ----------
    recorded : dict or None
        The leaf's D20 sidecar record (:func:`zagg.telemetry.read_sidecar`),
        or ``None`` when absent.
    semantic_hash : str or None
        The run's semantic-core hash (D19). ``None`` never skips.
    planned_ids : iterable of str, or None
        The unit's planned granule ids — resolved granule URLs (or paired-asset
        entries) on the aggregation path, STAC item ids/datetimes for raster.
        Any href form is accepted: both sides are reduced to the canonical
        driver-stripped bare id (:func:`zagg.telemetry.canonical_granule_id`)
        before anything is compared, so the driver a run reads with cannot move
        the verdict. ``None`` means the
        planned set is UNKNOWN and never refuses (``unknown-planned``); the
        empty list means the unit genuinely plans no inputs, which against a
        non-empty recorded set is a full contraction and does refuse.
    load_recorded_ids : callable, optional
        Zero-argument loader for the leaf's RECORDED id list, called at most
        once and ONLY when the recorded and planned ``granules_sha256``
        DISAGREE — never on the skip path, and never on the equal-hash /
        changed-semantic quadrant, whose verdict the hashes alone decide. The
        laziness is the whole point of keeping the list out of the record
        (issue #388's ruling). ``None`` (or a loader returning ``None``)
        means no recorded set is available: ``unrecorded-ids``.

    Returns
    -------
    dict
        ``{"action", "classification", "missing"}`` — ``action`` one of
        :data:`IDENTITY_ACTIONS`; ``missing`` the sorted ``recorded -
        planned`` difference (non-empty exactly on ``refuse``).
    """
    if recorded is None:
        return {"action": "rewrite", "classification": "no-sidecar", "missing": []}
    if planned_ids is None:
        # An UNKNOWN planned set is not the empty one: diffing against it would
        # name every recorded id as dropped and refuse the loudest way there is.
        # Ambiguity degrades to rewrite; ``[]`` below stays a real contraction.
        return {"action": "rewrite", "classification": "unknown-planned", "missing": []}
    # Both sides of every comparison below run in the CANONICAL id space
    # (:func:`zagg.telemetry.canonical_granule_id`, espg-ruled 2026-08-17): the
    # driver-stripped bare id. Without it a driver switch — pure packaging in
    # the D19 core — renames every planned id, and the diff would report the
    # whole recorded set as dropped and the whole planned set as added for a
    # rerun over exactly the same granules. Canonicalizing the RECORDED side
    # too is what lets a pre-epoch leaf (full hrefs on its sibling) diff
    # cleanly against a post-epoch plan. Such a leaf still REWRITES once — its
    # stored digest is in the href space, so ``hashes_match`` below is False by
    # construction and ``equal`` is unreachable — but it rewrites as
    # drift/expansion rather than refusing as a spurious contraction.
    planned = canonical_granule_ids(planned_ids) or []
    rec_hash = recorded.get("granules_sha256")
    semantic_match = semantic_hash is not None and recorded.get("semantic_hash") == semantic_hash
    hashes_match = rec_hash is not None and rec_hash == granules_sha256(planned)
    if hashes_match and semantic_match:
        return {"action": "skip", "classification": "equal", "missing": []}
    if hashes_match:
        # Equal hashes mean an identical sorted id multiset, so the diff is
        # provably empty and the ONLY reachable verdict is semantic-mismatch:
        # decide it here rather than paying the sibling GET for a read that
        # cannot change the answer. This is the ordinary config-only rerun
        # (same inputs, changed semantic core) — at CA o9 that would be 2,721
        # needless GETs of up to ~550 KB each, which is exactly the per-shard
        # cost the id list was split out of the sidecar to avoid. It also
        # keeps this quadrant OFF ``unrecorded-ids`` when the sibling is
        # absent: the guard is not inert here, it has proved no contraction.
        return {"action": "rewrite", "classification": "semantic-mismatch", "missing": []}
    # The ONE read the fast path exists to avoid: the recorded id list lives
    # beside the sidecar, not in it (issue #388's ruling on question (6)), and
    # is fetched only now that the hashes disagree.
    rec_ids = load_recorded_ids() if load_recorded_ids is not None else None
    if rec_ids is None:
        # No recorded set to diff — a pre-#388 leaf, a lost sibling PUT, or a
        # sibling that does not pair with the sidecar. Something mismatched,
        # but the direction is undecidable, so today's rewrite. NOT a benign
        # ambiguity: on a contracted rerun this is the wholesale rewrite the
        # guard exists to stop, and every leaf written before this release
        # lands here. Its own
        # classification for exactly that reason — count it apart from the
        # ordinary rewrites and say so in the operator docs (see the class
        # docstring's one-sided-conservatism note).
        return {"action": "rewrite", "classification": "unrecorded-ids", "missing": []}
    recorded_canonical = canonical_granule_ids(rec_ids) or []
    _warn_on_collapsed_recorded_ids(rec_ids, recorded_canonical)
    recorded_set = set(recorded_canonical)
    missing = sorted(recorded_set - set(planned))
    if missing:
        added = set(planned) - recorded_set
        return {
            "action": "refuse",
            "classification": "mixed" if added else "contraction",
            "missing": missing,
        }
    if set(planned) == recorded_set:
        # Same id set, yet the fast path failed: a semantic-core change over
        # identical inputs, an unrecorded semantic hash, or duplicate/order
        # drift in the hash's multiset. Not current, not a contraction.
        classification = "id-multiset-drift" if semantic_match else "semantic-mismatch"
        return {"action": "rewrite", "classification": classification, "missing": []}
    return {"action": "rewrite", "classification": "expansion", "missing": []}


__all__ = [
    "IDENTITY_ACTIONS",
    "STATUSES",
    "classify_leaf_identity",
    "has_run",
    "leaf_recorded_ids",
    "shard_status",
]
