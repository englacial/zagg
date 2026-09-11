"""Morton-hive store layout: leaf paths, manifest, and commit stamp (issue #199).

Phase 2 of the layout migration (``docs/design/sparse_coverage.md`` §2-§3).
Under ``output.store_layout: hive`` each shard is its own **self-describing
leaf zarr** under a morton digit tree::

    {store_root}/
      morton_hive.json               <- static manifest (§3); root-only exception
      {sign+base}/{d1}/.../{d_n}/    <- one decimal digit per level (D2)
        {full_id}.zarr/              <- vanilla zarr v3 leaf (D3)

- Ids are morton decimal strings (D1); the leaf path is computed by mortie's
  ``hive_path`` (the convention is owned by the mortie spec) and re-checked
  here against the node invariant.
- **Node invariant (D5)**: below the root a node contains only digit children
  (``[1-4]/``, or the ``{sign+base}`` component at the first level) and
  ``*.zarr`` objects — zero zarr metadata above the leaf, so 2,000 workers
  share no mutable state and a delimiter-LIST with no digit children is a
  definitive "nothing finer exists".
- **Manifest (D6)**: ``morton_hive.json`` is written once — at finalize since
  issue #252 (reader-facing only; workers never read it) — and never touched
  during a run; with it every shard path is computable with zero requests.
  The convention is versioned (``morton-hive/1``) from day one.
- **Commit stamp (D4)**: the shard's FINAL write is a root
  ``group.attrs.update(...)`` marking completion (plus cell count, timestamp,
  granule count). A ``.zarr/`` prefix whose root metadata lacks the stamp is
  debris — incomplete, ignorable, safe to overwrite on retry. This is NOT
  consolidated metadata: one small PUT on an object that must exist anyway.

- **Coverage (§4, issue #200)**: the stamp carries a ``coverage`` payload —
  tier 0 is the shard's morton box, the canonical <= 4-member cover of its
  occupied cells (:func:`zagg.grids.morton.morton_box`), padded to exactly
  four decimal-string slots with JSON-null sentinels. Zero extra requests
  (it rides the stamp PUT) and debris semantics are inherited: a torn
  worker's coverage never becomes visible. Exact cell-order occupancy is a
  zstd-compressed bitmap SIDECAR inside the leaf (``coverage.moc`` — the O8
  resolution; the one recorded exception to the vanilla-v3 leaf: data reads
  are unaffected, but member enumeration warns and skips it), written
  before the stamp and pointed to from the envelope; attrs stay lean and the
  extra GET is paid only by readers that pass the box test. The optional end-of-run
  root ``coverage.moc`` (issue #200 phase 3, default-on for hive) is a
  shard-order ranges MOC at the store root — the second root-only object,
  written fail-open by the dispatcher (locally) or a fire-and-forget worker
  invoke (Lambda), and a regenerable cache under D9. The manifest's
  ``pyramid`` block declares the overview schedule at template time and the
  §7 sweep populates its actuals (D11/D22 — issue #201).
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone

import numpy as np
import zarr
from zarr.errors import GroupNotFoundError

from zagg.store import open_object_store, put_object
from zagg.windows import leaf_name, split_leaf_name, union_time_range

logger = logging.getLogger(__name__)

#: Convention version recorded in the manifest and the commit stamp (D6).
HIVE_SPEC = "morton-hive/1"
#: The temporal superset convention (issue #246, D13/D15): declared iff the
#: manifest carries a temporal block; a ``/1`` store *is* a ``/2`` store with
#: ``schedule: none``, so ``/1`` stays the spec written for unwindowed stores.
HIVE_SPEC_V2 = "morton-hive/2"
#: Root manifest object name (the root-only exception to the node invariant).
MANIFEST_NAME = "morton_hive.json"
#: Root-group attrs key carrying the commit stamp (D4).
COMMIT_ATTR = "morton_hive_commit"
#: Convention version of the stamp's coverage payload (§4 tier 0, issue #200).
COVERAGE_SPEC = "morton-moc/1"
#: Fixed slot count of the tier-0 morton box (2-4 members, null-padded).
COVERAGE_BOX_SLOTS = 4
#: In-leaf occupancy-bitmap sidecar object name (issue #200 phase 2, O8) —
#: the one recorded exception to the "vanilla zarr v3 leaf" claim: a foreign
#: key inside ``{full_id}.zarr/`` that zarr readers ignore (data reads are
#: unaffected; ``members()``/``tree()`` emit a ``ZarrUserWarning`` and skip
#: it — review finding, PR #208 round 2).
COVERAGE_SIDECAR = "coverage.moc"
#: zstd level for the sidecar bitmap — fixed so identical occupancy produces
#: byte-identical sidecars across workers and backends.
_ZSTD_LEVEL = 3
#: Store-ROOT coverage object name (issue #200 phase 3): the shard-order MOC
#: for the one-GET bootstrap — the second root-only exception to the node
#: invariant, next to the manifest. Same name as the in-leaf sidecar
#: (:data:`COVERAGE_SIDECAR`), different location and encoding.
ROOT_COVERAGE_NAME = "coverage.moc"

#: Canonical semantic core object name at the product root (D19): a DERIVED
#: convenience (D9 cache class) — deterministic YAML of the output-defining
#: subset. The manifest's ``semantic_hash`` is the truth; divergence resolves
#: to the hash.
AGGREGATION_CORE_NAME = "aggregation.yaml"

#: Product-name grammar (D19; normative on the mortie spec page §6.5):
#: URL-safe lowercase alphanumerics plus ``-``/``_``, at most
#: :data:`PRODUCT_NAME_MAX` chars. The base-component exclusion (``-?[1-6]``,
#: :func:`_is_base_component`) is checked separately — a name shaped like a
#: hive base component would make the walker's child classification ambiguous
#: at a multi-product store root.
_PRODUCT_NAME_RE = re.compile(r"^[a-z0-9_-]+$")

#: Product-name length cap (D19; espg ruling mirrored from the mortie spec page
#: §6.5). Derivation: a POSIX filename component is 255 bytes; the immutable-
#: provenance decoration reserves 13 (``+`` + a 12-hex digest) ⇒ a 242-byte
#: hard ceiling. 192 sits 50 under that, and leaves the S3 total-key and
#: PATH_MAX budgets comfortable (the name also nests under the store prefix and
#: the digit tree). The grammar is single-byte per char, so char count == byte
#: count here.
PRODUCT_NAME_MAX = 192


def validate_product_name(name: str) -> str:
    """Validate a D19 product name; returns it.

    Grammar (normative on the mortie spec page §6.5): one or more of
    ``[a-z0-9_-]``, at most :data:`PRODUCT_NAME_MAX` characters, and never
    matching the morton base-component grammar ``-?[1-6]`` — the root-form
    discrimination (:func:`classify_store_root`) depends on names and digit
    components being disjoint. Names are URL-safe by construction (they appear
    in gridlook deep-links and web paths).
    """
    if not isinstance(name, str) or not _PRODUCT_NAME_RE.match(name):
        raise ValueError(
            f"product name {name!r} does not match the D19 grammar ([a-z0-9_-]+, "
            f"lowercase; normative on the mortie spec page)"
        )
    if len(name) > PRODUCT_NAME_MAX:
        raise ValueError(
            f"product name {name!r} is {len(name)} chars; the D19 grammar caps names "
            f"at {PRODUCT_NAME_MAX} (normative on the mortie spec page §6.5 — a POSIX "
            f"255-byte filename component less the 13-byte immutable-provenance "
            f"decoration)"
        )
    if _is_base_component(name):
        raise ValueError(
            f"product name {name!r} matches the morton base-component grammar "
            f"(-?[1-6]); such names are excluded so a store root's children stay "
            f"unambiguous (D19)"
        )
    return name


def product_root(store_root: str, name: str) -> str:
    """Root prefix of product ``name`` under a multi-product store (D19).

    A product subtree is a COMPLETE, unmodified morton-hive store (bare-named
    manifest + MOC + digit tree), so everything that takes a ``store_root``
    takes this value unchanged.
    """
    return f"{store_root.rstrip('/')}/{validate_product_name(name)}"


def effective_store_root(store_path: str, config) -> str:
    """The store root a run writes into: the product root when one is named.

    ``output.product_name`` (issue #299) prefixes the configured store path
    with the D19 ``{name}/`` product root; absent, the bare single-product
    layout is unchanged (byte-identical stores — the D19 revision is
    additive).
    """
    from zagg.config import get_product_name

    name = get_product_name(config)
    return product_root(store_path, name) if name else store_path


def classify_store_root(store_root: str, **store_kwargs) -> str:
    """Root-form discrimination by CONTENT (D19): what sits at ``store_root``.

    Returns ``"bare"`` (a manifest at the root ⇒ a single-product store — the
    digit tree lives right here), ``"products"`` (no manifest, and at least
    one name-shaped child prefix ⇒ a directory of product roots), or
    ``"empty"`` (neither). Base-component-shaped children without a manifest
    still classify as ``"bare"`` — the manifest write is async (issue #252),
    so a store mid-first-run must not be misread as a product directory.
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    if _read_json(store, MANIFEST_NAME) is not None:
        return "bare"
    listing = obstore.list_with_delimiter(store)
    children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
    if any(_is_base_component(c) for c in children):
        return "bare"
    names = [c for c in children if _is_valid_product_name(c)]
    return "products" if names else "empty"


def list_products(store_root: str, **store_kwargs) -> dict:
    """``{name: manifest}`` for every product under a multi-product root (D19).

    Discovery is the root listing itself — no name↔hash translation layer:
    each name-shaped child prefix is read for its bare-named
    ``morton_hive.json``. Children without a readable manifest are skipped
    (debris or mid-write; the D4 posture — absence of a manifest means the
    product is not yet discoverable).
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    listing = obstore.list_with_delimiter(store)
    children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
    products = {}
    for name in sorted(c for c in children if _is_valid_product_name(c)):
        manifest = read_manifest(product_root(store_root, name), **store_kwargs)
        if manifest is not None:
            products[name] = manifest
    return products


def _is_valid_product_name(name: str) -> bool:
    """Whether ``name`` satisfies the D19 product-name grammar (no raise)."""
    try:
        validate_product_name(name)
    except ValueError:
        return False
    return True


def shard_leaf_path(store_root: str, shard_key, window: str | None = None) -> str:
    """Absolute path of a shard's leaf zarr under ``store_root`` (D2/D3).

    Computed by mortie's ``hive_path`` — the layout convention is owned by the
    mortie spec — and re-checked against the node invariant (D5) so a future
    drift in either side fails loudly instead of writing a stray prefix.
    ``window`` (issue #246, D13) selects the shard's time-windowed leaf,
    ``{full_id}_{window}.zarr``, at the same node; ``None`` is the bare
    schedule-``none`` leaf, byte-identical to pre-windowing paths. Raises
    ``ValueError`` on an invalid shard key or window label.
    """
    from mortie import MortonIndexArray

    word = int(shard_key)
    if word < 0:
        raise ValueError(
            f"shard key must be a packed morton word (got {word}); parse a decimal "
            f"id with zagg.grids.morton.morton_word first"
        )
    rel = MortonIndexArray.from_words(np.asarray([word], dtype=np.uint64)).hive_path()[0]
    if window is not None:
        node, _sep, bare = rel.rpartition("/")
        rel = f"{node}/{leaf_name(bare.removesuffix('.zarr'), window)}"
    check_node_invariant(rel)
    return f"{store_root.rstrip('/')}/{rel}"


def check_node_invariant(rel_path: str) -> None:
    """Raise unless ``rel_path`` is a legal hive leaf path (D5).

    Below the root only digit components are allowed — ``{sign+base}``
    (optional ``-``, one digit ``1..6``) at the first level, one ``1..4`` digit
    per level after — terminating in ``{full_id}.zarr`` (or the windowed
    ``{full_id}_{window}.zarr``, issue #246: split on the first ``_``, window
    label per the frozen grammar) whose id equals the concatenated components.
    This is the walker's contract: any other name under the root (bar the
    manifest and the root ``coverage.moc``) breaks child classification.
    """
    parts = rel_path.strip("/").split("/")
    leaf = parts[-1]
    ok = len(parts) >= 2 and leaf.endswith(".zarr")
    if ok:
        head, digits = parts[0], parts[1:-1]
        try:
            full_id, _window = split_leaf_name(leaf)
        except ValueError:
            full_id = None  # malformed window label -> not a legal leaf
        ok = _is_base_component(head)
        ok = ok and all(len(d) == 1 and d in "1234" for d in digits)
        ok = ok and full_id == head + "".join(digits)
    if not ok:
        raise ValueError(f"path {rel_path!r} violates the hive node invariant (D5)")


def build_manifest(grid, dataset: dict | None = None, windowing: dict | None = None) -> dict:
    """Build the static ``morton_hive.json`` payload for one store (§3, D6).

    ``grid`` supplies the orders; ``dataset`` (typically the ShardMap's
    ``metadata``) supplies identity — only ``short_name`` and ``version`` are
    recorded. The split schedule is implicit under D2 (one digit per level down
    to the shard order) but recorded explicitly for forward compatibility. The
    ``pyramid`` block carries the template-time overview declaration —
    per-family order schedule + per-field D24 composability classes
    (:func:`zagg.sweep_overview.build_pyramid_block`, issue #201); the §7
    sweep populates ``materialized`` actuals but never rewrites the
    declaration (overviews are a second-pass sweep, never written at
    fan-out time — D11). A ``/2`` declaration additionally carries the
    ``multiscales`` discovery mirror beside it (issue #392, spec §4.9):
    derived from the block by :func:`zagg.multiscales.manifest_multiscales`,
    never authoritative, absent on ``/1``/declared-off manifests.

    ``windowing`` (issue #246) is the normalized declaration from
    :func:`zagg.config.get_windowing`; when given, the manifest declares
    ``spec: "morton-hive/2"`` and carries the D15 **temporal block** — the
    STATIC schema half of the temporal split (schedule, time encoding, the
    membership ``time_field``, the explicit windows list, and the append
    policy). Temporal EXTENT deliberately never lives here: actual ranges are
    leaf-stamp truth and root-summary cache. ``None`` writes the ``/1``
    manifest byte-identical to pre-windowing runs.
    """
    from zagg.semantics import semantic_hash
    from zagg.sweep_overview import build_pyramid_block

    dataset = dataset or {}
    manifest = {
        "spec": HIVE_SPEC_V2 if windowing else HIVE_SPEC,
        "dataset": {
            "short_name": dataset.get("short_name"),
            "version": dataset.get("version"),
        },
        # D19 (issue #299): the FULL sha256 of the canonicalized semantic
        # core — a frozen key, so reusing a product name/root with different
        # aggregation semantics refuses up front like an orders mismatch.
        "semantic_hash": semantic_hash(grid.config),
        "cell_order": int(grid.child_order),
        "shard_order": int(grid.parent_order),
        "split_schedule": [1] * int(grid.parent_order),
        # D21: how many morton digits each path component chunks. Existing
        # stores are retroactively 1 (the _frozen normalization); new stores
        # declare it explicitly.
        "path_grouping": 1,
        # chunk_order feeds the ruled issue #384 /2 default flip; the raster
        # and K==1 exemptions live inside build_pyramid_block.
        "pyramid": build_pyramid_block(
            grid.config, int(grid.parent_order), chunk_order=getattr(grid, "chunk_order", None)
        ),
        "generated_at": _utcnow(),
    }
    # The issue #392 discovery mirror: present exactly when the block above
    # declares /2 (absent = pre-convention store; the pyramid block wins on
    # any disagreement — it is the normative declaration, spec §4.5/§4.9).
    from zagg.multiscales import manifest_multiscales

    multiscales = manifest_multiscales(manifest)
    if multiscales is not None:
        manifest["multiscales"] = multiscales
    if windowing:
        temporal = {
            "schedule": windowing["schedule"],
            "time_field": windowing["time_field"],
            "epoch": windowing["epoch"],
            "scale": windowing["scale"],
            "units": windowing["units"],
            # D15 records calendar alongside encoding/units/epoch. Only
            # proleptic_gregorian is supported this round: the three scales
            # (utc/gps/tai) all derive from stdlib datetime, which is proleptic
            # Gregorian.
            "calendar": "proleptic_gregorian",
            # Generative schedules append by adding leaves the schedule already
            # describes (manifest untouched); the explicit list is the noted
            # D15 exception — appending outside it re-templates the manifest.
            "append_policy": (
                "re-template" if windowing["schedule"] == "explicit" else "new-window"
            ),
        }
        if windowing.get("windows"):
            temporal["windows"] = windowing["windows"]
        manifest["temporal"] = temporal
    return manifest


def validate_manifest(
    store_root: str, manifest: dict, *, overwrite: bool = False, **store_kwargs
) -> dict | None:
    """Read-only frozen-key precheck — the fail-fast half of the manifest guard.

    Split out of :func:`ensure_manifest` when the manifest WRITE came off the
    synchronous pre-dispatch path (issue #252). The write moving off the
    critical path is fine for readers, but it dragged the writer-side guard
    along with it — so an incompatible rerun could write mixed-order leaves
    before the check ever fired (D2). This function is the guard on its own:
    the lambda ping runs it BEFORE fan-out to refuse an incompatible existing
    store up front, while the write itself rides the async init-time setup
    invoke with a finalize backstop (issue #252 hybrid; the local dispatcher
    writes directly at init via :func:`ensure_manifest`, which calls this
    first).

    Performs exactly the checks :func:`ensure_manifest` does and writes nothing:
    reads the existing manifest; on an existing store whose FROZEN keys mismatch
    (:data:`_FROZEN_MANIFEST_KEYS`) it raises — the same ``does not match this
    run`` refusal without ``overwrite``, and the same ``list_with_delimiter``
    shard-data refusal with ``overwrite`` (the D2 old-order-masquerade footgun).
    Returns the existing manifest (``None`` on a fresh root).
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    existing = _read_json(store, MANIFEST_NAME)
    frozen_matches = _frozen_matches(existing, manifest)
    if existing is not None and not overwrite:
        if not frozen_matches:
            raise ValueError(
                f"{MANIFEST_NAME} at {store_root} does not match this run "
                f"(existing {existing!r} vs {manifest!r}); this store was templated "
                f"for different orders/identity — clear the store root (or pick a "
                f"new one) before writing with this configuration"
            )
        return existing
    if overwrite and existing is not None and not frozen_matches:
        # One delimiter-LIST: a {sign+base}-shaped child means shards were
        # already written under the OLD configuration. Their leaves are
        # stamped and walker-discoverable, so replacing just the manifest
        # would leave them masquerading as legal mixed-order data (D2).
        # ``semantic_hash`` is a frozen key (D19), so overwrite does NOT
        # bypass the semantic-hash refusal when both manifests carry one —
        # a changed aggregation block over existing data refuses right here.
        listing = obstore.list_with_delimiter(store)
        children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
        if any(_is_base_component(c) for c in children):
            raise ValueError(
                f"refusing to overwrite {MANIFEST_NAME} at {store_root} with "
                f"different orders/identity: the digit tree already has shard "
                f"data (e.g. {children[0]!r}/), and overwrite replaces the "
                f"manifest only — clear the store root first"
            )
    # Hash-guard coherence (issue #341): ``_frozen_matches`` EXEMPTS
    # ``semantic_hash`` when EITHER side lacks it (no hash to compare — the
    # exemption keeps pre-#299 stores resumable), so an overwrite proceeds past
    # a comparison that never happened. Warn on BOTH directions of that
    # exemption (fold review: the guard was asymmetric where the exemption is
    # symmetric), because they fail differently:
    #
    # * existing has no hash, this run does  -> the existing DATA's semantics are
    #   unverifiable. The store is coherent going forward (the rewrite stamps
    #   this run's hash, and every re-dispatched leaf is re-templated wholesale
    #   — ``emit_shard_template`` clears the leaf prefix first) but leaves this
    #   run does not rewrite may carry the old schema.
    # * existing HAS a hash, this run does not -> the overwrite strips the
    #   recorded hash from a hashed store, un-provenancing a #299 store rather
    #   than merely failing to verify one. Strictly the worse direction, and the
    #   one no warning covered.
    #
    # ``build_manifest`` always stamps a hash, so the second direction is not
    # reachable from a zagg-built manifest today — it is the "older zagg (or a
    # hand-assembled manifest) writes into a newer store" shape, one refactor of
    # ``build_manifest`` away from being live.
    hash_exempted = existing is not None and (existing.get("semantic_hash") is None) != (
        manifest.get("semantic_hash") is None
    )
    if overwrite and frozen_matches and hash_exempted:
        listing = obstore.list_with_delimiter(store)
        children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
        if any(_is_base_component(c) for c in children):
            if existing.get("semantic_hash") is None:
                detail = (
                    "the existing manifest predates semantic hashing (issue #299), so "
                    "semantic compatibility of the existing shard data cannot be "
                    "verified; re-dispatched leaves are re-templated wholesale, but "
                    "leaves this run does not rewrite may carry the old schema"
                )
            else:
                detail = (
                    "this run's manifest carries NO semantic hash while the existing "
                    "one does (issue #299), so the overwrite DROPS the recorded hash "
                    "from a hashed store — the existing shard data becomes "
                    "un-provenanced, not merely unverified"
                )
            logger.warning(f"overwriting {MANIFEST_NAME} at {store_root}: {detail}")
    return existing


def ensure_manifest(
    store_root: str,
    manifest: dict,
    *,
    overwrite: bool = False,
    config=None,
    **store_kwargs,
):
    """Write the root manifest; verify it on reruns (idempotent).

    Lifecycle (issue #252 hybrid): the PRIMARY write runs at init — the local
    dispatcher writes directly pre-dispatch; the lambda dispatcher fires the
    ``mode="setup"`` hive branch as a fire-and-forget Event invoke right
    after the ping — so the manifest typically lands within seconds of init
    (best-effort: the Event invoke shares worker concurrency and runs
    retries-0, deferring to the finalize backstop under throttling or a
    dropped invoke) and a reader can consume completed leaves while the store
    builds. Finalize calls this
    again as an idempotent BACKSTOP (an existing frozen-key-matching manifest
    is accepted — no second PUT): worker Event invokes run with retries 0, so
    a lost async init write self-heals at finalize. The fail-fast half of the
    guard is exposed separately as :func:`validate_manifest` (the ping's
    read-only precheck), which this function runs first.

    A retry into an existing hive store must be able to proceed (that is the
    D4 debris/retry model), so an existing manifest is accepted — but only if
    its FROZEN keys match the run's own (:data:`_FROZEN_MANIFEST_KEYS`: orders
    + identity + schedule — the flat path's ``_check_signature`` analogue).
    ``generated_at`` and ``pyramid`` are excluded: the pyramid block is
    populated/updated by the §7 sweep by design (D11), so comparing it would
    brick every resume after the first sweep.

    ``overwrite=True`` replaces the MANIFEST ONLY — it never clears data. To
    guard against the silent-corruption footgun (committed leaves from the old
    orders would survive a "re-template" and be indistinguishable from legal
    mixed-order data, D2), an overwrite that CHANGES the frozen keys refuses
    when the digit tree already has children (one delimiter-LIST); clear the
    store root first. Returns the manifest now in effect.
    """

    store = open_object_store(store_root, **store_kwargs)
    existing = validate_manifest(store_root, manifest, overwrite=overwrite, **store_kwargs)
    if existing is not None and not overwrite:
        return existing
    put_object(store, MANIFEST_NAME, json.dumps(manifest, indent=1).encode())
    if config is not None:
        write_semantic_core(store_root, config, **store_kwargs)
    return manifest


def write_semantic_core(store_root: str, config, **store_kwargs) -> None:
    """PUT the canonical semantic core as ``aggregation.yaml`` (D19).

    A derived convenience next to the manifest (D9 cache class): sorted-key,
    deterministic YAML of the output-defining subset, so product names stay
    human-inspectable without a registry. FAIL-OPEN — the manifest's
    ``semantic_hash`` is the truth, and the core is regenerable straight from
    the config (rewriting it is a single :func:`write_semantic_core` call), so
    a failed PUT must not fail the run. The D22 pyramid sweep is the intended
    owner of the rewrite once it lands; there is no regenerator today.
    """
    import yaml

    from zagg.semantics import semantic_core

    try:
        payload = yaml.safe_dump(semantic_core(config), sort_keys=True)
        put_object(
            open_object_store(store_root, **store_kwargs),
            AGGREGATION_CORE_NAME,
            payload.encode(),
        )
    except Exception as e:
        logger.warning(f"{AGGREGATION_CORE_NAME} write failed (fail-open, D9 cache class): {e}")


#: Manifest keys the resume match-check compares (orders + identity + split
#: and temporal schedules — a windowing change re-partitions the leaf names,
#: so it must refuse resume exactly like an orders change). ``generated_at``
#: (a timestamp) and ``pyramid`` (populated by the §7 sweep, D11) are mutable
#: by design and excluded. ``temporal`` projects to ``None`` on both sides for
#: pre-#246 manifests, so existing stores resume unchanged.
_FROZEN_MANIFEST_KEYS = (
    "spec",
    "dataset",
    "semantic_hash",
    "cell_order",
    "shard_order",
    "split_schedule",
    "path_grouping",
    "temporal",
)


def _frozen(manifest: dict) -> dict:
    """The frozen-key projection of a manifest (resume/overwrite match-check).

    ``path_grouping`` normalizes ``absent -> 1`` (D21: existing stores are
    retroactively ``1``), so appends to pre-D21 manifests never refuse on it.
    """
    frozen = {k: manifest.get(k) for k in _FROZEN_MANIFEST_KEYS}
    frozen["path_grouping"] = manifest.get("path_grouping") or 1
    return frozen


def _frozen_matches(existing: dict | None, manifest: dict) -> bool:
    """Whether two manifests agree on every frozen key (issue #299).

    ``semantic_hash`` is compared only when BOTH sides declare it: a
    pre-#299 manifest lacks the key, and refusing every append to an
    existing store on its absence would brick resumes — the orders/schedule
    keys still guard those stores. Two hash-carrying manifests must match
    exactly (D19: the hash is a frozen key).
    """
    if existing is None:
        return False
    fa, fb = _frozen(existing), _frozen(manifest)
    if fa["semantic_hash"] is None or fb["semantic_hash"] is None:
        fa.pop("semantic_hash")
        fb.pop("semantic_hash")
    return fa == fb


def _is_base_component(name: str) -> bool:
    """Whether ``name`` is a ``{sign+base}``-shaped hive root child (D5)."""
    base = name[1:] if name.startswith("-") else name
    return len(base) == 1 and base in "123456"


def read_manifest(store_root: str, **store_kwargs) -> dict | None:
    """Read ``morton_hive.json`` from a store root; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), MANIFEST_NAME)


def build_coverage(
    shard_key, occupied, cell_order: int, *, bitmap: bytes | None = None, full: bool = False
) -> dict:
    """Coverage payload for one shard's commit stamp (§4, issue #200).

    ``occupied`` is the shard's occupied cell words (mixed order allowed —
    the cells ``cells_with_data`` counts); the box is their canonical
    <= 4-member cover (:func:`zagg.grids.morton.morton_box`). ``None``/empty
    falls back to the trivial 1-member cover, the shard id itself — always a
    valid ancestor of its own coverage. Members are serialized as decimal
    morton strings (D1), padded to exactly :data:`COVERAGE_BOX_SLOTS` slots
    with trailing ``None`` (JSON null) sentinels — the recorded pad lean.
    ``cell_order`` records the order occupancy was measured at; ``source``
    the producer (``"worker"`` at the leaf tier — phase-3 root and
    sweep-composed payloads record theirs). ``generated_at`` is DELIBERATELY
    omitted at the leaf (review finding, PR #208): the payload rides the
    commit stamp, whose ``written_at`` is the one clock and one writer;
    root/ancestor carriers add their own timestamp fields under this same
    spec (per-carrier-optional).

    ``bitmap`` (phase 2, the O8 resolution) is the encoded sidecar payload
    from :func:`encode_coverage_bitmap`; when given the envelope grows the
    ``encoding``/``sidecar`` pointer plus compressed/raw byte sizes. A
    box-only envelope (``None``, the phase-1 shape) omits those keys — a
    reader treats their absence as "box only". Raises ``ValueError`` if the
    box escapes the shard's subtree (occupied cells from another shard are
    an upstream bug, never stamped).

    ``full`` (issue #246, D14) marks whole-subtree coverage: the ``encoding``
    discriminator becomes ``"full"`` and NO sidecar is written or pointed to
    — the shard id itself is the exact MOC, so readers skip the sidecar GET
    entirely. Decided by one popcount at stamp time (the caller's job);
    mutually exclusive with ``bitmap``.
    """
    if full and bitmap is not None:
        raise ValueError(
            "full=True and a bitmap payload are mutually exclusive: a fully "
            "occupied subtree writes no sidecar (D14)"
        )
    from zagg.grids.morton import morton_box, morton_decimal

    shard = morton_decimal(shard_key)
    if occupied is None or len(occupied) == 0:
        labels = [shard]
    else:
        labels = [morton_decimal(w) for w in morton_box(occupied)]
    if len(labels) > COVERAGE_BOX_SLOTS or any(not s.startswith(shard) for s in labels):
        raise ValueError(
            f"coverage box {labels} escapes shard {shard}'s subtree — occupied "
            f"cells must be the shard's own (the shard id is always a valid "
            f"trivial cover, so this is an upstream cell-assignment bug)"
        )
    coverage = {
        "spec": COVERAGE_SPEC,
        "box": labels + [None] * (COVERAGE_BOX_SLOTS - len(labels)),
        "cell_order": int(cell_order),
        "source": "worker",
    }
    if full:
        coverage["encoding"] = "full"
    elif bitmap is not None:
        n_bits = 4 ** (int(cell_order) - _decimal_order(shard))
        coverage.update(
            encoding="bitmap",
            sidecar=COVERAGE_SIDECAR,
            nbytes=len(bitmap),
            raw_nbytes=-(-n_bits // 8),
        )
    return coverage


def _decimal_order(decimal: str) -> int:
    """HEALPix order of a D1 decimal id (one digit per level past the base)."""
    return len(decimal) - (2 if decimal.startswith("-") else 1)


def _cell_ranks(shard: str, cells, cell_order: int) -> np.ndarray:
    """Bit index of each cell in the shard-subtree bitmap (frozen convention).

    Bit ``i`` is the i-th cell of the shard subtree at ``cell_order`` in
    ascending packed-word (Z-)order — equivalently the base-4 value of the
    cell's D1 digit tail with digits ``1..4`` mapped to ``0..3``. Raises
    ``ValueError`` for a cell outside the subtree or not at ``cell_order``
    (the bitmap is exact-order by construction; there is nothing conservative
    to fall back to).
    """
    from zagg.grids.morton import to_morton_array

    depth = int(cell_order) - _decimal_order(shard)
    ranks = np.empty(len(cells), dtype=np.int64)
    for i, dec in enumerate(to_morton_array(cells).decimal_repr()):
        tail = dec[len(shard) :]
        if not dec.startswith(shard) or len(tail) != depth:
            raise ValueError(
                f"cell {dec} is not an order-{cell_order} cell of shard {shard}; "
                f"the coverage bitmap encodes exact cell-order occupancy only"
            )
        rank = 0
        for ch in tail:
            rank = rank * 4 + (int(ch) - 1)
        ranks[i] = rank
    return ranks


def encode_coverage_bitmap(shard_key, occupied, cell_order: int) -> bytes:
    """zstd-compressed exact occupancy bitmap for one shard (issue #200 phase 2).

    The O8-resolved leaf encoding: a bit field over the shard subtree at
    ``cell_order`` — ``4^(cell_order - shard_order)`` bits, bit ``i`` per the
    :func:`_cell_ranks` convention (ascending packed-word order; base-4 digit
    tail), packed MSB-first within each byte (``np.packbits``), zstd-
    compressed at a fixed level. Raw size is deterministic
    (``ceil(4^depth / 8)`` bytes) regardless of fragmentation — the property
    that beat coarsen-to-fit ranges in the #202 item (6) measurement; the
    bit-order convention freezes with the mortie-side spec. zstd rides
    numcodecs, already in the tree via zarr's codec stack — no new
    dependency.
    """
    from numcodecs import Zstd

    from zagg.grids.morton import morton_decimal

    shard = morton_decimal(shard_key)
    depth = int(cell_order) - _decimal_order(shard)
    if depth <= 0:
        raise ValueError(f"cell_order {cell_order} is not below shard {shard}'s order")
    # Staging is one uint8 per BIT — 8x the raw bitmap (1 MB at the design
    # point: order-9 shards, order-19 cells). It is bounded by the shard's
    # cell count, which the worker already materializes for the leaf
    # template, so no extra guard here; coarse-shard + deep-cell configs
    # beyond that envelope are out of scope (review note, PR #208 round 2).
    bits = np.zeros(4**depth, dtype=np.uint8)
    bits[_cell_ranks(shard, occupied, cell_order)] = 1
    return bytes(Zstd(level=_ZSTD_LEVEL).encode(np.packbits(bits).tobytes()))


def decode_coverage_bitmap(payload: bytes, shard_key, cell_order: int) -> np.ndarray:
    """Occupied cell words from a sidecar bitmap payload (issue #200 phase 2).

    The inverse of :func:`encode_coverage_bitmap`: returns the sorted packed
    ``uint64`` cell words at ``cell_order`` whose bits are set — exact
    occupancy, no over-coverage. Posture (review finding, PR #208 round 2):
    a CORRUPT payload — zstd garbage, or a decompressed size that is not the
    exact raw bitmap size for the depth — raises loudly rather than
    zero-padding/truncating to a plausible partial cell set (a false
    negative, the one thing D9 forbids; the exact truth is intact in the
    leaf, so surfacing beats under-reporting). A MISSING sidecar degrades to
    ``None`` in :func:`read_coverage_bitmap`.
    """
    from numcodecs import Zstd

    from zagg.grids.morton import morton_decimal, morton_words_from_decimals

    shard = morton_decimal(shard_key)
    depth = int(cell_order) - _decimal_order(shard)
    raw = np.frombuffer(bytes(Zstd().decode(payload)), dtype=np.uint8)
    expected = -(-(4**depth) // 8)
    if raw.size != expected:
        raise ValueError(
            f"coverage sidecar decompressed to {raw.size} B; an order-{cell_order} bitmap "
            f"for shard {shard} is exactly {expected} B — refusing to zero-pad or truncate "
            f"(a partial cell set would be a false negative)"
        )
    bits = np.unpackbits(raw, count=4**depth)
    decimals = [shard + _rank_tail(int(rank), depth) for rank in np.flatnonzero(bits)]
    return np.sort(morton_words_from_decimals(decimals))


def write_coverage_sidecar(leaf_root: str, payload: bytes, **store_kwargs) -> None:
    """PUT the occupancy bitmap sidecar into a leaf (issue #200 phase 2).

    One object at ``{leaf}/coverage.moc`` — the recorded exception to the
    vanilla-v3 leaf, ignored by zarr readers (member enumeration warns and
    skips it; data reads are unaffected). Written BEFORE the commit
    stamp so the stamp stays the leaf's FINAL write (D4): in an unstamped
    prefix the sidecar is debris like everything else, and the wholesale
    retry re-template clears it.
    """

    put_object(open_object_store(leaf_root, **store_kwargs), COVERAGE_SIDECAR, payload)


def read_coverage_bitmap(
    leaf_root: str, *, coverage: dict | None = None, **store_kwargs
) -> np.ndarray | None:
    """A leaf's exact occupied cell words from its sidecar, or ``None``.

    Gates on the committed stamp's envelope (:func:`read_coverage`): no
    stamp, a box-only phase-1 payload (no ``encoding``/``sidecar`` keys), an
    unknown encoding, or a missing sidecar object all read ``None`` — the
    box is then the only index and readers degrade per D9, never to wrong
    answers. An ``encoding: "full"`` envelope (issue #246, D14) also reads
    ``None`` here — there IS no sidecar; the shard id itself is the exact
    MOC and :func:`zagg.coverage.bitmap_and` short-circuits on it. Pass an
    already-read ``coverage`` envelope to skip the stamp GET. A PRESENT-but-corrupt sidecar raises instead (see
    :func:`decode_coverage_bitmap` — degrading a corrupt payload would be
    indistinguishable from healthy box-only coverage). The shard id comes
    from the leaf basename — ``{full_id}.zarr``, or the windowed
    ``{full_id}_{window}.zarr`` (issue #246) — via the frozen first-``_``
    split; ``cell_order`` from the envelope. One GET, paid only by readers
    that want cell-level filtering.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.grids.morton import morton_word
    from zagg.store import open_store

    if coverage is None:
        coverage = read_coverage(open_store(leaf_root, **store_kwargs))
    if not coverage or coverage.get("encoding") != "bitmap" or not coverage.get("sidecar"):
        return None
    # Windowed leaves (issue #246) carry `{full_id}_{window}.zarr` basenames;
    # the shard id is the part before the first `_` (the frozen parse rule).
    shard = morton_word(split_leaf_name(leaf_root.rstrip("/").rsplit("/", 1)[-1])[0])
    store = open_object_store(leaf_root, **store_kwargs)
    try:
        data = obstore.get(store, str(coverage["sidecar"])).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return decode_coverage_bitmap(bytes(data), shard, int(coverage["cell_order"]))


def stamp_commit(
    leaf_store,
    *,
    cells_with_data: int,
    granule_count: int,
    coverage: dict | None = None,
    window: str | None = None,
    time_range: tuple | list | None = None,
    run_id: str | None = None,
) -> None:
    """Stamp a shard leaf complete — the shard's FINAL write (D4).

    One small PUT rewriting the leaf's root ``zarr.json`` (which the template
    already created), not consolidation. Until this lands, the leaf prefix is
    debris: a worker that dies mid-shard leaves no stamp, and a retry may
    overwrite the prefix wholesale. ``coverage`` (issue #200) attaches the
    tier-0 payload from :func:`build_coverage`; ``None`` writes the
    pre-coverage stamp unchanged.

    ``window``/``time_range`` (issue #246, D15): a windowed leaf's stamp is
    the TRUTH half of the temporal split — the window label plus the actual
    ``[t_min, t_max]`` written, as ISO-8601 UTC strings (ratified #246 Q2;
    the manifest keeps only the static schedule). A windowed stamp declares
    ``spec: "morton-hive/2"``; unwindowed stamps stay ``/1`` byte-identical.
    ``time_range`` without ``window`` is rejected (no unwindowed extent claim).

    ``run_id`` (issue #384, additive): STAGE-written artifacts stamp the
    sweep run that wrote them, the backstop the admission lease ruling
    requires — a skip-if-current read that sees a foreign FRESH stamp aborts
    loudly. Fleet-written leaves and columns never carry it; readers treat
    absence as "not a stage artifact", never as an error.
    """
    if window is None and time_range is not None:
        raise ValueError(
            "time_range rides windowed stamps only (D15: unwindowed leaves make "
            "no extent claim in the stamp); pass window= as well"
        )
    group = zarr.open_group(leaf_store, path="", mode="r+", zarr_format=3)
    stamp: dict = {
        "spec": HIVE_SPEC if window is None else HIVE_SPEC_V2,
        "complete": True,
        "cells_with_data": int(cells_with_data),
        "granule_count": int(granule_count),
        "written_at": _utcnow(),
    }
    if window is not None:
        stamp["window"] = str(window)
        if time_range is not None:
            # The stamp is the D15 TRUTH half — it fails CLOSED on a bad range
            # (unlike the fail-open cache union). Validate a 2-sequence of
            # parseable UTC instants with t_min <= t_max before it becomes
            # durable truth; the production path already builds this via
            # windows.iso_time_range (worker min/max, ordered), so this guards
            # direct callers. (review finding, PR #248)
            from zagg.windows import parse_utc

            try:
                lo, hi = time_range
                lo_dt, hi_dt = parse_utc(lo), parse_utc(hi)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"time_range must be a 2-sequence of parseable UTC instants "
                    f"[t_min, t_max]; got {time_range!r} ({e})"
                ) from e
            if lo_dt > hi_dt:
                raise ValueError(
                    f"time_range is reversed: t_min {lo!r} follows t_max {hi!r} "
                    f"(the D15 stamp is truth and must fail closed on a bad range)"
                )
            stamp["time_range"] = [str(t) for t in time_range]
    if coverage is not None:
        stamp["coverage"] = coverage
    if run_id is not None:
        stamp["run_id"] = str(run_id)
    group.attrs[COMMIT_ATTR] = stamp


def read_commit(leaf_store) -> dict | None:
    """The leaf's commit stamp, or ``None`` for debris / absent leaves (D4).

    Absence (no root group at all) and an unstamped root are the same answer:
    the shard is not complete. Presence requires the stamp — never infer
    completeness from the ``.zarr/`` prefix existing.
    """
    try:
        group = zarr.open_group(leaf_store, path="", mode="r", zarr_format=3)
    except (FileNotFoundError, GroupNotFoundError):
        return None
    stamp = group.attrs.get(COMMIT_ATTR)
    # A malformed (non-mapping) stamp is debris too — never half-trusted.
    return dict(stamp) if isinstance(stamp, dict) else None


def read_coverage(leaf_store) -> dict | None:
    """The leaf's tier-0 coverage payload, or ``None`` when absent (issue #200).

    Rides :func:`read_commit`: debris and absent leaves read ``None``, and so
    does a committed pre-coverage stamp (issue #199 stores carry no
    ``coverage`` key) — older stores keep reading fine. STRICT on the spec
    (review finding, PR #208): only ``spec == "morton-moc/1"`` payloads are
    returned; a malformed dict or an unknown/future spec reads as absent
    rather than half-parsed, so a new envelope version must be adopted here
    deliberately instead of leaking through to box consumers. Box members are
    decimal morton strings; parse one back with
    :func:`zagg.grids.morton.morton_word`.
    """
    stamp = read_commit(leaf_store)
    if stamp is None:
        return None
    coverage = stamp.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("spec") != COVERAGE_SPEC:
        return None
    return dict(coverage)


def _decimal_base(decimal: str) -> str:
    """The ``{sign+base}`` component of a D1 decimal id."""
    return decimal[:2] if decimal.startswith("-") else decimal[:1]


def _decimal_rank(decimal: str) -> int:
    """Base-4 value of a D1 digit tail (digits ``1..4`` -> ``0..3``)."""
    rank = 0
    for ch in decimal[len(_decimal_base(decimal)) :]:
        rank = rank * 4 + (int(ch) - 1)
    return rank


def _rank_tail(rank: int, depth: int) -> str:
    """Inverse of :func:`_decimal_rank`: the width-``depth`` digit tail."""
    digits = []
    for _ in range(depth):
        digits.append(str(rank % 4 + 1))
        rank //= 4
    return "".join(reversed(digits))


def build_root_coverage(
    shard_keys,
    order: int,
    *,
    source: str = "dispatcher",
    time_range: tuple | list | None = None,
    temporal: dict | None = None,
) -> dict:
    """Store-root coverage envelope from completed shard keys (issue #200 phase 3).

    The O1 serialization: JSON ranges under the ``morton-moc/1`` envelope,
    with ``encoding: "ranges"`` (vs the leaf sidecar's ``"bitmap"``), the
    shard ``order``, ``source`` and ``generated_at`` — the root carrier's
    staleness discriminators (per-carrier fields under the same spec; the
    leaf payload deliberately omits them, see :func:`build_coverage`). A
    range is an inclusive ``[first, last]`` run of same-order cells within
    ONE base cell, consecutive in base-4 digit-tail rank (ascending
    packed-word order — the bitmap's rank convention at the root). Endpoints
    are D1 decimal STRINGS: packed u64 words exceed 2^53, so raw JSON
    numbers would be silently mangled by any float-based parser (O1).

    ``time_range`` (issue #246, D15): the root summary optionally carries the
    ``[min, max]`` ISO-8601 UTC union of the run's leaf-stamp time ranges —
    CACHE, never truth (the per-leaf stamps are the truth; the walk and the
    sweep regenerate this). Omitted for unwindowed stores, keeping their root
    object byte-identical to pre-#246 runs.

    ``temporal`` (issue #480): the spec §10 ``zagg-coverage-toc/1`` section —
    the per-shard toc envelope word map plus the optional root time-digest,
    built by :func:`zagg.coverage_toc.build_temporal_section`. ``None`` for a
    store with no temporal channel, which keeps ITS root object byte-identical
    to a pre-#480 one; absence of the section is never a refusal.
    """
    from zagg.grids.morton import to_morton_array

    words = np.unique(np.asarray(shard_keys, dtype=np.uint64))
    if words.size == 0:
        raise ValueError("build_root_coverage requires at least one shard key")
    decs = list(to_morton_array(words).decimal_repr())
    bad = [d for d in decs if _decimal_order(d) != int(order)]
    if bad:
        raise ValueError(f"shard keys {bad[:3]} are not at shard order {order}")
    # np.unique sorts by packed word; at a fixed order the words of one base
    # cell are contiguous and rank-ascending, so one linear pass finds runs.
    ranges = []
    start = prev = decs[0]
    for dec in decs[1:]:
        same_run = (
            _decimal_base(dec) == _decimal_base(prev)
            and _decimal_rank(dec) == _decimal_rank(prev) + 1
        )
        if same_run:
            prev = dec
            continue
        ranges.append([start, prev])
        start = prev = dec
    ranges.append([start, prev])
    envelope = {
        "spec": COVERAGE_SPEC,
        "encoding": "ranges",
        "order": int(order),
        "source": source,
        "generated_at": _utcnow(),
        "ranges": ranges,
    }
    if time_range is not None:
        envelope["time_range"] = [str(t) for t in time_range]
    if temporal is not None:
        from zagg.coverage_toc import TEMPORAL_KEY

        envelope[TEMPORAL_KEY] = temporal
    return envelope


def root_coverage_words(envelope: dict) -> np.ndarray:
    """Shard words from a root envelope's ranges (inverse of the builder).

    Raises ``ValueError`` on malformed ranges (base-crossing, wrong order,
    reversed endpoints) — same loud posture as the bitmap decoder: a corrupt
    cache must never yield a plausible partial answer.

    Scale note (review, PR #208 round 3): expansion is O(covered shards) in
    a Python loop — milliseconds at coherent-run scale (the design point,
    shard order <= 11 regional products), but a full-sphere accumulated root
    (~3M order-9 / ~50M order-11 shards) would take minutes worker-side. An
    interval-space union on ``[base, lo_rank, hi_rank]`` triples (O(ranges),
    no word materialization) is the upgrade path if root objects ever reach
    continental-accumulation scale; out of scope here.
    """
    from zagg.grids.morton import morton_words_from_decimals

    order = int(envelope["order"])
    decimals = []
    for lo, hi in envelope["ranges"]:
        base = _decimal_base(lo)
        lo_rank, hi_rank = _decimal_rank(lo), _decimal_rank(hi)
        ok = _decimal_base(hi) == base and lo_rank <= hi_rank
        ok = ok and _decimal_order(lo) == order and _decimal_order(hi) == order
        if not ok:
            raise ValueError(f"malformed coverage range [{lo}, {hi}] at order {order}")
        decimals.extend(base + _rank_tail(r, order) for r in range(lo_rank, hi_rank + 1))
    return np.unique(morton_words_from_decimals(decimals))


def write_root_coverage(store_root: str, envelope: dict, **store_kwargs) -> dict:
    """GET-union-PUT the store-root ``coverage.moc`` (issue #200 phase 3).

    Incremental runs accumulate: a parsable existing object with the same
    spec/encoding/order is UNIONED with ``envelope`` before the PUT. An
    unparsable or incompatible existing object is logged and OVERWRITTEN —
    the root MOC is a regenerable cache (D9): the leaf stamps are the
    durable truth and the §7 sweep is the authoritative rebuilder, so
    merging with garbage would be worse than replacing it. CONCURRENT runs
    race benignly (review finding, PR #208 round 3): GET-union-PUT is not
    atomic and S3 has no compare-and-swap, so the last writer wins and its
    union may miss the loser's shards until the sweep or the next run
    re-unions — accepted under D9/O7 (a missing listing degrades to "reader
    doesn't see the newest run", never a wrong answer; do NOT add a lock).
    Returns the payload actually written.

    The spec §10 temporal section (issue #480) composes across the same seam
    (:func:`zagg.coverage_toc.merge_temporal_sections`): its per-shard word
    map unions elementwise under the grammar's join, its digest is replaced
    rather than unioned (weights are counts), and a producer carrying no
    section leaves an existing one standing. Two stores with no temporal
    channel at all still write byte-identical bytes to a pre-#480 zagg.
    """

    store = open_object_store(store_root, **store_kwargs)
    try:
        existing = _read_json(store, ROOT_COVERAGE_NAME)
    except ValueError:
        logger.warning(
            f"existing {ROOT_COVERAGE_NAME} at {store_root} is not JSON; overwriting "
            f"(regenerable cache — the sweep is the authoritative rebuilder)"
        )
        existing = None
    merged = envelope
    if isinstance(existing, dict):
        compatible = (
            existing.get("spec") == envelope.get("spec")
            and existing.get("encoding") == envelope.get("encoding")
            and existing.get("order") == envelope.get("order")
        )
        if compatible:
            try:
                union = np.union1d(root_coverage_words(existing), root_coverage_words(envelope))
                merged = build_root_coverage(
                    union,
                    int(envelope["order"]),
                    source=envelope.get("source", "dispatcher"),
                    # D15: incremental runs accumulate the time union too —
                    # cache semantics identical to the spatial ranges.
                    time_range=union_time_range(
                        existing.get("time_range"), envelope.get("time_range")
                    ),
                )
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"existing {ROOT_COVERAGE_NAME} at {store_root} failed to parse ({e}); "
                    f"overwriting (regenerable cache — the sweep rebuilds authoritatively)"
                )
        else:
            logger.warning(
                f"existing {ROOT_COVERAGE_NAME} at {store_root} has an incompatible "
                f"envelope; overwriting (regenerable cache)"
            )
    from zagg.coverage_toc import TEMPORAL_KEY, merge_temporal_sections

    # A rebuilt `merged` dropped the existing carrier's extra keys with it;
    # an OVERWRITE (`merged is envelope`) deliberately discards the stale
    # section too, exactly as it discards the stale ranges.
    carried = isinstance(existing, dict) and merged is not envelope
    section = merge_temporal_sections(
        existing.get(TEMPORAL_KEY) if carried else None, envelope.get(TEMPORAL_KEY)
    )
    if section is not None:
        merged[TEMPORAL_KEY] = section
    put_object(store, ROOT_COVERAGE_NAME, json.dumps(merged, indent=1).encode())
    return merged


def read_root_coverage(store_root: str, **store_kwargs) -> dict | None:
    """Read the store-root ``coverage.moc``; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), ROOT_COVERAGE_NAME)


def leaf_block_index(grid, block_index, shard_key) -> tuple:
    """Leaf-LOCAL storage block for a chunk in a hive leaf (issue #199 phase 2).

    The hive leaf's arrays are sized to one shard, so a chunk's block index is
    its position WITHIN the shard, not the global block ``iter_chunks`` yields.
    Derived from the existing ``shard_local_region`` seam (the sharded path's
    within-shard placement): the region's start divided by the chunk extent.
    At K==1 this is always ``(0,)``.
    """
    region = grid.shard_local_region(block_index, shard_key)
    return tuple(int(s.start) // int(c) for s, c in zip(region, grid.chunk_shape))


def _leaf_is_committed(leaf_path, store_kwargs, shard_key) -> bool:
    """Is a STAMPED leaf present at ``leaf_path``? (issue #388 skip precondition)

    The D20 stats sidecar is a SIBLING of the leaf ``.zarr``
    (:func:`zagg.telemetry.sidecar_path`), so the two diverge, and the
    diverged state is one this module already documents as reachable: a torn
    write clears the leaf and dies (``process_and_write_hive._leaf``'s
    clear-then-die note — the dispatcher writes no sidecar for a cell that
    raised, so the PRIOR run's record survives intact), and a lifecycle rule
    scoped to the leaf prefix reaps the tree while the JSON stays. Identity
    alone would then certify an absent leaf as ``current`` permanently —
    nothing rewrites the sidecar, so every later rerun skips too.
    :func:`zagg.dedup.shard_status`, the sibling identity check, already
    treats the stamp as the precondition it is (``read_commit(...) is None ->
    miss``); the gate follows it. ``leaf_path`` is the per-``(shard, window)``
    leaf on both seams, so the windowed stamp is the one checked. Fail-open
    toward RECOMPUTE: an unreadable store reads as uncommitted.
    """
    from zagg.store import open_store

    try:
        return read_commit(open_store(leaf_path, **store_kwargs)) is not None
    except Exception as e:
        logger.warning(
            f"skip-if-current: leaf stamp unreadable for shard {shard_key} "
            f"(rewriting, issue #388): {e}"
        )
        return False


def leaf_column_expectation(store_root, shard_key, grid, config, window):
    """This unit's issue #383 column artifact: ``(path, declared)`` (issue #388).

    The identity PAIR does not cover the leaf's ARTIFACT SET. The whole
    ``output`` block is outside :func:`zagg.semantics.semantic_core`, and
    ``pyramid`` is deliberately not a frozen manifest key (D11 — the §7 sweep
    populates it), so turning the leaf column on (or off) over an existing
    store moves neither identity half and every unit would read ``equal``
    while the column was never written. The gate closes that by verifying the
    artifact the way ``docs/specification.md`` §4.6 rules: a column exists
    exactly when the run declares one (:func:`zagg.column.leaf_column_plan`,
    the same gate the write site runs), so a disagreement between the
    declaration and what is COMMITTED beside the leaf is not current, whatever
    the record says. ``declared`` is ``None`` when the declaration cannot be
    resolved at all — also not skippable, so the rewrite raises
    ``write_leaf_column``'s named refusal instead of silently no-oping.

    Scope: this covers the ONE output knob that adds a leaf artifact today.
    Other ``output`` keys that change what a leaf CONTAINS rather than which
    objects sit beside it (``sharded`` is the reviewed example) are still
    outside the recorded identity — see the PR thread's standing question.
    """
    from zagg.column import column_name, leaf_column_plan

    label = window["label"] if window else None
    leaf = shard_leaf_path(store_root, shard_key, window=label)
    node_prefix = leaf.rstrip("/").rsplit("/", 1)[0]
    try:
        declared = leaf_column_plan(config, grid) is not None
    except Exception as e:
        logger.warning(
            f"skip-if-current: column declaration unresolved for shard {shard_key} "
            f"(rewriting, issue #388): {e}"
        )
        declared = None
    return f"{node_prefix}/{column_name(label)}", declared


def leaf_identity_gate(
    leaf_path,
    planned_ids,
    *,
    semantic_hash,
    allow_contraction,
    sidecar_spec,
    store_kwargs,
    shard_key,
    window=None,
    column_path=None,
    column_declared=None,
):
    """Skip-if-current / contraction-guard verdict for one leaf unit (issue #388).

    The shared pre-fold gate for BOTH leaf seams (:func:`process_and_write_hive`
    and ``zagg.processing.raster.process_and_write_raster_hive``): read the
    leaf's D20 stats sidecar and classify the planned ``(semantic_hash,
    granule-id set)`` identity pair against it
    (:func:`zagg.dedup.classify_leaf_identity`). Returns ``(identity, meta)``:

    - ``identity`` — the classification dict (``action`` / ``classification``
      / ``missing``). The caller stamps ``classification`` on its returned
      metadata as ``metadata["identity"]`` so run stats can count
      ``unrecorded-ids`` rewrites apart from ordinary ones.
    - ``meta`` — an early-return unit metadata dict when the unit must NOT
      fold: ``{"current": True}`` on an identity match, ``{"refused": True,
      "missing_granules": [...]}`` on a contraction without
      ``allow_contraction``. ``None`` means proceed with the wholesale D4
      rewrite exactly as today — including a contraction explicitly allowed
      by the flag (a normal update; the classification still rides).

    A skipped or refused unit writes NOTHING (no arrays, no stamp, no
    sidecar, no sub-map, no column): the caller returns ``meta`` untouched,
    so zero sweep dirtiness by construction. The sidecar read is fail-open —
    an unreadable, unparseable, or non-object sidecar degrades to today's
    rewrite (the ``no-sidecar`` classification), never blocks the unit.

    A matched identity is NOT sufficient to skip: the unit's ARTIFACTS must
    be current too, verified by reading them rather than the record.

    - the leaf's own D4 commit stamp is a precondition
      (:func:`_leaf_is_committed`), because the sidecar outlives the leaf
      under a torn write or a prefix-scoped lifecycle purge. A record that
      says ``equal`` over an absent/unstamped leaf classifies
      ``unstamped-leaf`` and rewrites.
    - ``column_path``/``column_declared`` (from
      :func:`leaf_column_expectation`; ``None`` on the raster seam, which
      writes no column) pin the issue #383 leaf column, whose declaration
      moves NEITHER identity half. A committed column where none is declared,
      or a declaration with no committed column, classifies ``column-drift``
      and rewrites.

    Both checks run ONLY on the skip arm — a refusal writes nothing either
    way, and a contraction over debris is still a contraction the operator
    should be told about.
    """
    from zagg.dedup import classify_leaf_identity, leaf_recorded_ids
    from zagg.telemetry import read_sidecar

    t0 = time.time()
    try:
        recorded = read_sidecar(leaf_path, spec=sidecar_spec, **store_kwargs)
    except Exception as e:
        logger.warning(
            f"skip-if-current: sidecar read failed for shard {shard_key} "
            f"(degrading to rewrite, issue #388): {e}"
        )
        recorded = None
    if recorded is not None and not isinstance(recorded, dict):
        # ``read_sidecar`` returns whatever the JSON decoded to, and the
        # classifier dereferences it with ``.get`` — a valid non-object body
        # (``[]``, ``"x"``, ``5``) would raise straight out of the seam and
        # FAIL the unit, which is exactly what fail-open promises not to do.
        logger.warning(
            f"skip-if-current: sidecar for shard {shard_key} is not a JSON object "
            f"({type(recorded).__name__}) — degrading to rewrite (issue #388)"
        )
        recorded = None
    identity = classify_leaf_identity(
        recorded,
        semantic_hash=semantic_hash,
        planned_ids=planned_ids,
        # LAZY by construction (issue #388's ruling on question (6)): the
        # recorded id list is a sibling object, GET only when the
        # granules_sha256 fast path failed and the diff has to be named.
        load_recorded_ids=lambda: leaf_recorded_ids(
            leaf_path, recorded, spec=sidecar_spec, store_kwargs=store_kwargs
        ),
    )
    if identity["action"] == "skip":
        drift = None
        if not _leaf_is_committed(leaf_path, store_kwargs, shard_key):
            drift = "unstamped-leaf"
        elif column_path is not None and column_declared is not _leaf_is_committed(
            column_path, store_kwargs, shard_key
        ):
            drift = "column-drift"
        if drift is not None:
            logger.warning(
                f"shard {shard_key}: sidecar records a matching identity but the unit's "
                f"artifacts are not current ({drift}) — rewriting (issue #388)"
            )
            identity = {"action": "rewrite", "classification": drift, "missing": []}
    base = {
        "shard_key": int(shard_key),
        # The unit is (shard, WINDOW): a refusal manifest that named only the
        # shard would be ambiguous on a windowed store, where the same shard
        # holds one leaf per window (issue #388's ruling on question (9)).
        "window": window,
        "identity": identity["classification"],
        "semantic_hash": semantic_hash,
        "cells_with_data": 0,
        "total_obs": 0,
        "granule_count": len(planned_ids) if planned_ids is not None else 0,
        "duration_s": time.time() - t0,
        "error": None,
    }
    if identity["action"] == "skip":
        logger.info(f"shard {shard_key}: current (identity match, issue #388) — fold skipped")
        return identity, {**base, "current": True}
    if identity["action"] == "refuse":
        missing = identity["missing"]
        if allow_contraction:
            logger.info(
                f"shard {shard_key}: contraction allowed by flag — rewriting "
                f"({len(missing)} recorded granule id(s) dropped)"
            )
            return identity, None
        shown = ", ".join(missing[:5]) + (f", +{len(missing) - 5} more" if len(missing) > 5 else "")
        logger.warning(
            f"shard {shard_key}: REFUSED — planned inputs drop {len(missing)} recorded "
            f"granule id(s) ({shown}); pass allow_contraction to rewrite (issue #388)"
        )
        return identity, {**base, "refused": True, "missing_granules": missing}
    return identity, None


def process_and_write_hive(
    shard_key,
    granule_urls,
    grid,
    s3_creds,
    store_root,
    config,
    *,
    store_kwargs,
    driver=None,
    handoff="arrow",
    aoi_payload=None,
    profile=False,
    window=None,
    skip_if_current=False,
    allow_contraction=False,
    semantic_hash=None,
    sidecar_spec=None,
):
    """Process one shard into its own hive leaf store (issue #199 phase 2).

    The SHARED per-shard write path for both backends (phase 3): the local
    runner's ``_cell_work`` and the Lambda handler's hive branch both call
    this, so leaf templating, chunk placement, ragged layout, and stamp
    ordering cannot drift between dispatchers. The shard's output is a
    self-describing leaf zarr at :func:`shard_leaf_path` ``(store_root,
    shard_key)`` (D3), with dense chunks written at leaf-LOCAL block indices
    and — as the shard's FINAL write — the D4 commit stamp on the leaf's root
    group. The leaf template is emitted lazily on the first chunk write
    (mirroring the Lambda handler's lazy store open), so a no-data shard never
    creates the ``.zarr/`` prefix; a worker that dies mid-shard leaves an
    UNSTAMPED prefix — debris, overwritten wholesale on retry
    (``overwrite=True`` on the leaf template makes the retry idempotent).
    When ``grid.sharded`` (issue #236) the dense chunks are not streamed:
    the K carriers accumulate and the whole leaf is written once
    (``write_leaf_to_zarr`` — one ShardingCodec object per array), mirroring
    the flat sharded switch in ``runner._process_and_write``.
    Phase timings are always collected (issue #297; formerly the opt-in
    ``profile`` gate of issues #100/#249): ``process_shard`` fills
    ``metadata["phase_timings"]`` with read/index/aggregate, and the leaf
    write work — interleaved with the stream (or a single post-stream pass
    when sharded), plus the ragged/coverage/stamp finalize — accumulates into
    an additive ``write`` phase alongside them. Unlike the flat path, the
    lazy leaf template emission counts as write: in hive the worker owns its
    leaf's template PUTs (there is no dispatcher-side template), so excluding
    them would hide real write-out cost. ``profile`` is retained and
    forwarded (it still gates dispatcher-side rollup verbosity). On success
    the O11 content hashes (issue #342, spec §5) are computed from the staged
    arrays, returned as ``metadata["content_hashes"]`` for the caller's D20
    sidecar, and timed as the ``hash`` phase.

    ``window`` (issue #246, D13/D15) is one dispatch unit's time window:
    ``{"label", "start", "end"}``, bounds half-open in DATASET units
    (converted once at dispatch). It selects the windowed leaf name, injects
    the observation-level ``time_field`` filter (the temporal analog of
    ``aoi_mask`` — see :func:`zagg.config.windowed_cell_config`), stamps the
    window label + the ACTUAL written ISO-UTC time range (also returned as
    ``metadata["time_range"]`` for the root-summary union), and arms the D14
    popcount (``encoding: "full"``). ``None`` is byte-identical to
    pre-windowing behavior.

    ``skip_if_current`` (issue #388) arms the worker-side leaf identity gate
    (:func:`leaf_identity_gate`) BEFORE any fold: a unit whose planned
    ``(semantic_hash, granule-id set)`` pair matches the leaf's recorded D20
    sidecar returns a ``{"current": True}`` metadata dict and writes nothing;
    a contraction (``recorded ∖ planned ≠ ∅``, the ruled predicate) returns
    ``{"refused": True, "missing_granules": [...]}`` unless
    ``allow_contraction`` rides the call. Default ``False`` keeps the GATE
    inert for callers that have not opted in (the Lambda handler); the
    granule-id sibling below is written either way, because it is what a
    LATER run's guard diffs against — a leaf must record its input set
    whether or not the run that wrote it had the gate armed.
    ``semantic_hash`` is the RUN config's D19 digest when the caller holds it
    (the local runner); ``None`` falls back to hashing this worker's own
    ``config`` — the same digest except under the per-cell
    ``granule_workers`` clamp (``runner._clamped_data_source``), where the
    drift is one-sided: it can only miss a skip (degrading to today's
    rewrite), never fake one. Either way the resolved hash is stamped into
    ``metadata["semantic_hash"]`` so a caller that never resolved it (the
    Lambda handler) still records the identity half via
    ``telemetry.build_record``'s validated metadata fallback.
    ``sidecar_spec`` is the manifest spec in effect, keying the sidecar name
    (``telemetry.sidecar_key``); the gate reads with it and degrades to
    rewrite when the sidecar is absent under that name. It also keys the
    granule-id SIBLING this seam writes after the commit stamp
    (``telemetry.write_granule_ids``, issue #388) — the recorded id set the
    contraction guard later diffs, kept out of the sidecar and the response
    envelope so an identity check stays one small GET.
    """
    from zagg.processing import (
        process_shard,
        write_dataframe_to_zarr,
        write_leaf_to_zarr,
        write_ragged_leaf_to_zarr,
    )
    from zagg.store import open_store

    # D19 identity half (issue #388): resolve BEFORE the window filter
    # injection below — recorded sidecars carry the RUN config's hash, and the
    # per-unit windowed config copy must not perturb the comparison.
    if semantic_hash is None:
        try:
            from zagg.semantics import semantic_hash as _semantic_hash

            semantic_hash = _semantic_hash(config)
        except Exception as e:
            logger.warning(f"semantic hash unavailable (fail-open, issue #388): {e}")

    windowing = None
    time_range_of = None
    if window is not None:
        from zagg.config import windowed_cell_config

        # Inject the window's observation filter into a per-unit config copy
        # (the issue #43 machinery — see windowed_cell_config).
        config, windowing = windowed_cell_config(config, window)
        time_range_of = windowing["time_field"]

    leaf_path = shard_leaf_path(store_root, shard_key, window=window["label"] if window else None)

    # Leaf identity gate (issue #388): per (shard, window) unit, before any
    # read or fold. A skipped/refused unit returns here having written NOTHING.
    identity = None
    if skip_if_current:
        # The issue #383 column rides this seam AFTER the gate, and its
        # declaration moves neither identity half — so the gate verifies the
        # artifact itself (leaf_column_expectation).
        from zagg.telemetry import canonical_granule_ids

        column_path, column_declared = leaf_column_expectation(
            store_root, shard_key, grid, config, window
        )
        identity, unit_meta = leaf_identity_gate(
            leaf_path,
            # ONE canonical id space for both sides of the gate (espg-ruled
            # 2026-08-17): the driver-stripped bare id, with paired-asset
            # entries (issue #425) identifying by their primary.
            canonical_granule_ids(granule_urls),
            semantic_hash=semantic_hash,
            allow_contraction=allow_contraction,
            sidecar_spec=sidecar_spec,
            store_kwargs=store_kwargs,
            shard_key=shard_key,
            window=window["label"] if window else None,
            column_path=column_path,
            column_declared=column_declared,
        )
        if unit_meta is not None:
            if unit_meta.get("current"):
                # Lifecycle touch (issue #388 phase 3): a skip must still
                # reset the purge clock on the unit's whole footprint — leaf
                # tree, sidecar/sub-map siblings, and the declared column
                # (the gate already verified declaration and artifact agree,
                # so the touch never resurrects the column-drift ambiguity).
                # Fail-open both here and inside: a failed touch logs and
                # counts, never fails or un-skips the unit.
                from zagg.config import get_touch_policy
                from zagg.lifecycle import touch_current_unit

                try:
                    counts = touch_current_unit(
                        leaf_path,
                        column_path=column_path if column_declared else None,
                        sidecar_spec=sidecar_spec,
                        store_kwargs=store_kwargs,
                        policy=get_touch_policy(config),
                    )
                except Exception as e:
                    logger.warning(
                        f"lifecycle touch failed for shard {shard_key} (fail-open, issue #388): {e}"
                    )
                    counts = {"touched": 0, "failed": 1}
                unit_meta["touched_objects"] = counts["touched"]
                unit_meta["touch_failed"] = counts["failed"]
                # A published target is NOT APPLICABLE, not failed (issue #495
                # phase 4) — but the pair above records that as 0/0, which is
                # byte-identical to a touch that never ran. Recorded so the
                # run parquet and the .status objects say it out loud; absent
                # when zero, so every non-published record stays as it was.
                if counts.get("skipped_paths"):
                    unit_meta["touch_skipped_paths"] = counts["skipped_paths"]
            return unit_meta

    box: dict = {}
    _write_elapsed = 0.0

    def _leaf():
        if "store" not in box:
            store = open_store(leaf_path, **store_kwargs)
            # overwrite=True: any existing prefix here is either debris from a
            # torn run (D4) or a prior committed write being redone — both are
            # replaced wholesale; per-leaf state never blocks a retry. Since
            # issue #341 the template DELETES the leaf prefix up front, so the
            # wholesale claim is literal: retired members of a narrowed schema
            # (and the prior attempt's coverage sidecar) are gone before the
            # new template lands, and no enumeration ever walks stale/orphan
            # member dirs (the pre-#341 walk warned on the sidecar and could
            # die on an orphan array dir).
            #
            # This DOES widen the redundant-duplicate-writer window (fold
            # review), and the change is deliberate. ``dispatch._LAMBDA_RETRYABLE``
            # classifies off the exception string of the ``Invoke`` call itself,
            # and for ``InvocationType=Event`` a request Lambda accepted whose
            # HTTP response timed out is indistinguishable from one it never
            # got — so a retry can produce a second live worker for one shard.
            # Before: A wrote + stamped, B templated over the top, and if B died
            # the leaf still held A's objects under A's stamp (stale-but-complete,
            # certified). After: B's clear removes A's committed leaf first, so a
            # B that dies mid-write leaves the leaf EMPTY and unstamped. Nothing
            # is silently corrupt — the stamp is written last, so the leaf reads
            # as debris and is re-dispatchable (test_leaf_clear_under_a_live_
            # writer_leaves_debris_not_corruption) — but a redundant retry can now
            # destroy a leaf that had already succeeded, which write-over could
            # not. Refusing the clear on a valid stamp is the lever if that
            # trade stops being acceptable; it is not taken here because D4 makes
            # "replaced wholesale" the contract and a stamp must never block a
            # retry.
            grid.emit_shard_template(store, overwrite=True)
            box["store"] = store
        return box["store"]

    # Sharded leaf output (issue #236): the sharded leaf template bundles each
    # dense array's K inner chunks into ONE ShardingCodec object, so the
    # per-chunk streaming write below would read-modify-write that object K
    # times (the same failure the flat sharded path warns about in
    # ``runner._process_and_write``). Mirror the flat switch: accumulate the K
    # carriers via ``chunk_results`` and write the whole leaf once after the
    # stream (``write_leaf_to_zarr`` — dense + ragged, one object each). The
    # re-added O(shard) dense term is ~1.3 MB at production geometry
    # (parent 11 / child 19: 65,536 cells x ~20 B across the dense arrays) —
    # trivial next to the ragged accumulation below, which this path folds
    # into the same single-write pass.
    sharded = getattr(grid, "sharded", False)
    chunk_results: list | None = [] if sharded else None

    # O11 staged-array sink (issue #342): the leaf writers record each
    # assembled slab here (refs on the sharded path; the streaming path fills
    # a leaf-wide slab chunk by chunk) so the content hashes below run over
    # the exact in-memory values written — the ratified hash source. On the
    # streaming path this re-adds the same O(dense leaf) term the sharded
    # path's slab pass already accepts above (~1.3 MB at production geometry).
    staged: dict = {}

    # Ragged fields accumulate across the streamed chunks (leaf-LOCAL blocks)
    # and are written ONCE after the stream (issue #209): the leaf's ragged
    # vlen array is a single ShardingCodec object spanning the shard, so a
    # per-chunk write here would read-modify-write that object K times.
    # Memory bound (review, PR #211): this re-adds an O(shard-payload) term to
    # the otherwise O(chunk) streaming path (issue #91) — at the o8 t-digest
    # scale this fix exists to unlock (sparse NEON o8: 17.6 M centroids × 8 B
    # ≈ 141 MB of held payload; ~200 MB peak through the single write, once
    # per-cell ``bytes``-object overhead and the assembled ~60 MB shard object
    # are counted). Accepted deliberately: workers run 4 GB (issue #193), the
    # dense side keeps its O(chunk) stream-and-free bound, and the
    # accumulation is what deletes the ~K×7-object PUT storm that was ~1/3 of
    # shard wall at CONUS scale (issue #209). If 88S-scale shards or the #148
    # streaming budget ever say otherwise, the escape valve is spilling the
    # ragged field back to per-inner-chunk writes against a regular-chunked
    # vlen array (the unsharded flat layout) — named here, not built.
    # Sibling envelope (issue #383): when the /2 declaration carries leaf-node
    # levels, the column fold at the TAIL of this function k-way merges this
    # same resident digest load once more — measured ~+2.0 GB transient at the
    # o8 scale above, on top of this accumulation, since nothing here is
    # released before that call (``column.write_leaf_column``'s memory note).
    ragged_chunks: list = []

    def _write_chunk(block_index, carrier, ragged):
        nonlocal _write_elapsed
        _t0 = time.time()
        store = _leaf()
        local = leaf_block_index(grid, block_index, shard_key)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=local, staged_out=staged)
        if ragged:
            ragged_chunks.append((local, ragged))
        _write_elapsed += time.time() - _t0

    # Occupied-cell sink (issue #200): the worker already holds the shard's
    # populated cell words; collect them here to derive the stamp's coverage.
    occupied: list = []
    _df_out, metadata = process_shard(
        grid,
        int(shard_key),
        granule_urls,
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        handoff=handoff,
        aoi_payload=aoi_payload,
        chunk_results=chunk_results,
        write_chunk=None if sharded else _write_chunk,
        occupied_out=occupied,
        time_range_of=time_range_of,
        profile=profile,
    )
    # Windowed stamp truth (D15): convert the worker's dataset-unit extent to
    # ISO-8601 UTC once, here — the same strings feed the stamp below and the
    # dispatcher's root-summary union (via the returned metadata).
    time_range = None
    if window is not None and metadata.get("time_range") is not None:
        from zagg.windows import iso_time_range

        time_range = iso_time_range(metadata["time_range"], windowing)
        metadata["time_range"] = time_range
    # The seam stamps the identity half (issue #388): a caller that never
    # resolved the hash itself (the Lambda handler) still records it in the
    # leaf sidecar via ``telemetry.build_record``'s validated metadata
    # fallback. The identity classification rides so run stats can count
    # ``unrecorded-ids`` rewrites apart from ordinary ones.
    if semantic_hash is not None:
        metadata.setdefault("semantic_hash", semantic_hash)
    if identity is not None:
        metadata["identity"] = identity["classification"]
    # Sharded leaf: ONE whole-leaf write per array (dense + ragged together,
    # issue #236), after the stream. The leaf template is emitted here (still
    # lazily, via ``_leaf``), so a shard that produced no chunks never creates
    # the ``.zarr/`` prefix — same contract as the streaming path's first
    # ``_write_chunk``.
    if sharded and chunk_results and not metadata.get("error"):
        _t0 = time.time()
        write_leaf_to_zarr(
            chunk_results, _leaf(), grid=grid, shard_key=int(shard_key), staged_out=staged
        )
        _write_elapsed += time.time() - _t0
    # Stamp ONLY a fully-written leaf: an errored shard (or one that streamed
    # no chunks) stays unstamped — debris by definition (D4). The stamp is the
    # last write, so its presence certifies everything before it landed — the
    # box payload rides it (zero extra requests), the exact-occupancy bitmap
    # sidecar is PUT just before it (issue #200 phase 2), and both inherit
    # its debris semantics: a torn worker's coverage never becomes visible.
    # The leaf write order is pinned: dense (streamed, or one object each when
    # sharded) -> ragged (one object, issue #209) -> coverage sidecar -> stamp
    # -> granule-id sibling (issue #388; after the stamp, inside the bracket).
    if "store" in box and not metadata.get("error"):
        _t0 = time.time()
        if not sharded:
            write_ragged_leaf_to_zarr(ragged_chunks, box["store"], grid=grid, staged_out=staged)
        words = np.concatenate(occupied) if occupied else None
        if words is not None and words.size == 0:
            words = None
        bitmap = None
        # D14 popcount (issue #246): a fully-occupied subtree stamps
        # ``encoding: "full"``, no sidecar. Gated on windowing (/2 stores
        # only) so schedule-none output stays object-for-object identical to
        # pre-#246 runs (the mortie spec files "full" under /2).
        depth = int(grid.child_order) - int(grid.parent_order)
        full = window is not None and words is not None and np.unique(words).size == 4**depth
        # Depth 0 (child_order == parent_order, a legal one-cell-per-shard
        # config) skips the sidecar: a 1-bit bitmap says nothing the stamp
        # itself doesn't, and encode would raise AFTER the chunk writes,
        # leaving the shard permanently unstampable debris (review finding,
        # PR #208 round 2). The envelope simply omits the pointer — box only.
        if words is not None and not full and depth > 0:
            bitmap = encode_coverage_bitmap(shard_key, words, grid.child_order)
            write_coverage_sidecar(leaf_path, bitmap, **store_kwargs)
        stamp_commit(
            box["store"],
            cells_with_data=metadata.get("cells_with_data", 0),
            granule_count=metadata.get("granule_count", 0),
            coverage=build_coverage(shard_key, words, grid.child_order, bitmap=bitmap, full=full),
            window=window["label"] if window else None,
            time_range=time_range,
        )
        # The recorded granule-id list, as this leaf's own sibling object
        # (issue #388): AFTER the stamp, so it never certifies a leaf that
        # did not land, and written here rather than at the caller's sidecar
        # PUT because ``granule_urls`` is the very list the identity gate
        # compares — one source for the recorded id space, on both backends.
        # Fail-open inside (telemetry class, D9). Inside the write bracket:
        # it is a write this seam performs, so ``phase_timings["write"]``
        # must account for it.
        from zagg.telemetry import canonical_granule_ids, write_granule_ids

        write_granule_ids(
            leaf_path,
            # Same canonical identity as the gate above: the recorded and
            # planned id spaces must share one shape (``write_granule_ids``
            # canonicalizes too — this keeps the two call sites reading alike).
            canonical_granule_ids(granule_urls),
            spec=sidecar_spec,
            **store_kwargs,
        )
        _write_elapsed += time.time() - _t0
    # Write-phase split (issue #249): read/index/aggregate come from
    # ``process_shard``; ``write`` is the leaf write-out above (template +
    # dense chunks + ragged + coverage sidecar + stamp). Same gate as the flat
    # Lambda handler's issue #100 write bracket: only a clean, actually-written
    # shard carries it, so a time-to-failure never lands as a write duration
    # and a no-data shard (no leaf) stays write-less.
    if not metadata.get("error") and "phase_timings" in metadata and "store" in box:
        metadata["phase_timings"]["write"] = _write_elapsed
    if not metadata.get("error") and "store" in box:
        # O11 content hashes (issue #342, spec §5): computed in-worker at
        # write, from the STAGED arrays (the ratified source — the write path
        # already holds every slab, so this is a memory-bandwidth pass).
        # Dense and ragged arrays are staged on BOTH leaf paths: the sharded
        # one-object-per-array pass and the per-chunk streaming path
        # (``sharded`` is forced off whenever a leaf holds one inner chunk —
        # the ``chunk_inner``-unset default). ``resolution: chunk`` companions
        # are the one read-back fallback inside ``hash_arrays``: they are
        # written per chunk-block, never as a leaf slab.
        # Recorded on ``metadata`` for the caller's D20
        # sidecar (``telemetry.build_record``). Hive-only by ratified decision
        # (3): flat layouts have no leaf sidecar to record into, and no flat
        # writer computes hashes — those stores stay verifiable by running
        # the §5 recipe manually. Fail-open: the record is telemetry-class
        # (D9), and §5.3 reads absence as unverifiable, never tampered — a
        # dropped record is strictly safer than a wrong one (the §5.2 raise
        # gate lands here as a warning + no record).
        _t0 = time.time()
        try:
            import warnings

            from zagg.content_hash import content_hashes_record, hash_arrays

            group = zarr.open_group(box["store"], path="", mode="r", zarr_format=3)
            with warnings.catch_warnings():
                # The leaf's own coverage sidecar is the one known non-zarr
                # object under the prefix; ``members()`` warn-skips it (the
                # ``process_and_write_raster_hive`` suppression precedent).
                warnings.filterwarnings("ignore", message=f"Object at {COVERAGE_SIDECAR}")
                metadata["content_hashes"] = content_hashes_record(
                    hash_arrays(group, staged=staged)
                )
        except Exception as e:
            logger.warning(f"O11 content hashing failed (fail-open, issue #342): {e}")
        else:
            # Same "populated phase_timings" gate as the write stamp above:
            # the timing rides an existing dict, never seeds one.
            if "phase_timings" in metadata:
                metadata["phase_timings"]["hash"] = time.time() - _t0
    # Leaf pyramid column (issue #383): written AFTER the leaf's own commit,
    # from the same resident staged slabs — the fleet side of #381 points
    # (1)-(3). Gated inside on the /2 declaration carrying leaf-node levels
    # (``output.pyramid.overviews``); a failure FAILS THE UNIT, and the
    # idempotent retry rewrites leaf + column wholesale.
    # The window filter injection above never touches ``config.output``, so
    # the windowed per-unit config copy carries the declaration unchanged.
    if not metadata.get("error") and "store" in box:
        from zagg.column import write_leaf_column

        _t0 = time.time()
        try:
            column = write_leaf_column(
                store_root,
                shard_key,
                grid,
                config,
                staged,
                window=window["label"] if window else None,
                time_range=time_range,
                granule_count=metadata.get("granule_count", 0),
                store_kwargs=store_kwargs,
            )
        except Exception as e:
            # Reported, not raised: the leaf is already COMMITTED here, so a
            # raise would discard the caller's whole telemetry envelope (the
            # D20 stats record, the leaf sidecar, the D22 sub-map) for data
            # that landed. ``metadata["error"]`` is the same unit-failure
            # channel a failed ``process_shard`` uses — the Lambda handler
            # returns 500 on it and the dispatcher retries, identical retry
            # semantics — while the caller keeps a coherent metadata dict to
            # build its failure record from.
            logger.error(f"leaf column write failed for shard {shard_key}: {e}")
            metadata["error"] = f"leaf column: {e}"
            metadata["column_error"] = str(e)
        else:
            if column is not None:
                metadata["leaf_column"] = column
                if "phase_timings" in metadata:
                    metadata["phase_timings"]["column"] = time.time() - _t0
    return metadata


def _read_json(obj_store, key: str) -> dict | None:
    """GET+parse one small JSON object; ``None`` when it does not exist."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(obj_store, key).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "COMMIT_ATTR",
    "COVERAGE_BOX_SLOTS",
    "COVERAGE_SIDECAR",
    "COVERAGE_SPEC",
    "HIVE_SPEC",
    "HIVE_SPEC_V2",
    "MANIFEST_NAME",
    "ROOT_COVERAGE_NAME",
    "build_coverage",
    "build_manifest",
    "build_root_coverage",
    "check_node_invariant",
    "decode_coverage_bitmap",
    "encode_coverage_bitmap",
    "ensure_manifest",
    "leaf_block_index",
    "leaf_column_expectation",
    "leaf_identity_gate",
    "process_and_write_hive",
    "read_commit",
    "read_coverage",
    "read_coverage_bitmap",
    "read_manifest",
    "read_root_coverage",
    "root_coverage_words",
    "shard_leaf_path",
    "stamp_commit",
    "write_coverage_sidecar",
    "write_root_coverage",
]
