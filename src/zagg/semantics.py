"""Semantic-core canonicalization + hash (issue #299, D19).

A product's identity splits in two (D19, ``docs/design/sparse_coverage.md``):
the **name** addresses it (the ``{store_root}/{name}/`` product root), and the
**``semantic_hash``** verifies it — sha256 over the canonicalized
**output-defining subset only**. Two configs with the same semantic core
produce the same values in every cell they share; everything else is
packaging.

Included (the semantic core):

- the ``aggregation`` block — functions, params, dtypes, fills, ragged kinds,
  declared coordinates, ``chunk_precompute`` — minus the ``handoff`` carrier
  choice (arrow vs pandas is a worker-internal transport) and the whole
  ``streaming`` block (every streaming regime shares one identity under the
  law-equivalence contract — see the exclusions below);
- the ``data_source`` **semantics** — which groups/variables/coordinates are
  read and how observations are filtered (``filters``/``quality_filter``,
  photon ``base_level``/``levels``, raster ``bands``/``nodata``/
  ``collections``/``static_data``) — minus the read machinery, the credential
  mechanism, the byte-movement bounds and fan-out sizing (``reader``,
  ``driver``, ``read_plan``, ``index``, ``anonymous``,
  ``credentials_provider``, ``source_region``,
  ``shard_workers``/``granule_workers``, ``read_workers``, ``write_buffer``);
- the grid **type + indexing scheme** (D19: cell order is a resolution axis
  (D24), parent/shard order and chunking are packaging — hashing the whole
  template would have made o8 and o9 runs different products and blocked
  mixed-order processing);
- the **pipeline type** (``spatial`` | ``temporal`` | ``event``; absent
  normalizes to ``spatial`` — espg-ruled on the PR #316 review: a temporal
  engine over the same aggregation block is a different product; D19's
  ratified list omitted it only because the temporal path wasn't in frame);
- the **leaf-shaping ``output`` knobs** (the issue #415 hash epoch, espg-ruled
  as PR #397 question (8)(c)): every knob that changes what a leaf *contains*
  or which artifacts sit *beside* it — :data:`OUTPUT_LEAF_SHAPING_KEYS` and
  :data:`GRID_LEAF_SHAPING_KEYS`. Before the epoch the whole ``output`` block
  was outside the core, so the issue #388 skip gate's identity pair could not
  see a leaf-shape change at all and read a config-changed rerun as ``equal``.
  Every knob here is folded in **resolved through its accessor**, never as
  spelled, so an explicit default hashes identically to an absent key (§8.3,
  the ``pipeline.type`` discipline);
- the raster **time-coordinate encoding** (``output.time_encoding``, spec §8 /
  issue #443), keyed only when non-default: toc words and legacy microseconds
  are different stored values meaning different things, exactly as
  ``weights: "flux"`` is;
- the **per-observation clock** (``output.time_source``, spec §8.2/§8.3 /
  issue #410) a temporal companion encodes its toc words from — one of the
  leaf-shaping ``output`` knobs above, keyed only when a field declares a
  companion. Two configs differing only in the declared epoch write different
  words for the same observations, so a rerun under a corrected epoch must not
  inherit the identity of the store it contradicts.

Excluded as packaging: all orders (``parent_order``/``child_order``/
``chunk_inner`` — the D24 resolution axis, which the epoch deliberately did
NOT touch: hashing them would still make o8 and o9 runs different products and
block mixed-order processing), store layout/path/name, ``coverage_moc`` and
``sweep`` (run triggers over regenerable D9 caches), the chunk-index block
(``data_source.index`` — the issue #499 epoch, espg-ruled 2026-09-13: read
machinery; see :data:`DATA_SOURCE_PACKAGING_KEYS`), ``consolidate_metadata``
(a derived finalize blob), the whole ``pyramid`` block (D11 keeps it out of
the manifest's frozen keys because the §7 sweep populates it; the leaf column
it declares is verified by READING the artifact instead — see
:data:`OUTPUT_LEAF_SHAPING_KEYS`), ``emit_cell_ids`` (the issue #304 D16
transition hatch — leaf-shaping, but scheduled for removal, so hashing it
would strand stores whose digest no legal config reproduces; see
:data:`GRID_LEAF_SHAPING_KEYS`), worker sizing, the whole
``aggregation.streaming`` block (mode AND its sizing knobs, under the
law-equivalence contract — espg-ruled 2026-08-17 on the PR #475 D19 question;
see :data:`AGGREGATION_PACKAGING_KEYS`), read knobs,
the credential mechanism and the byte-movement knobs
(``credentials_provider``/``anonymous``/``source_region``/``read_workers``/
``write_buffer`` select how source bytes are fetched or moved, never what is
computed — espg-ruled 2026-08-17),
catalog/bounds (run inputs, recorded per-run — catalog identity lives in the
D20 sidecar, never the product identity), and the per-variable
``overview_delta`` (issue #424 — the pyramid-fold budget shapes overview
artifacts only, and the overview family is already packaging). A variable's
``weights: "counts"`` normalizes away as the spec §2.0 absent-key default;
``weights: "flux"`` is output-defining and hashes.

Canonical form: the core dict serialized as sorted-key, compact,
ASCII-escaped JSON — so YAML comments, whitespace, and key order can never
change the hash (§8.3 canonicalization obligations). The hash is the **full
sha256 hex digest** (git-style: the full digest is what is compared; the
12-hex :func:`semantic_fingerprint` is the display/CLI shorthand — 48 bits,
comfortable for the only collision domain that matters, display within one
store's product listing).

This module formalizes the #89 signature seam (``grid.spatial_signature`` /
``config.output_field_signature`` are dict fingerprints; this is the
content-addressed form) — see the issue #299 thread for the design record.
"""

from __future__ import annotations

import hashlib
import json

from zagg.config import PipelineConfig, get_pipeline_type
from zagg.time_axis import DEFAULT_TIME_ENCODING

#: ``data_source`` keys that are read machinery or worker sizing, not output
#: semantics (D19 names the two as separate packaging categories).
#: Changing any of these must never change the ``semantic_hash``.
#:
#: The granule fan-out width joined at the issue #415 hash epoch, under BOTH
#: its spellings — canonical ``shard_workers`` (issue #232) and the legacy
#: ``granule_workers`` (issue #180) the config still honors. D19's ratified
#: exclusion list already named "worker size" as packaging; the keys were
#: simply never listed here, which made the seams' fallback hash
#: **clamp-sensitive** (PR #397 question (7)): both dispatchers send a
#: per-cell ``data_source`` whose ``granule_workers`` is clamped to
#: ``min(K, n_granules)`` (:func:`zagg.runner._clamped_data_source`, issue
#: #184), so a small shard's worker-side hash differed from the run-level
#: hash — and ``semantic_hash`` being in :data:`zagg.telemetry._EQ_OR_NONE_KEYS`
#: then collapsed the identity half to ``None`` in any rollup mixing clamped
#: and unclamped shards.
#:
#: The clamp writes ``granule_workers`` whatever the config spelled, so that
#: key alone closes the reported drift; ``shard_workers`` rides along because
#: the two ARE one knob (``zagg.processing.worker._granule_workers`` resolves
#: the canonical name first) and hashing the canonical spelling of an excluded
#: knob would be incoherent. Issue #415 named only the legacy alias, so the
#: canonical one is flagged for ruling on the epoch PR rather than assumed.
#:
#: ``credentials_provider`` (issue #213 Phase 4/6) joined at the same epoch,
#: **espg-ruled 2026-08-17**: it selects HOW source bytes are fetched — which
#: registry name mints the DAAC credentials the dispatcher attaches to every
#: event — and never WHAT is computed from them, the same D19 class as
#: ``reader``/``driver``/``read_plan``. Three reasons it is ruled *here*
#: rather than left pinned as-is (issue #449):
#:
#: * the same granules fetched with ``lpdaac`` creds, ``gesdisc`` creds, or an
#:   ``anonymous`` open produce byte-identical leaves — a hash that moves with
#:   the provider is a false product split. It is the same class as the read
#:   knobs and as ``anonymous``, already excluded (a *class*, not one knob
#:   under two names: ``anonymous`` is read only by the raster source-store
#:   kwargs, ``credentials_provider`` only by the point and temporal paths);
#: * the failure asymmetry is loud in the excluded direction and silent in the
#:   included one: the wrong credential fails the *fetch* — a 403 at read time,
#:   never a quiet wrong value — so nothing depends on the hash to catch it,
#:   while hashing it silently splits one product in two;
#: * a credential migration must never rehash unchanged data. The MERRA-2 /
#:   gesdisc path is the live case: moving an existing store from one provider
#:   registration to another (or adding the key to a config that ran without
#:   it) would otherwise refuse every leaf as ``semantic-mismatch`` and rewrite
#:   the store to produce the same bytes.
#:
#: Ruled at the epoch deliberately so the first GEDI store's identity is born
#: without an auth knob in it: no store carries a provider-bearing hash yet
#: (the ``lpdaac`` line lands with the GEDI template, PR #450), so this costs
#: nothing now and cannot be taken back cheaply later.
#:
#: ``read_workers``, ``write_buffer`` and ``source_region`` joined at the same
#: epoch, **espg-ruled 2026-08-17** (PR #420 question (2)(c)): each selects how
#: bytes are fetched or moved, never what is computed from them — the same D19
#: class as ``credentials_provider``, and it passes the same three-part test.
#:
#: * *Mechanism, not computation.* ``read_workers`` (issue #170) is the third
#:   fan-out width beside the two spellings above — the read budget is sized as
#:   ``granule_workers × read_workers × fetch width`` — and a pool width cannot
#:   change a decoded value. ``write_buffer`` bounds how many slabs are alive
#:   under the streamed raster sink (``zagg.processing.raster._write_buffer``):
#:   a peak-memory knob over an identical written result. ``source_region`` is
#:   the raster source store's AWS region kwarg, and it sits in the *same dict
#:   literal* as the already-excluded ``anonymous`` (``zagg.runner``'s
#:   ``src_kwargs``) — hashing one and not the other split a single "how do we
#:   open the source" decision across both sides of the D19 line.
#: * *The failure asymmetry runs the safe way.* Each fails LOUDLY in its own
#:   direction — a too-small pool is slower, a wrong region is a connection
#:   error, an over-large buffer is an OOM — so nothing depended on the digest
#:   to catch them, while hashing them silently splits one product in two.
#:   The live demonstration is dated, and it covers the **fan-out widths only**:
#:   two GEDI flux builds of the same shard on 2026-08-17 produced identical
#:   ``total_obs`` and ``cells_with_data`` in the exact single-block spill
#:   regime and still hashed apart on ``read_workers`` and the two ``*_workers``
#:   spellings above. It cannot demonstrate the other two — ``write_buffer``
#:   and ``source_region`` are read only on the RASTER path, and a GEDI flux
#:   build is the point path — so their exclusion rests on the arguments above,
#:   which stand without a measurement.
#: * *A machinery migration must never rehash unchanged data* — retuning a pool
#:   width or a buffer bound over an existing store would otherwise refuse
#:   every leaf as ``semantic-mismatch`` and rewrite it to produce the same
#:   bytes.
#:
#: Ruled at the epoch deliberately: the digest moves once here, and excluding
#: them later would cost a second epoch for knobs that never belonged in the
#: core.
#:
#: ``index`` — the chunk-index block (``backend`` inline/hierarchical/sidecar,
#: the sidecar ``store`` location, ``on_miss``) — joined at the **issue #499
#: epoch, espg-ruled 2026-09-13** (refs #547, PR #565): it is READ MACHINERY
#: in exactly the ``reader``/``read_plan`` sense. A sidecar miss processes the
#: file (``on_miss: build`` is a cache-population policy, not a filter), and a
#: different sidecar location yields byte-identical leaves — so hashing the
#: block split one product in two across every relocation of its index cache.
#: The live case is the one issue #499 is about: the sidecars move to a public
#: bucket, and a hash that named the old location would have refused every
#: append to the store they index. Unlike the 2026-08-17 epoch this one moves
#: digests that outlive it — both deployed stores carry an ``index`` block, so
#: their frozen manifest hashes stop reproducing (:func:`semantic_hash_legacy`
#: is the self-migrating guard; the migration note is in
#: ``docs/hive_layout.md``, "Migration: the index-exclusion epoch").
DATA_SOURCE_PACKAGING_KEYS = (
    "reader",
    "driver",
    "read_plan",
    "index",
    "anonymous",
    "credentials_provider",
    "source_region",
    "shard_workers",
    "granule_workers",
    "read_workers",
    "write_buffer",
)

#: ``aggregation`` keys that are packaging: the per-cell carrier choice
#: (issue #132) transports identical values either way, and the whole
#: ``streaming`` block — mode, ``buffer_granules``, ``block_bytes`` —
#: **espg-ruled 2026-08-17** (the PR #475 D19 question, option (b)): every
#: streaming regime shares one product identity, with bytes bounded by the
#: **law-equivalence contract** (``docs/design/sparse_coverage.md``, D19):
#: each streaming mode MUST land within the documented approximation law of
#: the pooled path, in one of three classes — exact (the single-block regime
#: and the summation reducers), the kway/``np.isclose`` class (the payload
#: channel across merge flushes and block closes), and the channel-specific
#: documented bounds for the companion channels (a located word folds to the
#: contributors' common ancestor, pinned as an ancestor-or-equal hull; the
#: packed composition word keeps presence exactly and counts within one lane
#: quantization) — and a mode that cannot maintain the class its channels
#: fall in may not join the block.
#: Enforcement is ``tests/test_spill.py::TestSpillWorkerSingleBlock`` (the
#: exactness half), ``tests/test_spill_crossblock.py``, and
#: ``tests/test_streaming.py``'s pooled-parity pins. The D19 record stated
#: this acceptance property twice while the code hashed the block as spelled;
#: the ruling lands the code on the record's side before any **long-lived**
#: store carries a streaming-declared digest (the deployed stores age out on
#: a 30-day cycle), so the epoch moves nothing that outlives it.
AGGREGATION_PACKAGING_KEYS = ("handoff", "streaming")

#: Per-variable aggregation keys that are packaging (issue #424):
#: ``overview_delta`` shapes the pyramid/overview fold budget only — the
#: overview family is packaging already (``output.pyramid`` never enters the
#: core), so the split budget must not move a base product's identity either.
VARIABLE_PACKAGING_KEYS = ("overview_delta",)

#: Display length of :func:`semantic_fingerprint` (12 hex = 48 bits; the
#: birthday bound puts same-store collision odds around 1e-8 at 1e4 products
#: — recorded rationale on the issue #299 thread).
FINGERPRINT_HEX = 12

#: Non-healpix grid keys that spatially define the product (F1, issue #299).
#: For rect/other grids the cell geometry is fixed by CRS + resolution +
#: bounds, so two such products differing in any of these are different
#: products (D24's resolution-axis exclusion is a HEALPix/morton composability
#: argument that does not extend to rect — over-discriminating is safe, a
#: semantic collision is not). HEALPix stays type + indexing scheme only.
GRID_SPATIAL_KEYS = ("crs", "resolution", "bounds")

#: ``output.grid`` keys that shape the LEAF, folded into the core's ``grid``
#: at the issue #415 hash epoch (PR #397 question (8)(c)).
#:
#: - ``sharded`` (issue #108) — the knob question (8) checked: flipping it
#:   over an existing store changes the leaf's object set (one ShardingCodec
#:   object per array vs K regular chunk objects) while the granule set holds,
#:   so pre-epoch the skip gate read ``equal`` and the store kept its old
#:   layout forever. The orders it interacts with (``chunk_inner``, and hence
#:   K) stay excluded — D24's resolution axis is untouched, so this is
#:   deliberately the one packing knob in the core.
#:
#:   The DECLARATION is hashed, not the effective layout, and the two limits
#:   that follow are **espg-ruled 2026-08-17** (PR #420 question (3)(a)): left
#:   exactly as landed, because resolving the effective layout needs K and K
#:   needs the orders D24 keeps out. Both are known, both are bounded:
#:
#:   * at ``K == 1`` the grid silently no-ops sharding (issue #215), so
#:     ``sharded: true`` and ``sharded: false`` write identical leaves and
#:     still hash apart — an over-discrimination, the safe direction (F1's
#:     recorded posture: "over-discriminating is safe, a semantic collision
#:     is not");
#:   * ``chunk_inner`` changes K, hence the leaf's object set, and still
#:     hashes equal — the object-layout hole is narrowed here, not closed.
#:     Closing it costs part of D24, which is why it stays open by ruling
#:     rather than by omission.
#:
#: ``emit_cell_ids`` (issue #304) is deliberately NOT here, though the D16
#: transition hatch does write an ADDITIONAL ``cell_ids`` array into every leaf
#: — excluded by name before the epoch, briefly folded in on the criterion
#: alone, and **ruled back out 2026-08-17** (PR #420 question (4)(b)). The
#: hatch is scheduled for REMOVAL, and a hashed hatch would leave every store
#: built with it ON carrying a digest that no legal config can reproduce once
#: the knob is gone — a permanently unverifiable identity, traded for a leaf
#: shape that artifact-level reading already covers. That is the same
#: precedent, and the same closing argument, that keeps ``output.pyramid`` out
#: (see :data:`OUTPUT_LEAF_SHAPING_KEYS`): a leaf's ARRAY INVENTORY is verified
#: by reading the leaf, not by the digest.
GRID_LEAF_SHAPING_KEYS = ("sharded",)

#: ``output`` keys outside ``grid`` that shape the LEAF (issue #415 epoch).
#:
#: - ``aoi_mask`` (issue #101) — masks cells outside the AOI, so the leaf's
#:   VALUES differ. The plainest semantic knob that was outside the core.
#: - ``windowing`` (issue #246, D13) — partitions observations by time into
#:   per-window leaves, so a windowed and an unwindowed store over the same
#:   granules shared a hash before the epoch. What this buys is **product
#:   identity and the D20 sidecar's identity half**, not the manifest guard:
#:   ``temporal`` is already a frozen manifest key (:data:`zagg.hive.
#:   _FROZEN_MANIFEST_KEYS`) and a schedule change also lands the unit on a
#:   ``{window}.zarr`` path with no sidecar (D23), so pre-dispatch refusal and
#:   the leaf gate were already covered — the hash is what a multi-product
#:   root and a fleet leaf's recorded identity compare. Folded in its
#:   NORMALIZED form (:func:`zagg.config.get_windowing`), so ``epoch``
#:   spellings and the issue #355 point-window sugar canonicalize.
#:
#: - ``time_source`` (issue #410) — the per-observation clock a spec §8.2/§8.3
#:   temporal companion encodes its toc words from, so the declared column,
#:   epoch, scale and units decide the companion's stored VALUES. Keyed only
#:   when a field declares a companion and folded in its RESOLVED form
#:   (:func:`_toc_source_leaf_shape`), so a clock nothing consumes hashes as
#:   absent and the ``output.windowing`` fallback is not a second product.
#:
#: ``output.pyramid`` is deliberately NOT here, though it does shape the leaf
#: (issue #383's column artifact), for three reasons that all point the same
#: way: the artifact half is already covered by reading the artifact —
#: :func:`zagg.hive.leaf_column_expectation`, the specification's §4.6 "read
#: the columns, not the manifest" posture, which is what closed question (8)'s
#: concrete regression; the block is excluded from the manifest's frozen keys
#: by D11 because the §7 sweep populates it; and the retrofit path
#: (:func:`zagg.sweep_overview.declare_pyramid`, ``--declare-pyramid``) exists
#: precisely to add a pyramid declaration to the config that built a store,
#: which its own semantic guard would then refuse. Hashing it would break a
#: supported workflow to re-cover ground already covered.
OUTPUT_LEAF_SHAPING_KEYS = ("aoi_mask", "windowing", "time_source")


#: D24 exact merge laws: aggregator name -> fold law. These are the reducers
#: whose per-cell outputs compose EXACTLY across cell orders — count/sum by
#: addition, min/max by extremum — so an up-tree fold of leaf values equals a
#: direct aggregation at the coarser order, byte for byte (§8.3). Names are
#: matched after :func:`_fold_function_name` normalization, so ``min``,
#: ``np.min`` and ``numpy.min`` all key the same law.
#:
#: The plain and nan-aware variants share a law ON PURPOSE, and the exactness
#: claim is against the **nan-skipping** reduction for both (review finding,
#: issue #201): a leaf's stored NaN is the same bytes whether it is the fill
#: sentinel or a NaN datum, so nothing downstream can tell them apart —
#: :func:`zagg.sweep_overview.fold_dense` skips both, and the overview records
#: :data:`zagg.sweep_overview.EXACT_NAN_POLICY` per field so a reader knows
#: which reduction it actually got. A NaN-propagating ``min``/``max``/``sum``
#: is unrecoverable at this seam, not merely unimplemented.
EXACT_MERGE_LAWS = {
    "len": "sum",
    "count": "sum",
    "sum": "sum",
    "nansum": "sum",
    "min": "min",
    "nanmin": "min",
    "max": "max",
    "nanmax": "max",
}

#: The four D24 composability classes, weakest first. ``exact`` folds byte-
#: equal; ``packed`` folds by a deterministic, order-independent law over
#: packed lane-quantized words (``merge_composition_kway`` — presence exact
#: through arbitrary chains, counts within one lane quantization per fold,
#: NOT byte-equal to a direct aggregation; issue #515); ``approximate``
#: merges natively but order-dependently (t-digests — ``np.isclose``
#: equality, the merge-vs-spill epistemic class); ``none`` has no merge law
#: and exists only at native resolution (per-field exclusion — the ruled D24
#: default, issue #201).
COMPOSABILITY_CLASSES = ("none", "packed", "approximate", "exact")


def _fold_function_name(name) -> str | None:
    """Normalize an aggregator ``function`` name for merge-law lookup.

    ``np.``/``numpy.`` prefixes strip (they resolve to the same callables in
    :func:`zagg.config.resolve_function`); other dotted paths pass through
    unchanged (their laws are keyed by full path, e.g. the t-digest builders).
    """
    if not isinstance(name, str):
        return None
    for prefix in ("np.", "numpy."):
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def field_composability(meta: dict) -> str:
    """The D24 composability class of one aggregation field's metadata.

    Derived from the field's aggregator via the merge-law flags the #217
    mergeable-reducer machinery already established (the same set
    ``validate_streaming`` accepts, widened by the exact sum/min/max laws):

    - ``exact`` — scalar ``function`` in :data:`EXACT_MERGE_LAWS`;
    - ``approximate`` — a ragged digest-family field with the standard
      ``(2,)`` centroid inner shape, **located or not** (merge is
      order-dependent; ``np.isclose`` equality class, cf. D24). The family is
      :data:`zagg.processing.streaming._DIGEST_FAMILY_FUNCTIONS` — the
      t-digest builders, the waveform flux digest (issue #508), and the
      ``build_tdigest_where`` strata builder (issue #515: row selection
      precedes the build, so a stratum's payload is an ordinary centroid
      array), every reducer whose stored payload folds by the k-way law;
    - ``packed`` — the packed composition word (``pack_composition``, spec §3;
      issue #515): a dense uint64 whose fold law is
      :func:`zagg.stats.composition.merge_composition_kway` over ``(word,
      n_signal)`` pairs, with ``n`` sourced from the ``of`` digest's per-cell
      weights (spec §3.3/§3.4). Deterministic and order-independent per fold
      call — presence exact through arbitrary chains, counts within one lane
      quantization per fold — but not byte-equal to a direct aggregation, so
      neither of the other two arms states it honestly. Classified only when
      the field's ``attrs.composition`` block names its ``of`` digest: the
      law's divisor comes from that field, so a word with no recorded linkage
      has no fold (``none``);
    - ``none`` — everything else: expressions, vector fields, chunk-resolution
      companions, a ``temporal: per-cell`` dense companion (see below), and any
      scalar reducer without an exact law (mean, std, median, quantiles, ...).

    **Both companion channels fold through the pyramid** (issue #410, rulings 3
    and 4): a digest field's companions are reduced over the same centroid
    partition the payload merge produces, by the k-way merge the kernel has
    carried since #279 — ``locations=`` to the deepest common ancestor of the
    contributors' words (spec §9.1), ``temporal=`` to their toc envelope (spec
    §8.3). Neither declaration removes the digest from the pyramid, and
    excluding located fields was the bug, not the design: a ``location:``
    declaration used to make the field vanish from every overview level.

    The temporal channel stays **per-centroid at every level**, symmetric with
    located — espg-ruled 2026-08-17, amending ruling 3, which had put a per-cell
    toc range at overview levels. §8.4's shape-coarsening reduction stays
    licensed for producers that want it, but zagg's digest pyramids do not use
    it: one companion shape means readers bind one contract, and the ranges an
    overview's per-centroid words carry degrade toward per-cell information
    content on their own as δ falls, which is what makes them informative
    exactly where elevation and time correlate.

    A ``temporal: per-cell`` field — the §8.2 dense shape, GEDI's leaf companion
    (ruling 2) — is a different object and stays ``none``. Its fold law is the
    word grammar's join over a cell group, not its own reducer, so classifying
    it by ``function`` would fold e.g. ``nanmax`` over toc words and emit a word
    whose conservative-envelope claim is false. Wiring a dense toc law is not
    what the ruling asked for, and no shipped config pairs a per-cell
    companion with a pyramid, so it is left out rather than guessed at.
    """
    from zagg.config import get_output_signature
    from zagg.time_axis import TOC_SHAPE_PER_CELL

    sig = get_output_signature(meta)
    if meta.get("expression") is not None or sig["resolution"] != "cell":
        return "none"
    if sig.get("temporal") == TOC_SHAPE_PER_CELL:
        return "none"
    function = _fold_function_name(meta.get("function"))
    if sig["kind"] == "ragged":
        from zagg.processing.streaming import _DIGEST_FAMILY_FUNCTIONS

        if meta.get("function") in _DIGEST_FAMILY_FUNCTIONS and tuple(sig["inner_shape"]) == (2,):
            return "approximate"
        return "none"
    if sig["kind"] == "scalar":
        from zagg.processing.streaming import _COMPOSITION_FUNCTION

        if meta.get("function") == _COMPOSITION_FUNCTION:
            of = ((meta.get("attrs") or {}).get("composition") or {}).get("of")
            return "packed" if of else "none"
        if function in EXACT_MERGE_LAWS:
            return "exact"
    return "none"


def composability_classes(config: PipelineConfig) -> dict[str, str]:
    """``{field_name: composability class}`` for every aggregation field (D24)."""
    from zagg.config import get_agg_fields

    return {name: field_composability(meta) for name, meta in get_agg_fields(config).items()}


def _without(mapping: dict, keys: tuple[str, ...]) -> dict:
    return {k: v for k, v in (mapping or {}).items() if k not in keys}


def _normalize_variables(aggregation: dict) -> dict:
    """Drop per-variable packaging keys and default-valued declarations (#424).

    Two normalizations, both hash-stability obligations:

    - :data:`VARIABLE_PACKAGING_KEYS` (``overview_delta``) drop outright —
      overview artifacts are packaging, so declaring the split fold budget
      hashes identically to omitting it;
    - ``weights: "counts"`` drops because it is the spec §2.0 **absent-key
      default** — the explicit and absent spellings mean the same bytes, so
      they must be the same product. ``weights: "flux"`` stays: the stored
      weight column means something else, which is exactly output-defining.
    """
    variables = (aggregation or {}).get("variables")
    if not variables:
        return aggregation
    out = {}
    for name, meta in variables.items():
        if isinstance(meta, dict):
            meta = {k: v for k, v in meta.items() if k not in VARIABLE_PACKAGING_KEYS}
            if meta.get("weights") == "counts":
                meta = {k: v for k, v in meta.items() if k != "weights"}
        out[name] = meta
    return {**aggregation, "variables": out}


def _prune_nulls(obj):
    """Recursively drop ``None``-valued keys from every dict in ``obj``.

    §8.3 canonicalization: a YAML explicit-null (``key:``) must hash identically
    to an absent key, at every depth — not just the top level. Applied to the
    whole core so a nested ``None`` (e.g. ``quality_filter.value:``) drops out.
    Lists are recursed but never pruned by value: list entries are positional,
    so a ``None`` element is content, not an absent key.
    """
    if isinstance(obj, dict):
        return {k: _prune_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_prune_nulls(v) for v in obj]
    return obj


def _windowing_leaf_shape(config: PipelineConfig):
    """The normalized ``output.windowing`` declaration, or ``None`` (D13).

    Normalized rather than as-spelled so ``epoch`` spellings and the issue
    #355 point-window sugar canonicalize (§8.3). Total by contract: an
    out-of-grammar block is ``_validate_windowing``'s refusal to raise, not
    the hash's — :func:`semantic_core` must not start raising on a config that
    hashed before the epoch (the Lambda worker builds its config without
    ``validate_config``), so such a block simply hashes as spelled.

    Totality is split by fault class rather than caught around the whole call:
    a non-mapping block is refused on SHAPE here, and a mapping missing a
    required key (or carrying an unparsable ``epoch``/window bound) raises
    ``KeyError``/``ValueError`` out of the normalizer, which is a GRAMMAR
    fault ``_validate_windowing`` also refuses by name. Anything else —
    ``TypeError``, ``AttributeError`` — would be a fault INSIDE the normalizer
    and is deliberately left to propagate rather than launder into a digest.
    """
    from zagg.config import get_windowing

    block = (config.output or {}).get("windowing")
    if not isinstance(block, dict):
        return block
    try:
        return get_windowing(config)
    except (KeyError, ValueError):
        return block


def _toc_source_leaf_shape(config: PipelineConfig):
    """The per-observation clock a temporal companion encodes from, or ``None``.

    ``output.time_source`` (issue #410) is leaf-shaping in the most literal
    sense :data:`OUTPUT_LEAF_SHAPING_KEYS` admits: a §8.2/§8.3 companion's
    stored toc words ARE the declared column converted from the declared epoch
    on the declared scale, so two configs differing only here write different
    words for the same observations. Without it in the core, a rerun under a
    corrected epoch would reuse the product identity of the store it
    contradicts — the failure D19 exists to refuse.

    Two conditions narrow it, both the epoch's own discipline:

    - ``None`` unless some field actually **declares** a companion. Declaring a
      clock nothing consumes moves no store byte, so it must not move the hash
      (the ``windowing``-prunes-when-unwindowed rule, one level down).
    - Folded **resolved** through :func:`zagg.time_axis.toc_source`, which
      applies the continuous-scale ``output.windowing`` fallback, so the two
      spellings of one clock — explicit block, or inherited from the windowing
      declaration beside it — are the same product rather than two.

    It sits INSIDE the ``output`` block, unlike ``time_encoding``: no store
    carries a temporal companion yet (the producer gate lifted in this same
    change), so placing it coherently costs nothing, where relocating
    ``time_encoding`` would move digests that already exist.

    Total by contract, exactly as :func:`_windowing_leaf_shape` is: a
    malformed block is ``_validate_time_source``'s refusal to raise, not the
    hash's, so a shape fault hashes as spelled rather than raising inside a
    digest the Lambda worker computes without ``validate_config``.
    """
    from zagg.config import get_agg_fields
    from zagg.time_axis import toc_source

    if not any(m.get("temporal") is not None for m in get_agg_fields(config).values()):
        return None
    block = (config.output or {}).get("time_source")
    if block is not None and not isinstance(block, dict):
        return block
    try:
        resolved = toc_source(config)
    except (KeyError, ValueError, TypeError):
        return block
    return dict(resolved) if resolved is not None else None


def semantic_core(config: PipelineConfig) -> dict:
    """The output-defining subset of ``config`` (D19), as a plain dict.

    Deterministic given the config's semantics: two configs differing only in
    packaging knobs (orders, chunking, worker size, read machinery, carrier,
    the ``aggregation.streaming`` block) map to the same core. ``None``-valued keys are pruned recursively so a
    YAML explicit-null hashes identically to an absent key (§8.3). The returned
    structure is JSON-serializable plain data (the YAML loader guarantees it).
    """
    from zagg.config import get_aoi_mask, get_sharded

    grid_cfg = (config.output or {}).get("grid", {}) or {}
    grid_type = grid_cfg.get("type", "healpix")
    grid: dict = {"type": grid_type}
    if grid_type == "healpix":
        # The one indexing scheme zagg writes (the morton store convention
        # rides D16 attrs; the underlying cell tiling is HEALPix NESTED).
        grid["indexing_scheme"] = "nested"
    else:
        # Rect/other: fold in the spatially-defining params when present (F1).
        for key in GRID_SPATIAL_KEYS:
            if key in grid_cfg:
                grid[key] = grid_cfg[key]
    # GRID_LEAF_SHAPING_KEYS, resolved (issue #415): `sharded`'s default is
    # grid-family-shaped exactly as `grids.from_config` resolves it — HEALPix
    # output defaults to sharded (issue #215), rect does not — so an explicit
    # `sharded: true` on a HEALPix config hashes identically to omitting it.
    # `emit_cell_ids` is NOT folded in (espg-ruled 2026-08-17) — see the
    # constant.
    grid["sharded"] = get_sharded(config, default=grid_type == "healpix")
    core: dict = {
        "aggregation": _normalize_variables(
            _without(config.aggregation, AGGREGATION_PACKAGING_KEYS)
        ),
        "data_source": _without(config.data_source, DATA_SOURCE_PACKAGING_KEYS),
        "grid": grid,
        # pipeline.type is output-defining (espg-ruled on the PR #316
        # review, 2026-07-21): a temporal/event engine over an identical
        # aggregation block is a DIFFERENT product. Absent normalizes to
        # the "spatial" default on both sides (get_pipeline_type's default),
        # the same discipline as the manifest's path_grouping absent=>1, so
        # every existing config hashes stably.
        "pipeline": {"type": get_pipeline_type(config)},
        # OUTPUT_LEAF_SHAPING_KEYS (issue #415): the knobs that decide what a
        # leaf CONTAINS or which artifacts sit beside it. Resolved, never as
        # spelled — an unwindowed store prunes `windowing` away entirely, so
        # only stores that actually declare a schedule pay for the key.
        "output": {
            "aoi_mask": get_aoi_mask(config),
            "windowing": _windowing_leaf_shape(config),
            "time_source": _toc_source_leaf_shape(config),
        },
    }
    # The time coordinate's encoding (spec §8, issue #443) is output-defining
    # for the same reason `weights: "flux"` is: the stored axis MEANS
    # something else. Keyed only when non-default, so every config written
    # before §8 — and the explicit `microseconds` spelling of the absent-key
    # default — hashes byte-identically to today. It stays TOP-LEVEL rather
    # than joining the `output` block above: stores carrying it already exist
    # (the shipped S2 config declares `toc`), so relocating the key would move
    # their committed digests for no gain. `time_source` has no such
    # constraint — see :func:`_toc_source_leaf_shape`.
    encoding = (config.output or {}).get("time_encoding")
    if encoding not in (None, DEFAULT_TIME_ENCODING):
        core["time_encoding"] = encoding
    return _prune_nulls(core)


def _canonical_json(core: dict) -> str:
    return json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_semantic_json(config: PipelineConfig) -> str:
    """The canonical serialized form the hash is computed over.

    Sorted keys, compact separators, ASCII-escaped: syntactic YAML edits
    (comments, whitespace, key order) cannot reach this string.
    """
    return _canonical_json(semantic_core(config))


def semantic_hash(config: PipelineConfig) -> str:
    """Full sha256 hex digest of the canonical semantic core (64 hex chars).

    The frozen manifest key (D19): reusing a product name with a different
    semantic hash refuses up front, exactly as an orders mismatch does. Always
    compare the FULL digest; display via :func:`semantic_fingerprint`.
    """
    return hashlib.sha256(canonical_semantic_json(config).encode()).hexdigest()


#: The pre-epoch canonicalization rules :func:`semantic_hash_legacy` reproduces:
#: ``1`` is the index-in-core rule every store built before the issue #499
#: epoch carries (the 2026-08-17 epoch left no store behind — its stores aged
#: out on the 30-day cycle — so there is nothing older to reproduce).
LEGACY_EPOCHS = (1,)


def semantic_hash_legacy(config: PipelineConfig, *, epoch: int = 1) -> str:
    """The digest a PRE-epoch zagg computed for ``config`` — the migration probe.

    Epoch 1 (issue #499) is the current core plus ``data_source.index``
    re-inserted and canonicalized the same way (null-pruned, sorted, compact),
    which is exactly what :func:`semantic_core` produced before the block left
    :data:`DATA_SOURCE_PACKAGING_KEYS`. For a config with no ``index`` block the
    two epochs agree, so this equals :func:`semantic_hash`.

    Its one consumer is the retrofit guard (:func:`zagg.sweep_overview.
    _semantic_guard`): a store whose frozen ``semantic_hash`` matches THIS
    digest was built by this config under the old rule, and ``declare_pyramid``
    migrates the manifest to the current digest in the same write. The append
    path deliberately does NOT consult it (:func:`zagg.hive._frozen_matches`
    compares current-epoch digests only) — the redeclare tool is the single
    migration point, so a pre-epoch store refuses an append by name until an
    operator has run it.
    """
    if epoch not in LEGACY_EPOCHS:
        raise ValueError(f"unknown semantic-hash epoch {epoch!r}; known: {LEGACY_EPOCHS}")
    core = semantic_core(config)
    index = (config.data_source or {}).get("index")
    if index is not None:
        core = {**core, "data_source": {**core["data_source"], "index": _prune_nulls(index)}}
    return hashlib.sha256(_canonical_json(core).encode()).hexdigest()


def semantic_fingerprint(digest: str) -> str:
    """12-hex display shorthand of a full ``semantic_hash`` digest."""
    if len(digest) < FINGERPRINT_HEX:
        raise ValueError(f"not a semantic hash digest: {digest!r}")
    return digest[:FINGERPRINT_HEX]


__all__ = [
    "AGGREGATION_PACKAGING_KEYS",
    "COMPOSABILITY_CLASSES",
    "DATA_SOURCE_PACKAGING_KEYS",
    "VARIABLE_PACKAGING_KEYS",
    "EXACT_MERGE_LAWS",
    "FINGERPRINT_HEX",
    "GRID_LEAF_SHAPING_KEYS",
    "GRID_SPATIAL_KEYS",
    "LEGACY_EPOCHS",
    "OUTPUT_LEAF_SHAPING_KEYS",
    "canonical_semantic_json",
    "composability_classes",
    "field_composability",
    "semantic_core",
    "semantic_fingerprint",
    "semantic_hash",
    "semantic_hash_legacy",
]
