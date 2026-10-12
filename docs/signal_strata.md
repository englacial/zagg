# Signal/noise strata and the composition field

Issue #321: ATL03 photon signal confidence cannot be reconstructed after
aggregation (the classifier needs the along-track axis, the atmospheric
background stream, and per-surface tuning), so the signal/background decision
is **committed at ingest** and carried through the store in three pieces:

1. **Two disjoint t-digest fields** per cell — `h_tdigest_signal` and
   `h_tdigest_noise` — split by the ATBD signal predicate: a photon is signal
   when **any** `signal_conf_ph` surface column clears the threshold
   (default `>= 2`, i.e. the ATBD `> 1`; selection rule A1 — the union is
   idempotent and narrowable downstream). Each stratum digest's total weight
   is the **exact** stratum photon count. The total photon-flux distribution
   is recovered at read by one deterministic two-way `merge_tdigests`.
2. **One packed `uint64` composition word** per cell
   (`zagg.stats.composition`, spec `zagg-composition/1`): eight 8-bit lanes
   of quantized fractions of the signal stratum — five per-surface lanes
   (`signal_conf_ph` column order) and three low/med/high lanes.
3. **Store attrs** recording the commitment: each stratum's *payload* array
   carries `stratum` + `signal_threshold`, and the composition array carries
   the versioned `composition` block. Readers bind to these, never to config
   conventions. The located sibling (`{field}_locations`) carries no user
   attrs: it is addressed through its payload array, which holds the pair's
   provenance.

> **Normative home.** The composition word's byte layout, lane order,
> presence-floor quantization, attrs block, and merge law are
> [`specification.md` §3](specification.md#3-zagg-composition1). This page is
> the narrative companion — why the strata exist and how the pieces compose.
> Where the two disagree, the spec wins.

The shipped template is `zagg/configs/atl03_tdigest_strata_healpix.yaml` —
**located strata is the default**: both digest fields carry `location:
leaf_id`, so each centroid stores a morton word in the `{field}_locations`
sibling arrays — an order-29 **point** word (the photon's reported position,
no area claim) for an unmerged centroid, the deepest common ancestor of its
members' words once centroids merge; a reader decodes kind and order from
each word, never from the centroid's weight (spec §2.2/§9.1). The
five confidence columns are read from the single 2-D `signal_conf_ph` dataset
via the per-variable `column` selector (`{path: ..., column: k}` — the
variable analogue of the structured-filter `column`); the shared path is
still read once. The template declares the leaf ladder too —
`output.pyramid: {overviews: [16, 15, 14, 13]}`, the v4 store's tier
([issue #620](https://github.com/englacial/zagg/issues/620); the two digest
fields and `composition` ride it, see
[hive_layout.md](hive_layout.md#the-templates-declare-the-ladder-issue-620)) —
so a run from it chains the staged sweep in its tail by default.

## Why the presence floor

The [spec §3.2 quantization](specification.md#32-quantization-the-presence-floor)
floors any nonzero lane count at 1 so `lane > 0` means "this flag occurred"
**exactly, at every N**, through arbitrary merge chains — a rare surface flag
never rounds away. Count recovery is exact whenever `N <= 254`, which is the
entire below-compression-knee regime: measured 99.56% of non-empty cells on
the live NEON store, full-mission pooling. The per-surface lanes are
overlapping marginals (`surf_type` is multi-hot): they do not sum to 255, and
they cannot split the height distribution per surface (decision D: strata
stay signal/noise, never per-surface). The level lanes are **absolute** —
always `conf == 2/3/4`, never renumbered against the threshold — so a product
committing a higher threshold ships empty lower lanes rather than shifted
ones, and one lane layout serves every product.

## Merge law

`merge_composition(word_a, n_a, word_b, n_b)` folds lanes as the
digest-weighted mean, re-quantized with the same presence floor — symmetric,
and associative **up to the bounded re-quantization error** (fold order never
affects presence; it affects counts only within `O(n/510)`, so folds are not
byte-stable across orders) — whose `n` inputs come from the signal digests'
total weights. The law is normative in
[spec §3.4](specification.md#34-merge-law) (a reader may fold views, so it is
part of the store contract, not a zagg implementation detail).

## Operational caveat

Strata and composition run on the **pooled path and the spill path** — exact
via the pooled replay in the single-block regime, and folding across block
closes on overflow shards (issue #370): per-block stratum digests merge like
any digest (row selection precedes the build), and the composition words
collapse in one k-way weighted lane mean (presence exact via the floor,
counts within the documented quantization bound — the folded word is not
byte-stable against the single-block result). The streaming **merge** surface
still rejects them: its per-flush fold rebuilds digests from the raw source
column (no `where` state) and would re-quantize composition lanes on every
flush.

## What the mask channel gains

With the strata in place, the HHDC reader's occupancy mask (issue #265)
upgrades from 2-state to 3-state from store contents alone. State the rule
against `count`, which the template already writes:

- **unobserved** — `count == 0`: no observation fell in the cell.
- **observed with zero signal returns** — `count > 0` and the signal stratum
  is empty: the laser crossed the cell and every photon was background.
- **observed with signal** — the signal stratum is non-empty.

`count` rather than "both strata empty" because the strata are keyed to
*finite* heights: `build_tdigest_where` and `pack_composition` both drop
non-finite `source` rows (that is what makes the `N_signal` alignment exact),
so a cell whose every observation carried a non-finite height would report
`count > 0`, both strata empty, `composition == 0`. ATL03 `h_ph` is not
nullable, so the shipped template cannot produce that cell — but the `count`
form is the robust rule for any source and costs nothing to prefer.
