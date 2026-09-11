# Upgrading a published store from `/1` to `/2`

A store built before its aggregation fields were declared **composable** has
no [leaf columns](specification.md#46-leaf-column-artifacts-zagg-column1): the
gen-1 partials the `zagg-pyramid/2` staged sweep folds are written by the leaf
worker at aggregation time, and only when the writing run's declaration admits
the fields. Such a store cannot take the `/2` staged sweep at any transport.

This page is the runbook that upgrades one **without re-aggregation**. The
cost is one leaf-reading pass — the same order as one `/1` cascade build, paid
once — because every input the column fold ever had is still sitting in the
leaf.

## Preconditions

- **The store is quiesced.** No aggregation run may be writing the same
  `(leaf, window)` while the backfill runs. The backfill is the one sweep-side
  writer of a leaf artifact, so it is the one pass for which the spec's
  fleet ∥ sweep disjointness does not hold (§4.6). The sweep-admission lease
  (§4.8) serializes it against other *sweeps*; nothing can serialize it
  against a fleet — proving quiescence would need a global run registry zagg
  deliberately does not have.

  As a smoke alarm the pass lists the store's status channel
  (`{store}.status/run-*/`, the per-run objects the event transport writes) and
  logs a loud **WARNING** naming any run with activity in the last 900 s — one
  Lambda worker wall, so an older object cannot belong to a unit still running.
  It never refuses and never fails the pass, and it is fail-open: a status
  channel that is absent, unreadable, or scoped away is silent. **Silence is
  not proof.** A store written by a pre-#327 dispatcher, or one whose status
  prefix was lifecycled away, looks exactly like a quiesced one — the
  precondition is still yours to confirm.
- **You have the config the store was built with.** `declare_pyramid` refuses
  a config whose `semantic_hash` the store's frozen one denies. `output.*` is
  not in the semantic core, so adding or changing `output.pyramid` on the
  original config hashes identically.
- The store's leaves are committed and its run records are at the product root
  (they are what discovery reads — never a recursive LIST).

## The four steps

### 1. Re-declare the manifest to `zagg-pyramid/2`

```bash
python -m zagg.sweep s3://bucket/prefix/product \
    --declare-pyramid config.yaml \
    --overviews 13
```

`--overviews` takes the **leaf cell resolutions**, finest first, and is the
lever this upgrade needs: without it the grid-less retrofit path can only
declare `/1`, because the issue #384 default flip fires off the *grid's*
resolved `chunk_order` and `declare_pyramid` has no grid to ask. The
resolutions are validated against the **manifest's own** `shard_order` and
`cell_order` — the store's truth wins over the config's — and nothing is
written if they do not fit.

The API form takes either lever:

```python
from zagg.sweep_overview import declare_pyramid

declare_pyramid(root, config, overviews=13)      # spell the schedule outright
declare_pyramid(root, config, chunk_order=13)    # or complete the #384 default
```

They are mutually exclusive (`chunk_order=` is inert once a schedule is
spelled, so passing both refuses rather than silently ignoring one), and
`chunk_order=` refuses a config that declares `output.pyramid: false` — there
is no pyramid for it to default. `overviews=` *does* override a
`pyramid: false` config, loudly: that is the point of the lever on a store
whose build declared no pyramid.

Check the summary before moving on. `declared_via` names the lever,
`overviews` is the expanded `(node, cells)` ladder the manifest now records,
and `fields` is the per-field D24 class map — **if every entry there is
`none`, stop**: the backfill will refuse, and the fix is the classifiers in
the config, not the store.

### 2. Backfill the leaf columns

```bash
python -m zagg.sweep s3://bucket/prefix/product --families columns
```

`columns` is a registered sweep family, so it needs no new transport and no
new mode: the work-set normalization and discovery (the same run-record scan,
never a recursive LIST) are inherited whole. It is deliberately **not** in the
default family set — a backfill is an explicit upgrade of a quiesced store,
never a routine rollup's side effect, so it must be spelled.

**Three entry points, all the same pass.** This CLI,
`run_sweep(root, leaves, families=["columns"])`, and a fleet `mode: "sweep"`
invoke naming the family — the handler's sweep arm forwards the event's
`families` and `partition` blocks as of
[#528](https://github.com/englacial/zagg/pull/528). A fleet-scale
**partitioned** backfill is the one combination still gated: the runner fires
partitions as concurrent `Event` invokes and the sweep lease is store-granular,
so it admits exactly one of them. Until that is ruled, fan a fleet backfill out
as a single unpartitioned invoke, or partition it sequentially in-process.

**`--partitions 2^n` bounds peak memory, but is not free here.** Unlike
`--stages`, which sweeps every partition under ONE lease, `--partitions`
gives this family one `run_sweep` per partition and the lease is acquired and
released *per partition* — so a `--partitions 64` pass leaves 63 unleased
windows in the middle of the very pass §4.6 sanctions as the second writer of
a leaf artifact. On a genuinely quiesced store (the precondition above) that
is harmless, since nothing else is admitted anyway; if you cannot guarantee
quiescence, run the backfill unpartitioned so the whole pass sits under a
single lease.

Per `(leaf, window)` the pass reads the leaf's commit stamp, skips the leaf if
its column is already current, and otherwise recomputes the column from the
leaf's stored arrays and writes it wholesale under the leaf's node prefix. The
summary counts `written` / `current` / `empty` (no committed leaf) / `failed`.
It is idempotent: a second pass writes nothing.

**`failed` must be 0 before step 3.** A leaf that cannot be read or folded is
counted and skipped rather than aborting the pass, so a partial result comes
back as an ordinary summary (with a `logger.error`) — and the staged sweep
would then fold a ladder over whatever columns happened to land, recording the
gaps only as `source_children` under-coverage in artifacts nobody reads. Check
the count, fix the leaves it names in the log, and re-run the backfill; it is
idempotent, so the second pass touches only what is missing. A pass that
manages to write NOTHING at all does not return a summary: it raises, naming
the last error, because every leaf failing the same way is a store-wide fault
(expired credentials, a denied column prefix, an outage), not N leaf faults.

The gate is the **manifest's** declaration, and every refusal names itself:

| Manifest says | What happens |
| --- | --- |
| `zagg-pyramid/1` (an `orders`/`spacing` schedule) | refused — re-declare (step 1) |
| `orders: []` (declared off) | refused — re-declare (step 1) |
| `/2`, but every field `class: "none"` | refused — fix the classifiers, re-declare |
| `/2` with composable fields | backfilled |

### 3. Build the ladder with the staged sweep

```bash
python -m zagg.sweep s3://bucket/prefix/product --stages
```

Nothing about this step is upgrade-specific — it is the ordinary `/2` staged
sweep, and it now finds the columns it needs. **Only run it once step 2
reported `failed: 0`**: this step reads the columns, and it cannot tell a leaf
whose column the backfill never wrote from one whose declaration carries none. The resulting ladder is
byte-identical to the one a store built pyramid-ON from the same inputs
produces (the acceptance test in `tests/test_column_backfill.py`; the only
differences anywhere are the sweep run's own `run_id` and the wall clocks).

### 4. Retire the `/1` ancestors

The `/1` overview zarrs the old declaration left behind are now **declared-off
regenerable debris** (D24 option A). `declare_pyramid` deliberately preserves
the `materialized` actuals across the re-declaration, so the manifest still
inventories what is on disk. Deleting them is optional and reversible — they
are cache, never truth — and there is no automated arm for it yet.

## What the upgrade does not change

- **No byte of the store format.** A backfilled column is byte-identical to
  the one the leaf's worker would have written from the same leaf, the two
  provenance timestamps aside. `zagg-column/1` is unchanged.
- **No leaf is rewritten.** The backfill reads leaves and writes columns.
- **No re-aggregation.** Nothing is re-read from the source granules.

## Residual: the same-second skip key

A column records no `generation` block and no `run_id` for its source leaf, so
the backfill's skip-if-current test keys on what the artifacts already carry:
the recorded declaration (node/cell orders, group set, per-field provenance),
the column's **realized structure** — every group's members and each member's
dtype, fill value and attrs, compared against the template the run would write
— then `written_at` ordering and the column stamp's copy of the **leaf's**
`granule_count`. The structure term is what catches the two drifts the
recorded `zagg_column` grammar cannot carry, both of which a re-declaration
really can move: a companion channel added or dropped (`location` /
`temporal`, neither recorded), and an exact field's `dtype` / `fill_value`. Both stamps resolve to whole seconds, so a leaf rewritten in
the same second as its column, at an unchanged granule count, reads as
current. On a quiesced store — the precondition above — that window is not
reachable; `backfill_columns(..., force=True)` is the unconditional rewrite if
you ever need it.
