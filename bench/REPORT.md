# Review of PR #17: generalized output grids

Combined design-doc review and empirical layout benchmark. Findings below
informed by reading the design doc, walking the existing pipeline
(`schema.py`, `processing.py`, `runner.py`, `catalog.py`,
`configs/atl06.yaml`, `docs/design/architecture.md`), and running the
benchmark fixtures in this directory.

Artifacts in this directory:
- [`layout_materialize.py`](layout_materialize.py) — write the bench
  fixtures (dense + fullsphere zarr stores at orders 8/10/12).
- [`layout_access.ipynb`](layout_access.ipynb) — xarray + dask access
  benchmark (open, xdggs decode, isin, `.values`, mean).
- [`layout_access_numpy.ipynb`](layout_access_numpy.ipynb) — pure
  numpy access benchmark, includes the `fast_read_sparse` and
  `fast_sel_at` recipes.

## TL;DR

1. The doc's biggest open question — array layout — has a more nuanced
   answer than first appeared. Pre-fix, dense pack wins by **19-24× at
   order 12**. With a small fast-read recipe (LIST + parallel
   `get_block_selection`, validated empirically), the gap collapses to
   **1.2× for I/O**. See §1.7. The remaining 20-80× cost on naive
   numpy `.values` workflows is structural (full-sphere arrays are 38×
   bigger), but sub-second in absolute terms.
2. The fast-read recipe is a **clean upstream win for everyone**: 3×
   speedup even on fully-populated dense arrays. Worth filing as a
   zarr-python issue + PR (§6.1).
3. The protocol abstraction in §4-5 is mostly right but conflates two
   distinct integers (spatial identity vs storage block index) into a
   single `shard_of` method. That should split (§2).
4. xdggs's default decoder doesn't read zagg's metadata, but xdggs's
   zarr-convention decoder does — passing `convention="zarr"` works.
   Worth documenting this kwarg in zagg's examples; one upstream xdggs
   bug worth filing (§3.6).
5. Five smaller substantive issues, listed in §3 below.

The revised recommendation: ship `zagg.fast_read()` as a public utility
*now* to close the I/O gap, file the upstream zarr PR, and let the
layout decision shift from "dense wins on perf" to a workflow-driven
choice between dense (smaller arrays, faster naive numpy) and full
sphere (cleaner protocol, append-friendly, harmless to indexed access).

## 1. Empirical layout benchmark

### 1.1 Setup

- [`layout_materialize.py`](layout_materialize.py) — synthesizes ~1300
  morton parents biased to southern HEALPix base cells (8-11), writes both
  dense-pack and full-sphere zarr stores at three child orders (8, 10, 12).
- [`layout_access.ipynb`](layout_access.ipynb) — xarray + dask access
  patterns (open, decode, isin small subset, materialize coord, full
  reduction).
- [`layout_access_numpy.ipynb`](layout_access_numpy.ipynb) — same access
  patterns, pure numpy via `zarr.open_group` + eager `arr[:]`. Bypasses
  xarray entirely.
- Stores: `s3://xagg/bench-layout/{dense,full}_p{P}_c{C}.zarr` (us-west-2).
- Versions: `xarray 2026.4.0`, `xdggs 0.6.0`, `zarr 3.1.5`, `dask 2026.3.0`.

### 1.2 Write-side result

Both layouts wrote in identical wall time at every order, because zarr is
sparse-on-write — only populated chunks generate PUTs.

| layout | order | shape | chunks total | populated | write |
|---|---|---|---|---|---|
| dense | 8 | 262K | 64 | 64 | 1.7 s |
| fullsphere | 8 | 786K | 192 | 64 | 1.6 s |
| dense | 10 | 4.2 M | 1,024 | 1,024 | 21.5 s |
| fullsphere | 10 | 12.5 M | 3,072 | 1,024 | 22.0 s |
| dense | 12 | 5.3 M | 1,300 | 1,300 | 27.5 s |
| fullsphere | 12 | **201 M** | **49,152** | 1,300 | **27.5 s** |

(Same data, same number of S3 PUTs, just different chunk indices.)

### 1.3 Read-side result (order 12, the regime that matters)

This is where the layouts diverge sharply.

**Dask backend (xarray + dask):**

| step | dense | fullsphere | ratio |
|---|---|---|---|
| open_zarr | 0.22 s | 0.28 s | 1.3× |
| sel_small (64 cells, isin) | 7.4 s | 177 s | **24×** |
| cell_ids.values | 5.7 s | 140 s | **24×** |
| h_mean.mean | 6.9 s | 148 s | **22×** |

RSS for `cell_ids.values`: dense **+41 MB**, fullsphere **+3.0 GB**.

**Pure numpy backend (zarr.open_group + arr[:]):**

| step | dense | fullsphere | ratio |
|---|---|---|---|
| open_group | 0.16 s | 0.17 s | 1.0× |
| read_cell_ids | 9.1 s | 172 s | **19×** |
| read_h_mean | 9.0 s | 170 s | **19×** |
| isin_64cells (in-memory) | 23 ms | 2.4 s | 100× |
| h_mean.mean (in-memory) | 33 ms | 0.9 s | 28× |

RSS for `read_cell_ids`: dense **+41 MB**, fullsphere **+1.6 GB** (= 201 M
× 8 bytes; matches bare data size).

### 1.4 Where the time goes

The numpy comparison is informative:

- The ~1.4 GB RSS gap between dask (3.0 GB) and numpy (1.6 GB) at
  fullsphere order 12 is dask's task-graph carrying state for 49k tasks.
  Confirmed graph overhead.
- Wall time barely improved going from dask to numpy (140 s → 172 s).
  So most of the 170 s is **not** dask-side. It is zarr's per-chunk
  Python iteration, even on chunks that resolve to fill_value with no I/O.

Best-fit decomposition for the order-12 fullsphere `read_cell_ids` (172 s):

| component | estimate |
|---|---|
| 1.6 GB buffer alloc + zero-init | ~2 s |
| 1,300 populated chunks via parallel GET (concurrency 128) | ~5 s |
| 49,152 empty-chunk Python iterations × ~3 ms each | **~150 s** |

The 150 s figure is hypothesis, not a measurement — it would need a
profiler run to confirm. But the numbers are consistent: dense (1,300
chunks) takes ~9 s, fullsphere (49,152 chunks) takes ~172 s, ratio matches
chunk-count ratio (38×) more than data-volume ratio (38×) or populated-
count ratio (1×).

### 1.5 Two upstream fixes implied — one is now empirically validated

Two distinct fixes are implied:

**A. zarr empty-chunk skip.** When `arr[:]` is called, do a single bulk
LIST on the chunk prefix; iterate only populated chunks; fill the rest
of the output buffer with one broadcast of fill_value.
- Impact: addresses the dominant ~150 s zarr-side iteration cost.
- **Status: validated empirically as `fast_read_sparse` (§1.7).** 64×
  speedup at order-12 fullsphere; 3× even on dense layouts.
- Difficulty: moderate. Touches zarr's core read path; needs careful
  handling of partial reads, sharding, codecs. Realistic 3-6 months
  from PR to release in zarr-python.

**B. dask graph-pruning.** Once zarr exposes populated chunks (built
into A or as a separate API), `dask.array.from_zarr` could use it to
prune the task graph at construction time.
- Impact: removes the ~1.4 GB graph RSS overhead seen at order-12
  fullsphere in dask.
- Doesn't help users who go through pure numpy (`fast_read_sparse`
  already addresses that path).
- Difficulty: moderate. Standalone but depends on A.

### 1.6 Layout verdict (revised after fast-read validation)

| concern | dense pack | full sphere |
|---|---|---|
| Read I/O (today, zarr 3.1.5) | 6.9 s | 173 s — **24× penalty** |
| Read I/O (post-fix, fast_read recipe) | 2.3 s | 2.7 s — **1.2× parity** |
| Buffer RAM after read | 41 MB | 1.6 GB |
| In-memory `np.isin` (64 cells) | 30 ms | 2.3 s |
| In-memory `np.nanmean` over data | 40 ms | 0.8 s |
| Indexed `.sel(cell_ids=...)` | needs `{cell_id → pos}` map | pure arithmetic, no state |
| Indexed `.sel(morton=...)` | catalog map + `mort2healpix` | `mort2healpix` only |
| Protocol shape | sparse → dense remap needed (§2) | morton ID = block index |
| Append a new region | array reshape | pure write |
| Re-emit cell_ids coord on open | needed (else lose `.sel` map) | optional (algorithmic) |

**Pre-fix, dense pack wins decisively** on read I/O alone.

**Post-fix, the choice depends on workflow** — and the protocol
asymmetry on `.sel` deserves more weight than I gave it earlier.

For naive `.values` numpy workflows: dense wins by 20-80× on in-
memory ops (sub-second in absolute terms at order 12).

For `.sel(cell_ids=...)` workflows (the canonical xdggs idiom):
- **Full sphere**: `position = cell_id`. Pure modular arithmetic,
  no auxiliary state. `chunk_idx = cell_id // chunk_size`,
  `within = cell_id % chunk_size`. xdggs `HealpixIndex` works
  natively with no layout-specific extension. A morton-indexed
  caller gets the same: `mort2healpix(morton)` → position →
  arithmetic. One stateless wrapper, ~2 lines.
- **Dense pack**: requires a `{cell_id → position}` map (or its
  inverse, `cell_ids` array as stored coord). The map is N-entry
  state that must be either: (a) read from the cell_ids zarr
  array on every open (re-paying the read cost), (b) cached by
  the consumer, or (c) embedded in zarr metadata. xdggs would
  need a dense-pack-aware variant or a coord-based lookup path.

Dense pack's `.sel` cost isn't the lookup itself — it's the **state
management overhead** that gets pushed onto every consumer of zagg
outputs. Full sphere makes the protocol stateless.

This is a real point against dense pack that the empirical I/O
benchmark didn't surface. With it weighted in, the layout decision
is genuinely close:

- **Dense pack:** wins on RAM and naive-numpy speed. Loses on
  protocol cleanliness and `.sel` ergonomics.
- **Full sphere:** wins on protocol cleanliness, `.sel` simplicity,
  and append-friendliness. Loses on RAM and naive numpy. Requires
  the upstream zarr fix (6.1) to be I/O-competitive.

The pragmatic choice today, given the upstream fix isn't yet
landed: **stay on dense pack as the default and ship
`zagg.fast_read()` as a public utility**, but acknowledge that the
case for migrating to full sphere is stronger than I initially
argued, and revisit after 6.1 (zarr) and 6.2 (xdggs MOC-from-
chunks) land. If both PRs ship, full sphere becomes the cleaner
default.

### 1.7 Fast-read recipe — empirical validation of the upstream fix

[`layout_access_numpy.ipynb`](layout_access_numpy.ipynb) now includes
`fast_read_sparse(arr)`.
The recipe, ~30 lines:

```python
def fast_read_sparse(arr):
    # 1. LIST populated chunk keys (one paginated S3 LIST)
    keys = sync(_collect(arr.store.list_prefix(f"{arr.path}/c/")))
    populated_idx = sorted(int(k.rsplit("/", 1)[-1]) for k in keys)

    # 2. Allocate output, pre-fill with fill_value (one numpy op)
    out = np.full(arr.shape, arr.fill_value, dtype=arr.dtype)

    # 3. Parallel-read populated chunks via get_block_selection
    with ThreadPoolExecutor(max_workers=128) as ex:
        for idx, data in ex.map(read_one, populated_idx):
            out[idx*chunk:(idx+1)*chunk] = data
    return out
```

**Order 12 results, both layouts:**

| layout | `arr[:]` (slow path) | `fast_read_sparse` | speedup |
|---|---|---|---|
| dense | 6.94 s | 2.30 s | 3.0× |
| fullsphere | **173.77 s** | **2.73 s** | **63.7×** |

Two findings:

- **Layouts now at parity for read I/O.** The empty-chunk problem
  vanishes once we list populated chunks before reading.
- **Dense layouts also benefit, ~3×.** The slow path's per-chunk
  Python loop dominates even with no empty chunks. The fast path
  uses 128-way parallelism that `arr[:]` doesn't match internally.

This is a **clean upstream win for everyone**, not just sparse-grid
users. Worth filing as a zarr-python issue + PR with this recipe as
the proof-of-concept.

In-memory operations on the loaded array still scale with array
size, not populated count:

| step (order 12, fullsphere) | dense | fullsphere | ratio |
|---|---|---|---|
| `np.isin` over coord (64 cells) | 30 ms | 2,337 ms | 78× |
| `np.nanmean` over data | 40 ms | 835 ms | 21× |

These are pure numpy traversals over arrays that are 38× larger
for fullsphere (200 M elements vs 5.3 M). Structural cost, not
patchable with this recipe — only relevant for naive workflows
that materialize the whole coord/data.

## 2. The protocol gap that matters most: shard_of vs block_index

The doc's §5.5 contract has every grid's `shard_of(cell_ids) → ShardKey`
return the natural spatial identifier — for HealpixGrid the morton parent
ID, for RectilinearGrid a `(row_block, col_block)` tuple, etc.

For 2D rectilinear that's simultaneously the *block index* zarr's
`set_block_selection` wants. For 1D HEALPix it isn't:

- Morton parent IDs are sparse — populated cells in [0, 196608) for
  Antarctic order-6 coverage are ~1300 specific values.
- The Zarr array has 1,300 chunks indexed densely 0..1299.
- Today's pipeline (`runner.py:218`) builds the morton-ID → dense-index
  map via `cell_to_idx = {cell: idx for idx, cell in enumerate(all_cells)}`
  and uses *that* as `chunk_idx` in `set_block_selection`.

So the protocol's `shard_of` returning the morton ID would not be a
drop-in for `set_block_selection`. Either:

- `shard_of` returns the dense block index, in which case the grid is
  stateful (holds the populated-chunk list), and the doc's "stateless
  instances" claim in §4 doesn't hold.
- A second method `block_index(shard_key) → tuple[int, ...]` translates
  spatial identity to storage position. Recommended — keeps `shard_of`
  pure.

This is also why §5.7's chunk-alignment test wouldn't catch a bug here —
that test compares `shard_size` to `chunk_shape_from_template`. Both are
sizes. Two `shard_of` implementations with different block-index semantics
can both pass that test and silently corrupt outputs.

**Recommended addition to the protocol:**

```python
def shard_of(self, cell_ids) -> ShardKey:
    """Spatial identity. Stable across sessions. Hashable."""

def block_index(self, shard_key: ShardKey) -> tuple[int, ...]:
    """Storage position of this shard in the array. Depends on grid
    state populated at emit_template time (e.g., the populated-base-cell
    set, or the catalog's parent-cell ordering)."""
```

Add a Phase-0 test that uses `block_index` to compute chunk addresses
across a few mock cell sets, and verify round-trip consistency.

### 2.1 Layout-dependence of this gap

This protocol gap is layout-specific. With **dense pack** (today's
layout), `shard_of` and `block_index` are genuinely different and
need separate methods. With **full sphere**, `block_index` collapses
to the identity function:

```python
class HealpixGrid:
    def shard_of(self, cell_ids):
        return cell_ids[0] // (4 ** self.delta)  # parent morton

    def block_index(self, shard_key):
        if self.layout == "dense":
            return (self._catalog_position[shard_key],)
        elif self.layout == "fullsphere":
            return (shard_key,)  # identity
```

This is one of the cleaner arguments for full sphere: the protocol
becomes uniformly stateless across grid types. Rectilinear's
`block_index` is already arithmetic (chunk position in the projected
plane). HEALPix-on-fullsphere matches that pattern. HEALPix-on-dense
is the odd one out, requiring stateful per-grid translation.

The benchmark §1.6 covers the perf trade-offs; this is the
protocol-cleanliness side of the same decision.

## 3. Other substantive issues

### 3.1 Template shape claim

The doc's §6 table says HealpixGrid shape is `(12 · 4^child_order,)`.
Actual code (`schema.py:144-152`) truncates to `(4^Δ · n_parent_cells,)`
when `n_parent_cells` is provided, which it always is in `runner.py:213`.
Either the table is wrong, or the regression test in §8 will fail. Pin the
contract explicitly.

### 3.2 Byte-identity fragility for Phase 0

The Phase 0 success criterion is "byte-identical HEALPix output vs. a
golden store." Today's chunk layout is densely indexed by `dict.keys()`
iteration order from the catalog file (`json.load` preserves insertion
order). Any reorder — different morton sort key, different
cell-discovery iteration, parallel-build nondeterminism — bit-drifts the
output. Phase 0 needs an explicit canonical sort (probably ascending
morton parent ID) for the catalog enumerate-order, otherwise the test is
a flake generator.

### 3.3 PolygonZoneGrid statelessness

§4 says "stateless interface, safe to pickle across Lambda boundaries."
A polygon-zone grid carries zone geometries (potentially MB) and a
precomputed centroid index — not stateless. More importantly, Lambda
invocations cross a *JSON* boundary, not a pickle boundary
(`runner.py:300, 410` use `json.dumps(event)`). There's a real design
question deferred: zones URI in event + worker fetch on cold-start, vs.
baked into Lambda layer, vs. /tmp pre-distribution. Worth a half-page in
§5.4.

### 3.4 H3 pentagon padding "≤0.1%" unjustified

Pentagon parents have 6 children where hexagons have 7. At Δ=3, a
pentagon parent has 6·7² = 294 children vs hexagon's 7³ = 343 — pentagon
chunks waste ~14% of *their* cells. Twelve pentagons × small fraction of
total cells gives a small global number, but the "≤0.1%" claim needs a
calculation parameterized by Δ.

### 3.5 Reprojection densification

§5.6 calls out densification for `RectilinearGrid.shard_footprint` only.
But the existing code already does this for HEALPix in `catalog.py:402`
(`mort2polygon(cell_id, step=32)`). Densification is a cross-grid
concern with a shared pattern, not a rectilinear-specific gotcha.

### 3.6 xdggs convention defaults (surfaced by the bench, smaller than first thought)

When the bench notebook tries bare `xdggs.decode(ds)` on a zagg output,
it fails:

```
KeyError: 'grid_name'
  at xdggs/conventions/xdggs.py:48 in Xdggs.decode
```

Initial reaction: a metadata format incompatibility. Actual diagnosis: a
convention-default mismatch. xdggs has three registered conventions —
`"xdggs"` (default), `"cf"`, `"zarr"`. They look for metadata in
different places:

| convention | metadata location | key for grid name |
|---|---|---|
| `"xdggs"` (default) | `cell_ids` variable's attrs | `grid_name` |
| `"cf"` | a `grid_mapping` variable | `grid_mapping_name` |
| `"zarr"` | `ds.attrs["dggs"]` | `name` |

Zagg writes the **zarr** convention (zarr-conventions/dggs v1; the
`schema_url` and `uuid` in `schema.py:86-90` match xdggs's `Zarr`
convention exactly). `xdggs.decode(ds)` without a `convention=` kwarg
falls through to the default `"xdggs"` convention, which can't find
`grid_name` because zagg writes the zarr-style location.

**The actual fix is a kwarg, not a rewrite of metadata:**

```python
decoded = xdggs.decode(ds, convention="zarr", name="cell_ids")
```

Tested against the bench store at order 8 — works, produces a
`HealpixInfo(level=8, indexing_scheme='nested', ...)` with a working
`HealpixIndex` (algorithmic, lazy-coord, no materialization).

**One small upstream xdggs bug:** `xdggs/conventions/zarr.py:128` uses
the `name` parameter as the dict key when constructing the index. If
`name` is None (the default), this becomes `{None: var}`, which later
trips xarray with `TypeError: keywords must be strings`. Should fall
back to `coordinate` from metadata. One-line PR.

**Action for zagg:** document the convention/name kwargs in the
notebook examples / README. The design doc's §6 and §9 references to
xdggs are correct in spirit (zagg outputs are xdggs-compatible via the
zarr convention) but worth specifying explicitly.

### 3.7 Consolidated metadata not discussed

The doc's §3 stage table mentions `zarr.consolidate_metadata` as a
"generic" step but doesn't discuss it. Worth a sub-section in §6:

- It's a zarr-python extension, not in the Zarr v3 spec — non-Python
  readers fall back to per-array `zarr.json` GETs and lose the cold-open
  perf win.
- Tension with D13's "cross-language portability" framing.
- Deterministic in zarr-python (sorted keys, no timestamps), so byte-
  identity for Phase 0 is preserved.

## 4. Minor / factual

- Doc references `configs/atl06.yaml`; actual path is
  `src/zagg/configs/atl06.yaml`. There are also eight yearly
  `sara_*_atl06.yaml` siblings in the same directory.
- Config field is `output.grid.type` (nested), not `output_grid.type`
  (top-level) as shown in §4. The existing config already carries
  `output.grid.type: healpix` — backward compat is *cleaner* than the
  doc claims.
- `architecture.md:10` mentions OPR alongside ATL06 as point inputs. If
  OPR isn't h5coro-readable, the v1 input restriction (D1) bites sooner
  than the doc implies.

## 5. Recommendations for PR #17

In rough priority order.

### 5.1 Pick a layout, explicitly (and ship a fast-read utility)

Add a §5.x to the design doc with the empirical layout choice:

> **Layout decision: dense pack as default, fast-read utility for
> users who hit the slow path.** The HealpixGrid array shape is
> `(4^Δ · n_parent_cells,)`, with one chunk per populated parent
> cell indexed by catalog enumerate-order.
>
> Empirical benchmarking (this report) showed full-sphere
> layout costs 19-24× more on every coord-touching operation at
> order 12 *with stock zarr-python 3.1.5*. A 30-line fast-read
> recipe (LIST populated chunks + parallel `get_block_selection`)
> closes the gap to 1.2× and is shipped as `zagg.fast_read()`.
> The same recipe gives 3× speedup on dense layouts too.
>
> Layout decision is reversible: the protocol's `block_index`
> method (§2) makes the dense remap explicit; switching to full
> sphere later means setting `block_index = identity` for HEALPix
> and re-emitting templates.

### 5.2 Split `shard_of` from `block_index`

Two protocol methods, contract documented in §5.5. Add a Phase-0 round-
trip test. (See §2 above.)

### 5.3 Document the xdggs decode kwargs

Update the notebook examples and README to call:

```python
ds = xr.open_zarr(...)
decoded = xdggs.decode(ds, convention="zarr", name="cell_ids")
```

File the upstream xdggs bug (`Zarr.decode` should default `name` to
`coordinate` from metadata). (See §3.6.)

### 5.4 Pin Phase 0 byte-identity preconditions

Make the canonical sort order for catalog enumeration explicit
(ascending morton parent at the parent_order). Add an assert in
`build_catalog` and a test. (See §3.2.)

### 5.5 Polygon-zone state model

Document the worker-side delivery mechanism for zone polygons in §5.4
even if not finalized: URI in event + cold-start fetch, vs. Lambda
layer, vs. pre-warmed /tmp. (See §3.3.)

### 5.6 Other doc fixes

- Correct template shape claim in §6 table.
- Justify or revise H3 pentagon padding number.
- Generalize the densification note in §5.6.
- Add a consolidated-metadata sub-section in §6.
- Fix paths and config field names per §4.

## 6. Upstream contributions worth filing

Ordered by leverage (estimated wall-time + RAM impact at order-12 full
sphere) to inform sequencing.

### 6.1 zarr-python: empty-chunk-aware read path (validated, highest leverage)

The `fast_read_sparse` recipe (LIST populated chunks + parallel
`get_block_selection`) delivers:

- **64× speedup** for sparse-grid arrays at order 12 (§1.7).
- **3× speedup** for fully-populated dense arrays at order 12 (the
  slow path doesn't fully parallelize even when no chunks are
  empty).
- Reduces dask graph state implicitly (no per-empty-chunk task).

Empirical proof-of-concept in
[`layout_access_numpy.ipynb`](layout_access_numpy.ipynb), ~30 lines. Worth filing as a zarr-python issue with the bench data
and a draft PR.

Production-shippable today: bundle into zagg as `zagg.fast_read()`
for users who want the speedup without waiting for upstream. The
internal API uses `arr.store.list_prefix` and
`arr.get_block_selection`, both stable in zarr-python 3.1+.

### 6.2 xdggs: build HealpixIndex from populated chunks (highest RAM leverage)

The dask notebook showed `xdggs.decode` itself taking 136 s and
**+3.1 GB RSS** at order-12 fullsphere. The `HealpixIndex`
constructor materializes the full `cell_ids` array (1.6 GB) and
feeds it to `healpix_geo.nested.RangeMOCIndex.from_arrays`, which
allocates working memory proportional to the input.

For HEALPix nested layouts where each populated chunk is a
contiguous range of sequential cell IDs (which is how zagg writes
them, by construction of `mort2healpix`), the MOC for the populated
set is just the union of those chunk ranges:

```python
# ~10 KB of state, no array materialization needed:
moc = MOC([(parent * 4**delta, (parent + 1) * 4**delta)
           for parent in populated_parents])
```

Building the MOC this way eliminates *both* the 1.6 GB cell_ids
allocation AND the ~1.5 GB MOC working memory — total estimated
RSS savings ~3 GB at order 12.

Implementation: `xdggs/healpix.py` `HealpixIndex.from_variables`
(or a new `from_zarr_array` classmethod) detects when the source is
a zarr array, calls 6.1's populated-chunk listing, and builds the
MOC range-by-range. ~50-100 lines. Depends on 6.1 being available
upstream or via `zagg.fast_read()`.

This is the second-highest leverage upstream change after 6.1, and
specifically what unblocks fullsphere from being viable on the
xdggs path.

### 6.3 xdggs: zarr-convention name fallback (trivial, file alongside 6.2)

`xdggs/conventions/zarr.py:128` should fall back from `name` to
`coordinate` when constructing the index dict. One-line PR. (See
§3.6.) Worth bundling with 6.2 as the same upstream conversation.

### 6.4 dask + zarr: graph pruning via populated-chunk list

If 6.1 lands, `dask.array.from_zarr` could use the populated-chunk
list to prune the task graph at construction time.

- Removes the ~1.4 GB graph RSS overhead seen at order-12
  fullsphere in dask.
- Doesn't help users on the pure-numpy or `zagg.fast_read()` path.
- Less urgent than 6.1 / 6.2.

Would primarily help users doing chunk-aware reductions on sparse
arrays (e.g., `.mean()` via dask).

### 6.5 Suggested filing order

1. **6.1** to `zarr-developers/zarr-python`, with the bench notebook
   linked. Get core team buy-in on the read-path optimization. *This
   unblocks 6.2 and 6.4.*
2. **6.3** to `xarray-contrib/xdggs` (the trivial fix). Quick win,
   builds rapport before the larger 6.2 ask.
3. **6.2** to `xarray-contrib/xdggs`, possibly `healpix-geo` too.
   Larger refactor, ideally after 6.1 has landed so xdggs can build
   on the new zarr API.
4. **6.4** to `dask/dask`. Smaller standalone benefit; can wait.

Combined post-fix outlook for fullsphere order-12 cell_ids access:

| step | today | post-6.1 | post-6.1+6.2 |
|---|---|---|---|
| `arr[:]` (zarr/numpy) | 173 s, 1.6 GB | ~3 s, 1.6 GB | ~3 s, 1.6 GB |
| `xdggs.decode` | 136 s, 3.1 GB | ~3 s, 3.1 GB | ~50 ms, ~10 KB |
| `dask cell_ids.values` | 140 s, 3.0 GB | ~5 s, 1.6 GB | (same) |

Without 6.2 specifically, fullsphere stays expensive on the xdggs
path even after 6.1. With both, fullsphere is competitive across
the entire stack.

---

## Appendix: full benchmark numbers

### Wall time (seconds), all orders

| order | step | dense (dask) | full (dask) | dense (numpy) | full (numpy) |
|---|---|---|---|---|---|
| 8 | open | 0.18 | 0.22 | 0.16 | 0.16 |
| 8 | read coord | 0.32 | 0.66 | 0.51 | 1.08 |
| 8 | reduce mean | 0.36 | 0.77 | 0.001 | 0.007 |
| 10 | open | 0.20 | 0.21 | 0.20 | 0.14 |
| 10 | read coord | 4.43 | 9.81 | 7.26 | 14.04 |
| 10 | reduce mean | 5.35 | 11.31 | 0.04 | 0.07 |
| 12 | open | 0.22 | 0.28 | 0.16 | 0.17 |
| 12 | read coord | 5.70 | 139.83 | 9.14 | 171.79 |
| 12 | reduce mean | 6.86 | 148.34 | 0.03 | 0.91 |

### RSS delta (MB) for read-coord step

| order | dense (dask) | full (dask) | dense (numpy) | full (numpy) |
|---|---|---|---|---|
| 8 | 2.2 | 5.5 | 3.3 | 6.7 |
| 10 | 63.7 | 160.9 | 35.4 | 101.3 |
| 12 | 40.8 | **3030.7** | 40.9 | **1577.5** |

### Storage in bench fixtures

Confirmed by `aws s3 ls s3://xagg/bench-layout/ --recursive --summarize`:
both layouts have identical populated-chunk counts (1300 at order 12),
and unwritten chunks consume zero bytes on disk.
