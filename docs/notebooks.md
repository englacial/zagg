# Notebooks

Every notebook under [`notebooks/`](https://github.com/englacial/zagg/tree/main/notebooks)
runs on [Binder](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks)
unless it says otherwise — no install, no credentials. The image is built from
`.binder/` (`zagg[analysis,catalog,viz]`), and a Binder-runnable notebook reads
only synthetic data, files in the git tree, anonymous NASA CMR-STAC granule
*metadata*, or the anonymous public
[source.coop](https://source.coop/englacial/zagg/benchmarks) store.

## The narrative series

One pipeline end to end, split into three notebooks, **each timing its own
stage** so end-to-end latency is tracked rather than guessed
([issue #328](https://github.com/englacial/zagg/issues/328), under
[#265](https://github.com/englacial/zagg/issues/265)). Timings come from the
shared [`zagg.notebook.StageTimer`][zagg.notebook.StageTimer], so all three
print the same table and `as_dict()` gives the machine-comparable form.

| Notebook | Leg | Stages timed |
| --- | --- | --- |
| [`01_query_shardmap.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/01_query_shardmap.ipynb) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/01_query_shardmap.ipynb) | query | CMR query · catalog build · shard assignment |
| [`02_dispatch_fleet.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/02_dispatch_fleet.ipynb) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/02_dispatch_fleet.ipynb) | write | dispatch wall · fleet completion · per-phase worker splits |
| [`03_read_tensors.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/03_read_tensors.ipynb) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/03_read_tensors.ipynb) | read | fetch · decode · vertical rasterize |

**1 — query.** CMR search → stac-geoparquet catalog → `ShardMap` over the NEON
SERC AOP box on the production o9/o19 HEALPix grid, then a drift check of the
rebuilt map against the pin in `tests/data/benchmark/targets.json`. Fully
anonymous: granule *metadata* search needs no Earthdata Login.

**2 — write.** The `zagg.client` facade: `Run.from_config(...)`
→ `dispatch()` → a `tqdm` bar over `as_completed` → `handle.status()`, with the
`read`/`index`/`aggregate`/`write` split the workers report summed across the
fleet. **Dual-mode**: the default `USE_REAL_FLEET = False` answers every invoke
from a stub Lambda client defined in the notebook, so the whole API surface —
futures, progress, `ShardError`, the post-run worker-invoke tail — runs
anonymously and at zero cost. Flipping the flag points the identical cells at a
deployed fleet. A stub run exercises the real client code paths but not the
worker, IAM, or S3: treat its timings as a shape, not a measurement.

**3 — read.** Decodes a t-digest product to the
`(tensor, mask, (offset, gain), morton_id)` reader contract (see
[Ragged store layout](ragged_layout.md#spatially-faithful-tensors-deinterleave-blocks-mask)),
doing fetch / decode / vertical-rasterize **by hand with public API** so each is
timed separately, then checking the hand-assembled tensor for exact equality
against `read_tensors`. Ends with bare-earth / canopy percentile surfaces and a
canopy-height model. **Dual-source**: `SOURCE = "synthetic"` (default) writes a
store to a temp directory with zagg's own write path — genuinely anonymous, and
scoreable against known ground truth; `SOURCE = "public"` points the same read
cells at a published hive product, one leaf at a time.

The split is the point: *fetch ≫ rasterize* means the transport is the wall
(the lever is concurrent leaf fetch), *rasterize ≫ fetch* means the per-cell CDF
loop is (the lever is vectorizing the read-side vertical binning). Both are open
follow-ups on [#265](https://github.com/englacial/zagg/issues/265).

## Reference notebooks

| Notebook | What it shows |
| --- | --- |
| [`custom_aggregations.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/custom_aggregations.ipynb) | The config-driven aggregation API on synthetic arrays |
| [`tdigest_reader_example.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/tdigest_reader_example.ipynb) | The rest of the reader API: `fit` policy, `block_order=`, `read_cell`, `read_raw_values` |
| [`shardmap_viewer.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/shardmap_viewer.ipynb) | Interactive shard outlines + granule footprints (polar-aware projection) |
| [`rasterized_zarr.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/rasterized_zarr.ipynb) | Rasterizing the published HEALPix store to an 8 km polar-stereo grid |
| [`sentinel2_fusion.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/sentinel2_fusion.ipynb) | Sentinel-2 ingest joined to ICESat-2 on a shared HEALPix grid |
| [`aoi_mask.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/aoi_mask.ipynb) | The strict-AOI per-cell mask (`output.aoi_mask`) |
| [`cost_reporting.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/cost_reporting.ipynb) | Max → estimated → actual invoke cost |
| [`jupyterhub_example.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/jupyterhub_example.ipynb) | Driving the API from a science hub |
| [`cryocloud_example.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/cryocloud_example.ipynb) | End-to-end ISMIP6 read + AWS Lambda fan-out — **not Binder-runnable** (needs live AWS + Earthdata credentials) |

## The stage timer

::: zagg.notebook.StageTimer
