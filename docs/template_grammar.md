# Template grammar

*Generated from the pydantic models in `zagg.config` by
`tools/render_schema_docs.py` (issue #677) — do not edit by hand. Export the
machine-readable form with `python -m zagg.config --schema [--block <name>]`,
which also annotates the capability-named fields with the names registered in
the running environment (`x-zagg-registered`); this page is rendered without
those hints so it does not depend on which plugins are installed.*

A template is one of three config kinds, picked the way `validate_config`
branches: `pipeline.type: temporal|event` selects the temporal grammar,
`data_source.reader: raster` the raster grammar, anything else the spatial
point-cloud grammar. Every block refuses unknown keys. Capability-named
fields (`credentials_provider`, `spatial_func`, `temporal_reducer`, `mask`, …)
take registry names — hints in the exported schema, never a closed set.


## Config kinds

- [`SpatialConfig`](#spatialconfig)
- [`RasterConfig`](#rasterconfig)
- [`TemporalConfig`](#temporalconfig)

## Models

### Aggregation

The spatial point-cloud aggregation block.

| key | type | description |
|---|---|---|
| `handoff` | `'pandas'` \| `'arrow'` | Per-cell carrier (issue #132): ``arrow`` (default; an arro3 Table) or ``pandas`` (tolerates nullable columns). Packaging, never hashed. |
| `streaming` | [`Streaming`](#streaming) | Bounded-memory regime knobs; absent is the pooled path. |
| `coordinates` | mapping of name → [`CoordinateSpec`](#coordinatespec) | Declared store coordinates by name (the HEALPix templates declare ``morton: {dtype: uint64, fill_value: 0}``). |
| `variables` | mapping of name → [`FieldSpec`](#fieldspec) | Output fields by name. |
| `chunk_precompute` | mapping of name → [`PrecomputeSpec`](#precomputespec) | Chunk-level scalars by name, computed once per chunk and exposed to the per-cell expressions (issue #30). |

### Asset

A sibling asset (e.g. GEDI L2A beside L1B) paired by record key (issue #425).

| key | type | description |
|---|---|---|
| `join` *(required)* | [`AssetJoin`](#assetjoin) | The record-key join aligning the asset's rows. |

### AssetJoin

``{left, right}``: the record-key datasets a sibling asset is joined on.

| key | type | description |
|---|---|---|
| `left` *(required)* | `string` | Record-key dataset path template on the primary source. |
| `right` *(required)* | `string` | Record-key dataset path template on the sibling asset. |

### AssetVariable

``{asset, path}``: a record-level variable read from a sibling asset (issue #464).

The dataset is read from the named ``data_source.assets`` entry's handle
and joined per record on that asset's declared ``join`` key; 1-D datasets
only (no ``column`` selector yet — richer companions ride issue #465).

| key | type | description |
|---|---|---|
| `asset` *(required)* | `string` | Name of the ``data_source.assets`` entry to read from. |
| `path` *(required)* | `string` | Dataset path template inside the sibling asset. |

### Band

One output field sampled pull-NN from a STAC asset's GeoTIFF.

| key | type | description |
|---|---|---|
| `asset` *(required)* | `string` | STAC asset key holding this band's GeoTIFF/COG. |
| `dtype` *(required)* | `string` | Stored dtype — bands keep exact source DNs, so there is no float default. |
| `fill_value` | `number` | Fill for unsampled cells; default 0. |
| `scale` | `number` | Recorded as the ``scale_factor`` array attr for readers; never applied. |
| `offset` | `number` | Recorded as the ``add_offset`` array attr for readers; never applied. |

### Bounds

Optional run bounds, recorded per run (never part of the product identity).

| key | type | description |
|---|---|---|
| `temporal` | [`TemporalBounds`](#temporalbounds) \| `null` | The granule time window. |
| `spatial` | any | Reserved: a spatial bound (unused today). |

### ChunkBoundaries

The a-priori chunk-boundary arm (issue #148 arm 2a): boundary parquet per granule.

| key | type | description |
|---|---|---|
| `prefix` *(required)* | `string` | s3:// or local directory prefix holding the boundary parquet. |
| `samples_per_chunk` | `integer` \| `null` | Override of the samples-per-chunk the boundaries were cut at. |

### CollectionDataSource

The gridded-collection source of the temporal/event engine (issue #12, #213).

``reader`` names a registered reader (``xarray_s3`` built in; plugins
register others) resolved at run time, never here.

| key | type | description |
|---|---|---|
| `reader` | `string` \| `null` | Registered reader name; absent is ``xarray_s3``. |
| `collections` | list of `string` \| mapping of name → [`CollectionOptions`](#collectionoptions) \| `null` \| `null` | A list of collection names, or a mapping name → per-collection options (``null`` for none). |
| `static_data` | mapping of name → `string` \| `null` | Static inputs by name → URI (masks, climatologies). |
| `credentials_provider` | `string` \| `null` | Credential-provider registry name for source reads (``gesdisc`` for MERRA-2); per-event ``s3_credentials`` win. |

### CollectionOptions

Per-collection reader options consumed by :func:`zagg.temporal.prepare_collection`.

Unknown keys pass through by design (e.g. ``doi``): catalog tooling
metadata rides beside the reader options.

| key | type | description |
|---|---|---|
| `coord_round` | `integer` \| `null` | Decimals to round lat/lon coordinates to, for source grids whose coordinate arrays carry float dirt. |
| `variables` | list of `string` \| `null` | Variables to load from the collection (default: all). |
| `time_offset` | `string` \| `null` | pandas offset string shifting the time axis, e.g. ``-30min``. |
| `resample` | [`Resample`](#resample) \| `null` | Resample the collection. |
| `derived` | mapping of name → `string` \| `null` | Derived variables: name → numpy expression string. |

### CoordinateSpec

A declared store coordinate (``aggregation.coordinates.<name>``), e.g. ``morton``.

| key | type | description |
|---|---|---|
| `dtype` | `string` | Stored numpy dtype (default ``float32``), e.g. ``uint64`` for ``morton``. |
| `fill_value` | any | Fill for unobserved cells (default ``"NaN"``; integer dtypes need an explicit integer fill, e.g. ``0`` for ``morton``). |

### ExpressionFilter

``{expression}`` — a base-level, aggregation-time row predicate that forfeits pushdown.

Evaluated over the read columns (``data_source.variables`` plus broadcast
segment-level variables) with the restricted numpy namespace; opaque to the
planner, so it never reduces bytes read.

| key | type | description |
|---|---|---|
| `expression` *(required)* | `string` | Python boolean expression over column names and ``np``. |
| `level` | `null` | Must be omitted or null: expression filters are base-level only. |

### FieldSpec

One output field of the spatial aggregation (``aggregation.variables.<name>``).

A scalar field by default; ``kind: vector`` (+ ``trailing_shape``) and
``kind: ragged`` (+ ``inner_shape``) declare the non-scalar carriers
(issues #29, #48). A ragged field may carry a ``location`` channel
(issue #87), a ``temporal`` companion (spec §8, issue #410), a ``weights``
declaration and an ``overview_delta`` fold budget (issue #424).

| key | type | description |
|---|---|---|
| `function` | `string` | Reducer name: ``len``/``count``, a numpy name (``mean``, ``nanmax``, ``quantile``), or a dotted path (``zagg.stats.tdigest.build_tdigest``) resolved by ``zagg.config.resolve_function`` at validation. |
| `expression` | `string` | Python expression over column names and ``np`` evaluated per cell (mutually exclusive with ``function``). |
| `source` | `string` | The read column (a ``data_source.variables`` name or a broadcast segment-level variable) the reducer takes as its positional argument. |
| `params` | mapping | Keyword arguments forwarded to the reducer. A string value naming a column (or an expression over columns) is evaluated per cell first; numbers pass through. ``locations``/``temporal`` are reserved for the injected channels. |
| `dtype` | `string` | Stored numpy dtype of the result (``float32``, ``uint64``…). |
| `fill_value` | any | Fill for unobserved cells: a number, or the string ``"NaN"`` (YAML ``.nan`` is canonicalized to it at load). Dense default ``"NaN"``. |
| `kind` | `'scalar'` \| `'vector'` \| `'ragged'` | Output carrier: ``scalar`` (default), ``vector``, ``ragged``. |
| `trailing_shape` | `integer` \| list of `integer` | Per-cell trailing shape of a ``vector`` field (an int or a list of ints). |
| `inner_shape` | `integer` \| list of `integer` | Per-element shape of a ``ragged`` field's payload rows, e.g. ``[2]`` for ``(mean, weight)`` centroid pairs. |
| `resolution` | `'cell'` \| `'chunk'` | ``cell`` (default) or ``chunk``: one value per chunk, written to a companion array at the chunk grid (the compact store for a chunk-uniform value). |
| `location` | `string` | Ragged location channel (issue #87): the per-observation morton column — ``leaf_id`` (the HEALPix read path's) or a data column — folded per centroid into a uint64 sibling array; requires a ``function`` reducer that accepts ``locations=``. |
| `temporal` | `'per-cell'` \| `'per-centroid'` | Temporal companion shape (spec §8.2/§8.3): ``per-cell`` (the field's own dense uint64 array holds the words; ``fill_value: 0``) or ``per-centroid`` (a uint64 sibling row-aligned with a ragged payload). Requires a toc-producing reducer and the store's clock (``output.time_source``). |
| `weights` | `'counts'` \| `'flux'` | Ragged payload weight semantics (spec §2.0): ``counts`` (the absent-key default, integer weights) or ``flux`` (positive reals; requires ``attrs.gain`` with ``name`` and ``version``). |
| `overview_delta` | `integer` | Pyramid-fold centroid budget for a ragged digest (issue #424), split from the leaf ``params.delta``; packaging, never hashed. |
| `attrs` | mapping | Free-form JSON provenance merged onto the field's array attrs. Spec-owned keys (the ragged layout block, ``weights`` on ragged, the temporal/located bindings) are stamped by the writer and refused here; a ``composition`` block is cross-checked against the field it describes (issue #321). |

### H5DataSource

The HDF5 point-cloud source read through h5coro (``reader: h5coro``, the default).

Flat form: ``groups`` × ``coordinates``/``variables`` path templates plus
filters. Hierarchical form (issue #43 phase B): ``levels`` + ``base_level``
declare the rates and links; the flat keys still describe the base level.
Vlen form (issue #425): ``coordinates.level`` names the record level the
per-sample coordinates expand from.

| key | type | description |
|---|---|---|
| `reader` | `string` \| `null` | Reader name; ``h5coro`` (default). ``raster`` selects the raster source grammar; the collection grammar is selected by ``pipeline.type: temporal\|event``, not by this key. |
| `driver` | `string` \| `null` | Granule access driver, ``s3`` (default) or ``https``. |
| `groups` | list of `string` \| `null` | HDF5 group names substituted for ``{group}`` in every path template (ICESat-2 beams: ``gt1l`` … ``gt3r``). |
| `coordinates` | mapping of name → `string` \| `null` | ``{latitude, longitude}`` dataset path templates, plus the reserved ``level`` key naming the vlen record level the coordinates expand from (issue #425). |
| `variables` | mapping of name → `string` \| [`PathColumn`](#pathcolumn) \| [`Synthesize`](#synthesize) \| `null` | Base-level columns: ``name: path-template``, ``name: {path, column}``, or ``name: {synthesize: linspace, level, start, stop}``. |
| `quality_filter` | [`QualityFilter`](#qualityfilter) \| `null` | Legacy ``{dataset, value}`` sugar for one base-level ``eq`` filter. |
| `filters` | list of [`StructuredFilter`](#structuredfilter) \| [`ExpressionFilter`](#expressionfilter) \| `null` | Ordered row predicates: structured ``{dataset, op, value\|values, ...}`` entries (pushdown-eligible) or ``{expression}`` escape hatches. When present the flat ``quality_filter`` is ignored. |
| `levels` | mapping of name → [`Level`](#level) \| `null` | The hierarchical rates of the source, keyed by level name (issue #43 phase B). |
| `base_level` | `string` \| `null` | The ``levels`` key of the finest rate; required with ``levels``. |
| `index` | mapping \| `null` | Virtual chunk-index backend block (issue #160): ``backend`` names a registered backend (``hierarchical``, ``inline``, or an entry-point backend such as ``sidecar``); the remaining keys are backend-specific. Absent resolves to ``inline`` (the compiled decode, issue #170), except under ``read_plan.chunk_boundaries``, where it is ``hierarchical``; ``{backend: hierarchical}`` pins the pure-h5coro walk. |
| `read_plan` | [`ReadPlan`](#readplan) \| `null` | Planned-read knobs: the spatial index level and its padding. |
| `credentials_provider` | `string` \| `null` | Credential-provider registry name for source S3 reads (``nsidc``, ``gesdisc``, ``lpdaac``, or a plugin); absent is the spatial default (NSIDC). |
| `shard_workers` | `integer` \| `null` | Source units (granules) in flight per shard (issue #232); default 4. |
| `granule_workers` | `integer` \| `null` | Legacy spelling of ``shard_workers`` (issue #180); canonical wins. |
| `read_workers` | `integer` \| `null` | Per-granule read fan-out width (issue #170); default 8. |
| `assets` | mapping of name → [`Asset`](#asset) \| `null` | Sibling assets paired by record key (issue #425), e.g. ``l2a: {join: {left, right}}``. |

### HealpixGrid

A HEALPix NESTED grid: cells at ``child_order``, shards at ``parent_order``.

| key | type | description |
|---|---|---|
| `type` *(required)* | `'healpix'` | Grid family. |
| `parent_order` *(required)* | `integer` | Shard / dispatch order: one worker per order-``parent_order`` cell. |
| `child_order` *(required)* | `integer` | Leaf cell resolution (order of the output cells). |
| `chunk_inner` | `integer` | Inner zarr chunk order between ``parent_order`` and ``child_order`` (K = 4^(chunk_inner - parent_order) chunks per shard); absent derives ``child_order - 6`` (64×64-cell inner chunks, issue #259) when sharded and that lies above ``parent_order``, else one chunk per shard (always so for raster). |
| `sharded` | `boolean` | Bundle a shard's K inner chunks into one ShardingCodec object (issue #108); HEALPix output defaults to true (issue #215). |
| `layout` | `'fullsphere'` \| `null` | HEALPix array layout; ``fullsphere`` is the only one (the deprecated ``dense`` pack was removed, issue #88). |
| `emit_cell_ids` | `boolean` | Transition hatch (issue #304): also write the legacy NESTED ``cell_ids`` array beside the ``morton`` coordinate. Default off. |
| `indexing_scheme` | `'nested'` | Descriptive only (the shipped templates spell ``nested``); the stored coordinate is morton (D16, issue #304). |
| `cell_ids_encoding` | `null` | Retired (issue #304): morton is the stored cell coordinate and NESTED ids are derived at read. Must be absent or null. |

### Icechunk

Options for the Icechunk companion repo (spec §11); ``true`` is this block with defaults.

The shard-order-relative checks run at init (``icechunk_refs.resolve_options``);
the shape and the one geometry-free invariant — a commit writes whole
manifests — are here.

| key | type | description |
|---|---|---|
| `commit` | `'ladder'` \| `'leaf'` | Commit regime: ``ladder`` (refs committed up the staged ladder) or ``leaf`` (per leaf). |
| `commit_order` | `integer` | Order a commit covers (default derived from the shard order). |
| `split_order` | `integer` | Order one manifest covers; never finer than ``commit_order``. |
| `retain_runs` | `integer` | Run tags finalize keeps; 0 (default) keeps every run. |

### Level

One rate of a multi-rate HDF5 source (e.g. ATL03 ``photons`` and ``segments``).

Each level declares its own group ``path``, optional ``coordinates``, and
``variables``, plus a ``link`` to its coarser parent (the base level has
none). ``variables`` is either a documentation-only list of names or, on a
non-base level, a ``{name: path-template}`` mapping declaring a readable
segment-level variable broadcast to the base rows at read time (issue #30)
— or an ``{asset, path}`` entry sourced from a sibling asset (issue #464).

| key | type | description |
|---|---|---|
| `path` *(required)* | `string` | HDF5 group path template (may contain ``{group}``). |
| `coordinates` | list of `string` \| mapping of name → `string` \| `null` | Coordinate dataset names within ``path``: a list, or a ``{latitude, longitude}`` mapping of dataset names. |
| `variables` | list of `string` \| mapping of name → `string` \| [`AssetVariable`](#assetvariable) \| `null` | A documentation-only list of names, or (non-base levels) a mapping of readable segment-level variables: ``name: path-template`` or ``name: {asset, path}``. |
| `link` | [`Link`](#link) \| `null` | Link to the coarser level; required on every non-base level, null on the base. |

### Link

Per-level link to the next coarser level: a contiguous-range parent→child tiling.

Parent segment ``p`` covers base-rate indices ``[index_beg[p] - index_base, ...)``
for ``count[p]`` children. ``reference_index`` is a reserved slot for a
future explicit-index-array variant and must stay null.

| key | type | description |
|---|---|---|
| `to` *(required)* | `string` | Key of the coarser level in ``levels``. |
| `index_beg` *(required)* | `string` | HDF5 path template of the per-parent start-index array. |
| `count` *(required)* | `string` | HDF5 path template of the per-parent child-count array. |
| `index_base` | `integer` | Subtracted from ``index_beg`` so 0-based indexing into the base array is direct (ATL03 ``ph_index_beg`` is 1-based: ``index_base: 1``). |
| `reference_index` | `null` | Reserved; must be null (explicit index arrays are not implemented). |

### Output

The store side of a spatial or raster pipeline.

| key | type | description |
|---|---|---|
| `store` | `string` | Store root: a local path or ``s3://bucket/prefix``. |
| `format` | `string` | Store format; ``zarr`` (default). |
| `endpoint_url` | `string` | Non-secret S3-compatible endpoint (R2, MinIO); never credentials. |
| `region` | `string` | AWS region of the output store. |
| `product_name` | `string` | D19 product name (issue #299): the run writes under ``{store}/{product_name}/``; absent is the bare single-product layout. |
| `grid` | [`HealpixGrid`](#healpixgrid) \| [`RectilinearGrid`](#rectilineargrid) | The output grid: ``type: healpix`` or ``type: rectilinear``. |
| `store_layout` | `'flat'` \| `'hive'` \| `null` | ``hive`` (one leaf zarr per shard under a morton digit tree; the HEALPix default) or ``flat`` (one shared store; the rectilinear default). |
| `touch` | `'auto'` \| `'always'` \| `'never'` | Skip-run lifecycle touch policy (issue #501): ``auto`` (default), ``always``, ``never``. |
| `aoi_mask` | `boolean` | Package a per-cell boolean ``aoi_mask`` array (issue #101); default off. |
| `consolidate_metadata` | `boolean` | Run zarr metadata consolidation at finalize (flat stores only); default off. |
| `coverage_moc` | `boolean` \| `null` | Write the root ``coverage.moc`` at the end of the run (hive + HEALPix only); defaults on for hive. |
| `sweep` | `boolean` \| `'none'` \| `'families'` \| `'stages'` \| `null` | End-of-run sweep (hive only): ``true``/``families`` (the families pass), ``stages`` (plus the staged ladder), ``false``/``none``. Absent resolves to ``stages`` on a hive store declaring a ladder, else ``families``; ``none`` on a flat store. |
| `leaf_versions` | `boolean` \| `null` | Versioned hive leaves (spec §1.5, issue #582); defaults on for hive. |
| `icechunk` | `boolean` \| [`Icechunk`](#icechunk) \| `null` | Icechunk companion repo (spec §11): a boolean, or an options block (an opt-in with knobs). Defaults on for non-raster hive stores. |
| `pyramid` | `False` \| [`Pyramid`](#pyramid) \| `null` | Overview pyramid: absent (defaults), ``false`` (off), or an options block (hive only). |
| `windowing` | [`Windowing`](#windowing) \| `null` | Temporal windowing of the hive leaves (issue #246). |
| `time_source` | [`TimeSource`](#timesource) | The per-observation clock for temporal companions (spec §8.3). |
| `time_encoding` | `'microseconds'` \| `'toc'` | Time-coordinate encoding of a raster ``(time, cells)`` product (spec §8): ``microseconds`` (default) or ``toc``. |

### PathColumn

``{path, column}``: one column of a 2-D dataset (issue #321).

| key | type | description |
|---|---|---|
| `path` *(required)* | `string` | HDF5 dataset path template (may contain ``{group}``). |
| `column` *(required)* | `integer` | 0-based column selector into the N-D dataset — the variable analogue of a structured filter's ``column``, so several scalar columns can be declared against one shared dataset path (deduped to a single read). |

### PrecomputeSpec

One ``aggregation.chunk_precompute`` entry: a chunk-level scalar (issue #30).

Evaluated ONCE per chunk over the shard's pooled columns, before the
per-cell loop; its name then enters the per-cell expression namespace.
Entries are independent (one cannot reference another).

| key | type | description |
|---|---|---|
| `function` | `string` | Reducer name: ``len``/``count``, a numpy name (``mean``, ``nanmax``, ``quantile``), or a dotted path (``zagg.stats.tdigest.build_tdigest``) resolved by ``zagg.config.resolve_function`` at validation. |
| `expression` | `string` | Python expression over column names and ``np`` evaluated per cell (mutually exclusive with ``function``). |
| `source` | `string` | The read column (a ``data_source.variables`` name or a broadcast segment-level variable) the reducer takes as its positional argument. |
| `params` | mapping | Keyword arguments forwarded to the reducer. A string value naming a column (or an expression over columns) is evaluated per cell first; numbers pass through. ``locations``/``temporal`` are reserved for the injected channels. |
| `dtype` | `string` | Stored numpy dtype of the result (``float32``, ``uint64``…). |

### Pyramid

The overview pyramid declaration (issue #201); ``false`` declares the family off.

Absent applies the defaults (an every-2-orders schedule below the shard
order; the all-time fold on a windowed store). ``overviews`` is the
``zagg-pyramid/2`` grammar (issue #382) and replaces ``orders``/``spacing``.

| key | type | description |
|---|---|---|
| `spacing` | `integer` | Every-N-orders overview schedule below the shard order (>= 1). |
| `orders` | list of `integer` | Explicit ancestor orders in ``[0, parent_order)``; wins over ``spacing``. |
| `overviews` | `integer` \| list of `integer` | Leaf cell resolutions of the column tier (``zagg-pyramid/2``): an int or a non-empty list of ints, strictly descending and strictly inside the shard's resolution window. |
| `all_time` | `boolean` | Maintain the ``all.zarr`` cross-window fold on a windowed store; resolves true there when omitted, and ``false`` is refused. |
| `fold_source` | `'cascade'` \| `'leaves'` | ``cascade`` (default: each coarse level folds from the level below) or the deprecated ``leaves``. |
| `exact_levels` | `integer` | Under ``cascade``: how many of the finest declared levels fold exactly from the leaves (>= 1). |
| `summarize` | mapping of name → [`Summarize`](#summarize) | D24 derived summaries: ``{source_field: {as: new_name, ...}}``; recorded in the pyramid block, never the semantic core. |

### QualityFilter

The flat ``quality_filter: {dataset, value}`` sugar: one base-level ``op: eq`` filter.

Only ``dataset`` and ``value`` are honored; use the structured ``filters:``
list to set ``op``, ``column``, ``keep`` and the rest.

| key | type | description |
|---|---|---|
| `dataset` *(required)* | `string` | Dataset path template the equality is read from. |
| `value` *(required)* | any | The value rows must equal to be kept. |

### RasterConfig

A raster (pull-NN) pipeline: ``data_source.reader: raster`` (issue #218).

| key | type | description |
|---|---|---|
| `worker` | [`Worker`](#worker) \| `null` | Optional Lambda worker-size selector ``{memory, extra_disk?}`` (issue #235). |
| `catalog` | `string` \| `null` | Optional path to the granule catalog. |
| `bounds` | [`Bounds`](#bounds) \| `null` | Optional ``{temporal: {start_date, end_date}}`` run bounds. |
| `pipeline` | [`SpatialPipeline`](#spatialpipeline) | ``{type: spatial}`` (the default). |
| `aggregation` | mapping | Must be empty: pull-NN yields exactly one value per cell per timestep, so there is nothing to reduce — the band map under ``data_source.bands`` replaces it. |
| `output` | [`Output`](#output) | The output grid (HEALPix only, never sharded), store layout, windowing. |
| `data_source` *(required)* | [`RasterDataSource`](#rasterdatasource) | The STAC-asset raster source: a band map plus source-store knobs. |

### RasterDataSource

The STAC raster source: ``reader: raster`` (issue #218).

Pull-NN yields exactly one value per cell per timestep, so the aggregation
block is replaced by a declarative band map.

| key | type | description |
|---|---|---|
| `reader` *(required)* | `'raster'` | Selects the raster (pull-NN) pipeline. |
| `bands` *(required)* | mapping of name → [`Band`](#band) | Output field name → ``{asset, dtype, fill_value?, scale?, offset?}``. |
| `nodata` | `number` \| `null` | Source nodata scalar: a cell whose sampled pixel equals it in any band is left at fill. |
| `anonymous` | `boolean` \| `null` | Open the source store without credentials (default true). |
| `source_region` | `string` \| `null` | AWS region of the source store. |
| `credentials_provider` | `string` \| `null` | Credential-provider registry name for source reads. |
| `shard_workers` | `integer` \| `null` | Acquisition groups (timesteps) sampling concurrently per shard; default 4, 1 is serial (issues #231/#232). |
| `write_buffer` | `integer` \| `null` | Streamed-write slab budget: 1 (default) is the strict serial bound; N overlaps up to N-1 slab writes with sampling. |

### ReadPlan

How a shard's byte-range reads are planned over the levels (issue #43 phase C).

| key | type | description |
|---|---|---|
| `spatial_index` | `string` \| `null` | The ``levels`` key whose coordinates select which parent records a shard needs (e.g. ``segments``); absent is the full-read route. |
| `pad` | `integer` | Parent records of padding on each side of the spatial selection. |
| `full_read_threshold` | `number` | Fraction of a group's records above which the planner reads it whole. |
| `chunk_boundaries` | [`ChunkBoundaries`](#chunkboundaries) \| `null` | The a-priori chunk-boundary arm; mutually exclusive with the ``inline`` index. |

### RectilinearGrid

A projected rectilinear grid: ``crs`` + ``resolution`` + ``bounds`` fix the cell geometry.

| key | type | description |
|---|---|---|
| `type` *(required)* | `'rectilinear'` | Grid family. |
| `crs` *(required)* | `string` | Coordinate reference system, e.g. ``EPSG:3031``. |
| `resolution` *(required)* | `number` | Cell size in CRS units. |
| `bounds` *(required)* | list of `number` | ``[xmin, ymin, xmax, ymax]`` in CRS units. |
| `chunk_shape` | list of `integer` | Shard tile ``[rows, cols]`` in cells (default ``[256, 256]``). |
| `chunk_inner` | list of `integer` | Inner zarr chunk ``[rows, cols]`` within the shard tile (issue #30 item 3); default the shard tile. |
| `sharded` | `boolean` | ShardingCodec bundling of the inner chunks; rect defaults off. |
| `emit_cell_ids` | `boolean` | Not applicable: rectilinear grids have no cell_ids coordinate. |

### Resample

``{freq, how?, scale?}``: resample a collection onto a coarser cadence.

| key | type | description |
|---|---|---|
| `freq` *(required)* | `string` | pandas frequency string, e.g. ``3h``. |
| `how` | `'sum'` \| `'mean'` | The resampling reducer. |
| `scale` | `number` | Multiplier applied after resampling. |

### SpatialConfig

A point-cloud → grid pipeline reading HDF5 through h5coro (the default kind).

| key | type | description |
|---|---|---|
| `worker` | [`Worker`](#worker) \| `null` | Optional Lambda worker-size selector ``{memory, extra_disk?}`` (issue #235). |
| `catalog` | `string` \| `null` | Optional path to the granule catalog. |
| `bounds` | [`Bounds`](#bounds) \| `null` | Optional ``{temporal: {start_date, end_date}}`` run bounds. |
| `pipeline` | [`SpatialPipeline`](#spatialpipeline) | ``{type: spatial}`` (the default). |
| `aggregation` | [`Aggregation`](#aggregation) | Per-field reducers over the read columns, chunk precomputes, carrier and streaming knobs. |
| `output` | [`Output`](#output) | The output grid, store layout and its companions, pyramid, windowing. |
| `data_source` | [`H5DataSource`](#h5datasource) | The HDF5 point source: groups, coordinates, variables, filters, levels. |

### SpatialPipeline

``pipeline`` of the point-cloud and raster kinds (absent means ``spatial``).

| key | type | description |
|---|---|---|
| `type` | `'spatial'` | The point-cloud → grid engine (the default kind). |

### Streaming

Bounded-memory read/aggregate regime (issues #370, #474); absent is the pooled path.

Packaging: every streaming regime shares one product identity under the
law-equivalence contract (``zagg.semantics``).

| key | type | description |
|---|---|---|
| `mode` | `'merge'` \| `'spill'` | ``merge`` (default): fold flushed buffers in memory; ``spill``: write buffers to /tmp partitions and aggregate once after the reads. |
| `buffer_granules` | `integer` | Granules of post-filter rows held before a flush (default 20). |
| `block_bytes` | `integer` | Spill-only cap on a block before it closes and folds. |

### StructuredFilter

``{level?, asset?, dataset, column?, op, value|values, keep?}`` — a pushdown-eligible predicate.

The only filter language the planner can inspect (read pushdown, issue #43
phase C). ``in``/``not_in`` take a ``values`` list; the other ops a scalar
``value``. ``column`` selects one column of an N-D flag array (its rank is
checked at read time, not here).

| key | type | description |
|---|---|---|
| `dataset` *(required)* | `string` | Dataset path template the predicate reads. |
| `op` *(required)* | `'eq'` \| `'ne'` \| `'ge'` \| `'le'` \| `'lt'` \| `'gt'` \| `'in'` \| `'not_in'` | Comparison operator: ``eq``/``ne``/``ge``/``le``/``lt``/``gt`` take ``value``; ``in``/``not_in`` take ``values``. |
| `value` | `number` \| `null` | Scalar operand for the scalar ops. |
| `values` | list of `number` \| `null` | Operand list for ``in``/``not_in``. |
| `column` | `integer` \| `null` | Column selector into an N-D flag dataset (e.g. ATL03 ``signal_conf_ph``); omit for a 1-D dataset. |
| `level` | `string` \| `null` | The ``levels`` key the predicate is evaluated at; absent/null is the base level. A non-base predicate keeps or drops whole parent records. |
| `asset` | `string` \| `null` | Sibling-asset predicate (issue #425): evaluated on the named ``data_source.assets`` entry's rows after the record-key join; record-level by construction, so ``level`` must be omitted. |
| `keep` | `boolean` | ``true`` (default) keeps matching rows; ``false`` drops them. |

### Summarize

One D24 derived-summary declaration: ``{as: <new field name>, ...}``.

| key | type | description |
|---|---|---|
| `as` *(required)* | `string` | Name of the derived overview field (must be new). |

### Synthesize

``{synthesize: linspace, level, start, stop}``: a per-record computed column (issue #425).

The column is never read: it is ``linspace(start, stop, count)`` over each
record's samples, where ``start``/``stop`` are record-level datasets on
``level`` (which must be ``coordinates.level``, the vlen record level).

| key | type | description |
|---|---|---|
| `synthesize` *(required)* | `'linspace'` | The synthesis kind; ``linspace`` is the only one. |
| `level` *(required)* | `string` | The record level the bounds are read at; must equal ``coordinates.level``. |
| `start` *(required)* | `string` | Dataset path template of the per-record start value. |
| `stop` *(required)* | `string` | Dataset path template of the per-record stop value. |

### TemporalAggregation

The temporal/event aggregation block: output variables by name.

| key | type | description |
|---|---|---|
| `variables` *(required)* | mapping of name → [`TemporalFieldSpec`](#temporalfieldspec) | Output variables by name. |

### TemporalBounds

``bounds.temporal``: the run's granule time window.

| key | type | description |
|---|---|---|
| `start_date` *(required)* | `string` | ISO-8601 start of the granule window (inclusive). |
| `end_date` *(required)* | `string` | ISO-8601 end of the granule window; a bare ``YYYY-MM-DD`` string is expanded to the end of that day. |

### TemporalConfig

A temporal/event pipeline over gridded collections (issue #12, #213).

| key | type | description |
|---|---|---|
| `worker` | [`Worker`](#worker) \| `null` | Optional Lambda worker-size selector ``{memory, extra_disk?}`` (issue #235). |
| `catalog` | `string` \| `null` | Optional path to the granule catalog. |
| `bounds` | [`Bounds`](#bounds) \| `null` | Optional ``{temporal: {start_date, end_date}}`` run bounds. |
| `pipeline` *(required)* | [`TemporalPipeline`](#temporalpipeline) | ``{type: temporal}`` or ``{type: event}``. |
| `aggregation` *(required)* | [`TemporalAggregation`](#temporalaggregation) | Output variables as registry capability names (spatial reducer, temporal reducer, mask, transform). |
| `output` | [`TemporalOutput`](#temporaloutput) | The tabular sink (``format``, ``store``). |
| `data_source` | [`CollectionDataSource`](#collectiondatasource) | The collection reader source: named collections with per-collection reader options, plus static data. |

### TemporalFieldSpec

One temporal/event output variable: capability names resolved by the registry at run time.

A typo in a capability name surfaces as ``UnknownCapability`` from
``process_event``, never as a load-time guess about installed plugins.

| key | type | description |
|---|---|---|
| `variable` *(required)* | `string` | Source variable name within the collection. |
| `collection` *(required)* | `string` | The ``data_source.collections`` entry to read it from. |
| `spatial_func` *(required)* | `string` | Registered spatial reducer over the masked grid (``max``, ``weighted_mean``…). |
| `temporal_reducer` *(required)* | `string` | Registered reducer over the event's time window (``max``, ``sum``…). |
| `mask` | `string` | Registered spatial mask name (default ``ais``). |
| `negate` | `boolean` | Negate the field values (after any transform, before the spatial reduction), e.g. for southward-positive fluxes. |
| `anomaly` | `boolean` | Sugar for ``transform: monthly_anomaly`` (subtract the climatology). |
| `transform` | `string` | Registered transform applied before reduction; wins over ``anomaly``. |
| `trigger` | `string` | Registered event-trigger predicate. |
| `trigger_mask` | `string` | Key into ``data_source.static_data`` naming the mask the trigger tests against (default ``ais_mask``); a static field name, not a registered mask provider. |
| `params` | mapping | Free-form mapping passed to the mask provider, field transform and event trigger (not to spatial funcs or reducers). |

### TemporalOutput

The tabular sink of the temporal/event engine.

| key | type | description |
|---|---|---|
| `format` | `string` \| `null` | Sink format (``parquet``); ``zarr`` is the spatial default. |
| `store` | `string` \| `null` | Output path or ``s3://`` URI. |
| `endpoint_url` | `string` \| `null` | Non-secret S3-compatible endpoint. |
| `region` | `string` \| `null` | AWS region of the output store. |
| `time_encoding` | `string` \| `null` | Accepted for symmetry; a non-default value is refused (raster only). |

### TemporalPipeline

``pipeline`` of the event-streaming engine (issue #12).

| key | type | description |
|---|---|---|
| `type` *(required)* | `'temporal'` \| `'event'` | ``temporal`` or ``event``: the event-streaming engine over gridded collections. |

### TimeSource

The per-observation clock a temporal companion's words are encoded from (spec §8.3).

``field`` must be a base-rate read column (checked against the data source
in ``validate_config``); ``scale`` must be continuous — a nominal-UTC
column is only good to the leap seconds since its epoch.

| key | type | description |
|---|---|---|
| `field` *(required)* | `string` | Per-observation time column (a read variable, base rate). |
| `epoch` *(required)* | `string` | The UTC instant the column counts from. |
| `scale` | `'gps'` \| `'tai'` | ``gps`` (default) or ``tai``. |
| `units` | `'seconds'` \| `'days'` | ``seconds`` (default) or ``days``. |

### WindowPoint

``{label, timestamp}``: sugar for the one-second window ``[t, t + 1s)``.

| key | type | description |
|---|---|---|
| `label` *(required)* | any | Window label in the explicit grammar. |
| `timestamp` *(required)* | `string` | ISO-8601 UTC instant the window starts at. |

### WindowRange

``{label, start, end}``: a half-open ``[start, end)`` window.

| key | type | description |
|---|---|---|
| `label` *(required)* | any | Window label in the explicit grammar (``[A-Za-z0-9-]+``). |
| `start` *(required)* | `string` | ISO-8601 UTC start (inclusive). |
| `end` *(required)* | `string` | ISO-8601 UTC end (exclusive). |

### Windowing

Temporal windowing (issue #246): windowed hive leaves, one per schedule label.

Absent, or ``schedule: none``, is the unwindowed store. A present schedule
requires the hive layout on a HEALPix grid. ``time_field`` must be a
base-rate ``data_source.variables`` column (checked in ``validate_config``);
on the raster path membership is the acquisition's STAC ``datetime``, so
``time_field`` is fixed and the conversion knobs are refused.

| key | type | description |
|---|---|---|
| `schedule` | `string` | ``none`` (default), ``yearly``, ``monthly``, ``daily``, or ``explicit`` (with ``windows``); ``quarterly`` is reserved and not implemented. |
| `time_field` | `string` | Per-observation timestamp column deciding window membership (a base-rate ``data_source.variables`` entry). |
| `epoch` | `string` | The dataset's zero time as an ISO-8601 UTC instant at whole-second precision (ICESat-2 delta_time: ``2018-01-01T00:00:00Z``). |
| `scale` | `'utc'` \| `'gps'` \| `'tai'` | Timescale of ``time_field``: ``utc`` (default), ``gps``, ``tai``. |
| `units` | `'seconds'` \| `'days'` | Units of ``time_field``: ``seconds`` (default) or ``days``. |
| `unit` | `'shard'` \| `'window'` | Dispatch unit (issue #586): ``shard`` (default; one invoke per shard emits every window its granules span) or ``window`` (one invoke per (shard, window)). |
| `windows` | list of [`WindowRange`](#windowrange) \| [`WindowPoint`](#windowpoint) \| `null` | The explicit windows (``schedule: explicit`` only): disjoint half-open ``{label, start, end}`` ranges or ``{label, timestamp}`` one-second points. |

### Worker

Lambda worker-size selector (issue #235): a pre-provisioned ``-<memory>[-disk]`` variant.

Absent is the unsuffixed default function; ignored by the local backend.

| key | type | description |
|---|---|---|
| `memory` *(required)* | `2048` \| `4096` \| `8192` | Memory in MB, one of 2048, 4096, 8192 — the pre-provisioned variant sizes. |
| `extra_disk` | `boolean` | Select the ``-disk`` twin with ``/tmp`` at Lambda's 10240 MB ceiling (issue #536; default false). |
