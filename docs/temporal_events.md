# Temporal events: the consumer contract

The temporal pipeline (`pipeline.type: temporal`, issues #12/#213) aggregates
*events* — spatiotemporal objects like storms — into one tabular row of scalar
attributes each. This page is the stable interface for libraries that feed
events into `zagg.agg()`: the event shapes both backends accept, the config
blocks that describe collections and specs, and the capability-resolution
policy that governs what runs where.

## The event model

An event is **one binary mask** with dims `(time, lat, lon)`: where and when
the object exists. `process_event` streams the mask's timesteps, applies a
per-timestep `spatial_func` under a `mask` provider, and reduces across time
with a `temporal_reducer` — one scalar per configured attribute.

There is deliberately no second mask channel: an attribute set needing a
different footprint (e.g. a precipitation lookahead window) is a *different
event set*. Run `agg()` once per mask family and join the tabular outputs on
`event_key`.

## Passing events

`zagg.agg(config, events=..., backend=...)` accepts events in two shapes,
chosen by the backend:

**Local backend** — in-memory tuples, one per event:

```python
(event_key, event_mask, collections, static_data)
```

- `event_key`: hashable identifier, echoed into the output row.
- `event_mask`: `xr.DataArray`, dims `(time, lat, lon)`.
- `collections`: `{name: xr.Dataset}` — the source fields, already opened.
  The runner applies the config's per-collection options (below) to each
  event's collections, so local and Lambda semantics match.
- `static_data`: `{name: DataArray | Dataset}` — e.g. `ais_mask`,
  `cell_areas`, `climatology`.

**Lambda backend** — URI dicts, one per event; the worker loads its own
inputs, keeping the async payload far under Lambda's 256 KB Event cap:

```python
{
    "event_key": "storm_00042",
    "event_mask_uri": "s3://.../masks/storm_00042.nc",
    "collection_uris": {"merra2_slv": ["s3://.../day1.nc4", "s3://.../day2.nc4"]},
    "static_uris": {"ais_mask": "s3://.../ais_mask.nc", "cell_areas": "s3://.../areas.nc"},
    "s3_credentials": {...},   # optional, per-event read credentials
}
```

- `event_mask_uri` / `static_uris` values: a single URI each (local path,
  `s3://`; `.zarr` opens as a store, a local non-`.zarr` path opens directly,
  and an `s3://` non-`.zarr` object is fetched as bytes and opened
  in-memory). A single-variable file becomes a `DataArray`.
- `collection_uris` values: one URI **or a list** — a multi-granule event
  (e.g. a storm spanning several MERRA-2 daily files) is opened per-URI,
  concatenated along `time`, and time-sorted.
- `s3_credentials`: optional. Events without it receive shared credentials
  fetched once from the `data_source.credentials_provider` registry name when
  the config sets one (`nsidc` and `gesdisc` ship as built-ins).

The full worker payload (mode, config, `return_results`, `result_url`) is
assembled by the runner — consumers supply only the dicts above.

## Describing collections

`data_source.collections` accepts a list of names, or a mapping carrying
declarative per-collection reader options:

```yaml
data_source:
  reader: xarray_s3
  collections:
    merra2_slv:                 # no options needed
    merra2_precip:
      variables: [PRECCU, PRECLS, PRECSN]      # subset before anything else
      time_offset: "-30min"                     # shift stamps onto the hour
      resample: {freq: "3h", how: sum, scale: 3600}  # rates -> totals
      derived:
        rainfall: "PRECCU + PRECLS"             # numpy expression
      doi: "10.5067/Q5GVUVUIVGO7"               # unknown keys pass through
```

Options apply in the order listed above (`prepare_collection`). `derived`
expressions evaluate in the same restricted namespace as the spatial
pipeline's `expression` fields: numpy plus the collection's variables, no
builtins. Unknown keys (like `doi`) are ignored by the reader — they are
metadata for catalog tooling. Validated option values fail at config load,
not per-worker.

## Spec keys

Each `aggregation.variables` entry:

| key | required | meaning |
|---|---|---|
| `variable` | yes | variable name in the collection (may be `derived`) |
| `collection` | yes | collection name |
| `spatial_func` | yes | per-timestep reduction (registry name) |
| `temporal_reducer` | yes | cross-timestep accumulator (registry name) |
| `mask` | no (`ais`) | mask provider: `full` / `ais` / `ocean` / registered |
| `anomaly` | no | sugar for `transform: monthly_anomaly` |
| `transform` | no | field-transform name applied per timestep |
| `negate` | no | negate the field (southward-positive fluxes) |
| `trigger` | no | event-trigger name gating which timesteps update (e.g. `first_landfall`) |
| `trigger_mask` | no | static field the trigger tests against (default `ais_mask`) |
| `params` | no | free-form mapping threaded into mask providers, field transforms, and event triggers (not spatial_funcs or reducers) |

## Capability resolution and the worker policy

Every name above resolves through `zagg.registry` at run time. The Lambda
payload is **pure data** — capability names, numpy expressions, URIs — and a
name only resolves against what is installed in the worker's layer. There is
no mechanism for shipping third-party code to the fleet, by design (issue
#213): on `backend="lambda"`, worker-side capabilities are zagg built-ins and
declarative config, full stop.

The registries stay open on `backend="local"`: register a custom trigger,
reducer, mask, transform, or reader (directly via `zagg.registry.register_*`
or a `zagg.plugins` entry point) and run it on your own machine. To make a
capability Lambda-eligible, upstream it into zagg as a built-in — prototyping
locally as a plugin and promoting what proves out is the intended
contribution funnel.

Credential providers run orchestrator-side, so they carry no such
restriction; `data_source.credentials_provider` may name a plugin provider on
either backend — including non-NASA S3-compatible sources. The spatial
pipeline honors the same key for its S3-driver source reads, defaulting to
`nsidc` when the key is absent; the HTTPS driver instead authenticates with an
EDL bearer token and does not consult credential providers.
