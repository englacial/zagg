"""Temporal aggregation primitives for zagg.

Streaming accumulators for cross-timestep reduction, per-timestep spatial
functions for masked gridded data, and a domain-agnostic :func:`process_event`
worker. These are the building blocks for temporal and event-based aggregation
pipelines (e.g. computing storm summary statistics from reanalysis data).

The built-ins seed the canonical registries in :mod:`zagg.registry` (#73) by
*name*; external plugins add more through the same ``register_*`` helpers, and
:func:`process_event` resolves every capability by name so the loop carries no
domain knowledge. xarray is used only through methods on the arrays passed in,
so importing this module stays cheap for spatial-only installs.

Ported from antarctic_AR_dataset/artools/cloud/.
"""

import numpy as np

from . import registry

# ---------------------------------------------------------------------------
# Temporal accumulators (streaming reducers)
# ---------------------------------------------------------------------------


class MaxAccumulator:
    """Running maximum across timesteps."""

    def __init__(self):
        self.value = -np.inf

    def update(self, val):
        if val is not None and not np.isnan(val):
            self.value = max(self.value, val)

    def finalize(self):
        return float(self.value) if self.value != -np.inf else np.nan


class MinAccumulator:
    """Running minimum across timesteps."""

    def __init__(self):
        self.value = np.inf

    def update(self, val):
        if val is not None and not np.isnan(val):
            self.value = min(self.value, val)

    def finalize(self):
        return float(self.value) if self.value != np.inf else np.nan


class SumAccumulator:
    """Running sum across timesteps."""

    def __init__(self):
        self.value = 0.0
        self.has_data = False

    def update(self, val):
        if val is not None and not np.isnan(val):
            self.value += val
            self.has_data = True

    def finalize(self):
        return float(self.value) if self.has_data else np.nan


class WeightedMeanAccumulator:
    """Running weighted mean across timesteps.

    Spatial functions paired with this return ``(weighted_sum, weight_sum)``
    tuples. The final result is the ratio.
    """

    def __init__(self):
        self.weighted_sum = 0.0
        self.weight_sum = 0.0

    def update(self, val):
        if val is None:
            return
        weighted_sum, weight_sum = val
        if not np.isnan(weighted_sum) and not np.isnan(weight_sum):
            self.weighted_sum += weighted_sum
            self.weight_sum += weight_sum

    def finalize(self):
        if self.weight_sum > 0:
            return float(self.weighted_sum / self.weight_sum)
        return np.nan


class MeanOfRatiosAccumulator:
    """Unweighted mean over timesteps of per-timestep weighted ratios.

    Spatial functions paired with this return ``(weighted_sum, weight_sum)``
    tuples, like :class:`WeightedMeanAccumulator` — but each timestep
    contributes its *ratio* with equal weight, instead of pooling the parts
    across time. The two estimators diverge whenever the footprint's weight
    varies over time; this one matches the downstream ``compute_average``
    reference kernel (issue #213, parity question 1). Zero-weight (empty
    footprint) timesteps are skipped, mirroring the reference's NaN-skipping
    time mean.
    """

    def __init__(self):
        self.ratio_sum = 0.0
        self.count = 0

    def update(self, val):
        if val is None:
            return
        weighted_sum, weight_sum = val
        if np.isnan(weighted_sum) or np.isnan(weight_sum) or weight_sum == 0:
            return
        self.ratio_sum += weighted_sum / weight_sum
        self.count += 1

    def finalize(self):
        return float(self.ratio_sum / self.count) if self.count else np.nan


class FirstLandfallCapture:
    """Captures a value only at the first triggered (e.g. landfall) timestep."""

    def __init__(self):
        self.value = None

    def update(self, val):
        if self.value is None and val is not None:
            self.value = val

    def finalize(self):
        if self.value is None:
            return np.nan
        if isinstance(self.value, tuple):
            weighted_sum, weight_sum = self.value
            if weight_sum > 0:
                return float(weighted_sum / weight_sum)
            return np.nan
        return float(self.value)


registry.register_reducer("max", MaxAccumulator)
registry.register_reducer("min", MinAccumulator)
registry.register_reducer("sum", SumAccumulator)
registry.register_reducer("weighted_mean", WeightedMeanAccumulator)
registry.register_reducer("mean_of_ratios", MeanOfRatiosAccumulator)
registry.register_reducer("first_landfall", FirstLandfallCapture)

#: The reducer registry (``zagg.registry.REDUCERS``); plugins may add more via
#: ``registry.register_reducer``.
REDUCERS = registry.REDUCERS


# ---------------------------------------------------------------------------
# Per-timestep spatial functions (operate on gridded fields with masks)
# ---------------------------------------------------------------------------


def _apply_mask(storm_mask_t, ais_mask_subset, mask_type):
    """Combine an event mask with a spatial mask for one timestep.

    Parameters
    ----------
    storm_mask_t : xr.DataArray
        Binary event mask for one timestep (lat, lon).
    ais_mask_subset : xr.DataArray
        Spatial (e.g. ice-sheet) mask subsetted to the event extent.
    mask_type : str
        ``"ais"`` = event AND mask, ``"ocean"`` = event AND NOT mask,
        ``"full"`` = event only.

    Returns
    -------
    xr.DataArray
    """
    if mask_type == "ais":
        return storm_mask_t.where(ais_mask_subset, 0)
    elif mask_type == "ocean":
        return storm_mask_t.where(~ais_mask_subset, 0)
    else:
        return storm_mask_t


def spatial_max(var_t, combined_mask, cell_areas):
    """Max value under the masked footprint for one timestep.

    Selects the unscaled values under ``combined_mask > 0`` (matching
    :func:`spatial_min`) so the two agree for non-binary masks too.
    """
    masked = var_t.where(combined_mask > 0)
    vals = masked.values[~np.isnan(masked.values)]
    if len(vals) == 0:
        return np.nan
    return float(np.nanmax(vals))


def spatial_min(var_t, combined_mask, cell_areas):
    """Min value under the masked footprint for one timestep."""
    masked = var_t.where(combined_mask > 0)
    vals = masked.values[~np.isnan(masked.values)]
    if len(vals) == 0:
        return np.nan
    return float(np.nanmin(vals))


def spatial_weighted_sum(var_t, combined_mask, cell_areas):
    """Area-weighted sum under the masked footprint for one timestep."""
    return float((var_t * combined_mask * cell_areas).sum().values)


def spatial_weighted_mean_parts(var_t, combined_mask, cell_areas):
    """Return ``(weighted_sum, weight_sum)`` for a streaming mean."""
    weights = cell_areas * combined_mask
    weighted_sum = float((var_t * combined_mask * cell_areas).sum().values)
    weight_sum = float(weights.sum().values)
    return (weighted_sum, weight_sum)


def spatial_max_gradient(var_t, combined_mask, cell_areas):
    """Max gradient magnitude under the masked footprint for one timestep."""
    if (combined_mask == 0).all().values:
        return np.nan

    rads = var_t.assign_coords(
        lon=np.radians(var_t.lon),
        lat=np.radians(var_t.lat),
    )
    r = 6378  # Earth radius in km
    lat_partials = rads.differentiate("lat") / r
    # The longitude metric term carries 1/sin(lat), which blows up to inf at the
    # equator (sin(lat) == 0). Mask those rows out of the gradient rather than
    # letting inf propagate through nanmax.
    sin_lat = np.sin(rads.lat)
    lon_metric = sin_lat.where(sin_lat != 0) * r
    lon_partials = rads.differentiate("lon") / lon_metric

    magnitude = np.sqrt(lon_partials**2 + lat_partials**2)
    grad_vals = magnitude.values * combined_mask.values
    nonzero = grad_vals[combined_mask.values > 0]
    if len(nonzero) == 0:
        return np.nan
    return float(np.nanmax(nonzero))


def spatial_min_level_then_weighted_mean(var_t, combined_mask, cell_areas):
    """For 3-D vars: min over levels, then area-weighted mean.

    Returns a ``(weighted_sum, weight_sum)`` tuple.
    """
    var_2d = var_t.min("lev") if "lev" in var_t.dims else var_t

    weights = cell_areas * combined_mask
    weight_sum = float(weights.sum().values)
    if weight_sum == 0:
        return (np.nan, np.nan)

    weighted_sum = float((var_2d * combined_mask * cell_areas).sum().values)
    return (weighted_sum, weight_sum)


registry.register_spatial_func("max", spatial_max)
registry.register_spatial_func("min", spatial_min)
registry.register_spatial_func("weighted_sum", spatial_weighted_sum)
registry.register_spatial_func("weighted_mean", spatial_weighted_mean_parts)
registry.register_spatial_func("max_gradient", spatial_max_gradient)
registry.register_spatial_func("min_over_levels", spatial_min_level_then_weighted_mean)

#: The spatial-function registry (``zagg.registry.SPATIAL_FUNCS``); plugins may
#: add more via ``registry.register_spatial_func``.
SPATIAL_FUNCS = registry.SPATIAL_FUNCS

#: Built-in spatial funcs that multiply by ``cell_areas``; ``process_event``
#: refuses a spec using one of these when no ``cell_areas`` static field exists.
_AREA_WEIGHTED_FUNCS = frozenset({"weighted_sum", "weighted_mean", "min_over_levels"})


# ---------------------------------------------------------------------------
# Built-in mask providers and field transforms
#
# A mask provider has signature ``fn(event_mask_t, static_data, spec) -> mask``;
# a field transform has signature ``fn(var_t, static_data, spec) -> var_t``.
# These built-ins are domain-neutral (the *combine* op and a generic monthly
# anomaly). The *meaning* of "ais" — i.e. which static array is the ice sheet —
# is config (``data_source.static_data.ais_mask``); domain-specific providers
# such as precip masking are contributed by plugins.
# ---------------------------------------------------------------------------


def mask_full(event_mask_t, static_data, spec):
    """Event footprint only (no spatial mask)."""
    return event_mask_t


def mask_ais(event_mask_t, static_data, spec):
    """Event AND the ``ais_mask`` static field."""
    return _apply_mask(event_mask_t, static_data["ais_mask"].astype(bool), "ais")


def mask_ocean(event_mask_t, static_data, spec):
    """Event AND NOT the ``ais_mask`` static field."""
    return _apply_mask(event_mask_t, static_data["ais_mask"].astype(bool), "ocean")


registry.register_mask_provider("full", mask_full)
registry.register_mask_provider("ais", mask_ais)
registry.register_mask_provider("ocean", mask_ocean)

#: The mask-provider registry (``zagg.registry.MASK_PROVIDERS``).
MASK_PROVIDERS = registry.MASK_PROVIDERS


def monthly_anomaly(var_t, static_data, spec):
    """Subtract the monthly climatology for ``spec['variable']`` from ``var_t``.

    Expects ``static_data['climatology']`` to be an ``xr.Dataset`` with the
    aggregated variable indexed by a ``month`` dimension. The current month is
    read from ``var_t``'s ``time`` coordinate.
    """
    clim = static_data["climatology"][spec["variable"]]
    month = int(np.asarray(var_t["time"].dt.month))
    clim_m = clim.sel(month=month)
    # Align climatology to the timestep's spatial extent before subtracting.
    clim_m = clim_m.sel(lat=var_t["lat"].values, lon=var_t["lon"].values)
    return var_t - clim_m


registry.register_field_transform("monthly_anomaly", monthly_anomaly)

#: The field-transform registry (``zagg.registry.FIELD_TRANSFORMS``).
FIELD_TRANSFORMS = registry.FIELD_TRANSFORMS


# ---------------------------------------------------------------------------
# Built-in event triggers
#
# An event trigger has signature ``fn(event_mask, static_data, spec) ->
# timestep value(s)``; :func:`process_event` updates a spec's accumulator only
# on the returned timesteps. Never-firing triggers return an empty array, so
# every gated spec finalizes to NaN.
# ---------------------------------------------------------------------------


def first_intersection(event_mask, static_data, spec):
    """Earliest timestep where the event overlaps a named static mask.

    The mask is ``static_data[spec['trigger_mask']]`` (default ``ais_mask``),
    so the built-in expresses "first landfall on the ice sheet" without any
    domain knowledge: the *meaning* of the mask is config. Returns an empty
    array when the event never intersects it.

    Like the mask providers, the static mask must already share the event
    mask's spatial coordinates (``process_event`` subsets static fields to the
    event extent before calling triggers); misaligned coordinates raise
    xarray's exact-join alignment error.
    """
    name = spec.get("trigger_mask") or "ais_mask"
    if name not in static_data:
        raise ValueError(
            f"event trigger requires static field {name!r}, but static_data only has "
            f"{sorted(static_data)}"
        )
    static_mask = static_data[name].astype(bool)
    space_dims = [d for d in event_mask.dims if d != "time"]
    hits = (event_mask.where(static_mask, 0) > 0).any(space_dims)
    times = event_mask["time"].values[np.asarray(hits.values, dtype=bool)]
    if times.size == 0:
        return times
    return times.min()


registry.register_event_trigger(
    "first_intersection",
    first_intersection,
    description="First timestep where the event overlaps spec['trigger_mask'] (default 'ais_mask')",
)
registry.register_event_trigger(
    "first_landfall",
    first_intersection,
    description="Alias of first_intersection (first timestep on the ice-sheet mask)",
)

#: The event-trigger registry (``zagg.registry.EVENT_TRIGGERS``).
EVENT_TRIGGERS = registry.EVENT_TRIGGERS


# ---------------------------------------------------------------------------
# Event worker core
# ---------------------------------------------------------------------------


def process_event(
    event_key,
    event_mask,
    collections,
    specs,
    static_data,
    *,
    plugins=registry,
    max_resident_timesteps=None,
):
    """Aggregate one spatiotemporal event into a row of scalar attributes.

    The temporal counterpart of :func:`zagg.processing.process_shard`: it
    streams over an event's timesteps, applies a per-timestep spatial function
    under a mask, and reduces the results across time. All domain-specific
    behaviour (mask semantics, anomaly/derivation transforms, event triggers,
    spatial functions, reducers) is resolved *by name* through the plugin
    registry, so the loop itself carries no storm/AR knowledge.

    Parameters
    ----------
    event_key : hashable
        Identifier for this event (e.g. ``storm_id``); echoed in the output.
    event_mask : xr.DataArray
        Binary mask with dims ``(time, lat, lon)`` describing where/when the
        event is present.
    collections : dict[str, xr.Dataset]
        Source datasets keyed by collection name. May be lazy; this function
        subsets to the event extent and the current time batch, then
        ``.compute()``s, to bound memory.
    specs : list[dict]
        Aggregation specs (see :func:`specs_from_config`). Each must provide
        ``output_name``, ``variable``, ``collection``, ``spatial_func``,
        ``temporal_reducer``, ``mask``; optional keys: ``negate``,
        ``transform`` (field-transform name; ``anomaly: true`` desugars to
        ``transform: monthly_anomaly`` in :func:`specs_from_config`),
        ``trigger`` (event-trigger name) and ``trigger_mask`` (static-mask
        name the trigger tests against; ``first_intersection`` defaults to
        ``ais_mask``).
    static_data : dict[str, xr.DataArray | xr.Dataset]
        Static fields keyed by name, e.g. ``ais_mask``, ``cell_areas``,
        ``climatology``. Spatial fields are subset to the event extent.
    plugins : module or object, optional
        Resolver exposing ``get_reducer`` / ``get_spatial_func`` /
        ``get_mask_provider`` / ``get_field_transform`` / ``get_event_trigger``.
        Defaults to :mod:`zagg.registry`.
    max_resident_timesteps : int, optional
        Number of timesteps to hold in memory per batch. ``None`` loads all.

    Returns
    -------
    results : dict[str, float]
        ``{output_name: scalar}`` for every spec.
    metadata : dict
        ``event_key``, ``timesteps_processed``, ``n_specs``, ``collections``.
    """
    lats = event_mask["lat"].values
    lons = event_mask["lon"].values

    # Subset spatial static fields to the event extent once (climatology keeps
    # its month dimension; fields without lat/lon are passed through).
    static_sub = {}
    for name, arr in static_data.items():
        try:
            static_sub[name] = arr.sel(lat=lats, lon=lons)
        except (KeyError, ValueError):
            static_sub[name] = arr
    cell_areas = static_sub.get("cell_areas")
    if cell_areas is None and any(spec["spatial_func"] in _AREA_WEIGHTED_FUNCS for spec in specs):
        offenders = sorted(
            {spec["spatial_func"] for spec in specs if spec["spatial_func"] in _AREA_WEIGHTED_FUNCS}
        )
        raise ValueError(
            f"area-weighted spatial funcs {offenders} require a 'cell_areas' static field, "
            "but static_data has none"
        )

    # One accumulator per spec, plus resolved trigger timesteps (if any).
    accumulators = {}
    spec_triggers = {}
    for spec in specs:
        out = spec["output_name"]
        accumulators[out] = plugins.get_reducer(spec["temporal_reducer"])()
        trigger_name = spec.get("trigger")
        if trigger_name:
            trig = plugins.get_event_trigger(trigger_name)(event_mask, static_sub, spec)
            # Keep native dtype (e.g. datetime64) so membership matches `t`.
            spec_triggers[out] = set(np.atleast_1d(trig))
        else:
            spec_triggers[out] = None

    times = list(np.asarray(event_mask["time"].values))
    batch_size = max_resident_timesteps or len(times) or 1
    n_processed = 0

    for start in range(0, len(times), batch_size):
        batch_times = times[start : start + batch_size]

        # Load each needed collection for this batch, bounded to extent/time.
        loaded = {}
        for spec in specs:
            cname = spec["collection"]
            if cname in loaded:
                continue
            sub = collections[cname].sel(lat=lats, lon=lons).sel(time=batch_times)
            loaded[cname] = sub.compute() if hasattr(sub, "compute") else sub

        for t in batch_times:
            storm_t = event_mask.sel(time=t)
            for spec in specs:
                out = spec["output_name"]
                trig = spec_triggers[out]
                if trig is not None and t not in trig:
                    continue

                var_t = loaded[spec["collection"]][spec["variable"]].sel(time=t)
                # Single transform apply path (``anomaly: true`` desugars here).
                if spec.get("transform"):
                    var_t = plugins.get_field_transform(spec["transform"])(var_t, static_sub, spec)
                if spec.get("negate"):
                    var_t = -var_t

                mask_t = plugins.get_mask_provider(spec["mask"])(storm_t, static_sub, spec)
                value = plugins.get_spatial_func(spec["spatial_func"])(var_t, mask_t, cell_areas)
                accumulators[out].update(value)
            n_processed += 1

    results = {out: acc.finalize() for out, acc in accumulators.items()}
    metadata = {
        "event_key": event_key,
        "timesteps_processed": n_processed,
        "n_specs": len(specs),
        "collections": sorted({s["collection"] for s in specs}),
    }
    return results, metadata


# ---------------------------------------------------------------------------
# Config bridge
# ---------------------------------------------------------------------------


def specs_from_config(config):
    """Convert temporal aggregation config to internal spec dicts.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[dict]
        Each dict has keys: ``output_name``, ``variable``, ``collection``,
        ``spatial_func``, ``temporal_reducer``, ``mask``, ``negate``, and the
        optional generic hooks ``transform`` (field-transform name),
        ``trigger`` (event-trigger name), ``trigger_mask`` (static-mask
        name for the trigger), and ``params`` (a free-form mapping passed
        through verbatim -- capabilities receive the full spec, so registered
        masks/transforms/triggers read their tuning knobs from
        ``spec["params"]`` without any zagg change).

    Notes
    -----
    ``anomaly: true`` is pure sugar for ``transform: monthly_anomaly``: it is
    desugared here so :func:`process_event` has a *single* transform apply path
    (the generic ``transform`` hook) and a spec carrying both keys can never
    double-apply the climatology subtraction (issue #12,
    https://github.com/englacial/zagg/pull/70#issuecomment-4835411687). An
    explicit ``transform`` wins if both are set; a same-named ``transform``
    therefore collapses to a single application.
    """
    specs = []
    for name, meta in config.aggregation.get("variables", {}).items():
        transform = meta.get("transform")
        if transform is None and meta.get("anomaly", False):
            transform = "monthly_anomaly"
        specs.append(
            {
                "output_name": name,
                "variable": meta["variable"],
                "collection": meta["collection"],
                "spatial_func": meta["spatial_func"],
                "temporal_reducer": meta["temporal_reducer"],
                "mask": meta.get("mask", "ais"),
                "negate": meta.get("negate", False),
                "transform": transform,
                "trigger": meta.get("trigger"),
                "trigger_mask": meta.get("trigger_mask"),
                "params": dict(meta.get("params") or {}),
            }
        )
    return specs


# ---------------------------------------------------------------------------
# Temporal reader (load collections + static_data from S3 / local)
# ---------------------------------------------------------------------------


def open_dataset(uri, *, credentials=None, endpoint_url=None, region="us-west-2", unsigned=False):
    """Open a single xarray ``Dataset`` from a local path or ``s3://`` URI.

    A ``.zarr`` URI opens through :func:`zagg.store.open_store` (the obstore S3
    stack the rest of zagg uses); any other suffix (NetCDF/HDF5) is fetched as
    bytes and opened in-memory, so the reader needs no ``s3fs``/``fsspec`` on the
    Lambda worker. ``credentials``/``endpoint_url`` are the camelCase write/read
    creds used elsewhere; omit to use the ambient chain (execution role).
    ``unsigned`` sends anonymous (unsigned) requests — the mechanism for public
    buckets, where a request *signed* by scoped credentials is denied by the
    cross-account rule even though anonymous access is granted (issue #223).
    """
    import xarray as xr

    if unsigned and credentials:
        raise ValueError("unsigned=True and explicit credentials are mutually exclusive")

    if uri.endswith(".zarr"):
        from .store import open_store

        store = open_store(
            uri,
            read_only=True,
            credentials=credentials,
            endpoint_url=endpoint_url,
            region=region,
            **({"skip_signature": True} if unsigned else {}),
        )
        return xr.open_zarr(store)

    if not uri.startswith("s3://"):
        return xr.open_dataset(uri)

    import io

    import obstore

    from .store import _S3_READONLY_RETRY_CONFIG, open_object_store, parse_s3_path

    bucket, key = parse_s3_path(uri)
    # Pure read of a static input: same failure-latency policy as the .zarr
    # branch above (open_object_store has no read_only concept, issue #186).
    store = open_object_store(
        f"s3://{bucket}",
        credentials=credentials,
        endpoint_url=endpoint_url,
        region=region,
        retry_config=_S3_READONLY_RETRY_CONFIG,
        **({"skip_signature": True} if unsigned else {}),
    )
    payload = obstore.get(store, key).bytes()
    return xr.open_dataset(io.BytesIO(bytes(payload)))


def _input_channel(input_credentials):
    """Resolve the consumer-input credential channel (issue #223).

    ``input_credentials`` is the payload field covering ``event_mask_uri`` and
    ``static_uris`` — consumer-owned inputs, as opposed to the source
    collections that ``s3_credentials`` was fetched for: an explicit camelCase
    creds dict, the string ``"unsigned"`` (anonymous requests — public
    buckets), or ``None`` for the ambient chain (execution role).

    Returns
    -------
    tuple[dict | None, bool]
        ``(credentials, unsigned)`` ready for :func:`open_dataset`.
    """
    if input_credentials == "unsigned":
        return None, True
    if input_credentials is None or isinstance(input_credentials, dict):
        return input_credentials, False
    raise ValueError(
        f"input_credentials must be a credentials dict, 'unsigned', or None "
        f"(got {input_credentials!r})"
    )


def _eval_derived(name, expression, ds):
    """Evaluate a ``derived`` variable expression over a collection's variables.

    Same restricted-namespace contract as the spatial pipeline's ``expression``
    fields (numpy + the collection's data variables; no builtins)."""
    ns = {"__builtins__": {}, "np": np, "numpy": np, **{v: ds[v] for v in ds.data_vars}}
    try:
        return eval(expression, ns)  # noqa: S307
    except NameError as e:
        raise ValueError(
            f"derived variable {name!r}: {e}; available variables: {sorted(ds.data_vars)}"
        ) from None


def prepare_collection(ds, options):
    """Apply declarative per-collection reader options (issue #213, Phase 3).

    Applied in order: ``variables`` (subset the collection), ``time_offset``
    (shift timestamps, e.g. ``"-30min"`` moves half-hour stamps onto the hour),
    ``resample: {freq, how, scale}`` (scale then resample -- turns 1-hourly
    rates into e.g. 3-hourly totals), ``derived`` (materialize new variables
    from numpy expressions over the collection's variables). The order matches
    the MERRA-2 precip flow these options generalize; a falsy ``options``
    returns ``ds`` unchanged. Unknown option keys (e.g. ``doi``) are ignored
    here -- they are metadata for catalog tooling.
    """
    if not options:
        return ds
    variables = options.get("variables")
    if variables:
        ds = ds[list(variables)]
    offset = options.get("time_offset")
    if offset:
        import pandas as pd

        ds = ds.assign_coords(time=ds["time"] + pd.to_timedelta(offset).to_timedelta64())
    resample = options.get("resample")
    if resample:
        scale = resample.get("scale", 1)
        if scale != 1:
            ds = ds * scale
        ds = getattr(ds.resample(time=resample["freq"]), resample.get("how", "sum"))()
    for name, expr in (options.get("derived") or {}).items():
        ds = ds.assign(**{name: _eval_derived(name, expr, ds)})
    return ds


def read_temporal_inputs(
    collection_uris,
    static_uris,
    *,
    credentials=None,
    endpoint_url=None,
    region="us-west-2",
    collection_options=None,
    input_credentials=None,
    extent=None,
):
    """Load the ``collections`` and ``static_data`` :func:`process_event` needs.

    The reader half of the Lambda ``process_event`` mode (issue #12, Phase 7b),
    mirroring what the local ``TemporalStrategy`` receives ready-made: each maps
    a name to a dataset/array opened from S3 (or local). Collections stay full
    ``Dataset``s; static entries are returned as single ``DataArray``s when the
    file holds exactly one variable (the mask/anomaly providers index a single
    field), else as the ``Dataset``.

    Parameters
    ----------
    collection_uris : dict[str, str | list[str]]
        ``{collection_name: uri or [uris]}`` for each collection the specs
        read. A list (an event spanning several granules) is opened per-URI,
        concatenated along ``time``, and time-sorted.
    static_uris : dict[str, str]
        ``{static_name: uri}`` for e.g. ``ais_mask`` / ``climatology`` /
        ``cell_areas``.
    credentials, endpoint_url, region
        Forwarded to :func:`open_dataset` for the **collections** — the source
        datasets these credentials were fetched for (e.g. the GES DISC STS
        creds from ``credentials_provider``).
    collection_options : dict[str, dict], optional
        Per-collection declarative options applied by
        :func:`prepare_collection`; the runner derives this from
        ``data_source.collections`` via :func:`zagg.config.collection_options`.
    input_credentials : dict | str | None, optional
        Credential channel for the **consumer-owned statics** (issue #223): an
        explicit creds dict, ``"unsigned"`` (anonymous — public buckets), or
        ``None`` for the ambient chain. Source credentials scoped to GES DISC
        (or any other provider) are denied on other buckets by the
        cross-account rule, so statics must not ride the collections channel.
    extent : tuple, optional
        ``(lats, lons)`` coordinate arrays of the event (issue #225). When
        given, every granule is subset to the event extent (and the
        collection's ``variables``, when configured), **loaded**, and its
        backing byte buffer released before the next granule opens — bounding
        peak worker memory to ~one granule regardless of storm length. Without
        it, datasets stay lazy over their full-file buffers (the pre-#225
        behavior); on a 4 GB Lambda a multi-day, multi-collection event OOMs
        that way.

    Returns
    -------
    tuple[dict, dict]
        ``(collections, static_data)`` ready for :func:`process_event`.
    """
    kw = {"credentials": credentials, "endpoint_url": endpoint_url, "region": region}
    in_creds, unsigned = _input_channel(input_credentials)
    static_kw = {
        "credentials": in_creds,
        "endpoint_url": endpoint_url,
        "region": region,
        "unsigned": unsigned,
    }
    options = collection_options or {}
    collections = {}
    for name, uris in collection_uris.items():
        uri_list = list(uris) if isinstance(uris, (list, tuple)) else [uris]
        keep = (options.get(name) or {}).get("variables")
        parts = []
        for u in uri_list:
            ds = open_dataset(u, **kw)
            if extent is not None:
                lats, lons = extent
                part = ds[list(keep)] if keep else ds
                part = part.sel(lat=lats, lon=lons).load()
                ds.close()
                ds = part
            parts.append(ds)
        if len(parts) == 1:
            ds = parts[0]
        else:
            import xarray as xr

            ds = xr.concat(parts, dim="time").sortby("time")
        collections[name] = prepare_collection(ds, options.get(name))
    static_data = {}
    for name, uri in static_uris.items():
        ds = open_dataset(uri, **static_kw)
        data_vars = list(ds.data_vars)
        static_data[name] = ds[data_vars[0]] if len(data_vars) == 1 else ds
    return collections, static_data


def _xarray_s3_reader(collection_uris, static_uris, **kwargs):
    """Registry entry for the built-in reader.

    Late-binds the module-level :func:`read_temporal_inputs` so a runtime
    override of that name is honored by registry resolution too."""
    return read_temporal_inputs(collection_uris, static_uris, **kwargs)


registry.register_reader(
    "xarray_s3",
    _xarray_s3_reader,
    description=(
        "obstore/xarray reader: Zarr or NetCDF from local/s3:// URIs; multi-URI "
        "collections concat along time; applies declarative collection_options"
    ),
)

#: The reader registry (``zagg.registry.READERS``).
READERS = registry.READERS
