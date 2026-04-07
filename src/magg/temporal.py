"""Temporal aggregation pipeline for magg.

Provides streaming accumulators for cross-timestep reduction and
per-timestep spatial functions for gridded data with masks. These are
the building blocks for temporal and event-based aggregation pipelines
(e.g., computing storm summary statistics from reanalysis data).

Ported from antarctic_AR_dataset/artools/cloud/.
"""

import numpy as np


# ---------------------------------------------------------------------------
# Temporal accumulators
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


class FirstLandfallCapture:
    """Captures a value only at the first landfall timestep."""

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


TEMPORAL_REDUCERS = {
    "max": MaxAccumulator,
    "min": MinAccumulator,
    "sum": SumAccumulator,
    "weighted_mean": WeightedMeanAccumulator,
    "first_landfall": FirstLandfallCapture,
}


# ---------------------------------------------------------------------------
# Per-timestep spatial functions (operate on gridded fields with masks)
# ---------------------------------------------------------------------------

def _apply_mask(storm_mask_t, ais_mask_subset, mask_type):
    """Combine storm mask with spatial mask for one timestep.

    Parameters
    ----------
    storm_mask_t : xr.DataArray
        Binary storm mask for one timestep (lat, lon).
    ais_mask_subset : xr.DataArray
        AIS mask subsetted to storm extent.
    mask_type : str
        ``"ais"`` = storm AND AIS, ``"ocean"`` = storm AND NOT AIS,
        ``"full"`` = storm only.

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
    """Max value under masked footprint for one timestep."""
    vals = (var_t * combined_mask).values[combined_mask.values > 0]
    if len(vals) == 0:
        return np.nan
    return float(np.nanmax(vals))


def spatial_min(var_t, combined_mask, cell_areas):
    """Min value under masked footprint for one timestep."""
    masked = var_t.where(combined_mask > 0)
    vals = masked.values[~np.isnan(masked.values)]
    if len(vals) == 0:
        return np.nan
    return float(np.nanmin(vals))


def spatial_weighted_sum(var_t, combined_mask, cell_areas):
    """Area-weighted sum under masked footprint for one timestep."""
    return float((var_t * combined_mask * cell_areas).sum().values)


def spatial_weighted_mean_parts(var_t, combined_mask, cell_areas):
    """Return ``(weighted_sum, weight_sum)`` for streaming mean."""
    weights = cell_areas * combined_mask
    weighted_sum = float((var_t * combined_mask * cell_areas).sum().values)
    weight_sum = float(weights.sum().values)
    return (weighted_sum, weight_sum)


def spatial_max_gradient(var_t, combined_mask, cell_areas):
    """Max gradient magnitude under masked footprint for one timestep."""
    if (combined_mask == 0).all().values:
        return np.nan

    rads = var_t.assign_coords(
        lon=np.radians(var_t.lon),
        lat=np.radians(var_t.lat),
    )
    r = 6378  # Earth radius in km
    lat_partials = rads.differentiate("lat") / r
    lon_partials = rads.differentiate("lon") / (np.sin(rads.lat) * r)

    magnitude = np.sqrt(lon_partials ** 2 + lat_partials ** 2)
    grad_vals = magnitude.values * combined_mask.values
    nonzero = grad_vals[combined_mask.values > 0]
    if len(nonzero) == 0:
        return np.nan
    return float(np.nanmax(nonzero))


def spatial_min_level_then_weighted_mean(var_t, combined_mask, cell_areas):
    """For 3D vars: min over levels, then area-weighted mean.

    Returns ``(weighted_sum, weight_sum)`` tuple.
    """
    var_2d = var_t.min("lev") if "lev" in var_t.dims else var_t

    weights = cell_areas * combined_mask
    weight_sum = float(weights.sum().values)
    if weight_sum == 0:
        return (np.nan, np.nan)

    weighted_sum = float((var_2d * combined_mask * cell_areas).sum().values)
    return (weighted_sum, weight_sum)


SPATIAL_FUNCTIONS = {
    "max": spatial_max,
    "min": spatial_min,
    "weighted_sum": spatial_weighted_sum,
    "weighted_mean": spatial_weighted_mean_parts,
    "max_gradient": spatial_max_gradient,
    "min_over_levels": spatial_min_level_then_weighted_mean,
}


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
        ``spatial_func``, ``temporal_reducer``, ``mask``, ``is_anomaly``,
        ``negate``, ``precip``.
    """
    specs = []
    for name, meta in config.aggregation.get("variables", {}).items():
        specs.append({
            "output_name": name,
            "variable": meta["variable"],
            "collection": meta["collection"],
            "spatial_func": meta["spatial_func"],
            "temporal_reducer": meta["temporal_reducer"],
            "mask": meta.get("mask", "ais"),
            "is_anomaly": meta.get("anomaly", False),
            "negate": meta.get("negate", False),
            "precip": meta.get("precip", False),
        })
    return specs
