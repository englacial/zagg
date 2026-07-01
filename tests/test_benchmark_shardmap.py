"""Shard-map drift check for the pinned benchmark targets (issue #110).

Rebuilds each pinned shard map from CMR (same AOI + temporal window + grid as
``targets.json``) and asserts the densest shard hasn't materially drifted, so a
silent change in CMR coverage that would move the benchmark target gets caught
loudly instead of surfacing as a phantom cost/runtime regression.

This needs the network (CMR) and, for the rectilinear maps, the exact-S2
``spherely`` backend, so it is decoupled from the unit suite: it runs only when
``ZAGG_BENCHMARK_DRIFT=1`` is set (the `benchmark-drift` workflow does this on a
native x86_64 runner where the spherely wheel installs). The check is
**tie-tolerant** -- several shards tie for densest in this AOI, and the lowest-key
tiebreak is deterministic but fragile to a +/-0 count nudge, so we compare the
densest *granule count* (within +/-1), not the exact shard key.
"""

import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(REPO / ".github" / "scripts"))
import bench_metrics  # noqa: E402

MANIFEST = json.loads((BENCH / "targets.json").read_text())


def resolve_aoi_temporal_cmr(sm_meta: dict) -> tuple[dict, dict, dict]:
    """Resolve a shard map's ``aoi``/``temporal``/``cmr`` (issue #121).

    A per-entry override wins; an absent key falls back to the top-level manifest
    default. Existing single-AOI (NEON) shard maps carry no override, so they
    resolve byte-identically to the top-level ``aoi``/``temporal``/``cmr``.
    """
    return (
        sm_meta.get("aoi", MANIFEST["aoi"]),
        sm_meta.get("temporal", MANIFEST["temporal"]),
        sm_meta.get("cmr", MANIFEST["cmr"]),
    )


pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("ZAGG_BENCHMARK_DRIFT") != "1",
        reason="set ZAGG_BENCHMARK_DRIFT=1 to run the CMR shard-map drift check",
    ),
]


def _containing_shard(parent_grid, shard_key: int) -> int:
    """The parent-grid shard containing a finer shard (via its center point).

    HEALPix cells nest, so a finer cell's center maps unambiguously into its
    containing coarser cell; routing through ``assign``/``shards_of`` keeps this
    on the same mortie machinery the shard maps themselves are built with.
    """
    import numpy as np
    from mortie import mort2geo

    lat, lon = mort2geo(np.array([shard_key], dtype=np.uint64))
    leaf = parent_grid.assign(np.atleast_1d(lat), np.atleast_1d(lon))
    return int(parent_grid.shards_of(leaf)[0])


def _config_for_shardmap(sm_key: str) -> Path:
    """Any target's config that uses this shard map (config sets the grid).

    Searches the committed matrix first, then ``provisional_targets`` (issue
    #130 block) — the 88S stress shard maps (issue #148) are referenced only by
    provisional targets, and the drift check still needs their grid config.
    """
    provisional = {
        k: v for k, v in MANIFEST.get("provisional_targets", {}).items() if k != "_comment"
    }
    for target in list(MANIFEST["targets"].values()) + list(provisional.values()):
        if target["shardmap"] == sm_key:
            return BENCH / target["config"]
    raise AssertionError(f"no target references shardmap '{sm_key}'")


@pytest.mark.parametrize("sm_key", list(MANIFEST["shardmaps"]))
def test_pinned_shardmap_no_drift(sm_key):
    from zagg.catalog import load_polygon, polygon_to_bbox
    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import CMRSource, Query
    from zagg.config import load_config
    from zagg.grids import from_config

    sm_meta = MANIFEST["shardmaps"][sm_key]
    committed = json.loads((BENCH / sm_meta["path"]).read_text())
    backend = committed["metadata"]["backend"]
    if backend == "spherely":
        pytest.importorskip("spherely")

    cfg = load_config(str(_config_for_shardmap(sm_key)))
    grid = from_config(cfg)
    # Resolve this shard map's AOI/temporal/CMR: a per-entry override (issue #121)
    # falls back to the top-level manifest default. NEON entries have no override.
    aoi, temporal, cmr = resolve_aoi_temporal_cmr(sm_meta)
    # aoi.file is relative to the manifest dir, like the config/shardmap paths.
    parts = load_polygon(str(BENCH / aoi["file"]))
    bbox = polygon_to_bbox(parts)

    query = Query(
        cmr["short_name"],
        cmr["version"],
        temporal["start"],
        temporal["end"],
        region=bbox,
        provider=cmr["provider"],
    )
    catalog = CMRSource().fetch(query)
    rebuilt = ShardMap.build(
        catalog, grid, region=parts, backend=backend, footprint=cmr["footprint"]
    )

    # A nested pin (issue #148: the 88S o10 stress shard is the densest o10
    # shard INSIDE the pinned o9 stress shard, so one o9 extraction pass covers
    # both orders) is compared against the same nested quantity, not the global
    # densest — otherwise a correct rebuild would read as drift.
    shard_keys, granules = rebuilt.shard_keys, rebuilt.granules
    nested_in = sm_meta.get("nested_in")
    if nested_in:
        parent_key = int(MANIFEST["shardmaps"][nested_in]["shard_key"])
        parent_grid = from_config(load_config(str(_config_for_shardmap(nested_in))))
        keep = [
            i
            for i, k in enumerate(shard_keys)
            if int(_containing_shard(parent_grid, int(k))) == parent_key
        ]
        shard_keys = [shard_keys[i] for i in keep]
        granules = [granules[i] for i in keep]

    key, n = bench_metrics.select_densest_shard({"shard_keys": shard_keys, "granules": granules})
    pinned_n = sm_meta["n_granules"]
    # Tie-tolerant: the densest *count* is the stable quantity; an equally-dense
    # reselection (different key, same count) is fine -- a count drift is not.
    assert abs(n - pinned_n) <= 1, (
        f"{sm_key}: densest granule count drifted {pinned_n} -> {n} "
        f"(rebuilt densest shard {key}). Re-pin the shard map + targets.json."
    )
