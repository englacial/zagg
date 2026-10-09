"""The 88S reassessment latitude-sweep matrix (issue #148).

``tests/data/benchmark/targets_88s_latitude_sweep.json`` is the sweep the
reassessment asks for: one o9 shard per 0.3-deg band at 85 / 85.5 / 86 / 86.5 /
87 / 87.5 degrees south plus the pinned 88S row, each on a 4 GB-disk and an
8 GB-disk worker, all on the shipped sidecar + hive + spill + sharded
configuration. The matrix is DATA -- the benchmark directory's README is explicit
that reshaping the matrix is a data edit there, with no change to the workflow or
the runner -- so these tests are what stops that data from being wrong.

They check three things the manifest cannot check about itself: that the sweep
really is the base configuration (the per-target keys AND the config they point
at), that the pinned/unpinned split stays consistent as bands get pinned, and
that the pinned 88S row is the committed ``targets.json`` pin rather than a fork
of it.

Offline: no CMR, no AWS. The one dispatch path exercised is ``dry_run``.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BENCH = REPO / "tests" / "data" / "benchmark"
# Neither ``.github/scripts`` nor ``tools`` is an installed package, so both go
# on the path ONCE here rather than inside the tests that use them -- an insert
# in a test body stacks an identical entry per call.
sys.path.insert(0, str(REPO / ".github" / "scripts"))
sys.path.insert(0, str(REPO / "tools"))

import bench_metrics  # noqa: E402
import make_lat_ring_aoi as ring  # noqa: E402
import run_benchmark  # noqa: E402

SWEEP = BENCH / "targets_88s_latitude_sweep.json"

#: The bands the reassessment names, in degrees SOUTH, and the shard-map entry
#: each one is keyed by.
BANDS = {
    "healpix_o9_85s": 85.0,
    "healpix_o9_85_5s": 85.5,
    "healpix_o9_86s": 86.0,
    "healpix_o9_86_5s": 86.5,
    "healpix_o9_87s": 87.0,
    "healpix_o9_87_5s": 87.5,
    "healpix_o9_88s": 88.0,
}

#: The two worker arms: "4GB worker with disk" and "8GB worker with disk".
ARMS = [4096, 8192]


@pytest.fixture(scope="module")
def manifest():
    loaded, _ = run_benchmark.load_targets(str(SWEEP))
    return loaded


def all_rows(manifest: dict) -> dict:
    """Every sweep row, pinned or not, minus the ``pending_targets`` prose key."""
    pending = {k: v for k, v in manifest["pending_targets"].items() if k != "_comment"}
    return {**manifest["targets"], **pending}


def test_the_seven_bands_are_the_ones_the_reassessment_names(manifest):
    """No band added, none dropped -- the sweep's x-axis is the requested one."""
    assert list(manifest["shardmaps"]) == list(BANDS)


def test_every_band_carries_its_own_committed_aoi(manifest):
    """Each entry names its own ring geojson, and that file is the one phase 1 generates.

    The manifest has no top-level ``aoi`` on purpose (every band IS a different
    AOI, so a missing override would silently inherit a neighbour's ring, or
    ``bench_metrics.resolve_aoi_temporal_cmr``'s NEON fallback). That only holds
    while every entry actually carries one.
    """
    assert "aoi" not in manifest
    for sm_key, lat in BANDS.items():
        aoi = manifest["shardmaps"][sm_key]["aoi"]
        assert aoi["file"] == ring.aoi_filename(lat)
        assert (BENCH / aoi["file"]).exists()


def test_the_matrix_is_seven_bands_by_two_worker_arms(manifest):
    """Fourteen rows, each (band, memory) pair present exactly once, no duplicate names."""
    rows = all_rows(manifest)
    assert len(rows) == len(BANDS) * len(ARMS)
    assert not set(manifest["targets"]) & set(manifest["pending_targets"])
    seen = {(row["shardmap"], row["worker"]["memory"]) for row in rows.values()}
    assert seen == {(sm_key, memory) for sm_key in BANDS for memory in ARMS}


def test_every_row_is_the_reassessment_base_configuration(manifest):
    """sidecar + spill + sharded + no AOI mask + tdigest on a disk worker, on every row.

    This is the whole premise of the sweep: latitude and worker size are the only
    things that vary, so a row that quietly differs on the read backend or the
    streaming mode would put an unrelated A/B on the same axis.
    """
    for name, row in all_rows(manifest).items():
        assert row["index_backend"] == "sidecar", name
        assert row["streaming_mode"] == "spill", name
        assert row["sharded"] is True, name
        assert row["aoi_mask"] is False, name
        assert row["aggregator"] == "tdigest", name
        assert row["grid_type"] == "healpix", name
        assert row["worker"]["extra_disk"] is True, name
        assert row["worker"]["memory"] in ARMS, name
        assert (BENCH / row["config"]).exists(), name


def test_the_config_the_rows_point_at_is_hive_o9_sharded(manifest):
    """The half of the base configuration that lives in the config, not the target.

    ``hive`` and ``o9`` are config properties -- ``run_benchmark`` never sets
    them per target -- so asserting the target keys alone would leave the two
    words in the reassessment's base configuration unchecked.
    """
    from zagg.config import get_aoi_mask, get_store_layout, load_config

    configs = {row["config"] for row in all_rows(manifest).values()}
    assert len(configs) == 1, "one pipeline config for the whole sweep"
    config = load_config(str(BENCH / configs.pop()))
    assert get_store_layout(config) == "hive"
    assert config.output["grid"]["parent_order"] == 9
    assert config.output["grid"]["sharded"] is True
    assert config.data_source["granule_workers"] == 4
    # The no-mask arm is what keeps the absent top-level ``aoi`` safe:
    # ``run_target`` reads ``manifest["aoi"]`` only when the config asks for a
    # strict AOI mask.
    assert get_aoi_mask(config) is False


@pytest.mark.parametrize("memory", ARMS)
def test_worker_blocks_resolve_the_provisioned_disk_variants(manifest, memory):
    """``worker`` resolves to ``process-shard-<memory>-disk``, the variants that must exist."""
    row = next(r for r in all_rows(manifest).values() if r["worker"]["memory"] == memory)
    resolved = run_benchmark.resolve_variant(manifest["dispatch"]["function_name"], row["worker"])
    assert resolved == f"process-shard-{memory}-disk"


def test_pinned_and_unpinned_bands_stay_consistent(manifest):
    """A band is pinned and in ``targets``, or unpinned and in ``pending_targets`` -- never mixed.

    This is the assertion that makes pinning a band a safe edit: moving its rows
    across without writing the pin (or writing the pin without moving the rows)
    fails here instead of failing mid-dispatch, where ``run_target`` reads
    ``shard_key`` unconditionally and would raise a bare ``KeyError``.
    """
    for sm_key, meta in manifest["shardmaps"].items():
        pinned = "shard_key" in meta
        assert pinned != (meta.get("pin") == "unpinned"), f"{sm_key}: pin state is ambiguous"
        block = manifest["targets"] if pinned else manifest["pending_targets"]
        rows = [name for name, row in all_rows(manifest).items() if row["shardmap"] == sm_key]
        assert len(rows) == len(ARMS)
        for name in rows:
            assert name in block, f"{name}: pin state and target block disagree"
        if pinned:
            assert (BENCH / meta["path"]).exists()
            assert meta["n_granules"] > 0


def test_the_88s_row_is_the_committed_pin_not_a_fork(manifest):
    """The one pinned band reuses ``targets.json``'s entry verbatim.

    Forking it would put the mission's worst shard behind two pins that can
    drift apart, and only one of them (``targets.json``'s) is covered by the
    drift guard and the re-pin driver.
    """
    committed = json.loads((BENCH / "targets.json").read_text())["shardmaps"]["healpix_o9_88s"]
    ours = manifest["shardmaps"]["healpix_o9_88s"]
    for key in ("path", "shard_key", "n_granules", "catalog_parquet", "aoi"):
        assert ours[key] == committed[key], key


def test_the_pinned_map_still_selects_its_pin(manifest):
    """The committed map re-derives the pin, through the same rule the matrix uses.

    Offline and cheap: the map is committed pruned to the pinned shard, so this
    reads one shard, not the 0.7 GB ring.
    """
    meta = manifest["shardmaps"]["healpix_o9_88s"]
    shardmap = json.loads((BENCH / meta["path"]).read_text())
    shard_key, n_granules = bench_metrics.select_densest_shard(shardmap)
    assert (shard_key, n_granules) == (meta["shard_key"], meta["n_granules"])


def test_pinned_rows_dispatch_dry(manifest):
    """``run_target --dry-run`` resolves every pinned row end to end, touching no AWS.

    The wiring check the manifest most needs: config load, grid construction,
    backend/streaming/worker injection and record building all run, and a real
    dispatch would be the only remaining step.
    """
    for name in manifest["targets"]:
        record = run_benchmark.run_target(
            name,
            manifest,
            BENCH,
            store="",
            region="us-west-2",
            function_name=manifest["dispatch"]["function_name"],
            context={"commit": "test"},
            dry_run=True,
        )
        assert record["target"] == name
        # Each row's OWN band, looked up through the row: a pinned band moves
        # its two rows into ``targets`` (_pin_recipe step 4), so an expectation
        # hard-coded to one band would go red on the next band pinned and read
        # as a dispatch bug in the row someone just added.
        band = manifest["shardmaps"][manifest["targets"][name]["shardmap"]]
        assert record["shard_key"] == band["shard_key"]
        assert record["streaming_mode"] == "spill"
        assert record["index_backend"] == "sidecar"
        assert record["store_layout"] == "hive"


def test_the_sweep_temporal_window_matches_the_snapshot_that_pins_it(manifest):
    """The sweep window is the one ``cat_88s.parquet`` was fetched over.

    An entry carrying ``catalog_parquet`` rebuilds from the snapshot, so the
    snapshot's window -- not the manifest's -- is what the 88S pin actually
    reflects. Recording a different window here would make the sweep's bands
    incomparable to the row they extend.
    """
    import pyarrow.parquet as pq

    path = BENCH / manifest["shardmaps"]["healpix_o9_88s"]["catalog_parquet"]
    meta = pq.ParquetFile(path).schema_arrow.metadata[b"zagg:catalog_meta"]
    snapshot = json.loads(meta.decode())
    assert manifest["temporal"]["start"] == snapshot["start_date"]
    assert manifest["temporal"]["end"] == snapshot["end_date"]
