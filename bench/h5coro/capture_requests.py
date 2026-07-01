"""Capture the exact h5coro read workload of one zagg benchmark shard (issue #149).

Runs the real worker (``zagg.processing.process_shard``) over one benchmark
shard's locally cached granules with h5coro's ``FileDriver``, recording every
``H5Coro.readDatasets`` call (dataset path + hyperslice, per call, per granule)
to JSON. The captured request lists are the frozen workload that every backend
variant replays (``bench_replay.py``), so all candidates are measured on the
byte-identical read pattern zagg actually issues — not a full-file scan.

The wall/CPU/RSS numbers recorded here are informative only (capture adds
wrapper overhead); measured baselines come from ``bench_replay.py``.

Usage (repo root, dev venv)::

    python bench/h5coro/capture_requests.py --order 10 \
        --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules
"""

import argparse
import json
import os
import resource
import sys
import time
from pathlib import Path

from h5coro import filedriver
from h5coro.h5coro import H5Coro

from zagg.config import get_handoff, load_config
from zagg.grids import from_config
from zagg.processing import process_shard

REPO = Path(__file__).resolve().parents[2]
CONFIGS = REPO / "tests" / "data" / "benchmark" / "configs"
SHARDMAPS = REPO / "tests" / "data" / "benchmark" / "shardmaps"


def max_rss_mb() -> float:
    """Peak RSS of this process in MB (ru_maxrss is bytes on macOS, KB on Linux)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 2**20 if sys.platform == "darwin" else rss / 2**10


def densest_shard(shardmap: dict) -> tuple[int, list[str]]:
    """Return (shard_key, granule basenames) for the shard with the most granules."""
    keys, granules = shardmap["shard_keys"], shardmap["granules"]
    i = max(range(len(keys)), key=lambda j: len(granules[j]))
    names = [g["https"].split("/")[-1] for g in granules[i]]
    return int(keys[i]), names


def _jsonable_hyperslice(hyperslice) -> list | None:
    if hyperslice is None:
        return None
    return [[int(a), int(b)] for a, b in hyperslice]


def install_recorder() -> dict[str, list]:
    """Wrap ``H5Coro.readDatasets`` to log each call's entries, keyed by resource."""
    calls_by_resource: dict[str, list] = {}
    original = H5Coro.readDatasets

    def recording(self, datasets, **kwargs):
        entries = []
        for d in datasets:
            if isinstance(d, str):
                entries.append({"dataset": d, "hyperslice": None})
            else:
                entries.append(
                    {
                        "dataset": d["dataset"],
                        "hyperslice": _jsonable_hyperslice(d.get("hyperslice")),
                    }
                )
        resource_name = os.path.basename(str(getattr(self, "resource", "?")))
        calls_by_resource.setdefault(resource_name, []).append(entries)
        return original(self, datasets, **kwargs)

    H5Coro.readDatasets = recording
    return calls_by_resource


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--order", type=int, required=True, help="HEALPix benchmark order (9 or 10)")
    ap.add_argument("--granule-dir", required=True, help="directory of locally cached .h5 granules")
    ap.add_argument("--out", default=None, help="output JSON (default requests/o<order>.json)")
    args = ap.parse_args()

    config = load_config(str(CONFIGS / f"atl03_tdigest_healpix_o{args.order}.yaml"))
    shardmap = json.loads((SHARDMAPS / f"sm_healpix_o{args.order}.json").read_text())
    shard_key, names = densest_shard(shardmap)

    granule_dir = Path(os.path.expanduser(args.granule_dir))
    paths = [granule_dir / n for n in names]
    missing = [p.name for p in paths if not p.exists()]
    if missing:
        sys.exit(f"missing {len(missing)} granules in {granule_dir}: {missing[:3]}...")

    grid = from_config(config, populated_shards=[int(s) for s in shardmap["shard_keys"]])
    calls_by_resource = install_recorder()

    print(f"o{args.order}: shard {shard_key}, {len(paths)} granules")
    wall0, cpu0 = time.perf_counter(), time.process_time()
    df, meta = process_shard(
        grid,
        shard_key,
        [str(p) for p in paths],
        s3_credentials={},
        config=config,
        driver="https",  # URL passthrough; FileDriver reads local paths
        h5coro_driver=filedriver.FileDriver,
        handoff=get_handoff(config),
        chunk_results=[],
    )
    wall, cpu = time.perf_counter() - wall0, time.process_time() - cpu0

    out = Path(args.out) if args.out else Path(__file__).parent / "requests" / f"o{args.order}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    n_calls = sum(len(c) for c in calls_by_resource.values())
    payload = {
        "meta": {
            "order": args.order,
            "shard_key": shard_key,
            "config": f"tests/data/benchmark/configs/atl03_tdigest_healpix_o{args.order}.yaml",
            "shardmap": f"tests/data/benchmark/shardmaps/sm_healpix_o{args.order}.json",
            "n_granules": len(paths),
            "n_calls": n_calls,
        },
        "capture_run": {
            "wall_s": round(wall, 3),
            "cpu_s": round(cpu, 3),
            "max_rss_mb": round(max_rss_mb(), 1),
            "total_obs": int(meta["total_obs"]),
            "cells_with_data": int(meta["cells_with_data"]),
        },
        "granules": [
            {"resource": name, "calls": calls_by_resource.get(name, [])} for name in names
        ],
    }
    out.write_text(json.dumps(payload) + "\n")
    print(
        f"captured {n_calls} readDatasets calls -> {out}\n"
        f"worker (with capture overhead): wall {wall:.1f}s cpu {cpu:.1f}s "
        f"rss {max_rss_mb():.0f}MB obs {meta['total_obs']}"
    )


if __name__ == "__main__":
    main()
