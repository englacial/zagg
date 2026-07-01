"""Replay a captured h5coro workload against a read backend (issue #149).

Reads the request lists frozen by ``capture_requests.py`` and issues them,
call-for-call, against one backend adapter, measuring wall time, CPU time and
peak RSS. Every returned array is checksummed (sha256 over dtype + shape + raw
bytes); ``--write-baseline`` records the reference checksums and every other
variant is hard-gated against them — a variant that returns different bytes
fails, it does not get a benchmark row.

Adapters: ``h5coro`` (the pure-Python package as installed — the current-code
baseline or the numpy-patched comparable, depending on the active environment).
Phase 3 adds ``shim`` (sliderule C++ via pybind11); phase 4 adds ``hidefix``.

Usage (repo root)::

    python bench/h5coro/bench_replay.py --requests bench/h5coro/requests/o10.json \
        --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules \
        --variant h5coro-1.0.4 --write-baseline
    python bench/h5coro/bench_replay.py ... --variant h5coro-numpy \
        --baseline bench/h5coro/results/checksums_o10.json
    python bench/h5coro/bench_replay.py ... --profile   # cProfile decomposition
"""

import argparse
import cProfile
import hashlib
import json
import os
import pstats
import resource
import sys
import time
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# adapters: name -> read_granule(path, calls) -> {(call_idx, dataset): ndarray}
# ---------------------------------------------------------------------------


def _h5coro_read_granule(path: str, calls: list) -> dict:
    from h5coro import filedriver
    from h5coro.h5coro import H5Coro

    h5 = H5Coro(path, filedriver.FileDriver, errorChecking=True, verbose=False)
    out = {}
    try:
        for ci, entries in enumerate(calls):
            request = [
                e["dataset"]
                if e["hyperslice"] is None
                else {"dataset": e["dataset"], "hyperslice": [tuple(h) for h in e["hyperslice"]]}
                for e in entries
            ]
            promise = h5.readDatasets(request, block=True)
            for e in entries:
                out[(ci, e["dataset"])] = np.asarray(promise[e["dataset"]])
    finally:
        if hasattr(h5, "close"):
            h5.close()
    return out


ADAPTERS = {"h5coro": _h5coro_read_granule}


# ---------------------------------------------------------------------------


def max_rss_mb() -> float:
    """Peak RSS of this process in MB (ru_maxrss is bytes on macOS, KB on Linux)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 2**20 if sys.platform == "darwin" else rss / 2**10


def checksum(arr: np.ndarray) -> str:
    h = hashlib.sha256()
    h.update(arr.dtype.str.encode())
    h.update(str(arr.shape).encode())
    h.update(np.ascontiguousarray(arr).tobytes())
    return h.hexdigest()


# cProfile self-time buckets: exact h5coro 1.0.4 function names, matched only
# inside the h5coro package so unrelated same-named functions don't pollute rows.
H5CORO_FUNCS = {
    "inflate": {"inflateChunk"},
    "shuffle": {"shuffleChunk"},
    "btree": {"readBTreeV1", "readBTreeNodeV1"},
    "slice_assembly": {"readSlice", "hypersliceIntersection", "hypersliceSubset"},
    "metadata": {
        "readSuperblock",
        "readObjHdr",
        "readObjHdrV0",
        "readObjHdrV1",
        "readMessagesV0",
        "readMessagesV1",
        "readMessage",
        "readSymbolTable",
        "readFractalHeap",
        "readDirectBlock",
        "readIndirectBlock",
        "readVLString",
        "readArray",
    },
    "field_unpack": {"readField"},
}
# C builtins appear in pstats with decorated names ("<built-in method zlib.decompress>",
# "method 'read' of '_io.BufferedReader' objects") — match by substring.
BUILTIN_SUBSTRINGS = {
    "zlib_c": ("zlib.decompress",),
    "file_io": ("_io.BufferedReader", "_io.FileIO", "posix.", "io.open"),
    "np_convert": ("numpy.frombuffer", "frombuffer", "ascontiguousarray"),
}


def bucket_profile(stats: pstats.Stats) -> dict:
    """Aggregate cProfile self-time (tottime) into read-path buckets."""
    totals = dict.fromkeys([*H5CORO_FUNCS, *BUILTIN_SUBSTRINGS], 0.0)
    totals["other"] = 0.0
    grand = 0.0
    for (filename, _line, funcname), (_cc, _nc, tottime, _ct, _callers) in stats.stats.items():
        grand += tottime
        bucket = None
        if "h5coro" in filename:
            bucket = next((b for b, names in H5CORO_FUNCS.items() if funcname in names), None)
        if bucket is None:
            bucket = next(
                (b for b, subs in BUILTIN_SUBSTRINGS.items() if any(s in funcname for s in subs)),
                None,
            )
        totals[bucket or "other"] += tottime
    return {"total_s": round(grand, 3), **{k: round(v, 3) for k, v in totals.items()}}


def run(args: argparse.Namespace) -> None:
    payload = json.loads(Path(args.requests).read_text())
    granule_dir = Path(os.path.expanduser(args.granule_dir))
    read_granule = ADAPTERS[args.adapter]

    baseline = None
    if args.baseline:
        baseline = json.loads(Path(args.baseline).read_text())

    checksums: dict[str, str] = {}
    mismatches: list[str] = []
    per_granule = []
    wall0, cpu0 = time.perf_counter(), time.process_time()
    for g in payload["granules"]:
        if not g["calls"]:
            continue
        path = granule_dir / g["resource"]
        g0 = time.perf_counter()
        out = read_granule(str(path), g["calls"])
        per_granule.append(round(time.perf_counter() - g0, 3))
        for (ci, ds), arr in out.items():
            key = f"{g['resource']}:{ci}:{ds}"
            digest = checksum(arr)
            checksums[key] = digest
            if baseline is not None and baseline.get(key) != digest:
                mismatches.append(key)
        del out
    wall, cpu = time.perf_counter() - wall0, time.process_time() - cpu0

    results_dir = Path(args.requests).parent.parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    label = Path(args.requests).stem

    if baseline is not None and mismatches:
        print(f"CORRECTNESS FAILURE: {len(mismatches)} mismatched arrays, e.g.:")
        for k in mismatches[:10]:
            print(f"  {k}")
        sys.exit(1)

    if args.write_baseline:
        ref = results_dir / f"checksums_{label}.json"
        ref.write_text(json.dumps(checksums) + "\n")
        print(f"baseline checksums ({len(checksums)} arrays) -> {ref}")

    result = {
        "variant": args.variant,
        "adapter": args.adapter,
        "requests": label,
        "platform": sys.platform,
        "wall_s": round(wall, 3),
        "cpu_s": round(cpu, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
        "n_arrays": len(checksums),
        "n_granules": len(per_granule),
        "correctness": "baseline-written"
        if args.write_baseline
        else ("pass" if baseline is not None else "unchecked"),
        "per_granule_wall_s": per_granule,
    }
    out_path = results_dir / f"replay_{label}_{args.variant}.json"
    out_path.write_text(json.dumps(result, indent=1) + "\n")
    print(
        f"{args.variant} on {label}: wall {wall:.1f}s cpu {cpu:.1f}s rss {max_rss_mb():.0f}MB "
        f"({len(checksums)} arrays, correctness={result['correctness']}) -> {out_path}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--requests", required=True, help="captured workload JSON")
    ap.add_argument("--granule-dir", required=True, help="directory of cached .h5 granules")
    ap.add_argument("--variant", required=True, help="row label, e.g. h5coro-1.0.4")
    ap.add_argument("--adapter", default="h5coro", choices=sorted(ADAPTERS))
    ap.add_argument("--baseline", default=None, help="checksums JSON to gate against")
    ap.add_argument("--write-baseline", action="store_true", help="record reference checksums")
    ap.add_argument("--profile", action="store_true", help="cProfile + bucketed decomposition")
    args = ap.parse_args()

    if args.profile:
        prof = cProfile.Profile()
        prof.enable()
        run(args)
        prof.disable()
        stats = pstats.Stats(prof)
        label = Path(args.requests).stem
        results_dir = Path(args.requests).parent.parent / "results"
        stats.dump_stats(results_dir / f"profile_{label}_{args.variant}.pstats")
        buckets = bucket_profile(stats)
        (results_dir / f"profile_{label}_{args.variant}.json").write_text(
            json.dumps(buckets, indent=1) + "\n"
        )
        print("decomposition (self time, s):", json.dumps(buckets, indent=1))
    else:
        run(args)


if __name__ == "__main__":
    main()
