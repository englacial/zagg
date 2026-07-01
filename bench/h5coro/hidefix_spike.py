"""Phase-4 spike: replay a captured workload against hidefix (issue #149).

hidefix (github.com/gauteh/hidefix) is a pure-Rust index-first HDF5 reader.
This script builds the hidefix index per granule (timed — this is hidefix's
analogue of h5coro's metadata walk), replays every captured ``readDatasets``
call against ``hidefix.Index.dataset(...)[slices]``, and hard-gates every
returned array against the reference checksums from ``bench_replay.py``
(imported, not reimplemented, so the gate cannot diverge).

The Python binding (0.12.0) exposes *no* index serialization (``pickle`` raises
``TypeError``), so serialized-index build time/size — the numbers feeding issue
#148's chunk-offset-cache design — are measured through the Rust CLI ``hfxidx``
(``cargo install hidefix --features clap,bincode,flexbuffers``), passed via
``--hfxidx``. Without it those fields are null.

API quirk this script compensates for: hidefix's ``__getitem__`` *squeezes*
every dimension whose count is <= 1 (``read_py_array`` in src/python.rs), so a
length-1 slice of the 2-D ``signal_conf_ph`` comes back ``(5,)`` instead of
``(1, 5)``; arrays are reshaped to the request's expected shape before
checksumming. It also only accepts a *tuple* of slices, not a bare slice.

Usage (repo root)::

    python bench/h5coro/hidefix_spike.py --requests bench/h5coro/requests/o10.json \
        --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules \
        --baseline bench/h5coro/results/checksums_o10.json \
        --hfxidx /path/to/hfxidx --note "quiet host"
"""

import argparse
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import hidefix

sys.path.append(str(Path(__file__).resolve().parent))
from bench_replay import checksum, max_rss_mb  # noqa: E402


def read_granule(idx: "hidefix.Index", calls: list) -> tuple[dict, list]:
    """Replay one granule's calls; return ({(call_idx, dataset): ndarray}, failures)."""
    out, failures = {}, []
    for ci, entries in enumerate(calls):
        for e in entries:
            name, hs = e["dataset"], e["hyperslice"]
            try:
                ds = idx.dataset(name)
                if ds is None:
                    raise KeyError(f"dataset not in index: {name}")
                shape = tuple(int(n) for n in ds.shape())
                if hs is None:
                    slices = tuple(slice(0, n) for n in shape)
                else:
                    slices = tuple(slice(int(a), int(b)) for a, b in hs)
                    slices += tuple(slice(0, n) for n in shape[len(slices) :])
                expected = tuple(s.stop - s.start for s in slices)
                arr = ds[slices]
                if arr.shape != expected:  # hidefix squeezes count<=1 dims
                    arr = arr.reshape(expected)
                out[(ci, name)] = arr
            except Exception as exc:  # record precisely, keep going
                failures.append(
                    {"call": ci, "dataset": name, "error": f"{type(exc).__name__}: {exc}"}
                )
    return out, failures


def serialize_index(hfxidx: str, granule: Path) -> dict:
    """Time `hfxidx <granule> <out>` (bincode) and stat both serialized sizes.

    The subprocess wall time includes process startup *and* re-indexing (hfxidx
    re-runs Index::index before serializing) — it is an upper bound on
    encode+write cost, reported alongside the in-process index_build_s.
    """
    row: dict = {}
    with tempfile.TemporaryDirectory() as td:
        for fmt in ("bincode", "flexbuffers"):
            dst = Path(td) / f"idx.{fmt}"
            t0 = time.perf_counter()
            proc = subprocess.run(
                [hfxidx, str(granule), str(dst), "--out-type", fmt],
                capture_output=True,
                text=True,
            )
            dt = time.perf_counter() - t0
            if proc.returncode != 0:
                row[f"index_size_{fmt}_bytes"] = None
                row[f"hfxidx_{fmt}_error"] = proc.stderr.strip()[-200:]
            else:
                row[f"index_size_{fmt}_bytes"] = dst.stat().st_size
                if fmt == "bincode":
                    row["hfxidx_wall_s"] = round(dt, 3)
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--requests", required=True, help="captured workload JSON")
    ap.add_argument("--granule-dir", required=True, help="directory of cached .h5 granules")
    ap.add_argument("--baseline", required=True, help="reference checksums JSON to gate against")
    ap.add_argument("--variant", default=None, help="row label (default hidefix-<ver>)")
    ap.add_argument("--hfxidx", default=None, help="hfxidx binary for serialized-index metrics")
    ap.add_argument("--note", default=None, help="environment note embedded in the results row")
    args = ap.parse_args()

    ver = importlib.metadata.version("hidefix")
    variant = args.variant or f"hidefix-{ver}"
    payload = json.loads(Path(args.requests).read_text())
    baseline = json.loads(Path(args.baseline).read_text())
    granule_dir = Path(os.path.expanduser(args.granule_dir))

    n_pass = 0
    mismatches: list[str] = []
    failures: list[dict] = []
    per_granule = []
    wall0, cpu0 = time.perf_counter(), time.process_time()
    for g in payload["granules"]:
        if not g["calls"]:
            continue
        path = granule_dir / g["resource"]
        g0 = time.perf_counter()
        idx = hidefix.Index(str(path))
        build_s = time.perf_counter() - g0
        out, fails = read_granule(idx, g["calls"])
        gwall = time.perf_counter() - g0
        for f in fails:
            f["resource"] = g["resource"]
        failures.extend(fails)
        for (ci, ds), arr in out.items():
            key = f"{g['resource']}:{ci}:{ds}"
            if baseline.get(key) == checksum(arr):
                n_pass += 1
            else:
                mismatches.append(key)
        per_granule.append(
            {
                "resource": g["resource"],
                "index_build_s": round(build_s, 4),
                "read_s": round(gwall - build_s, 3),
                "wall_s": round(gwall, 3),
            }
        )
        del out, idx
    wall, cpu = time.perf_counter() - wall0, time.process_time() - cpu0

    if args.hfxidx:
        for row in per_granule:
            row.update(serialize_index(args.hfxidx, granule_dir / row["resource"]))

    n_expected = sum(len(entries) for g in payload["granules"] for entries in g["calls"])
    if mismatches:
        correctness = f"fail: {len(mismatches)}/{n_expected} mismatched"
    elif failures:
        correctness = f"partial: {n_pass}/{n_expected}"
    else:
        correctness = "pass"

    label = Path(args.requests).stem
    sizes = [r.get("index_size_bincode_bytes") for r in per_granule]
    result = {
        "variant": variant,
        "adapter": "hidefix",
        "requests": label,
        "platform": sys.platform,
        "wall_s": round(wall, 3),
        "cpu_s": round(cpu, 3),
        "max_rss_mb": round(max_rss_mb(), 1),
        "n_arrays": n_pass + len(mismatches),
        "n_granules": len(per_granule),
        "correctness": correctness,
        "index_build_s_total": round(sum(r["index_build_s"] for r in per_granule), 3),
        "index_size_bytes_total": sum(s for s in sizes if s) if any(sizes) else None,
        "note": args.note,
        "per_granule_wall_s": [r["wall_s"] for r in per_granule],
        "per_granule": per_granule,
        "mismatches": mismatches[:20],
        "failures": failures,
    }
    results_dir = Path(args.requests).parent.parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    out_path = results_dir / f"replay_{label}_{variant}.json"
    out_path.write_text(json.dumps(result, indent=1) + "\n")
    print(
        f"{variant} on {label}: wall {wall:.1f}s cpu {cpu:.1f}s rss {max_rss_mb():.0f}MB "
        f"(index build {result['index_build_s_total']}s, {result['n_arrays']} arrays, "
        f"correctness={correctness}) -> {out_path}"
    )
    if mismatches:
        for k in mismatches[:10]:
            print(f"  MISMATCH {k}")
        sys.exit(1)
    if failures:
        for f in failures[:10]:
            print(f"  FAILED {f['resource']}:{f['call']}:{f['dataset']} — {f['error']}")


if __name__ == "__main__":
    main()
