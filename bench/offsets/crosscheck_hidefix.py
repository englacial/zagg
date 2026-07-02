"""Cross-validate extract_offsets output against hidefix's index (issue #158).

The hard gate mirroring PR #150's byte-equality methodology: for every chunk
of every dataset of every locally cached granule, the ``(byte_offset,
nbytes)`` pair produced by our extractors must equal the one hidefix records
in its serialized index — and route (a) h5py must equal route (b) h5coro
row-for-row. Any mismatch fails the run (non-zero exit); nothing is papered
over.

hidefix side: the ``zagg-bench-hidefix`` podman image (built from PR #150's
``Containerfile.hidefix``) ships ``/usr/local/bin/hfxidx``, which serializes
an index to flexbuffers. Per-dataset entries carry a raw ``chunks`` byte blob
of little-endian u64 records ``(addr, size, offset[D])`` — 24 bytes for 1-D,
32 for 2-D (verified against h5py on chunk 0 of ``gt2r/heights/lat_ph`` of
``ATL03_20190105163308_01260202_007_01.h5``: addr 1233125376, size 502279).

Parsing the flexbuffers dump needs the pure-Python ``flatbuffers`` package —
a bench-tool-only dependency (scratch venv or ``pip install --user``), not a
zagg dependency.

Usage (see bench/offsets/README.md):

    python bench/offsets/crosscheck_hidefix.py \
        --offsets-dir <out-dir from extract_offsets.py, with h5py/+h5coro/> \
        --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules \
        --fx-dir <scratch>/fx --make-index \
        --report-out bench/offsets/results/crosscheck_neon.json
"""

from __future__ import annotations

import argparse
import json
import struct
import subprocess
from pathlib import Path

import pandas as pd
from extract_offsets import DEFAULT_DATASETS, OFFSETS_DTYPES, _chunk_rows, read_offsets_parquet

PODMAN_IMAGE = "zagg-bench-hidefix"

#: Join key + compared values (everything the arm-(2b) reader consumes).
KEY_COLS = ["beam", "dataset", "chunk_idx"]
VALUE_COLS = ["elem_start", "elem_end", "byte_offset", "nbytes"]
#: For extractor-vs-extractor pairs both sides carry the filter mask too;
#: hidefix's index does not store it, so hidefix pairs compare VALUE_COLS.
STRICT_VALUE_COLS = VALUE_COLS + ["filter_mask"]
GATE_COLS = ["chunk_idx"] + VALUE_COLS


def make_index(granule: Path, fx_path: Path, image: str = PODMAN_IMAGE) -> None:
    """Run hfxidx in the PR #150 container to serialize one granule's index."""
    fx_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{granule.parent}:/data:ro",
            "-v",
            f"{fx_path.parent}:/out",
            image,
            "hfxidx",
            f"/data/{granule.name}",
            f"/out/{fx_path.name}",
            "--out-type",
            "flexbuffers",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:  # surface hfxidx/podman stderr, not just the exit code
        raise RuntimeError(
            f"hfxidx failed for {granule.name} (rc={proc.returncode}):\n{proc.stderr}"
        )


def parse_hidefix_fx(fx_path: Path) -> pd.DataFrame:
    """Parse a hfxidx flexbuffers dump into the extract_offsets row schema.

    Walks the group tree; emits rows only for ``gt??/<group>/<name>`` datasets
    (the beam-relative paths extract_offsets uses). ``chunk_idx`` is computed
    from each record's element offset via the same row-major linear-index
    helper the extractors use, so the join does not depend on hidefix's
    internal chunk ordering.
    """
    from flatbuffers import flexbuffers

    root = flexbuffers.Loads(fx_path.read_bytes())["root"]
    rows: list[tuple] = []

    def walk_group(group: dict, path: tuple[str, ...]) -> None:
        for name, ds in group.get("datasets", {}).items():
            if len(path) != 2 or not path[0].startswith("gt"):
                continue
            (dkey,) = [k for k in ds if k.startswith("D")]  # e.g. D1/D2 by rank
            d = ds[dkey]
            shape = tuple(int(x) for x in d["shape"])
            chunk_shape = tuple(int(x) for x in d["chunk_shape"])
            blob = bytes(d["chunks"])
            rec = struct.Struct(f"<{2 + len(shape)}Q")
            assert len(blob) % rec.size == 0, f"chunk blob not {rec.size}-aligned: {fx_path}"
            entries = []
            for i in range(len(blob) // rec.size):
                addr, size, *offset = rec.unpack_from(blob, i * rec.size)
                entries.append((tuple(offset), 0, addr, size))
            rows.extend(
                r[:-1]  # drop filter_mask: hidefix's index does not store it
                for r in _chunk_rows(path[0], f"{path[1]}/{name}", shape, chunk_shape, entries)
            )
        for name, sub in group.get("groups", {}).items():
            walk_group(sub, path + (name,))

    walk_group(root, ())
    cols = ["beam", "dataset"] + GATE_COLS
    df = pd.DataFrame(rows, columns=cols)
    return df.astype({c: OFFSETS_DTYPES[c] for c in cols}).sort_values(
        ["beam", "dataset", "chunk_idx"], ignore_index=True
    )


def compare(
    ours: pd.DataFrame, theirs: pd.DataFrame, label: str, value_cols: list[str] = VALUE_COLS
) -> dict:
    """Chunk-for-chunk comparison on the gate columns; returns a result dict."""
    m = ours[KEY_COLS + value_cols].merge(
        theirs[KEY_COLS + value_cols],
        on=KEY_COLS,
        how="outer",
        suffixes=("_a", "_b"),
        indicator=True,
    )
    bad = m["_merge"] != "both"
    for c in value_cols:
        bad |= m[f"{c}_a"] != m[f"{c}_b"]
    mismatches = m[bad]
    return {
        "comparison": label,
        "n_chunks": int(len(m)),
        "n_mismatch": int(len(mismatches)),
        "ok": len(mismatches) == 0,
        # to_json round-trip: join misses leave float NaN, which json.dumps
        # would emit as bare (non-strict) NaN; to_json maps them to null.
        "mismatch_sample": json.loads(mismatches.head(20).to_json(orient="records")),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--offsets-dir", required=True, help="extract_offsets --out-dir (h5py/+h5coro/)"
    )
    ap.add_argument("--granule-dir", required=True, help="local .h5 cache (for --make-index)")
    ap.add_argument("--fx-dir", required=True, help="where hfxidx flexbuffers dumps live/land")
    ap.add_argument(
        "--make-index", action="store_true", help="run hfxidx via podman if .fx missing"
    )
    ap.add_argument("--image", default=PODMAN_IMAGE)
    ap.add_argument("--report-out", default=None, help="JSON report path")
    args = ap.parse_args(argv)

    offsets_dir = Path(args.offsets_dir)
    fx_dir = Path(args.fx_dir)
    parquets = sorted((offsets_dir / "h5py").glob("*.offsets.parquet"))
    if not parquets:
        raise SystemExit(f"no offsets parquets under {offsets_dir}/h5py — run extract_offsets.py")
    # coverage is judged against the cache, not the parquet glob: a granule
    # the extractor never produced output for must fail, not vanish.
    cached = {p.name for p in Path(args.granule_dir).expanduser().glob("*.h5")}
    covered = {p.name.removesuffix(".offsets.parquet") + ".h5" for p in parquets}
    uncovered = sorted(cached - covered)

    report: list[dict] = []
    total = {"granules": 0, "chunks": 0, "failures": 0}
    for pq in parquets:
        gid = pq.name.removesuffix(".offsets.parquet") + ".h5"
        df_a, _ = read_offsets_parquet(pq)
        df_b, _ = read_offsets_parquet(offsets_dir / "h5coro" / pq.name)
        fx = fx_dir / (gid.removesuffix(".h5") + ".fx")
        if not fx.exists():
            if not args.make_index:
                raise SystemExit(f"missing {fx} (pass --make-index to generate)")
            make_index(Path(args.granule_dir).expanduser() / gid, fx, args.image)
        # hidefix indexes every dataset under a beam (bckgrd_atlas/ etc.);
        # the gate covers the read-path datasets the extractor *targets*
        # (intent, not observation — a dataset our side dropped entirely must
        # surface as right_only, not vanish from the join).
        df_h = parse_hidefix_fx(fx)
        df_h = df_h[df_h["dataset"].isin(DEFAULT_DATASETS)]
        results = [
            compare(df_a, df_b, "h5py-vs-h5coro", value_cols=STRICT_VALUE_COLS),
            compare(df_a, df_h, "h5py-vs-hidefix"),
            compare(df_b, df_h, "h5coro-vs-hidefix"),
        ]
        ok = all(r["ok"] for r in results)
        report.append({"granule": gid, "ok": ok, "results": results})
        total["granules"] += 1
        total["chunks"] += results[0]["n_chunks"]
        total["failures"] += 0 if ok else 1
        print(f"{gid}: {'OK' if ok else 'MISMATCH'} ({results[0]['n_chunks']} chunks)")

    total["uncovered_granules"] = len(uncovered)
    summary = {"summary": total, "uncovered": uncovered, "granules": report}
    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=1) + "\n")
    print(
        f"cross-check: {total['granules']} granules, {total['chunks']} chunks, "
        f"{total['failures']} failing, {len(uncovered)} cached granules without offsets"
    )
    return 1 if total["failures"] or uncovered else 0


if __name__ == "__main__":
    raise SystemExit(main())
