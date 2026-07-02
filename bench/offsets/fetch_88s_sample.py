"""88S sample: offsets extraction on pinned-shard granules over HTTPS/EDL (issue #158).

Validates the extractor end-to-end on granules from the pinned 88S o9 stress
shard (pin ``11530494877603201033``, 5,620 granules — see the #148 thread).
For each sample granule, from a workstation:

1. **stream** — route (b) h5coro over ``HTTPDriver`` with an EDL bearer token
   (earthaccess netrc login): the extraction Lambda's exact read path. Since
   offsets extraction touches only HDF5 metadata, this needs a few MB of
   ranged GETs, not the ~2 GB granule; the per-granule GET count and byte
   volume are recorded.
2. **download** — fetch the granule to a temp dir (EDL token, requests), run
   routes (a)+(b) locally, hfxidx via podman, and the same three-way
   cross-check gate as the NEON run; then **delete the download** (EOSDIS
   redistribution rules — only the offsets parquet and report are kept).
   Streamed and local route-(b) frames must also match exactly.

Sample selection: first / middle / last of the pinned shard's granule list
(``tests/data/benchmark/shardmaps/sm_healpix_o9_88s.json`` on the PR #152
branch, ``metadata.pruned`` shard) — earliest cycle, mid-mission, latest, so
all three v007 reprocessing eras are touched. URLs are inlined here rather
than read from that unmerged branch.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from crosscheck_hidefix import STRICT_VALUE_COLS, compare, make_index, parse_hidefix_fx
from extract_offsets import (
    DEFAULT_DATASETS,
    extract_offsets_h5coro,
    extract_offsets_h5py,
    write_offsets_parquet,
)

#: First / middle / last granule of the pinned o9 88S shard's 5,620-granule list.
SAMPLE_GRANULES = (
    "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL03/007/2018/10/14/ATL03_20181014103720_02410111_007_01.h5",
    "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL03/007/2022/01/25/ATL03_20220125022450_05161411_007_01.h5",
    "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL03/007/2025/06/01/ATL03_20250601165401_11722711_007_01.h5",
)


def _edl_token() -> str:
    import earthaccess

    auth = earthaccess.login(strategy="netrc")
    if not auth.authenticated:
        raise SystemExit("EDL login failed (no ~/.netrc urs.earthdata.nasa.gov entry?)")
    return auth.token["access_token"]


def _counting_driver_class(base, stats: dict):
    """Subclass a h5coro driver so every ranged GET (incl. the superblock
    fetch during ``H5Coro`` construction, which pulls the first 4 MiB cache
    line — where NSIDC's repacked files keep most of the metadata) is
    counted."""

    class Counting(base):
        def read(self, pos, size):
            stats["n_gets"] += 1
            stats["bytes"] += size  # requested size (== received for a 206 range hit)
            return super().read(pos, size)

        def copy(self, max_connections=None):
            # base.copy() hardcodes the base class, which would let reads
            # escape the counter on any driver-copying path (e.g. h5coro's
            # forked multiProcess B-tree readers -- unused here, but cheap to
            # stay correct against).
            return type(self)(self.resource, self.cached_credentials, max_connections)

    return Counting


def stream_extract(url: str, token: str) -> tuple[object, dict, dict]:
    """Route (b) over HTTPS: returns (df, meta, transfer_stats)."""
    import h5coro
    from h5coro import webdriver

    stats = {"n_gets": 0, "bytes": 0}
    t0 = time.time()
    # HTTPS URLs are used as-is (zagg's _make_url_rewriter only strips s3://)
    h5obj = h5coro.H5Coro(
        url, _counting_driver_class(webdriver.HTTPDriver, stats), credentials=token
    )
    try:
        df, meta = extract_offsets_h5coro(h5obj, url.rsplit("/", 1)[-1])
    finally:
        if hasattr(h5obj, "close"):
            h5obj.close()
    transfer = {
        "total_wall_s": round(time.time() - t0, 3),  # incl. superblock fetch
        "n_gets": stats["n_gets"],
        "mb_transferred": round(stats["bytes"] / 2**20, 2),
    }
    return df, meta, transfer


def download(url: str, token: str, dest_dir: Path) -> tuple[Path, float]:
    """Fetch one granule over HTTPS with the EDL token; returns (path, wall_s).

    The bearer token is only presented to the EDL/NSIDC host — requests
    strips ``Authorization`` on the cross-host redirect to the presigned S3
    URL, which is also why that hop still authenticates (the signature is in
    the query string). Caveat: the caller holds the file in a
    ``TemporaryDirectory``, which a SIGKILL mid-download would strand in
    ``$TMPDIR`` — check for leftover ``ATL03_*.h5`` there after any hard
    abort (EOSDIS no-redistribute).
    """
    import requests

    dest = dest_dir / url.rsplit("/", 1)[-1]
    t0 = time.time()
    with requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        stream=True,
        timeout=(10, 60),  # connect, per-read; a stalled socket must not hang forever
    ) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    return dest, round(time.time() - t0, 1)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out-dir", default="bench/offsets/out_88s")
    ap.add_argument("--report-out", default=None, help="JSON report path")
    ap.add_argument("--skip-download", action="store_true", help="stream-only (no h5py/hidefix)")
    ap.add_argument("--skip-hidefix", action="store_true", help="no podman on this host")
    ap.add_argument("--granules", nargs="*", default=list(SAMPLE_GRANULES))
    args = ap.parse_args(argv)

    token = _edl_token()
    out_dir = Path(args.out_dir)
    report: list[dict] = []
    for url in args.granules:
        gid = url.rsplit("/", 1)[-1]
        rec: dict = {"granule": gid, "url": url}

        df_s, meta_s, transfer = stream_extract(url, token)
        rec["stream"] = {"extract_wall_s": meta_s["wall_s"], **transfer}
        dest = out_dir / "h5coro-https" / f"{gid.removesuffix('.h5')}.offsets.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_offsets_parquet(df_s, meta_s, dest)
        rec["n_chunks"] = meta_s["n_chunks"]

        if not args.skip_download:
            with tempfile.TemporaryDirectory() as tmp:  # download deleted on exit
                local, dl_wall = download(url, token, Path(tmp))
                rec["download_wall_s"] = dl_wall
                df_a, meta_a = extract_offsets_h5py(str(local), gid)
                import h5coro
                from h5coro import filedriver

                h5obj = h5coro.H5Coro(str(local), filedriver.FileDriver, errorChecking=True)
                try:
                    df_b, meta_b = extract_offsets_h5coro(h5obj, gid)
                finally:
                    h5obj.close()
                rec["local_wall_s"] = {"h5py": meta_a["wall_s"], "h5coro": meta_b["wall_s"]}
                results = [
                    compare(df_a, df_b, "h5py-vs-h5coro", value_cols=STRICT_VALUE_COLS),
                    compare(df_s, df_b, "https-vs-local-h5coro", value_cols=STRICT_VALUE_COLS),
                ]
                if not args.skip_hidefix:
                    fx = Path(tmp) / f"{gid.removesuffix('.h5')}.fx"
                    make_index(local, fx)
                    df_h = parse_hidefix_fx(fx)
                    # intent-scoped, matching crosscheck_hidefix.main (a
                    # dataset both routes dropped must surface as right_only)
                    df_h = df_h[df_h["dataset"].isin(DEFAULT_DATASETS)]
                    results.append(compare(df_a, df_h, "h5py-vs-hidefix"))
                rec["results"] = results
                rec["ok"] = all(r["ok"] for r in results)
        report.append(rec)
        print(json.dumps({k: v for k, v in rec.items() if k != "results"}, indent=1))

    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=1) + "\n")
    failures = [r for r in report if not r.get("ok", True)]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
