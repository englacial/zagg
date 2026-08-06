"""Dispatch the pinned benchmark shard(s) to Lambda and emit metrics (issue #110).

For each requested target this loads the pinned config + shard map, dispatches the
ONE densest shard to AWS Lambda via ``zagg.runner.agg`` (with ``profile=True`` so
the worker phase timings come back), and flattens the run summary into a record
(``bench_metrics.build_record``). It writes the records as JSON (consumed by
``update_series``) and a markdown comment body (posted on PRs).

Run identity (commit/ref/event/pr-number) is passed in by the workflow; this
script does the dispatch + arithmetic only. It needs AWS credentials in the
environment (the workflow supplies them via OIDC role assumption) and the Lambda
to already be deployed -- it never stands up or mutates infrastructure.

Usage::

    python run_benchmark.py --targets tests/data/benchmark/targets.json \\
        --target tdigest_healpix_o11_sharded --target tdigest_healpix_o11_inner \\
        --store-prefix s3://my-bucket/zagg-bench \\
        --region us-west-2 --function-name process-shard \\
        --event pr --commit "$SHA" --ref "$REF" --pr-number 123 \\
        --out-json metrics.json --out-comment comment.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Benchmark targets are launched concurrently (issue #137). Each target is one
# independent single-shard Lambda dispatch, so this bounds how many run at once;
# the matrix is small (~single digits), so the cap only guards a pathological
# manifest.
_MAX_TARGET_CONCURRENCY = 16

# Allow ``import bench_metrics`` whether run as a script or imported by tests.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402
import bench_objects  # noqa: E402

from zagg.config import (  # noqa: E402
    get_aoi_mask,
    get_handoff,
    get_store_layout,
    load_config,
)
from zagg.grids import from_config  # noqa: E402


def _aoi_parts(geojson_path: Path):
    """Exterior rings ``[(lats, lons), ...]`` from an AOI GeoJSON (coverage form)."""
    import numpy as np
    from shapely.geometry import shape

    geom = shape(json.loads(Path(geojson_path).read_text())["features"][0]["geometry"])
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    return [
        (np.asarray(p.exterior.coords.xy[1]), np.asarray(p.exterior.coords.xy[0])) for p in polys
    ]


def _shardmap_with_mask(shardmap_path: Path, grid, aoi_file: Path) -> Path:
    """Attach the strict-AOI per-cell mask to a committed (mask-free) shard map.

    The mask arm's committed map is the plain o9 map; the ~190 KB per-cell
    ``aoi_mask`` column is *derived* (grid + AOI, mortie — it does not move
    granules), so it is built here on the fly rather than committed (issue #202).
    Returns a temp path carrying the plain map + the ``aoi_mask`` column; the
    worker expands it identically to a pre-baked map. Built on the runner before
    dispatch, so the timed Lambda invocation is unchanged.
    """
    import tempfile

    from zagg.catalog.shardmap import ShardMap

    sm = ShardMap.from_json(str(shardmap_path))
    aoi_moc = grid.aoi_moc(_aoi_parts(aoi_file))
    sm.aoi_mask = [[int(w) for w in grid.aoi_shard_moc(aoi_moc, int(k))] for k in sm.shard_keys]
    sm.metadata["aoi_mask"] = True
    out = Path(tempfile.mkdtemp(prefix="zagg-bench-mask-")) / "shardmap_aoimask.json"
    sm.to_json(str(out))
    return out


def _measure_objects(config, grid, store: str, shard_key: int, *, region: str) -> dict:
    """LIST the run's output store and compare against the expected model (#240).

    Returns the ``objects`` payload ``bench_metrics.build_record`` threads into
    the record: measured total, the exact expectation (null when the layout's
    count is data-dependent), the per-shard attribution, the per-run root
    telemetry tally (issue #362 — reported, never audited), and the mismatch
    description (null when clean) that ``main`` hard-fails on. Uses the same
    store factory (and credentials) the dispatch just wrote through, so a LIST
    failure is a real run failure, not a swallowed warning.
    """
    return bench_objects.measure_objects(
        store,
        grid=grid,
        shard_keys=[shard_key],
        n_shards=1,
        store_layout=get_store_layout(config),
        region=region,
    )


def load_targets(path: str) -> tuple[dict, Path]:
    """Load the targets manifest; return it plus its directory (for rel paths)."""
    p = Path(path).resolve()
    with open(p) as f:
        return json.load(f), p.parent


def _resolve(base: Path, rel: str) -> Path:
    return (base / rel).resolve()


def all_target_names(manifest: dict) -> list[str]:
    """Names that ``run all`` iterates: the committed merge matrix only.

    ``provisional_targets`` (issue #130) are PR-tree-only and runnable by explicit
    ``--target`` name, but are deliberately excluded here so they never join the
    permanent every-merge matrix until the carrier decision is made.
    """
    return list(manifest["targets"].keys())


def _resolve_target(manifest: dict, name: str) -> dict:
    """Look a target up in the committed matrix, then the provisional block.

    ``provisional_targets`` lets ``/benchmark --target <name>`` run a PR-tree-only
    target (the pandas-vs-arrow carrier comparison, issue #130) without that target
    being part of ``targets`` (the committed merge matrix).
    """
    targets = manifest.get("targets", {})
    if name in targets:
        return targets[name]
    provisional = manifest.get("provisional_targets", {})
    if name in provisional:
        return provisional[name]
    known = list(targets) + list(provisional)
    raise KeyError(f"unknown target '{name}'; have {known}")


def resolve_variant(function_name: str, worker: dict | None) -> str:
    """The pre-provisioned worker variant a target's ``worker:`` block names.

    ``-<memory>`` plus ``-disk`` when ``extra_disk``, appended to the base
    function — the same rule as ``zagg.runner._resolve_function_name``. No
    ``worker:`` block resolves the base function itself. Shared by
    ``run_target`` and ``main``'s pre-dispatch staleness guard so the two
    cannot drift.
    """
    if not worker:
        return function_name
    return function_name + f"-{worker['memory']}" + ("-disk" if worker.get("extra_disk") else "")


def check_variant_current(client, function_name: str, resolved: str) -> str:
    """Refuse to dispatch onto a stale pre-provisioned worker variant (issue #341).

    The deploy path updates the base function and its worker-size variants from
    the same zip (deploy_lambda.sh); a variant whose ``CodeSha256`` differs from
    the base's is running different code than the run believes it is testing —
    the run 30503334617 failure mode, where ``-4096-disk`` served standup-time
    code.

    The two probes are read separately and treated differently (fold review),
    because ``except Exception`` around both swallowed the guard's own strongest
    positive signal:

    * **base probe fails** → degrade to a warning and return ``"skipped"``. This
      is the genuine "this stack's invoke role carries no
      ``lambda:GetFunctionConfiguration`` at all" case, and the only one where
      continuing is defensible.
    * **base probe succeeds, variant probe fails** → hard-fail. Under the issue
      #341 enumeration ruling ``InvokeBenchmarkFunctions`` lists exact ARNs, so
      an ``AccessDenied`` here means the variant is absent from the in-repo
      enumeration — and a variant nobody enumerated for the invoke role is one
      nobody enumerated for the deploy role either, i.e. one ``deploy_lambda.sh``
      WARN-and-skipped, i.e. definitely stale. ``ResourceNotFound`` is harder
      still: the dispatch would fail anyway, later and more opaquely.
    * hashes differ → hard-fail.

    Returns the guard's provenance status (``"verified"`` | ``"skipped"``), which
    rides the benchmark record so a number measured on an unverified build is not
    byte-indistinguishable from a verified one (issue #296).
    """
    try:
        base_sha = client.get_function_configuration(FunctionName=function_name)["CodeSha256"]
    except Exception as e:
        print(
            f"WARN: could not probe base function {function_name} ({e}); dispatching "
            f"{resolved} without the staleness guard — the run record is stamped "
            f"variant_guard=skipped",
            file=sys.stderr,
        )
        return "skipped"
    try:
        variant_sha = client.get_function_configuration(FunctionName=resolved)["CodeSha256"]
    except Exception as e:
        raise RuntimeError(
            f"worker variant {resolved} is NOT REACHABLE ({e}) while base "
            f"{function_name} probes fine, so this is specific to the variant: it is "
            f"either not provisioned by template.yaml or absent from the enumerated "
            f"InvokeBenchmarkFunctions ARNs in deployment/aws/benchmark_cicd.yaml. "
            f"Either way deploy_lambda.sh cannot have updated it, so it would run "
            f"code this benchmark is not testing (issue #341) — provision/enumerate "
            f"the variant, or point the target's worker: block at one that exists."
        ) from e
    if base_sha != variant_sha:
        raise RuntimeError(
            f"worker variant {resolved} is STALE: its CodeSha256 {variant_sha} does not "
            f"match base {function_name}'s {base_sha}. The variant would run code this "
            f"benchmark is not testing (issue #341) — redeploy the variant family "
            f"(deploy_lambda.sh updates the base and every provisioned variant from "
            f"the same zip) and re-dispatch."
        )
    return "verified"


def run_target(
    name: str,
    manifest: dict,
    base: Path,
    *,
    store: str,
    region: str,
    function_name: str,
    context: dict,
    dry_run: bool = False,
    variant_guard: str = "base",
) -> dict:
    """Dispatch one target's shard and return its benchmark record."""
    from zagg import __version__ as zagg_version

    target = _resolve_target(manifest, name)
    shardmap_key = target["shardmap"]
    shardmap_meta = manifest["shardmaps"][shardmap_key]
    config_path = _resolve(base, target["config"])
    shardmap_path = _resolve(base, shardmap_meta["path"])
    shard_key = int(shardmap_meta["shard_key"])
    n_granules = shardmap_meta.get("n_granules")

    config = load_config(str(config_path))
    # Per-cell carrier (issues #130/#132). Inherit from the config
    # (``aggregation.handoff``, default ``"arrow"``) unless the target pins an
    # explicit override for a pandas-vs-arrow A/B; the override still wins.
    handoff = target.get("handoff") or get_handoff(config)
    # The ShardingCodec (issue #108) is the experimental variable of the forward
    # benchmark (issue #133): the matrix carries ``sharded: true|false`` per target
    # so one config drives both columns. Apply it to the grid block (where
    # ``get_sharded`` reads it) when present; absent leaves the config's own
    # default, so frozen/legacy targets dispatch byte-identically to before.
    if "sharded" in target:
        config.output.setdefault("grid", {})["sharded"] = bool(target["sharded"])
    # Read-backend A/B (issue #193): the live matrix sets data_source.index from
    # each target's ``index_backend`` (inline vs sidecar), so one config per
    # order drives both columns. sidecar consumes/builds the granule-keyed
    # manifests; inline builds the chunk map on the fly. Absent -> config's own
    # index (frozen/legacy/provisional targets dispatch unchanged).
    backend = target.get("index_backend")
    if backend == "sidecar":
        config.data_source["index"] = {
            "backend": "sidecar",
            "on_miss": "build",
            "store": "s3://sliderule-public-cors/zagg-index/ATL03/007",
        }
    elif backend == "inline":
        config.data_source["index"] = {"backend": "inline"}
    elif backend == "hierarchical":
        config.data_source["index"] = {"backend": "hierarchical"}
    # Streaming-mode axis (issue #272): a target may run the spill path on the
    # disk-provisioned worker (issue #235/#258) instead of the default merge
    # path. Inject the mode into the aggregation block and the worker-size
    # selector into ``config.worker``; the target's ``worker`` block resolves the
    # pre-provisioned ``-disk`` variant OFF the base function name (so the release
    # ``process-shard`` -> ``process-shard-4096-disk`` and the per-merge test
    # ``process-shard-test`` -> ``process-shard-test-4096-disk``). ``tmp_mb`` (the
    # #258 rule: memory + 2048 for ``-disk``, else 512) is threaded into the
    # record so bench_metrics can bill the ephemeral /tmp cost.
    streaming_mode = target.get("streaming_mode")
    if streaming_mode:
        config.aggregation.setdefault("streaming", {})["mode"] = streaming_mode
    worker = target.get("worker")
    tmp_mb = None
    resolved_function_name = function_name
    if worker:
        config.worker = dict(worker)
        resolved_function_name = resolve_variant(function_name, worker)
        tmp_mb = worker["memory"] + 2048 if worker.get("extra_disk") else 512
    # parent_order lives in the config grid for HEALPix; the kwarg is just a
    # legacy fallback. Rect grids ignore it. ``from_config`` gives us the grid
    # object the area/cost derivation needs.
    grid = from_config(config)

    ctx = dict(
        context,
        target=name,
        aggregator=target["aggregator"],
        grid_type=target["grid_type"],
        grid_size=target["grid_size"],
        shard_key=shard_key,
        # ShardingCodec A/B label (issue #133), recorded into the series so the
        # renderer can split the new matrix from frozen rows. None on targets
        # without the key (the provisional/legacy ones).
        codec=target.get("codec"),
        # Read-axis label (issue #170): the *_cached targets share codec
        # "inner" with the real inner column, so the renderer needs this to
        # give them their own panel instead of silently overwriting it.
        read=target.get("read"),
        # Read-backend axis (issue #193): "inline"|"sidecar" -- the live matrix's
        # A/B, split by the renderer into its two columns. None on frozen rows.
        index_backend=target.get("index_backend"),
        # Store-layout axis (issue #240 phase 4): "flat"|"hive" from the
        # target's config, so the hive regression arm is identifiable in the
        # series (renderers key the 2x2 panels on flat rows).
        store_layout=get_store_layout(config),
        # Streaming-mode + ephemeral axis (issue #272): "spill" on the -disk arm,
        # its /tmp size for the ephemeral cost. None on the default merge targets.
        streaming_mode=streaming_mode,
        tmp_mb=tmp_mb,
        # Staleness-guard provenance (issues #341/#296): "verified" when the
        # resolved variant's CodeSha256 was checked against the base, "skipped"
        # when the probe degraded, "base" when no variant was resolved. Without
        # it a number measured on an unverified build reads identically to one
        # measured on a verified build.
        variant_guard=variant_guard,
    )

    objects = None
    if dry_run:
        # Wiring check only: no AWS, no dispatch. Emit a record with empty metrics.
        summary: dict = {}
    else:
        from zagg.runner import agg

        # NOTE: the issue #341 CodeSha256 staleness guard runs in ``main``,
        # BEFORE the dispatch pool spins up (fold review) — a stale variant
        # must refuse without the sibling targets' dispatches being billed.

        # AOI-mask arm (issue #202): the committed map is the plain o9 map; build
        # the strict-AOI per-cell mask on the fly (mortie, no spherely) and
        # dispatch the augmented map, rather than committing the ~190 KB derived
        # payload. The nomask arm dispatches the committed map unchanged.
        if get_aoi_mask(config):
            shardmap_path = _shardmap_with_mask(
                shardmap_path, grid, _resolve(base, manifest["aoi"]["file"])
            )

        summary = agg(
            config,
            catalog=str(shardmap_path),
            store=store,
            backend="lambda",
            # morton_cell takes the external shard label (the decimal morton
            # string for HEALPix — issue #199), not the raw packed-word digits.
            morton_cell=grid.shard_label(shard_key),
            region=region,
            function_name=resolved_function_name,
            overwrite=True,
            handoff=handoff,
            profile=True,
            # A benchmark measures one clean invocation and records a failure as
            # a failure -- never re-invoke (and never pay) to re-fail (#119).
            max_retries=1,
        )
        # Object-count tripwire (issue #240): LIST the store the run just wrote
        # and compare against the config-derived expectation, so a sharded-write
        # bypass (the issue #215 blow-up) is recorded -- and hard-failed by
        # ``main`` -- instead of drifting as second-order cost.
        if store:
            objects = _measure_objects(config, grid, store, shard_key, region=region)

    return bench_metrics.build_record(
        summary,
        grid=grid,
        context=ctx,
        n_granules=n_granules,
        zagg_version=zagg_version,
        objects=objects,
    )


def check_requested_variants(
    manifest: dict, names: list[str], function_name: str, *, region: str
) -> dict:
    """Probe every distinct worker variant the requested targets resolve (#341).

    Called from ``main`` BEFORE the dispatch pool (fold review): running the
    guard inside ``run_target`` meant a stale variant only raised after its
    sibling targets had already been launched — the pool's ``shutdown(wait=True)``
    then billed every one of them to completion (up to 900 s each) and the
    exception escaped ``main`` before ``metrics.json`` was written, so the run
    paid in full and recorded nothing. Up here the refusal costs two
    ``GetFunctionConfiguration`` calls and zero dispatches.

    Distinct variants are probed once, not once per target that shares them.
    Returns ``{target_name: "verified" | "skipped" | "base"}`` — the per-record
    provenance stamp (``"base"`` = no ``worker:`` block, nothing to verify).
    """
    import boto3

    client = None
    by_variant: dict[str, str] = {}
    status: dict[str, str] = {}
    for name in names:
        resolved = resolve_variant(function_name, _resolve_target(manifest, name).get("worker"))
        if resolved == function_name:
            status[name] = "base"
            continue
        if resolved not in by_variant:
            if client is None:
                client = boto3.client("lambda", region_name=region)
            by_variant[resolved] = check_variant_current(client, function_name, resolved)
        status[name] = by_variant[resolved]
    return status


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the zagg Lambda benchmark.")
    parser.add_argument("--targets", required=True, help="Path to targets.json manifest")
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Target key to run (repeatable). Omit to run every target.",
    )
    parser.add_argument(
        "--store-prefix",
        default=None,
        help="Output store prefix; each target writes <prefix>/<commit>/<target>.zarr "
        "(<prefix>/<target>.zarr when --commit is empty)",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument(
        "--function-name",
        default=os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard"),
        help="Lambda function name",
    )
    parser.add_argument("--event", default="", help="Trigger kind (pr|merge|manual)")
    parser.add_argument("--commit", default="", help="Commit SHA under test")
    parser.add_argument("--ref", default="", help="Branch / PR ref label")
    parser.add_argument("--pr-number", default=None, help="PR number (if a PR run)")
    parser.add_argument("--out-json", default="metrics.json", help="Records JSON output")
    parser.add_argument("--out-comment", default=None, help="Markdown comment output")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Lambda dispatch; emit empty-metric records (wiring check).",
    )
    parser.add_argument(
        "--worker-note",
        default="",
        help="One-line banner for the PR comment when the benchmarked worker is "
        "the stable deploy, not this PR's code (issue #25).",
    )
    parser.add_argument(
        "--fail-on-empty",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit non-zero if any real target comes back with zero observations "
        "or null peak memory -- the signature of a silent OOM that would "
        "otherwise keep the job green (issue #145). Skipped under --dry-run, "
        "which emits empty metrics by design; --no-fail-on-empty opts out.",
    )
    parser.add_argument(
        "--fail-on-object-mismatch",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit non-zero when a target's measured store object count deviates "
        "from the config-derived expectation -- the sharded-write-bypass "
        "tripwire (issues #240/#215). Skipped under --dry-run (no store is "
        "written); --no-fail-on-object-mismatch opts out.",
    )
    args = parser.parse_args(argv)

    manifest, base = load_targets(args.targets)
    # "run all" (no --target) iterates the committed merge matrix only; provisional
    # (PR-tree-only) targets are run by explicit --target name (issue #130).
    names = args.target or all_target_names(manifest)
    known = set(manifest.get("targets", {})) | set(manifest.get("provisional_targets", {}))

    pr_number = int(args.pr_number) if args.pr_number not in (None, "", "0") else None
    context = {
        "timestamp": _utc_now_iso(),
        "commit": args.commit,
        "ref": args.ref,
        "event": args.event,
        "pr_number": pr_number,
    }

    # Validate every requested target up front so an unknown name fails before any
    # dispatch (and before the pool spins up).
    for name in names:
        if name not in known:
            raise SystemExit(f"unknown target '{name}'; have {sorted(known)}")

    # Worker-variant staleness guard (issue #341), in the same pre-dispatch slot
    # as the name validation above: a target whose ``worker:`` block resolves a
    # pre-provisioned variant must run the same code as the base function the
    # deploy path just updated. Read-only probes, no dispatch, no auth yet.
    guard_status = dict.fromkeys(names, "base")
    if not args.dry_run:
        guard_status = check_requested_variants(
            manifest, names, args.function_name, region=args.region
        )

    # Authenticate once up front so the concurrent targets below don't race to
    # initialize earthaccess's process-global auth singleton (issue #137). Skipped
    # under --dry-run (no dispatch, no auth) and pointless for a single target (no
    # fan-out); each agg() still logs in, but now hits the warmed singleton.
    if not args.dry_run and len(names) > 1:
        from zagg.auth import ensure_logged_in

        ensure_logged_in()

    def _dispatch(name: str) -> dict:
        store = None
        if args.store_prefix:
            # Per-commit scoping (issue #362): see bench_metrics.store_path.
            store = bench_metrics.store_path(args.store_prefix, name, commit=args.commit)
        return run_target(
            name,
            manifest,
            base,
            store=store,
            region=args.region,
            function_name=args.function_name,
            context=context,
            dry_run=args.dry_run,
            variant_guard=guard_status.get(name, "base"),
        )

    # Launch all targets concurrently (issue #137). Each is an independent
    # single-shard Lambda dispatch, so fanning them out is cold-favoring (a fresh
    # container per concurrent invoke) and cuts wall-clock ~N x; the runtime metric
    # is billable worker-seconds (init-independent), so en-masse launch doesn't bias
    # it. Results are collected then re-ordered back to ``names`` for deterministic
    # output/series rows; a target that raises propagates (aborting the run) exactly
    # as the prior serial loop did.
    records_by_name: dict = {}
    max_workers = min(len(names), _MAX_TARGET_CONCURRENCY) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_dispatch, name): name for name in names}
        for fut in as_completed(futures):
            records_by_name[futures[fut]] = fut.result()
    records = [records_by_name[name] for name in names]
    for record in records:
        print(
            f"[{record['target']}] obs={record['total_obs']} runtime_s={record['runtime_s']} "
            f"cost/shard=${record['cost_per_shard_usd']} "
            f"cost/100km2=${record['cost_per_100km2_usd']} "
            f"max_memory_mb={record['max_memory_mb']} "
            f"objects={record.get('objects_write_path')} write-path "
            f"(expected {record.get('objects_expected')}; "
            f"{record.get('objects_total')} gross, telemetry {record.get('objects_telemetry')})"
        )

    Path(args.out_json).write_text(json.dumps(records, indent=2))
    if args.out_comment:
        Path(args.out_comment).write_text(
            bench_metrics.comment_markdown(records, worker_note=args.worker_note)
        )

    # A silently OOM'd target records obs=0 / max_memory_mb=None but the job
    # otherwise stays green, so a memory regression can merge (and land a junk
    # series row) unnoticed (issue #145). Fail loudly on any real target that
    # came back empty; --dry-run emits empty metrics by design, so it's exempt.
    if args.fail_on_empty and not args.dry_run:
        empty = [r["target"] for r in records if bench_metrics.has_empty_metrics(r)]
        if empty:
            print(
                "benchmark target(s) returned empty metrics (obs=0 / "
                f"max_memory_mb=None), likely a silent OOM: {', '.join(empty)}",
                file=sys.stderr,
            )
            return 1

    # Object-count tripwire (issue #240): a store whose object count deviates
    # from the config-derived expectation means the write layout regressed (a
    # sharded-write bypass writes ~K objects per array instead of one -- issue
    # #215). Checked AFTER the outputs are written, so metrics.json/the comment
    # still record both the measured and expected counts of the failing run.
    if args.fail_on_object_mismatch and not args.dry_run:
        mismatched = [r for r in records if r.get("objects_mismatch")]
        if mismatched:
            for r in mismatched:
                print(
                    f"[{r['target']}] store object-count mismatch: {r['objects_mismatch']}",
                    file=sys.stderr,
                )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
