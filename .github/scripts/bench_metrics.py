"""Pure metric derivations for the Lambda benchmark CI (issue #110).

Kept import-light and side-effect-free so the workflow CLIs (``run_benchmark``,
``update_series``, ``plot_series``) and the unit tests can all call in. The live
Lambda dispatch lives in ``run_benchmark.py``; everything here is arithmetic over
the run summary ``zagg.runner.agg`` already returns plus the pinned target
metadata, so it runs with no AWS/network access.

The benchmark dispatches exactly ONE shard, so the summary's per-fan-out worker
stats collapse to that single worker: ``worker_max_s`` is the shard's runtime and
``estimated_cost_usd`` is the cost of that one shard.
"""

from __future__ import annotations

import math

# Cost model and grid types come straight from the package so the benchmark can
# never drift from what production actually bills/uses (arm64, 4 GB -- issue #110/#193).
from zagg.dispatch import LAMBDA_MEMORY_GB, LAMBDA_PRICE_PER_GB_SEC
from zagg.grids.healpix import HealpixGrid
from zagg.grids.rectilinear import RectilinearGrid

# Mean Earth radius (IUGG); a HEALPix shard is the sphere area split evenly across
# the 12*4^parent_order cells at the shard (parent) order.
EARTH_RADIUS_KM = 6371.0088
EARTH_AREA_KM2 = 4.0 * math.pi * EARTH_RADIUS_KM**2

# Columns of the retained parquet series (also the keys of the per-run record).
# Ordered for a stable on-disk schema; new columns append at the end.
RECORD_COLUMNS = [
    "timestamp",
    "commit",
    "ref",
    "event",
    "pr_number",
    "target",
    "aggregator",
    "grid_type",
    "grid_size",
    "shard_key",
    "n_granules",
    "total_obs",
    "runtime_s",
    "gb_seconds",
    "cost_per_shard_usd",
    "shard_area_km2",
    "cost_per_100km2_usd",
    "function_timeout_s",
    "worker_pct_timeout",
    "memory_gb",
    "price_per_gb_sec",
    "zagg_version",
    # Appended for issue #120 (stable schema -> new columns go last). Peak worker
    # RSS in MB; null on rows recorded before the worker reported it.
    "max_memory_mb",
    # Appended for issue #133: the ShardingCodec A/B variable -- "sharded" or
    # "inner" (the forward matrix's two columns). Null on legacy/frozen rows that
    # predate the codec axis, so the renderer splits new (codec.notna) from frozen
    # (codec.isna) series on this column.
    "codec",
    # Read-axis label (issue #170): "cached" on the sidecar-read companion
    # targets, None otherwise. Keeps the cached column out of the inner
    # codec slot in the renderer; legacy rows read back NaN and key on codec.
    "read",
    # Wall-time breakdown (issue #180): end-to-end AOI dispatch wall and its
    # three phases. ``runtime_s`` above is the single worker's compute;
    # ``total_wall_s`` is what the orchestrator actually waits (setup +
    # fanout/poll + finalize). ``finalize_s`` is the zarr metadata
    # consolidation invoke -- a ~fixed cost worth tracking for regression
    # (issue #191). Null on rows recorded before these were surfaced.
    "total_wall_s",
    "setup_s",
    "fanout_s",
    "finalize_s",
    # Read-backend axis (issue #193): "inline" | "sidecar" -- the live matrix's
    # A/B column. Null on frozen/legacy rows (codec/read axes predate it).
    "index_backend",
    # Store object counts (issue #240): the measured object total of the run's
    # output store and the config-derived expectation (null when the layout's
    # count is data-dependent, i.e. not exact) -- the sharded-write-bypass
    # tripwire (issue #215). Null on rows recorded before the metric existed.
    # The per-run record additionally carries the JSON-only
    # ``objects_per_shard`` / ``objects_mismatch`` keys, which the reindex to
    # these columns deliberately drops from the parquet series.
    "objects_total",
    "objects_expected",
    # Store-layout axis (issue #240 phase 4): "flat" | "hive". Null on rows
    # recorded before the hive arm existed (read back as flat). The renderers
    # key the inline/sidecar x AOI-mask panels on flat rows only.
    "store_layout",
]


def select_densest_shard(shardmap: dict) -> tuple[int, int]:
    """Return ``(shard_key, n_granules)`` for the densest shard in a shard map.

    Densest = the shard assigned the most granules; ties are broken by the lowest
    ``shard_key``. The rule is deterministic so the pinned benchmark target and
    the drift test (which rebuilds the map from CMR) agree on the same shard.
    """
    pairs = list(zip(shardmap["shard_keys"], shardmap["granules"], strict=True))
    if not pairs:
        raise ValueError("shard map has no shards")
    # min over (-count, key): most granules first, then smallest key.
    neg_count, shard_key = min((-len(g), int(k)) for k, g in pairs)
    return shard_key, -neg_count


def shard_area_km2(grid) -> float:
    """Real-world area of one shard (dispatch unit) for the given output grid."""
    if isinstance(grid, HealpixGrid):
        return EARTH_AREA_KM2 / (12.0 * 4.0**grid.parent_order)
    if isinstance(grid, RectilinearGrid):
        # Projected CRS in metres: one shard is chunk_h x chunk_w cells.
        return (grid.chunk_h * grid.res_y) * (grid.chunk_w * grid.res_x) / 1e6
    raise TypeError(f"unsupported grid type for area: {type(grid).__name__}")


def _runtime_s(summary: dict) -> float | None:
    """Single-shard runtime: the lone worker's wall, falling back to the rollup."""
    for key in ("worker_max_s", "lambda_time_s", "wall_time_s"):
        val = summary.get(key)
        if val is not None:
            return float(val)
    return None


def memory_pct_of_cap(max_memory_mb, memory_gb) -> float | None:
    """Fraction of the Lambda memory cap a shard peaked at (issue #120).

    ``max_memory_mb / (memory_gb * 1024)`` -- 0.0 at idle, ~1.0 at the OOM wall.
    None when either input is missing (``None`` or the float ``NaN`` a legacy
    parquet row degrades to) or the cap is non-positive, so callers (chart
    colouring, comment table) degrade gracefully on legacy rows. Not clamped: a
    value slightly over 1.0 is a real OOM signal worth surfacing.
    """
    if max_memory_mb is None or memory_gb is None:
        return None
    # A legacy parquet row reads back as NaN, not None, after the reindex.
    if isinstance(max_memory_mb, float) and math.isnan(max_memory_mb):
        return None
    cap_mb = memory_gb * 1024.0
    if cap_mb <= 0:
        return None
    return max_memory_mb / cap_mb


def build_record(
    summary: dict,
    *,
    grid,
    context: dict,
    n_granules: int | None = None,
    zagg_version: str = "",
    objects: dict | None = None,
) -> dict:
    """Flatten a one-shard ``agg`` summary into a benchmark record.

    ``context`` carries the run identity (timestamp/commit/ref/event/pr_number/
    target/aggregator/grid_type/grid_size/shard_key) that the summary does not.
    ``objects`` (issue #240) carries the store object-count measurement from
    ``run_benchmark._measure_objects`` -- ``None`` (dry-run / no store) leaves
    the object columns null.
    """
    area = shard_area_km2(grid)
    cost = summary.get("estimated_cost_usd")
    cost_per_100km2 = None
    if cost is not None and area > 0:
        cost_per_100km2 = cost * 100.0 / area

    record = {
        "timestamp": context.get("timestamp", ""),
        "commit": context.get("commit", ""),
        "ref": context.get("ref", ""),
        "event": context.get("event", ""),
        "pr_number": context.get("pr_number"),
        "target": context.get("target", ""),
        "aggregator": context.get("aggregator", ""),
        "grid_type": context.get("grid_type", ""),
        "grid_size": context.get("grid_size", ""),
        "shard_key": context.get("shard_key"),
        "n_granules": n_granules,
        "total_obs": summary.get("total_obs"),
        "runtime_s": _runtime_s(summary),
        "gb_seconds": summary.get("gb_seconds"),
        "cost_per_shard_usd": cost,
        "shard_area_km2": area,
        "cost_per_100km2_usd": cost_per_100km2,
        "function_timeout_s": summary.get("function_timeout_s"),
        "worker_pct_timeout": summary.get("worker_pct_timeout"),
        "memory_gb": LAMBDA_MEMORY_GB,
        "price_per_gb_sec": LAMBDA_PRICE_PER_GB_SEC,
        "zagg_version": zagg_version,
        # Null-safe: absent on an empty/legacy summary -> None -> null parquet cell.
        "max_memory_mb": summary.get("max_memory_mb"),
        # The ShardingCodec A/B label (issue #133), carried in by run_benchmark from
        # the target. None on targets that don't set it (legacy/frozen rows).
        "codec": context.get("codec"),
        "read": context.get("read"),
        "index_backend": context.get("index_backend"),
        # Store-layout axis (issue #240 phase 4): threaded from the target's
        # config by run_benchmark; None on legacy rows (renderers read flat).
        "store_layout": context.get("store_layout"),
        # Wall-time breakdown (issue #180): total end-to-end AOI dispatch wall
        # plus its phases, straight from the agg summary.
        "total_wall_s": summary.get("wall_time_s"),
        "setup_s": summary.get("setup_s"),
        "fanout_s": summary.get("fanout_s"),
        "finalize_s": summary.get("finalize_s"),
    }
    # Store object counts (issue #240). The two scalar columns are retained in
    # the parquet series; per_shard/mismatch ride the metrics.json record only
    # (update_series's reindex drops them).
    o = objects or {}
    record["objects_total"] = o.get("objects_total")
    record["objects_expected"] = o.get("objects_expected")
    record["objects_per_shard"] = o.get("objects_per_shard")
    record["objects_mismatch"] = o.get("objects_mismatch")
    return record


def _fmt(value, spec: str = "") -> str:
    if value is None:
        return "n/a"
    # A failed/legacy row reads back as the float NaN (not None) from parquet;
    # render it as n/a too, and never let it reach ``format(nan, ',d')`` (raises).
    if isinstance(value, float) and math.isnan(value):
        return "n/a"
    return format(value, spec) if spec else str(value)


# Column headers for every rendered benchmark table -- the PR comment, the
# published ``latest.md``, and the latest-table PNG all use these in this order.
TABLE_HEADERS = [
    "target",
    "obs",
    "runtime (s)",
    "wall (s)",
    "cost/shard",
    "cost/100 km²",
    "% timeout",
    "mem (MB)",
    "% cap",
    "objects",
]


def _objects_cell(record: dict) -> str:
    """``measured/expected`` store object counts (issue #240), ``n/a``-safe.

    ``10/10`` when the expectation is exact, bare ``10`` when it is bounded
    (expected null), ``n/a`` on legacy/dry-run rows (both null, or NaN from a
    legacy parquet row).
    """

    def _num(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return int(value)

    total = _num(record.get("objects_total"))
    if total is None:
        return "n/a"
    expected = _num(record.get("objects_expected"))
    return f"{total}/{expected}" if expected is not None else str(total)


def format_record_cells(r: dict) -> dict:
    """Display strings for one record's benchmark columns (keyed by header), plus
    the raw memory fraction under ``mem_frac`` (``None`` when unknown).

    One formatting source so a number renders identically wherever a benchmark
    table is drawn -- the markdown tables and the PNG -- and never disagree.
    """
    mem_frac = memory_pct_of_cap(r.get("max_memory_mb"), r.get("memory_gb"))
    obs = r.get("total_obs")
    return {
        "target": _fmt(r.get("target")),
        "obs": _fmt(obs, ",d") if obs is not None else "n/a",
        "runtime (s)": _fmt(r.get("runtime_s"), ".1f"),
        "wall (s)": _fmt(r.get("total_wall_s"), ".1f"),
        "cost/shard": "$" + _fmt(r.get("cost_per_shard_usd"), ".5f"),
        "cost/100 km²": "$" + _fmt(r.get("cost_per_100km2_usd"), ".5f"),
        "% timeout": _fmt(r.get("worker_pct_timeout"), ".0%"),
        "mem (MB)": _fmt(r.get("max_memory_mb"), ".0f"),
        "% cap": _fmt(mem_frac, ".0%"),
        "objects": _objects_cell(r),
        "mem_frac": mem_frac,
    }


def _table_block(records: list[dict]) -> list[str]:
    """Markdown table lines (header + alignment + one row per record)."""
    lines = [
        "| " + " | ".join(TABLE_HEADERS) + " |",
        "| " + " | ".join(["---"] + ["---:"] * (len(TABLE_HEADERS) - 1)) + " |",
    ]
    for r in records:
        cells = format_record_cells(r)
        lines.append("| " + " | ".join(cells[h] for h in TABLE_HEADERS) + " |")
    return lines


def comment_markdown(records: list[dict], worker_note: str = "") -> str:
    """Render a PR comment table from one run's benchmark records.

    Ephemeral (posted on PRs, not retained -- issue #110): one row per target so a
    reviewer sees cost/runtime regressions in the PR thread without the noise of
    keeping every pre-merge point in the series.

    ``worker_note`` (issue #25): a one-line banner shown above the table when the
    benchmark ran against the *stable* deployed worker but the PR touches
    lambda-deployed code -- so the numbers don't reflect this PR. Keeps a
    plausible-but-wrong figure from reading as real.
    """
    marker = "<!-- zagg-benchmark -->"
    if not records:
        return f"{marker}\nNo benchmark records were produced."

    head = records[0]
    lines = [marker, f"### Lambda benchmark — `{_fmt(head.get('commit'))[:7]}`", ""]
    if worker_note:
        lines += [f"> ⚠️ {worker_note}", ""]
    lines += _table_block(records)
    lines += [
        "",
        f"_arm64 · {_fmt(head.get('memory_gb'))} GB · "
        f"${_fmt(head.get('price_per_gb_sec'))}/GB-s · one shard/target · "
        "pre-merge runs are not retained._",
    ]
    return "\n".join(lines)


def latest_markdown(records: list[dict]) -> str:
    """Render the *retained* latest-merge table, published as ``latest.md`` on the
    benchmarks branch (issue #110).

    Same columns as the PR comment, but without the upsert marker / worker-note
    and with a retained-merge footer. This is the human-readable companion to the
    machine-readable ``metrics.json`` written beside it -- both are the path an
    agent should follow to reference current benchmark numbers, rather than
    scraping the chart/table images.
    """
    if not records:
        return "No benchmark records were produced."

    head = records[0]
    lines = [
        f"### Latest Lambda benchmark — `{_fmt(head.get('commit'))[:7]}`",
        "",
        f"_{_fmt(head.get('timestamp'))} · arm64 · {_fmt(head.get('memory_gb'))} GB · "
        f"${_fmt(head.get('price_per_gb_sec'))}/GB-s · one densest shard/target · "
        "retained merge point._",
        "",
    ]
    lines += _table_block(records)
    lines += ["", "Machine-readable companion: `metrics.json` (same directory)."]
    return "\n".join(lines)
