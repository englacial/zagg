"""Re-declare a live store's pyramid to the dense ``/2`` every-order ladder.

The store-track S1 artifact (Track C of the 2026-09-10 plan; espg ruling of
record: **dense spacing-1** — every order ``[shard_order-1 .. 0]``, matching
spec §4.4's every-order law). Two intended invocations, run by the operator
(never by an agent — live-store writes are operator runs):

    # atl03_tdigest_o9: replaces the v1 spacing-2 [7,5,3,1] block
    # (declared 08-24, never materialized) with the dense ladder.
    uv run python tools/redeclare_dense_ladder.py \\
        s3://<bucket>/<prefix>/atl03_tdigest_o9.zarr \\
        --config <original-atl03-config>.yaml --overviews 13

    # gedi_flux_o9: replaces the declared-off block (``orders: []``,
    # the demo/12 ruling) with the same-shape ladder.
    uv run python tools/redeclare_dense_ladder.py \\
        s3://<bucket>/<prefix>/gedi_flux_o9.zarr \\
        --config <original-gedi-config>.yaml --overviews 12

Both go through :func:`zagg.sweep_overview.declare_pyramid` — the issue #358
retrofit path, whose behavior on a declared-but-never-materialized ``/1``
block and on a declared-off block is pinned by
``tests/test_sweep_overview.py::TestDeclarePyramid`` (the two live-store-shape
tests). ``--overviews N`` injects ``output.pyramid.overviews: N`` into the
loaded config, which is exactly the retrofit edit ``declare_pyramid``'s
semantic guard blesses: ``output.*`` is not in the semantic core, so the
original config plus the pyramid knob hashes identically. The config must be
the store's ORIGINAL build config (for the live ATL03 store that is the
parent_order-9 / delta-4096 variant, which is NOT in the repo — the semantic
guard refuses the repo's delta-8192 variant by hash).

**DRY-RUN is the default.** Without ``--execute`` nothing is written: the
manifest is read (anonymously with ``--anon``), the replacement block is
derived the same way ``declare_pyramid`` derives it, and the manifest diff,
the ladder table, and the semantic-hash verdict are printed. ``--execute``
performs the real single-PUT RMW with full store-truth validation (leaf
probe + semantic guard + frozen-key recheck) — that flag is the operator's.

Version gate: the live stores were written by zagg 0.52.0, whose classifier
admits ``composition`` as ``packed``. A pre-0.52 environment silently drops
``composition`` from the declaration, so this script refuses to run there.
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys


def _require_packed_classifier() -> None:
    """Refuse a pre-0.52 environment (the silent-composition-drop trap)."""
    from zagg.semantics import COMPOSABILITY_CLASSES

    if "packed" not in COMPOSABILITY_CLASSES:
        raise SystemExit(
            "this zagg lacks the 'packed' composability class (pre-0.52) — the live "
            "stores were written by zagg 0.52.0 and a re-declaration from here would "
            "silently drop 'composition' from the ladder; run from zagg >= 0.52.0"
        )


def _load_config_with_ladder(path: str, overviews: int | None):
    """The retrofit config: the original plus the ``/2`` ladder knob."""
    from zagg.config import load_config

    cfg = load_config(path)
    if overviews is None:
        return cfg
    knob = cfg.output.get("pyramid")
    knob = dict(knob) if isinstance(knob, dict) else {}
    # A stale /1 schedule alongside the injected knob would be dead weight in
    # the derivation (``overviews`` wins) — drop it so the config states one
    # schedule, not two.
    knob.pop("orders", None)
    knob.pop("spacing", None)
    knob["overviews"] = int(overviews)
    cfg.output["pyramid"] = knob
    return cfg


def _derive_block(config, manifest: dict) -> dict:
    """The replacement block, derived exactly as ``declare_pyramid`` derives it."""
    from zagg.pyramid import validate_overviews
    from zagg.sweep_overview import build_pyramid_block

    shard_order = int(manifest["shard_order"])
    block = json.loads(json.dumps(build_pyramid_block(config, shard_order)))
    if "overviews" not in block:
        # A /1 or declared-off derivation means the ladder knob never reached
        # the config — this script exists to install the /2 dense ladder only.
        raise SystemExit(
            "the config derives no /2 ladder (output.pyramid absent, false, or a /1 "
            "orders/spacing schedule) — pass --overviews <cell resolution> (13 for "
            "atl03_tdigest_o9, 12 for gedi_flux_o9)"
        )
    validate_overviews(
        block["overviews"][0]["cells"],
        parent_order=shard_order,
        child_order=int(manifest["cell_order"]),
    )
    return block


def _semantic_verdict(manifest: dict, config) -> str:
    from zagg.semantics import semantic_fingerprint, semantic_hash

    stored = manifest.get("semantic_hash")
    if not stored:
        return "semantic_hash ABSENT (pre-#299 store) — fold methods will be taken on trust"
    supplied = semantic_hash(config)
    if supplied == stored:
        return f"semantic_hash MATCH ({semantic_fingerprint(stored)})"
    return (
        f"semantic_hash MISMATCH — config {semantic_fingerprint(supplied)} vs store "
        f"{semantic_fingerprint(stored)}; --execute WILL REFUSE. Supply the store's "
        f"ORIGINAL build config (output.* edits do not change the hash)"
    )


def _print_dry_run(manifest: dict, block: dict, verdict: str) -> None:
    prior = manifest.get("pyramid")
    print(f"semantic guard: {verdict}")
    print()
    if "overviews" in block:
        print("declared ladder (node -> member cell resolutions):")
        for entry in block["overviews"]:
            print(f"  {entry['node']:>2} -> {entry['cells']}")
    fields = block["overview"].get("fields") or {}
    print("field classes: " + json.dumps({n: m.get("class") for n, m in fields.items()}))
    prior_overview = prior.get("overview") if isinstance(prior, dict) else None
    materialized = prior_overview.get("materialized") if isinstance(prior_overview, dict) else None
    if materialized is not None:
        print("prior 'materialized' actuals present — declare_pyramid preserves them verbatim")
    print()
    if prior == block:
        print("manifest pyramid block: IDENTICAL — --execute would be a no-op (no PUT)")
        return
    before = json.dumps(prior, indent=1, sort_keys=True).splitlines()
    after = json.dumps(block, indent=1, sort_keys=True).splitlines()
    print("manifest pyramid diff (stored -> re-declared):")
    for line in difflib.unified_diff(before, after, "pyramid (stored)", "pyramid (re-declared)"):
        print(line.rstrip("\n"))
    print()
    print("DRY-RUN: nothing was written. Re-run with --execute to install (operator only).")


def main(argv=None) -> int:
    _require_packed_classifier()
    parser = argparse.ArgumentParser(
        description="Re-declare a hive store's pyramid to the dense /2 every-order "
        "ladder (dry-run by default; --execute performs the declare_pyramid RMW)."
    )
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument(
        "--config",
        required=True,
        metavar="CONFIG_YAML",
        help="The store's ORIGINAL pipeline config (the semantic guard refuses any other)",
    )
    parser.add_argument(
        "--overviews",
        type=int,
        default=None,
        metavar="CELL_RES",
        help="Leaf overview resolution injected as output.pyramid.overviews (the /2 "
        "declaration; the fixed every-order ladder above the shard follows from it). "
        "13 for atl03_tdigest_o9 (chunk order), 12 for gedi_flux_o9. Omit only if "
        "the config already declares the /2 knob itself",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually install the declaration (full declare_pyramid validation + one "
        "manifest PUT). Without it this is a dry-run: read-only, prints the diff",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--anon",
        action="store_true",
        help="Anonymous store access (skip_signature) — dry-run reads of public buckets",
    )
    args = parser.parse_args(argv)

    from zagg.hive import MANIFEST_NAME, read_manifest

    store_kwargs: dict = {"region": args.region}
    if args.anon:
        store_kwargs["skip_signature"] = True
    config = _load_config_with_ladder(args.config, args.overviews)
    manifest = read_manifest(args.store_root, **store_kwargs)
    if manifest is None:
        raise SystemExit(f"no {MANIFEST_NAME} at {args.store_root} — not a hive store root")
    if any(manifest.get(k) is None for k in ("shard_order", "cell_order")):
        raise SystemExit(
            f"the {MANIFEST_NAME} at {args.store_root} declares no shard_order/cell_order"
        )
    block = _derive_block(config, manifest)

    if not args.execute:
        _print_dry_run(manifest, block, _semantic_verdict(manifest, config))
        return 0

    if args.anon:
        raise SystemExit("--execute writes the manifest; it cannot run with --anon")
    from zagg.sweep_overview import declare_pyramid

    summary = declare_pyramid(args.store_root, config, store_kwargs=store_kwargs)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
