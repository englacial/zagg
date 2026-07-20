"""Semantic-core canonicalization + hash (issue #299, D19).

A product's identity splits in two (D19, ``docs/design/sparse_coverage.md``):
the **name** addresses it (the ``{store_root}/{name}/`` product root), and the
**``semantic_hash``** verifies it — sha256 over the canonicalized
**output-defining subset only**. Two configs with the same semantic core
produce the same values in every cell they share; everything else is
packaging.

Included (the semantic core):

- the ``aggregation`` block — functions, params, dtypes, fills, ragged kinds,
  declared coordinates, ``chunk_precompute`` — minus the ``handoff`` carrier
  choice (arrow vs pandas is a worker-internal transport);
- the ``data_source`` **semantics** — which groups/variables/coordinates are
  read and how observations are filtered (``filters``/``quality_filter``,
  photon ``base_level``/``levels``, raster ``bands``/``nodata``/
  ``collections``/``static_data``) — minus the read machinery (``reader``,
  ``driver``, ``read_plan``, ``anonymous``);
- the grid **type + indexing scheme** (D19: cell order is a resolution axis
  (D24), parent/shard order and chunking are packaging — hashing the whole
  template would have made o8 and o9 runs different products and blocked
  mixed-order processing).

Excluded as packaging: all orders (``parent_order``/``child_order``/
``chunk_inner``), ``sharded``, store layout/path, ``emit_cell_ids`` (the
issue #304 transition hatch), worker sizing, streaming mode, read knobs,
catalog/bounds (run inputs, recorded per-run — catalog identity lives in the
D20 sidecar, never the product identity).

Canonical form: the core dict serialized as sorted-key, compact,
ASCII-escaped JSON — so YAML comments, whitespace, and key order can never
change the hash (§8.3 canonicalization obligations). The hash is the **full
sha256 hex digest** (git-style: the full digest is what is compared; the
12-hex :func:`semantic_fingerprint` is the display/CLI shorthand — 48 bits,
comfortable for the only collision domain that matters, display within one
store's product listing).

This module formalizes the #89 signature seam (``grid.spatial_signature`` /
``config.output_field_signature`` are dict fingerprints; this is the
content-addressed form) — see the issue #299 thread for the design record.
"""

from __future__ import annotations

import hashlib
import json

from zagg.config import PipelineConfig

#: ``data_source`` keys that are read machinery, not output semantics (D19).
#: Changing any of these must never change the ``semantic_hash``.
DATA_SOURCE_PACKAGING_KEYS = ("reader", "driver", "read_plan", "anonymous")

#: ``aggregation`` keys that are packaging: the per-cell carrier choice
#: (issue #132) transports identical values either way.
AGGREGATION_PACKAGING_KEYS = ("handoff",)

#: Display length of :func:`semantic_fingerprint` (12 hex = 48 bits; the
#: birthday bound puts same-store collision odds around 1e-8 at 1e4 products
#: — recorded rationale on the issue #299 thread).
FINGERPRINT_HEX = 12

#: Non-healpix grid keys that spatially define the product (F1, issue #299).
#: For rect/other grids the cell geometry is fixed by CRS + resolution +
#: bounds, so two such products differing in any of these are different
#: products (D24's resolution-axis exclusion is a HEALPix/morton composability
#: argument that does not extend to rect — over-discriminating is safe, a
#: semantic collision is not). HEALPix stays type + indexing scheme only.
GRID_SPATIAL_KEYS = ("crs", "resolution", "bounds")


def _without(mapping: dict, keys: tuple[str, ...]) -> dict:
    return {k: v for k, v in (mapping or {}).items() if k not in keys}


def _prune_nulls(obj):
    """Recursively drop ``None``-valued keys from every dict in ``obj``.

    §8.3 canonicalization: a YAML explicit-null (``key:``) must hash identically
    to an absent key, at every depth — not just the top level. Applied to the
    whole core so a nested ``None`` (e.g. ``quality_filter.value:``) drops out.
    Lists are recursed but never pruned by value: list entries are positional,
    so a ``None`` element is content, not an absent key.
    """
    if isinstance(obj, dict):
        return {k: _prune_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_prune_nulls(v) for v in obj]
    return obj


def semantic_core(config: PipelineConfig) -> dict:
    """The output-defining subset of ``config`` (D19), as a plain dict.

    Deterministic given the config's semantics: two configs differing only in
    packaging knobs (orders, chunking, worker size, read machinery, carrier)
    map to the same core. ``None``-valued keys are pruned recursively so a
    YAML explicit-null hashes identically to an absent key (§8.3). The returned
    structure is JSON-serializable plain data (the YAML loader guarantees it).
    """
    grid_cfg = (config.output or {}).get("grid", {}) or {}
    grid_type = grid_cfg.get("type", "healpix")
    grid: dict = {"type": grid_type}
    if grid_type == "healpix":
        # The one indexing scheme zagg writes (the morton store convention
        # rides D16 attrs; the underlying cell tiling is HEALPix NESTED).
        grid["indexing_scheme"] = "nested"
    else:
        # Rect/other: fold in the spatially-defining params when present (F1).
        for key in GRID_SPATIAL_KEYS:
            if key in grid_cfg:
                grid[key] = grid_cfg[key]
    core: dict = {
        "aggregation": _without(config.aggregation, AGGREGATION_PACKAGING_KEYS),
        "data_source": _without(config.data_source, DATA_SOURCE_PACKAGING_KEYS),
        "grid": grid,
    }
    return _prune_nulls(core)


def canonical_semantic_json(config: PipelineConfig) -> str:
    """The canonical serialized form the hash is computed over.

    Sorted keys, compact separators, ASCII-escaped: syntactic YAML edits
    (comments, whitespace, key order) cannot reach this string.
    """
    return json.dumps(
        semantic_core(config), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def semantic_hash(config: PipelineConfig) -> str:
    """Full sha256 hex digest of the canonical semantic core (64 hex chars).

    The frozen manifest key (D19): reusing a product name with a different
    semantic hash refuses up front, exactly as an orders mismatch does. Always
    compare the FULL digest; display via :func:`semantic_fingerprint`.
    """
    return hashlib.sha256(canonical_semantic_json(config).encode()).hexdigest()


def semantic_fingerprint(digest: str) -> str:
    """12-hex display shorthand of a full ``semantic_hash`` digest."""
    if len(digest) < FINGERPRINT_HEX:
        raise ValueError(f"not a semantic hash digest: {digest!r}")
    return digest[:FINGERPRINT_HEX]


__all__ = [
    "AGGREGATION_PACKAGING_KEYS",
    "DATA_SOURCE_PACKAGING_KEYS",
    "FINGERPRINT_HEX",
    "GRID_SPATIAL_KEYS",
    "canonical_semantic_json",
    "semantic_core",
    "semantic_fingerprint",
    "semantic_hash",
]
