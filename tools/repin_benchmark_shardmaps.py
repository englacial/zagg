"""DELIBERATELY re-pin the benchmark shard-map fixtures (issue #444).

The pins under ``tests/data/benchmark/`` — the committed
``shardmaps/sm_*.json`` maps plus their ``targets.json`` ``shard_key`` /
``n_granules`` entries — move only on purpose. A run of this script IS that
purpose: a convention or grammar change made the old words wrong (the authalic
latitude flip, issue #438 / PR #441, is the case it was written from) and the
pins are being restated against it. Nothing here detects drift. The accident
detector is and stays ``tests/test_benchmark_shardmap.py::
test_pinned_shardmap_no_drift``, which rebuilds the same maps and fails loudly
when a pin moves on its own; this script is that guard's deliberate
counterpart, and it CALLS the guard's own rebuild and pin functions —
``drift.rebuild_shardmap`` and ``drift.select_pin`` — rather than restating
them, so the two cannot build or pin differently. A second copy of the recipe
is precisely what let the uncommitted scratch driver drift off the guard.

One run, per shard-map entry named on the command line:

1. rebuilds the map with ``drift.rebuild_shardmap`` — the entry's config for
   the grid, its resolved AOI/temporal/CMR (per-entry override over the
   top-level manifest default), the committed map's ``metadata.backend``, and
   either the committed ``catalog_parquet`` snapshot when the entry carries one
   (offline) or a live CMR fetch when it does not;
2. selects the pin with ``drift.select_pin`` — for a ``nested_in`` entry over
   the finer shards inside the pinned parent shard only, never the global
   densest, and against the parent's pin as it stands on disk NOW;
3. prunes the written map to the pinned shard when the committed map is pruned
   (``metadata.pruned`` — the 88S ring maps, whose full form is hundreds of MB
   of JSON), carrying that note over verbatim: it is editorial prose, not a
   derived quantity;
4. writes the map and updates the entry's ``shard_key`` / ``n_granules`` in
   ``targets.json``, leaving every other byte of that hand-formatted file
   alone. The entry's ``note`` is prose and is NOT rewritten — restate it in
   the same commit.

``--check`` stops after (3) and reports how the rebuild differs from the
committed bytes instead of writing anything.
``tests/test_benchmark_shardmap.py::test_offline_pin_reproduces_committed_map``
is that path as an acceptance test over the two offline entries. The NEON trio
cannot be checked offline: an ATL03 footprint quad blankets the whole NEON box,
so a local full-catalog snapshot over-includes and inflates the pins
(``tests/data/benchmark/README.md``, "Reproducing / re-pinning the NEON maps")
— they need the live fetch, which is why only the ring entries carry
``catalog_parquet``.

Run from a zagg checkout::

    uv run python tools/repin_benchmark_shardmaps.py --check healpix_o9_88s
    uv run python tools/repin_benchmark_shardmaps.py healpix_o9_88s healpix_o10_88s

Every ``targets.json`` shard map is HEALPix today, so every re-pin runs on the
mortie backend. If a rectilinear map is ever added to the manifest, re-pinning
it will need the non-PyPI exact-S2 ``spherely`` fork installed (README): the
rebuild takes the committed map's own ``metadata.backend``, so ``ShardMap.build``
raises rather than quietly falling back to mortie.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "tests" / "data" / "benchmark"
TARGETS = BENCH / "targets.json"
# The drift guard owns the rebuild recipe and the pin rule; CALLING them
# (rather than restating them) is what keeps this driver on the guard's rails.
sys.path.insert(0, str(REPO / "tests"))

import test_benchmark_shardmap as drift  # noqa: E402

from zagg.catalog.shardmap import ShardMap  # noqa: E402

#: Genuinely volatile: the build's own wall clock, different every run.
#: Legitimately excused from the comparison forever.
VOLATILE_META = ("build_wall_s",)

#: NOT volatile — deterministic on both sides, and simply STALE in the
#: committed bytes: PR #447 made the unpinned HEALPix cover order the SHARD
#: order, where the committed maps recorded the chunk order they were built at
#: (13 -> the grid's 9/10). The assignment it produces is unchanged.
#:
#: This exemption is meant to EXPIRE. The next deliberate re-pin — a run of
#: this script — writes the current value and makes both sides agree, at which
#: point the exemption stops excusing a known delta and starts masking a real
#: regression. ``test_offline_pin_reproduces_committed_map`` pins the stale
#: committed value so it fails loudly then and this tuple gets emptied.
STALE_META = ("mortie_order",)

#: How ``--check`` labels a metadata key that moved without the pin moving.
EXCUSED_META = {
    **{k: " (volatile)" for k in VOLATILE_META},
    **{k: " (stale committed value, drop at the next re-pin)" for k in STALE_META},
}


def entry(sm_key: str) -> dict:
    """One shard-map entry, read from ``targets.json`` as it stands NOW.

    Deliberately not ``drift.MANIFEST`` (loaded once at import): re-pinning a
    parent and its ``nested_in`` child in one run has to extract the child
    against the parent's freshly written pin, not the one this process started
    with. The top-level ``aoi``/``temporal``/``cmr`` defaults the guard
    resolves against are never rewritten here, so those stay the guard's.
    """
    return json.loads(TARGETS.read_text())["shardmaps"][sm_key]


def committed(sm_key: str) -> dict:
    """The committed shard map an entry points at, as JSON."""
    return json.loads((BENCH / entry(sm_key)["path"]).read_text())


def prune_to_pin(rebuilt: ShardMap, key: int, note: str) -> ShardMap:
    """The rebuild reduced to its pinned shard, carrying ``metadata.pruned``.

    The 88S ring maps are committed pruned — their full form is hundreds of MB
    of JSON. ``metadata`` stays the FULL build's (``total_shards`` /
    ``total_pairs`` count the ring, not the surviving shard), matching the
    committed maps, and ``note`` is carried over verbatim: it is editorial
    prose, not a derived quantity.

    ``aoi_mask`` is sliced with ``shard_keys`` rather than dropped: it is
    documented as parallel to them (``ShardMap``), so positional construction
    would silently write a maskless map, and slicing it wrong would be worse
    still. No committed benchmark map carries one today (the strict-AOI arm
    builds its mask at dispatch), so this is the latent case, not a live one.
    """
    i = [j for j, k in enumerate(rebuilt.shard_keys) if int(k) == key][0]
    return ShardMap(
        rebuilt.grid_signature,
        [rebuilt.shard_keys[i]],
        [rebuilt.granules[i]],
        {**rebuilt.metadata, "pruned": note},
        aoi_mask=None if rebuilt.aoi_mask is None else [rebuilt.aoi_mask[i]],
    )


def repin(sm_key: str) -> tuple[ShardMap, int, int]:
    """``(map to write, shard_key, n_granules)`` for one entry.

    The rebuild and the pin rule are the guard's — ``drift.rebuild_shardmap``
    and ``drift.select_pin`` — called, not restated, so this driver cannot
    build or pin differently from the accident detector. Only the parent-pin
    source differs: a ``nested_in`` child extracts against the parent's pin as
    it stands on disk NOW, which is the parent's FRESH pin when both are
    re-pinned in one run.

    The written map is the rebuild pruned to the pinned shard when the
    committed map is pruned, and the whole rebuild otherwise.
    """
    sm_meta = entry(sm_key)
    rebuilt = drift.rebuild_shardmap(sm_key, sm_meta)
    nested_in = sm_meta.get("nested_in")
    parent_key = int(entry(nested_in)["shard_key"]) if nested_in else None
    key, n = drift.select_pin(rebuilt, sm_meta, parent_key)
    note = committed(sm_key)["metadata"].get("pruned")
    if note is None:
        return rebuilt, key, n
    return prune_to_pin(rebuilt, key, note), key, n


def differences(sm_key: str, mapped: ShardMap) -> list[str]:
    """How a rebuilt map differs from the committed one, key by key."""
    old_map = committed(sm_key)
    out = []
    for key in ("grid_signature", "shard_keys", "granules"):
        if getattr(mapped, key) != old_map[key]:
            out.append(f"{key} differs")
    old, new = old_map["metadata"], mapped.metadata
    for key in sorted(set(old) | set(new)):
        if old.get(key) != new.get(key):
            why = EXCUSED_META.get(key, "")
            out.append(f"metadata.{key}: {old.get(key)!r} -> {new.get(key)!r}{why}")
    return out


def update_targets(text: str, sm_key: str, key: int, n: int) -> str:
    """Restate one shard-map entry's pin literals in ``targets.json`` text.

    Surgical rather than a load/dump round trip: the manifest is hand-formatted
    (compact inline ``worker`` objects), so re-serializing would churn lines
    this re-pin does not touch. The entry's prose ``note`` is left alone.

    The entry is located on ``"<key>":`` WITH the colon, which only a JSON key
    can be followed by: a bare ``"<key>"`` also matches a ``"nested_in"``
    *value* (``healpix_o10_88s`` names ``healpix_o9_88s`` that way), so the
    unanchored form would rewrite the wrong entry whenever a child happened to
    be written before its parent.
    """
    decoder = json.JSONDecoder()
    maps_at = text.index("{", text.index('"shardmaps"'))
    _, maps_end = decoder.raw_decode(text, maps_at)
    entry_at = text.index("{", text.index(f'"{sm_key}":', maps_at, maps_end))
    _, entry_end = decoder.raw_decode(text, entry_at)
    entry = text[entry_at:entry_end]
    for field, value in (("shard_key", key), ("n_granules", n)):
        entry, hits = re.subn(rf'("{field}":\s*)\d+', rf"\g<1>{value}", entry, count=1)
        if hits != 1:
            raise ValueError(f"targets.json entry {sm_key!r} has no {field} literal to restate")
    return text[:entry_at] + entry + text[entry_end:]


def nesting_depth(known: dict, sm_key: str) -> int:
    """How many ``nested_in`` hops separate an entry from an unnested ancestor.

    Re-pin order keys on this depth rather than on a parent/child boolean: a
    grandchild (an o11 nested in an o10 nested in an o9 — the nested-pin design
    of issue #148 does not forbid it) sorts EQUAL to its own parent under a
    boolean, leaving whatever order the command line happened to give, which
    can be the wrong one.
    """
    seen: list[str] = []
    while known[sm_key].get("nested_in"):
        seen.append(sm_key)
        sm_key = known[sm_key]["nested_in"]
        if sm_key in seen:
            raise ValueError(f"targets.json nested_in cycle at {sm_key!r}")
    return len(seen)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "shardmaps",
        nargs="+",
        help="shard-map entry names from targets.json (e.g. healpix_o9_88s). "
        "Re-pin only what you mean to commit — each map is a deliberate move.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="rebuild and report differences against the committed bytes; write nothing.",
    )
    args = parser.parse_args(argv)
    known = json.loads(TARGETS.read_text())["shardmaps"]
    unknown = sorted(set(args.shardmaps) - set(known))
    if unknown:
        parser.error(f"unknown shard map(s) {unknown} (known: {sorted(known)})")
    # A nested entry extracts against its parent's pin as it stands on disk, so
    # re-pin shallowest-first even when the command line names them the other
    # way round -- a child re-pinned before its parent would extract against
    # the STALE parent shard and commit a wrong fixture.
    for sm_key in sorted(args.shardmaps, key=lambda k: nesting_depth(known, k)):
        mapped, key, n = repin(sm_key)
        print(f"{sm_key}: pin {key} at {n} granules")
        for line in differences(sm_key, mapped) or ["identical to the committed map"]:
            print(f"  {line}")
        if args.check:
            continue
        path = entry(sm_key)["path"]
        mapped.to_json(str(BENCH / path))
        TARGETS.write_text(update_targets(TARGETS.read_text(), sm_key, key, n))
        print(f"  wrote {path} + its targets.json pin")
        print("  restate the entry's note by hand — this driver does not write prose")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
