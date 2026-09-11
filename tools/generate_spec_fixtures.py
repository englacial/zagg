"""Generate the committed spec-conformance fixtures with zagg's REAL writers.

The `docs/specification.md` §7 fixtures (issue #340), under
``tests/data/spec/``: tiny hive stores, each one shard leaf written by the
production write path — ``hive.ensure_manifest`` +
``hive.process_and_write_hive`` (leaf template, sharded dense + ragged
writes, coverage sidecar, commit stamp), or its raster twin
``processing.raster.process_and_write_raster_hive`` — plus one MANIFEST-ONLY
pyramid declaration written by the production declaration paths (issue #382,
no leaf beneath it). Each carries a committed ``*.expected.json`` recording
the decoded values and the §5 O11 content hashes.
``tests/test_spec_conformance.py`` asserts
the fixtures against the shipping readers AND against spec-text-only
decoders, so the spec, the fixtures, and the reader cannot drift apart
silently. moczarr vendors the same fixtures for its parity gates
(espg/moczarr#19/#20).

Run from a zagg checkout::

    uv run python tools/generate_spec_fixtures.py

Geometry (both leaf fixtures): parent order 4 / chunk order 5 / cell order 6 —
16 cells per shard, K = 4 inner chunks of 4 cells, `sharded` (the D17 hive
default), deliberately tiny. Chunk ordinal 2 is left EMPTY to pin the
shard-index absence sentinel (§1.5); populated chunks carry empty cells to
pin the ``b""`` fill (§1.1). Observations are synthetic and deterministic
(seeded rng); regeneration reproduces the same logical values, though stamp
timestamps and compressed bytes may differ across zstd versions — the
conformance tests assert decoded values, never object bytes.

- ``minimal/`` — one UNLOCATED digest field (`h_tdigest`) + `count`.
- ``kitchen_sink/`` — located signal/noise strata + `composition` + `count`
  (the `atl03_tdigest_strata_healpix.yaml` field shapes), including a
  single-photon cell that packs the §3.1 golden word `0xFF000000FF0000FF`
  and a noise-only cell whose signal payload is the empty ``(0, 2)`` array.
- ``flux/`` — the §2.0 weights-declaration surface (issue #424): one
  flux-declared digest field (`rx_flux`, ``weights: flux`` stamped as the
  SIBLING attrs key beside the ``ragged`` block, ``gain`` provenance attrs)
  + `count`. Payload weights are fractional positive reals built through the
  real merge algebra, so per-cell weight sums are NOT integers — the pin
  that a flux reader must not round-trip weights through counts. The
  committed ``minimal/`` (which predates §2.0 and is not regenerated) is the
  absent-key ⇒ counts pin.
- ``column/`` — the ``minimal`` inputs plus an explicit
  ``output.pyramid.overviews: 5`` knob, so the SAME worker invocation also
  writes the §4.6 leaf column (issue #383): ``all.pyramid.zarr`` beside the
  leaf, groups {5, 4} (declared base + the node-order member), `role:
  column` + ``zagg_column`` attrs, its own commit stamp and O11 record.
- ``pyramid/`` — MANIFEST ONLY: the §4.5 ``zagg-pyramid/2`` declaration
  (issue #382, collapsed grammar), produced by the production declaration
  paths end to end (``/1`` template -> production sweep bookkeeping ->
  ``declare_pyramid`` retrofit to ``/2``). Its grid is shard order 3 (see
  :data:`PYRAMID_GRID`) so one manifest carries every ``/2`` reading a
  decoder must tell apart: a multi-resolution leaf entry, the fixed
  every-order ladder rooted at node 0, the #376 fold keys, and the
  preserved ``/1``-era ``materialized`` actuals — plus, through the
  ``pyramid.expected.json`` record of the raw config knob, the leaf-list
  form of the declaration the expansion was derived from. Since issue #392
  the same manifest also carries the §4.9 ``multiscales`` discovery mirror
  installed beside every ``/2`` block (``column/``, committed earlier and
  unregenerated, is the absent-key ⇒ pre-convention pin). No store beneath
  it on purpose — the block is a template-time artifact, decodable from
  ``morton_hive.json`` alone; the ``/2`` artifacts a fleet writes are the
  ``column/`` fixture's job (issue #383 — sweep-side levels are issue #384).
- ``raster_toc/`` — the §8 temporal-declaration surface (issue #443): a
  RASTER ``(time, cells)`` hive leaf (two bands + ``morton`` + ``time``, the
  one unsharded fixture — raster never shards) whose ``time`` coordinate is
  ``uint64`` toc words carrying the ``temporal`` attrs block and no CF
  ``units``/``calendar``. Its three timesteps commit BOTH word variants: two
  multi-member acquisition groups become conservative RANGE words (one from
  member instants seconds apart, one from the STAC ``start_datetime``/
  ``end_datetime`` pair) and a single-member group stays an exact TIMESTAMP.
  Written through ``processing.raster.process_and_write_raster_hive`` with
  only the COG *sampling* faked — a committed fixture must regenerate with
  no network and no GDAL. The other fixtures, which carry no
  ``temporal`` key anywhere, are the absent-key ⇒ legacy pin.

- ``temporal/`` — the §8.2/§8.3/§9 COMPANION surface (issue #410):
  ``minimal/``'s geometry and cell plan with a located AND temporal digest
  field (payload + ``h_tdigest_locations`` + ``h_tdigest_times``, each
  sibling stamped with the declaration of the words IT holds) plus the dense
  per-cell ``observed`` toc array and ``count``. Both toc variants ride both
  shapes: the 1-observation cell is an exact TIMESTAMP, every
  multi-observation cell and merged centroid a conservative RANGE. The words
  are computed here from the generator's inputs and handed to the write path
  — the aggregation kernel that will produce them is #410's next PR — so the
  expectations stay input-derived. ``kitchen_sink/``'s two
  ``*_locations/zarr.json`` siblings, committed before §9 and deliberately
  NOT refreshed (``git checkout --`` them after a regen), are the
  absent-``located`` ⇒ §2.2 pin.
  ``temporal/`` is ALSO the only fixture carrying a **root ``coverage.moc``**
  (issue #480): the §10 ``zagg-coverage-toc/1`` section, written here by the
  production sweep writer (``MocFamily``'s leaf read + finisher) — and, from
  issue #489, the only one with the §10.5 ``coverage.toc`` word-set cover
  sibling that same finisher PUTs beside it. The other fixtures declare no
  temporal field, so a sweep of one produces no section (and no sibling) —
  leaving them without either root object IS §10's absence rule, and keeps
  those trees byte-identical.

- ``demoted/`` — the §4.3 ``demotions`` surface (issue #518):
  ``kitchen_sink/``'s store swept under a hand-installed ``/1`` cascade
  manifest whose composition ``of`` divisor is MIS-DECLARED ``class:
  "none"``. Two committed overviews: the exact-from-leaves level folds
  composition cleanly (no ``demotions`` key — the absence pin), the
  cascaded level fires ``divisor-missing`` and records it.

STALE BY DESIGN: some committed bytes deliberately pin an older writer era
and must NOT be refreshed by a regen — currently ``kitchen_sink/``'s two
``*_locations/zarr.json`` siblings (the pre-§9 absent-``located`` pin
above). Running this script rewrites them; ``git checkout --`` those files
after a targeted regen. ``kitchen_sink/`` was otherwise refreshed for issue
#515 (its manifest now declares the strata ``approximate`` + ``composition``
``packed``, and its ``all.pyramid.zarr`` column carries the strata,
locations and composition groups through the real fold). When regenerating
for one fixture only, ``git checkout --`` the others; a deliberate refresh
of an older-era fixture is its own commit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

#: Order-4 shard both fixtures write (decimal morton key; northern base 1).
SHARD_KEY = "11213"
#: Chunk ordinal (0..3) deliberately left empty — the §1.5 sentinel pin.
EMPTY_CHUNK = 2
#: t-digest compression budget: small enough that the 300-obs cell merges
#: observations into weight>1 centroids (exercising §2.2 common ancestors).
DELTA = 16
#: The §3.1 golden-word photon: per-surface confidences at threshold 2 pack
#: lanes [255, 0, 0, 255, 0, 0, 0, 255] = 0xFF000000FF0000FF.
GOLDEN_CONF = (4, -1, 0, 3, 1)
#: The ``pyramid/`` fixture's ``output.pyramid`` knob (§4.5, issue #382,
#: collapsed grammar): leaf cell resolutions only. Two members exercise the
#: multi-resolution leaf entry; everything above the shard is the fixed
#: every-order ladder, expanded by the production path into the manifest.
PYRAMID_KNOB = {"overviews": [5, 4]}
#: The pyramid fixture's grid: shard order 3 (not the leaf fixtures' 4) so
#: the leaf window (parent_order, child_order) = (3, 6) has TWO interior
#: resolutions — a multi-member leaf entry is impossible on the 4/6 window.
PYRAMID_GRID = {
    "type": "healpix",
    "parent_order": 3,
    "child_order": 6,
    "chunk_inner": 5,
    "sharded": True,
}
#: The synthetic per-level stage actuals the ``pyramid/`` fixture's finisher
#: RMW records (issue #384): the §4.5 regimes this geometry derives — d = 1,
#: node 2 a gather of gen-1 members, nodes 1/0 merges of the relayed gen-1
#: partials (merges-from-raw 2, never 3 for an upfront level).
PYRAMID_STAGE_ACTUALS = {
    2: {
        "cells": 3,
        "regime": "stage-gather",
        "merges_from_raw": 1,
        "source_children": {"folded": 4, "missing": 0, "unreadable": 0},
    },
    1: {
        "cells": 2,
        "regime": "stage-merge",
        "merges_from_raw": 2,
        "source_children": {"folded": 4, "missing": 0, "unreadable": 0},
    },
    0: {
        "cells": 1,
        "regime": "stage-merge",
        "merges_from_raw": 2,
        "source_children": {"folded": 4, "missing": 0, "unreadable": 0},
    },
}

#: Fields the ``pyramid/`` fixture declares beyond the ``minimal`` pair, so
#: one manifest carries every composability class: exact (``count`` +
#: ``h_min``), approximate (``h_tdigest``), and ``none`` (``h_mean`` — no
#: exact merge law, D24).
PYRAMID_EXTRA_VARIABLES = {
    "h_min": {"function": "nanmin", "source": "h", "dtype": "float32", "fill_value": 0},
    "h_mean": {"function": "mean", "source": "h", "dtype": "float32", "fill_value": 0},
}
#: The ``/1``-era sweep actuals the retrofit must preserve (§4.5): the
#: ``{order: fold_source}`` shape the production bookkeeping writer takes.
PYRAMID_V1_ACTUALS = {1: "leaves", 0: "cascade"}

#: The ``flux/`` fixture's §2.0 calibration provenance (issue #424): flux
#: weights are meaningless without the gain constant that produced them, so
#: the spec requires name + version in the payload array's attrs.
FLUX_GAIN = {"name": "spec-fixture-gain", "version": "1"}


def _cell_photons(rng, n, *, signal_frac=0.6):
    """Synthetic photons: heights + 5 confidence columns, ``n`` rows."""
    h = np.round(rng.normal(30.0, 5.0, n), 3).astype(np.float64)
    conf = np.full((n, 5), -1, dtype=np.int64)
    n_sig = int(round(n * signal_frac))
    for i in range(n):
        if i < n_sig:  # signal: strongest confidence 2..4 on 1-2 surfaces
            surfaces = rng.choice(5, size=int(rng.integers(1, 3)), replace=False)
            conf[i, surfaces] = rng.integers(2, 5)
        else:  # noise: everything below threshold
            conf[i] = rng.integers(-2, 2, 5)
    return h, conf


def _point_words(grid, cell_word, n, rng):
    """Order-29 point-kind words jittered around the cell's center."""
    from mortie import MortonIndexArray

    from zagg.grids.morton import morton_words

    lat, lon = grid.cell_centers(np.array([cell_word], dtype=np.uint64))
    lat0, lon0 = float(np.asarray(lat).ravel()[0]), float(np.asarray(lon).ravel()[0])
    lats = np.clip(lat0 + rng.uniform(-1e-5, 1e-5, n), -89.9, 89.9)
    lons = lon0 + rng.uniform(-1e-5, 1e-5, n)
    return morton_words(MortonIndexArray.from_latlon(lats, lons, points=True))


def _flux_digest(h, rng):
    """A §2.0 flux payload built through the real merge algebra (issue #424).

    Fractional positive weights (photoelectron-scale uniforms), sorted
    sub-centroids, k-way merged at :data:`DELTA` — so the committed payload
    exercises `_compress` over WEIGHTED sub-centroids and its per-cell weight
    sums are not integers. A single sample passes through uncompressed (the
    kway single-contributor law), pinning the singleton fractional weight.
    """
    from zagg.stats.tdigest import merge_tdigests_kway

    h = np.asarray(h, dtype=np.float64)
    weights = rng.uniform(0.5, 30.0, len(h))
    order = np.argsort(h, kind="stable")
    sub = np.column_stack([h[order], weights[order]]).astype(np.float32)
    if len(sub) < 2:
        return sub
    # Two sorted halves through the k-way fold: a real weighted compression.
    return merge_tdigests_kway([sub[0::2], sub[1::2]], delta=DELTA)


def _config(kitchen_sink: bool, pyramid: dict | None = None, flux: bool = False):
    from zagg.config import PipelineConfig

    variables: dict = {
        "count": {"function": "len", "source": "h", "dtype": "int32", "fill_value": 0}
    }
    if flux:
        variables["rx_flux"] = {
            "kind": "ragged",
            # The generator feeds precomputed payloads through the write path
            # (the fake process_shard below), so the declared reducer is never
            # exercised here — the real flux transform lands with issue #425.
            "function": "zagg.stats.tdigest.build_tdigest",
            "source": "h",
            "inner_shape": [2],
            "dtype": "float32",
            "fill_value": 0,
            "params": {"delta": DELTA},
            "weights": "flux",
            "attrs": {"gain": dict(FLUX_GAIN)},
        }
    elif kitchen_sink:
        for stratum in ("signal", "noise"):
            variables[f"h_tdigest_{stratum}"] = {
                "kind": "ragged",
                "function": "zagg.stats.tdigest.build_tdigest_where",
                "source": "h",
                "location": "leaf_id",
                "inner_shape": [2],
                "dtype": "float32",
                "fill_value": 0,
                "params": {"delta": DELTA},
                "attrs": {"stratum": stratum, "signal_threshold": 2},
            }
        variables["composition"] = {
            "function": "zagg.stats.composition.pack_composition",
            "source": "h",
            "dtype": "uint64",
            "fill_value": 0,
            "params": {"threshold": 2},
            "attrs": {
                "composition": {
                    "spec": "zagg-composition/1",
                    "lanes": [
                        "land",
                        "ocean",
                        "sea_ice",
                        "land_ice",
                        "inland_water",
                        "low",
                        "med",
                        "high",
                    ],
                    "of": "h_tdigest_signal",
                    "threshold": 2,
                }
            },
        }
    else:
        variables["h_tdigest"] = {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "source": "h",
            "inner_shape": [2],
            "dtype": "float32",
            "fill_value": 0,
            "params": {"delta": DELTA},
        }
    output: dict = {
        "store_layout": "hive",
        "grid": {
            "type": "healpix",
            "parent_order": 4,
            "child_order": 6,
            "chunk_inner": 5,
            "sharded": True,
        },
    }
    if pyramid is not None:
        output["pyramid"] = pyramid
    return PipelineConfig(
        data_source={"groups": ["g"]},
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": variables,
        },
        output=output,
    )


def _build_cells(grid, shard, kitchen_sink: bool, flux: bool = False):
    """Per-cell synthetic inputs and their expected decoded values.

    Returns ``(by_chunk, expected_cells)`` where ``by_chunk`` maps a chunk
    ordinal to ``{local_cell: field_values}`` ready for the write path.
    """
    from zagg.stats.composition import pack_composition
    from zagg.stats.tdigest import build_tdigest, build_tdigest_where

    rng = np.random.default_rng(340)
    children = grid.children(shard)
    # (chunk ordinal, local cell, n obs, kind) — kind only used kitchen-sink.
    plan = [
        (0, 0, 40, "mixed"),
        (0, 2, 1, "golden"),
        (1, 1, 5, "noise_only"),
        (3, 3, 300, "mixed"),
    ]
    by_chunk: dict = {}
    expected = []
    for chunk, local, n, kind in plan:
        cell_index = chunk * grid.cells_per_chunk + local
        cell_word = int(children[cell_index])
        h, conf = _cell_photons(rng, n, signal_frac={"mixed": 0.6}.get(kind, 0.0))
        if kind == "golden":
            conf = np.array([GOLDEN_CONF], dtype=np.int64)
        if kind == "noise_only":
            conf[:] = rng.integers(-2, 2, conf.shape)
        record: dict = {"index": cell_index, "morton": str(cell_word), "count": n}
        fields: dict = {"count": n}
        if flux:
            digest = _flux_digest(h, rng)
            fields["rx_flux"] = digest
            record["rx_flux"] = [[float(m), float(w)] for m, w in digest]
            # The weight total (float64 sum over the float32 weights), recorded
            # so the non-integer pin is a committed expectation, not derived.
            record["flux_sum"] = float(digest[:, 1].astype(np.float64).sum())
        elif kitchen_sink:
            words = _point_words(grid, cell_word, n, rng)
            signal = (conf >= 2).any(axis=1)
            for stratum, mask in (("signal", signal), ("noise", ~signal)):
                digest, locs = build_tdigest_where(h, DELTA, where=mask, locations=words)
                fields[f"h_tdigest_{stratum}"] = (digest, locs)
                record[f"h_tdigest_{stratum}"] = [[float(m), float(w)] for m, w in digest]
                record[f"h_tdigest_{stratum}_locations"] = [str(int(w)) for w in locs]
            word = pack_composition(
                h,
                conf_land=conf[:, 0],
                conf_ocean=conf[:, 1],
                conf_sea_ice=conf[:, 2],
                conf_land_ice=conf[:, 3],
                conf_inland_water=conf[:, 4],
                threshold=2,
            )
            fields["composition"] = word
            record["composition"] = str(word)
            record["n_signal"] = int(signal.sum())
        else:
            digest = build_tdigest(h, DELTA)
            fields["h_tdigest"] = digest
            record["h_tdigest"] = [[float(m), float(w)] for m, w in digest]
        by_chunk.setdefault(chunk, {})[local] = fields
        expected.append(record)
    return by_chunk, expected


def _fake_process_shard(grid, by_chunk, kitchen_sink: bool, ragged_field: str = "h_tdigest"):
    """A ``process_shard`` stand-in feeding the REAL sharded leaf write path."""

    def fake(g, shard_key, urls, **kwargs):
        occupied = []
        for ordinal, (block, children) in enumerate(grid.iter_chunks(int(shard_key))):
            cells = by_chunk.get(ordinal, {})
            if not cells:
                kwargs["chunk_results"].append((block, pd.DataFrame(), {}))
                continue
            n = grid.cells_per_chunk
            df = pd.DataFrame({"morton": np.asarray(children, dtype=np.uint64)})
            df["count"] = np.array(
                [cells.get(i, {}).get("count", 0) for i in range(n)], dtype=np.int32
            )
            ragged: dict = {}
            if kitchen_sink:
                df["composition"] = np.array(
                    [cells.get(i, {}).get("composition", 0) for i in range(n)],
                    dtype=np.uint64,
                )
                for field in ("h_tdigest_signal", "h_tdigest_noise"):
                    ids = sorted(i for i, f in cells.items() if field in f)
                    ragged[field] = (
                        [cells[i][field][0] for i in ids],
                        ids,
                        [cells[i][field][1] for i in ids],
                    )
            else:
                ids = sorted(cells)
                ragged[ragged_field] = ([cells[i][ragged_field] for i in ids], ids)
            kwargs["chunk_results"].append((block, df, ragged))
            occupied.extend(int(children[i]) for i in sorted(cells))
        kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
        return pd.DataFrame(), {
            "shard_key": int(shard_key),
            "cells_with_data": len(occupied),
            "total_obs": 346,
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": None,
        }

    return fake


def _element_bytes(element) -> bytes:
    """One vlen cell's payload bytes, per the §5.2 normalization.

    ``None`` (an unwritten cell may decode as ``None``, not ``b""``) is
    zero-length; bytes are as-is; a typed ``/2`` ndarray cell normalizes to
    C-contiguous little-endian bytes. Anything else RAISES — a silently wrong
    digest is worse than no digest.
    """
    if element is None:
        return b""
    if isinstance(element, bytes | bytearray | memoryview):
        return bytes(element)
    if isinstance(element, str):
        return element.encode()
    if isinstance(element, np.ndarray):
        values = np.ascontiguousarray(element)
        if values.dtype.byteorder == ">":
            values = values.astype(values.dtype.newbyteorder("<"))
        return values.tobytes()
    raise ValueError(f"vlen element of type {type(element).__name__} has no O11 byte recipe")


def _o11_hashes(leaf_path: str) -> dict:
    """The §5 O11 recipe over every array beneath the leaf root."""
    import zarr

    group = zarr.open_group(zarr.storage.LocalStore(leaf_path), mode="r", zarr_format=3)
    hashes: dict[str, str] = {}
    for key, node in group.members(max_depth=None):
        if not isinstance(node, zarr.Array):
            continue
        values = np.ascontiguousarray(node[...])
        if values.dtype.kind == "O":  # vlen: length-prefixed payloads, C order
            digest = hashlib.sha256()
            for element in values.ravel(order="C"):
                payload = _element_bytes(element)
                digest.update(len(payload).to_bytes(8, "little"))
                digest.update(payload)
            hashes[key] = digest.hexdigest()
            continue
        if values.dtype.byteorder == ">":
            values = values.astype(values.dtype.newbyteorder("<"))
        hashes[key] = hashlib.sha256(values.tobytes()).hexdigest()
    combined = hashlib.sha256("\n".join(sorted(hashes.values())).encode()).hexdigest()
    # Key-sorted so regeneration is diff-clean: ``group.members()`` yields
    # concurrently, so insertion order varies per run. ``combined`` is immune
    # either way (it sorts the DIGESTS), which is a real robustness property
    # of the recipe — but the recorded map would otherwise churn every run.
    return {"arrays": dict(sorted(hashes.items())), "combined": combined}


def build(out: Path, kitchen_sink: bool, pyramid: dict | None = None, flux: bool = False) -> None:
    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid
    from zagg.grids.morton import morton_word

    cfg = _config(kitchen_sink, pyramid=pyramid, flux=flux)
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=True)
    shard = morton_word(SHARD_KEY)
    by_chunk, expected_cells = _build_cells(grid, shard, kitchen_sink, flux=flux)
    assert EMPTY_CHUNK not in by_chunk

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    root = str(out)
    hive.ensure_manifest(
        root,
        hive.build_manifest(grid, dataset={"short_name": "SPEC_FIXTURE", "version": "1"}),
    )
    original = processing.process_shard
    processing.process_shard = _fake_process_shard(
        grid, by_chunk, kitchen_sink, ragged_field="rx_flux" if flux else "h_tdigest"
    )
    try:
        meta = hive.process_and_write_hive(
            shard,
            ["s3://fixture/a.h5"],
            grid,
            {},
            root,
            cfg,
            store_kwargs={},
        )
    finally:
        processing.process_shard = original
    assert meta.get("error") is None, meta

    leaf_rel = hive.shard_leaf_path("", shard).lstrip("/")
    expected = {
        "shard": SHARD_KEY,
        "leaf": leaf_rel,
        "group": grid.group_path,
        "shard_order": 4,
        "chunk_order": 5,
        "cell_order": 6,
        "cells_per_chunk": grid.cells_per_chunk,
        "chunks_per_shard": grid.chunks_per_shard,
        "empty_chunk": EMPTY_CHUNK,
        "delta": DELTA,
        "cells": expected_cells,
        "content_hashes": _o11_hashes(str(out / leaf_rel)),
    }
    if flux:
        # The §2.0 declaration + provenance the conformance tests assert
        # against the committed array attrs (issue #424).
        expected["weights"] = "flux"
        expected["gain"] = dict(FLUX_GAIN)
    if pyramid is not None:
        # The §4.6 column fixture (issue #383): the SAME worker invocation
        # wrote the leaf's column; record the raw knob, the decoded groups,
        # and the column's own O11 hashes so a spec-text decoder can be
        # asserted against committed bytes.
        # ``meta["leaf_column"]`` is the seam's own key (issue #388 renamed it
        # from the SQL-reserved ``column``); the fixture's ``column`` block
        # below is the fixture schema's name and stays as published.
        assert meta.get("leaf_column"), meta
        expected["declared"] = pyramid
        expected["column"] = _column_expected(
            out / leaf_rel.rsplit("/", 1)[0] / meta["leaf_column"], meta["leaf_column"]
        )
    expected_path = out.parent / f"{out.name}.expected.json"
    expected_path.write_text(json.dumps(expected, indent=1) + "\n")
    print(f"{out.name}: leaf {leaf_rel}, {len(expected_cells)} populated cells")


def _column_expected(column_dir: Path, basename: str) -> dict:
    """The committed §4.6 column record: attrs, decoded groups, O11 hashes."""
    import zarr

    store = zarr.storage.LocalStore(str(column_dir))
    attrs = dict(zarr.open_group(store, mode="r", zarr_format=3).attrs)
    fields = attrs["zagg_column"]["fields"]
    groups: dict = {}
    for res in sorted(attrs["zagg_column"]["groups"], key=int, reverse=True):
        group = zarr.open_group(store, path=res, mode="r", zarr_format=3)
        record: dict = {"morton": [str(w) for w in group["morton"][:]]}
        for name, meta in fields.items():
            # Element typing comes from the attrs block (§4.6 carries dtype +
            # inner_shape for exactly this), never hardcoded: a kitchen-sink
            # column's digest field need not be float32/(-1, 2), and its
            # ``none``-class fields are absent from the artifact entirely.
            if meta["class"] == "approximate":
                dtype = np.dtype(meta["dtype"]).newbyteorder("<")
                inner = tuple(meta["inner_shape"])
                record[name] = [
                    np.frombuffer(bytes(p), dtype).reshape((-1, *inner)).tolist()
                    for p in group[name][:]
                ]
            elif meta["class"] == "exact":
                record[name] = [v.item() for v in group[name][:]]
        groups[res] = record
    return {
        "object": basename,
        "role": attrs["role"],
        "zagg_column": attrs["zagg_column"],
        "commit": {
            k: v for k, v in attrs["morton_hive_commit"].items() if k != "written_at"
        },  # timestamps move on regeneration; the spec pins fields, not clocks
        "groups": groups,
        "content_hashes": _o11_hashes(str(column_dir)),
    }


def build_demoted(out: Path) -> None:
    """The §4.3 ``demotions`` fixture (issue #518): the packed rail, recorded.

    The ``kitchen_sink/`` store swept under a hand-installed ``/1`` cascade
    manifest whose ``of`` divisor digest is MIS-DECLARED ``class: "none"`` —
    the acceptance shape issue #518 names (a manifest outliving its writer,
    spec §4.5). The exact-from-leaves level still folds ``composition`` (the
    leaf ARRAYS carry the digest regardless of its declared class), so its
    attrs carry no ``demotions`` key; the cascaded level folds a child
    overview that never materializes the divisor, fires ``divisor-missing``
    on its one contributor, and records it. ``demoted.expected.json`` pins
    both levels — the clean level's absent key included — so a reader's
    zero-open filtering can be asserted against committed bytes.
    """
    import zarr

    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.pyramid import declared_fields
    from zagg.store import open_object_store, open_store, put_object
    from zagg.sweep import _node_rel
    from zagg.sweep_overview import OVERVIEW_ATTR, sweep_overviews

    build(out, kitchen_sink=True)
    fields, excluded = declared_fields(_config(kitchen_sink=True))
    assert not excluded, excluded
    # The mis-declared divisor: composition stays packed-with-of while the
    # digest it names is declared class ``none``. ``declared_fields`` never
    # writes this pair (it demotes composition at declare time) — the shape
    # arises from a manifest written by another era or another tool.
    fields["h_tdigest_signal"] = {"class": "none"}
    manifest = read_manifest(str(out))
    manifest["pyramid"] = {
        "spec": "zagg-pyramid/1",
        "overview": {
            "spacing": 1,
            "orders": [3, 2],
            "all_time": False,
            "fold_source": "cascade",
            "exact_levels": 1,
            "fields": fields,
        },
    }
    put_object(open_object_store(str(out)), MANIFEST_NAME, json.dumps(manifest, indent=1).encode())
    counts = sweep_overviews(str(out), manifest, {SHARD_KEY: {None}})
    assert counts["failed"] == 0 and counts["written"] == 2, counts

    def _attrs(node):
        group = zarr.open_group(
            open_store(f"{out}/{_node_rel(node)}/all.zarr", read_only=True),
            path="",
            mode="r",
            zarr_format=3,
        )
        return dict(dict(group.attrs)[OVERVIEW_ATTR])

    clean, demoted = _attrs("1121"), _attrs("112")
    assert "demotions" not in clean, clean
    assert demoted.get("demotions"), demoted
    # MERGED into the leaf record ``build`` wrote (the leaf-identity gates —
    # semantic hash, granules sibling — key off those fields), plus the
    # demotion surface this fixture exists for.
    expected_path = out.parent / f"{out.name}.expected.json"
    expected = json.loads(expected_path.read_text())
    expected["orders"] = [3, 2]
    expected["clean"] = {
        "node": "1121",
        "object": f"{_node_rel('1121')}/all.zarr",
        "fold_source": clean["fold_source"],
    }
    expected["demoted"] = {
        "node": "112",
        "object": f"{_node_rel('112')}/all.zarr",
        "fold_source": demoted["fold_source"],
        "demotions": demoted["demotions"],
    }
    expected_path.write_text(json.dumps(expected, indent=1) + "\n")
    print(f"{out.name}: demotions at node 112 -> {demoted['demotions']}")


def build_pyramid(out: Path) -> None:
    """The manifest-only fixture: the §4.5 ``zagg-pyramid/2`` declaration.

    Every byte of the committed manifest comes from a production declaration
    path, in the order a real store would live it: templated under ``/1``
    (``hive.build_manifest`` — no ``overviews`` knob), given sweep actuals by
    the production bookkeeping writer
    (``sweep_overview._update_manifest_pyramid``, the function the real ``/1``
    sweep calls), then retrofitted to ``/2`` with ``declare_pyramid`` and the
    :data:`PYRAMID_KNOB` config — which must preserve those ``/1``-era
    actuals verbatim (§4.5). No leaf exists on purpose (the declaration is a
    template-time artifact; ``declare_pyramid``'s field probe skips loudly),
    so this fixture writes exactly one object: ``morton_hive.json``.

    The expectations are derived HERE from the generator's INPUTS
    (:data:`PYRAMID_KNOB` expanded by the §4.5 leaf-entry rule and the §4.4
    fixed-ladder law, slab lengths by the §4.4 rule), never read back out of
    the written manifest — the same discipline the leaf fixtures' cell
    values follow.
    """
    from zagg import hive
    from zagg.grids import HealpixGrid
    from zagg.sweep_overview import _update_manifest_pyramid, declare_pyramid

    cfg_v1 = _config(False)
    cfg_v2 = _config(False, pyramid=PYRAMID_KNOB)
    for cfg in (cfg_v1, cfg_v2):
        cfg.aggregation["variables"].update(PYRAMID_EXTRA_VARIABLES)
        cfg.output["grid"] = dict(PYRAMID_GRID)
    grid = HealpixGrid(3, 6, layout="fullsphere", config=cfg_v1, chunk_inner=5, sharded=True)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    hive.ensure_manifest(
        str(out),
        hive.build_manifest(grid, dataset={"short_name": "SPEC_FIXTURE", "version": "1"}),
    )
    assert _update_manifest_pyramid(str(out), dict(PYRAMID_V1_ACTUALS), {})
    summary = declare_pyramid(str(out), cfg_v2)
    assert summary["updated"] is True, summary
    # Per-entry actuals (issue #384): the finisher's manifest RMW is the
    # production writer. The per-level inputs are synthetic (no leaves exist
    # here) but their regimes are the §4.5 law for this geometry — d = 1, so
    # node 2 gathers (cells 3 == shard) and nodes 1/0 merge the relayed
    # gen-1 partials at exactly 2 merges from raw.
    from zagg.sweep_stages import run_finisher

    run_finisher(
        str(out),
        hive.read_manifest(str(out)),
        {},
        dict(PYRAMID_STAGE_ACTUALS),
        run_id="spec-fixture",
    )
    # The fully expanded (node, cells) list, spelled from the KNOB by the
    # §4.5 leaf-entry rule plus the §4.4 fixed-ladder law (d = base - shard;
    # one member per order from shard - 1 down to 0), and §4.4's slab rule.
    s = PYRAMID_GRID["parent_order"]
    resolutions = list(PYRAMID_KNOB["overviews"])
    d = resolutions[-1] - s
    levels = [{"node": s, "cells": resolutions}] + [
        {"node": k, "cells": [k + d]} for k in range(s - 1, -1, -1)
    ]
    chunk = PYRAMID_GRID["chunk_inner"]
    expected = {
        "shard_order": s,
        "chunk_order": chunk,
        "cell_order": PYRAMID_GRID["child_order"],
        "declared": PYRAMID_KNOB,
        "overviews": levels,
        "slabs": [[4 ** (r - e["node"]) for r in e["cells"]] for e in levels],
        "fold_source": "cascade",
        "exact_levels": 1,
        "materialized": {
            "orders": sorted(PYRAMID_V1_ACTUALS),
            "fold_sources": {str(k): v for k, v in PYRAMID_V1_ACTUALS.items()},
        },
        "fields": {
            "count": "exact",
            "h_tdigest": "approximate",
            "h_min": "exact",
            "h_mean": "none",
        },
        # §4.5 per-entry actuals (issue #384) — regimes/counts from the
        # synthetic finisher inputs above; the leaf entry records the
        # leaf-column law. Timestamp/run-id values are not pinned.
        "actuals": {
            str(s): {"regime": "leaf-column", "merges_from_raw": 1},
            **{
                str(k): {
                    "regime": v["regime"],
                    "merges_from_raw": v["merges_from_raw"],
                    "source_children": v["source_children"],
                }
                for k, v in PYRAMID_STAGE_ACTUALS.items()
            },
        },
        # The §4.5 omitted-knob default for this geometry ([chunk_order] at
        # the leaf, then the same fixed ladder), pinned so zagg's derivation
        # cannot drift from the formula on the page.
        "default_overviews": [{"node": s, "cells": [chunk]}]
        + [{"node": k, "cells": [k + (chunk - s)]} for k in range(s - 1, -1, -1)],
        # The §4.9 discovery mirror the retrofit installs beside a /2 block
        # (issue #392) — spelled from the same INPUTS by §4.9's projection
        # rules (datasets verbatim from the expanded levels, artifact kind by
        # the node-order rule, order2res the flat per-order lookup), never
        # read back from the written manifest.
        "multiscales": [
            {
                "spec": "zagg-multiscales/1",
                "name": "SPEC_FIXTURE",
                "base": {"order": s, "cells": [PYRAMID_GRID["child_order"]]},
                "datasets": [
                    {
                        "order": e["node"],
                        "cells": list(e["cells"]),
                        "artifact": "column" if e["node"] == s else "overview",
                    }
                    for e in levels
                ],
                "order2res": {str(e["node"]): list(e["cells"]) for e in levels},
                "fields": {
                    "count": "exact",
                    "h_tdigest": "approximate",
                    "h_min": "exact",
                    "h_mean": "none",
                },
                "fold": {"fold_source": "cascade", "exact_levels": 1},
            }
        ],
    }
    (out.parent / f"{out.name}.expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(f"{out.name}: manifest-only, {len(levels)} level entries, /1 actuals preserved")


#: The ``raster_toc/`` fixture's acquisition groups (§8, issue #443). Three
#: timesteps that between them exercise both toc word variants: two
#: multi-member datatakes (a RANGE word — one derived from member instants
#: seconds apart, one from the STAC ``start_datetime``/``end_datetime`` pair)
#: and one single-member acquisition (an exact TIMESTAMP word).
RASTER_GRANULES = [
    {
        "id": "dt-1-a",
        "assets": {"red": "s3://fixture/dt1a_red.tif", "scl": "s3://fixture/dt1a_scl.tif"},
        "datetime": "2025-06-15T15:06:40+00:00",
        "time_key": "dt-1",
    },
    {
        "id": "dt-1-b",
        "assets": {"red": "s3://fixture/dt1b_red.tif", "scl": "s3://fixture/dt1b_scl.tif"},
        "datetime": "2025-06-15T15:06:47+00:00",
        "time_key": "dt-1",
    },
    {
        "id": "dt-2-a",
        "assets": {"red": "s3://fixture/dt2a_red.tif", "scl": "s3://fixture/dt2a_scl.tif"},
        "datetime": "2025-06-18T15:06:40+00:00",
        "time_key": "dt-2",
        "time_start": "2025-06-18T15:06:38+00:00",
        "time_end": "2025-06-18T15:06:49+00:00",
    },
    {
        "id": "dt-3-a",
        "assets": {"red": "s3://fixture/dt3a_red.tif", "scl": "s3://fixture/dt3a_scl.tif"},
        "datetime": "2025-06-21T15:06:40+00:00",
        "time_key": "dt-3",
    },
]
#: The raster fixture's bands: the shipped Sentinel-2 pair, trimmed to two.
RASTER_BANDS = {
    "red": {"asset": "red", "dtype": "uint16", "fill_value": 0, "scale": 0.0001, "offset": -0.1},
    "scl": {"asset": "scl", "dtype": "uint8", "fill_value": 0},
}


def _raster_acquisitions() -> list[dict]:
    """The real acquisition envelope per group, from ``RASTER_GRANULES``.

    The same grouping the encoder applies (``time_key``, ``time_start``/
    ``time_end`` falling back to ``datetime``), in the same row order the
    axis has (ascending group earliest-item ``datetime``), with the ``+00:00``
    suffix trimmed so the values compare as naive ISO instants.
    """
    span: dict[str, tuple[str, str, str]] = {}
    for g in RASTER_GRANULES:
        key = g["time_key"]
        lo, hi = g.get("time_start", g["datetime"]), g.get("time_end", g["datetime"])
        if key in span:
            was_lo, was_hi, was_dt = span[key]
            span[key] = (min(was_lo, lo), max(was_hi, hi), min(was_dt, g["datetime"]))
        else:
            span[key] = (lo, hi, g["datetime"])
    return [
        {"key": key, "start": lo[:-6], "end": hi[:-6]}
        for key, (lo, hi, _dt) in sorted(span.items(), key=lambda kv: (kv[1][2], kv[0]))
    ]


def _raster_slab(t_idx: int, n_cells: int):
    """One deterministic ``(cells,)`` slab per band, with fill holes.

    Cells whose ordinal is congruent to the timestep mod 5 stay at the band
    fill — pull-NN leaves a cell outside the source footprint untouched, so a
    reader must not assume every band row is populated.
    """
    ordinals = np.arange(n_cells)
    valid = (ordinals % 5) != (t_idx % 5)
    red = np.where(valid, 1000 + 37 * t_idx + ordinals, 0).astype(np.uint16)
    scl = np.where(valid, 4 + (ordinals % 3), 0).astype(np.uint8)
    return {"red": red, "scl": scl}, valid


def _raster_toc_config():
    """The ``raster_toc/`` fixture's config, factored out of the builder so a
    test can recompute the fixture's ``semantic_hash`` from it (issue #415)."""
    from zagg.config import load_config_from_dict

    return load_config_from_dict(
        {
            "data_source": {"reader": "raster", "bands": RASTER_BANDS, "nodata": 0},
            "output": {
                "grid": {
                    "type": "healpix",
                    "parent_order": 4,
                    "child_order": 6,
                    "chunk_inner": 5,
                    # Raster never shards (§8/#247): K = 4 inner chunks of 4
                    # cells, one object per (timestep, chunk).
                    "sharded": False,
                },
                "store_layout": "hive",
                "time_encoding": "toc",
            },
        }
    )


def build_raster_toc(out: Path) -> None:
    """The §8 ``raster_toc/`` fixture: a toc-declared ``(time, cells)`` leaf.

    Written through the production raster hive seam
    (``processing.raster.process_and_write_raster_hive`` — leaf template,
    per-timestep slab streaming, coverage sidecar, commit stamp, O11
    hashing), with only the COG **sampling** faked: the fixture pins the
    stored bytes and the time-axis declaration, not the pull-NN arithmetic
    (``tests/test_raster.py`` owns that), and a committed fixture must never
    need network or GDAL to regenerate.
    """
    import zarr

    from zagg import hive
    from zagg.grids import from_config
    from zagg.grids.morton import morton_word
    from zagg.processing import raster as raster_mod
    from zagg.time_axis import decode_time_axis, time_axis_attrs

    cfg = _raster_toc_config()
    grid = from_config(cfg)
    shard = morton_word(SHARD_KEY)
    n_cells = grid.cells_per_shard

    def _fake_process_raster_shard(
        _grid, shard_key, granules, _config, time_index, *, on_slab=None, occupied_out=None, **_kw
    ):
        cells = np.asarray(_grid.children(int(shard_key)), dtype=np.uint64)
        occupied = np.zeros(n_cells, dtype=bool)
        for t_idx in sorted(time_index.values()):
            slab, valid = _raster_slab(t_idx, n_cells)
            occupied |= valid
            on_slab(t_idx, slab)
        if occupied_out is not None:
            occupied_out.append(cells[occupied])
        return {}, {
            "shard_key": shard_key,
            "granule_count": len(granules),
            "skipped": 0,
            "timesteps": len(time_index),
            "raster_bytes_read": 0,
            "raster_px_decoded": 0,
            "raster_px_sampled": 0,
        }

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    root = str(out)
    hive.ensure_manifest(
        root,
        hive.build_manifest(grid, dataset={"short_name": "SPEC_FIXTURE_RASTER", "version": "1"}),
    )
    original = raster_mod.process_raster_shard
    raster_mod.process_raster_shard = _fake_process_raster_shard
    try:
        meta = raster_mod.process_and_write_raster_hive(
            shard, RASTER_GRANULES, grid, root, cfg, store_kwargs={}
        )
    finally:
        raster_mod.process_raster_shard = original
    assert meta.get("leaf_written"), meta

    leaf_rel = hive.shard_leaf_path("", shard).lstrip("/")
    store = zarr.storage.LocalStore(str(out / leaf_rel))
    group = zarr.open_group(store, path=grid.group_path, mode="r", zarr_format=3)
    words = np.asarray(group["time"][:], dtype=np.uint64)
    attrs = dict(group["time"].attrs)
    assert attrs == time_axis_attrs("toc"), attrs
    lo, hi = decode_time_axis(words, attrs)
    expected = {
        "shard": SHARD_KEY,
        "leaf": leaf_rel,
        "group": grid.group_path,
        "shard_order": 4,
        "chunk_order": 5,
        "cell_order": 6,
        "cells_per_chunk": grid.cells_per_chunk,
        "cells_per_shard": n_cells,
        "time_encoding": "toc",
        # The ``time`` array's attrs verbatim: the §8 declaration and
        # nothing else (no CF units/calendar under this encoding).
        "time_attrs": attrs,
        # uint64 words as decimal strings — JSON numbers cannot carry them.
        "time_words": [str(int(w)) for w in words],
        # What a conforming decode yields: ns since the Unix epoch, ``end``
        # exclusive for a range and equal to ``start`` for a timestamp.
        "time_bounds_ns": [
            [str(int(bound.astype("int64"))) for bound in pair] for pair in zip(lo, hi, strict=True)
        ],
        # The REAL acquisition envelopes the stored words must contain — the
        # §8 conservative-containment claim, pinned on committed bytes.
        # DERIVED from RASTER_GRANULES, never transcribed: a hand-typed copy
        # would keep passing after an input edit moved the words, which is
        # the one drift this block exists to catch.
        "acquisitions": _raster_acquisitions(),
        "morton": [str(int(w)) for w in group["morton"][:]],
        "bands": {name: group[name][:].tolist() for name in RASTER_BANDS},
        "content_hashes": _o11_hashes(str(out / leaf_rel)),
    }
    (out.parent / f"{out.name}.expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(f"{out.name}: leaf {leaf_rel}, {len(words)} timesteps, {n_cells} cells")


#: The ``temporal/`` fixture's per-observation clock (§8.2/§8.3, issue #410).
#: A base instant per populated cell, plus one step per observation, so every
#: multi-observation cell spans a real interval and a 1-observation cell is a
#: real instant. Well inside the toc grammar's range and far from its epoch.
TEMPORAL_BASE = "2019-05-14T02:11:07.250000000"
#: Seconds between consecutive observations of one cell. Wide enough that a
#: merged centroid's envelope is a genuine range rather than a rounding
#: artifact, narrow enough that a cell's whole span stays sub-hour.
TEMPORAL_STEP_S = 3
#: The cell whose clock is pushed forward to build §10.5's GAP (issue #489).
#: The last cell of the plan, so the fixture's cover is TWO words with a hole
#: between them rather than one bucket that swallows the whole ~30-minute
#: fixture — which is what makes the §7 parity and containment claims
#: falsifiable for an external reader.
TEMPORAL_GAP_CELL = 3
#: How far, in whole days. §10.5's guaranteed floor for a surviving gap is
#: TWO bucket spans (2 × 2^45 ns ≈ 19.5 h at the pinned cover order); five days
#: clears three, so a whole ALIGNED bucket stays uncovered wherever the base
#: instant falls on the grid. Whole days keep the fixture's clock on the same
#: fractional second, which is what lets the kernel-parity test drive these
#: instants through a float-seconds axis and still land on the committed
#: words to the nanosecond.
TEMPORAL_GAP_DAYS = 5


def _temporal_gap_offset_ns() -> int:
    """The gap cell's clock offset, checked against §10.5's two-span floor."""
    from zagg.coverage_toc import TEMPORAL_COVER_ORDER

    offset = TEMPORAL_GAP_DAYS * 86_400 * 10**9
    assert offset >= 2 * (1 << (63 - TEMPORAL_COVER_ORDER)), "gap below §10.5's survival floor"
    return offset


def _temporal_gap(words: np.ndarray) -> tuple[int, int]:
    """An aligned interval no §10.5 cover of ``words`` may claim.

    The widest hole between the input envelopes (internal-ns scale, the one
    ``toc2time`` speaks), narrowed to the bucket grid at the pinned order:
    the buckets wholly inside the hole are exactly the ones quantization
    cannot widen into, so the interval is uncovered by construction rather
    than by inspection of the object under test.
    """
    from mortie import toc2time

    from zagg.coverage_toc import TEMPORAL_COVER_ORDER

    lo, hi = (np.atleast_1d(np.asarray(x, dtype=np.uint64)) for x in toc2time(words))
    # A timestamp decodes to (t, t); a range's decoded end is exclusive.
    spans = sorted((int(a), max(int(b), int(a) + 1)) for a, b in zip(lo, hi, strict=True))
    reach, hole = spans[0][1], (0, 0)
    for start, end in spans[1:]:
        if start > reach and start - reach > hole[1] - hole[0]:
            hole = (reach, start)
        reach = max(reach, end)
    span = 1 << (63 - TEMPORAL_COVER_ORDER)
    gap = (-(-hole[0] // span) * span, (hole[1] // span) * span)
    assert gap[1] > gap[0], f"no whole aligned bucket inside {hole} — raise TEMPORAL_GAP_SPANS"
    return gap


def _obs_times_ns(n: int, cell_ordinal: int) -> np.ndarray:
    """Per-observation instants for one cell, in ns since the Unix epoch.

    Ordered with the cell's value-sorted rank (see :func:`_temporal_words`),
    and offset per cell so no two cells share a clock.
    """
    base = np.datetime64(TEMPORAL_BASE, "ns").astype("int64")
    step = int(TEMPORAL_STEP_S * 1_000_000_000)
    return base + cell_ordinal * 97 * step + np.arange(n, dtype="int64") * step


def _toc_words(lo_ns: np.ndarray, hi_ns: np.ndarray) -> np.ndarray:
    """Envelope words for ``[lo, hi]`` pairs: exact timestamp when degenerate.

    The §8 honesty rule in one place — an instant is never widened into a
    range, a real interval never narrowed into an instant.
    """
    import mortie

    lo = np.asarray(mortie.from_datetime64(lo_ns.astype("datetime64[ns]")), dtype="uint64")
    hi = np.asarray(mortie.from_datetime64(hi_ns.astype("datetime64[ns]")), dtype="uint64")
    out = np.asarray(mortie.span2toc(lo, hi), dtype="uint64")
    instant = lo == hi
    if instant.any():
        out[instant] = np.asarray(mortie.time2toc(lo[instant]), dtype="uint64")
    return out


def _centroid_runs(digest, n_obs: int):
    """``(start, stop)`` observation runs per centroid row.

    The generator assigns each observation a time monotone in its VALUE and
    §2.1 sorts payload rows ascending by mean, so centroid ``i`` covers a
    contiguous run of the value-ordered observations, delimited by the
    cumulative weights before it. That contiguity is what lets the fixture
    state each centroid's true member set — and therefore assert §8.3's
    conservative-containment claim rather than assume it.
    """
    weights = np.asarray(digest[:, 1], dtype=np.float64).round().astype("int64")
    assert int(weights.sum()) == n_obs, (int(weights.sum()), n_obs)
    bounds = np.concatenate([[0], np.cumsum(weights)])
    return list(zip(bounds[:-1], bounds[1:], strict=True))


def _temporal_config():
    """The ``temporal/`` fixture's config: both companion shapes at once.

    **This config is deliberately un-submittable, and constructed rather than
    validated.** It has no reader — the fake ``process_shard`` below stands in
    for one — so it declares no ``output.time_source``, which
    ``validate_config`` requires of every ``temporal:`` field
    (``config._validate_temporal_producer``), and its ``observed`` reducer is a
    stand-in rather than :func:`zagg.stats.toc.cell_envelope`. The generator
    computes both companions' words itself and hands them to the production
    write path, which is what lets the §7 fixture pin store BYTES independently
    of the kernel that produces them.

    That independence is the point, and it is checked from the other side:
    ``tests/test_spec_conformance.py::TestTemporalCompanions::``
    ``test_the_production_kernel_reproduces_the_committed_words`` drives these
    same inputs through the real reducers (``build_tdigest(..., temporal=)`` and
    ``cell_envelope``) and asserts every committed word. So the generator stays
    a hand-computed oracle on purpose — nothing here should grow a
    ``validate_config`` call — while the production path is pinned to it.
    """
    from zagg.config import PipelineConfig

    return PipelineConfig(
        data_source={"groups": ["g"]},
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "source": "h", "dtype": "int32", "fill_value": 0},
                "h_tdigest": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest",
                    "source": "h",
                    "location": "leaf_id",
                    # §8.3: a uint64 sibling sharing the digest's offsets.
                    "temporal": "per-centroid",
                    "inner_shape": [2],
                    "dtype": "float32",
                    "fill_value": 0,
                    "params": {"delta": DELTA},
                },
                # §8.2: the dense per-cell companion. The generator feeds its
                # words through the write path (the fake process_shard below),
                # so the declared reducer is never exercised here — the toc
                # reducer itself lands with the #410 kernel PR.
                "observed": {
                    "function": "nanmax",
                    "source": "h",
                    "dtype": "uint64",
                    "fill_value": 0,
                    "temporal": "per-cell",
                },
            },
        },
        output={
            "store_layout": "hive",
            "grid": {
                "type": "healpix",
                "parent_order": 4,
                "child_order": 6,
                "chunk_inner": 5,
                "sharded": True,
            },
        },
    )


def _fake_temporal_shard(grid, by_chunk):
    """``process_shard`` stand-in for ``temporal/``: dense + both channels."""

    def fake(g, shard_key, urls, **kwargs):
        occupied = []
        for ordinal, (block, children) in enumerate(grid.iter_chunks(int(shard_key))):
            cells = by_chunk.get(ordinal, {})
            if not cells:
                kwargs["chunk_results"].append((block, pd.DataFrame(), {}))
                continue
            n = grid.cells_per_chunk
            df = pd.DataFrame({"morton": np.asarray(children, dtype=np.uint64)})
            df["count"] = np.array(
                [cells.get(i, {}).get("count", 0) for i in range(n)], dtype=np.int32
            )
            # §8.2: an unobserved cell keeps the reserved 0 fill.
            df["observed"] = np.array(
                [cells.get(i, {}).get("observed", 0) for i in range(n)], dtype=np.uint64
            )
            ids = sorted(cells)
            # The §8.3 4-tuple: payloads, cells, location words, toc words.
            ragged = {
                "h_tdigest": (
                    [cells[i]["h_tdigest"][0] for i in ids],
                    ids,
                    [cells[i]["h_tdigest"][1] for i in ids],
                    [cells[i]["h_tdigest"][2] for i in ids],
                )
            }
            kwargs["chunk_results"].append((block, df, ragged))
            occupied.extend(int(children[i]) for i in sorted(cells))
        kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
        return pd.DataFrame(), {
            "shard_key": int(shard_key),
            "cells_with_data": len(occupied),
            "total_obs": sum(c["count"] for cells in by_chunk.values() for c in cells.values()),
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": None,
        }

    return fake


def build_temporal(out: Path) -> None:
    """The §8.2/§8.3/§9 ``temporal/`` fixture: both companion shapes, declared.

    One leaf on the same 4/5/6 geometry as ``minimal/``, carrying a located
    AND temporal digest field — payload plus its two uint64 siblings — and a
    dense per-cell toc companion, each word array stamped with the
    declaration of the words IT holds. Both toc variants are committed (the
    1-observation cell is an exact timestamp, every multi-observation cell
    and every merged centroid a conservative range), so a reader implementing
    only one variant fails a §7 fixture.

    Declaration-scoped, like the PR it lands in: the words are computed here
    from the generator's inputs and handed to the production write path. The
    aggregation kernel that will produce them is issue #410's next PR — which
    is exactly why the expectations are derived from inputs, never read back.
    """
    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid
    from zagg.grids.morton import morton_word
    from zagg.stats.tdigest import build_tdigest

    cfg = _temporal_config()
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=True)
    shard = morton_word(SHARD_KEY)
    children = grid.children(shard)
    rng = np.random.default_rng(410)

    by_chunk: dict = {}
    expected_cells = []
    # The minimal/kitchen_sink cell plan: chunk ordinal 2 stays EMPTY, and the
    # 1-observation cell is what commits the exact-timestamp variant.
    for ordinal, (chunk, local, n) in enumerate([(0, 0, 40), (0, 2, 1), (1, 1, 5), (3, 3, 300)]):
        cell_index = chunk * grid.cells_per_chunk + local
        cell_word = int(children[cell_index])
        h = np.round(rng.normal(30.0, 5.0, n), 3).astype(np.float64)
        words = np.asarray(_point_words(grid, cell_word, n, rng))
        # Times rise with VALUE, so a centroid's members are contiguous in the
        # payload's own §2.1 ordering (see _centroid_runs).
        order = np.argsort(h, kind="stable")
        h, words = h[order], words[order]
        times_ns = _obs_times_ns(n, ordinal)
        if ordinal == TEMPORAL_GAP_CELL:
            # The §10.5 gap, built into the INPUTS (issue #489): this cell's
            # clock sits whole buckets past the rest of the plan, so the
            # committed coverage.toc carries a real hole. Done here rather
            # than in `_obs_times_ns`, whose clock the other §8 expectations
            # (and the frozen digests over the other cells) ride unchanged.
            times_ns = times_ns + _temporal_gap_offset_ns()
        digest, locs = build_tdigest(h, DELTA, locations=words)
        runs = _centroid_runs(digest, n)
        per_centroid = _toc_words(
            times_ns[[lo for lo, _ in runs]], times_ns[[hi - 1 for _, hi in runs]]
        )
        per_cell = int(_toc_words(times_ns[:1], times_ns[-1:])[0])
        by_chunk.setdefault(chunk, {})[local] = {
            "count": n,
            "observed": per_cell,
            "h_tdigest": (digest, locs, per_centroid),
        }
        expected_cells.append(
            {
                "index": cell_index,
                "morton": str(cell_word),
                "count": n,
                "h_tdigest": [[float(m), float(w)] for m, w in digest],
                "h_tdigest_locations": [str(int(w)) for w in locs],
                "h_tdigest_times": [str(int(w)) for w in per_centroid],
                "observed": str(per_cell),
                # The REAL member instants the stored words must contain
                # (§8.2/§8.3's conservative-containment claim), derived from
                # the same inputs the words were built from — never
                # transcribed, so an input edit that moves the words fails
                # the conformance assertion instead of sliding past it.
                "centroid_spans_ns": [
                    [str(int(times_ns[lo])), str(int(times_ns[hi - 1]))] for lo, hi in runs
                ],
                "obs_span_ns": [str(int(times_ns[0])), str(int(times_ns[-1]))],
            }
        )
    assert EMPTY_CHUNK not in by_chunk

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    root = str(out)
    hive.ensure_manifest(
        root,
        hive.build_manifest(grid, dataset={"short_name": "SPEC_FIXTURE", "version": "1"}),
    )
    original = processing.process_shard
    processing.process_shard = _fake_temporal_shard(grid, by_chunk)
    try:
        meta = hive.process_and_write_hive(
            shard, ["s3://fixture/a.h5"], grid, {}, root, cfg, store_kwargs={}
        )
    finally:
        processing.process_shard = original
    assert meta.get("error") is None, meta

    # The §10 root coverage sidecar, through the PRODUCTION writer: the MOC
    # family's own leaf read plus its finisher, which is exactly what a sweep
    # runs. This is the only fixture that gets a root coverage.moc — every
    # other fixture store declares no temporal field, so a sweep of one would
    # produce no section at all, and writing a bare carrier there would churn
    # four committed trees for nothing (§10's absence rule, pinned as byte
    # identity by the conformance suite).
    from mortie import toc_overlaps, toc_reduce

    from zagg.coverage_toc import (
        COVER_CAP,
        COVER_KEY,
        COVER_SPEC,
        TEMPORAL_COVER_ORDER,
        cover_words,
        coverage_toc,
        coverage_toc_digest,
        quantize_words,
        read_cover,
    )
    from zagg.sweep import MocFamily

    family = MocFamily()
    contribution, _written_at = family.read_leaf(root, SHARD_KEY, None, "morton-hive/1", {})
    family.finish(root, [{"payload": contribution}], 4, {})
    envelope = hive.read_root_coverage(root)
    root_digest, root_words = coverage_toc_digest(envelope)
    # The shard envelope word is DERIVED from the generator's inputs — the
    # join over every per-centroid word it handed the writer — so a writer
    # that folds the wrong thing fails here instead of certifying itself. The
    # digest rows are the writer's committed output read back (pinned the way
    # column/'s group values are); the claims that matter over them — weight
    # conservation and per-centroid containment — are derived, from the cell
    # plan's own observation counts and the instants recorded per cell.
    shard_word = int(
        toc_reduce(
            np.concatenate(
                [cell["h_tdigest"][2] for cells in by_chunk.values() for cell in cells.values()]
            ).astype(np.uint64)
        )
    )
    assert coverage_toc(envelope) == {SHARD_KEY: shard_word}, coverage_toc(envelope)

    # The §10.5 word-set cover sibling (issue #489), through the same
    # production writer: derived from the identical inputs — the quantized
    # normalize over every per-centroid word handed to the writer — so the
    # committed object is pinned against the generator, never against itself.
    every_word = np.concatenate(
        [cell["h_tdigest"][2] for cells in by_chunk.values() for cell in cells.values()]
    ).astype(np.uint64)
    expect_cover = quantize_words(every_word)
    cover_obj = read_cover(root)
    assert cover_obj["spec"] == COVER_SPEC == envelope["temporal"][COVER_KEY]
    assert cover_obj["order"] == 4 and cover_obj["temporal_order"] == TEMPORAL_COVER_ORDER
    decoded_cover = cover_words(cover_obj)
    assert set(decoded_cover) == {SHARD_KEY}
    assert np.array_equal(decoded_cover[SHARD_KEY], expect_cover), decoded_cover
    # The §10.5 parity invariant, on the committed pair.
    assert int(toc_reduce(expect_cover)) == int(toc_reduce(quantize_words([shard_word])))
    # The gap `TEMPORAL_GAP_CELL` bought, DERIVED from the same inputs: the
    # widest hole between consecutive input envelopes, pulled IN to the
    # bucket grid, so what is left is whole aligned bucket(s) no widening law
    # may claim. A cover that swallowed the hole (or a single-word one, the
    # shape this fixture had before issue #489) fails right here, in the
    # generator, rather than certifying itself downstream.
    gap_start, gap_end = _temporal_gap(every_word)
    assert len(expect_cover) >= 2, expect_cover
    assert not bool(np.any(np.atleast_1d(toc_overlaps(expect_cover, gap_start, gap_end))))

    leaf_rel = hive.shard_leaf_path("", shard).lstrip("/")
    expected = {
        "shard": SHARD_KEY,
        "leaf": leaf_rel,
        "group": grid.group_path,
        "shard_order": 4,
        "chunk_order": 5,
        "cell_order": 6,
        "cells_per_chunk": grid.cells_per_chunk,
        "chunks_per_shard": grid.chunks_per_shard,
        "empty_chunk": EMPTY_CHUNK,
        "delta": DELTA,
        # The §10 root coverage temporal section: the tier-1 word (derived),
        # the tier-2 digest (the writer's, read back) and the weight total the
        # cell plan says it must carry.
        "root_coverage": {
            "object": "coverage.moc",
            "spec": "zagg-coverage-toc/1",
            "fields": ["h_tdigest"],
            "shards": {SHARD_KEY: str(shard_word)},
            "obs_total": sum(c["count"] for c in expected_cells),
            "digest": {
                "delta": envelope["temporal"]["digest"]["delta"],
                "centroids": [[float(m), float(w)] for m, w in root_digest],
                "times": [str(int(w)) for w in root_words],
            },
        },
        # The §10.5 sibling: the object name, its markers, and the DERIVED
        # word set (quantized from the same inputs as the tier-1 word above),
        # so the conformance suite pins the committed coverage.toc without
        # the object certifying itself.
        "cover": {
            "object": "coverage.toc",
            "spec": COVER_SPEC,
            "temporal_order": TEMPORAL_COVER_ORDER,
            "cap": COVER_CAP,
            "fields": ["h_tdigest"],
            "element": {"dtype": "uint64", "shape": [-1]},
            "encoding": "base64",
            "count": len(expect_cover),
            "words": [str(int(w)) for w in expect_cover],
            # An interval on the §8 INTERNAL scale that the committed cover
            # must not claim — derived above from the member instants, never
            # transcribed. It is what makes the parity and containment claims
            # discriminating: a cover that bridges the fixture's two clusters
            # over-claims here (§10.5's never-bridge law).
            "gap_ns": [str(int(gap_start)), str(int(gap_end))],
        },
        # The declarations the conformance tests assert against the committed
        # attrs — each on the array that HOLDS the words (§8/§9), and the
        # payload's binding, which is a sibling key of the ragged block.
        "declarations": {
            "observed": {"spec": "zagg-toc/1", "shape": "per-cell", "grammar": "mortie-toc/1"},
            "h_tdigest_times": {
                "spec": "zagg-toc/1",
                "shape": "per-centroid",
                "grammar": "mortie-toc/1",
            },
            "h_tdigest_locations": {
                "spec": "zagg-located/1",
                "shape": "per-centroid",
                "grammar": "mortie-morton/1",
            },
        },
        "times_binding": "h_tdigest_times",
        "cells": expected_cells,
        "content_hashes": _o11_hashes(str(out / leaf_rel)),
    }
    (out.parent / f"{out.name}.expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(
        f"{out.name}: leaf {leaf_rel}, {len(expected_cells)} populated cells, both toc "
        f"variants, root coverage.moc with {len(root_digest)} digest centroids"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "tests" / "data" / "spec",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="fixture names to (re)generate (default: all). The older-era "
        "fixtures are stale by design (see module docstring) — regenerate "
        "only what you mean to commit.",
    )
    args = parser.parse_args()
    builders = {
        "minimal": lambda: build(args.out / "minimal", kitchen_sink=False),
        "kitchen_sink": lambda: build(args.out / "kitchen_sink", kitchen_sink=True),
        "column": lambda: build(args.out / "column", kitchen_sink=False, pyramid={"overviews": 5}),
        "pyramid": lambda: build_pyramid(args.out / "pyramid"),
        "demoted": lambda: build_demoted(args.out / "demoted"),
        "flux": lambda: build(args.out / "flux", kitchen_sink=False, flux=True),
        "raster_toc": lambda: build_raster_toc(args.out / "raster_toc"),
        "temporal": lambda: build_temporal(args.out / "temporal"),
    }
    unknown = set(args.only or ()) - set(builders)
    if unknown:
        parser.error(f"unknown fixture name(s) {sorted(unknown)} (known: {sorted(builders)})")
    for name in args.only if args.only is not None else builders:
        builders[name]()


if __name__ == "__main__":
    main()
