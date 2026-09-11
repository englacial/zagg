"""Overview-pyramid E2E validation harness (issue #434).

Read-only acceptance gate for a swept multiresolution store: given a hive
store root (local path or ``s3://``), check the declared pyramid ladder
against the materialized overview objects and validate fold-correctness
against the data one level finer — declaration ↔ materialized nodes, exact
count conservation, t-digest k-way fold-correctness (weight-exact, CDF
within tolerance), and the packed composition word (issue #515). Value
checks SAMPLE nodes and cells so a production store is read, never swept:
the only store-wide operations are the leaf roster (root ``coverage.moc``,
or one flat list) and one small metadata GET per declared node.

Two modes share every check:

- **Fixture mode** (``full=True``): a local store the test suite built and
  swept (``tests/test_pyramid_e2e.py``) — every node, every populated cell,
  ladder-total conservation, and ``resweep=True`` for the skip-gate
  idempotency check (an immediate re-sweep is a no-op, issues #417/#421).
- **Production mode** (CLI): anonymous read-only sampling against the
  published stores::

      python -m zagg.pyramid_check \\
          s3://us-west-2.opendata.source.coop/englacial/zagg/demo/atl03_tdigest_o9.zarr \\
          --anon

  Against a declared-but-unswept store this reports the pre-sweep baseline
  (0/N declared nodes materialized) cleanly and exits nonzero — it never
  raises for that. The sweep itself is fleet-side (issue #547); production
  idempotency is asserted there, and this harness only reads.

The printed checklist mirrors issue #434's phases: declaration (the
build-side contract), materialization (the sweep's output), read-back, then
the conservation checks. Digest comparisons re-fold contributors with the
k-way merge law at the manifest's overview δ (issue #424) and re-sort
centroids by mean before any CDF interpolation — concatenation does not
preserve the sort, and an unsorted digest interpolates garbage (the
sort-before-interp trap).

Both pyramid grammars are read: ``zagg-pyramid/1`` derives each level's cell
order by constant depth (spec §4.4), ``zagg-pyramid/2`` takes the manifest's
expanded ``overviews`` list as the contract. The fixture suite exercises the
``/1`` sweep (both production stores declare ``/1`` today); ``/2`` leaf-node
members (inside the leaf zarrs) are out of scope — validation folds against
the base arrays either way.
"""

from __future__ import annotations

import json
import logging

import numpy as np

logger = logging.getLogger(__name__)

#: Probe quantiles at which stored and re-folded digest CDFs are compared.
PROBE_QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)
#: Max |CDF_stored - CDF_refold| at a probe, as a fraction of total weight.
#: δ=512 folds are ~1/512 accurate; 0.02 leaves room for channel tie-order
#: differences between the sweep's fold and this harness's channel-less one.
CDF_TOL = 0.02
#: Digest total-weight agreement: counts carried as float32 sums — exact in
#: substance, compared with float rounding headroom.
WEIGHT_RTOL = 1e-6

#: Checklist keys, in print order (mirrors issue #434's phases).
CHECKS = (
    "declaration",
    "materialization",
    "readback",
    "counts",
    "digests",
    "composition",
    "idempotency",
)


def _entry(status: str, detail: str, **extra) -> dict:
    return {"status": status, "detail": detail, **extra}


def _rank(tail: str) -> int:
    """Base-4 rank of a D1 digit tail (digits ``1..4``)."""
    rank = 0
    for ch in tail:
        rank = rank * 4 + (int(ch) - 1)
    return rank


def _tail(rank: int, depth: int) -> str:
    """Inverse of :func:`_rank`: the width-``depth`` digit tail."""
    from zagg.hive import _rank_tail

    return _rank_tail(rank, depth)


def _order(decimal: str) -> int:
    from zagg.hive import _decimal_order

    return _decimal_order(decimal)


def _base_len(decimal: str) -> int:
    from zagg.hive import _decimal_base

    return len(_decimal_base(decimal))


def _ladder(manifest: dict) -> list[tuple[int, int]]:
    """Above-shard ``(node_order, overview_cell_order)`` pairs, finest first.

    ``zagg-pyramid/1``: constant depth at the declared ``orders`` (spec §4.4,
    ``t = c - (s - k)``). ``zagg-pyramid/2``: the manifest's fully expanded
    ``overviews`` list is the reader contract (readers never re-derive the
    ladder); every above-shard entry carries exactly one member.
    """
    pyramid = manifest.get("pyramid") or {}
    if not isinstance(pyramid, dict):
        return []
    s, c = int(manifest["shard_order"]), int(manifest["cell_order"])
    if pyramid.get("overviews"):
        pairs = [
            (int(e["node"]), int(max(e["cells"])))
            for e in pyramid["overviews"]
            if int(e["node"]) < s
        ]
        return sorted(pairs, reverse=True)
    orders = (pyramid.get("overview") or {}).get("orders") or []
    ks = sorted({int(k) for k in orders if 0 <= int(k) < s}, reverse=True)
    return [(k, c - (s - k)) for k in ks]


def _composable_fields(manifest: dict) -> dict:
    """The manifest's composable field entries, by the sweep's own gate."""
    from zagg.column import _is_composable

    decl = (manifest.get("pyramid") or {}).get("overview") or {}
    return {
        n: dict(m)
        for n, m in (decl.get("fields") or {}).items()
        if isinstance(m, dict) and _is_composable(m)
    }


def _leaf_roster(store_root, manifest, store_kwargs, mode) -> tuple[list[str], str]:
    """Committed shard decimals + the source used (``coverage.moc``/``list``).

    ``auto`` prefers the root ``coverage.moc`` (one GET) and falls back to a
    flat list of the store; ``moc``/``list`` force one source. The roster is
    what the declared-node enumeration derives from, so a stale root MOC
    understates the ladder — the report records the source for exactly that
    reason.
    """
    from zagg.grids.morton import morton_decimal
    from zagg.hive import read_root_coverage, root_coverage_words

    shard_order = int(manifest["shard_order"])
    if mode in ("auto", "moc"):
        envelope = read_root_coverage(store_root, **store_kwargs)
        if envelope is not None and int(envelope.get("order", -1)) == shard_order:
            words = root_coverage_words(envelope)
            return sorted(morton_decimal(int(w)) for w in words), "coverage.moc"
        if mode == "moc":
            raise ValueError(
                f"no usable root coverage.moc at {store_root} "
                f"(need order {shard_order}); rerun with --roster list"
            )
    return _list_leaves(store_root, shard_order, store_kwargs), "list"


def _list_leaves(store_root, shard_order: int, store_kwargs) -> list[str]:
    """Flat-list fallback: leaf zarr roots at ``{base}/{digits}/{decimal}.zarr``."""
    import obstore

    from zagg.store import open_object_store

    store = open_object_store(store_root, **store_kwargs)
    leaves = set()
    for batch in obstore.list(store):
        for meta in batch:
            parts = str(meta["path"]).split("/")
            if len(parts) != shard_order + 3 or parts[-1] != "zarr.json":
                continue
            decimal = "".join(parts[:-2])
            if parts[-2] == f"{decimal}.zarr":
                leaves.add(decimal)
    return sorted(leaves)


def _declared_nodes(leaves: list[str], k: int) -> list[str]:
    return sorted({d[: _base_len(d) + k] for d in leaves})


def _probe_nodes(store_root, nodes, store_kwargs) -> tuple[dict, dict]:
    """``({node: attrs | None}, {node: error})`` — one small GET per node.

    ``None`` means the object is genuinely ABSENT (not-found), which is the
    pre-sweep baseline. Every other transport failure — expired or missing
    credentials, a wrong region, a throttled/5xx burst — is an ERROR, kept in
    the second map so it can never be reported as "declared but unmaterialized"
    (review finding): a credential mistake and an unswept store are opposite
    diagnoses and must not print the same sentence. An unparsable ``zarr.json``
    keeps its parse error alongside the empty-attrs (partial) verdict.
    """
    import concurrent.futures

    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store
    from zagg.sweep import _node_rel

    store = open_object_store(store_root, **store_kwargs)

    def probe(node):
        try:
            raw = obstore.get(store, f"{_node_rel(node)}/all.zarr/zarr.json").bytes()
        except (FileNotFoundError, NotFoundError):
            return node, None, None
        except Exception as exc:
            return node, None, f"{type(exc).__name__}: {exc}"
        try:
            return node, dict(json.loads(bytes(raw)).get("attributes") or {}), None
        except Exception as exc:
            return node, {}, f"unparsable zarr.json: {type(exc).__name__}: {exc}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(probe, nodes))
    return (
        {node: attrs for node, attrs, _ in results},
        {node: err for node, _, err in results if err is not None},
    )


def _missing_mask(values: np.ndarray, fill_value) -> np.ndarray:
    """Missing-cell mask matching the sweep's fill semantics."""
    if values.dtype.kind == "f":
        fill = np.array(fill_value, dtype=values.dtype)[()]
        if np.isnan(fill):
            return np.isnan(values)
        return np.isnan(values) | (values == fill)
    return values == np.array(fill_value, dtype=values.dtype)[()]


def _exact_expected(values: np.ndarray, method: str, fill_value):
    """Direct reduction of contributor values under an exact merge law."""
    present = values[~_missing_mask(values, fill_value)]
    if not len(present):
        return None  # all-missing group: the stored cell must be the fill
    if method == "sum":
        return present.sum(dtype=np.float64)
    if method == "min":
        return present.min()
    if method == "max":
        return present.max()
    raise ValueError(f"unknown exact merge law {method!r}")


def _payload_bytes(raw) -> bytes:
    """One ragged cell's payload normalized to bytes.

    Callers read ragged cells through 1-length SLICES (the repo test
    convention), never scalar indexing: a scalar read hands back nested 0-d
    arrays whose empty payload round-trips as ``|S1`` — one spurious NUL.
    """
    if raw is None:
        return b""
    return bytes(raw)


def _sorted_by_mean(digest: np.ndarray) -> np.ndarray:
    """Re-sort centroids by mean — REQUIRED before any CDF/quantile interp."""
    return digest[np.argsort(digest[:, 0], kind="stable")]


def _digest_mismatch(stored: np.ndarray, ref: np.ndarray) -> str | None:
    """Compare a stored digest against the independent k-way re-fold.

    Total weight must agree exactly (float rounding headroom only — digest
    weights are counts, spec §2); the value distribution within
    :data:`CDF_TOL` of the total at every probe quantile.
    """
    from zagg.stats.tdigest import cdf_from_tdigest, quantile_from_tdigest

    stored, ref = _sorted_by_mean(stored), _sorted_by_mean(ref)
    total_stored = float(stored[:, 1].sum())
    total_ref = float(ref[:, 1].sum())
    if abs(total_stored - total_ref) > max(0.5, WEIGHT_RTOL * total_ref):
        return f"weight {total_stored} != re-fold {total_ref}"
    probes = np.unique([quantile_from_tdigest(ref, q) for q in PROBE_QUANTILES])
    worst = 0.0
    for x in probes:
        diff = abs(float(cdf_from_tdigest(stored, x)) - float(cdf_from_tdigest(ref, x)))
        worst = max(worst, diff / max(total_ref, 1.0))
    if worst > CDF_TOL:
        return f"CDF deviates {worst:.4f} of total (tol {CDF_TOL})"
    return None


class _Harness:
    """One validation pass's shared context: store handles + group cache."""

    def __init__(self, store_root, manifest, store_kwargs, *, rng, sample_nodes, sample_cells):
        self.store_root = str(store_root).rstrip("/")
        self.manifest = manifest
        self.store_kwargs = dict(store_kwargs)
        self.rng = rng
        self.sample_nodes = sample_nodes
        self.sample_cells = sample_cells
        self.shard_order = int(manifest["shard_order"])
        self.cell_order = int(manifest["cell_order"])
        self.fields = _composable_fields(manifest)
        self._groups: dict = {}

    def _open(self, rel: str, inner: int):
        import zarr

        from zagg.store import open_store

        key = (rel, int(inner))
        if key not in self._groups:
            store = open_store(f"{self.store_root}/{rel}", read_only=True, **self.store_kwargs)
            self._groups[key] = zarr.open_group(store, path=str(inner), mode="r", zarr_format=3)
        return self._groups[key]

    def node_group(self, node: str, t: int):
        from zagg.sweep import _node_rel

        return self._open(f"{_node_rel(node)}/all.zarr", t)

    def leaf_group(self, decimal: str):
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path

        rel = shard_leaf_path("", morton_word(decimal)).lstrip("/")
        return self._open(rel, self.cell_order)

    def containers(self, cell_dec: str, src_order: int, roster: list[str]) -> list[str]:
        """Roster members contributing to the cell at ``cell_dec``."""
        if _order(cell_dec) >= src_order:
            container = cell_dec[: _base_len(cell_dec) + src_order]
            return [container] if container in roster else []
        return [d for d in roster if d.startswith(cell_dec)]

    def span(self, cell_dec: str, container_dec: str, src_cell_order: int) -> tuple[int, int]:
        """Contiguous index span of ``cell_dec``'s descendants in a container array."""
        if len(cell_dec) <= len(container_dec):
            return 0, 4 ** (src_cell_order - _order(container_dec))
        depth = src_cell_order - _order(cell_dec)
        return _rank(cell_dec[len(container_dec) :]) * 4**depth, 4**depth

    def contributions(self, cell_dec, source, field: str) -> list:
        """The contributor slab slices for one output cell, one field."""
        src_order, src_cell_order, roster, opener = source
        out = []
        for container in self.containers(cell_dec, src_order, roster):
            group = opener(container)
            start, n = self.span(cell_dec, container, src_cell_order)
            out.append(np.asarray(group[field][start : start + n]))
        return out


def validate_pyramid(
    store_root: str,
    *,
    store_kwargs: dict | None = None,
    sample_nodes: int = 3,
    sample_cells: int = 8,
    seed: int = 0,
    full: bool = False,
    resweep: bool = False,
    roster: str = "auto",
) -> dict:
    """Run the issue #434 checklist against a store; return the report dict.

    ``full=True`` (fixture mode) checks every node and every populated cell
    and adds ladder-total count conservation; otherwise ``sample_nodes``
    nodes per declared order and ``sample_cells`` populated cells per node
    (plus one empty cell), drawn with ``seed``. ``resweep=True`` re-runs the
    ``/1`` overview sweep on a LOCAL store and requires it to be a no-op
    (refused for ``s3://`` roots: production sweeps are fleet-side, issue
    #547). The report's ``checks`` map carries one ``status``/``detail``
    entry per :data:`CHECKS` phase; ``passed`` is True iff no check failed.
    """
    from zagg.hive import read_manifest

    store_kwargs = dict(store_kwargs or {})
    report: dict = {"store": store_root, "checks": {}}
    checks = report["checks"]

    def skip_rest(reason, *, after):
        for name in CHECKS[CHECKS.index(after) + 1 :]:
            if name == "idempotency":
                continue
            checks[name] = _entry("skip", reason)

    manifest = read_manifest(store_root, **store_kwargs)
    checks["idempotency"] = _entry(
        "skip", "production sweeps are fleet-side (issue #547); asserted in fixture mode"
    )
    if manifest is None:
        checks["declaration"] = _entry("fail", "no morton_hive.json manifest at the store root")
        skip_rest("no manifest", after="declaration")
        return _finish(report)

    # -- [1] declaration: the build-side contract.
    report["spec"] = manifest.get("spec")
    pyramid = manifest.get("pyramid") or {}
    report["pyramid_spec"] = pyramid.get("spec") if isinstance(pyramid, dict) else None
    if manifest.get("temporal") is not None:
        checks["declaration"] = _entry(
            "fail",
            "windowed store — this harness validates unwindowed stores only "
            "(both issue #547 targets are unwindowed)",
        )
        skip_rest("windowed store", after="declaration")
        return _finish(report)
    ladder = _ladder(manifest)
    fields = _composable_fields(manifest)
    classes: dict = {}
    decl_fields = ((pyramid.get("overview") or {}).get("fields") or {}) if pyramid else {}
    for name, meta in decl_fields.items():
        cls = meta.get("class") if isinstance(meta, dict) else None
        classes.setdefault(str(cls), []).append(name)
    report["ladder"] = [{"node": k, "cells": t} for k, t in ladder]
    report["field_classes"] = {c: sorted(n) for c, n in classes.items()}
    if not ladder:
        checks["declaration"] = _entry(
            "fail", "no pyramid overview declaration in the manifest (declared: False)"
        )
        skip_rest("no declaration", after="declaration")
        return _finish(report)
    if not fields:
        checks["declaration"] = _entry(
            "fail",
            f"declared ladder carries no composable fields (classes: "
            f"{report['field_classes']}) — a v1-era declaration; redeclare per issue #547",
        )
        skip_rest("no composable fields", after="declaration")
        return _finish(report)
    detail = (
        f"{report['pyramid_spec']} ladder {[k for k, _ in ladder]}; composable fields "
        f"{sorted(fields)} (classes {report['field_classes']})"
    )
    checks["declaration"] = _entry("pass", detail)

    # -- roster: what the declared-node enumeration derives from.
    leaves, roster_source = _leaf_roster(store_root, manifest, store_kwargs, roster)
    report["roster"] = {"source": roster_source, "leaves": len(leaves)}
    if not leaves:
        checks["materialization"] = _entry("fail", f"empty leaf roster (source {roster_source})")
        skip_rest("empty roster", after="materialization")
        return _finish(report)

    # -- [2] materialization: declared node roster vs stored overview objects.
    # "Materialized" means COMMITTED, in the sweep's own sense: the node's root
    # group carries the ``role: overview`` attr AND the D4 commit stamp. The
    # write order pins role/provenance attrs BEFORE the stamp, so role alone
    # accepts a torn write that :func:`zagg.sweep_overview._overview_committed`
    # refuses — and whose arrays the cascade deliberately left at fill (review
    # finding). Both stamps live in the one ``zarr.json`` already probed, so
    # this costs no extra GET. A bare/attr-less node object or a role-without-
    # stamp one is a partial write (e.g. the aborted 2026-08-25 sweep's debris,
    # issue #547 forensics): unmaterialized, reported separately.
    from zagg.hive import COMMIT_ATTR
    from zagg.sweep_overview import ROLE_ATTR

    def committed(attrs) -> bool:
        return (
            attrs is not None
            and attrs.get(ROLE_ATTR) == "overview"
            and isinstance(attrs.get(COMMIT_ATTR), dict)
        )

    declared = {k: _declared_nodes(leaves, k) for k, _ in ladder}
    probes: dict = {}
    per_order = {}
    missing: list = []
    partial: list = []
    probe_errors: list = []
    for k, _t in ladder:
        probed, errored = _probe_nodes(store_root, declared[k], store_kwargs)
        probe_errors.extend(f"{n}: {e}" for n, e in sorted(errored.items()))
        probes[k] = {n: attrs if committed(attrs) else None for n, attrs in probed.items()}
        found = [n for n, attrs in probes[k].items() if attrs is not None]
        partial.extend(
            n for n, attrs in sorted(probed.items()) if attrs is not None and not committed(attrs)
        )
        per_order[k] = {"declared": len(declared[k]), "materialized": len(found)}
        missing.extend(sorted(set(declared[k]) - set(found)))
    total_declared = sum(v["declared"] for v in per_order.values())
    total_found = sum(v["materialized"] for v in per_order.values())
    report["nodes"] = {str(k): v for k, v in per_order.items()}
    report["missing_nodes"] = missing[:50]
    report["partial_nodes"] = partial[:50]
    report["probe_errors"] = probe_errors[:50]
    summary = ", ".join(
        f"o{k} {per_order[k]['materialized']}/{per_order[k]['declared']}" for k, _ in ladder
    )
    debris = f"; {len(partial)} partial uncommitted node object(s) {partial[:8]}" if partial else ""
    # A probe that failed for any reason OTHER than not-found is not evidence
    # about the sweep at all: it fails loudly and separately, so a credential
    # or throttle problem can never print as the pre-sweep baseline.
    if probe_errors:
        checks["materialization"] = _entry(
            "fail",
            f"{len(probe_errors)} probe error(s) — node state is UNKNOWN, not a "
            f"sweep verdict: {probe_errors[:3]}",
        )
        skip_rest("node probes failed", after="materialization")
        return _finish(report)
    if total_found == 0:
        checks["materialization"] = _entry(
            "fail",
            f"declared but unmaterialized: 0/{total_declared} nodes ({summary}) — "
            f"pre-sweep baseline{debris}",
        )
        skip_rest("no materialized nodes (pre-sweep baseline)", after="materialization")
        return _finish(report)
    status = "pass" if total_found == total_declared and not partial else "fail"
    checks["materialization"] = _entry(
        status, f"{total_found}/{total_declared} declared nodes materialized ({summary}){debris}"
    )

    # -- [3..6] value checks on sampled (or, in full mode, all) nodes.
    rng = np.random.default_rng(seed)
    harness = _Harness(
        store_root,
        manifest,
        store_kwargs,
        rng=rng,
        sample_nodes=sample_nodes,
        sample_cells=sample_cells,
    )
    _value_checks(harness, ladder, declared, probes, leaves, checks, report, full=full)

    # -- [7] idempotency (fixture mode): an immediate re-sweep is a no-op.
    if resweep:
        checks["idempotency"] = _resweep_check(store_root, manifest, leaves, store_kwargs)
    return _finish(report)


def _value_checks(harness, ladder, declared, probes, leaves, checks, report, *, full):
    """Read-back + counts + digests + composition over the sampled nodes."""
    errors: dict = {"readback": [], "counts": [], "digests": [], "composition": []}
    counted = {"readback": 0, "counts": 0, "digests": 0, "composition": 0}
    count_meta = harness.fields.get("count")
    digest_fields = {
        n: m
        for n, m in harness.fields.items()
        if m.get("class") == "approximate" and list(m.get("inner_shape") or [2]) == [2]
    }
    packed_fields = {n: m for n, m in harness.fields.items() if m.get("class") == "packed"}
    exact_fields = {
        n: m for n, m in harness.fields.items() if m.get("class") == "exact" and n != "count"
    }

    for i, (k, t) in enumerate(ladder):
        nodes = [n for n in declared[k] if probes[k].get(n) is not None]
        if not full and len(nodes) > harness.sample_nodes:
            picks = harness.rng.choice(len(nodes), harness.sample_nodes, replace=False)
            nodes = sorted(nodes[int(p)] for p in picks)
        source = _source_for(harness, ladder, i, declared, probes, leaves)
        for node in nodes:
            _check_node(
                harness,
                node,
                k,
                t,
                source,
                probes[k][node],
                count_meta,
                exact_fields,
                digest_fields,
                packed_fields,
                errors,
                counted,
                full=full,
            )

    if full and count_meta is not None:
        materialized = {
            k: [n for n in declared[k] if probes[k].get(n) is not None] for k, _ in ladder
        }
        _ladder_totals(harness, ladder, materialized, leaves, count_meta, errors, counted)

    for name in ("readback", "counts", "digests", "composition"):
        errs = errors[name]
        if name == "counts" and count_meta is None:
            checks[name] = _entry("fail", "no exact 'count' field declared — nothing to conserve")
        elif name == "digests" and not digest_fields:
            checks[name] = _entry("skip", "no (k,2) approximate digest field declared")
        elif name == "composition" and not packed_fields:
            checks[name] = _entry("skip", "no packed composition field declared")
        elif errs:
            checks[name] = _entry(
                "fail", f"{len(errs)} mismatch(es); first: {errs[0]}", mismatches=errs[:20]
            )
        else:
            checks[name] = _entry("pass", f"{counted[name]} check(s), all consistent")
    report["sampled"] = counted


def _source_for(harness, ladder, i, declared, probes, leaves):
    """The fold-source tier for ladder level ``i``: one level finer, or leaves."""
    if i == 0:
        return (
            harness.shard_order,
            harness.cell_order,
            leaves,
            harness.leaf_group,
        )
    k_src, t_src = ladder[i - 1]
    materialized = [n for n in declared[k_src] if probes[k_src].get(n) is not None]
    return (k_src, t_src, materialized, lambda node: harness.node_group(node, t_src))


def _check_node(
    harness,
    node,
    k,
    t,
    source,
    attrs,
    count_meta,
    exact_fields,
    digest_fields,
    packed_fields,
    errors,
    counted,
    *,
    full,
):
    from zagg.grids.morton import morton_word
    from zagg.stats.composition import merge_composition_kway
    from zagg.stats.tdigest import merge_tdigests_kway
    from zagg.sweep_overview import (
        OVERVIEW_ATTR,
        ROLE_ATTR,
        decode_digest,
        overview_fold_delta,
        payload_weight,
    )

    # Read-back: role/provenance attrs, group opens, arrays present.
    counted["readback"] += 1
    if attrs.get(ROLE_ATTR) != "overview":
        errors["readback"].append(
            f"{node}: root attr {ROLE_ATTR!r} != 'overview' ({attrs.get(ROLE_ATTR)!r})"
        )
    if OVERVIEW_ATTR not in attrs:
        errors["readback"].append(f"{node}: missing {OVERVIEW_ATTR!r} provenance attrs")
    try:
        group = harness.node_group(node, t)
    except Exception as exc:
        errors["readback"].append(f"{node}: overview group open failed: {exc}")
        return
    arrays = set(group.array_keys())
    wanted = {"morton", *harness.fields}
    if not wanted <= arrays:
        errors["readback"].append(f"{node}: arrays missing {sorted(wanted - arrays)}")
        return

    # Populated-cell sample from the count array (count is the presence law).
    if count_meta is None:
        return
    fill = count_meta.get("fill_value", 0)
    counts = np.asarray(group["count"][:])
    populated = np.flatnonzero(~_missing_mask(counts, fill))
    cells = populated
    if not full and len(populated) > harness.sample_cells:
        cells = np.sort(harness.rng.choice(populated, harness.sample_cells, replace=False))
    empty = np.flatnonzero(_missing_mask(counts, fill))
    empty_cell = int(empty[0]) if len(empty) else None

    # Morton sanity: rank arithmetic must agree with the stored cell words.
    if len(cells):
        j = int(cells[0])
        cell_dec = node + _tail(j, t - k)
        word = int(np.asarray(group["morton"][j]))
        if word != morton_word(cell_dec):
            errors["readback"].append(
                f"{node}[{j}]: morton {word} != {morton_word(cell_dec)} for {cell_dec}"
            )
            return

    # Composition §3.3 attrs block, once per node.
    for name, meta in packed_fields.items():
        try:
            block = dict(group[name].attrs.get("composition") or {})
        except Exception as exc:
            errors["composition"].append(f"{node}/{name}: attrs unreadable: {exc}")
            continue
        if block.get("spec") != "zagg-composition/1" or block.get("of") != meta.get("of"):
            errors["composition"].append(
                f"{node}/{name}: §3.3 attrs {block} do not bind spec/of "
                f"('zagg-composition/1'/{meta.get('of')!r})"
            )
        if meta.get("threshold") is not None and block.get("threshold") != meta["threshold"]:
            errors["composition"].append(
                f"{node}/{name}: threshold {block.get('threshold')} != declared {meta['threshold']}"
            )

    for j in cells:
        cell_dec = node + _tail(int(j), t - k)
        # Counts: exact conservation.
        counted["counts"] += 1
        parts = harness.contributions(cell_dec, source, "count")
        expected = _exact_expected(
            np.concatenate(parts) if parts else np.array([], dtype=counts.dtype),
            "sum",
            fill,
        )
        stored = counts[int(j)]
        if expected is None:
            if not _missing_mask(np.asarray([stored]), fill)[0]:
                errors["counts"].append(f"{node}[{int(j)}]: stored {stored}, no contributors")
        elif int(stored) != int(round(float(expected))):
            errors["counts"].append(f"{node}[{int(j)}]: stored {stored} != fold {expected}")

        # Other exact fields, same law check.
        for name, meta in exact_fields.items():
            parts = harness.contributions(cell_dec, source, name)
            vals = (
                np.concatenate(parts)
                if parts
                else np.array([], dtype=np.asarray(group[name][int(j)]).dtype)
            )
            expected = _exact_expected(
                vals, meta.get("method", "sum"), meta.get("fill_value", "NaN")
            )
            stored = np.asarray(group[name][int(j)])[()]
            counted["counts"] += 1
            if expected is None:
                if not _missing_mask(np.asarray([stored]), meta.get("fill_value", "NaN"))[0]:
                    errors["counts"].append(f"{node}[{int(j)}]/{name}: {stored} vs no contributors")
            elif not np.isclose(float(stored), float(expected), rtol=1e-6, equal_nan=True):
                errors["counts"].append(f"{node}[{int(j)}]/{name}: {stored} != fold {expected}")

        # Digests: k-way re-fold, weight-exact + CDF within tolerance.
        for name, meta in digest_fields.items():
            dtype = meta.get("dtype", "float32")
            payloads = [
                p
                for chunk in harness.contributions(cell_dec, source, name)
                for p in chunk.tolist()
                if p is not None and len(p)
            ]
            raw = _payload_bytes(group[name][int(j) : int(j) + 1][0])
            counted["digests"] += 1
            if not payloads:
                if len(raw):
                    errors["digests"].append(
                        f"{node}[{int(j)}]/{name}: stored digest, no contributors"
                    )
                continue
            if not len(raw):
                errors["digests"].append(
                    f"{node}[{int(j)}]/{name}: empty digest, contributors exist"
                )
                continue
            arrays64 = [decode_digest(p, dtype).astype(np.float64) for p in payloads]
            ref = merge_tdigests_kway(arrays64, delta=overview_fold_delta(meta))
            stored_digest = decode_digest(raw, dtype).astype(np.float64)
            if problem := _digest_mismatch(stored_digest, ref):
                errors["digests"].append(f"{node}[{int(j)}]/{name}: {problem}")

        # Composition: exact k-way word merge, n from the `of` digest.
        for name, meta in packed_fields.items():
            of = meta["of"]
            of_meta = harness.fields.get(of) or {}
            of_dtype = of_meta.get("dtype", "float32")
            inner = tuple(of_meta.get("inner_shape") or (2,))
            words = harness.contributions(cell_dec, source, name)
            digests = harness.contributions(cell_dec, source, of)
            parts = []
            for wchunk, dchunk in zip(words, digests):
                for w, p in zip(wchunk.tolist(), dchunk.tolist()):
                    n = payload_weight(p, of_dtype, inner)
                    if n > 0:
                        parts.append((int(w), n))
            expected_word = merge_composition_kway(parts) if parts else 0
            stored_word = int(np.asarray(group[name][int(j)]))
            counted["composition"] += 1
            if stored_word != expected_word:
                errors["composition"].append(
                    f"{node}[{int(j)}]/{name}: word {stored_word} != k-way merge {expected_word}"
                )

    # One empty cell keeps its fills across every field.
    if empty_cell is not None:
        for name in packed_fields:
            if int(np.asarray(group[name][empty_cell])) != 0:
                errors["composition"].append(f"{node}[{empty_cell}]/{name}: empty cell word != 0")
        for name in digest_fields:
            if len(_payload_bytes(group[name][empty_cell : empty_cell + 1][0])):
                errors["digests"].append(f"{node}[{empty_cell}]/{name}: empty cell has a digest")


def _ladder_totals(harness, ladder, materialized, leaves, count_meta, errors, counted):
    """Full mode: whole-store count totals conserve at every ladder level.

    Walks the MATERIALIZED nodes only — a missing node already failed the
    materialization check, and its absence shows here as a short total.
    """
    fill = count_meta.get("fill_value", 0)

    def total(groups):
        out = 0
        for group in groups:
            values = np.asarray(group["count"][:])
            out += int(values[~_missing_mask(values, fill)].sum())
        return out

    base = total(harness.leaf_group(d) for d in leaves)
    for k, t in ladder:
        counted["counts"] += 1
        level = total(harness.node_group(n, t) for n in materialized[k])
        if level != base:
            errors["counts"].append(f"ladder o{k}: total {level} != base {base}")


def _resweep_check(store_root, manifest, leaves, store_kwargs) -> dict:
    """Fixture-mode skip gate (issues #417/#421): re-sweep must be a no-op."""
    from zagg.sweep_overview import sweep_overviews

    if str(store_root).startswith("s3://"):
        return _entry("fail", "resweep refused for s3:// roots — production sweeps are fleet-side")
    counts = sweep_overviews(
        store_root, manifest, {d: {None} for d in leaves}, store_kwargs=store_kwargs
    )
    if counts.get("written") or counts.get("failed"):
        return _entry("fail", f"re-sweep was not a no-op: {counts}")
    return _entry("pass", f"immediate re-sweep is a no-op ({counts.get('current', 0)} current)")


def _finish(report: dict) -> dict:
    checks = report["checks"]
    for name in CHECKS:
        checks.setdefault(name, _entry("skip", "not reached"))
    report["passed"] = all(c["status"] != "fail" for c in checks.values())
    return report


def format_report(report: dict) -> str:
    """The printed checklist, mirroring issue #434's phases."""
    lines = [f"overview-pyramid E2E validation — {report['store']}"]
    if report.get("spec"):
        ladder = ", ".join(f"o{e['node']}→cells {e['cells']}" for e in report.get("ladder", []))
        lines.append(
            f"  store {report['spec']} | pyramid {report.get('pyramid_spec')} | {ladder or 'no ladder'}"
        )
    if report.get("roster"):
        roster = report["roster"]
        lines.append(f"  leaf roster: {roster['leaves']} shards (source: {roster['source']})")
    for name in CHECKS:
        entry = report["checks"][name]
        lines.append(f"  [{entry['status'].upper():4}] {name:15} {entry['detail']}")
    lines.append(f"VERDICT: {'PASS' if report['passed'] else 'FAIL'}")
    return "\n".join(lines)


def main(argv=None) -> int:
    """CLI: ``python -m zagg.pyramid_check <store_root> [--anon] ...``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Issue #434 overview-pyramid E2E validation: declaration vs "
        "materialized nodes, count conservation, digest fold-correctness, packed "
        "composition — read-only, sampled."
    )
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument(
        "--anon", action="store_true", help="Anonymous S3 reads (public stores, no credentials)"
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--sample-nodes", type=int, default=3, help="Nodes sampled per declared order (default: 3)"
    )
    parser.add_argument(
        "--sample-cells",
        type=int,
        default=8,
        help="Populated cells sampled per node (default: 8)",
    )
    parser.add_argument("--seed", type=int, default=0, help="Sampling seed (default: 0)")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Check every node and every populated cell + ladder totals (fixture-scale stores)",
    )
    parser.add_argument(
        "--roster",
        choices=("auto", "moc", "list"),
        default="auto",
        help="Leaf roster source: root coverage.moc, a flat store list, or auto (default)",
    )
    parser.add_argument("--json", default=None, metavar="PATH", help="Also write the report JSON")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    store_kwargs: dict = {}
    if args.store_root.startswith("s3://"):
        store_kwargs["region"] = args.region
        if args.anon:
            store_kwargs["skip_signature"] = True
    report = validate_pyramid(
        args.store_root,
        store_kwargs=store_kwargs,
        sample_nodes=args.sample_nodes,
        sample_cells=args.sample_cells,
        seed=args.seed,
        full=args.full,
        roster=args.roster,
    )
    print(format_report(report))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=1)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
