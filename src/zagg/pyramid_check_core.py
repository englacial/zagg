"""Shared legs of the overview-pyramid E2E validation harness (issue #434).

The grammar-independent machinery both validation arms run on:
:mod:`zagg.pyramid_check` (the ``zagg-pyramid/1`` cascade arm and the CLI)
and :mod:`zagg.pyramid_check_v2` (the ``zagg-pyramid/2`` staged arm). What
lives here is exactly what does not depend on which grammar declared the
ladder: the leaf roster, the one-GET-per-node probes, the commit gate,
:class:`_Harness` (store handles, contributor reads, the span arithmetic),
the value laws (:func:`_check_node`: exact conservation, digest k-way
re-fold, packed composition), and the check settlement. The two spines own
what differs — the declaration grammar, the fold-source tiers, and the
artifact provenance each write path stamps.

Split from :mod:`zagg.pyramid_check` with the ``/2`` arm (module cap §4,
raised on the PR); the public surface stays on ``zagg.pyramid_check``.
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


def _field_companions(name: str, meta: dict) -> list:
    from zagg.sweep_overview import field_companions

    return field_companions(name, meta)


def _composable_fields(manifest: dict) -> dict:
    """The manifest's composable field entries, by the sweep's own gate."""
    from zagg.column import _is_composable

    decl = (manifest.get("pyramid") or {}).get("overview") or {}
    return {
        n: dict(m)
        for n, m in (decl.get("fields") or {}).items()
        if isinstance(m, dict) and _is_composable(m)
    }


def _node_object_rel(node: str) -> str:
    """An above-shard ladder artifact's relative zarr root (``all.zarr``)."""
    from zagg.sweep import _node_rel

    return f"{_node_rel(node)}/all.zarr"


def _column_object_rel(decimal: str) -> str:
    """A leaf's column artifact root (§4.6): ``{node prefix}/all.pyramid.zarr``."""
    from zagg.column import column_name
    from zagg.grids.morton import morton_word
    from zagg.hive import shard_leaf_path

    leaf_rel = shard_leaf_path("", morton_word(decimal)).lstrip("/")
    return f"{leaf_rel.rsplit('/', 1)[0]}/{column_name(None)}"


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


def _probe_nodes(store_root, nodes, store_kwargs, *, rel=_node_object_rel) -> tuple[dict, dict]:
    """``({node: attrs | None}, {node: error})`` — one small GET per node.

    ``rel`` maps a node decimal to its artifact's relative zarr root — the
    ladder's ``all.zarr`` by default, the §4.6 ``all.pyramid.zarr`` for the
    ``/2`` leaf-column tier. ``None`` means the object is genuinely ABSENT
    (not-found), which is the pre-sweep baseline. Every other transport
    failure — expired or missing credentials, a wrong region, a
    throttled/5xx burst — is an ERROR, kept in the second map so it can
    never be reported as "declared but unmaterialized" (review finding): a
    credential mistake and an unswept store are opposite diagnoses and must
    not print the same sentence. An unparsable ``zarr.json`` keeps its parse
    error alongside the empty-attrs (partial) verdict.
    """
    import concurrent.futures

    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    store = open_object_store(store_root, **store_kwargs)

    def probe(node):
        try:
            raw = obstore.get(store, f"{rel(node)}/zarr.json").bytes()
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


def _committed(attrs, role: str = "overview") -> bool:
    """The sweep's own commit gate: the role attr AND the D4 stamp.

    The write order pins role/provenance attrs BEFORE the stamp, so role
    alone accepts a torn write the writers' own current-checks refuse.
    """
    from zagg.hive import COMMIT_ATTR
    from zagg.sweep_overview import ROLE_ATTR

    return (
        attrs is not None
        and attrs.get(ROLE_ATTR) == role
        and isinstance(attrs.get(COMMIT_ATTR), dict)
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
        # ``{sibling array: owning field}`` for every declared companion
        # channel (issue #410): a located/temporal field's ``{field}_locations``
        # / ``{field}_times`` slabs are written by both fold paths, so an
        # overview that lost one is not a valid overview. Their VALUES cannot be
        # re-derived here (the words are keyed to the centroid partition the
        # merge produced); presence and row alignment can.
        self.companions = {
            sibling: name
            for name, meta in self.fields.items()
            if meta.get("class") == "approximate"
            for _kwarg, sibling in _field_companions(name, meta)
        }
        self._groups: dict = {}
        self.warnings: list = []

    def warn(self, message: str) -> None:
        """Record a check the harness DECLINED to make, for the report.

        Skipping quietly is the failure mode the negative suite exists to
        rule out: everything the pass could not validate is named here and
        printed by :func:`zagg.pyramid_check.format_report`.
        """
        if message not in self.warnings and len(self.warnings) < 200:
            self.warnings.append(message)

    def _open(self, rel: str, inner: int):
        import zarr

        from zagg.store import open_store

        key = (rel, int(inner))
        if key not in self._groups:
            store = open_store(f"{self.store_root}/{rel}", read_only=True, **self.store_kwargs)
            self._groups[key] = zarr.open_group(store, path=str(inner), mode="r", zarr_format=3)
        return self._groups[key]

    def node_group(self, node: str, t: int):
        return self._open(_node_object_rel(node), t)

    def column_group(self, decimal: str, r: int):
        """A leaf column's resolution group (§4.6) — the ``/2`` gen-1 tier."""
        return self._open(_column_object_rel(decimal), r)

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

    def contributions(self, cell_dec, source, field: str) -> tuple[list, bool]:
        """``(slab slices, complete)`` for one output cell, one field.

        Both contributor-side reads are guarded: each is a case the SWEEP
        treats as ordinary, so neither may raise out of a read-only run
        (review finding). A roster member whose object is absent or unreadable
        — the root ``coverage.moc`` is a D9 regenerable cache and can name a
        leaf that is gone — sets ``complete`` False, and the caller declines
        the cell rather than compare against a short fold. A contributor
        missing the declared FIELD is supported schema evolution: it
        contributes fill, which is exactly no contribution, so the comparison
        stays valid and ``complete`` stays True.
        """
        src_order, src_cell_order, roster, opener = source
        out, complete = [], True
        for container in self.containers(cell_dec, src_order, roster):
            try:
                group = opener(container)
            except Exception as exc:
                self.warn(f"contributor {container} unreadable ({exc}) — cells it covers skipped")
                complete = False
                continue
            try:
                arr = group[field]
            except KeyError:
                self.warn(f"contributor {container} lacks field {field!r} — contributes fill")
                continue
            start, n = self.span(cell_dec, container, src_cell_order)
            out.append(np.asarray(arr[start : start + n]))
        return out, complete

    def paired_contributions(self, cell_dec, source, word_field: str, of_field: str):
        """``(parts, poisoned, complete)`` for a packed field and its divisor.

        Mirrors the fold kernels' half-pair poison rule
        (``sweep_overview._fold_node`` and ``sweep_stage._merge_slabs``): a
        contributor carrying the ``of`` digest but NOT the word leaves every
        output cell it covers at the fill word ``0``, whatever its siblings
        contributed — so the expected word there is the fill, not a k-way
        merge (spec §3.3). The reverse skew (word without divisor) contributes
        nothing and poisons nothing. Pairing per CONTAINER also keeps the two
        arrays aligned, which zipping two independent ``contributions`` calls
        does not once either side drops a container.
        """
        src_order, src_cell_order, roster, opener = source
        parts, poisoned, complete = [], False, True
        for container in self.containers(cell_dec, src_order, roster):
            try:
                group = opener(container)
            except Exception as exc:
                self.warn(f"contributor {container} unreadable ({exc}) — cells it covers skipped")
                complete = False
                continue
            has_word, has_of = word_field in group, of_field in group
            if has_of and not has_word:
                poisoned = True
                continue
            if not has_of:
                continue
            start, n = self.span(cell_dec, container, src_cell_order)
            parts.append(
                (
                    np.asarray(group[word_field][start : start + n]),
                    np.asarray(group[of_field][start : start + n]),
                )
            )
        return parts, poisoned, complete


def _field_groups(harness, report) -> tuple:
    """Class-keyed field maps for the value checks, wide fields named.

    Returns ``(count_meta, exact_fields, digest_fields, wide_fields,
    packed_fields)``. Approximate fields whose ``inner_shape`` is not the
    ``(k, 2)`` digest this harness can re-fold are NAMED in the report and
    warnings rather than silently dropped (review finding): "N check(s), all
    consistent" must not stand for a declared composable field NOTHING
    touched.
    """
    count_meta = harness.fields.get("count")
    digest_fields = {
        n: m
        for n, m in harness.fields.items()
        if m.get("class") == "approximate" and list(m.get("inner_shape") or [2]) == [2]
    }
    wide_fields = {
        n: list(m.get("inner_shape") or [2])
        for n, m in harness.fields.items()
        if m.get("class") == "approximate" and list(m.get("inner_shape") or [2]) != [2]
    }
    if wide_fields:
        report["unchecked_fields"] = wide_fields
        for n, shape in sorted(wide_fields.items()):
            harness.warn(f"field {n!r}: inner_shape {shape} != [2] — digest values NOT validated")
    packed_fields = {n: m for n, m in harness.fields.items() if m.get("class") == "packed"}
    exact_fields = {
        n: m for n, m in harness.fields.items() if m.get("class") == "exact" and n != "count"
    }
    return count_meta, exact_fields, digest_fields, wide_fields, packed_fields


def _settle_value_checks(
    checks,
    report,
    harness,
    errors,
    counted,
    *,
    count_meta,
    digest_fields,
    wide_fields,
    packed_fields,
) -> None:
    """One status/detail entry per value check, from the accumulated errors.

    Zero comparisons is not a pass: a declared, applicable check that never
    ran validated NOTHING, and an acceptance gate that reports that as a
    pass is a rubber stamp (review finding).
    """
    for name in ("readback", "counts", "digests", "composition"):
        errs = errors[name]
        if name == "counts" and count_meta is None:
            checks[name] = _entry("fail", "no exact 'count' field declared — nothing to conserve")
        elif name == "digests" and not digest_fields:
            checks[name] = _entry(
                "skip" if not wide_fields else "fail",
                "no (k,2) approximate digest field declared"
                if not wide_fields
                else f"every declared approximate field has inner_shape != [2] "
                f"({wide_fields}) — NOT validated by this harness",
            )
        elif name == "composition" and not packed_fields:
            checks[name] = _entry("skip", "no packed composition field declared")
        elif errs:
            checks[name] = _entry(
                "fail", f"{len(errs)} mismatch(es); first: {errs[0]}", mismatches=errs[:20]
            )
        elif not counted[name]:
            checks[name] = _entry(
                "fail",
                f"0 {name} comparison(s) performed — NOTHING was validated "
                f"(declared fields present, but no sampled node/cell reached the check)",
            )
        else:
            checks[name] = _entry("pass", f"{counted[name]} check(s), all consistent")
    if wide_fields and digest_fields:
        checks["digests"]["detail"] += (
            f"; {len(wide_fields)} declared approximate field(s) NOT validated "
            f"(inner_shape != [2]): {sorted(wide_fields)}"
        )
    report["sampled"] = counted
    if harness.warnings:
        report["warnings"] = list(harness.warnings)


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
    compose_exact=True,
    gather=False,
    role="overview",
    provenance_attr=None,
    group_getter=None,
):
    """The value laws over one artifact's resolution group, both grammars.

    ``source`` is the fold-source tier the SPINE resolved (``/1``: the node's
    own recorded provenance; ``/2``: the leaf-column tier the staged sweep
    reads). ``gather`` marks a ``/2`` stage-gather level, whose content is
    gen-1 bytes ASSIGNED, never re-merged: the packed word there must equal
    the single contributor's word byte-for-byte — re-quantizing it through
    the k-way merge can move a lane by one quantum and false-fail a level
    the sweep wrote correctly (§3.4 drift is per MERGE, and a gather is not
    one). ``role``/``provenance_attr``/``group_getter`` retarget the
    read-back leg at the artifact kind (ladder overview, or a §4.6 column
    group under ``role: column``).
    """
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

    provenance_attr = provenance_attr or OVERVIEW_ATTR
    open_group = group_getter or harness.node_group

    # Read-back: role/provenance attrs, group opens, arrays present.
    counted["readback"] += 1
    if attrs.get(ROLE_ATTR) != role:
        errors["readback"].append(
            f"{node}: root attr {ROLE_ATTR!r} != {role!r} ({attrs.get(ROLE_ATTR)!r})"
        )
    if provenance_attr not in attrs:
        errors["readback"].append(f"{node}: missing {provenance_attr!r} provenance attrs")
    try:
        group = open_group(node, t)
    except Exception as exc:
        errors["readback"].append(f"{node}: group open failed at resolution {t}: {exc}")
        return
    arrays = set(group.array_keys())
    wanted = {"morton", *harness.fields, *harness.companions}
    if not wanted <= arrays:
        errors["readback"].append(f"{node}[{t}]: arrays missing {sorted(wanted - arrays)}")
        return
    # A companion sibling must be row-aligned with the payload it rides (§9.1):
    # a channel that silently vanished or shortened cannot be caught by value.
    for sibling, owner in harness.companions.items():
        if group[sibling].shape != group[owner].shape:
            errors["readback"].append(
                f"{node}: companion {sibling} shape {group[sibling].shape} != "
                f"{owner} {group[owner].shape}"
            )

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
    # The fill side of the presence law, sampled the same way: an output cell
    # at fill must have NO contributors carrying data. Without it a node whose
    # slabs are entirely blank runs zero per-cell comparisons and reports
    # "pass" on blank payloads — the rubber stamp (review finding).
    empty_probe = empty
    if len(empty) > harness.sample_cells:
        empty_probe = np.sort(harness.rng.choice(empty, harness.sample_cells, replace=False))

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

    # Composition §3.3 attrs block, once per artifact group.
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
        parts, complete = harness.contributions(cell_dec, source, "count")
        if not complete:
            continue  # an unreadable contributor: this cell is not validated
        counted["counts"] += 1
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
            parts, complete = harness.contributions(cell_dec, source, name)
            if not complete:
                continue
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
            chunks, complete = harness.contributions(cell_dec, source, name)
            if not complete:
                continue
            payloads = [p for chunk in chunks for p in chunk.tolist() if p is not None and len(p)]
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

        # Composition: exact k-way word merge, n from the `of` digest. Run
        # only when the fold tier is PINNED (the /1 node's own provenance, or
        # the /2 grammar's derived regime) — an exact compare against a
        # guessed tier is a false fail, not a check.
        for name, meta in (packed_fields if compose_exact else {}).items():
            of = meta["of"]
            of_meta = harness.fields.get(of) or {}
            of_dtype = of_meta.get("dtype", "float32")
            inner = tuple(of_meta.get("inner_shape") or (2,))
            paired, poisoned, complete = harness.paired_contributions(cell_dec, source, name, of)
            if not complete:
                continue
            parts = []
            for wchunk, dchunk in paired:
                for w, p in zip(wchunk.tolist(), dchunk.tolist()):
                    n = payload_weight(p, of_dtype, inner)
                    if n > 0:
                        parts.append((int(w), n))
            # A half-paired contributor poisons the whole cell to the fill
            # word, siblings included (spec §3.3) — that is the fold kernels'
            # rule, so it is the expectation here too. A GATHER cell's word is
            # the single contributor's, assigned — see the docstring.
            if poisoned or not parts:
                expected_word = 0
            elif gather and len(parts) == 1:
                expected_word = parts[0][0]
            else:
                expected_word = merge_composition_kway(parts)
            stored_word = int(np.asarray(group[name][int(j)]))
            counted["composition"] += 1
            if stored_word != expected_word:
                errors["composition"].append(
                    f"{node}[{int(j)}]/{name}: word {stored_word} != "
                    f"{'gathered gen-1 word' if gather else 'k-way merge'} {expected_word}"
                )

    # One empty cell keeps its fills across every field.
    if empty_cell is not None:
        for name in packed_fields:
            if int(np.asarray(group[name][empty_cell])) != 0:
                errors["composition"].append(f"{node}[{empty_cell}]/{name}: empty cell word != 0")
        for name in digest_fields:
            if len(_payload_bytes(group[name][empty_cell : empty_cell + 1][0])):
                errors["digests"].append(f"{node}[{empty_cell}]/{name}: empty cell has a digest")

    # ... and the fill side of the presence law: no contributor may carry data.
    for j in empty_probe:
        cell_dec = node + _tail(int(j), t - k)
        parts, complete = harness.contributions(cell_dec, source, "count")
        if not complete:
            continue
        got = _exact_expected(
            np.concatenate(parts) if parts else np.array([], dtype=counts.dtype), "sum", fill
        )
        counted["counts"] += 1
        if got is not None and float(got) != 0.0:
            errors["counts"].append(
                f"{node}[{int(j)}]: cell is fill, but contributors total {got} — "
                f"the fold dropped data (blank/short node)"
            )


def _ladder_materialization(store_root, ladder, leaves, store_kwargs, checks, report) -> tuple:
    """The declared-roster ↔ committed-artifacts check, shared by both arms.

    Probes one ``zarr.json`` per declared above-shard node and settles the
    ``materialization`` check. Returns ``(probes, declared, state)`` with
    ``state`` one of ``"ok"`` (value checks may proceed), ``"baseline"``
    (declared but 0 materialized — the pre-sweep report), or ``"errors"``
    (probe transport failures: node state is UNKNOWN, never a sweep verdict).
    """
    declared = {k: _declared_nodes(leaves, k) for k, _ in ladder}
    probes: dict = {}
    per_order = {}
    missing: list = []
    partial: list = []
    probe_errors: list = []
    for k, _t in ladder:
        probed, errored = _probe_nodes(store_root, declared[k], store_kwargs)
        probe_errors.extend(f"{n}: {e}" for n, e in sorted(errored.items()))
        probes[k] = {n: attrs if _committed(attrs) else None for n, attrs in probed.items()}
        found = [n for n, attrs in probes[k].items() if attrs is not None]
        partial.extend(
            n for n, attrs in sorted(probed.items()) if attrs is not None and not _committed(attrs)
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
        return probes, declared, "errors"
    if total_found == 0:
        checks["materialization"] = _entry(
            "fail",
            f"declared but unmaterialized: 0/{total_declared} nodes ({summary}) — "
            f"pre-sweep baseline{debris}",
        )
        return probes, declared, "baseline"
    status = "pass" if total_found == total_declared and not partial else "fail"
    checks["materialization"] = _entry(
        status, f"{total_found}/{total_declared} declared nodes materialized ({summary}){debris}"
    )
    return probes, declared, "ok"


def _finish(report: dict, names) -> dict:
    checks = report["checks"]
    for name in names:
        checks.setdefault(name, _entry("skip", "not reached"))
    report["passed"] = all(c["status"] != "fail" for c in checks.values())
    return report
