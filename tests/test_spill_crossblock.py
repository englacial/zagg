"""Cross-block spill folds for located / strata / composition fields (issue #370).

Phase 1 — located ragged fields and ``build_tdigest_where`` strata survive a
block close: ``_fold_block`` builds per-block ``(digest, locations)`` partials
with the same reducers the pooled path uses (the ``leaf_id`` point words are
already spilled — they are a read-carrier column) and folds them under the
located ``merge_tdigests`` / ``merge_tdigests_kway`` overloads, so overflow
shards trade location resolution (merged centroids coarsen to common
ancestors — the located channel's defined behavior above weight 1) instead of
raising ``SpillOverflowError``.

Byte-equality tests pin ``shard_workers: 1`` (see ``test_spill.py``).
"""

import numpy as np
import pandas as pd
import pytest

from zagg.config import PipelineConfig
from zagg.grids import HealpixGrid
from zagg.processing import process_shard
from zagg.processing.spill import SpillAggregator
from zagg.processing.streaming import validate_spill_fold, validate_streaming
from zagg.stats.composition import counts_from_composition, unpack_composition
from zagg.stats.tdigest import quantile_from_tdigest
from zagg.time_axis import TOC_WORD_COLUMN

_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}

_SIGNAL = "h_ph > 0"
_NOISE = "~(h_ph > 0)"

_CONF_COLS = (
    "signal_conf_land",
    "signal_conf_ocean",
    "signal_conf_sea_ice",
    "signal_conf_land_ice",
    "signal_conf_inland_water",
)


def _composition_field(threshold=2):
    return {
        "function": "zagg.stats.composition.pack_composition",
        "source": "h_ph",
        "dtype": "uint64",
        "fill_value": 0,
        "params": {
            "conf_land": "signal_conf_land",
            "conf_ocean": "signal_conf_ocean",
            "conf_sea_ice": "signal_conf_sea_ice",
            "conf_land_ice": "signal_conf_land_ice",
            "conf_inland_water": "signal_conf_inland_water",
            "threshold": threshold,
        },
    }


def _variables(located=False, strata=False, pairwise=False, delta=16, temporal=False):
    fn = "zagg.stats.tdigest.build_tdigest" + ("_pairwise" if pairwise else "")
    base = {
        "kind": "ragged",
        "function": fn,
        "source": "h_ph",
        "inner_shape": [2],
        "params": {"delta": delta},
        "dtype": "float32",
        "fill_value": 0,
    }
    if located:
        base["location"] = "leaf_id"
    if temporal:
        base["temporal"] = "per-centroid"
    variables = {
        "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
    }
    if strata:
        for name, where in (("h_sig", _SIGNAL), ("h_noise", _NOISE)):
            f = {**base, "function": "zagg.stats.tdigest.build_tdigest_where"}
            f["params"] = {**base["params"], "where": where}
            variables[name] = f
    else:
        variables["h_tdigest"] = base
    return variables


#: The declared clock a ``temporal:`` config encodes its words from (§8.3): a
#: base-rate ``delta_time`` column on a continuous scale, epoched at the ATL03
#: mission epoch. Materialized by ``_config`` only when a field declares a
#: companion, so every pre-#410 config in this module is untouched.
_EPOCH = "2018-01-01T00:00:00"
_TIME_SOURCE = {"field": "delta_time", "epoch": _EPOCH, "scale": "gps", "units": "seconds"}


def _config(variables, streaming=None):
    agg = {"variables": variables}
    if streaming is not None:
        agg["streaming"] = streaming
    output = {"time_source": _TIME_SOURCE} if _has_temporal(variables) else {}
    return PipelineConfig(
        data_source={
            "reader": "h5coro",
            "driver": "s3",
            "groups": ["gt1l"],
            "index": {"backend": "hierarchical"},
            "shard_workers": 1,
        },
        aggregation=agg,
        output=output,
    )


def _has_temporal(variables) -> bool:
    return any(v.get("temporal") for v in variables.values())


_SPILL = {"buffer_granules": 1, "mode": "spill"}


def _grid(cfg):
    return HealpixGrid(6, 8, layout="fullsphere", config=cfg)


def _shard_key():
    from mortie import geo2mort

    return int(geo2mort(-78.5, -132.0, order=6)[0])


def _point_leafs(grid, cell, n, rng):
    """``n`` order-29 point words strictly inside child cell ``cell``."""
    from mortie import mort2geo

    lat, lon = mort2geo(np.array([cell], dtype=np.uint64))
    out = np.empty(0, dtype=np.uint64)
    scale = 0.2
    while len(out) < n:
        # mortie 1.0: array in, array out -- a length-1 input stays length 1
        lats = float(lat[0]) + rng.uniform(-scale, scale, 4 * n)
        lons = float(lon[0]) + rng.uniform(-scale, scale, 4 * n)
        leafs = np.asarray(grid.assign(lats, lons))
        out = np.concatenate([out, leafs[np.asarray(grid.cells_of(leafs)) == cell]])
        scale *= 0.5
    return out[:n]


def _granule_dfs(
    grid,
    shard_key,
    cell_idx_lists,
    obs_per_cell=60,
    seed=0,
    conf=False,
    nan_cells=(),
    times=False,
    nan_frac=0.0,
):
    """One DataFrame per granule; real order-29 point leafs per chosen cell.

    ``conf=True`` adds the five ``signal_conf_*`` columns (ATL03's ``-1..4``
    range) for composition tests. Cells in ``nan_cells`` get all-NaN heights
    (an empty digest but a nonzero count); ``nan_frac`` instead NaNs a scattered
    ~fraction of every OTHER cell's rows, leaving each cell partially finite —
    the case where a payload/companion misalignment over the reducer's
    ``[finite]`` mask is observable. ``times=True`` adds the
    ``delta_time`` clock column ``_TIME_SOURCE`` declares — one granule per
    DAY, rows a millisecond apart within it, so every granule (and therefore
    every forced block) covers a disjoint instant range: a cross-block fold
    that dropped or mispaired a block's words could not still envelope them.
    """
    rng = np.random.default_rng(seed)
    children = np.asarray(grid.children(shard_key), dtype=np.uint64)
    dfs = []
    for g, idxs in enumerate(cell_idx_lists):
        n = obs_per_cell * len(idxs)
        h, leaf = [], []
        for ci in idxs:
            vals = rng.normal(0.0, 10.0, obs_per_cell).astype(np.float32)
            if ci in nan_cells:
                vals[:] = np.nan
            elif nan_frac:
                vals[rng.random(obs_per_cell) < nan_frac] = np.nan
            h.append(vals)
            leaf.append(_point_leafs(grid, int(children[ci]), obs_per_cell, rng))
        cols = {"h_ph": np.concatenate(h), "leaf_id": np.concatenate(leaf)}
        if times:
            cols["delta_time"] = 86400.0 * (g + 1) + 1e-3 * np.arange(n, dtype=np.float64)
        if conf:
            for name in _CONF_COLS:
                cols[name] = rng.integers(-1, 5, n).astype(np.int8)
        dfs.append(pd.DataFrame(cols))
    return dfs


def _run(monkeypatch, cfg, grid, shard_key, dfs):
    reads = iter(dfs)
    monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: next(reads))
    monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
    monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
    ragged: dict = {}
    df_out, meta = process_shard(
        grid,
        shard_key,
        [f"s3://b/g{i}.h5" for i in range(len(dfs))],
        s3_credentials=_CREDS,
        config=cfg,
        ragged_out=ragged,
    )
    return df_out, ragged, meta


def _force_tiny_blocks(monkeypatch):
    monkeypatch.setattr("zagg.processing.spill._default_block_bytes", lambda k, tmp_dir=None: 1)


_CELL_LISTS = [[0, 4, 8], [2, 4, 10], [1, 8, 9], [0, 10, 15], [4, 8, 10]]


def _contributor_rows(dfs, grid, cell, mask_fn=None):
    """Every ``(h_ph value, leaf word)`` row the granules contribute to ``cell``."""
    values, words = [], []
    for df in dfs:
        in_cell = np.asarray(grid.cells_of(df["leaf_id"].values)) == cell
        keep = in_cell
        if mask_fn is not None:
            keep = in_cell & mask_fn(df)
        values.append(df["h_ph"].values[keep])
        words.append(df["leaf_id"].values[keep])
    return np.concatenate(values), np.concatenate(words)


def _contributors(dfs, grid, cell, mask_fn=None):
    """Every leaf word the granules contribute to child cell ``cell``."""
    return _contributor_rows(dfs, grid, cell, mask_fn)[1]


def _assert_ancestor_or_equal(locs, contributors):
    """Each merged location lies between the contributor hull and a member.

    Membership per centroid is not observable from the output, so pin both
    bounds: every location is (a) the ancestor-or-equal of at least one
    contributor word (it sits on a member's path) and (b) a descendant-or-equal
    of the common ancestor of ALL contributors (it cannot escape the hull).
    """
    from mortie import clip2order, common_ancestor, orders_of

    contributors = np.asarray(contributors, dtype=np.uint64)
    hull = np.uint64(common_ancestor(contributors))
    hull_order = int(orders_of(np.array([hull], dtype=np.uint64))[0])
    loc_orders = np.asarray(orders_of(locs))
    for loc, order in zip(locs, loc_orders):
        assert np.any(clip2order(int(order), contributors) == loc), (
            f"location {loc} is no contributor's ancestor"
        )
        assert np.uint64(clip2order(hull_order, loc)[0]) == hull, (
            f"location {loc} escapes the contributor hull {hull}"
        )


def _contributor_times(dfs, grid, cell, mask_fn=None):
    """Every toc word the granules contribute to ``cell``, encoded as pooled does.

    Straight through :func:`zagg.time_axis.observation_words` with the declared
    clock, which is the ONE conversion both paths use — so this is the raw
    instant set the fold's per-centroid envelopes must account for, not a
    re-derivation of them.

    Restricted to rows with a FINITE ``h_ph``: ``build_tdigest`` drops non-finite
    values along with the companion rows that go with them, so a non-finite row's
    instant is never stored, and including it here would compare the output
    against an observation no path ever kept.
    """
    from zagg.time_axis import observation_words

    out = []
    for df in dfs:
        keep = np.asarray(grid.cells_of(df["leaf_id"].values)) == cell
        keep = keep & np.isfinite(df["h_ph"].values)
        if mask_fn is not None:
            keep = keep & mask_fn(df)
        out.append(
            observation_words(
                df["delta_time"].values[keep], epoch=_EPOCH, scale="gps", units="seconds"
            )
        )
    return np.concatenate(out)


def _assert_envelope_conservation(times, contributors):
    """The folded words account for every observation, and escape none of them.

    Membership per centroid is not observable from the output (as with
    locations), so pin the two statements that are — both EXACT at any fold
    depth over the rows the reducer keeps, because the §8.2 join is a semilattice
    and a fold partitions the members:

    (a) the join over the stored per-centroid words equals the join over the raw
    observation words — no instant dropped by a block close, none invented; and
    (b) every stored word's ``[start, end)`` lies inside that whole envelope,
    which is what a reader's §8.3 containment claim rests on.

    "Every observation" means every FINITE-valued one: ``build_tdigest`` drops
    non-finite values and their companion rows, so ``contributors`` must come
    from :func:`_contributor_times`, which masks the same way. Both claims are
    upper bounds — a caller wanting a floor pins the weight-1 rows too.
    """
    import mortie

    from zagg.stats.toc import cell_envelope

    times = np.asarray(times, dtype=np.uint64)
    contributors = np.asarray(contributors, dtype=np.uint64)
    whole = int(cell_envelope(contributors))
    assert int(cell_envelope(times)) == whole, "the folded words lost or gained an instant"
    lo, hi = (int(b[0]) for b in mortie.toc2time(np.array([whole], dtype=np.uint64)))
    starts, ends = mortie.toc2time(times)
    assert int(starts.min()) >= lo and int(ends.max()) <= hi, "a folded word escapes the envelope"


def _channels_of(entry):
    """``(payloads, cell_indices, locations, times)`` from any ragged arity.

    Through the production normalizer rather than a local unpack, so the test
    reads the sink entry by the same contract the writer does.
    """
    from zagg.processing.write import _ragged_entry

    return _ragged_entry(entry)


def _pooled_build(meta, n=8):
    """Call one ragged field's declared reducer exactly as the pooled path does.

    Mirrors ``calculate_cell_statistics``: resolve the declared params over the
    cell's columns, then call ``resolve_function(meta['function'])(values,
    **params)``. Used to pin that a mis-declared ``where`` fails the same way on
    both paths.
    """
    from zagg.config import resolve_function
    from zagg.processing.aggregate import _compile_param_expr

    cell_data = {"h_ph": np.linspace(-1.0, 1.0, n, dtype=np.float32)}
    ns = {"__builtins__": {}, "np": np, "numpy": np, **cell_data}
    params = {
        k: (eval(_compile_param_expr(v), ns) if isinstance(v, str) else v)
        for k, v in (meta.get("params") or {}).items()
    }
    return resolve_function(meta["function"])(cell_data["h_ph"], **params)


_WAVEFORM_FUNCTION = "zagg.stats.waveform.build_waveform_digest"


def _waveform_variables(temporal=True, delta=64):
    """A ``rx_flux``-shaped field: the gedi01b template's reducer + channel.

    Sources ``h_ph`` and reads the noise-model columns via the params-as-column
    path, exactly as the shipped template wires ``rxwaveform``/``noise_mean``/
    ``noise_stddev`` (issue #508 phase 1 harness).
    """
    field = {
        "kind": "ragged",
        "function": _WAVEFORM_FUNCTION,
        "source": "h_ph",
        "inner_shape": [2],
        "dtype": "float32",
        "fill_value": 0,
        "params": {
            "delta": delta,
            "counts": "wf_counts",
            "noise_mean": "wf_noise_mean",
            "noise_stddev": "wf_noise_stddev",
            "gain": 1.0,
            "false_positive_rate": 1.0e-3,
            "samples_per_record": 60,
        },
    }
    if temporal:
        field["temporal"] = "per-centroid"
    return {
        "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
        "rx_flux": field,
    }


def _with_waveform_columns(dfs, seed=0):
    """Add the noise-model columns ``_waveform_variables`` declares.

    Counts sit well above the zero-mean noise floor, so every row survives the
    clip and ``sum(weights)`` over a cell is the sum of its rows' counts.
    """
    rng = np.random.default_rng(seed)
    for df in dfs:
        n = len(df)
        df["wf_counts"] = rng.uniform(5.0, 50.0, n)
        df["wf_noise_mean"] = np.zeros(n, dtype=np.float64)
        df["wf_noise_stddev"] = np.full(n, 0.1, dtype=np.float64)
    return dfs


class TestWaveformSpillBaseline:
    """How the spill path treats ``build_waveform_digest`` TODAY (issue #508).

    Phase 1 characterization, recorded before any registry changes: the SERC
    GEDI fleet runs (0.47–0.49) completed under ``{mode: spill}``, and the
    mechanism that carried them is the NON-mergeable single-block regime.
    ``validate_spill_fold`` refuses the builder (it is outside
    ``_TDIGEST_SPILL_FUNCTIONS``) — and on the SHIPPED template the refusal is
    OVER-determined, naming the per-shot ``single_shot_value``/``shot_*``
    scalar companions alongside it — so ``SpillAggregator`` records the
    verdict, replays the pooled machinery byte-identically while the shard
    fits in one block, and raises ``SpillOverflowError`` naming the field on
    the first block close. The synthetic below declares only ``count`` +
    ``rx_flux`` so the builder is isolated as the cause; the last test pins
    the deployed config, where it is not the only one. Issue #508 changes
    NONE of this: the shared digest-family registry feeds the D24 pyramid
    classification (stored-payload folds), not the build-time spill fold,
    which would need the noise-model columns re-threaded per block.
    """

    def test_probe_refuses_the_waveform_builder(self):
        # The per-centroid channel passes the temporal arm (issue #477); the
        # refusal is the ragged function arm — no cross-block fold law.
        cfg = _config(_waveform_variables(), streaming=_SPILL)
        with pytest.raises(ValueError, match="'rx_flux'.*build_waveform_digest.*fold law"):
            validate_spill_fold(cfg)

    def test_spill_accepts_the_config_as_non_mergeable(self):
        # Accepted — single-block exact — with the probe's verdict recorded so
        # a block close can name the field (issue #474 message discipline).
        cfg = _config(_waveform_variables(), streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert not agg._mergeable
        assert agg._digest_fields == {}
        assert "rx_flux" in agg._fold_problems and "fold law" in agg._fold_problems
        agg.close()

    def test_single_block_regime_is_byte_identical_to_pooled(self, monkeypatch):
        # The SERC mechanism: one block -> the pooled replay, payload AND the
        # per-centroid temporal channel byte-identical to the pooled path.
        key = _shard_key()
        pooled_cfg = _config(_waveform_variables())
        spill_cfg = _config(_waveform_variables(), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _with_waveform_columns(
            _granule_dfs(grid, key, _CELL_LISTS[:3], obs_per_cell=40, seed=6, times=True)
        )
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        df_s, ragged_s, _ = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        assert set(ragged_p) == set(ragged_s) == {"rx_flux"}
        pay_p, idx_p, locs_p, times_p = _channels_of(ragged_p["rx_flux"])
        pay_s, idx_s, locs_s, times_s = _channels_of(ragged_s["rx_flux"])
        assert idx_p == idx_s and len(pay_p) > 0
        assert locs_p is None and locs_s is None
        for a, b in zip(pay_p, pay_s, strict=True):
            np.testing.assert_array_equal(a, b)
        for a, b in zip(times_p, times_s, strict=True):
            np.testing.assert_array_equal(a, b)

    def test_block_close_raises_overflow_naming_the_field(self, monkeypatch):
        # The recorded boundary of the mechanism above: past one block there is
        # no fold law, and the overflow splices the probe's verdict.
        from zagg.processing.spill import SpillOverflowError

        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        cfg = _config(_waveform_variables(), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _with_waveform_columns(
            _granule_dfs(grid, key, _CELL_LISTS[:2], obs_per_cell=10, seed=1, times=True)
        )
        with pytest.raises(SpillOverflowError, match="'rx_flux'.*build_waveform_digest.*fold law"):
            _run(monkeypatch, cfg, grid, key, dfs)

    def test_the_shipped_template_is_refused_by_more_than_the_builder(self):
        # The synthetic above isolates the builder; the DEPLOYED config is
        # over-determined. Seven of its eight fields are per-shot scalars with
        # no cross-block fold either, so even if issue #508 admitted
        # ``build_waveform_digest`` to a shared digest-family registry the GEDI
        # store would stay non-mergeable -- ``spill_blocks_closed: 0`` remains a
        # hard precondition of the shipped template. Its own
        # ``worker.extra_disk`` comment says the same: "validate_spill_fold
        # rejects it (build_waveform_digest and the single_shot_value
        # companions have no cross-block fold law)".
        from zagg.config import default_config

        with pytest.raises(ValueError) as exc:
            validate_spill_fold(default_config("gedi01b_waveform_healpix_hive"))
        message = str(exc.value)
        assert "'rx_flux'" in message and "build_waveform_digest" in message
        for name in (
            "shot_count",
            "shot_number",
            "noise_mean",
            "noise_stddev",
            "rx_energy",
            "elevation_bin0",
            "elevation_lastbin",
        ):
            assert f"'{name}'" in message
        assert "zagg.stats.waveform.single_shot_value" in message


class TestSpillFoldProbe:
    """The spill mergeability probe (validate_spill_fold) vs merge mode."""

    def test_located_config_is_mergeable(self):
        cfg = _config(_variables(located=True), streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert agg._mergeable
        assert agg._digest_fields["h_tdigest"].location == "leaf_id"
        agg.close()

    def test_strata_config_is_mergeable(self):
        cfg = _config(_variables(strata=True), streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert agg._mergeable
        assert agg._digest_fields["h_sig"].where == _SIGNAL
        agg.close()

    def test_located_strata_config_is_mergeable(self):
        cfg = _config(_variables(located=True, strata=True), streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert agg._mergeable
        agg.close()

    def test_expression_field_still_has_no_fold(self):
        variables = _variables()
        variables["h_spread"] = {"expression": "np.nanmax(h_ph) - np.nanmin(h_ph)"}
        with pytest.raises(ValueError, match="expression fields have no cross-block fold"):
            validate_spill_fold(_config(variables))

    def test_non_tdigest_ragged_still_has_no_fold(self):
        variables = _variables()
        variables["h_raw"] = {
            "function": "np.sort",
            "source": "h_ph",
            "kind": "ragged",
            "inner_shape": [1],
        }
        with pytest.raises(ValueError, match="h_raw.*fold law"):
            validate_spill_fold(_config(variables))

    def test_chunk_precompute_still_has_no_fold(self):
        # Chunk-scoped scalars are computed over the whole shard's pooled rows,
        # which no per-block fold can reconstruct. Rejecting them is also what
        # makes _resolve_param's namespace identical to the pooled one (no
        # chunk_scalars ingredient), so the branch is load-bearing.
        cfg = _config(_variables(), streaming=_SPILL)
        cfg.aggregation["chunk_precompute"] = {"anchor": {"function": "mean", "source": "h_ph"}}
        with pytest.raises(ValueError, match="chunk_precompute.*no cross-block fold"):
            validate_spill_fold(cfg)

    @pytest.mark.parametrize("located", [False, True])
    def test_per_centroid_temporal_config_is_mergeable(self, located):
        # The refusal-removal pin (issue #477): the per-centroid companion has a
        # cross-block fold state now, alone and beside the located channel, so
        # the probe accepts it and the field classifies with both channels.
        cfg = _config(_variables(located=located, temporal=True), streaming=_SPILL)
        validate_spill_fold(cfg)  # no raise
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert agg._mergeable
        f = agg._digest_fields["h_tdigest"]
        assert f.temporal and bool(f.location) == located
        agg.close()

    def test_per_cell_temporal_still_has_no_cross_block_fold(self):
        # Only the per-centroid shape folds. A per-cell companion is a scalar
        # reducer over the derived word column with no per-block accumulator
        # here, and it must be refused BY NAME rather than reported as a plain
        # unmergeable scalar function.
        variables = _variables()
        variables["observed"] = {
            "function": "zagg.stats.toc.cell_envelope",
            "source": "toc_word",
            "dtype": "uint64",
            "fill_value": 0,
            "temporal": "per-cell",
        }
        with pytest.raises(ValueError, match="'observed'.*'per-cell'.*no cross-block fold state"):
            validate_spill_fold(_config(variables, streaming=_SPILL))

    def test_temporal_cannot_stream_under_merge_mode_either(self):
        # mode: merge carries no companion channel at all, so it still refuses —
        # and now routes to the mode that folds one.
        variables = _variables(temporal=True)
        with pytest.raises(ValueError, match="temporal companions.*cannot stream"):
            validate_streaming(_config(variables))
        with pytest.raises(ValueError, match="mode: spill"):
            validate_streaming(_config(variables))

    def test_mis_declared_inner_shape_still_has_no_fold(self):
        # The fold stores merged (k, 2) digests directly, so a mis-declared
        # inner_shape must fail here rather than disagree with the store schema
        # readers key on.
        variables = _variables()
        variables["h_tdigest"]["inner_shape"] = [3]
        with pytest.raises(ValueError, match="h_tdigest.*inner_shape.*no cross-block fold"):
            validate_spill_fold(_config(variables))

    def test_where_function_without_where_param_rejected(self):
        # A stratum field that lost its `where` (copy-paste drop) must fail at
        # the probe, not silently fold the WHOLE population into the stratum's
        # name. The pooled replay raises TypeError for the same config.
        variables = _variables(strata=True)
        variables["h_sig"]["params"] = {"delta": 16}
        with pytest.raises(ValueError, match="h_sig.*build_tdigest_where requires a 'where'"):
            validate_spill_fold(_config(variables))
        with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'where'"):
            _pooled_build(variables["h_sig"])

    def test_where_param_on_plain_tdigest_rejected(self):
        # The other direction: a `where` on a non-where reducer must not make
        # the fold silently mask — the pooled replay raises TypeError.
        variables = _variables()
        variables["h_tdigest"]["params"] = {"delta": 16, "where": _SIGNAL}
        with pytest.raises(ValueError, match="h_tdigest.*'where' is only meaningful"):
            validate_spill_fold(_config(variables))
        with pytest.raises(TypeError, match="unexpected keyword argument 'where'"):
            _pooled_build(variables["h_tdigest"])

    def test_mis_declared_where_config_is_not_mergeable(self):
        # Belt and braces: the probe's rejection is what keeps the fold from
        # ever classifying such a field, so _fold_block cannot pick a reducer
        # the config did not declare.
        variables = _variables()
        variables["h_tdigest"]["params"] = {"delta": 16, "where": _SIGNAL}
        cfg = _config(variables, streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert not agg._mergeable
        assert agg._digest_fields == {}
        agg.close()

    def test_mode_merge_validation_unchanged(self):
        # The spill probe widening must not leak into mode: merge — its
        # per-flush fold degrades locations continuously, ignores where, and
        # would re-quantize composition lanes every flush.
        with pytest.raises(ValueError, match="located ragged"):
            validate_streaming(_config(_variables(located=True)))
        with pytest.raises(ValueError, match="h_sig"):
            validate_streaming(_config(_variables(strata=True)))
        variables = _variables()
        variables["composition"] = _composition_field()
        with pytest.raises(ValueError, match="composition.*not.*mergeable"):
            validate_streaming(_config(variables))

    def test_merge_mode_messages_route_to_spill(self):
        # Phase 3 (issue #370): the merge-mode rejections keep their
        # strictness but now name the fold behavior and the mode: spill
        # remedy, so a config author lands on the working mode in one read.
        for variables in (
            _variables(located=True),
            _variables(strata=True),
        ):
            with pytest.raises(ValueError, match="mode: spill"):
                validate_streaming(_config(variables))
        variables = _variables()
        variables["composition"] = _composition_field()
        with pytest.raises(ValueError, match="mode: spill"):
            validate_streaming(_config(variables))

    def test_overflow_message_names_the_fold_surface(self, monkeypatch):
        # A genuinely non-foldable config crossing the threshold names what
        # the fold DOES cover, WHICH field put it outside that surface (the
        # probe's verdict, spliced in), and the remedies.
        from zagg.processing.spill import SpillOverflowError

        _force_tiny_blocks(monkeypatch)
        variables = _variables()
        variables["h_spread"] = {"expression": "np.nanmax(h_ph) - np.nanmin(h_ph)"}
        cfg = _config(variables, streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, _shard_key(), _CELL_LISTS[:2], obs_per_cell=10, seed=1)
        with pytest.raises(
            SpillOverflowError,
            match="cross-block fold law.*'h_spread': expression fields.*memory tier",
        ):
            _run(monkeypatch, cfg, grid, _shard_key(), dfs)


class TestLocatedMultiBlock:
    """Located fields across forced block closes: digests exact vs unlocated,
    locations bounded by the contributor hull."""

    @pytest.mark.parametrize("pairwise", [False, True])
    def test_digest_bytes_match_unlocated_and_locations_bounded(self, monkeypatch, pairwise):
        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        out = {}
        dfs_cache = None
        for located in (False, True):
            cfg = _config(_variables(located=located, pairwise=pairwise), streaming=_SPILL)
            grid = _grid(cfg)
            if dfs_cache is None:
                dfs_cache = _granule_dfs(grid, key, _CELL_LISTS, seed=11)
            out[located] = _run(monkeypatch, cfg, grid, key, list(dfs_cache))
        (df_u, ragged_u, _), (df_l, ragged_l, _) = out[False], out[True]
        pd.testing.assert_frame_equal(df_u, df_l)
        vals_u, idx_u = ragged_u["h_tdigest"]
        vals_l, idx_l, locs_l = ragged_l["h_tdigest"]
        assert idx_u == idx_l
        # The digest channel is identical with or without locations (the
        # located build/merge overloads change only the companion channel).
        for a, b in zip(vals_u, vals_l, strict=True):
            np.testing.assert_array_equal(a, b)
        # Locations row-aligned with the payloads, uint64, hull-bounded.
        grid = _grid(_config(_variables(located=True), streaming=_SPILL))
        children = np.asarray(grid.children(key), dtype=np.uint64)
        assert len(locs_l) == len(vals_l)
        for cell_i, digest, locs in zip(idx_l, vals_l, locs_l, strict=True):
            assert locs.dtype == np.uint64
            assert locs.shape == (len(digest),)
            _assert_ancestor_or_equal(locs, _contributors(dfs_cache, grid, int(children[cell_i])))

    def test_counts_and_weights_exact_vs_pooled(self, monkeypatch):
        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        pooled_cfg = _config(_variables(located=True))
        spill_cfg = _config(_variables(located=True), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=100, seed=4)
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        df_s, ragged_s, _ = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        vals_p, idx_p, _ = ragged_p["h_tdigest"]
        vals_s, idx_s, _ = ragged_s["h_tdigest"]
        assert idx_p == idx_s
        for dp, ds in zip(vals_p, vals_s, strict=True):
            # Total weight is the exact observation count either way.
            assert float(dp[:, 1].sum()) == float(ds[:, 1].sum())
            for q in (0.1, 0.5, 0.9):
                assert abs(quantile_from_tdigest(ds, q) - quantile_from_tdigest(dp, q)) < 1.0

    def test_below_knee_locations_are_exact_point_words(self, monkeypatch):
        # n <= delta across every block: the fold stays loss-free, every
        # centroid is weight 1, and its location is the exact order-29 point
        # word — the multiset of locations equals the contributor words, AND
        # each digest row's location is that row's own leaf. The multiset check
        # alone is permutation-invariant, so it would pass a shuffled location
        # channel; the per-row map below is what pins the alignment.
        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        cfg = _config(_variables(located=True, delta=512), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=20, seed=8)
        _, ragged, _ = _run(monkeypatch, cfg, grid, key, list(dfs))
        vals, idx, locs = ragged["h_tdigest"]
        children = np.asarray(grid.children(key), dtype=np.uint64)
        assert len(vals) > 0
        for cell_i, digest, cell_locs in zip(idx, vals, locs, strict=True):
            assert (digest[:, 1] == 1.0).all()
            values, contributors = _contributor_rows(dfs, grid, int(children[cell_i]))
            np.testing.assert_array_equal(np.sort(cell_locs), np.sort(contributors))
            # Weight-1 centroids: digest[i, 0] IS an observation value (float32,
            # the payload dtype), so the value identifies its row. Heights are
            # drawn from a continuous normal, so ties are measure-zero — assert
            # uniqueness rather than assume it.
            keys = values.astype(np.float32)
            assert len(np.unique(keys)) == len(keys)
            value_to_leaf = dict(zip(keys.tolist(), contributors.tolist(), strict=True))
            for centroid, loc in zip(digest, cell_locs, strict=True):
                assert value_to_leaf[float(centroid[0])] == int(loc)

    def test_multi_block_actually_engaged(self, monkeypatch):
        _force_tiny_blocks(monkeypatch)
        closes = {"n": 0}
        orig = SpillAggregator._close_block

        def counting(self):
            closes["n"] += 1
            orig(self)

        monkeypatch.setattr(SpillAggregator, "_close_block", counting)
        key = _shard_key()
        cfg = _config(_variables(located=True), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=2)
        _, ragged, meta = _run(monkeypatch, cfg, grid, key, dfs)
        assert closes["n"] >= len(_CELL_LISTS)
        assert meta["total_obs"] > 0
        assert len(ragged["h_tdigest"]) == 3  # located 3-tuple delivered


class TestTemporalMultiBlock:
    """The §8.3 per-centroid companion across forced block closes (issue #477).

    Each granule covers its own day (``_granule_dfs(times=True)``) and
    ``_force_tiny_blocks`` closes a block per flush, so every emitted centroid
    that spans more than one granule spans more than one block: a channel that
    was dropped, truncated, or paired with the wrong partition could not still
    envelope its members.
    """

    def _run_spill(self, monkeypatch, seed, located=False, pairwise=False, delta=16, obs=60):
        key = _shard_key()
        cfg = _config(
            _variables(located=located, pairwise=pairwise, delta=delta, temporal=True),
            streaming=_SPILL,
        )
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=obs, seed=seed, times=True)
        _force_tiny_blocks(monkeypatch)
        df_out, ragged, meta = _run(monkeypatch, cfg, grid, key, list(dfs))
        assert meta["phase_timings"]["spill_blocks_closed"] == len(_CELL_LISTS)
        return grid, key, dfs, ragged

    @pytest.mark.parametrize("pairwise", [False, True])
    def test_envelope_conservation_across_block_closes(self, monkeypatch, pairwise):
        grid, key, dfs, ragged = self._run_spill(monkeypatch, seed=21, pairwise=pairwise)
        import mortie

        vals, idx, locs, times = _channels_of(ragged["h_tdigest"])
        assert locs is None  # temporal-only: no location slot claimed
        assert len(times) == len(vals) > 0
        children = np.asarray(grid.children(key), dtype=np.uint64)
        merged = False
        for cell_i, digest, words in zip(idx, vals, times, strict=True):
            assert words.dtype == np.uint64
            assert words.shape == (len(digest),)
            contributors = _contributor_times(dfs, grid, int(children[cell_i]))
            _assert_envelope_conservation(words, contributors)
            # Both claims above are UPPER bounds — a fold that widened every
            # centroid to the whole-cell envelope would satisfy them. The floor
            # is the weight-1 rows: a centroid holding a single observation must
            # still carry that observation's exact instant, never a range.
            singles = words[digest[:, 1] == 1.0]
            assert len(singles) > 0, "no weight-1 centroid survived the fold"
            assert not mortie.toc_is_range(singles).any(), "a weight-1 word widened to a range"
            assert np.isin(singles, contributors).all(), "a weight-1 word is no contributor's"
            merged |= bool(mortie.toc_is_range(words).any())
        # δ=16 over 300 observations per cell: compression is real, so at least
        # one stored word must be a RANGE — otherwise conservation above would be
        # trivially satisfied by untouched per-observation timestamps.
        assert merged, "no centroid merged; the fold was not exercised"

    def test_below_knee_words_are_the_exact_observation_instants(self, monkeypatch):
        # n <= delta across every block: no compression anywhere, every centroid
        # is weight 1, and its word is that observation's exact timestamp. The
        # multiset check alone is permutation-invariant, so the per-row map is
        # what pins the channel's alignment to the payload (mirrors the located
        # below-knee test one channel over).
        grid, key, dfs, ragged = self._run_spill(monkeypatch, seed=22, delta=512, obs=20)
        vals, idx, _, times = _channels_of(ragged["h_tdigest"])
        children = np.asarray(grid.children(key), dtype=np.uint64)
        assert len(vals) > 0
        for cell_i, digest, words in zip(idx, vals, times, strict=True):
            assert (digest[:, 1] == 1.0).all()
            cell = int(children[cell_i])
            values, _ = _contributor_rows(dfs, grid, cell)
            instants = _contributor_times(dfs, grid, cell)
            np.testing.assert_array_equal(np.sort(words), np.sort(instants))
            keys = values.astype(np.float32)
            assert len(np.unique(keys)) == len(keys)
            value_to_word = dict(zip(keys.tolist(), instants.tolist(), strict=True))
            for centroid, word in zip(digest, words, strict=True):
                assert value_to_word[float(centroid[0])] == int(word)

    def test_partially_nan_cells_keep_the_channel_row_aligned(self, monkeypatch):
        # build_tdigest drops non-finite VALUES and the companion rows that go
        # with them. A whole-NaN cell cannot catch a [finite]-mask misalignment
        # between payload and companion — nothing survives to align — but a
        # PARTIALLY NaN one can: the surviving instants must be exactly the
        # finite rows', paired with the finite rows' own values.
        key = _shard_key()
        cfg = _config(_variables(temporal=True, delta=512), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(
            grid, key, _CELL_LISTS, obs_per_cell=20, seed=26, times=True, nan_frac=0.4
        )
        _force_tiny_blocks(monkeypatch)
        df_out, ragged, meta = _run(monkeypatch, cfg, grid, key, list(dfs))
        assert meta["phase_timings"]["spill_blocks_closed"] == len(_CELL_LISTS)
        children = np.asarray(grid.children(key), dtype=np.uint64)
        vals, idx, _, times = _channels_of(ragged["h_tdigest"])
        assert len(vals) > 0
        for cell_i, digest, words in zip(idx, vals, times, strict=True):
            cell = int(children[cell_i])
            values, _ = _contributor_rows(dfs, grid, cell)
            finite = np.isfinite(values)
            assert finite.any() and not finite.all(), "the cell is not partially NaN"
            # The count is over every row; the digest only over the finite ones.
            assert int(df_out["count"].values[cell_i]) == len(values)
            assert words.shape == (len(digest),)
            assert len(digest) == int(finite.sum())
            # δ=512 over ≤60 rows: nothing merges, so each centroid is one finite
            # observation and its word is that row's exact instant.
            assert (digest[:, 1] == 1.0).all()
            instants = _contributor_times(dfs, grid, cell)
            np.testing.assert_array_equal(np.sort(words), np.sort(instants))
            keys = values[finite]
            assert len(np.unique(keys)) == len(keys)
            value_to_word = dict(zip(keys.tolist(), instants.tolist(), strict=True))
            for centroid, word in zip(digest, words, strict=True):
                assert value_to_word[float(centroid[0])] == int(word)

    def test_kway_fold_is_block_order_independent(self, monkeypatch):
        # The k-way collapse is one pass over all parts, so permuting the block
        # (== granule) order permutes only the parts list: payload AND companion
        # bytes must come back identical. The channel would break this if its
        # ties were resolved by input position rather than by the word.
        key = _shard_key()
        cfg = _config(_variables(temporal=True, located=True), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=60, seed=23, times=True)
        _force_tiny_blocks(monkeypatch)
        forward = _run(monkeypatch, cfg, grid, key, list(dfs))
        reverse = _run(monkeypatch, cfg, _grid(cfg), key, list(reversed(dfs)))
        pd.testing.assert_frame_equal(forward[0], reverse[0])
        f_vals, f_idx, f_locs, f_times = _channels_of(forward[1]["h_tdigest"])
        r_vals, r_idx, r_locs, r_times = _channels_of(reverse[1]["h_tdigest"])
        assert f_idx == r_idx
        for a, b in zip(f_vals, r_vals, strict=True):
            np.testing.assert_array_equal(a, b)
        for a, b in zip(f_times, r_times, strict=True):
            np.testing.assert_array_equal(a, b)
        for a, b in zip(f_locs, r_locs, strict=True):
            np.testing.assert_array_equal(a, b)

    def test_cell_envelope_agrees_with_pooled(self, monkeypatch):
        # The cross-regime statement §8.3 licenses: the folded centroid
        # partition differs from pooled's (t-digest merge is approximate across
        # fold orders), so the WORDS differ — but the cell-level join over them
        # is the same token either way, and the digest stays within the fold
        # law's bounds (exact total weight, close quantiles).
        from zagg.stats.toc import cell_envelope

        key = _shard_key()
        pooled_cfg = _config(_variables(temporal=True))
        spill_cfg = _config(_variables(temporal=True), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=100, seed=24, times=True)
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        _force_tiny_blocks(monkeypatch)
        df_s, ragged_s, _ = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        vals_p, idx_p, _, times_p = _channels_of(ragged_p["h_tdigest"])
        vals_s, idx_s, _, times_s = _channels_of(ragged_s["h_tdigest"])
        assert idx_p == idx_s
        for dp, ds, tp, ts in zip(vals_p, vals_s, times_p, times_s, strict=True):
            assert int(cell_envelope(tp)) == int(cell_envelope(ts))
            assert float(dp[:, 1].sum()) == float(ds[:, 1].sum())
            for q in (0.1, 0.5, 0.9):
                assert abs(quantile_from_tdigest(ds, q) - quantile_from_tdigest(dp, q)) < 1.0

    def test_located_and_temporal_ride_the_same_fold(self, monkeypatch):
        # The CA shape (issue #477's blocked config): build_tdigest_where strata
        # carrying BOTH channels. Each stratum's siblings must stay row-aligned
        # with its own payload, with each channel bounded by that stratum's own
        # contributors — a channel folded against the other's partition, or
        # against the unmasked population, would fail one of the two.
        key = _shard_key()
        cfg = _config(_variables(strata=True, located=True, temporal=True), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=100, seed=25, times=True)
        _force_tiny_blocks(monkeypatch)
        _, ragged, meta = _run(monkeypatch, cfg, grid, key, list(dfs))
        assert meta["phase_timings"]["spill_blocks_closed"] == len(_CELL_LISTS)
        children = np.asarray(grid.children(key), dtype=np.uint64)
        for name, mask_fn in (
            ("h_sig", lambda df: df["h_ph"].values > 0),
            ("h_noise", lambda df: ~(df["h_ph"].values > 0)),
        ):
            vals, idx, locs, times = _channels_of(ragged[name])
            assert len(vals) == len(locs) == len(times) > 0
            for cell_i, digest, cell_locs, words in zip(idx, vals, locs, times, strict=True):
                cell = int(children[cell_i])
                assert cell_locs.shape == words.shape == (len(digest),)
                _assert_ancestor_or_equal(cell_locs, _contributors(dfs, grid, cell, mask_fn))
                _assert_envelope_conservation(words, _contributor_times(dfs, grid, cell, mask_fn))

    def test_the_derived_word_column_is_never_spilled(self, monkeypatch):
        # The channel costs the spill record nothing beyond the clock column the
        # config already reads: the toc words are DERIVED at fold time, so
        # ``toc_word`` must not appear in the block schema.
        cfg = _config(_variables(temporal=True), streaming=_SPILL)
        grid = _grid(cfg)
        df = _granule_dfs(grid, _shard_key(), [[0, 4]], obs_per_cell=5, seed=1, times=True)[0]
        agg = SpillAggregator(cfg, grid, "pandas", 1)
        agg.add_read(df)
        agg.flush()
        assert [name for name, _ in agg._block.schema] == ["h_ph", "leaf_id", "delta_time"]
        agg.close()


class TestStrataMultiBlock:
    """build_tdigest_where strata across forced block closes."""

    def _pooled_and_spill(self, monkeypatch, located=False, seed=4):
        key = _shard_key()
        pooled_cfg = _config(_variables(strata=True, located=located))
        spill_cfg = _config(_variables(strata=True, located=located), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=100, seed=seed)
        pooled = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        _force_tiny_blocks(monkeypatch)
        spilled = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        return key, grid, dfs, pooled, spilled

    def test_stratum_weights_exact_and_quantiles_close_vs_pooled(self, monkeypatch):
        _, _, _, (df_p, ragged_p, _), (df_s, ragged_s, _) = self._pooled_and_spill(monkeypatch)
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        for name in ("h_sig", "h_noise"):
            vals_p, idx_p = ragged_p[name]
            vals_s, idx_s = ragged_s[name]
            assert idx_p == idx_s
            for dp, ds in zip(vals_p, vals_s, strict=True):
                # Stratum membership is decided per row before the build, so
                # the stratum count (total weight) is exact across blocks.
                assert float(dp[:, 1].sum()) == float(ds[:, 1].sum())
                for q in (0.1, 0.5, 0.9):
                    assert abs(quantile_from_tdigest(ds, q) - quantile_from_tdigest(dp, q)) < 1.5

    def test_located_strata_locations_bounded_per_stratum(self, monkeypatch):
        key, grid, dfs, _, (_, ragged_s, _) = self._pooled_and_spill(
            monkeypatch, located=True, seed=6
        )
        children = np.asarray(grid.children(key), dtype=np.uint64)
        masks = {
            "h_sig": lambda df: df["h_ph"].values > 0,
            "h_noise": lambda df: ~(df["h_ph"].values > 0),
        }
        for name, mask_fn in masks.items():
            vals, idx, locs = ragged_s[name]
            assert len(vals) > 0
            for cell_i, digest, cell_locs in zip(idx, vals, locs, strict=True):
                assert cell_locs.shape == (len(digest),)
                contributors = _contributors(dfs, grid, int(children[cell_i]), mask_fn)
                # Only the stratum's own rows contribute to its locations.
                _assert_ancestor_or_equal(cell_locs, contributors)

    def test_strata_and_composition_config_is_mergeable(self):
        # The full stratified-product shape (issue #321 exemplar): strata +
        # composition + count, accepted by the spill fold probe (issue #370
        # option (a)). A non-fold scalar stays rejected alongside.
        variables = _variables(located=True, strata=True)
        variables["composition"] = _composition_field()
        cfg = _config(variables, streaming=_SPILL)
        validate_spill_fold(cfg)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        assert agg._mergeable
        assert "composition" in agg._composition_fields
        agg.close()
        variables["h_mean"] = {"function": "mean", "source": "h_ph"}
        with pytest.raises(ValueError, match="h_mean.*no.*cross-block fold"):
            validate_spill_fold(_config(variables))

    def test_strata_invariant_to_block_placement(self, monkeypatch):
        # The same rows split into different granules (hence different block
        # boundaries under buffer_granules=1 + tiny blocks) must land the
        # exact same per-cell stratum weights, and close quantiles.
        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        cfg_a = _config(_variables(strata=True), streaming=_SPILL)
        grid = _grid(cfg_a)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=80, seed=3)
        # Placement B: the same rows, re-partitioned into two granules.
        pooled_rows = pd.concat(dfs, ignore_index=True)
        half = len(pooled_rows) // 2
        dfs_b = [pooled_rows.iloc[:half].copy(), pooled_rows.iloc[half:].copy()]
        cfg_b = _config(_variables(strata=True), streaming=_SPILL)
        _, ragged_a, _ = _run(monkeypatch, cfg_a, grid, key, dfs)
        _, ragged_b, _ = _run(monkeypatch, cfg_b, _grid(cfg_b), key, dfs_b)
        for name in ("h_sig", "h_noise"):
            vals_a, idx_a = ragged_a[name]
            vals_b, idx_b = ragged_b[name]
            assert idx_a == idx_b
            for da, db in zip(vals_a, vals_b, strict=True):
                assert float(da[:, 1].sum()) == float(db[:, 1].sum())
                for q in (0.1, 0.5, 0.9):
                    assert abs(quantile_from_tdigest(da, q) - quantile_from_tdigest(db, q)) < 1.5


def _true_lane_counts(dfs, grid, cell, threshold=2):
    """Ground-truth composition lane counts + n_signal for one cell's rows."""
    conf_rows, finite_rows = [], []
    for df in dfs:
        in_cell = np.asarray(grid.cells_of(df["leaf_id"].values)) == cell
        conf_rows.append(df.loc[in_cell, list(_CONF_COLS)].to_numpy(np.int64))
        finite_rows.append(np.isfinite(df.loc[in_cell, "h_ph"].to_numpy(np.float64)))
    conf = np.concatenate(conf_rows)[np.concatenate(finite_rows)]
    signal = (conf >= threshold).any(axis=1)
    n = int(signal.sum())
    counts = np.zeros(8, dtype=np.int64)
    if n:
        csig = conf[signal]
        counts[:5] = (csig >= threshold).sum(axis=0)
        strongest = csig.max(axis=1)
        for i, level in enumerate((2, 3, 4)):
            counts[5 + i] = int((strongest == level).sum())
    return counts, n


class TestCompositionMultiBlock:
    """merge_composition_kway across forced block closes (issue #370 option (a))."""

    @pytest.mark.parametrize("declared", [None, "uint64"])
    def test_prealloc_dtype_and_fill_match_single_block(self, monkeypatch, declared):
        # The two spill regimes must agree on the stored dtype and on an empty
        # cell's fill. Nothing forces a composition field to declare `dtype`,
        # and the merged prealloc used to default it to uint64 while every
        # other emission path (and the single-block replay) defaults float32.
        key = _shard_key()
        field = _composition_field()
        if declared is None:
            del field["dtype"], field["fill_value"]
        else:
            field["dtype"] = declared
        variables = {
            "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
            "composition": field,
        }
        cfg = _config(dict(variables), streaming=_SPILL)
        dfs = _granule_dfs(_grid(cfg), key, _CELL_LISTS[:2], obs_per_cell=8, seed=5, conf=True)
        df_1, _, _ = _run(monkeypatch, cfg, _grid(cfg), key, list(dfs))
        multi_cfg = _config(dict(variables), streaming=_SPILL)
        _force_tiny_blocks(monkeypatch)
        df_m, _, _ = _run(monkeypatch, multi_cfg, _grid(multi_cfg), key, list(dfs))
        expected = np.dtype("float32" if declared is None else declared)
        assert df_1["composition"].dtype == expected
        assert df_m["composition"].dtype == expected
        # Empty cells (no observation at all) take the same sentinel in both
        # regimes: NaN for the float default, the declared 0 for the word.
        empty = df_1["count"].values == 0
        assert empty.any()
        w1, wm = df_1["composition"].values[empty], df_m["composition"].values[empty]
        if declared is None:
            assert np.isnan(w1).all() and np.isnan(wm).all()
        else:
            assert (w1 == 0).all() and (wm == 0).all()

    def test_non_numeric_fill_on_the_word_is_named(self, monkeypatch):
        # An integer-dtype field with a string sentinel is rejected by name
        # (_integer_fill), not by numpy's int('NaN') deep in the prealloc.
        key = _shard_key()
        variables = {
            "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
            "composition": {**_composition_field(), "fill_value": "NaN"},
        }
        cfg = _config(dict(variables), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS[:2], obs_per_cell=8, seed=6, conf=True)
        _force_tiny_blocks(monkeypatch)
        with pytest.raises(ValueError, match="cannot hold a non-numeric sentinel"):
            _run(monkeypatch, cfg, grid, key, list(dfs))

    def test_presence_exact_counts_within_fold_bound(self, monkeypatch):
        key = _shard_key()
        variables = {
            "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
            "composition": _composition_field(),
        }
        pooled_cfg = _config(dict(variables))
        spill_cfg = _config(dict(variables), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=60, seed=21, conf=True)
        df_p, _, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        _force_tiny_blocks(monkeypatch)
        df_s, _, _ = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        np.testing.assert_array_equal(df_p["count"].values, df_s["count"].values)
        children = np.asarray(grid.children(key), dtype=np.uint64)
        checked = 0
        for i, cell in enumerate(children):
            word_p, word_s = int(df_p["composition"].values[i]), int(df_s["composition"].values[i])
            truth, n = _true_lane_counts(dfs, grid, int(cell))
            if n == 0:
                assert word_p == 0 and word_s == 0
                continue
            # Presence (lane > 0) survives every fold exactly — that is the
            # spec's floor guarantee — and matches the pooled word's presence.
            np.testing.assert_array_equal(unpack_composition(word_p) > 0, truth > 0)
            np.testing.assert_array_equal(unpack_composition(word_s) > 0, truth > 0)
            # Below n=254 the pooled word recovers counts exactly; the folded
            # word stays within ONE quantization of it — the k-way collapse
            # quantizes once over every block's part, so the bound does not
            # grow with the block count (a pairwise chain's would).
            assert n <= 254
            np.testing.assert_array_equal(counts_from_composition(word_p, n), truth)
            tol = 1 + n / 255.0
            assert np.abs(counts_from_composition(word_s, n) - truth).max() <= tol
            checked += 1
        assert checked >= 3

    def test_full_stratified_product_survives_block_closes(self, monkeypatch):
        # The issue #370 acceptance shape: located strata + composition +
        # count through a forced multi-block run lands every channel.
        key = _shard_key()
        variables = _variables(located=True, strata=True)
        variables["composition"] = _composition_field()
        pooled_cfg = _config(dict(variables))
        spill_cfg = _config(dict(variables), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=80, seed=9, conf=True)
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        _force_tiny_blocks(monkeypatch)
        df_s, ragged_s, meta_s = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        assert meta_s["total_obs"] > 0
        np.testing.assert_array_equal(df_p["count"].values, df_s["count"].values)
        # Composition present and presence-consistent with pooled.
        for wp, ws in zip(df_p["composition"].values, df_s["composition"].values, strict=True):
            np.testing.assert_array_equal(
                unpack_composition(int(wp)) > 0, unpack_composition(int(ws)) > 0
            )
        # Both strata deliver the located 3-tuple with exact stratum weights.
        for name in ("h_sig", "h_noise"):
            vals_p, idx_p, _ = ragged_p[name]
            vals_s, idx_s, locs_s = ragged_s[name]
            assert idx_p == idx_s
            assert len(locs_s) == len(vals_s)
            for dp, ds, ls in zip(vals_p, vals_s, locs_s, strict=True):
                assert float(dp[:, 1].sum()) == float(ds[:, 1].sum())
                assert ls.shape == (len(ds),)


class TestFoldColumnDiagnostics:
    """A declared column the block never carried fails with the pooled path's
    named ValueError, not a bare KeyError re-wrapped as SpillReduceError."""

    @staticmethod
    def _agg_with_block(variables, cols):
        cfg = _config(variables, streaming=_SPILL)
        grid = _grid(cfg)
        agg = SpillAggregator(cfg, grid, "pandas", 1)
        assert agg._mergeable
        cells = np.asarray(grid.children(_shard_key()), dtype=np.uint64)[:1]
        agg._block.append(np.zeros(1, dtype=np.uint64), cells, cols)
        return agg

    def test_missing_location_column_named(self):
        agg = self._agg_with_block(_variables(located=True), {"h_ph": np.zeros(1, np.float32)})
        with pytest.raises(ValueError, match="h_tdigest.*location: 'leaf_id'.*spilled block"):
            agg._fold_block(agg._block)
        agg.close()

    def test_missing_source_column_named(self):
        agg = self._agg_with_block(_variables(located=True), {"leaf_id": np.zeros(1, np.uint64)})
        with pytest.raises(ValueError, match="h_tdigest.*source: 'h_ph'.*spilled block"):
            agg._fold_block(agg._block)
        agg.close()

    def test_missing_composition_source_named(self):
        variables = _variables()
        variables["composition"] = {**_composition_field(), "source": "h_absent"}
        agg = self._agg_with_block(variables, {"h_ph": np.zeros(1, np.float32)})
        with pytest.raises(ValueError, match="'composition'.*source: 'h_absent'.*spilled block"):
            agg._fold_block(agg._block)
        agg.close()

    def test_missing_composition_param_column_named(self):
        # A typo'd conf column used to fall through _resolve_param as a literal
        # string and die inside np.column_stack, naming neither the field nor
        # the column ("the array at index 4 has size 1").
        variables = _variables()
        field = _composition_field()
        field["params"] = {**field["params"], "conf_ocean": "signal_conf_ocaen"}
        variables["composition"] = field
        cols = {c: np.zeros(1, np.int64) for c in _CONF_COLS}
        cols["h_ph"] = np.zeros(1, np.float32)
        agg = self._agg_with_block(variables, cols)
        with pytest.raises(
            ValueError, match="'composition'.*conf_ocean.*signal_conf_ocaen.*spilled block"
        ):
            agg._fold_block(agg._block)
        agg.close()

    def test_missing_where_column_named(self):
        # A where expression over an unread column reached build_tdigest_where
        # and raised "where shape () does not match values shape (n,)".
        variables = _variables(strata=True)
        variables["h_sig"]["params"] = {**variables["h_sig"]["params"], "where": "flag > 0"}
        agg = self._agg_with_block(variables, {"h_ph": np.zeros(1, np.float32)})
        with pytest.raises(ValueError, match="h_sig.*where: 'flag > 0'.*\\['flag'\\]"):
            agg._fold_block(agg._block)
        agg.close()

    def test_missing_clock_column_named(self):
        # The temporal channel's words are derived from output.time_source.field,
        # which is a read column like any other: a block that never carried it
        # must fail with the POOLED path's named message, not a bare KeyError.
        agg = self._agg_with_block(_variables(temporal=True), {"h_ph": np.zeros(1, np.float32)})
        with pytest.raises(ValueError, match="output.time_source.field 'delta_time' is not"):
            agg._fold_block(agg._block)
        agg.close()

    def test_where_may_read_the_derived_word_column(self):
        # The fold's namespace carries the derived toc_word column exactly as
        # the pooled one does, so a where expression over it must resolve rather
        # than be reported as a column the block never carried.
        variables = _variables(strata=True, temporal=True)
        variables["h_sig"]["params"] = {**variables["h_sig"]["params"], "where": "toc_word > 0"}
        cols = {"h_ph": np.ones(1, np.float32), "delta_time": np.ones(1, np.float64)}
        agg = self._agg_with_block(variables, cols)
        agg._fold_block(agg._block)  # no raise
        agg.close()

    def test_location_may_not_name_the_derived_word_column(self):
        # ... but toc_word is resolvable, not spilled: it must stay out of the
        # source/location membership checks. Widening the one `available` list
        # for every check let a `location: toc_word` slip past this named raise
        # into mortie.common_ancestor, and made the message advertise a column
        # the block never carried.
        variables = _variables(located=True, temporal=True)
        variables["h_tdigest"]["location"] = TOC_WORD_COLUMN
        cols = {"h_ph": np.ones(1, np.float32), "delta_time": np.ones(1, np.float64)}
        agg = self._agg_with_block(variables, cols)
        with pytest.raises(ValueError, match="h_tdigest.*location: 'toc_word'.*spilled block") as e:
            agg._fold_block(agg._block)
        listed = str(e.value).split("available: ", 1)[1]
        assert TOC_WORD_COLUMN not in listed, f"the message advertises a column never spilled: {e}"
        agg.close()

    def test_expression_over_spilled_columns_passes(self):
        # np/numpy and the block's own columns are the whole namespace, so a
        # real expression over them must not trip the check.
        variables = _variables(strata=True)
        variables["h_sig"]["params"] = {
            **variables["h_sig"]["params"],
            "where": "np.isfinite(h_ph) & (h_ph > 0)",
        }
        agg = self._agg_with_block(variables, {"h_ph": np.ones(1, np.float32)})
        agg._fold_block(agg._block)  # no raise
        agg.close()

    def test_reducer_thread_surfaces_the_named_cause(self):
        # On the overlap path the fold runs off-thread; _join_reducer wraps the
        # failure, so the named ValueError must be the cause (not a KeyError).
        from zagg.processing.spill import SpillReduceError

        agg = self._agg_with_block(_variables(located=True), {"h_ph": np.zeros(1, np.float32)})
        agg._reduce_one(agg._block)
        with pytest.raises(SpillReduceError) as exc:
            agg._join_reducer()
        assert isinstance(exc.value.__cause__, ValueError)
        assert "location: 'leaf_id'" in str(exc.value.__cause__)
        agg.close()

    def test_empty_block_needs_no_columns(self):
        # A block that was never appended to (the final open block of a shard
        # whose last flush closed one) has no schema and nothing to check.
        cfg = _config(_variables(located=True), streaming=_SPILL)
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 1)
        agg._fold_block(agg._block)  # no raise
        agg.close()


class TestUnlocatedUnchanged:
    """Issue #370 adds nothing to the spill record — the fold reads the
    already-spilled read-carrier columns — so unlocated configs' spilled
    bytes are unchanged by construction."""

    def test_spill_record_schema_is_the_read_carrier(self):
        # Same schema whether or not the config declares location: the
        # location channel is served from the existing leaf_id column, never
        # an appended one; an unlocated spill record gains no column.
        df = None
        schemas = {}
        for located in (False, True):
            cfg = _config(_variables(located=located), streaming=_SPILL)
            grid = _grid(cfg)
            if df is None:
                df = _granule_dfs(grid, _shard_key(), [[0, 4]], obs_per_cell=5, seed=1)[0]
            agg = SpillAggregator(cfg, grid, "pandas", 1)
            agg.add_read(df.copy())
            agg.flush()
            schemas[located] = [name for name, _ in agg._block.schema]
            agg.close()
        assert schemas[False] == schemas[True] == ["h_ph", "leaf_id"]

    def test_nan_cell_multi_block_empty_digest_real_count(self, monkeypatch):
        # An all-NaN cell across blocks: exact count, no digest and no
        # locations entry emitted — matching pooled behavior.
        key = _shard_key()
        pooled_cfg = _config(_variables(located=True))
        spill_cfg = _config(_variables(located=True), streaming=_SPILL)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=30, seed=14, nan_cells={4})
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        _force_tiny_blocks(monkeypatch)
        df_s, ragged_s, _ = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        vals_p, idx_p, locs_p = ragged_p["h_tdigest"]
        vals_s, idx_s, locs_s = ragged_s["h_tdigest"]
        assert idx_p == idx_s  # the NaN cell is absent from both
        assert len(locs_s) == len(vals_s)
        children = np.asarray(grid.children(key), dtype=np.uint64)
        nan_cell = int(children[4])
        counted = df_s["count"].values[4] if len(df_s) > 4 else None
        # The NaN cell holds observations (count > 0) yet emits no payload.
        nan_rows = sum(
            int((np.asarray(grid.cells_of(df["leaf_id"].values)) == nan_cell).sum()) for df in dfs
        )
        assert nan_rows > 0
        assert 4 not in idx_s
        assert counted == nan_rows


class TestFoldRegimeVisibility:
    """The fold regime must be loud (one warning) and queryable
    (``spill_blocks_closed`` in the stats metadata) — issue #370."""

    _MSG = "leaves the exact single-block regime"

    def test_first_block_close_warns_once_and_counts_land(self, monkeypatch, caplog):
        _force_tiny_blocks(monkeypatch)
        key = _shard_key()
        cfg = _config(_variables(located=True), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=20, seed=5)
        with caplog.at_level("WARNING", logger="zagg.processing.spill"):
            _, _, meta = _run(monkeypatch, cfg, grid, key, dfs)
        warned = [r for r in caplog.records if self._MSG in r.message]
        assert len(warned) == 1  # once per shard, not once per close
        assert warned[0].levelname == "WARNING"
        # buffer_granules=1 + 1-byte threshold: every flush closes a block.
        assert meta["phase_timings"]["spill_blocks_closed"] == len(_CELL_LISTS)

    def test_single_block_run_is_silent_and_reads_zero(self, monkeypatch, caplog):
        key = _shard_key()
        cfg = _config(_variables(located=True), streaming=_SPILL)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=20, seed=5)
        with caplog.at_level("WARNING", logger="zagg.processing.spill"):
            _, _, meta = _run(monkeypatch, cfg, grid, key, dfs)
        assert not any(self._MSG in r.message for r in caplog.records)
        assert meta["phase_timings"]["spill_blocks_closed"] == 0


class TestConfigBlockBytes:
    """``aggregation.streaming.block_bytes`` (issue #474): the fold-regime
    threshold reachable from config — no monkeypatched ``_default_block_bytes``
    — with the folded results matching pooled per the issue #370 laws."""

    def test_small_block_bytes_engages_fold_and_folds_correctly(self, monkeypatch):
        key = _shard_key()
        spill = {"buffer_granules": 1, "mode": "spill", "block_bytes": 1}
        pooled_cfg = _config(_variables())
        spill_cfg = _config(_variables(), streaming=spill)
        grid = _grid(pooled_cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=40, seed=13)
        df_p, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid, key, list(dfs))
        df_s, ragged_s, meta = _run(monkeypatch, spill_cfg, _grid(spill_cfg), key, list(dfs))
        # buffer_granules=1 + a 1-byte config threshold: every flush closes.
        assert meta["phase_timings"]["spill_blocks_closed"] == len(_CELL_LISTS)
        # Counts fold exactly; digests carry the exact total weight and stay
        # within t-digest accuracy of pooled (issue #370 kway law).
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        vals_p, idx_p = ragged_p["h_tdigest"]
        vals_s, idx_s = ragged_s["h_tdigest"]
        assert idx_p == idx_s
        for dp, ds in zip(vals_p, vals_s, strict=True):
            assert float(dp[:, 1].sum()) == float(ds[:, 1].sum())
            for q in (0.1, 0.5, 0.9):
                assert abs(quantile_from_tdigest(ds, q) - quantile_from_tdigest(dp, q)) < 1.0

    def test_large_block_bytes_stays_single_block(self, monkeypatch):
        # A generous config threshold keeps the exact single-block regime.
        # 32 MiB, not something like 1 GiB: an explicit threshold is
        # headroom-checked against the REAL $TMPDIR (the aggregator gets no
        # tmp_dir), doubled for the overlap pair — so this demands exactly
        # _MIN_SPILL_BYTES, the default path's own floor, instead of turning a
        # small/full /tmp into a spurious "spill needs N bytes" failure. It is
        # still ~4 orders of magnitude above the ~6 KB this test spills.
        key = _shard_key()
        spill = {"buffer_granules": 1, "mode": "spill", "block_bytes": 1 << 25}
        cfg = _config(_variables(), streaming=spill)
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=20, seed=5)
        _, _, meta = _run(monkeypatch, cfg, grid, key, dfs)
        assert meta["phase_timings"]["spill_blocks_closed"] == 0
