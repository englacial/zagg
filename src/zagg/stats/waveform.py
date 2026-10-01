"""Sensor-generic waveform flux transform (issue #425).

The write transform for vlen waveform sensors (GEDI, LVIS, any future
full-waveform product — the sensor lives in the config, never here):
parameterized ONLY by a noise model (per-record mean/stddev columns), a named
gain constant, and a declared operating point (ruling on issue #422).

Per sample: ``weight = (count − noise_mean) × gain`` — detected
photoelectrons under the linear gain model — **thresholded to zero** below
``noise_mean + n_σ·σ``, with ``n_σ`` derived from the declared
false-positive target (:func:`threshold_sigma`): the probability that any
pure-noise sample in a record survives the clip is bounded by the target,
accounting for the record's sample count and the noise correlation length.
Zero-weight rows are dropped, which is what collapses per-shot row count from
the full window (~1,420 samples on GEDI) to the signal extent.

:func:`build_waveform_digest` then sorts the surviving ``(value, weight)``
sub-centroids and runs the SHARED compress path of :mod:`zagg.stats.tdigest`
(:func:`~zagg.stats.tdigest._compress`), so the stored payload is a §2
weights-sorted centroid array and pooled multi-shot merge is the existing
:func:`~zagg.stats.tdigest.merge_tdigests_kway` fold — no new merge law.
While the per-cell row count stays under the k1 budget the payload is
loss-free (one row per surviving sample), so the clipped waveform is exactly
reconstructable from the stored rows plus the per-record companions
(elevation endpoints, noise moments, gain — see the gedi01b template).

Companions (plain names, ruling on issue #422): :func:`shot_count` /
:func:`shot_number` implement the pooled-cell record identity — the count of
distinct contributing records, and the record id when unique (sentinel 0
otherwise); :func:`single_shot_value` passes a record-constant column through
when one record owns the cell (NaN otherwise).
"""

from __future__ import annotations

import numpy as np

from zagg.stats.tdigest import _centroid_envelopes, _check_words, _compress

__all__ = [
    "build_waveform_digest",
    "shot_count",
    "shot_number",
    "single_shot_value",
    "threshold_sigma",
    "waveform_weights",
]

_DEFAULT_DELTA = 4096  # the uniform packaged δ (espg ruling 2026-09-13, issue #547)


def _probit(p: np.ndarray | float) -> np.ndarray | float:
    """Standard normal quantile Φ⁻¹(p), pure numpy (Acklam's approximation).

    Peter Acklam's rational approximation (relative error < 1.15e-9 over
    (0, 1)) — scipy-free, matching the module's pure-numpy discipline. Scalar
    in, scalar out; array in, array out.
    """
    p_arr = np.asarray(p, dtype=np.float64)
    scalar = p_arr.ndim == 0
    p_arr = np.atleast_1d(p_arr)
    if np.any((p_arr <= 0.0) | (p_arr >= 1.0)):
        raise ValueError("probit requires 0 < p < 1")

    a = (
        -39.6968302866538,
        220.946098424521,
        -275.928510446969,
        138.357751867269,
        -30.6647980661472,
        2.50662827745924,
    )
    b = (
        -54.4760987982241,
        161.585836858041,
        -155.698979859887,
        66.8013118877197,
        -13.2806815528857,
    )
    c = (
        -0.00778489400243029,
        -0.322396458041136,
        -2.40075827716184,
        -2.54973253934373,
        4.37466414146497,
        2.93816398269878,
    )
    d = (0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742)

    p_low = 0.02425
    out = np.empty_like(p_arr)

    lo = p_arr < p_low
    hi = p_arr > 1.0 - p_low
    mid = ~(lo | hi)

    if lo.any():
        q = np.sqrt(-2.0 * np.log(p_arr[lo]))
        out[lo] = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if hi.any():
        q = np.sqrt(-2.0 * np.log(1.0 - p_arr[hi]))
        out[hi] = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if mid.any():
        q = p_arr[mid] - 0.5
        r = q * q
        out[mid] = (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
            * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
        )
    return float(out[0]) if scalar else out


def threshold_sigma(
    false_positive_rate: float,
    samples_per_record: int = 1,
    correlation_length: float = 1.0,
) -> float:
    """n_σ such that a pure-noise record survives the clip with ≤ the target rate.

    The declared operating point (issue #422 ruling): ``false_positive_rate``
    bounds the probability that ANY noise-only sample of a record clears the
    ``noise_mean + n_σ·σ`` threshold. A record of ``samples_per_record``
    samples whose noise is correlated over ``correlation_length`` samples has
    ``n_eff = samples_per_record / correlation_length`` effective independent
    trials, so the per-trial tail probability is the Šidák split
    ``1 − (1 − fp)^(1/n_eff)`` and ``n_σ = Φ⁻¹(1 − p_trial)`` under the
    Gaussian noise model. Both knobs are DECLARED (stamped into the field's
    provenance attrs), never estimated per shot — the threshold is a fixed,
    reproducible property of the product.
    """
    if not (0.0 < false_positive_rate < 1.0):
        raise ValueError(f"false_positive_rate must be in (0, 1) (got {false_positive_rate!r})")
    if samples_per_record < 1:
        raise ValueError(f"samples_per_record must be >= 1 (got {samples_per_record!r})")
    if correlation_length <= 0.0:
        raise ValueError(f"correlation_length must be > 0 (got {correlation_length!r})")
    n_eff = max(float(samples_per_record) / float(correlation_length), 1.0)
    p_trial = 1.0 - (1.0 - float(false_positive_rate)) ** (1.0 / n_eff)
    return float(_probit(1.0 - p_trial))


def waveform_weights(
    counts: np.ndarray,
    noise_mean: np.ndarray,
    noise_stddev: np.ndarray,
    gain: float = 1.0,
    n_sigma: float = 0.0,
) -> np.ndarray:
    """Threshold-to-zero flux weights: ``(count − noise_mean) × gain``.

    Samples at or below ``noise_mean + n_σ·σ`` weigh exactly zero (the
    ratified threshold-to-zero clip — the value is zeroed, not shifted, so
    the surviving weights stay unbiased detected-photoelectron estimates
    under the linear gain model). ``gain`` is the named, versioned conversion
    constant (photoelectrons per count); ``noise_mean``/``noise_stddev`` are
    per-sample columns (record-rate values broadcast by the reader).
    """
    counts = np.asarray(counts, dtype=np.float64)
    noise_mean = np.asarray(noise_mean, dtype=np.float64)
    noise_stddev = np.asarray(noise_stddev, dtype=np.float64)
    weights = (counts - noise_mean) * float(gain)
    weights[counts <= noise_mean + float(n_sigma) * noise_stddev] = 0.0
    return weights


def build_waveform_digest(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    *,
    counts: np.ndarray,
    noise_mean: np.ndarray,
    noise_stddev: np.ndarray,
    gain: float = 1.0,
    false_positive_rate: float = 1e-3,
    samples_per_record: int = 1,
    correlation_length: float = 1.0,
    temporal: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, np.ndarray]:
    """Flux-weighted t-digest of a cell's waveform samples.

    The ``kind: ragged`` reducer for waveform sensors: ``values`` (the
    per-sample coordinate, e.g. synthesized elevation) is the reduced
    dimension; ``counts``/``noise_mean``/``noise_stddev`` arrive as
    row-aligned columns via the params-as-column path; ``gain`` and the
    operating point (``false_positive_rate``, ``samples_per_record``,
    ``correlation_length``) are declared scalars. Weights come from
    :func:`waveform_weights` at :func:`threshold_sigma`'s n_σ; zero-weight and
    non-finite rows are dropped; the surviving ``(value, weight)`` rows sort
    by value and compress under the shared k1 budget
    (:func:`zagg.stats.tdigest._compress`), so the result is a standard §2
    centroid array — ``sum(weights)`` estimates the cell's detected
    photoelectrons, any quantile is recoverable, and cross-block/overview
    folds are the existing :func:`~zagg.stats.tdigest.merge_tdigests_kway`.

    ``temporal`` is the §8.3 companion channel (issue #410): per-sample
    ``uint64`` toc words, row-aligned with ``values``, which the clip mask and
    the value co-sort carry along so each output centroid gets the envelope of
    the samples that survived into it
    (:func:`zagg.stats.tdigest._centroid_envelopes`). Given, the return is a
    ``(digest, words)`` pair. The per-centroid **shape** is the one the espg
    ruling of 2026-08-17 makes universal, identical to the located channel's,
    and its *at every level* half describes this reducer's stores too:
    ``build_waveform_digest`` is a member of the shared digest family
    (``zagg.processing.streaming._DIGEST_FAMILY_FUNCTIONS``, issue #508), so a
    waveform field is D24 class ``approximate`` and folds through the overview
    pyramid when one is declared — each overview level carries the
    per-centroid companion beside the folded payload. The build-time GATES are
    unchanged (the merge/spill folds still refuse the builder, so a waveform
    shard aggregates pooled or single-block-spill only), but the build-time
    WORK is not: ``leaf_column_plan`` filters the declaration through the same
    composable classes, so a pyramid-ON waveform config also folds this column
    worker-side (:func:`zagg.column.fold_column` at the tail of
    ``hive.process_and_write_hive``) — a node-order k-way merge over every
    resident digest, whose measured envelope
    (:func:`zagg.column.write_leaf_column`'s memory note: ~2.0 GB at ~17.6M
    centroids, on top of a loaded 4 GB heap) is NOT validated at GEDI's
    21-33M-kept-rows-per-shard scale. So the ruled deployment path for a
    GEDI-scale store is pyramids-OFF aggregation, then
    :func:`zagg.sweep_overview.declare_pyramid` plus a sweep-only overview
    pass (the runbook contingency) — which builds the ladder without ever
    putting the column fold on the aggregating worker.

    What the words say is ruling 2's honesty property: a waveform record's
    samples share one instant, so a single-shot cell's centroids all carry that
    exact timestamp and only genuinely pooled cells produce ranges — "exact when
    ``shot_count == 1``, a range when pooled", now expressed per centroid rather
    than per cell.

    Returns the ``(k, 2)`` float32 centroid array (``(0, 2)`` when nothing
    survives the clip).
    """
    values = np.asarray(values, dtype=np.float64)
    for name, col in (
        ("counts", counts),
        ("noise_mean", noise_mean),
        ("noise_stddev", noise_stddev),
    ):
        if np.shape(col) != values.shape:
            raise ValueError(
                f"{name} shape {np.shape(col)} does not match values shape {values.shape}"
            )
    if temporal is not None:
        temporal = _check_words(temporal, "temporal", values.shape)
    n_sigma = threshold_sigma(false_positive_rate, samples_per_record, correlation_length)
    weights = waveform_weights(counts, noise_mean, noise_stddev, gain=gain, n_sigma=n_sigma)
    keep = (weights > 0.0) & np.isfinite(values) & np.isfinite(weights)
    values, weights = values[keep], weights[keep]
    if len(values) == 0:
        empty = np.empty((0, 2), dtype=np.float32)
        return (empty, np.empty(0, dtype=np.uint64)) if temporal is not None else empty
    order = np.argsort(values, kind="stable")
    out_means, out_weights, starts = _compress(values[order], weights[order], float(delta))
    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if temporal is None:
        return out
    # The clip mask drops samples, so the channel is masked and co-sorted with
    # the values before reducing over the partition ``_compress`` returned —
    # never reindexed afterwards, which would misalign it from the payload.
    return out, _centroid_envelopes(temporal[keep][order], starts, len(values))


def shot_count(values: np.ndarray) -> int:
    """Distinct contributing records in the cell (companion reducer).

    ``values`` is the per-sample record-id column (e.g. GEDI ``shot_number``
    broadcast to sample rate); the count says whether the cell's per-record
    companions are meaningful (``1``) or pooled (``> 1``).
    """
    values = np.asarray(values)
    return int(len(np.unique(values))) if values.size else 0


def shot_number(values: np.ndarray):
    """The cell's record id when exactly one record contributes, else 0.

    The ratified sentinel semantics (issue #422): a pooled cell
    (``shot_count > 1``) records 0 — real record ids are never 0 — so readers
    branch on one value instead of cross-checking two arrays.
    """
    values = np.asarray(values)
    if values.size == 0:
        return values.dtype.type(0) if values.dtype.kind in "iu" else 0
    uniq = np.unique(values)
    return uniq[0] if len(uniq) == 1 else uniq.dtype.type(0)


def single_shot_value(values: np.ndarray) -> float:
    """A record-constant column's value when one record owns the cell, else NaN.

    For the per-record L1B pass-throughs (``noise_mean``, ``noise_stddev``,
    ``rx_energy``, the elevation endpoints): the broadcast column is constant
    within a record, so a single-valued cell returns that value and a pooled
    cell (mixed values) returns NaN — valid exactly when ``shot_count == 1``.
    """
    values = np.asarray(values, dtype=np.float64)
    if values.size == 0:
        return float("nan")
    uniq = np.unique(values)
    return float(uniq[0]) if len(uniq) == 1 else float("nan")
