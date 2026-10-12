import os

import numpy as np
import pandas as pd
import pytest
from zarr.storage import MemoryStore

from zagg.config import default_config, get_data_vars


@pytest.fixture
def zarr_store():
    """Create a fresh in-memory Zarr store for each test."""
    return MemoryStore()


@pytest.fixture
def mock_dataframe_factory():
    """Factory to create mock DataFrames matching process_morton_cell output."""
    from mortie import generate_morton_children, geo2mort

    data_vars = get_data_vars(default_config())

    def _create(lat: float, lon: float, parent_order: int, child_order: int) -> pd.DataFrame:
        parent_morton = geo2mort(lat, lon, order=parent_order)

        children = generate_morton_children(parent_morton[0], child_order)
        n = len(children)

        # D16 flip (issue #304): worker carriers hold morton only — the legacy
        # cell_ids column is no longer produced by default. Tests that index
        # fullsphere arrays by NESTED position derive it via nested_ids().
        df = pd.DataFrame({"morton": children}).assign(
            **{var: np.random.randn(n).astype(np.float32) for var in data_vars if var != "count"}
        )
        df = df.assign(count=np.random.randn(n).astype(np.int32))
        return df

    return _create


def nested_ids(df):
    """NESTED uint64 ids for a carrier's ``morton`` column (test indexing).

    The D16 flip (issue #304) removed the legacy ``cell_ids`` column from
    worker carriers; fullsphere tests still index arrays by NESTED position,
    so this derives it from the morton words — the same read-side fabrication
    moczarr performs.
    """
    from mortie import mort2healpix

    return mort2healpix(np.asarray(df["morton"], dtype=np.uint64))[0]


def point_words(n, seed, lat0=45.0, lon0=45.0, spread=1e-4):
    """Order-29 point-kind morton words for ``n`` points near one location.

    Shared fixture helper for the location channel (issue #87). Jitter is tiny
    so all words share a HEALPix base cell (the same guarantee one grid cell's
    observations carry), as ``mortie.common_ancestor`` requires. Unwrapped via
    the sanctioned :func:`zagg.grids.morton.morton_words` boundary adapter.
    """
    from mortie import MortonIndexArray

    from zagg.grids.morton import morton_words

    rng = np.random.default_rng(seed)
    lats = lat0 + rng.uniform(-spread, spread, n)
    lons = lon0 + rng.uniform(-spread, spread, n)
    return morton_words(MortonIndexArray.from_latlon(lats, lons, points=True))


#: Base instant for :func:`toc_words` — well inside the toc grammar's range and
#: far from its 1850 epoch, matching the §7 fixture generator's clock.
TOC_BASE = "2019-05-14T02:11:07.250000000"


def toc_words(n, step_s=3.0, base=TOC_BASE):
    """``n`` exact toc **timestamp** words, one per observation, ``step_s`` apart.

    Shared fixture helper for the temporal channel (spec §8.3, issue #410):
    per-observation instants, so every multi-member fold produces a genuine
    range and every singleton round-trips an exact nanosecond timestamp.
    Returns the packed ``uint64`` words a reducer's ``temporal=`` takes.
    """
    import mortie

    when = np.datetime64(base, "ns").astype("int64") + (
        np.arange(n, dtype="int64") * int(step_s * 1_000_000_000)
    )
    return np.asarray(
        mortie.time2toc(mortie.from_datetime64(when.astype("datetime64[ns]"))), dtype=np.uint64
    )


@pytest.fixture(autouse=True)
def _no_s3_run_stats(monkeypatch):
    """Keep unit tests hermetic: never PUT the run stats parquet to real S3.

    The dispatcher's run-level parquet write (issue #297) is fail-open in
    production, but a unit test driving a lambda-path harness with an
    ``s3://`` store path must not attempt a live PUT (ambient local
    credentials could reach a real bucket). Relative local store paths are
    skipped too: tests using ``"./out.zarr"`` would otherwise scribble
    accumulating ``stats_*.parquet`` files into the repo working directory.
    Absolute ``tmp_path`` stores stay live so the parquet wiring is still
    integration-tested; a test that wants the s3 branch re-patches
    ``zagg.runner._write_run_stats`` itself.
    """
    from zagg import runner

    real = runner._write_run_stats

    def guard(store_path, rows, *, summary=None, **kwargs):
        sp = str(store_path)
        if sp.startswith("s3://") or not os.path.isabs(sp):
            # Skip the live PUT (real S3, or a relative path that would land in
            # the repo cwd), but mirror the real helper's schema contract
            # (issue #297): a skipped write still leaves ``run_stats_path``
            # present (None), so the summary key set stays deterministic.
            if summary is not None:
                summary["run_stats_path"] = None
            return
        return real(store_path, rows, summary=summary, **kwargs)

    monkeypatch.setattr(runner, "_write_run_stats", guard)


@pytest.fixture(autouse=True)
def _tail_families_sweep_lands_nothing(monkeypatch):
    """Stub the tail's families-sweep barrier (issue #610): one invoke, no wait.

    The Lambda tails fire the families pass partitioned and BLOCK on the
    workers' store-root records; the stub clients the dispatcher harnesses
    use land none, so the real seam would poll out its budget. The stub fires
    the one ``mode="sweep"`` Event invoke those harnesses have always
    observed (through ``runner._invoke_lambda_sweep``, so a test's own patch
    of it still intercepts) and reports it landed. The seam's own tests
    override this fixture by name (``tests/test_sweep_families_fleet.py``).
    """
    from zagg import runner

    def one_invoke(client, function_name, store_path, leaves, *, output_creds_event=None, **_kw):
        runner._invoke_lambda_sweep(
            client, function_name, store_path, leaves, output_creds_event=output_creds_event
        )
        return {"partitions": 1, "fired": 1, "landed": 1, "finisher": "ok"}

    monkeypatch.setattr(runner, "_invoke_lambda_families_sweep", one_invoke)


@pytest.fixture(autouse=True)
def _no_run_stats_verify(monkeypatch):
    """Disable the issue #313 fire->verify->re-fire poll by default in tests.

    The verify leg does read-only existence polls against the OUTPUT store —
    a real network HEAD for the s3:// store paths unit harnesses use, plus a
    bounded sleep-poll either way. A zero window reports success after zero
    reads (the documented single-fire escape hatch), keeping dispatch tests
    hermetic and fast; the re-fire tests set the window back explicitly.
    """
    from zagg import runner

    monkeypatch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", 0.0)
