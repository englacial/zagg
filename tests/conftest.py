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
    from mortie import generate_morton_children, geo2mort, mort2healpix

    data_vars = get_data_vars(default_config())

    def _create(lat: float, lon: float, parent_order: int, child_order: int) -> pd.DataFrame:
        parent_morton = geo2mort(lat, lon, order=parent_order)

        children = generate_morton_children(parent_morton[0], child_order)
        cell_ids, _ = mort2healpix(children)
        n = len(children)

        df = pd.DataFrame({"morton": children, "cell_ids": cell_ids}).assign(
            **{var: np.random.randn(n).astype(np.float32) for var in data_vars if var != "count"}
        )
        df = df.assign(count=np.random.randn(n).astype(np.int32))
        return df

    return _create


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


@pytest.fixture(autouse=True)
def _no_s3_run_stats(monkeypatch):
    """Keep unit tests hermetic: never PUT the run stats parquet to real S3.

    The dispatcher's run-level parquet write (issue #297) is fail-open in
    production, but a unit test driving a lambda-path harness with an
    ``s3://`` store path must not attempt a live PUT (ambient local
    credentials could reach a real bucket). Local-path writes stay live so
    the parquet wiring is still integration-tested; a test that wants the
    s3 branch re-patches ``zagg.runner._write_run_stats`` itself.
    """
    from zagg import runner

    real = runner._write_run_stats

    def guard(store_path, rows, *, summary=None, **kwargs):
        if str(store_path).startswith("s3://"):
            # Skip the live PUT, but mirror the real helper's schema contract
            # (issue #297): a skipped write still leaves ``run_stats_path``
            # present (None), so the summary key set stays deterministic.
            if summary is not None:
                summary["run_stats_path"] = None
            return
        return real(store_path, rows, summary=summary, **kwargs)

    monkeypatch.setattr(runner, "_write_run_stats", guard)
