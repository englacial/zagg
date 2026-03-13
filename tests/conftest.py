import numpy as np
import pandas as pd
import pytest
from zarr.storage import MemoryStore

from magg.config import default_config, get_data_vars


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
