"""Tabular output writer for temporal/event pipelines.

One row per event, columns are the aggregation outputs. Writes Parquet or HDF5;
the HDF5 key ``catalog`` matches the antarctic_AR_dataset convention so existing
downstream readers work unchanged.
"""

import logging

logger = logging.getLogger(__name__)

HDF5_KEY = "catalog"


class TabularWriter:
    """Write ``{event_key: {output_name: value}}`` rows to Parquet or HDF5.

    Parameters
    ----------
    fmt : str
        Output format: ``"parquet"``/``"pq"`` or ``"hdf5"``/``"h5"`` (default).
        A ``.parquet`` store-path suffix also selects Parquet regardless of fmt.
    """

    def __init__(self, fmt: str = "hdf5"):
        self.fmt = (fmt or "hdf5").lower()

    def _is_parquet(self, store_path: str) -> bool:
        return self.fmt in ("parquet", "pq") or str(store_path).endswith(".parquet")

    def to_frame(self, rows: dict):
        """Return the result rows as a pandas DataFrame (index = event key)."""
        import pandas as pd

        df = pd.DataFrame.from_dict(rows, orient="index")
        df.index.name = "event_key"
        return df

    def write(self, rows: dict, store_path: str):
        """Write ``rows`` to ``store_path`` and return the DataFrame written."""
        df = self.to_frame(rows)
        if self._is_parquet(store_path):
            df.to_parquet(store_path)
        else:
            df.to_hdf(store_path, key=HDF5_KEY)
        logger.info(f"Wrote {len(df)} event rows to {store_path}")
        return df
