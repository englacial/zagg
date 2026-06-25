"""The tabular output writer for the temporal/event pipeline (issue #12, Phase 6).

The temporal/event core produces one row per event with scalar attribute columns
(challenge #5 / "Output: HDF5/DataFrame" in issue #12). :class:`TabularWriter`
flattens :func:`zagg.temporal.process_event` result rows into a table and writes
it to Parquet or CSV -- both using ``pyarrow``/``pandas``, which are already core
dependencies, so the temporal output path needs **no new dependency**.

HDF5 output is supported when the optional ``h5py`` extra is installed; without
it, requesting HDF5 raises a clear, actionable error rather than silently
swapping formats. (``h5coro`` -- zagg's HDF5 dependency -- is a byte-range
*reader* only, so it cannot write.)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import register_writer

#: File-extension -> serialisation format, so a bare path picks the writer.
_EXT_FORMAT = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".h5": "hdf5",
    ".hdf5": "hdf5",
    ".he5": "hdf5",
}


@register_writer("tabular")
@register_writer("parquet")
@register_writer("csv")
@register_writer("hdf5")
class TabularWriter:
    """Writer for tabular temporal/event output (``output.format: tabular``).

    ``write`` takes the result rows the temporal strategy collects -- a list of
    ``{"event_key", "results", "meta"}`` dicts (the ``summary["results"]`` shape)
    -- flattens them to one row per event, and serialises the table.
    """

    def to_frame(self, rows) -> pd.DataFrame:
        """Flatten temporal result rows into one ``DataFrame`` row per event.

        Each row's ``results`` dict becomes scalar columns; ``event_key`` is the
        leading column and ``timesteps_processed`` (from ``meta``) is carried
        through when present. Missing per-event outputs become ``NaN`` so events
        with differing spec coverage still align into one table.
        """
        records = []
        for row in rows:
            record = {"event_key": row["event_key"]}
            meta = row.get("meta") or {}
            if "timesteps_processed" in meta:
                record["timesteps_processed"] = meta["timesteps_processed"]
            record.update(row.get("results") or {})
            records.append(record)
        return pd.DataFrame.from_records(records)

    def write(self, rows, path, *, output_format: str | None = None, key: str = "events"):
        """Serialise result rows to ``path`` as Parquet, CSV, or HDF5.

        Parameters
        ----------
        rows : list of dict
            Result rows (the ``summary["results"]`` shape).
        path : str or Path
            Output file. The format is inferred from the extension unless
            ``output_format`` is given.
        output_format : str, optional
            ``"parquet"`` (default when the extension is unknown), ``"csv"``, or
            ``"hdf5"``. Overrides the extension.
        key : str
            HDF5 dataset key (ignored for parquet/csv). Default ``"events"``.

        Returns
        -------
        Path
            The path written.

        Raises
        ------
        ValueError
            For an unknown ``output_format``.
        ModuleNotFoundError
            For ``hdf5`` when the optional ``h5py`` extra is not installed.
        """
        path = Path(path)
        fmt = output_format or _EXT_FORMAT.get(path.suffix.lower(), "parquet")
        frame = self.to_frame(rows)

        if fmt == "parquet":
            frame.to_parquet(path, index=False)
        elif fmt == "csv":
            frame.to_csv(path, index=False)
        elif fmt == "hdf5":
            self._write_hdf5(frame, path, key)
        else:
            raise ValueError(
                f"unknown tabular output format {fmt!r} (expected 'parquet', 'csv', or 'hdf5')"
            )
        return path

    def _write_hdf5(self, frame: pd.DataFrame, path: Path, key: str) -> None:
        """Write ``frame`` to HDF5 via the optional ``h5py`` extra.

        Each column becomes a dataset under ``key`` (string columns stored as a
        variable-length UTF-8 dtype). Raises an actionable error when ``h5py`` is
        not installed -- the dependency is intentionally optional (see module
        docstring) so the core temporal path stays Parquet/CSV-only.
        """
        try:
            import h5py
        except ModuleNotFoundError as exc:  # pragma: no cover - exercised via skip
            raise ModuleNotFoundError(
                "HDF5 tabular output requires the optional 'h5py' package, which is not "
                "installed. Install it (e.g. `pip install h5py`) or write Parquet/CSV "
                "instead (output.format: parquet)."
            ) from exc

        import numpy as np

        with h5py.File(path, "w") as f:
            group = f.create_group(key)
            for column in frame.columns:
                values = frame[column].to_numpy()
                if values.dtype == object or values.dtype.kind in ("U", "S"):
                    group.create_dataset(
                        column,
                        data=values.astype(str),
                        dtype=h5py.string_dtype(encoding="utf-8"),
                    )
                else:
                    group.create_dataset(column, data=np.asarray(values))
