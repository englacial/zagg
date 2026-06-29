"""The tabular output writer for the temporal/event pipeline (issue #12, Phase 6).

The temporal/event core produces one row per event with scalar attribute columns
(challenge #5 / the issue #12 "Output: HDF5/DataFrame (tabular)" row).
:class:`TabularWriter` flattens :func:`zagg.temporal.process_event` result rows
into a table and writes it to Parquet (default) or CSV -- both using
``pyarrow``/``pandas``, which are already core dependencies, so the temporal
output path needs **no new dependency**.

The issue's comparison table names HDF5 as the AR repo's tabular output; zagg
standardises on Parquet instead (see PR #70 discussion) -- the temporal output is
flat tabular, not gridded, so it does not map onto the Zarr/xarray model, and
Parquet already ships in core. (``h5coro`` -- zagg's HDF5 dependency -- is a
byte-range *reader* only, so it cannot write regardless.)
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
}


@register_writer("tabular")
@register_writer("parquet")
@register_writer("csv")
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

    def write(self, rows, path, *, output_format: str | None = None):
        """Serialise result rows to ``path`` as Parquet or CSV.

        Parameters
        ----------
        rows : list of dict
            Result rows (the ``summary["results"]`` shape).
        path : str or Path
            Output file. The format is inferred from the extension unless
            ``output_format`` is given.
        output_format : str, optional
            ``"parquet"`` (default when the extension is unknown) or ``"csv"``.
            Overrides the extension.

        Returns
        -------
        Path
            The path written.

        Raises
        ------
        ValueError
            For an unknown ``output_format``.
        """
        path = Path(path)
        fmt = output_format or _EXT_FORMAT.get(path.suffix.lower(), "parquet")
        frame = self.to_frame(rows)

        if fmt == "parquet":
            frame.to_parquet(path, index=False)
        elif fmt == "csv":
            frame.to_csv(path, index=False)
        else:
            raise ValueError(f"unknown tabular output format {fmt!r} (expected 'parquet' or 'csv')")
        return path
