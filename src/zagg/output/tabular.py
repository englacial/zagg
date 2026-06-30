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
        """Serialise result rows to a local ``path`` as Parquet or CSV.

        Parameters
        ----------
        rows : list of dict
            Result rows (the ``summary["results"]`` shape).
        path : str or Path
            Output file. The format is inferred from the extension unless
            ``output_format`` is given. For an ``s3://`` target use
            :func:`write_tabular` instead.
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

    def to_bytes(self, rows, *, output_format: str = "parquet") -> bytes:
        """Serialise result rows to an in-memory Parquet/CSV byte buffer.

        The byte-producing twin of :meth:`write`, used by the remote (S3) write
        path so the same flattening logic feeds both local files and object
        ``put``. ``output_format`` must be an explicit ``"parquet"`` or ``"csv"``
        (there is no path suffix to infer from).
        """
        import io

        frame = self.to_frame(rows)
        buf = io.BytesIO()
        if output_format == "parquet":
            frame.to_parquet(buf, index=False)
        elif output_format == "csv":
            frame.to_csv(buf, index=False)
        else:
            raise ValueError(
                f"unknown tabular output format {output_format!r} (expected 'parquet' or 'csv')"
            )
        return buf.getvalue()


def _resolve_format(store_path: str, output_format: str | None) -> str:
    """Pick the concrete ``parquet``/``csv`` serialisation for ``store_path``.

    An explicit ``output_format`` wins; otherwise the file suffix decides, and
    an unknown suffix falls back to ``parquet`` (the temporal default). The
    generic ``tabular`` alias is treated as "infer from the suffix".
    """
    if output_format and output_format != "tabular":
        return output_format
    suffix = store_path.rsplit("/", 1)[-1].lower()
    for ext, fmt in _EXT_FORMAT.items():
        if suffix.endswith(ext):
            return fmt
    return "parquet"


def write_tabular(
    rows,
    store_path,
    *,
    output_format: str | None = None,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    region: str = "us-west-2",
) -> str:
    """Serialise temporal result rows to a local file or an ``s3://`` object.

    The single tabular write entry point shared by the local runner and the
    Lambda ``process_event`` handler (issue #12, Phase 7b). A local path routes
    through :meth:`TabularWriter.write`; an ``s3://`` path serialises to bytes
    and ``put``s the single object via ``obstore`` (the same S3 stack the Zarr
    store uses -- no ``s3fs``/``fsspec``).

    Parameters
    ----------
    rows : list of dict
        Result rows (the ``summary["results"]`` shape).
    store_path : str
        Local path or ``s3://bucket/key.parquet``.
    output_format : str, optional
        ``"parquet"`` (default) or ``"csv"``; inferred from the suffix when
        omitted. ``"tabular"`` is treated as "infer from the suffix".
    credentials : dict, optional
        Explicit S3 write credentials (camelCase ``accessKeyId`` /
        ``secretAccessKey`` / optional ``sessionToken``); omit to use the
        ambient chain (execution role). Ignored for local writes.
    endpoint_url : str, optional
        Custom S3-compatible endpoint. Ignored for local writes.
    region : str
        AWS region for the S3 ``put``. Default ``"us-west-2"``.

    Returns
    -------
    str
        The path/URI written.
    """
    writer = TabularWriter()
    if not store_path.startswith("s3://"):
        # Let the local writer infer parquet/csv from the suffix (None == infer);
        # a concrete format name is passed through as the explicit serialisation.
        local_fmt = None if (output_format in (None, "tabular")) else output_format
        return str(writer.write(rows, store_path, output_format=local_fmt))

    import obstore
    from obstore.store import S3Store

    from ..store import parse_s3_path, s3_store_options

    fmt = _resolve_format(store_path, output_format)
    payload = writer.to_bytes(rows, output_format=fmt)
    bucket, key = parse_s3_path(store_path)
    if not key:
        raise ValueError(f"s3 tabular output needs an object key, got bucket only: {store_path!r}")

    # Shares the Zarr store's credential/path-style rule (issue #12, Phase 7b).
    opts = s3_store_options(credentials=credentials, endpoint_url=endpoint_url, region=region)
    store = S3Store(bucket, **opts)
    obstore.put(store, key, payload)
    return store_path
