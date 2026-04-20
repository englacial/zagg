# Store

The store module provides a factory for opening Zarr stores from path strings. Paths starting with `s3://` open S3-backed stores; all other paths open local filesystem stores.

## Factory

::: zagg.store.open_store

## Helpers

::: zagg.store.parse_s3_path
