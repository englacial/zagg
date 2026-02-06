# magg

Multi-resolution aggregation for ICESat-2 ATL06 data using morton/healpix indexing.

## Background

`magg` processes ICESat-2 ATL06 land ice elevation data into gridded aggregates using HEALPix/morton spatial indexing. It is designed for massively parallel execution on commodity cloud workers (e.g., AWS Lambda), producing [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) output following the [DGGS convention](https://github.com/zarr-conventions/dggs).

The library is organized into four modules:

- **`magg.schema`**: Output schema definition via [`CellStatsSchema`][magg.schema.CellStatsSchema], Zarr template creation, and derived constants
- **`magg.processing`**: Core aggregation pipeline --- reading HDF5, spatial filtering, statistics calculation, and Zarr writing
- **`magg.catalog`**: CMR granule catalog builder that maps parent morton cells to S3 granule URLs
- **`magg.auth`**: NASA Earthdata authentication for S3 access to NSIDC data

## Design Philosophy

1. **Data selection is declarative using STAC** --- query interfaces, not file paths
2. **Aggregation doesn't duplicate the at-rest data** --- source data is fetched for processing but discarded after aggregation
3. **Build leaves-to-root** --- invert the tree construction order to enable parallel processing with small workers

## Quick example

```python
from magg.processing import process_morton_cell
from magg.auth import get_nsidc_s3_credentials

# Authenticate once
creds = get_nsidc_s3_credentials()

# Process a single parent cell
df_out, metadata = process_morton_cell(
    parent_morton=-6134114,
    parent_order=6,
    child_order=12,
    granule_urls=["s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/..."],
    s3_credentials=creds,
)
```

## Contributing

1. Clone the repository: `git clone https://github.com/englacial/magg.git`
2. Install development dependencies: `uv sync --all-groups`
3. Run the test suite: `uv run pytest`

## License

`magg` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
