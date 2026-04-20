# Processing

The processing module contains the core aggregation pipeline: reading HDF5 data from S3, spatial filtering by morton cell, computing statistics, and writing results to Zarr.

## Pipeline

::: zagg.processing.process_morton_cell

## Statistics

::: zagg.processing.calculate_cell_statistics

## Zarr I/O

::: zagg.processing.write_dataframe_to_zarr
