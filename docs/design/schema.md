# Schema Design

## Single Source of Truth

[`CellStatsSchema`][magg.schema.CellStatsSchema] is a [pandera](https://pandera.readthedocs.io/) `DataFrameModel` that serves as the single source of truth for:

- **Column definitions and types** --- coordinate columns (`cell_ids`, `morton`) and data variables (`count`, `h_mean`, etc.)
- **Aggregation recipes** --- each data variable's `pa.Field(metadata=...)` encodes the aggregation function, source column, and parameters
- **Zarr array configuration** --- dtype and fill value per column, used by [`xdggs_spec`][magg.schema.xdggs_spec] to generate the Zarr template

The module-level constants [`COORDS`][magg.schema.COORDS] and [`DATA_VARS`][magg.schema.DATA_VARS] are derived from the schema, not hardcoded.

## Field Metadata

Each field in `CellStatsSchema` carries metadata describing its role and behavior:

| Key | Values | Description |
|-----|--------|-------------|
| `role` | `"coord"` or `"data_var"` | Whether the field is a coordinate or aggregated statistic |
| `zarr_dtype` | e.g. `"float32"`, `"int32"` | Data type for the Zarr array |
| `fill_value` | `0` or `"NaN"` | Fill value for unoccupied cells |
| `agg` | e.g. `"nanmin"`, `"quantile"` | Aggregation function name (data_var only) |
| `source` | e.g. `"h_li"` | Input column from the raw observations |
| `params` | e.g. `{"q": 0.25}` | Extra parameters for the aggregation function |

## Aggregation Dispatch

[`calculate_cell_statistics`][magg.processing.calculate_cell_statistics] is data-driven: it iterates the schema's aggregation metadata and dispatches to [`AGG_FUNCTIONS`][magg.processing.AGG_FUNCTIONS], a registry mapping function names to callables.

Available aggregation functions:

| Name | Description | Params |
|------|-------------|--------|
| `count` | Number of observations | --- |
| `nanmin` | Minimum value | --- |
| `nanmax` | Maximum value | --- |
| `nanvar` | Variance | --- |
| `quantile` | Quantile value | `q`: quantile (0--1) |
| `weighted_mean` | Inverse-variance weighted mean | `weight_col`: uncertainty column |
| `weighted_sigma` | Uncertainty of weighted mean | `weight_col`: uncertainty column |

## Extending the Schema

To add a new output statistic:

1. Add a field to `CellStatsSchema` with appropriate metadata:

    ```python
    h_iqr: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "iqr",           # new function name
            "source": "h_li",
            "params": {},
        },
    )
    ```

2. If the aggregation function doesn't exist yet, add it to `AGG_FUNCTIONS`:

    ```python
    AGG_FUNCTIONS["iqr"] = lambda values, **kw: float(
        np.quantile(values, 0.75) - np.quantile(values, 0.25)
    )
    ```

3. Update `ATL06AggregationMembers` TypedDict to include the new field for type checking.

Everything else --- `DATA_VARS`, the Zarr template, `calculate_cell_statistics`, and `process_morton_cell` --- adapts automatically.

## Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `cell_ids` | uint64 | HEALPix cell ID at child order |
| `morton` | int64 | Morton index at child order |
| `count` | int32 | Number of observations |
| `h_mean` | float32 | Inverse-variance weighted mean elevation |
| `h_sigma` | float32 | Uncertainty of weighted mean |
| `h_min`, `h_max` | float32 | Elevation range |
| `h_variance` | float32 | Variance |
| `h_q25`, `h_q50`, `h_q75` | float32 | Quartiles |
