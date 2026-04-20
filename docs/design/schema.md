# Schema Design

## Single Source of Truth

The YAML pipeline config (`configs/atl06.yaml`) is the single source of truth for:

- **Column definitions and types** --- coordinate columns (`cell_ids`, `morton`) and data variables (`count`, `h_mean`, etc.)
- **Aggregation recipes** --- each data variable specifies a `function` (resolved via `resolve_function()`) or an `expression` (evaluated at runtime), plus `source` column and `params`
- **Zarr array configuration** --- dtype and fill value per column, used by [`xdggs_spec`][zagg.schema.xdggs_spec] to generate the Zarr template

Coordinate and data variable column names are derived from the config via `get_coords()` and `get_data_vars()`, not hardcoded.

## Aggregation Config

Each variable in `aggregation.variables` carries metadata describing its behavior:

| Key | Values | Description |
|-----|--------|-------------|
| `function` | e.g. `"min"`, `"average"`, `"quantile"` | Function resolved via `resolve_function()` (numpy shorthand or dotted path) |
| `expression` | e.g. `"1.0 / np.sqrt(np.sum(1.0 / s_li**2))"` | Python expression evaluated with column arrays in namespace |
| `source` | e.g. `"h_li"` | Input column from the raw observations |
| `params` | e.g. `{weights: "1.0 / s_li**2"}`, `{q: 0.25}` | Extra parameters; string values referencing data_source variables are resolved to arrays |
| `dtype` | e.g. `"float32"`, `"int32"` | Data type for the Zarr array |
| `fill_value` | `0` or `"NaN"` | Fill value for unoccupied cells |

`function` and `expression` are mutually exclusive.

## Aggregation Dispatch

[`calculate_cell_statistics`][zagg.processing.calculate_cell_statistics] is config-driven: it iterates the aggregation variable metadata and dispatches via `resolve_function()` (for `function`-based fields) or `evaluate_expression()` (for `expression`-based fields).

Available aggregation functions (via numpy or dotted import paths):

| Name | Description | Params |
|------|-------------|--------|
| `len` / `count` | Number of observations | --- |
| `min` | Minimum value | --- |
| `max` | Maximum value | --- |
| `var` | Variance | --- |
| `average` | Weighted average | `weights`: column name or expression |
| `quantile` | Quantile value | `q`: quantile (0--1) |

## Extending the Schema

To add a new output statistic, add a variable entry to the YAML config:

```yaml
aggregation:
  variables:
    h_iqr:
      expression: "float(np.quantile(h_li, 0.75) - np.quantile(h_li, 0.25))"
      dtype: float32
```

Or using a function:

```yaml
    h_median:
      function: median
      source: h_li
      dtype: float32
```

Everything else --- the Zarr template, `calculate_cell_statistics`, and `process_morton_cell` --- adapts automatically.

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
