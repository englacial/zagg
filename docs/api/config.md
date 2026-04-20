# Config

The config module loads YAML pipeline configurations that define what data to read, how to aggregate it, and where to write output. See [configs/atl06.yaml](https://github.com/englacial/zagg/blob/main/src/zagg/configs/atl06.yaml) for the default configuration.

## Loading

::: zagg.config.load_config

::: zagg.config.load_config_from_dict

::: zagg.config.default_config

## Validation

::: zagg.config.validate_config

## Function Resolution

::: zagg.config.resolve_function

::: zagg.config.evaluate_expression

## Accessors

::: zagg.config.get_agg_fields

::: zagg.config.get_coords

::: zagg.config.get_data_vars

::: zagg.config.get_child_order

::: zagg.config.get_store_path

## Types

::: zagg.config.PipelineConfig
