# Config

The config module loads YAML pipeline configurations that define what data to read, how to aggregate it, and where to write output. See [configs/atl06.yaml](https://github.com/englacial/magg/blob/main/src/magg/configs/atl06.yaml) for the default configuration.

## Loading

::: magg.config.load_config

::: magg.config.load_config_from_dict

::: magg.config.default_config

## Validation

::: magg.config.validate_config

## Function Resolution

::: magg.config.resolve_function

::: magg.config.evaluate_expression

## Accessors

::: magg.config.get_agg_fields

::: magg.config.get_coords

::: magg.config.get_data_vars

::: magg.config.get_child_order

::: magg.config.get_store_path

## Types

::: magg.config.PipelineConfig
