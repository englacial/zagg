# Build configs of the live stores

The pipeline configs that built the two published stores, recovered verbatim
from the `config` block of each store's run record (`<root>.status/run-<id>/manifest.json`)
and re-serialized as YAML. They are the `--config` inputs to
`tools/redeclare_dense_ladder.py` (issue #547 step 1): the tool's semantic
guard compares the config's D19 `semantic_hash` against the manifest's frozen
one and refuses anything that did not build the store. Both files reproduce
their store's hash under current `main` (pinned in `tests/test_store_configs.py`).

| file | store | run record | semantic_hash |
|---|---|---|---|
| `atl03_tdigest_o9.build_config.yaml` | `englacial/zagg/demo/atl03_tdigest_o9.zarr` | `run-f1c11f8e259b4bb5a4c7aa3a7762eff8` (2026-08-19) | `b9b15fdde78f…` |
| `gedi_flux_o9.build_config.yaml` | `englacial/zagg/demo/gedi_flux_o9.zarr` | `run-57202e68560f4dc296b56d4fb3a351fe` (2026-08-26) | `4f8287947a83…` |

Both stores were built at **δ=4096** and `parent_order: 9`. The templates under
`src/zagg/configs/` carry δ=8192 (the loss-free leaf bound ruled on issues
#414/#424) and differ from these in more than δ, so a template with δ edited
does **not** pass the guard — use these files as-is. They describe the stores
that exist; they are not templates for new builds (store v2: issue #560).

Regenerate from the run record:

```sh
aws s3 cp --no-sign-request s3://us-west-2.opendata.source.coop/englacial/zagg/demo/<store>.zarr.status/run-<id>/manifest.json - \
  | python -c 'import json,sys,yaml; yaml.safe_dump(json.load(sys.stdin)["config"], sys.stdout, sort_keys=True)'
```
