# AWS Lambda

AWS Lambda function for processing ICESat-2 ATL06 data by morton cell.

## Overview

The Lambda function processes a single morton cell (order 6) by:

1. Reading HDF5 files directly from S3 using h5coro (no downloads)
2. Spatial filtering using morton indexing
3. Calculating summary statistics for child cells (order 12)
4. Writing xdggs-enabled Zarr to S3

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Lambda Function (process-shard)                      │
│  ──────────────────────────────────────────────────────────  │
│  Runtime: Python 3.12                                       │
│  Memory: 2048 MB (2 GB)                                     │
│  Timeout: 900s (15 minutes)                                 │
│  ──────────────────────────────────────────────────────────  │
│  Code (~5 MB):                                              │
│    - deployment/aws/lambda_handler.py (AWS wrapper)         │
│    - src/zagg/ package (processing, auth, catalog)          │
│  ──────────────────────────────────────────────────────────  │
│  Layer (~70 MB compressed, ~240 MB uncompressed):           │
│    - numpy, pandas, h5coro, mortie, pyproj, odc-geo         │
│    - fastparquet, cramjam, shapely, astropy, earthaccess    │
│    - pydantic-zarr, zarr, obstore, pyarrow                  │
└─────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `deployment/aws/lambda_handler.py` | AWS Lambda wrapper function |
| `src/zagg/processing.py` | Cloud-agnostic core processing logic |
| `src/zagg/auth.py` | NASA Earthdata authentication helper |
| `src/zagg/catalog/` | CMR/STAC shard-map (granule catalog) builder (`python -m zagg.catalog`) |
| `deployment/aws/invoke_lambda.py` | Orchestration script |
| `deployment/aws/build_layer.sh` | Lambda layer build script (`x86_64`/`arm64`) |

## Event Payload

```json
{
  "shard_key": 123456,
  "parent_order": 6,
  "child_order": 12,
  "granule_urls": [
    "s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/2023/12/18/...",
    "s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/2023/12/19/..."
  ],
  "store_path": "s3://your-output-bucket/atl06/production.zarr",
  "s3_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "..."
  },
  "output_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "...",
    "endpointUrl": "https://...",
    "region": "us-west-2"
  }
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `shard_key` | int | Yes | Grid-agnostic shard identifier (HEALPix: the parent-cell morton index) |
| `parent_order` | int | Yes | Order of parent cell (typically 6); HEALPix-only (`null` for other grids) |
| `child_order` | int | HEALPix only | Order of child cells for statistics (typically 12); omitted for non-HEALPix grids |
| `granule_urls` | list | Yes | Pre-computed list of S3 URLs from catalog |
| `store_path` | str | Yes | Output Zarr store path (e.g. `s3://bucket/prefix.zarr`) |
| `s3_credentials` | dict | Yes | NSIDC S3 credentials for reading source data |
| `output_credentials` | dict | No | Explicit credentials for *writing* the output store. Omit to use the execution role (in-account writes). Supply to write an external / S3-compatible target. Keys: `accessKeyId`, `secretAccessKey`, optional `sessionToken`/`endpointUrl`/`region`. |

!!! note "Grid-neutral event fields"
    The unit of work is a **shard** — for HEALPix, one parent (order-6) cell. The
    orchestrator and the catalog use that vocabulary (`python -m zagg.catalog`
    emits a shard map with `shard_keys` + a `grid_signature`). The Lambda
    **event** schema uses the grid-neutral field name `shard_key` (the shard
    identifier for any grid; for HEALPix it is the parent-cell morton index).
    `parent_order`/`child_order` are HEALPix-specific: `parent_order` is
    forwarded for every grid (`null` for non-HEALPix), while `child_order` is
    only required/sent for HEALPix runs. See `deployment/aws/lambda_handler.py`.
    This rename landed via [#24](https://github.com/englacial/zagg/issues/24).

### S3 Credentials

Credentials are obtained by the orchestrator once before invoking Lambda functions:

```python
from zagg.auth import get_nsidc_s3_credentials

# Get credentials (valid for ~1 hour)
s3_creds = get_nsidc_s3_credentials()

# Pass to each Lambda invocation
event = {
    "shard_key": -6134114,
    "parent_order": 6,
    "child_order": 12,
    "granule_urls": [...],
    "store_path": "s3://output-bucket/atl06/production.zarr",
    "s3_credentials": s3_creds,
}
```

This approach avoids rate limiting from 1,872 simultaneous NASA logins and eliminates an AWS Secrets Manager dependency.

### Output Credentials (external write targets)

By default the function writes the output store with its **execution role**,
which reaches the in-account output bucket, `sliderule-public-cors`, and zagg's
published prefix on Source Cooperative (issue #495) — omit `output_credentials`
entirely for all three. Injection is for targets we have **not** negotiated a
bucket policy with: a collaborator's private bucket, or an S3-compatible store
like R2/MinIO. Supply `output_credentials` in the event — symmetric to how
`s3_credentials` injects read credentials:

```python
from zagg import load_config, agg

results = agg(
    config, catalog="catalog.json", backend="lambda",
    store="s3://a-collaborators-private-bucket/shared/dataset.zarr",
    output_credentials={  # runtime-only; never store in config/YAML
        "accessKeyId": "ASIA...",
        "secretAccessKey": "...",
        "sessionToken": "...",        # optional
        # "endpointUrl": "https://...",  # optional: R2/MinIO etc.
        # "region": "us-west-2",         # optional
    },
)
```

From the CLI, point `--output-creds` at a JSON file holding that dict (keeps
secrets out of shell history):

```bash
python -m zagg --config atl06.yaml --catalog catalog.json --backend lambda \
  --store s3://a-collaborators-private-bucket/shared/dataset.zarr \
  --output-creds /path/to/output-creds.json
```

The non-secret `endpoint_url` / `region` may also be set in the config's
`output:` section (overridable at runtime); **credentials are runtime-only**.
`endpointUrl` is only needed for non-AWS S3-compatible stores. Dotted bucket
names (e.g. `us-west-2.opendata.source.coop`) and custom endpoints use
path-style addressing automatically.

A write target this account does not own carries
`x-amz-acl: bucket-owner-full-control` on the requests that **create objects**
(issue #495). S3 object ownership follows the *writing* account, so without that
canned ACL a cross-account PUT under the `ObjectWriter` setting leaves objects
the bucket owner cannot manage or delete — Source Cooperative's in-region upload
path requires it. Two shapes qualify, and the second is the one phase 3 added:

- `output_credentials` **without** an `endpointUrl` — an un-negotiated target;
- an **ambient** (execution-role) write to a bucket zagg publishes to but does
  not own. Today that is `us-west-2.opendata.source.coop`, the one entry in
  `zagg.store._PUBLISHED_BUCKETS`. Since the fleet now reaches it with the
  execution role and no injected credentials, keying the header on credentials
  alone would publish owner-less objects silently.

Reads and lists carry no ACL header at all, and that split is not cosmetic
(issue #522). obstore has no ACL config key, so the header rides as a default
request header — and obstore puts default headers into the SigV4 signature on
every request *except* `ListObjectsV2`. Not just the keyed ones: the bucket-level
`POST ?delete` bulk delete signs it too
(`tests/test_store_acl_signing.py::test_only_the_list_path_leaves_the_acl_unsigned`),
so the list is the single miss, which is why splitting the handle is the shape
of the fix. On a `ListObjectsV2` the header is on the wire but outside
`SignedHeaders`, which S3 rejects outright:

```
403 AccessDenied: There were headers present in the request which were not signed
<HeadersNotSigned>x-amz-acl</HeadersNotSigned>
```

A single handle therefore cannot both publish and list, and listing is not
optional — the per-leaf template guard lists the digit tree and the client
status poller lists its `.status/` channel. So `zagg.store` opens **two** handles
for such a target: the one callers hold is clean, and an ACL-bearing twin hangs
off it for object-creating requests. `open_store` returns a Zarr store that
routes its own writes to the twin; raw-obstore writes go through
`zagg.store.put_object`, which does the same. Callers do not choose: the seam is
enforced by test, and a direct call to an object-creating obstore API — `put`,
`put_async`, `open_writer`, `copy` or `rename` — anywhere under `src/zagg` or in
`deployment/aws/*.py` fails the suite. The guard parses the AST rather than
grepping, so aliasing the import (`import obstore as obs`, `from obstore import
put`) does not get past it.

It is derived, not configured: there is no ACL knob to set. Writes to buckets we
*do* own — the output bucket, `sliderule-public-cors` — still send no header;
that is deliberate, since the header requires `s3:PutObjectAcl` on the target
and the execution role holds it only on the published prefix (see
`deployment/aws/template.yaml`). Any target reached through an `endpointUrl` is
excluded —
both the S3-compatible stores behind that knob (R2, MinIO), which do not
implement canned ACLs at all, and an endpoint-routed *AWS* target such as the
retired `data.source.coop` proxy hop, which this native-write path exists to
replace. A caller that must send no ACL at all can pass
`client_options={"default_headers": {"x-amz-acl": None}}`, which strips the
header; nothing in the Lambda config surface does.

### Write probe {#write-probe}

Reachability is not permission, so the pre-fan-out ping
([issue #495](https://github.com/englacial/zagg/issues/495)) does not stop at
the read-only store check: it PUT-then-DELETEs one zero-byte object before any
worker is dispatched. **Two requests, added to the ping, and only for `s3://`
stores** (a local store has nothing to prove). It is not hive-specific — every
`s3://` ping runs it, the raster path included.

Why it exists: credentials that can read the store but not write it are exactly
how a fresh cross-account grant fails, and Source Cooperative's in-region path
vends no credentials of its own (our IAM role writes through *their* bucket
policy), so no interactive step would catch a misconfigured grant. Without the
probe the first real write is the fire-and-forget `mode="setup"` invoke whose
failure nobody sees, and the denial surfaces only after every worker has read
and aggregated its shard.

**Grant requirement.** The probe writes `<store>.status/probe-<uuid>` — the
run's async-result sibling, *not* the store root and *not* a prefix of its own.
That prefix is one the run already needs writable (the async invoke/poll
transport writes every per-shard status object under it), so a grant covering
`<store>/*` + `<store>.status/*` passes the probe exactly when the run's real
writes would succeed. Nothing new to enumerate. Keeping the probe out of the
store root is deliberate: `docs/specification.md` §5.2 makes the leaf hash set
discovery-based, so a probe object stranded inside a leaf by a denied DELETE
would be a *key-set difference* and a verifier would report an intact leaf as
tampered.

**What it covers, and what it does not.** The PUT proves `s3:PutObject`, and one
small PUT is representative of the multipart path
(`CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload` are all
authorized by `s3:PutObject`). It cannot exercise `s3:AbortMultipartUpload` or
`s3:ListMultipartUploadParts`, which the grant carries deliberately for
aborted/retried uploads — a grant missing those still passes.

**Outcomes.** A failed PUT is **fail-closed**: the ping returns 500 tagged
`"check": "write_probe"` and the dispatcher refuses the run, naming the failing
request (a denied grant being the likely cause) rather than sending you to clear
a store root that is not the problem. A failed DELETE is **fail-open** — write
permission is proven, which is what the preflight gates on — but it is reported
(`probe_delete: false` plus `probe_key` in the 200 body) and the dispatcher logs
a warning naming the stranded object and the likely missing `s3:DeleteObject`.
Do not ignore it: `s3:DeleteObject` is not optional for zagg's real writes
(store overwrite, manifest cleanup), so that run is likely to fail later, and
each run leaves one zero-byte object behind under a prefix nothing sweeps.

> Not to be confused with the manual `s3://BUCKET/PREFIX/.probe` check in
> [`benchmark-cicd.md`](benchmark-cicd.md) — that one is a human-run
> `aws s3 cp` *inside* the prefix, cleaned up by hand in the same command. The
> automated probe deliberately never writes inside the store root.

## Deployment

### Recommended: CloudFormation standup

The recommended way to stand up the backend in a fresh AWS account is the
committed CloudFormation template, driven by `stand_up.sh`, which creates the
execution role, dependency layer, and function in one stack:

```bash
OUTPUT_BUCKET=my-results-bucket bash deployment/aws/stand_up.sh
```

See **[Standing Up the Backend](standup.md)** for the full walkthrough: what the
script does, the parameter/environment-variable reference, cross-region staging,
and teardown. The stack always creates the IAM execution role, so the identity
running the standup needs `iam:CreateRole` — in an account whose deploy identity
cannot (e.g. an AWS SSO "power user" set), have an admin run the standup itself.

### Worker-size variants {#worker-size-variants}

The stack pre-provisions six size variants of the worker (issue #235) --
same code, layer, and role as `process-shard`, differing only in memory and
`/tmp` -- so a run picks its size by *function name*, with no
admin-role `UpdateFunctionConfiguration` swap and no serialization between
concurrent runs of different workloads:

| Function | Memory | `/tmp` |
|----------|--------|--------|
| `process-shard-2048` | 2048 MB | 512 MB |
| `process-shard-4096` | 4096 MB | 512 MB |
| `process-shard-8192` | 8192 MB | 512 MB |
| `process-shard-2048-disk` | 2048 MB | 4096 MB |
| `process-shard-4096-disk` | 4096 MB | 6144 MB |
| `process-shard-8192-disk` | 8192 MB | 10240 MB |

Select a variant from the aggregation YAML with the optional top-level
`worker:` block (alongside `pipeline:`):

```yaml
worker:
  memory: 2048       # one of 2048 | 4096 | 8192
  extra_disk: false  # true -> the -disk twin (/tmp = memory + 2048 MB)
```

Resolution precedence (`_resolve_function_name` in `zagg/runner.py`): an
explicit `agg(function_name=...)` / `--function-name` wins verbatim; else the
base name from `ZAGG_LAMBDA_FUNCTION_NAME` (default `process-shard`, so test
stacks compose -- e.g. `process-shard-test-2048`) gets the `worker:` suffix
appended; no block invokes the unsuffixed default, exactly as before. Invalid
`worker:` values fail at config load with the allowed set named.

!!! note "Cost caveat: memory buys vCPU"
    Lambda allocates vCPU proportional to memory, so halving memory halves
    $/GB-s **and** halves compute. CPU-bound shards (e.g. dense ATL03
    aggregation) stretch in duration and eat most of the savings; I/O-bound
    work (raster sampling, temporal readers) keeps nearly the full 2x. Pick
    the per-template default from the workload's bottleneck, not price alone
    (see the issue #213 utilization analysis).

### Legacy / manual deploy {#legacy-manual-deploy}

!!! warning "Not the recommended path"
    The steps below hand-assemble the function zip and create/update the Lambda
    with raw `aws lambda` calls. They are kept for understanding what the
    template builds and for one-off tweaks, but the
    **[CloudFormation standup](standup.md)** above is the preferred, reproducible
    way to deploy. The maintainer in-place code updater
    `deployment/aws/deploy.sh` (pulls the latest CI artifacts and runs
    `aws lambda update-function-code`) is a convenience over the manual
    `update-function-code` step; it updates an already-deployed function and does
    not create the role/function/bucket.

#### Step 1: Create the function package

```bash
cd /path/to/zagg

# Create function.zip with handler and zagg package
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip zagg/ -i "*.py" && cd ..
```

#### Step 2: Build and deploy the Lambda layer

See [ARM64 Layer](arm64.md) for building and deploying the Lambda layer.

#### Step 3: Create the Lambda function

```bash
aws lambda create-function \
  --function-name process-shard \
  --runtime python3.12 \
  --architectures arm64 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://deployment/aws/function.zip \
  --timeout 900 \
  --memory-size 2048 \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:zagg-layer-arm64:VERSION
```

#### Updating function code

```bash
# Re-create the zip
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip zagg/ -i "*.py" && cd ..

# Update the Lambda function
aws lambda update-function-code \
  --function-name process-shard \
  --zip-file fileb://deployment/aws/function.zip
```

## Testing

```bash
# Raise the open-file limit before fanning out: each concurrent worker holds
# one socket to the Lambda endpoint, and the default soft limit (often 256)
# would otherwise cap concurrency. See "Concurrency, workers, and file
# descriptors" below.
ulimit -n 8192

# Build a shard map
uv run python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson

# Test locally first (no Lambda required)
uv run python -m zagg --config atl06.yaml --catalog catalog.json \
  --store ./test.zarr --max-cells 1

# Dry run with the Lambda orchestrator
uv run python deployment/aws/invoke_lambda.py \
  --config atl06.yaml --catalog catalog.json --dry-run
```

## Concurrency, workers, and file descriptors

The Lambda backend fans out one synchronous `invoke` per cell across a thread
pool, and each in-flight worker holds an open socket to the Lambda endpoint.
Two limits bound how many can run at once, and the orchestrator checks both
**before** dispatch so cells are never silently dropped:

- **Open file descriptors (`ulimit -n`).** If concurrent workers exceed the
  process's open-file soft limit (256 on stock macOS / many Linux shells),
  invokes fail with `OSError: [Errno 24] Too many open files` — a client-side
  failure AWS never sees. The runner derives a safe ceiling from the soft limit
  and surfaces errno-24 with actionable guidance instead of a raw connection
  error. Raise the limit before a large run: `ulimit -n 8192`.
- **Account Lambda concurrency.** The runner reads the account
  `ConcurrentExecutions` ceiling and current usage (CloudWatch) and clamps
  workers to the available headroom (5% padding, floored at 100 free slots), so
  a run can't saturate the account pool and throttle itself or other Lambda
  activity. This degrades gracefully if the dispatch role lacks
  `lambda:GetAccountSettings` / `cloudwatch:GetMetricStatistics` — it then
  bounds workers by the FD limit alone.

Keep `--max-workers ≤ min(ulimit -n − headroom, account concurrency)`. The
orchestrator enforces this automatically; setting `ulimit -n` higher simply
raises the FD ceiling it can use.

## Performance

| Metric | Value |
|--------|-------|
| Average execution time | 2--3 minutes per cell |
| Maximum execution time | 10 minutes |
| Lambda timeout | 15 minutes (900s) |
| Configured memory | 2048 MB |
| Typical memory usage | 1--1.5 GB |
| Cold start | 3--5 seconds |

## Warm-container memory and self-recycle

Warm (reused) sandboxes retain process RSS across invocations — the issue
#169 forensics showed container-lifetime memory ratcheting 959 → 1650 →
2029 MB → OOM at the 2047 MB cap across four back-to-back fleet runs on the
same 9 sandboxes, even with the glibc allocator tunables
(`MALLOC_ARENA_MAX`/`MALLOC_TRIM_THRESHOLD_`, issue #143) deployed. Two
mechanisms address this (issue #171):

- **Container telemetry** — every worker result envelope carries
  `container_cold`, `container_generation`, `rss_start_mb`, `sandbox_id`,
  and `container_init_ts`; the run summary rolls these into
  `worker_cold_starts` / `worker_warm_starts` /
  `worker_rss_start_max_by_gen` (flat across generations = healthy;
  climbing = the ratchet).
- **Self-recycle** — after an async invocation's result envelope is safely
  mirrored to its `result_url`, the handler exits the sandbox
  (`os._exit(0)`) when current RSS ≥ `ZAGG_RECYCLE_RSS_MB` (template
  default 1400) or the sandbox has served `ZAGG_RECYCLE_MAX_INVOCATIONS`
  **recycle-eligible (async) invocations** (template default 1 — recycle
  after every async invocation, the cold-every-time posture). Set either to
  `0`/empty to disable that check. The next invocation then starts on a
  fresh container instead of ratcheting toward OOM. Synchronous invocations
  never self-recycle (the response would be lost) and don't consume the
  recycle budget (issue #177: the runner's sync setup invoke warms a
  sandbox, so counting it made `MAX_INVOCATIONS=1` deliver generation-2
  workers); `container_generation` telemetry still counts every invocation.

!!! warning "The raw `Errors` metric is 100% noise under this posture"
    A self-exit after the result write is counted as a runtime error by
    Lambda's `Errors` metric — **cosmetically only**: the result object at
    `result_url` is the source of truth for the orchestrator (issue #153),
    and `MaximumRetryAttempts: 0` in the template guarantees no zombie
    retry. With the default `RecycleMaxInvocations=1`, *every* async
    invocation self-recycles, so raw `Errors` ≈ invocation count. Each
    recycle logs one structured line first:

    ```
    ZAGG_SELF_RECYCLE rss_mb=<current> async_served=<n> generation=<n> threshold=<crossed limit>
    ```

    The template materializes the real-vs-expected split as CloudWatch
    metrics (namespace `zagg/lambda`, per function): metric filters on both
    log groups publish `ProcessSelfRecycleCount` / `ExtractSelfRecycleCount`
    (the `ZAGG_SELF_RECYCLE` line — expected exits) and
    `ProcessWorkerErrorCount` / `ExtractWorkerErrorCount` (genuine failure
    signatures only: `[ERROR]` lines, tracebacks, `Task timed out`,
    `Runtime.OutOfMemory`, nonzero runtime exits — a clean self-exit
    reports "Runtime exited *without providing a reason*" and is
    deliberately not matched). **Alarm and dashboard on
    `WorkerErrorCount`, never on the raw `Errors` metric.**

    Two operational corollaries: **never attach an async `OnFailure`
    destination** (SQS/SNS/EventBridge) to these functions while the
    recycle-every-invocation posture is active — it would receive every
    invocation; and on a **fresh** stack create with
    `CreateLogMetricFilters=false`, invoke each function once (Lambda
    creates the log groups lazily; the filters need them to exist), then
    update the stack with `true`.

For guaranteed all-cold fleets (certification/benchmark baselines) there is
also the dispatch-side big hammer: `agg(..., force_cold=True)` bumps a
`ZAGG_COLD_EPOCH` function-environment marker before fan-out, invalidating
every warm sandbox at once. It requires `lambda:GetFunctionConfiguration` +
`lambda:UpdateFunctionConfiguration` on the *caller* and chills the warm
pool for all users of the function, so it is off by default and independent
of the self-recycle knobs (both can be enabled).

## Staged pyramid sweep over the fleet {#staged-sweep}

The `zagg-pyramid/2` above-shard ladder is built by the **staged dense sweep**
(issues #384/#416): stage workers fold child columns into parent columns, one
dispatch tuple at a time, finest tuple first. `python -m zagg.sweep <root>
--stages` runs the whole thing in one local process. Issue #519 added the
fleet transport so it can also run entirely worker-side.

You need the fleet transport whenever the **dispatcher cannot write to the
store**. The canonical case is a Source Cooperative–published store: the
bucket policy names the *fleet execution role* as the write identity
(#495/#496), so a local stage worker has no sanctioned write path at all and
the CLI cannot serve those stores. It is also the D8 posture everywhere else —
the Lambda dispatcher never PUTs; workers do.

### Wire grammar

No new mode. A `stage` block on the existing `mode: "sweep"` event selects the
staged arm, reusing that event's credential resolution and its
`leaves` / `discover` work-set transport:

```json
{
  "mode": "sweep",
  "store_path": "s3://bucket/prefix.zarr",
  "leaves": [[1152921504606846982, null]],
  "stage": {
    "role": "stage",
    "run_id": "stage-20260825T094152Z-53c774",
    "run_started": "2026-08-25T09:41:52+00:00",
    "dispatch": 3,
    "nodes": ["1111", "1112"],
    "batch": 0,
    "tuple_width": 3,
    "records_from": "s3://bucket/prefix.zarr.status/run-stage-20260825T094152Z-53c774"
  }
}
```

| Key | Role | Description |
|-----|------|-------------|
| `role` | both | `"stage"` (default) or `"finisher"` |
| `run_id` | both | The sweep run's identity: the lease, the skip-key / foreign-stamp namespace, and the status prefix all key on it |
| `run_started` | stage | Dispatcher-pinned UTC ISO stamp, shared by every worker of the run. A worker computing its own would read a sibling's fresh stamp as a foreign sweep's |
| `dispatch` | stage | The tuple's dispatch order. The worker runs exactly that one tuple |
| `nodes` | stage | This invoke's dispatch nodes, as morton decimals. Must be non-empty and every entry must sit at exactly `dispatch` order — the worker refuses otherwise. In the example above the store is shard-order 6, so at `tuple_width: 3` the dispatch orders are 3 and 0, and an order-3 dispatch takes 4-digit nodes |
| `batch` | stage | Optional (defaults to `0`); which batch of that tuple this is, and what names the record object |
| `unit` | stage | Optional ([issue #586](https://github.com/englacial/zagg/issues/586)). Which of the nodes' stage units this invoke runs: `"window"` — ONE window's fold, named by `window` — or `"close"`, the per-node step that follows a node's window units (the all-time fold). Absent, the worker runs the nodes whole — every window, then the close, serially — which is an unwindowed store's one unit per node and what a dispatcher predating the units sends. Refused by name against an unwindowed store |
| `window` | stage | The window label. **Required** with `unit: "window"`, refused with anything else |
| `pipeline_run_id` | both | Optional ([issue #593](https://github.com/englacial/zagg/issues/593)). The id of the aggregation run this sweep **completes** — distinct from `run_id`, the sweep's own. Recorded verbatim in every stage record, `finisher.json` and the store-root run record; absent, they record `null`, which vouches for no run |
| `tuple_width` | stage | Optional; defaults to `zagg.sweep_stage.DEFAULT_TUPLE_WIDTH`. Always the **run's** width, sized schedule or not — a tuple's own width rides in `child_order` instead, so an unsized run's events are byte-identical to those a dispatcher predating [issue #610](https://github.com/englacial/zagg/issues/610) sent. A finisher takes no tuple width — one on a finisher block is inert |
| `child_order` | stage | Optional ([issue #610](https://github.com/englacial/zagg/issues/610)). The order of the child columns this tuple reads — its span, `[dispatch, child_order)`, outright. A sized schedule dispatches at orders no single `tuple_width` lands on, so the span cannot be re-derived worker-side; absent, the width selects the tuple out of the fixed-width schedule as before. A worker predating #610 ignores it and refuses such a tuple by name (`no stage tuple dispatches at order D`) rather than folding the wrong span |
| `partition` | stage | Optional `{"index", "of"}`; recorded on the stage rows |
| `lease_ttl_s` | both | Optional lease TTL override |
| `records_from` | both | **Required.** The run's status prefix — a store **sibling** (`<store>.status/run-<run_id>`, `zagg.client_transport.run_status_prefix`). Each invoke PUTs its record there; the finisher reads them back. Both roles refuse by name without it: a stage worker that wrote no record would fold correctly and then be indistinguishable from a lost invoke, and a finisher without the records has no per-level actuals at all. A finisher also refuses a prefix holding zero records (a *partial* set is fine — under-coverage is recorded and self-heals) |
| `touch_policy` | finisher | Optional (defaults to `"auto"`); the `output.touch` declaration (issue #501) governing the `aggregation.yaml` touch |
| `barrier_timed_out` | finisher | Optional, defaults `false`. Sent by the dispatcher when any tuple's barrier expired; the finisher records it in the store-root run record and in `finisher.json`, so the durable record says the per-level actuals may be short |
| `short_orders` | finisher | Optional ([issue #610](https://github.com/englacial/zagg/issues/610)); the ladder orders of every tuple the dispatcher is missing a unit record for. The finisher **withholds** their manifest `actuals` — the entries keep whatever stood — and records the rest as usual, so a walled tuple's levels are no longer stamped as if observed. Reported back as `short_orders` and `finisher.actuals_withheld`; absent, nothing is withheld |

Every store write stays worker-side. The dispatcher only invokes and polls.

### How a run is sequenced

`zagg.sweep_fleet.run_stage_sweep_fleet` mirrors the in-process driver's tuple
ordering exactly:

0. **size the schedule** to the per-invoke fold, when the caller asked for
   it — the run tail does, the dispatcher's own default does not
   ([issue #610](https://github.com/englacial/zagg/issues/610)). Each
   fixed-width tuple is subdivided inside its own `[dispatch, child_order)`
   span until its fattest dispatch node folds at most
   `zagg.sweep_partition.STAGE_TARGET_NODES` nodes, so a store that needs no
   narrowing keeps the fixed-width schedule itself. The fold is the **dense
   bound** `(4^width − 1) / 3` unless a `coverage` MOC was handed in, in
   which case it is measured over the set a worker folds (the run's leaves ∪
   that coverage); nothing is read from the store either way. A dispatch node
   folds its whole subtree down to the tuple's child order in one invoke, so
   this — not `max_nodes_per_invoke` — is what bounds a *node's* fold: at
   width 3 a dense tuple folds 21 nodes however small the node cap, which is
   what walled the v3 ladder's `[2,1,0]` tuple at 900 s. The two compose:
   `max_nodes_per_invoke` nodes ride one invoke and each folds its own
   subtree, so the schedule is sized against
   `STAGE_TARGET_NODES / max_nodes_per_invoke`, and under payload-only
   packing (`None`, a whole tuple on one worker) the target bounds no invoke
   at all — the dispatcher logs that and sizes as if one node an invoke. Each
   tuple's row reports the `width` it was given and the `fold_max` that chose
   it;
1. **fan out** one tuple's stage units, batched under `max_nodes_per_invoke`
   *and* the 250 KB async payload cap — whichever binds first closes a batch —
   with one `InvocationType="Event"` invoke per batch. On an unwindowed store
   a unit is a dispatch node. On a windowed one it is a **`(node, window)`
   pair** ([issue #586](https://github.com/englacial/zagg/issues/586)): one
   fan-out per window over the nodes that window is dirty beneath, every
   window in flight at once, each event carrying its own window's leaf refs
   alone;
2. **soft-barrier** — poll the status prefix until every batch's stage record
   lands, or the barrier budget expires. Per #381 point (6) the barrier is a
   *scheduling* preference, not a correctness device: under-coverage is
   recorded in each artifact's own `source_children` and heals on the next
   pass, so an expired barrier logs loudly and the run proceeds;
3. on a windowed store that declares `output.pyramid.all_time`, the tuple's
   **close** units — one per node, the all-time fold over the node's
   per-window overviews — are fired once its window units are in. The next
   tuple reads only the window units' stage columns, so it is fired at the
   same moment and the two share one barrier; the last tuple's closes get
   their own before the finisher. A store that does not declare the fold
   fires no close and waits for nothing;
4. next tuple; then the **finisher** invoke last — root `coverage.moc`,
   manifest per-level actuals, `aggregation.yaml` touch, lease release —
   **unless no tuple produced a dispatch node at all** (every leaf skipped as
   mixed-order, or filtered out by scope), in which case nothing fires, there
   is no finisher and no barrier, and the summary says so:
   `skipped: "no dispatch nodes"` with `finisher.fired: false`. A finisher
   over zero stage records refuses by design, so firing it would buy one
   guaranteed 500 the Event invoke hides plus a full barrier on a record that
   can never land.

The unit list is `zagg.sweep_units.stage_units` — the same function the
worker's own pass runs — so the records the dispatcher waits for are exactly
the units the workers execute. A tuple's row in the dispatcher's summary
counts its window units (`batches` / `records_seen`) and its closes
(`close_batches` / `close_records_seen`); a unit whose record never landed is
**named** in `missing_units` (`{batch, nodes, unit, window}`, the first 50;
`missing_unit_count` is exact), so a dead `(node, window)` is told apart from
a late tuple. Those rows are also what the finisher is told to **withhold**:
the orders of every tuple short a record ride the finisher event as
`short_orders`, and their manifest `actuals` are left as they stood rather
than stamped from a run that did not observe them (issue #610 — the v3 run
recorded actuals for its walled `[2,1,0]` tuple exactly as it did for the
complete orders 8..3). The dispatcher's summary carries the same list. A window unit that *raises* is not one that died: on a windowed
store the worker counts it, names it in its record's `unit_errors`, and still
writes the record, so the barrier is met at once. Either way the failure
costs that window's artifacts and nothing else — the node's other windows
are separate invokes over separate objects — and the close records the gap
(`source_windows.missing` on the all-time artifact).

The declaration is handed to the dispatcher (`windowed=`, `all_time=`), like
`shard_order`, because it never reads the manifest; the runner's tail takes
both from the config. A caller that says neither gets whole-node events,
which are correct on every store — the worker then runs a windowed node's
windows serially.

#### Which function the stage invokes go to

The staged tail runs on its **own tier**: the 8,192 MB / 10 GB-`/tmp` variant
of the run's function family (`process-shard-8192-disk` for the default
stack), resolved by `runner._resolve_stage_function_name` — the run's
function with its `-<memory>[-disk]` suffix replaced, or
`ZAGG_LAMBDA_STAGE_FUNCTION_NAME` verbatim when set. It is the interim ruled
on issue #586 (2026-09-26): the 0.55 fleet's order-6 stage died at the 4 GB
cap on 64 unwindowed leaves. The fold is now streamed block by block
(`zagg.sweep_fold` — a worker holds one block of inputs, not a level), and
the tier stays until that has been measured on the fleet; stage nodes sit at
order 6 and coarser, so the larger function is a small share of a run. The
dispatcher's role must be allowed to invoke that function. Both dispatchers
resolve it the same way — `python -m zagg` (`runner._run_lambda`) and the
`zagg.client` facade's tail — from the run's own resolved function name
(`runner.DEFAULT_FUNCTION_NAME`, `process-shard`, when nothing else names
one), and both name the run in the stage event (`pipeline_run_id`), which is
what the operator `finalize` matches.

#### How wide the fan-out is

`max_nodes_per_invoke` caps how many dispatch nodes one invoke folds, and its
**default is 1 — one dispatch node per invoke, at every tuple** (espg ruling,
issue #547, 2026-09-11). That is the fan-out an `output.sweep: "stages"` run
gets unless it says otherwise: the runner's tail passes no value and inherits
it, and so does the ad-hoc driver below. The ruling was made on ATL03's T1 —
the 110-node o6 tuple whose fattest node is already a full 4³ subtree of leaf
columns against the 900 s wall — and applying it at every tuple is safe there
because the coarser tuples are small (22 nodes, then 3).

It is **orchestration only**: dispatch nodes own disjoint subtrees, so the
grouping changes no store bytes (the byte-identity oracle re-runs at
`max_nodes_per_invoke=1`). What it does change is the invoke count, and that
scales with the store's order: the finest tuple of an o9 store is ~49,000
dispatch nodes, so one node per invoke means ~49,000 `Event` invokes, ~49,000
objects under the run's status prefix, and a barrier that re-LISTs a 49,000-name
prefix every `poll_interval_s`. Sweeping a store that large, pass a cap sized to
the per-invoke wall — or `max_nodes_per_invoke=None`, which restores
payload-only packing (a whole tuple on one worker whenever it fits the 250 KB
cap, which is what this transport did before the ruling).

Both the ad-hoc driver and the runner's tail
(`runner._invoke_lambda_stage_sweep`) take the knob by that name.

One barrier is bounded by `barrier_timeout_s` (default 2,700 s — three times
the 900 s function timeout: queue drain, one throttle redelivery, and the
slowest worker's own run), and the *sum* of them by `total_barrier_budget_s`
(default 7,200 s), so the tail's worst case is a constant rather than a
function of the store's order. Past the total, each remaining barrier degrades
to a single check. An expiry is recorded as `barrier_timed_out` on the
dispatcher's run summary, forwarded on the finisher's stage block, and stamped
into the store-root run record (`sweep_stats_{ts}_stages.json`) and
`finisher.json` — so a run whose actuals may be short says so durably, not
only in a dispatcher log that dies with the process. When the invoke was merely
*queued* rather than lost, its late artifacts are still correct — a same-run
sibling is not foreign, so nothing aborts — and the cost is exactly that the
finisher's per-level actuals under-report until the next pass heals them.

Admission is the ordinary per-store sweep lease (`sweep.lease.json`). The
*first* stage worker creates the intent; every sibling of the same run reads
its own back; a live foreign intent refuses the invoke by name. Release is the
finisher's final act, so a run that dies mid-fan-out leaves a claimable
intent, not an open store.

### Running it

Opt in with `output.sweep: "stages"` — the same knob the spatial local
dispatcher reads — and both **spatial** Lambda tails chain the fleet sweep
after the rollup-families leg, auto-scoped to the run's own footprint:
`runner._run_lambda` (`python -m zagg` / `agg`) and the `client` facade's
`Run.dispatch` (issue #588, either transport), through the one seam
`runner._invoke_lambda_stage_sweep`.

!!! warning "Only the spatial Lambda tails chain it"
    `output.sweep: "stages"` validates on any hive config, and every tail
    reads it as truthy and runs the rollup-families sweep — but only the
    spatial Lambda tails go on to dispatch the staged one. Two tails do not:

    * **raster** (`data_source.reader: raster`) does not chain, and is right
      not to. A raster store is column-less by construction — there are no
      digest columns to fold above the shard — so it declares no
      `zagg-pyramid/2` ladder at all, and a stage worker handed one would
      refuse it at the `/2` declaration gate (`zagg.sweep_stage.ladder_entries`)
      rather than build anything. Nothing is missing from such a store.
    * a session **reattached** with `Run.attach` runs the families sweep when
      it runs the tail, and never the staged one: attach is observe-only, and
      the staged sweep (with its lease) is the launching dispatcher's. A run
      whose launcher died before or during the staged sweep is finished by
      hand — see [Recovering a run whose launcher died](#recovering-a-run).

Ad hoc, drive `zagg.sweep_fleet.run_stage_sweep_fleet` with a boto3 Lambda
client:

```python
import boto3
from zagg.sweep_fleet import run_stage_sweep_fleet

summary = run_stage_sweep_fleet(
    boto3.client("lambda"),
    "process-shard-8192-disk",  # the STAGE tier of runner.DEFAULT_FUNCTION_NAME's family
    "s3://bucket/prefix.zarr",
    leaves,                    # [(shard_key, window), ...]
    shard_order=6,
    store_kwargs={"region": "us-west-2"},
    # Omitted here, so the ruled default rides: one dispatch node per invoke.
    # max_nodes_per_invoke=None,   # payload-only packing instead
    # A windowed store: one invoke per (node, window), plus the per-node
    # close when the store declares the all-time fold.
    # windowed=True, all_time=True,
    # The aggregation run this sweep completes (issue #593), if any.
    # pipeline_run_id="<run id>",
)
```

`shard_order` is passed in rather than read from the manifest on purpose: a
dispatcher role may hold nothing but `lambda:InvokeFunction` against the
store itself.

**Computing the assignment from the store's coverage.** By default the
dispatch nodes come from the work set alone. Hand in the store's coverage MOC
and each tuple's node set becomes the ruled computation instead — the coverage
resolved to that tuple's dispatch order, intersected with the shard set — so
the worker count per tuple is *evaluated from the store*, never a number
written down anywhere:

```python
from zagg.hive import read_root_coverage, root_coverage_words

coverage = root_coverage_words(read_root_coverage("s3://bucket/prefix.zarr"))
summary = run_stage_sweep_fleet(..., coverage=coverage)   # composes with scope=
```

The read happens **operator-side**, like `shard_order`: the dispatcher itself
never touches the store (D8). The run summary records `coverage_computed` so
the run says which derivation it used.

**Permissions.** Nothing new on the worker side — the execution role already
writes the store and the `<store>.status/` sibling (the issue #151 async result
channel), which is where the stage records land. The dispatcher needs
`lambda:InvokeFunction`, plus **two** grants on that sibling prefix:
`s3:ListBucket` for the barrier's poll and `s3:GetObject` for the finisher
record it reports (`stage_records`, `levels`, `lease`, `record`,
`duration_s`). The v2 Event transport already both lists and gets that prefix,
so a correctly scoped dispatcher role needs no new grant, and no
CloudFormation, layer, or IAM template change ships with this transport. The
run tail's families barrier ([issue #610](https://github.com/englacial/zagg/issues/610))
polls the same prefix — `<store>.status/run-<run_id>/` — so it needs no new
grant either.

Mind the shape if you are writing the policy by hand: `s3:ListBucket` is a
**bucket-level** action, so its `Resource` is the bucket ARN and the prefix
rides an `s3:prefix` condition. A key-level ARN matches nothing:

```yaml
# WRONG — never matches
- Effect: Allow
  Action: s3:ListBucket
  Resource: arn:aws:s3:::bucket/prefix.zarr.status/*

# Right
- Effect: Allow
  Action: s3:ListBucket
  Resource: arn:aws:s3:::bucket
  Condition: { StringLike: { s3:prefix: "prefix.zarr.status/*" } }
- Effect: Allow
  Action: s3:GetObject
  Resource: arn:aws:s3:::bucket/prefix.zarr.status/*
```

Both failures are quiet rather than loud. A missing list grant reads in the run
log exactly like slow workers — "the prefix is empty" and "the LIST failed" are
the same empty set — until the dispatcher gives up after five consecutive
faulted LISTs and proceeds fail-open. A missing `s3:GetObject` completes the
run and simply drops the finisher's reporting from the summary.

!!! note "Dispatch nodes come from the work set, not from the store"
    The dispatcher cannot read the root `coverage.moc`, so it derives dispatch
    nodes from the leaves it holds. The in-process pass derives them from work
    set ∪ root MOC, so a node whose *only* leaves are untouched siblings
    recorded in the MOC is not invoked. That is the same scoped-sweep posture
    `stage_sweep_after_run` already has (#381 point (11)); a worker still folds
    every child on disk under each node it *is* handed, so untouched siblings
    are folded in, never dropped.

!!! warning "The tail blocks while the sweeps run"
    Unlike every other end-of-run invoke, the two sweep legs are not
    fire-and-forget. The families leg ([issue #610](https://github.com/englacial/zagg/issues/610))
    waits on two barriers — its partitions', then its finisher's — of up to
    `DEFAULT_BARRIER_TIMEOUT_S` (2,700 s) each. The staged sweep's tuple
    ordering has to be held by somebody, and the dispatcher is the only party
    that sees each tuple finish; its barrier waits are bounded by a total
    budget. Both legs are fail-open (D9) — a refused lease, a lost invoke or
    an expired barrier costs one later `python -m zagg.sweep` (or
    `--stages`) pass, never a wrong answer. A dispatcher
    killed mid-barrier leaves the run's lease held until its TTL expires into
    claimability, which is the lease's designed recovery.

### Why the fleet build is trusted

Grouping is a dispatch knob, never grammar — the **merge-source law** (espg
ruling 2026-08-09, issue #384) makes the build a fixed function of the store,
independent of tuple width, partitioning, and executor. Splitting a tuple
across invokes is therefore free: dispatch nodes at one order own disjoint
subtrees, and a tuple's folds read only columns one tuple *finer*.

That is not an argument, it is the test. `TestByteIdentityOracle` in
`tests/test_sweep_stage_fleet.py` builds a column-bearing store, runs the CLI
staged sweep, snapshots every object, resets the store to its pre-sweep bytes,
runs the fleet path with the Lambda client mocked to execute the worker arm
in-process, and byte-compares every store artifact — chunk data exactly, JSON
modulo the run identity and the clock. The two run records are checked for
shape rather than bytes (one of each, no leftover lease), since they differ
between executors by design. Any other difference is a transport bug.

It runs that comparison across every degree of freedom the transport has:
tuple widths 1, 2 and 3 (136–181 objects each), both executors at the same
width so the comparison stays full; a multi-batch fan-out with the
payload cap squeezed so one tuple needs several invokes; an executor that runs
each worker only while the barrier is waiting, so the barrier is falsifiable;
a windowed store; and a store whose digest field carries both the located and
the temporal companion channel. Two negative controls prove the comparison has
teeth — a wrong fold and a scoped-out subtree both fail it. One arm is
deliberately narrower: the CLI-at-width-3 vs fleet-at-width-1 comparison drops
each artifact's group `zarr.json`, because `source_children` names a
genuinely different fold across widths, and re-checks those attrs separately
so nothing else can hide behind the exclusion.

The one **documented** difference between executors is pinned by its own test:
the dispatcher derives dispatch nodes from the work set it holds, so a subtree
that appears only in the root MOC is not invoked. It is real, and on an
appended store it bites on every incremental run — the root MOC carries every
prior run's shards while the work set carries only this one's.

For the **chained** case the identity claim still holds, but not because the
work set covers the MOC (it does not). It holds because the local twin is
scoped the same way: `stage_sweep_after_run` passes the run's own shard
decimals as `scope`, which filters the in-process pass's `work set ∪ MOC`
right back down to the run's footprint. Same node set, same bytes. Drive the
fleet transport over a work set narrower than the store's coverage and the
difference is exactly what the test pins — the un-invoked subtree keeps its
prior ladder and heals on the next pass that does include it.

!!! info "Live-fleet validation is deferred"
    The acceptance above is fully offline by design. Validation against a real
    deployed function rides the next release plus a cents-scale probe on a
    column-bearing probe store (the SERC GEDI 0.50 probe store is exactly that
    testbed); no deploy, no Lambda invoke, and no S3 write was made from the
    implementing branch. The `stage` arm ships in the function zip like any
    other handler change — there is no template, layer, or IAM change to
    stage first.

## Recovering a run whose launcher died {#recovering-a-run}

The process that launches a run — `python -m zagg`, or a notebook's
`Run.dispatch` — also finishes it. Workers write every byte, but the launcher
holds the order: it fans the shards out and, once the last one settles (after
the idempotent store-manifest backstop and the root `coverage.moc`), fires:

| End-of-run step | What lands | If the launcher dies before it |
|---|---|---|
| 1. run record | `stats_<ts>_<run_id>.parquet` at the store root, then the marker `<store>.status/run-<run_id>/tail.json` | the leaves exist but no run record names them, and the hand sweeps below find their work in the run records |
| 2. rollup sweep | the rollup families: `4^k` partition invokes sized from the run's leaf count, then the finisher — handed the partition records' names, it composes the root `coverage.moc`/`coverage.toc` from the accumulators they carry and reads no leaf for `stats`/`moc`/`submap` (the `/1` `overview` fold still opens its leaves); each lands its `sweep_stats_<ts>[_p<i>of<n>].json` record at the store root (what `python -m zagg.sweep` and this runbook read) and a copy as `families-p<i>of<n>.json` / `families-finisher.json` under `<store>.status/run-<run_id>/` — the dispatcher awaits those copies and reports `families_sweep: {partitions, fired, landed, finisher, accumulators, run_id, records_from, duration_s}` on the summary and the handle ([issue #610](https://github.com/englacial/zagg/issues/610)) | partitions already invoked finish; the finisher never fires and the root `coverage.moc`/`coverage.toc` stay as they were: `python -m zagg.sweep <store>` regenerates them |
| 3. staged sweep (`output.sweep: "stages"` only) | the ladder and its Icechunk node commits, over a schedule **sized to the per-invoke fold** so no dispatch node folds more than `STAGE_TARGET_NODES` nodes — one invoke's own fold is that times the dispatch nodes it is handed, which is 1 at the tail's default ([issue #610](https://github.com/englacial/zagg/issues/610)); any tuple short a unit record has its orders named to the finisher as `short_orders`, whose manifest actuals are then withheld rather than stamped; last, the finisher releases the lease, then `sweep_stats_<ts>_stages.json` lands at the store root | nodes already invoked finish; later tuples and the finisher never fire, no record lands, and `sweep.lease.json` stays held until 900 s (its default TTL) past its last heartbeat |
| 4. Icechunk finalize | the `finalize <run_id>` commit and the tag `run-<run_id>` | the run is untagged |

Shards the launcher had not dispatched yet never run. Work through the steps
below from a host in the store's region: they read status objects and, for the
hand sweep, the leaf columns.

**1. Find the run and where it stopped.** A Lambda-dispatched run has a
prefix *beside* the store, `<store>.status/run-<run_id>/` (a `run-stage-…`
prefix is a staged sweep's own). In it:

- `manifest.json` — the dispatch manifest: `run_id`, `dispatched_at`, the
  config the run shipped, and its shard list (`shards: null` plus
  `shards_omitted` on a large run: a *slim* manifest). Pick the run by
  `dispatched_at`. No manifest at all means the setup write was lost or the
  block did not fit its event (the launcher's summary said
  `dispatch_manifest: "dropped"`): neither `Run.attach` nor `finalize` can
  serve that run — go to step 4.
- `shard-<key>.json` — one per shard that reported (`status`: `ok`,
  `no_data` or `failed`); `tail.json` — the run-record step completed.

At the store root, a `sweep_stats_<ts>_stages.json` newer than the run with a
`finisher` block and neither `error` nor `barrier_timed_out` is a completed
staged sweep; its `pipeline_run_id` says which run it completed (`null`: a
pass that named none). In the repo (`<store>/icechunk`), `init <run_id>` (or `split
ratchet … <run_id>`, when its init re-cuts the split) opens each run on
`main`, and a finished run has its tag:

```python
from zagg.icechunk_refs import open_repo

repo = open_repo("s3://bucket/store.zarr", store_kwargs={"region": "us-west-2"})
sorted(repo.list_tags())       # run-<run_id>; retain_runs > 0 deletes the older ones
[s.message for s in repo.ancestry(branch="main")][:10]    # newest first
```

**2. Reattach.**

```python
from zagg.client import Run

handle = Run.attach("s3://bucket/store.zarr", "<run_id>")  # the store root as dispatched
handle.wait()                    # every shard settled, plus whatever tail attach ran
handle.status()                  # {"pending": 0, "ok": n, "failed": n}
handle.icechunk_finalize         # the tag record, {"skipped": ...}, {"error": ...} or None
handle.dispatch_manifest, handle.unreported_shards        # "full" | "slim", and a count
```

Attach rebuilds the handle from the manifest and the status objects, for
unwindowed spatial runs (a windowed run raises). It only observes shards: it
never re-dispatches one. A `failed` status, or no status by the drop deadline
— the function timeout plus 150 s after the manifest's `dispatched_at` —
resolves as a failure, so wait that long after the launcher died before
attaching, or a shard still running can be recorded as failed. Shards
that failed or never ran need a new dispatch: `Run.dispatch(shard_keys=…)` on
a `Run.from_config(...)` built from the run's config and shard map (the
handle carries no granules). That is a new run with its own `init` commit,
after which `finalize` of the old run is always skipped (the new run's tag
covers it), so finalize the old run first if you want it tagged.

With no `tail.json`, the attached session runs the tail itself, through worker
invokes: the store-manifest backstop, the root `coverage.moc`, the run record
and the rollup sweep. It **never** runs the staged sweep, and for a `sweep:
"stages"` run it never tags (`handle.icechunk_finalize` is `{"skipped": …}`);
only a run pinned `commit: "leaf"` with no staged sweep is tagged by attach,
and only while it is the repo's newest run. With `tail.json` present, attach
does that finalize and nothing else.

A slim manifest names no shards, so attach takes them from the status objects.
While `handle.unreported_shards` is non-zero the handle is a snapshot of the
shards that reported and runs **no tail**; past the drop deadline the rest
never report, attach cannot finish that run, and its warning says to
re-dispatch.

**3. Finish the run by hand, in this order** — only once the
run's own `stats_<ts>_<run_id>.parquet` is at the store root (the tail writes
it, step 2). Without it the sweep cannot find the run's leaves, yet still
succeeds over other runs' leaves, and a pass you NAME for this run is one
`finalize` accepts: it would tag a ladder without them. Re-dispatch instead.

```
python -m zagg.sweep s3://bucket/store.zarr             # the rollup families
python -m zagg.sweep s3://bucket/store.zarr --stages --pipeline-run-id <run_id>   # sweep: "stages" only
python -m zagg.icechunk_ops s3://bucket/store.zarr finalize <run_id>   # ditto
```

`--pipeline-run-id` is what lets the `finalize` after it tag: `finalize`
takes only a completed staged-sweep record that **names the run**
([issue #593](https://github.com/englacial/zagg/issues/593)), written since
the run's init commit. A `--stages` pass without it records
`pipeline_run_id: null`, vouches for no run, and `finalize` refuses, naming
the record it found and this command. The newest record that names the run
decides: a later pass that names none neither vouches for the run nor
blocks it, and neither does a record that is not a JSON object (passed over
with a warning). So a named retry that failed refuses even when an earlier
named pass completed: run the named pass again, to completion. Record keys
resolve to one second, so a pass finishing in the same second as the named
one overwrites its record; leave a second between them.

The families pass is idempotent; run it even when `tail.json` exists, since
the launcher may have died before the families invoke (attach then runs no
tail). *The sweeps* run in the calling process and write the store from it, so
they need the store's write credentials and a host in the store's region.
Where the operator cannot write the store (a Source Cooperative–published
one), drive the staged sweep's fleet form instead — `run_stage_sweep_fleet`,
under [Running it](#running-it) — with `leaves` the run's own `(shard_key,
window)` pairs, which keeps it to the run's footprint as the chained sweep is
(`zagg.sweep.discover_leaves(store, store_kwargs=…)` gives every run's), and
`shard_order` the store manifest's, and `pipeline_run_id="<run_id>"` (the
fleet form's spelling of the flag). The CLI `--stages` pass covers every leaf
the run records name, not only this run's: a whole-store re-fold on one host
(`--partitions` bounds its memory, not its scope). On success it prints its
summary: `record`, the `sweep_stats_<ts>_stages.json` it wrote, and a
`finisher` block with `lease_released: true`. Skip that pass when step 1
found a completed staged sweep newer than the run whose `pipeline_run_id` is
this run's.

- `No completed leaves found in the store's run records` — no run record,
  this run's included, names a completed leaf: there is nothing to fold.
- `SweepRefusedError: a sweep already holds this store: run '…'` — the dead
  launcher's sweep (or a live one) holds the lease. Wait out the `ttl` the
  message quotes and run it again; the new sweep claims the expired lease.
- A sweep that fails midway leaves a record carrying `error` and the lease
  held: wait out the TTL and run it again.

*The finalize* on an `s3://` store is one synchronous Lambda invoke; this host
reads nothing from the store and writes nothing, and needs only invoke rights.
The function is `--function-name`, else `ZAGG_LAMBDA_FUNCTION_NAME`, else
`process-shard`, and the deployed worker must be at least as new as this
operation: an older one fails on the event's missing `config` before any
write, and the command reports a worker that `predates the operator
finalize` — deploy a current worker. On success it prints the worker's
report: `tagged: true`, the `tag`, its `snapshot`, the `stage_record` that
vouched for the ladder and the `function_name` invoked.
`tagged: false` comes with `skipped`: either the tag `already exists`, or `a
later run has committed since` — it only ever tags the repo's **newest** run.
A refusal raises and writes nothing:

- `no staged-sweep record … since run …'s init commit`, or `… does not show a
  completed sweep` — run the sweep above (again), then finalize.
- `no dispatch manifest with a config` — wrong store or run id, or the
  manifest was lost or dropped: this run cannot be tagged here (step 4).
- `no init commit for run …` — the run never initialized this repo, or a
  later run's retention squashed its commits: nothing to tag.
- `may not have finalized` (a `RuntimeError`) — the invoke failed or its
  response was lost, so the outcome is unknown: run it again, an existing tag
  is reported and nothing is rewritten.

**4. Or do nothing — only if the run's own `stats_<ts>_<run_id>.parquet` is at
the store root.** An untagged run's commits stay on `main`, and the next
run's tag covers them. What waits is the ladder — and, for a ladder-committed
run, the repo's refs — over leaves no sweep has reached: a later run's
chained sweep is scoped to its own footprint, so those leaves are folded by
the next sweep that includes them, such as the unscoped `--stages` pass
above. Without that record no sweep ever finds the run's leaves (discovery
reads the run records only): re-dispatch its shards (step 2).

The reference for each piece: the [staged sweep](../hive_layout.md#the-staged-sweep-issue-384),
the [Icechunk repo and `finalize`](../hive_layout.md#the-icechunk-companion-repo),
and specification §11.4.

## Cost Estimate

**Per invocation** (180s average, 2 GB memory): ~$0.006

**Full run** (~1,300 cells at order 6): ~$2 including S3 and CloudWatch costs.

## Troubleshooting

!!! warning "Missing s3_credentials"
    Ensure your orchestrator script calls [`get_nsidc_s3_credentials`][zagg.auth.get_nsidc_s3_credentials] and passes the credentials to each Lambda invocation.

!!! info "No granules found"
    This is normal for cells outside the data coverage area. The function returns gracefully with `error: "No granules found"`.

!!! warning "S3 write permission denied"
    Check that the Lambda execution role has `s3:PutObject` permission for the output bucket.

!!! warning "Too many open files"
    `[Errno 24] Too many open files` means concurrent workers exceeded the
    open-file soft limit and cells would be dropped. Raise it (`ulimit -n 8192`)
    or lower `--max-workers`. See "Concurrency, workers, and file descriptors"
    above — the orchestrator now clamps workers to the FD and account-concurrency
    limits automatically.
