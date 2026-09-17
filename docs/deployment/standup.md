# Standing up the backend (CloudFormation)

The **recommended** way to deploy the zagg serverless backend into an AWS
account is the committed CloudFormation template
(`deployment/aws/template.yaml`), driven by the `deployment/aws/stand_up.sh`
wrapper. One command creates the execution role, the dependency layer, and the
`process-shard` function as a single stack from pre-built release artifacts:

```bash
OUTPUT_BUCKET=my-results-bucket bash deployment/aws/stand_up.sh
```

This is preferred over the manual `aws lambda create-function` /
`publish-layer-version` steps (see [AWS Lambda](lambda.md#legacy-manual-deploy)):
the stack is reproducible, versioned, and tears down cleanly, and you never have
to hand-assemble zips or wire up the IAM role yourself.

## What `stand_up.sh` does

`stand_up.sh` is a thin, **verbose** wrapper around `aws cloudformation deploy`
(it echoes each AWS command before running it). End to end it:

1. **Resolves the artifact version.** Lambda code (the deps layer + function
   zips) is published by the release pipeline to the public
   **`sliderule-public-cors` distribution bucket**
   (`s3://sliderule-public-cors/<minor>/`), keyed by zagg *minor* version
   (`0.N.x` -> `0.N`). The minor is read from the repo's latest git tag (so a
   fresh clone needs no install), falling back to the installed `zagg`, or an
   explicit `LAMBDA_VERSION` override (`LAMBDA_VERSION=latest` reads the
   newest published minor from the bucket's `versions.json`).
2. **Locates the artifacts for the chosen `ARCH`** (`arm64` default, or
   `x86_64`) -- `lambda_layer_<arch>.zip` and
   `lambda_function_<arch>_py312.zip`.
3. **Verifies the minor is actually staged and asks for confirmation.** The
   layer and function keys are HEAD-checked on the distribution bucket before
   any stack call; an unstaged minor (e.g. one derived from a repo ahead of
   the last release) fails fast with the staged minors listed, instead of
   surfacing as a CloudFormation `NoSuchKey` rollback. The resolved bucket/keys/version
   are then echoed and the script prompts before deploying (pass `--yes` to
   skip the prompt in unattended runs).
4. **Stages code into a same-region bucket if needed.** CloudFormation requires
   Lambda code to live in a bucket **in the stack's own region**. In
   **us-west-2** (where the distribution bucket lives) the stack reads straight
   from it -- no bucket of your own required. In **any other region** you provide
   `STAGING_BUCKET` (a bucket you own in that region) and `stand_up.sh` copies
   the zips into it first.
5. **Deploys `template.yaml`** with `aws cloudformation deploy
   --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND` (the latter
   acknowledges the `AWS::LanguageExtensions` macro that expands the
   worker-size variants), passing the resolved architecture, artifact
   bucket/keys, output bucket, and role settings as parameter overrides.
6. **Prints the stack outputs** (function ARN/name, layer ARN, role ARN, output
   bucket).

## What the stack creates

`template.yaml` provisions (see the file for the authoritative definition):

- **`ProcessFn`** -- the `process-shard` Lambda (`python3.12`, handler
  `lambda_handler.lambda_handler`, default 4096 MB / 900 s timeout), wired to the
  layer and execution role -- plus its `-extract` twin (own concurrency pool
  for full-archive extraction runs).
- **The worker-size variants** -- six additional functions sharing the same
  code, layer, and role, pre-provisioned so a run can pick its memory//tmp
  size by *name* with no admin-role config swap (selected via the config
  `worker:` block or `agg(function_name=...)` -- see
  [AWS Lambda](lambda.md#worker-size-variants)):

  | Function | Memory | `/tmp` |
  |----------|--------|--------|
  | `process-shard-2048` | 2048 MB | 512 MB |
  | `process-shard-4096` | 4096 MB | 512 MB |
  | `process-shard-8192` | 8192 MB | 512 MB |
  | `process-shard-2048-disk` | 2048 MB | 4096 MB |
  | `process-shard-4096-disk` | 4096 MB | 6144 MB |
  | `process-shard-8192-disk` | 8192 MB | 10240 MB |

  `-disk` `/tmp` is memory + 2048 MB (10240 is Lambda's ceiling). The
  unsuffixed `process-shard`/`-extract` pair stays the no-config default.
  Each variant carries the same async-invoke hygiene (retries 0 / event age
  60 s) and self-recycle/worker-error metric-filter pair as the base
  function; idle variants cost nothing (Lambda bills invocations only).
- **`DepsLayer`** -- the dependency layer version (`<FunctionName>-deps`).
- **`ExecutionRole`** -- always created by the stack, named by
  `ExecutionRoleName` (default `zagg-lambda-execution`). It trusts
  `lambda.amazonaws.com` and grants CloudWatch Logs plus, statement by
  statement: `Get/Put/DeleteObject` **and** `ListBucket` on the output bucket;
  `Get/Put/DeleteObject` on the public `sliderule-public-cors` bucket
  (whole-bucket, by intent -- PR #176; that bucket is being retired, issue
  #499 -- its sidecar cache was copied 2026-09-17 to the Source Cooperative
  `englacial/zagg/sidecar/*` prefix, which the grant below does **not**
  cover); and, on Source Cooperative, `Get/Put/PutObjectAcl/DeleteObject`
  plus the multipart pair on the published object prefix
  `englacial/zagg/demo/*`, with plain `ListBucket` granted on the **bucket**
  rather than the prefix, unconditioned on purpose, because S3 answers a GET
  for an absent key with 404 only when the caller holds bucket-level
  `ListBucket`, and zagg's absence checks catch 404 only.
  `ListBucketMultipartUploads` is deliberately absent: it is bucket-wide and
  cannot be prefix-constrained, so Source Cooperative does not grant it and
  uploads leaked by a killed worker age out on their 7-day lifecycle rule
  instead. Standing the stack up therefore needs `iam:CreateRole`: in an
  IAM-constrained account (e.g. an AWS SSO power-user), have an admin run the
  standup rather than mint a role separately. The name is explicit and stable
  because it is zagg's **published identity** -- Source Cooperative names this
  ARN in their bucket policy -- so a second stack in the same account must
  override `EXECUTION_ROLE_NAME` to avoid a collision.

  **Run the first update after [issue #495](https://github.com/englacial/zagg/issues/495)
  with an idle fleet.** Adding `RoleName` to an already-deployed *unnamed* role
  is a **replacement**, not an in-place edit: CloudFormation creates the new
  role, re-points the five `Role:` references, then **deletes the old one**
  during cleanup. A warm Lambda sandbox holds credentials vended from the old
  role, so that delete invalidates them and any 900 s worker still mid-shard
  starts getting `AccessDenied`. IAM/Lambda propagation is not instant either
  -- `UpdateFunctionConfiguration` against a freshly created role can fail with
  *The role defined for the function cannot be assumed by Lambda* until IAM
  catches up; CloudFormation retries, but a rollback there leaves five
  functions to re-point by hand. The same applies to any later change of
  `EXECUTION_ROLE_NAME` on a live stack.
- **`OutputBucket`** -- created only when `CreateOutputBucket=true`; otherwise the
  bucket named by `OutputBucketName` must already exist and be writable by the
  role.

> Publishing to **Source Cooperative** goes through the execution role itself
> (issue #495): the fleet is one communal writer, and staging
> (`sliderule-public*`) versus published (source.coop) is a difference in store
> maturity, not in identity. Credential **injection** remains for targets we
> have not negotiated a bucket policy with -- a collaborator's private bucket,
> R2/MinIO -- see
> [AWS Lambda](lambda.md#output-credentials-external-write-targets).
>
> **Publish into a subdirectory of the granted prefix, never at the prefix
> root.** A run's status objects are a string-concatenated *sibling* of the
> store, not a child: a store at
> `s3://us-west-2.opendata.source.coop/englacial/zagg/demo/foo.zarr` puts them
> under `.../demo/foo.zarr.status/`, inside the grant, but a store written at
> the prefix root (`.../englacial/zagg/demo`) puts them at
> `.../englacial/zagg/demo.status/`, outside it. The
> [write probe](lambda.md#write-probe) makes that fail closed before fan-out
> rather than mid-campaign, so the policy is deliberately not widened for it.

## `stand_up.sh` environment variables

Behavior is driven by environment variables; the only flag is `--yes` (skip
the pre-deploy confirmation prompt):

| Variable | Default | Purpose |
|----------|---------|---------|
| `OUTPUT_BUCKET` | *(required)* | Bucket where results are written; the execution role is scoped to it |
| `CREATE_BUCKET` | `false` | `true` makes the stack create `OUTPUT_BUCKET` |
| `EXECUTION_ROLE_NAME` | `zagg-lambda-execution` | Name of the execution role -- zagg's published identity. Override for a second stack in the same account |
| `ARCH` | `arm64` | `arm64` or `x86_64` (both py3.12) |
| `REGION` | `us-west-2` | Deployment region |
| `STAGING_BUCKET` | *(none)* | Required outside us-west-2: a same-region bucket the release zips are copied into |
| `LAMBDA_VERSION` | *(derived)* | Lambda minor to deploy (default: the repo's latest git tag, else the installed zagg; `latest` reads `versions.json`). Whatever it resolves to must be staged on the distribution bucket -- verified before the stack call |
| `STACK_NAME` | `zagg-backend` | CloudFormation stack name |
| `DIST_BUCKET` / `DIST_PREFIX` / `DIST_REGION` | `sliderule-public-cors` / *(none)* / `us-west-2` | Override to self-host a copy of the release artifacts |

These map onto the `template.yaml` parameters (`Architecture`, `ArtifactBucket`,
`LayerS3Key`, `FunctionS3Key`, `OutputBucketName`, `CreateOutputBucket`,
`ExecutionRoleName`); `MemorySize` and `Timeout` keep their template defaults
and aren't surfaced as script variables.

## Examples

```bash
# us-west-2, output bucket already exists
OUTPUT_BUCKET=my-results bash deployment/aws/stand_up.sh

# Different region -- stage the zips into a bucket you own there first
REGION=us-east-1 OUTPUT_BUCKET=my-results STAGING_BUCKET=my-stage \
  bash deployment/aws/stand_up.sh

# A second stack in the same account: the role name must not collide
STACK_NAME=zagg-backend-test FUNCTION_NAME=process-shard-test \
  EXECUTION_ROLE_NAME=zagg-lambda-execution-test \
  OUTPUT_BUCKET=my-results bash deployment/aws/stand_up.sh
```

## Updating and tearing down

Re-running `stand_up.sh` with a newer `LAMBDA_VERSION` (or after the
distribution bucket is re-populated for the current minor) updates the stack in
place. To remove everything:

```bash
aws cloudformation delete-stack --stack-name zagg-backend --region us-west-2
```

The distribution bucket is populated automatically on release: pushing a
version tag runs `publish.yml`'s `distribute` job
(`.github/scripts/distribute_zips.sh`), which pushes the four CI-built zips
(plus `SHA256SUMS`) to `s3://sliderule-public-cors/<minor>/` and updates the
top-level `versions.json` index.
