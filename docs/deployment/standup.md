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
   zips) is published to a public **source.coop mirror**, keyed by zagg *minor*
   version (`0.N.x` -> `0.N`). The minor is read from the repo's latest git tag
   (so a fresh clone needs no install), falling back to the installed `zagg`, or
   an explicit `LAMBDA_VERSION` override.
2. **Locates the artifacts for the chosen `ARCH`** (`arm64` default, or
   `x86_64`) -- `lambda_layer_<arch>.zip` and
   `lambda_function_<arch>_py312.zip`.
3. **Stages code into a same-region bucket if needed.** CloudFormation requires
   Lambda code to live in a bucket **in the stack's own region**. In
   **us-west-2** (where the mirror lives) the stack reads straight from the
   mirror -- no bucket of your own required. In **any other region** you provide
   `STAGING_BUCKET` (a bucket you own in that region) and `stand_up.sh` copies
   the zips into it first.
4. **Deploys `template.yaml`** with `aws cloudformation deploy
   --capabilities CAPABILITY_NAMED_IAM`, passing the resolved architecture,
   artifact bucket/keys, output bucket, and role settings as parameter
   overrides.
5. **Prints the stack outputs** (function ARN/name, layer ARN, role ARN, output
   bucket).

## What the stack creates

`template.yaml` provisions (see the file for the authoritative definition):

- **`ProcessFn`** -- the `process-shard` Lambda (`python3.12`, handler
  `lambda_handler.lambda_handler`, default 2048 MB / 900 s timeout), wired to the
  layer and execution role.
- **`DepsLayer`** -- the dependency layer version (`<FunctionName>-deps`).
- **`ExecutionRole`** -- created only when `CreateExecutionRole=true` (the
  default). It trusts `lambda.amazonaws.com` and is scoped least-privilege to
  CloudWatch Logs plus `Get/Put/DeleteObject` + `ListBucket` on **one** output
  bucket. In IAM-constrained accounts (e.g. an AWS SSO power-user that lacks
  `iam:CreateRole`), set `CreateExecutionRole=false` and pass a pre-made role via
  `ExecutionRoleArn` -- see [Execution Role](execution-role.md).
- **`OutputBucket`** -- created only when `CreateOutputBucket=true`; otherwise the
  bucket named by `OutputBucketName` must already exist and be writable by the
  role.

> Writing to **external** object stores (source.coop, other accounts/clouds)
> does *not* go through the execution role -- those use credentials injected
> per-invocation in the event (see [AWS Lambda](lambda.md#output-credentials-external-write-targets)).
> So the role stays scoped to a single in-account bucket.

## `stand_up.sh` environment variables

Behavior is driven entirely by environment variables (the script takes no
positional arguments):

| Variable | Default | Purpose |
|----------|---------|---------|
| `OUTPUT_BUCKET` | *(required)* | Bucket where results are written; the execution role is scoped to it |
| `CREATE_BUCKET` | `false` | `true` makes the stack create `OUTPUT_BUCKET` |
| `CREATE_ROLE` | `true` | `false` skips role creation; requires `ROLE_ARN` |
| `ROLE_ARN` | *(none)* | Pre-existing execution-role ARN, required only when `CREATE_ROLE=false` |
| `ARCH` | `arm64` | `arm64` or `x86_64` (both py3.12) |
| `REGION` | `us-west-2` | Deployment region |
| `STAGING_BUCKET` | *(none)* | Required outside us-west-2: a same-region bucket the mirror zips are copied into |
| `LAMBDA_VERSION` | *(derived)* | Lambda minor to deploy (default: the repo's latest git tag, else the installed zagg) |
| `STACK_NAME` | `zagg-backend` | CloudFormation stack name |
| `MIRROR_BUCKET` / `MIRROR_PREFIX` / `MIRROR_REGION` | source.coop | Override to self-host the artifact mirror |

These map onto the `template.yaml` parameters (`Architecture`, `ArtifactBucket`,
`LayerS3Key`, `FunctionS3Key`, `OutputBucketName`, `CreateOutputBucket`,
`CreateExecutionRole`, `ExecutionRoleArn`); `MemorySize` and `Timeout` keep their
template defaults and aren't surfaced as script variables.

## Examples

```bash
# us-west-2, stack creates the role, output bucket already exists
OUTPUT_BUCKET=my-results bash deployment/aws/stand_up.sh

# Different region -- stage the zips into a bucket you own there first
REGION=us-east-1 OUTPUT_BUCKET=my-results STAGING_BUCKET=my-stage \
  bash deployment/aws/stand_up.sh

# IAM-constrained account: admin made the role, you deploy against it
CREATE_ROLE=false ROLE_ARN=arn:aws:iam::123456789012:role/zagg-exec \
  OUTPUT_BUCKET=my-results bash deployment/aws/stand_up.sh
```

## Updating and tearing down

Re-running `stand_up.sh` with a newer `LAMBDA_VERSION` (or after the mirror is
re-populated for the current minor) updates the stack in place. To remove
everything:

```bash
aws cloudformation delete-stack --stack-name zagg-backend --region us-west-2
```

Maintainers (re)populate the mirror after a release with
`deployment/aws/publish_mirror.sh <minor>`, which pushes the four CI-built zips
(plus `SHA256SUMS`) to `s3://<mirror>/englacial/zagg/lambda/<minor>/`.
