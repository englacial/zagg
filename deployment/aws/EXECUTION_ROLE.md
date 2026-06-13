# zagg Lambda execution role

> ⚠️ **Status: out of date / unverified — see [#34](https://github.com/englacial/zagg/issues/34).**
> This document covers **only** the IAM-constrained path: accounts whose deploy
> identity *cannot* create IAM roles (e.g. an AWS SSO "power user" permission
> set), where an admin creates the execution role out of band and you deploy with
> `CreateExecutionRole=false`. The **supported, verified** path is the default —
> run `stand_up.sh` (→ `template.yaml` with `CreateExecutionRole=true`) from an
> identity that has IAM access, and the backend stack creates the role for you,
> with no out-of-band step. The from-scratch AWS deploy validated in
> [#32](https://github.com/englacial/zagg/pull/32) exercised that default path;
> the SSO no-`iam:CreateRole` flow described below has **not** been re-validated
> and may be stale. Treat it as legacy until it is re-tested.

The `process-shard` Lambda needs an IAM **execution role** — the identity it runs
as, which lets it write CloudWatch logs and write results to the in-account
output bucket (e.g. `sliderule-public`).

By default you don't have to think about this: `stand_up.sh` deploys
`template.yaml` with `CreateExecutionRole=true`, and the stack creates the role
for you. **This document is only for the other case** — accounts where the
identity running the deploy *cannot create IAM roles* (for example an AWS SSO
"power user" permission set, which grants everything except IAM). There, an
account admin creates the role once, out of band, and hands you its ARN.

## Who does what

| Step | Who | Needs `iam:CreateRole`? |
|------|-----|-------------------------|
| Create the execution role (`execution_role.yaml`) | account **admin** | yes |
| Deploy the backend against that role (`stand_up.sh`) | **you** | no |

The role lives in its **own** small stack, owned by the admin. The backend stack
(`zagg-backend`) then contains **zero IAM resources**, so every future
update you make to it — new layer, new function code — is pure Lambda/S3/CloudFormation
and needs no admin involvement ever again.

> Do **not** ask the admin to run `stand_up.sh` for you. That would fold the IAM
> role into the backend stack, making the admin a recurring dependency for any
> later update that touches the role. Keep the role in its own stack.

## What the role grants (least privilege)

The role trusts `lambda.amazonaws.com` and allows exactly:

- **CloudWatch Logs** — create/write the function's own log group.
- **S3 write** — `Get/Put/DeleteObject` + `ListBucket` on **one** output bucket
  (`OutputBucketName`, e.g. `sliderule-public`) and nothing else.

It deliberately stays scoped to a single in-account bucket. **Writing to
external object stores (source.coop, other accounts/clouds) does NOT go through
this role** — those use credentials injected per-invocation in the event (see
[issue #26](https://github.com/englacial/zagg/issues/26)). So there is never a
reason to widen this role to reach external buckets; keep it fail-closed.

## Creating the role (admin)

### Option A — CloudFormation (preferred)

`execution_role.yaml` is a standalone CloudFormation template. Deploy it as its
own stack:

```bash
aws cloudformation deploy \
  --template-file execution_role.yaml \
  --stack-name zagg-exec-role \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides OutputBucketName=sliderule-public \
  --region us-west-2
```

Parameters:

| Parameter | Default | Notes |
|-----------|---------|-------|
| `OutputBucketName` | *(required)* | Bucket the function may write into, e.g. `sliderule-public`. |
| `RoleName` | `zagg-process-shard-exec` | Name of the created role. |
| `FunctionName` | `process-shard` | Used only to scope the log-group ARN; must match `FunctionName` in `template.yaml`. |

> `CAPABILITY_NAMED_IAM` is required because the template sets an explicit
> `RoleName`. If your account's guardrails dislike named IAM resources, drop the
> `RoleName` property and deploy with plain `CAPABILITY_IAM` instead.

Then read the role ARN back out:

```bash
aws cloudformation describe-stacks --stack-name zagg-exec-role \
  --region us-west-2 --query 'Stacks[0].Outputs[?OutputKey==`RoleArn`].OutputValue' \
  --output text
```

### Option B — manual / console

If you'd rather not use CloudFormation, create a role that trusts Lambda and
attach an inline policy. Trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole" }
  ]
}
```

Permissions policy (replace the account id, region, and bucket name):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "WriteFunctionLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:us-west-2:ACCOUNT_ID:log-group:/aws/lambda/process-shard:*" },
    { "Sid": "WriteOutputObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::sliderule-public/*" },
    { "Sid": "ListOutputBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::sliderule-public" }
  ]
}
```

## Deploying the backend against the role (you)

With the role ARN in hand, stand up the backend with role creation turned off:

```bash
CREATE_ROLE=false \
ROLE_ARN=arn:aws:iam::ACCOUNT_ID:role/zagg-process-shard-exec \
OUTPUT_BUCKET=sliderule-public \
./stand_up.sh
```

`stand_up.sh` passes these through to `template.yaml` as
`CreateExecutionRole=false` and `ExecutionRoleArn=<arn>`; the backend stack then
references your role instead of creating one.

> The `OUTPUT_BUCKET` you pass here is informational in this path — the binding
> constraint is the role's own policy. Keep them matching: invoking with a
> `store_path` outside the bucket the role allows fails at runtime with
> `AccessDenied`.
