# Lambda benchmark CI/CD setup

This guide stands up the Lambda-benchmark pipeline (issue #110): on every
same-repo PR and every merge to `main`, GitHub Actions dispatches the single
densest shard over the NEON SERC AOP box to the deployed `process-shard` Lambda,
harvests seconds + cost, and tracks **cost/shard** and **cost/100 km²** over
merge history.

The *code* (workflows, scripts, pinned targets) ships in the repo. This page is
the one-time *infrastructure* setup: the AWS OIDC trust, a scoped IAM role, an S3
output bucket, the GitHub repo variables/secrets, the `benchmarks` data branch,
and the `benchmark` label — each with a step to confirm it worked.

You need: admin on the GitHub repo, an AWS account with the `process-shard`
Lambda already deployed (see [Standing Up the Backend](standup.md)), the AWS CLI
authenticated to that account, and an Earthdata Login (the orchestrator mints
NSIDC S3 read credentials it forwards to the Lambda).

Throughout, substitute your values for `ACCOUNT_ID`, `REGION` (e.g.
`us-west-2`), `FUNCTION_NAME` (default `process-shard`), and the repo
`englacial/zagg`.

---

## What the pipeline needs (overview)

| Piece | Why |
| --- | --- |
| GitHub OIDC provider in AWS | Lets Actions mint short-lived AWS creds with **no stored secret** |
| Scoped IAM role (`zagg-benchmark`) | What those creds can do: invoke the one Lambda, read/write the one bucket prefix |
| S3 bucket / prefix | Where the benchmark Zarr stores are written |
| Repo **variables** `BENCHMARK_*` | Non-secret config (role ARN, region, function, store prefix) |
| Repo **secrets** `EARTHDATA_*` | Earthdata Login for the orchestrator to mint NSIDC read creds |
| `benchmarks` branch | Holds the retained parquet series + rendered charts |
| `benchmark` label | One of the two fork-PR on-demand triggers |

---

## 1. Create the GitHub OIDC identity provider in AWS

This is account-wide; skip if you already have it (e.g. from another repo).

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

**Verify** — it should list the provider:

```bash
aws iam list-open-id-connect-providers
# -> arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com
```

> The thumbprint is no longer security-critical (AWS validates GitHub's OIDC
> certificate via its own trust store) but the argument is still required.

---

## 2. Create the scoped IAM role

The role is assumable **only** by this repo's Actions, and can do **only** what
the benchmark needs. Two policy documents: a trust policy (who can assume) and a
permission policy (what it can do).

### 2a. Trust policy — pin to the repo

`trust.json` — the `sub` wildcard `repo:englacial/zagg:*` restricts assumption to
**this** repo's Actions (a fork's OIDC `sub` is `repo:<fork>/zagg:*` and can't
match), which is the standard scope. Branch/PR/fork-context gating is enforced in
the workflows themselves (same-repo guard for the auto path, write/admin actor
check for the on-demand path). If you want to tighten the trust at the IAM layer
too, replace the wildcard with explicit subs (e.g.
`repo:englacial/zagg:ref:refs/heads/main` and `repo:englacial/zagg:pull_request`)
— but note the on-demand fork path runs under `pull_request_target`, whose `sub`
is the **base** ref, so don't over-restrict or you'll lock it out:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:englacial/zagg:*"
      }
    }
  }]
}
```

### 2b. Permission policy — least privilege

`permissions.json` — invoke the one function, read its config (for the
`worker_pct_timeout` metric), and read/write the benchmark bucket prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "InvokeBenchmarkLambda",
      "Effect": "Allow",
      "Action": ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"],
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:FUNCTION_NAME"
    },
    {
      "Sid": "BenchmarkBucket",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::BUCKET",
        "arn:aws:s3:::BUCKET/PREFIX/*"
      ]
    }
  ]
}
```

> The Lambda reads the ATL03 source granules itself, using the NSIDC S3
> credentials the orchestrator forwards — that's the **Lambda execution role**'s
> concern, not this role's. This role only invokes + writes output.

### 2c. Create the role and attach the policy

```bash
aws iam create-role --role-name zagg-benchmark \
  --assume-role-policy-document file://trust.json

aws iam put-role-policy --role-name zagg-benchmark \
  --policy-name zagg-benchmark-access \
  --policy-document file://permissions.json
```

**Verify** — note the ARN (you need it in step 4):

```bash
aws iam get-role --role-name zagg-benchmark --query 'Role.Arn' --output text
# -> arn:aws:iam::ACCOUNT_ID:role/zagg-benchmark
```

---

## 3. Provision the S3 output bucket / prefix

Reuse an existing bucket or make one. The store prefix is where each target's
Zarr lands (overwritten every run — these are throwaway).

```bash
aws s3 mb s3://BUCKET --region REGION   # if it doesn't exist
```

**Verify** — a write+read+delete round-trip with the prefix you'll configure:

```bash
echo ok | aws s3 cp - s3://BUCKET/PREFIX/.probe
aws s3 ls s3://BUCKET/PREFIX/.probe && aws s3 rm s3://BUCKET/PREFIX/.probe
```

---

## 4. Configure GitHub repo variables and secrets

The workflows read four **variables** (non-secret) and two **secrets**.

```bash
# Variables (Settings -> Secrets and variables -> Actions -> Variables)
gh variable set BENCHMARK_ROLE_ARN      --body "arn:aws:iam::ACCOUNT_ID:role/zagg-benchmark"
gh variable set BENCHMARK_AWS_REGION    --body "REGION"
gh variable set BENCHMARK_FUNCTION_NAME --body "FUNCTION_NAME"
gh variable set BENCHMARK_STORE_PREFIX  --body "s3://BUCKET/PREFIX"

# Secrets (Earthdata Login for the orchestrator)
gh secret set EARTHDATA_USERNAME --body "your-edl-username"
gh secret set EARTHDATA_PASSWORD --body "your-edl-password"
```

**Verify**:

```bash
gh variable list   # 4 BENCHMARK_* entries
gh secret list     # EARTHDATA_USERNAME, EARTHDATA_PASSWORD
```

---

## 5. Bootstrap the `benchmarks` data branch

The retained parquet series and the rendered charts live on an orphan
`benchmarks` branch. The merge job checks it out and **fails if it's missing**,
so create it once:

```bash
git switch --orphan benchmarks
git rm -rf . >/dev/null 2>&1 || true
printf '# zagg benchmark data\n\nRetained `series.parquet` + rendered `site/` charts (issue #110).\n' > README.md
git add README.md
git commit -m "init benchmarks data branch"
git push origin benchmarks
git switch main
```

**Verify**:

```bash
git ls-remote --heads origin benchmarks   # one ref printed
```

---

## 6. Create the `benchmark` label

One of the two fork-PR on-demand triggers (the other is a `/benchmark` comment).

```bash
gh label create benchmark \
  --description "Run the Lambda benchmark on this PR" --color FBCA04
```

**Verify**: `gh label list | grep benchmark`.

---

## 7. End-to-end verification

1. **Manual run.** Trigger the workflow by hand — it runs the merge path
   (retains + publishes):

   ```bash
   gh workflow run "Lambda Benchmark" --ref main
   gh run watch
   ```

   On success, the `benchmarks` branch gains `series.parquet` and `site/`:

   ```bash
   git fetch origin benchmarks
   git ls-tree -r origin/benchmarks --name-only | grep -E 'series.parquet|site/'
   ```

2. **PR comment.** Open a small same-repo PR (non-draft) and confirm a
   `🤖`-free benchmark table comment appears (marker `<!-- zagg-benchmark -->`),
   updated in place on each push — and that the point is **not** added to the
   retained series (PR runs are ephemeral).

3. **Fork on-demand.** On a fork PR, comment `/benchmark` (or apply the
   `benchmark` label) as a write/admin user and confirm the run fires; confirm a
   non-write user's `/benchmark` is **ignored** (the gate logs a notice).

If a run fails at *Assume AWS role*, re-check the trust policy `sub` matches the
repo and the OIDC provider exists (steps 1–2). If it fails minting NSIDC creds,
re-check the `EARTHDATA_*` secrets (step 4).

---

## Surfacing the charts on GitHub Pages

The repo already serves the mkdocs docs from the Pages *Actions* source
(`docs.yml`), and a repo has a single Pages site — so the benchmark does **not**
deploy Pages itself. Instead the charts are **embedded in the docs**: the
[Benchmark results](benchmark.md) page references the rendered PNGs by their raw
URL on the `benchmarks` branch, so they appear in the docs site and refresh as
each merge updates the branch — no docs rebuild, no Pages reconfiguration.

The PNGs also render directly in the `benchmarks` branch file view on GitHub if
you want the raw artifacts.

---

## Cost and safety notes

- **Bounded spend.** Each run dispatches **one** shard per target, hard-capped by
  the Lambda timeout (720 s deploy default ≤ 900 s AWS max) — pennies per run.
  The auto PR job has per-PR concurrency, so rapid pushes don't stack billed runs.
- **No fork auto-runs.** Fork PRs never get the role automatically; a write/admin
  maintainer must opt in per PR (`/benchmark` or the `benchmark` label). Doing so
  runs the fork's checked-out code with the (minimally-scoped) role — that's the
  cost of benchmarking a fork; the role can only invoke the one Lambda and write
  the one bucket prefix.
- **Rotating creds.** OIDC mints fresh AWS creds per run (nothing to rotate).
  Rotate the `EARTHDATA_*` secrets per your normal policy.

---

# Deploy automation (issue #25)

The benchmark above measures the **deployed** Lambda worker. For a PR's worker
changes to actually be measured, the PR's code has to be deployed first. This
second pipeline does that across three tiers:

- **Internal PRs / merges that touch the deployed closure** (`src/zagg/**`,
  `deployment/aws/**`, `pyproject.toml`) → build arm64 + redeploy a separate
  `process-shard-test` function, then benchmark against it. Non-affecting changes
  benchmark the stable function.
- **Fork PRs** → never redeploy; the benchmark comment is annotated that the
  numbers reflect the stable worker, not the PR.
- **Releases (tags)** → update the **production** function in place and publish
  the zips to a public, listable bucket (`sliderule-public-cors`), replacing the
  source.coop mirror over a transition window.

This is the **"AWS update for this PR"** step in the rollout order
(`#112` → benchmark setup → the deploy PR → this). Until these resources +
variables exist, the deploy/distribute/prod jobs **skip cleanly** — the benchmark
keeps running against the stable function and releases still attach zips.

## 8. The `process-shard-test` stack

A second copy of the backend stack, named `process-shard-test`, so PR benchmarks
never touch production. Stand it up **once** (the template already parameterizes
the function name); subsequent PR deploys only `update-function-code`, no stack
churn.

```bash
OUTPUT_BUCKET=sliderule-public \
STACK_NAME=zagg-backend-test \
FUNCTION_NAME=process-shard-test \
  ./deployment/aws/stand_up.sh
```

(`stand_up.sh` passes `FUNCTION_NAME` to the template's `FunctionName` parameter.)
Subsequent PR deploys only `update-function-code`, so the stack is stood up once.
**Verify:** `aws lambda get-function --function-name process-shard-test` returns
the function.

## 9. Deploy + release IAM roles (OIDC)

Two more roles, both trusting this repo via the OIDC provider from section 1
(reuse the same `trust.json`):

- **`zagg-benchmark-deploy`** (test tier) — update the test function + stage the
  layer:
  - `lambda:UpdateFunctionCode`, `lambda:UpdateFunctionConfiguration`,
    `lambda:PublishLayerVersion`, `lambda:GetFunction` on `process-shard-test`
    (+ its `-deps` layer).
  - `s3:PutObject` on `s3://sliderule-public/lambda-test/*` (the staged layer).
- **`zagg-lambda-release`** (release tier) — update **production** + publish:
  - the same `lambda:*` actions on `process-shard` (+ `process-shard-deps`).
  - `s3:PutObject`/`s3:GetObject` on `s3://sliderule-public-cors/*` (distribute).

**Verify:** `aws iam get-role --role-name zagg-benchmark-deploy` /
`zagg-lambda-release`.

> The release role can mutate production, so gate it (section 11) — don't rely on
> the trust policy alone.

## 10. The `sliderule-public-cors` distribution bucket

A **real public** bucket (readable + **listable** from anywhere — unlike
`s3://sliderule-public`, which is cryocloud-only and no-list), in **us-west-2**
(CloudFormation reads Lambda code same-region) so `stand_up.sh` can fetch it
directly. Listing lets a user resolve `LAMBDA_VERSION=latest` from
`versions.json` instead of being pinned to their clone's version.

```bash
aws s3 mb s3://sliderule-public-cors --region us-west-2
# Disable the account block-public-access for this bucket, then:
aws s3api put-public-access-block --bucket sliderule-public-cors \
  --public-access-block-configuration BlockPublicPolicy=false,RestrictPublicBuckets=false
aws s3api put-bucket-policy --bucket sliderule-public-cors --policy '{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "PublicReadList",
    "Effect": "Allow",
    "Principal": "*",
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::sliderule-public-cors",
                 "arn:aws:s3:::sliderule-public-cors/*"]
  }]
}'
aws s3api put-bucket-cors --bucket sliderule-public-cors --cors-configuration '{
  "CORSRules": [{
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"]
  }]
}'
```

**Verify** (anonymous read + list both work):

```bash
aws s3 ls s3://sliderule-public-cors/ --no-sign-request
aws s3 cp s3://sliderule-public-cors/versions.json - --no-sign-request   # after the first release
```

> The bucket must be in the same region as the **production** Lambda — the
> release job's `publish-layer-version` reads the layer zip from it in-region.

## 11. GitHub Environments, variables, and tag protection

The deploy/release jobs read these **variables** (and reuse the section-4
`EARTHDATA_*` secrets):

```bash
# Benchmark test-deploy (tier 2)
gh variable set BENCHMARK_DEPLOY_ROLE_ARN    --body "arn:aws:iam::ACCOUNT_ID:role/zagg-benchmark-deploy"
gh variable set BENCHMARK_TEST_FUNCTION_NAME --body "process-shard-test"
gh variable set BENCHMARK_TEST_STAGE_BUCKET  --body "sliderule-public"
# Release (tier 3)
gh variable set LAMBDA_RELEASE_ROLE_ARN   --body "arn:aws:iam::ACCOUNT_ID:role/zagg-lambda-release"
gh variable set LAMBDA_PROD_FUNCTION_NAME --body "process-shard"
gh variable set LAMBDA_DIST_BUCKET        --body "sliderule-public-cors"
gh variable set LAMBDA_AWS_REGION         --body "us-west-2"
```

Protect the production deploy:

- **`production` Environment** (Settings → Environments → New → `production`) with
  a **required reviewer** — the release workflow's `deploy-prod` job waits for an
  approval before it mutates the live function.
- **Tag protection** for `*.*.*` so only maintainers can cut a release (a tag is
  what triggers the prod deploy + PyPI publish).

**Verify:** `gh variable list` shows the seven new vars; the Environments page
shows `production` with a reviewer.

## 12. Verify the deploy tiers

1. **Tier 2.** Open an internal PR that edits `src/zagg/processing/` — the
   benchmark run should include a `deploy-test` job and the comment should have no
   stale-worker banner. A docs-only PR should skip the deploy and (correctly) show
   no banner either.
2. **Tier 1.** On a fork PR that touches `src/zagg/`, a maintainer `/benchmark` →
   the comment carries the **"worker = stable `main`"** banner.
3. **Tier 3.** Cut a pre-release tag → the GitHub release gets the four zips
   attached, `s3://sliderule-public-cors/<minor>/` is populated with a
   `versions.json`, and (after the `production` approval) `process-shard` is
   updated. `LAMBDA_VERSION=latest ./stand_up.sh` then resolves the new minor.

## Distribution transition (source.coop → CORS bucket)

`stand_up.sh` now prefers `sliderule-public-cors` and **falls back to the
source.coop mirror** for any minor not yet on the new bucket — so older standups
keep working while new releases populate the new bucket. Once enough releases have
published to the CORS bucket, retire the source.coop mirror (and its
`publish_mirror.sh` step) in a follow-up. Set `LAMBDA_VERSION=latest` to always
pull the newest published minor.
