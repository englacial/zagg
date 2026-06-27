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
